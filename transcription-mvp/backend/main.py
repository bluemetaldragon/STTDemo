import asyncio
import json
import logging
import os
import threading

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from deepgram import DeepgramClient
from deepgram.core.events import EventType

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("transcription-backend")

DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")
if not DEEPGRAM_API_KEY:
    raise RuntimeError("DEEPGRAM_API_KEY environment variable is not set")
logger.info(f"✓ API key loaded: {DEEPGRAM_API_KEY[:5]}... (length {len(DEEPGRAM_API_KEY)})")

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health():
    return {"status": "ok", "deepgram_configured": bool(DEEPGRAM_API_KEY)}


def extract_transcript(message) -> str:
    """Safely extract transcript from Deepgram v1 message object."""
    try:
        if hasattr(message, "channel"):
            channel = message.channel
            if hasattr(channel, "alternatives"):
                alt = channel.alternatives
                if alt and len(alt) > 0:
                    return alt[0].transcript or ""
            elif isinstance(channel, list) and len(channel) > 0:
                for ch in channel:
                    if hasattr(ch, "alternatives"):
                        alt = ch.alternatives
                        if alt and len(alt) > 0 and alt[0].transcript:
                            return alt[0].transcript
        elif hasattr(message, "results"):
            results = message.results
            if hasattr(results, "channels"):
                channels = results.channels
                if channels and len(channels) > 0:
                    alt = channels[0].alternatives
                    if alt and len(alt) > 0:
                        return alt[0].transcript or ""
        elif hasattr(message, "transcript"):
            return message.transcript
        return None
    except Exception as e:
        logger.error(f"Error extracting transcript: {e}")
        return None


@app.websocket("/ws/transcribe")
async def websocket_transcribe(websocket: WebSocket):
    await websocket.accept()
    logger.info(f"Frontend connected: {websocket.client}")

    client = DeepgramClient(api_key=DEEPGRAM_API_KEY)
    frontend_closed = False
    main_loop = asyncio.get_running_loop()
    stop_keepalive = threading.Event()

    async def send_json_safe(payload: dict):
        nonlocal frontend_closed
        if frontend_closed:
            return
        try:
            await websocket.send_json(payload)
        except Exception:
            frontend_closed = True

    def on_open(_):
        logger.info("✅ Deepgram connection opened")
        asyncio.run_coroutine_threadsafe(
            send_json_safe({"type": "deepgram_ready"}),
            main_loop
        )

    def on_message(message):
        try:
            transcript = extract_transcript(message)
            if transcript:
                is_final = getattr(message, "is_final", False)
                speech_final = getattr(message, "speech_final", False)
                logger.info(f"📝 Transcript: '{transcript}' (final: {is_final})")
                asyncio.run_coroutine_threadsafe(
                    send_json_safe({
                        "type": "transcript",
                        "text": transcript,
                        "final": is_final,
                        "speech_final": speech_final,
                    }),
                    main_loop
                )
            else:
                logger.debug(f"No transcript in message: {type(message)}")
        except Exception as e:
            logger.error(f"Transcript handler error: {e}", exc_info=True)

    def on_error(error):
        logger.error(f"❌ Deepgram error: {error}")
        asyncio.run_coroutine_threadsafe(
            send_json_safe({"type": "error", "message": str(error)}),
            main_loop
        )

    def on_close(_):
        logger.info("Deepgram connection closed")

    # Deepgram connection – your original parameters (no changes)
    with client.listen.v1.connect(
        model="nova-3",
        language="en-US",
        interim_results=True,
        punctuate=True,
        smart_format=True,
        endpointing=300,          # unchanged
        vad_events=True,
    ) as connection:
        connection.on(EventType.OPEN, on_open)
        connection.on(EventType.MESSAGE, on_message)
        connection.on(EventType.ERROR, on_error)
        connection.on(EventType.CLOSE, on_close)

        # ---- IMPROVED KEEPALIVE ----
        # Deepgram recommends sending a newline character every few seconds
        # to prevent the connection from timing out.
        def keepalive_loop():
            while not stop_keepalive.is_set():
                if stop_keepalive.wait(5):
                    return
                try:
                    # Send a simple newline – this keeps the connection alive
                    # without affecting the audio stream.
                    connection.send("\n")
                    logger.debug("Sent keep‑alive (newline) to Deepgram")
                except Exception as e:
                    logger.warning(f"Keep‑alive failed: {e}")
                    return

        keepalive_thread = threading.Thread(target=keepalive_loop, daemon=True)
        keepalive_thread.start()

        # Start Deepgram listening thread
        listener_thread = threading.Thread(target=connection.start_listening, daemon=True)
        listener_thread.start()

        try:
            while True:
                msg = await websocket.receive()
                if "bytes" in msg:
                    audio_bytes = msg["bytes"]
                    logger.info(f"🎤 Received binary chunk: {len(audio_bytes)} bytes")
                    connection.send_media(audio_bytes)
                elif "text" in msg:
                    try:
                        payload = json.loads(msg["text"])
                        if payload.get("type") == "stop":
                            logger.info("Stop command received")
                            break
                        else:
                            logger.warning(f"Unknown text message: {payload}")
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON: {e}")
                else:
                    logger.warning(f"Unexpected message: {msg.keys()}")
        except WebSocketDisconnect:
            logger.info("Frontend WebSocket disconnected")
            frontend_closed = True
        except Exception as e:
            logger.exception("Backend error")
            await send_json_safe({"type": "error", "message": str(e)})
        finally:
            stop_keepalive.set()
            try:
                connection.send_finalize()
                connection.send_close_stream()
                logger.info("Sent finalize and close to Deepgram")
            except Exception as e:
                logger.warning(f"Failed to close Deepgram stream gracefully: {e}")

    logger.info("Deepgram connection closed (context manager exited)")