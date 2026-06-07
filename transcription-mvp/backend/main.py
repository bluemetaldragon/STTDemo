import asyncio
import json
import logging
import os
from contextlib import suppress

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


@app.websocket("/ws/transcribe")
async def websocket_transcribe(websocket: WebSocket):
    await websocket.accept()
    logger.info(f"Frontend connected: {websocket.client}")

    client = DeepgramClient()
    frontend_closed = False

    async def send_json_safe(payload: dict):
        nonlocal frontend_closed
        if frontend_closed:
            return
        try:
            await websocket.send_json(payload)
        except Exception:
            frontend_closed = True

    try:
        # Using the documented context manager pattern for WebSocket connections
        # Reference: https://deepwiki.com/deepgram/deepgram-python-sdk/3.2-websocket-api-for-tts
        with client.listen.v2.connect(
            model="nova-3",
            encoding="linear16",
            sample_rate=16000,
            language="en-US",
            interim_results=True,
            punctuate=True,
            smart_format=True,
            endpointing=300,
            vad_events=True,
        ) as connection:
            # Register event callbacks as documented
            # Reference: https://github.com/deepgram/deepgram-python-sdk
            def on_open(open_event):
                logger.info("Deepgram connection opened")
                asyncio.create_task(
                    send_json_safe({"type": "deepgram_ready"})
                )

            def on_message(message):
                try:
                    # Extract transcript from the message object
                    # Reference: Listen examples show accessing results via message
                    if hasattr(message, "channel"):
                        alternatives = message.channel.alternatives
                        if alternatives and alternatives[0].transcript:
                            transcript_text = alternatives[0].transcript
                            asyncio.create_task(
                                send_json_safe(
                                    {
                                        "type": "transcript",
                                        "text": transcript_text,
                                        "final": getattr(message, "is_final", False),
                                        "speech_final": getattr(message, "speech_final", False),
                                    }
                                )
                            )
                except Exception as e:
                    logger.error(f"Transcript handler error: {e}")

            def on_error(error):
                logger.error(f"Deepgram error: {error}")
                asyncio.create_task(
                    send_json_safe({"type": "error", "message": str(error)})
                )

            def on_close(close_event):
                logger.info("Deepgram connection closed")

            connection.on(EventType.OPEN, on_open)
            connection.on(EventType.MESSAGE, on_message)
            connection.on(EventType.ERROR, on_error)
            connection.on(EventType.CLOSE, on_close)

            # Start listening as documented
            # Reference: start_listening() initiates the receive loop
            connection.start_listening()

            # Main loop: receive binary audio or text control messages
            while True:
                message = await websocket.receive()

                if "bytes" in message and message["bytes"] is not None:
                    audio_chunk = message["bytes"]
                    if audio_chunk:
                        # Send audio via documented send_media method
                        # Reference: V1SocketClient send_media(bytes) method
                        connection.send_media(audio_chunk)

                elif "text" in message and message["text"] is not None:
                    raw_text = message["text"]
                    try:
                        payload = json.loads(raw_text)
                    except json.JSONDecodeError:
                        await send_json_safe(
                            {"type": "error", "message": "Invalid JSON text message"}
                        )
                        continue

                    if payload.get("type") == "stop":
                        logger.info("Stop command received, closing connection")
                        break

                elif message.get("type") == "websocket.disconnect":
                    break

    except WebSocketDisconnect:
        logger.info("Frontend WebSocket disconnected")
    except Exception as e:
        logger.exception("Backend error")
        await send_json_safe({"type": "error", "message": str(e)})