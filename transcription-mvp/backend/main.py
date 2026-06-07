import asyncio
import json
import logging
import os
from contextlib import suppress
from typing import Optional

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
    raise RuntimeError("DEEPGRAM_API_KEY is not set")

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

    deepgram: Optional[DeepgramClient] = None
    dg_connection = None
    closed = False
    frontend_closed = False

    async def send_json_safe(payload: dict):
        nonlocal frontend_closed
        if frontend_closed:
            return
        try:
            await websocket.send_json(payload)
        except Exception:
            frontend_closed = True

    async def close_everything():
        nonlocal closed, frontend_closed, dg_connection
        if closed:
            return
        closed = True

        if dg_connection is not None:
            with suppress(Exception):
                finish_result = dg_connection.finish()
                if asyncio.iscoroutine(finish_result):
                    await finish_result

        if not frontend_closed:
            with suppress(Exception):
                await websocket.close()
            frontend_closed = True

    try:
        deepgram = DeepgramClient(DEEPGRAM_API_KEY)
        dg_connection = deepgram.listen.websocket.v("1")

        async def handle_open(*args, **kwargs):
            logger.info("Deepgram connection opened")
            await send_json_safe({"type": "deepgram_ready"})

        async def handle_transcript(result, *args, **kwargs):
            try:
                channel = getattr(result, "channel", None)
                alternatives = getattr(channel, "alternatives", None) if channel else None
                transcript = ""

                if alternatives and len(alternatives) > 0:
                    transcript = getattr(alternatives[0], "transcript", "") or ""

                if transcript:
                    await send_json_safe(
                        {
                            "type": "transcript",
                            "text": transcript,
                            "final": bool(getattr(result, "is_final", False)),
                            "speech_final": bool(getattr(result, "speech_final", False)),
                        }
                    )
            except Exception as e:
                logger.exception("Transcript handler failed")
                await send_json_safe({"type": "error", "message": f"Transcript handler error: {str(e)}"})

        async def handle_error(error, *args, **kwargs):
            logger.exception("Deepgram error: %s", error)
            await send_json_safe({"type": "error", "message": str(error)})

        async def handle_close(*args, **kwargs):
            logger.info("Deepgram connection closed")

        dg_connection.on(EventType.OPEN, handle_open)
        dg_connection.on(EventType.MESSAGE, handle_transcript)   # Note: MESSAGE, not TRANSCRIPT
        dg_connection.on(EventType.ERROR, handle_error)
        dg_connection.on(EventType.CLOSE, handle_close)

        options = {
            "model": "nova-3",
            "language": "en-US",
            "encoding": "linear16",
            "sample_rate": 16000,
            "channels": 1,
            "interim_results": True,
            "smart_format": True,
            "punctuate": True,
            "endpointing": 300,
        }

        start_result = dg_connection.start(options)
        if asyncio.iscoroutine(start_result):
            await start_result

        # Main loop: receive binary audio or text control messages
        while True:
            message = await websocket.receive()

            if "bytes" in message and message["bytes"] is not None:
                audio_chunk = message["bytes"]
                if audio_chunk:
                    send_result = dg_connection.send(audio_chunk)
                    if asyncio.iscoroutine(send_result):
                        await send_result

            elif "text" in message and message["text"] is not None:
                raw_text = message["text"]
                try:
                    payload = json.loads(raw_text)
                except json.JSONDecodeError:
                    await send_json_safe(
                        {"type": "error", "message": "Invalid JSON text message received from frontend"}
                    )
                    continue

                if payload.get("type") == "stop":
                    logger.info("Stop command received")
                    await close_everything()
                    break

            elif message.get("type") == "websocket.disconnect":
                break

    except WebSocketDisconnect:
        logger.info("Frontend WebSocket disconnected")
    except Exception as e:
        logger.exception("Backend error")
        await send_json_safe({"type": "error", "message": str(e)})
    finally:
        await close_everything()