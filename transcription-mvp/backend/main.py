import os
import json
import base64
import asyncio
import logging

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from deepgram import (
    DeepgramClient,
    LiveTranscriptionEvents,
    LiveOptions,
    LiveResultResponse
)

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")
if not DEEPGRAM_API_KEY:
    raise RuntimeError("DEEPGRAM_API_KEY not set")

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

    # Initialize Deepgram client
    deepgram = DeepgramClient(DEEPGRAM_API_KEY)
    dg_connection = deepgram.listen.websocket.v("1")

    # Event handlers
    async def on_open(self, open, **kwargs):
        logger.info("Deepgram connection open")
        await websocket.send_json({"type": "deepgram_ready"})

    async def on_message(self, result: LiveResultResponse, **kwargs):
        try:
            # Extract transcript data
            channel = result.channel
            alternatives = channel.alternatives
            if not alternatives:
                return
            transcript = alternatives[0].transcript
            if not transcript:
                return

            # Send to frontend as JSON
            await websocket.send_json({
                "type": "transcript",
                "text": transcript,
                "final": result.is_final,
                "speech_final": result.speech_final,
            })
        except Exception as e:
            logger.error(f"Error processing transcript: {e}")

    async def on_error(self, error, **kwargs):
        logger.error(f"Deepgram error: {error}")
        await websocket.send_json({"type": "error", "message": str(error)})

    async def on_close(self, close, **kwargs):
        logger.info("Deepgram connection closed")

    # Register callbacks
    dg_connection.on(LiveTranscriptionEvents.Open, on_open)
    dg_connection.on(LiveTranscriptionEvents.Transcript, on_message)
    dg_connection.on(LiveTranscriptionEvents.Error, on_error)
    dg_connection.on(LiveTranscriptionEvents.Close, on_close)

    # Configure live options
    options = LiveOptions(
        model="nova-3",
        encoding="linear16",
        sample_rate=16000,
        channels=1,
        interim_results=True,
        vad_events=True,
        endpointing=300,
        smart_format=True,
        punctuate=True,
    )

    try:
        dg_connection.start(options)
        logger.info("Deepgram connection started")
    except Exception as e:
        logger.error(f"Failed to start Deepgram: {e}")
        await websocket.close(code=1011, reason="Deepgram connection failed")
        return

    # Main loop: receive binary audio or text control messages
    try:
        while True:
            msg = await websocket.receive()
            msg_type = msg.get("type")

            if msg_type == "websocket.receive":
                # Binary audio data
                if "bytes" in msg:
                    audio_bytes = msg["bytes"]
                    dg_connection.send(audio_bytes)

                # Text control message (like "stop")
                elif "text" in msg:
                    data = json.loads(msg["text"])
                    if data.get("type") == "stop":
                        logger.info("Received stop signal, closing Deepgram stream")
                        dg_connection.finish()
                        break

            elif msg_type == "websocket.disconnect":
                logger.info("Frontend disconnected")
                break

    except WebSocketDisconnect:
        logger.info("Frontend disconnected")
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        try:
            await websocket.send_json({"type": "error", "message": str(e)})
        except:
            pass
    finally:
        dg_connection.finish()
        try:
            await websocket.close()
        except:
            pass
        logger.info("Connections closed")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)