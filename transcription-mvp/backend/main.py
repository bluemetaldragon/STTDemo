import os
import json
import asyncio
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import websockets

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")
if not DEEPGRAM_API_KEY:
    raise RuntimeError("DEEPGRAM_API_KEY not set")

# Note: We are requesting linear16 (raw PCM)
DEEPGRAM_WS_URL = (
    "wss://api.deepgram.com/v1/listen"
    "?model=nova-2"
    "&encoding=linear16"
    "&sample_rate=16000"
    "&channels=1"
    "&interim_results=true"
    "&vad_events=true"
    "&endpointing=300"
    "&smart_format=true"
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def send_frontend(websocket: WebSocket, payload: dict):
    try:
        await websocket.send_json(payload)
    except Exception as e:
        logger.warning(f"Failed sending to frontend: {e}")

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.websocket("/ws/transcribe")
async def websocket_transcribe(websocket: WebSocket):
    await websocket.accept()
    logger.info(f"Frontend connected: {websocket.client}")

    dg_ws = None
    dg_listener_task = None

    try:
        # Connect to Deepgram
        dg_ws = await websockets.connect(
            DEEPGRAM_WS_URL,
            extra_headers={"Authorization": f"Token {DEEPGRAM_API_KEY}"}
        )
        logger.info("Connected to Deepgram")

        async def read_deepgram():
            try:
                async for message in dg_ws:
                    # Deepgram sends JSON metadata/results
                    data = json.loads(message)
                    msg_type = data.get("type")

                    if msg_type == "Results":
                        channel = data.get("channel", {})
                        alternatives = channel.get("alternatives", [])
                        transcript = alternatives[0].get("transcript", "") if alternatives else ""

                        if transcript:
                            await send_frontend(
                                websocket,
                                {
                                    "type": "transcript",
                                    "text": transcript,
                                    "final": data.get("is_final", False),
                                },
                            )
                    elif msg_type == "UtteranceEnd":
                        await send_frontend(websocket, {"type": "utterance_end"})
            except Exception as e:
                logger.error(f"Deepgram read error: {e}")

        dg_listener_task = asyncio.create_task(read_deepgram())

        # Main Loop: Handle mixed Text and Binary
        while True:
            # receive() handles both text and bytes messages
            message = await websocket.receive()

            if "bytes" in message:
                # This is our raw PCM audio
                await dg_ws.send(message["bytes"])

            elif "text" in message:
                # This is a control command (like 'stop')
                data = json.loads(message["text"])
                if data.get("type") == "stop":
                    logger.info("Stop command received")
                    break

    except WebSocketDisconnect:
        logger.info("Frontend disconnected")
    except Exception as e:
        logger.exception(f"Server error: {e}")
    finally:
        if dg_listener_task:
            dg_listener_task.cancel()
        if dg_ws:
            await dg_ws.close()
        try:
            await websocket.close()
        except:
            pass
        logger.info("Cleanup complete")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)