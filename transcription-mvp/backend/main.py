import os
import json
import base64
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

DEEPGRAM_WS_URL = (
    "wss://api.deepgram.com/v1/listen"
    "?model=nova-3"
    "&encoding=linear16"
    "&sample_rate=16000"
    "&channels=1"
    "&interim_results=true"
    "&vad_events=true"
    "&endpointing=300"
    "&smart_format=true"
    "&punctuate=true"
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

async def deepgram_keepalive(dg_ws):
    try:
        while True:
            await asyncio.sleep(8)
            await dg_ws.send(json.dumps({"type": "KeepAlive"}))
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.warning(f"KeepAlive stopped: {e}")

@app.get("/health")
async def health():
    return {"status": "ok", "deepgram_configured": bool(DEEPGRAM_API_KEY)}

@app.websocket("/ws/transcribe")
async def websocket_transcribe(websocket: WebSocket):
    await websocket.accept()
    logger.info(f"Frontend connected: {websocket.client}")

    dg_ws = None
    dg_listener_task = None
    dg_keepalive_task = None

    try:
        dg_ws = await websockets.connect(
            DEEPGRAM_WS_URL,
            additional_headers={
                "Authorization": f"Token {DEEPGRAM_API_KEY}"
            }
        )
        logger.info("Connected to Deepgram")

        async def read_deepgram():
            try:
                async for message in dg_ws:
                    if isinstance(message, bytes):
                        continue

                    data = json.loads(message)
                    logger.info(f"Deepgram event: {json.dumps(data)[:500]}")

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
                                    "speech_final": data.get("speech_final", False),
                                },
                            )

                    elif msg_type == "Metadata":
                        await send_frontend(
                            websocket,
                            {
                                "type": "metadata",
                                "request_id": data.get("request_id"),
                                "duration": data.get("duration"),
                            },
                        )

                    elif msg_type == "UtteranceEnd":
                        await send_frontend(websocket, {"type": "utterance_end"})

                    elif msg_type == "SpeechStarted":
                        await send_frontend(websocket, {"type": "speech_started"})

                    elif msg_type == "Error":
                        await send_frontend(
                            websocket,
                            {"type": "error", "message": data}
                        )

            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.exception(f"Error reading Deepgram messages: {e}")
                await send_frontend(websocket, {"type": "error", "message": str(e)})

        dg_listener_task = asyncio.create_task(read_deepgram())
        dg_keepalive_task = asyncio.create_task(deepgram_keepalive(dg_ws))

        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)

            if msg.get("type") == "audio":
                audio_bytes = base64.b64decode(msg["data"])
                await dg_ws.send(audio_bytes)

            elif msg.get("type") == "stop":
                await dg_ws.send(json.dumps({"type": "CloseStream"}))
                break

    except WebSocketDisconnect:
        logger.info("Frontend disconnected")

    except Exception as e:
        logger.exception(f"Unexpected server error: {e}")
        try:
            await send_frontend(websocket, {"type": "error", "message": str(e)})
        except Exception:
            pass

    finally:
        if dg_keepalive_task:
            dg_keepalive_task.cancel()

        if dg_listener_task:
            dg_listener_task.cancel()

        if dg_ws:
            try:
                await dg_ws.close()
            except Exception:
                pass

        try:
            await websocket.close()
        except Exception:
            pass

        logger.info("Connections closed")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)