import json
import base64
import logging
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os
import websockets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")

if not DEEPGRAM_API_KEY:
    logger.error("❌ DEEPGRAM_API_KEY not set!")
    exit(1)

# Deepgram WebSocket URL with API key as a query parameter
DEEPGRAM_URL = (
    f"wss://api.deepgram.com/v1/listen?"
    f"api_key={DEEPGRAM_API_KEY}"
    f"&encoding=linear16&sample_rate=16000&channels=1"
    f"&interim_results=true&vad=true&model=nova-3"
)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
logger.info("✓ FastAPI app initialized")

async def safe_send_json(websocket: WebSocket, data: dict):
    try:
        await websocket.send_json(data)
    except Exception as e:
        logger.error(f"Failed to send JSON to frontend: {e}")

@app.websocket("/ws/transcribe")
async def websocket_transcribe(websocket: WebSocket):
    logger.info(f"🔌 New WebSocket connection from {websocket.client}")
    await websocket.accept()

    dg_ws = None
    deepgram_listener_task = None

    try:
        logger.info("⏳ Connecting to Deepgram...")
        dg_ws = await websockets.connect(DEEPGRAM_URL)
        logger.info("✅ Connected to Deepgram")

        async def forward_transcripts():
            try:
                async for message in dg_ws:
                    if isinstance(message, str):
                        try:
                            data = json.loads(message)
                            logger.info(f"📨 Deepgram message: {json.dumps(data)[:200]}")
                            if data.get("type") == "Error":
                                logger.error(f"❌ Deepgram error: {data.get('description', 'No description')}")
                                await safe_send_json(websocket, {"type": "error", "message": data})
                                continue
                            transcript = data.get("channel", {}).get("alternatives", [{}])[0].get("transcript")
                            if transcript:
                                is_final = data.get("is_final", False)
                                logger.info(f"📝 Transcript: '{transcript}' (final: {is_final})")
                                await safe_send_json(websocket, {"type": "transcript", "text": transcript, "final": is_final})
                        except json.JSONDecodeError:
                            logger.warning(f"Non-JSON message from Deepgram: {message[:200]}")
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"Deepgram WebSocket closed: code={e.code}, reason={e.reason}")
            except Exception as e:
                logger.error(f"Error in forward_transcripts: {e}", exc_info=True)

        deepgram_listener_task = asyncio.create_task(forward_transcripts())

        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            if msg.get("type") == "audio":
                audio_bytes = base64.b64decode(msg["data"])
                # Log first few sample values to detect silence
                sample_count = min(len(audio_bytes) // 2, 10)
                samples = []
                for i in range(sample_count):
                    sample = int.from_bytes(audio_bytes[i*2:(i+1)*2], 'little', signed=True)
                    samples.append(sample)
                logger.info(f"🎤 Received {len(audio_bytes)} bytes, first {sample_count} samples: {samples}")
                await dg_ws.send(audio_bytes)
                logger.info(f"✅ Sent audio chunk to Deepgram ({len(audio_bytes)} bytes)")
            elif msg.get("type") == "stop":
                logger.info("🛑 Stop signal received from frontend")
                break

    except WebSocketDisconnect:
        logger.info("⚠️ Frontend WebSocket disconnected")
    except Exception as e:
        logger.error(f"🔥 Unexpected error: {e}", exc_info=True)
    finally:
        if deepgram_listener_task:
            deepgram_listener_task.cancel()
        if dg_ws:
            await dg_ws.close()
            logger.info("🔒 Closed Deepgram WebSocket")
        await websocket.close()
        logger.info("🔒 Closed frontend WebSocket")

@app.get("/health")
async def health():
    return {"status": "ok", "deepgram_configured": bool(DEEPGRAM_API_KEY)}

if __name__ == "__main__":
    logger.info("🚀 Starting server...")
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)