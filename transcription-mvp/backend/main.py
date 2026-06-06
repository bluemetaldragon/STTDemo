import json
import base64
import logging
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os
from deepgram import DeepgramClient, LiveTranscriptionEvents

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment
load_dotenv()
DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")
DEEPGRAM_MODEL = os.getenv("DEEPGRAM_MODEL", "nova-3")
DEEPGRAM_LANGUAGE = os.getenv("DEEPGRAM_LANGUAGE", "en")

if not DEEPGRAM_API_KEY:
    logger.error("❌ DEEPGRAM_API_KEY not set!")

# Create app
app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger.info("✓ FastAPI app initialized")

# Initialize Deepgram
try:
    deepgram_client = DeepgramClient(api_key=DEEPGRAM_API_KEY)
    logger.info("✓ Deepgram client initialized")
except Exception as e:
    logger.error(f"❌ Failed to initialize Deepgram: {e}")
    deepgram_client = None

# Health check
@app.get("/health")
async def health():
    logger.info("Health check called")
    return {
        "status": "ok",
        "deepgram_configured": deepgram_client is not None
    }

# Helper to safely send JSON from any thread
async def safe_send_json(websocket: WebSocket, data: dict):
    try:
        await websocket.send_json(data)
    except Exception as e:
        logger.error(f"Failed to send JSON: {e}")

# WebSocket endpoint
@app.websocket("/ws/transcribe")
async def websocket_transcribe(websocket: WebSocket):
    logger.info(f"🔌 WebSocket connection from {websocket.client}")
    
    await websocket.accept()
    logger.info("✓ WebSocket accepted")
    
    if not deepgram_client:
        logger.error("❌ Deepgram not configured")
        await websocket.send_json({
            "type": "error",
            "message": "Deepgram not configured"
        })
        await websocket.close()
        return
    
    # Capture the main event loop for thread-safe callbacks
    main_loop = asyncio.get_running_loop()
    
    # Create Deepgram connection
    logger.info("Creating Deepgram live connection...")
    dg_connection = deepgram_client.listen.live.v("1")
    logger.info("✓ Deepgram connection created")
    
    # Setup handlers
    def on_open(self, open, **kwargs):
        logger.info("✓ Deepgram connection opened")
    
    def on_message(self, msg, **kwargs):
        try:
            if msg.type == LiveTranscriptionEvents.Transcript:
                transcript_text = msg.channel.alternatives[0].transcript
                is_final = msg.is_final
                if transcript_text:
                    logger.info(f"📝 Transcript: {transcript_text} (final: {is_final})")
                    # Schedule coroutine in the main event loop
                    asyncio.run_coroutine_threadsafe(
                        safe_send_json(websocket, {
                            "type": "transcript",
                            "text": transcript_text,
                            "final": is_final
                        }),
                        main_loop
                    )
        except Exception as e:
            logger.error(f"Error in on_message: {e}")
    
    def on_error(self, error, **kwargs):
        logger.error(f"❌ Deepgram error: {error}")
        asyncio.run_coroutine_threadsafe(
            safe_send_json(websocket, {
                "type": "error",
                "message": str(error)
            }),
            main_loop
        )
    
    dg_connection.on(LiveTranscriptionEvents.Open, on_open)
    dg_connection.on(LiveTranscriptionEvents.Transcript, on_message)
    dg_connection.on(LiveTranscriptionEvents.Error, on_error)
    
    logger.info("Starting Deepgram listen...")
    dg_connection.start({
        "model": DEEPGRAM_MODEL,
        "language": DEEPGRAM_LANGUAGE,
        "encoding": "linear16",
        "sample_rate": 16000,
        "channels": 1,
        "interim_results": True,
        "vad": True
    })
    logger.info("✓ Deepgram listening")
    
    # Receive audio from client
    try:
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            
            if msg.get("type") == "audio":
                audio_data = base64.b64decode(msg.get("data", ""))
                logger.info(f"🎤 Received audio chunk: {len(audio_data)} bytes")
                dg_connection.send(audio_data)
            elif msg.get("type") == "stop":
                logger.info("Stop signal received")
                break
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        logger.info("Cleaning up Deepgram connection...")
        dg_connection.finish()
        await websocket.close()
        logger.info("WebSocket closed")

if __name__ == "__main__":
    logger.info("🚀 Starting server...")
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)