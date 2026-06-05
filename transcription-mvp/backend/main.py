import json
import base64
import logging
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

# WebSocket endpoint
@app.websocket("/ws/transcribe")
async def websocket_transcribe(websocket: WebSocket):
    logger.info(f"🔌 WebSocket connection from {websocket.client}")
    
    try:
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
                    transcript_text = msg.transcript.channel.alternatives[0].transcript
                    is_final = msg.transcript.is_final
                    
                    if transcript_text:
                        logger.info(f"📝 Transcript: {transcript_text} (final: {is_final})")
                        
                        # Send to client
                        import asyncio
                        asyncio.create_task(websocket.send_json({
                            "type": "transcript",
                            "text": transcript_text,
                            "final": is_final
                        }))
            except Exception as e:
                logger.error(f"Error in on_message: {e}")
        
        def on_error(self, error, **kwargs):
            logger.error(f"❌ Deepgram error: {error}")
            import asyncio
            asyncio.create_task(websocket.send_json({
                "type": "error",
                "message": str(error)
            }))
        
        dg_connection.on(LiveTranscriptionEvents.Open, on_open)
        dg_connection.on(LiveTranscriptionEvents.Transcript, on_message)
        dg_connection.on(LiveTranscriptionEvents.Error, on_error)
        
        logger.info("Starting Deepgram listen...")
        dg_connection.start({
            "model": DEEPGRAM_MODEL,
            "language": DEEPGRAM_LANGUAGE,
            "encoding": "linear16",
            "sample_rate": 16000,
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
                    dg_connection.send(audio_data)
                    
                elif msg.get("type") == "stop":
                    logger.info("Stop signal received")
                    break
                    
        except WebSocketDisconnect:
            logger.info("WebSocket disconnected")
        
    except Exception as e:
        logger.error(f"❌ WebSocket error: {e}", exc_info=True)
        try:
            await websocket.send_json({
                "type": "error",
                "message": f"Error: {str(e)}"
            })
        except:
            pass
    
    finally:
        logger.info("WebSocket closed")
        try:
            await websocket.close()
        except:
            pass

if __name__ == "__main__":
    logger.info("🚀 Starting server...")
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)