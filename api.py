# api.py
import logging
import asyncio
from typing import Dict, List, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import uuid
from agent import FederalRegistryAgent

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("api")

app = FastAPI(
    title="Federal Registry Assistant API",
    description="API for querying and retrieving information from the Federal Registry",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For production, specify your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
    
    async def connect(self, websocket: WebSocket, client_id: str):
        await websocket.accept()
        self.active_connections[client_id] = websocket
    
    def disconnect(self, client_id: str):
        if client_id in self.active_connections:
            del self.active_connections[client_id]
    
    async def send_message(self, message: str, client_id: str):
        if client_id in self.active_connections:
            await self.active_connections[client_id].send_text(message)

manager = ConnectionManager()

# Input/Output models
class QueryRequest(BaseModel):
    query: str
    history: Optional[List[Dict[str, str]]] = None

class QueryResponse(BaseModel):
    answer: str
    history: List[Dict[str, str]]

# Initialize the agent
agent = FederalRegistryAgent()

# Add startup event
@app.on_event("startup")
async def startup_event():
    """Initialize the agent and load data on startup"""
    try:
        await agent.initialize_data()
    except Exception as e:
        logger.error(f"Error during startup: {str(e)}")

# Root endpoint
@app.get("/")
async def root():
    """Root endpoint that returns API information"""
    return {
        "name": "Federal Registry Assistant API",
        "version": "1.0.0",
        "endpoints": {
            "GET /": "API information",
            "POST /query": "Process a query",
            "GET /health": "Health check",
            "WS /ws/{client_id}": "WebSocket endpoint for real-time chat"
        }
    }

# HTTP endpoints
@app.post("/query", response_model=QueryResponse)
async def process_query(request: QueryRequest):
    """Process a query via HTTP POST"""
    try:
        result = await agent.process_query(request.query, request.history)
        return result
    except Exception as e:
        logger.error(f"Error processing query: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "ok"}

# WebSocket endpoint
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    """WebSocket endpoint for real-time chat"""
    await manager.connect(websocket, client_id)
    try:
        history = []
        while True:
            # Receive message from client
            data = await websocket.receive_text()
            
            # Process the query
            try:
                result = await agent.process_query(data, history)
                history = result["history"]
                
                # Send result back to client
                await websocket.send_json(result)
            except Exception as e:
                logger.error(f"Error processing websocket query: {str(e)}")
                await websocket.send_json({"error": str(e)})
    except WebSocketDisconnect:
        manager.disconnect(client_id)

# Add this new endpoint to serve the HTML file
@app.get("/ui")
async def get_ui():
    """Serve the UI HTML file"""
    return FileResponse("static/index.html")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)