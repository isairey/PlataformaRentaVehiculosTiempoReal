from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from websocket.connection_manager import ConnectionManager

router = APIRouter()
manager = ConnectionManager()

@router.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    await manager.connect(websocket, user_id)
    try:
        while True:
            # Wait for messages from the client
            data = await websocket.receive_text()
            print(f"Received message: {data}")
    except WebSocketDisconnect:
        await manager.disconnect(websocket, user_id) 