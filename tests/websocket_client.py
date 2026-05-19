import asyncio
import websockets
import json

async def test_websocket():
    uri = "ws://localhost:8000/ws/USR2"
    async with websockets.connect(uri) as websocket:
        print("Connected to WebSocket")
        
        try:
            while True:
                message = await websocket.recv()
                data = json.loads(message)
                print(f"Received message: {json.dumps(data, indent=2)}")
        except websockets.ConnectionClosed:
            print("Connection closed")

if __name__ == "__main__":
    asyncio.run(test_websocket()) 