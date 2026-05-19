from fastapi import WebSocket
from typing import Dict, List
from database import users_collection
from colorama import Fore, Style, init

init(autoreset=True)

class ConnectionManager:
    def __init__(self):
        # Store active connections by user_id
        self.active_connections: Dict[str, List[WebSocket]] = {}
    
    async def connect(self, websocket: WebSocket, user_id: str):
        await websocket.accept()
        if user_id not in self.active_connections:
            self.active_connections[user_id] = []
        self.active_connections[user_id].append(websocket)
    
    async def disconnect(self, websocket: WebSocket, user_id: str):
        if user_id in self.active_connections:
            self.active_connections[user_id].remove(websocket)
            if not self.active_connections[user_id]:
                del self.active_connections[user_id]
    
    async def send_personal_message(self, message: dict, user_id: str):
        print(f"{Fore.BLUE}[WebSocket] Sending personal message to {user_id}: {message['type']}")
        if user_id in self.active_connections:
            for connection in self.active_connections[user_id]:
                await connection.send_json(message)
    
    async def broadcast_to_employees(self, message: dict):
        print(f"{Fore.BLUE}[WebSocket] Broadcasting message to employees: {message['type']}")
        for user_id, connections in self.active_connections.items():  
            user = users_collection.find_one({"_id": user_id})
            if user and user['role'] == "EMPLOYEE":
                
                for connection in connections:  
                    await connection.send_json(message)

    async def broadcast_to_customers(self, message: dict):
        print(f"{Fore.BLUE}[WebSocket] Broadcasting message to customers: {message['type']}")
        for user_id, connections in self.active_connections.items():
            user = users_collection.find_one({"_id": user_id})
            if user and user['role'] == "CUSTOMER":
                for connection in connections:
                    await connection.send_json(message)