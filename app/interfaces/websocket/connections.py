"""
WebSocket connection manager for handling multiple client connections.
"""

import json
import logging
from typing import Dict, List, Optional, Any
from fastapi import WebSocket, WebSocketDisconnect
from datetime import datetime

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages WebSocket connections and message broadcasting."""
    
    def __init__(self):
        # Store active connections by connection_id
        self.active_connections: Dict[str, WebSocket] = {}
        # Store user metadata for each connection
        self.connection_metadata: Dict[str, Dict[str, Any]] = {}
        # Store connections by room/group
        self.rooms: Dict[str, List[str]] = {}
        
    async def connect(
        self, 
        websocket: WebSocket, 
        connection_id: str,
        user_id: Optional[str] = None,
        room: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Accept WebSocket connection and store it."""
        try:
            await websocket.accept()
            self.active_connections[connection_id] = websocket
            
            # Store connection metadata
            self.connection_metadata[connection_id] = {
                "user_id": user_id,
                "room": room,
                "connected_at": datetime.utcnow(),
                "metadata": metadata or {}
            }
            
            # Add to room if specified
            if room:
                if room not in self.rooms:
                    self.rooms[room] = []
                self.rooms[room].append(connection_id)
            
            logger.info(f"WebSocket connected: {connection_id} (user: {user_id}, room: {room})")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting WebSocket {connection_id}: {e}")
            return False
    
    def disconnect(self, connection_id: str):
        """Remove WebSocket connection."""
        if connection_id in self.active_connections:
            # Remove from rooms
            metadata = self.connection_metadata.get(connection_id, {})
            room = metadata.get("room")
            if room and room in self.rooms:
                if connection_id in self.rooms[room]:
                    self.rooms[room].remove(connection_id)
                if not self.rooms[room]:  # Remove empty room
                    del self.rooms[room]
            
            # Remove connection and metadata
            del self.active_connections[connection_id]
            del self.connection_metadata[connection_id]
            
            logger.info(f"WebSocket disconnected: {connection_id}")
    
    async def send_personal_message(
        self, 
        message: str, 
        connection_id: str
    ) -> bool:
        """Send message to specific connection."""
        if connection_id in self.active_connections:
            try:
                websocket = self.active_connections[connection_id]
                await websocket.send_text(message)
                return True
            except WebSocketDisconnect:
                self.disconnect(connection_id)
            except Exception as e:
                logger.error(f"Error sending message to {connection_id}: {e}")
                self.disconnect(connection_id)
        return False
    
    async def send_personal_json(
        self, 
        data: Dict[str, Any], 
        connection_id: str
    ) -> bool:
        """Send JSON data to specific connection."""
        try:
            message = json.dumps(data, ensure_ascii=False)
            return await self.send_personal_message(message, connection_id)
        except Exception as e:
            logger.error(f"Error serializing JSON for {connection_id}: {e}")
            return False
    
    async def broadcast_to_room(
        self, 
        message: str, 
        room: str,
        exclude_connection: Optional[str] = None
    ):
        """Broadcast message to all connections in a room."""
        if room in self.rooms:
            disconnected_connections = []
            
            for connection_id in self.rooms[room]:
                if connection_id == exclude_connection:
                    continue
                    
                if connection_id in self.active_connections:
                    try:
                        websocket = self.active_connections[connection_id]
                        await websocket.send_text(message)
                    except WebSocketDisconnect:
                        disconnected_connections.append(connection_id)
                    except Exception as e:
                        logger.error(f"Error broadcasting to {connection_id}: {e}")
                        disconnected_connections.append(connection_id)
            
            # Clean up disconnected connections
            for connection_id in disconnected_connections:
                self.disconnect(connection_id)
    
    async def broadcast_json_to_room(
        self, 
        data: Dict[str, Any], 
        room: str,
        exclude_connection: Optional[str] = None
    ):
        """Broadcast JSON data to all connections in a room."""
        try:
            message = json.dumps(data, ensure_ascii=False)
            await self.broadcast_to_room(message, room, exclude_connection)
        except Exception as e:
            logger.error(f"Error serializing JSON for room {room}: {e}")
    
    async def broadcast_to_all(
        self, 
        message: str,
        exclude_connection: Optional[str] = None
    ):
        """Broadcast message to all active connections."""
        disconnected_connections = []
        
        for connection_id, websocket in self.active_connections.items():
            if connection_id == exclude_connection:
                continue
                
            try:
                await websocket.send_text(message)
            except WebSocketDisconnect:
                disconnected_connections.append(connection_id)
            except Exception as e:
                logger.error(f"Error broadcasting to {connection_id}: {e}")
                disconnected_connections.append(connection_id)
        
        # Clean up disconnected connections
        for connection_id in disconnected_connections:
            self.disconnect(connection_id)
    
    async def broadcast_json_to_all(
        self, 
        data: Dict[str, Any],
        exclude_connection: Optional[str] = None
    ):
        """Broadcast JSON data to all active connections."""
        try:
            message = json.dumps(data, ensure_ascii=False)
            await self.broadcast_to_all(message, exclude_connection)
        except Exception as e:
            logger.error(f"Error serializing JSON for broadcast: {e}")
    
    def get_connection_count(self) -> int:
        """Get total number of active connections."""
        return len(self.active_connections)
    
    def get_room_count(self, room: str) -> int:
        """Get number of connections in a room."""
        return len(self.rooms.get(room, []))
    
    def get_user_connections(self, user_id: str) -> List[str]:
        """Get all connection IDs for a specific user."""
        return [
            conn_id for conn_id, metadata in self.connection_metadata.items()
            if metadata.get("user_id") == user_id
        ]
    
    def get_connection_info(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """Get connection metadata."""
        return self.connection_metadata.get(connection_id)