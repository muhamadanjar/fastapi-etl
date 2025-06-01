"""
WebSocket messaging implementation for real-time client communication.
"""

import asyncio
import logging
import json
from typing import Any, Optional, Dict, List, Set
from datetime import datetime
from dataclasses import dataclass

from fastapi import WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from .base import (
    Message,
    MessageInterface,
    MessageHandler,
    MessageFilter,
    MessageStatus,
)

logger = logging.getLogger(__name__)


@dataclass
class WebSocketConnection:
    """WebSocket connection wrapper."""
    websocket: WebSocket
    client_id: str
    connected_at: datetime
    subscriptions: Set[str]
    metadata: Dict[str, Any]
    
    async def send_message(self, message: Message) -> bool:
        """Send message to WebSocket client."""
        try:
            if self.websocket.application_state == WebSocketState.CONNECTED:
                await self.websocket.send_text(json.dumps(message.to_dict(), default=str))
                return True
        except Exception as e:
            logger.error(f"Failed to send message to client {self.client_id}: {e}")
        return False
    
    async def close(self, code: int = 1000) -> None:
        """Close WebSocket connection."""
        try:
            if self.websocket.application_state == WebSocketState.CONNECTED:
                await self.websocket.close(code)
        except Exception as e:
            logger.error(f"Error closing WebSocket for client {self.client_id}: {e}")


class WebSocketMessaging(MessageInterface):
    """
    WebSocket messaging for real-time client communication.
    
    Features:
    - Real-time bidirectional communication
    - Topic-based subscriptions
    - Connection management
    - Broadcast messaging
    - Room/group support
    """
    
    def __init__(self):
        self._connections: Dict[str, WebSocketConnection] = {}
        self._topic_subscriptions: Dict[str, Set[str]] = {}  # topic -> client_ids
        self._rooms: Dict[str, Set[str]] = {}  # room -> client_ids
        self._handlers: Dict[str, MessageHandler] = {}
        self._is_consuming = False