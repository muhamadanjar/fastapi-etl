"""
Messaging Manager - Orchestrates different messaging systems.
"""

import asyncio
import logging
from typing import Optional, Dict, Any, List
from enum import Enum

from .base import MessageInterface, Message
from .redis_messaging import RedisMessaging
from .websocket_messaging import WebSocketMessaging
from .rabbitmq_messaging import RabbitMQMessaging, rabbitmq_manager
from .sse_messaging import SSEMessaging

logger = logging.getLogger(__name__)


class MessagingType(Enum):
    """Messaging system types."""
    REDIS = "redis"
    WEBSOCKET = "websocket"
    SSE = "sse"
    ALL = "all"
    PERSISTENT = "persistent"  # Redis + RabbitMQ
    REALTIME = "realtime" 


class MessagingManager:
    """
    Unified messaging manager that orchestrates multiple messaging systems.
    
    Features:
    - Multiple messaging backends
    - Unified publishing interface
    - Cross-system message routing
    - Health monitoring
    - Graceful degradation
    """
    
    def __init__(self):
        self._redis_messaging: Optional[RedisMessaging] = None
        self._websocket_messaging: Optional[WebSocketMessaging] = None
        self._sse_messaging: Optional[SSEMessaging] = None
        self._rabbitmq_messaging: Optional[RabbitMQMessaging] = None
        self._is_initialized = False
    
    async def initialize(
        self,
        enable_redis: bool = True,
        enable_websocket: bool = True,
        enable_rabbitmq: bool = False,
        enable_sse: bool = True,
        redis_config: Optional[Dict[str, Any]] = None,
        rabbitmq_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Initialize messaging systems.
        
        Args:
            enable_redis: Whether to enable Redis messaging
            enable_websocket: Whether to enable WebSocket messaging
            enable_sse: Whether to enable SSE messaging
            redis_config: Redis configuration
        """
        try:
            # Initialize Redis messaging
            if enable_redis:
                try:
                    self._redis_messaging = RedisMessaging(**(redis_config or {}))
                    await self._redis_messaging.connect()
                    await self._redis_messaging.start_consuming()
                    logger.info("Redis messaging initialized")
                except Exception as e:
                    logger.error(f"Failed to initialize Redis messaging: {e}")
                    self._redis_messaging = None
            
            # Initialize WebSocket messaging
            if enable_websocket:
                try:
                    self._websocket_messaging = WebSocketMessaging()
                    await self._websocket_messaging.start_consuming()
                    logger.info("WebSocket messaging initialized")
                except Exception as e:
                    logger.error(f"Failed to initialize WebSocket messaging: {e}")
                    self._websocket_messaging = None
            
            # Initialize SSE messaging
            if enable_sse:
                try:
                    self._sse_messaging = SSEMessaging()
                    await self._sse_messaging.start_consuming()
                    logger.info("SSE messaging initialized")
                except Exception as e:
                    logger.error(f"Failed to initialize SSE messaging: {e}")
                    self._sse_messaging = None
            # Initialize RabbitMQ messaging        
            if enable_rabbitmq:
                try:
                    await rabbitmq_manager.connect(**(rabbitmq_config or {}))
                    self._rabbitmq_messaging = rabbitmq_manager.get_messaging()
                    if self._rabbitmq_messaging:
                        await self._rabbitmq_messaging.start_consuming()
                    logger.info("RabbitMQ messaging initialized")
                except Exception as e:
                    logger.error(f"Failed to initialize RabbitMQ messaging: {e}")
                    self._rabbitmq_messaging = None
            
            self._is_initialized = True
            logger.info("Messaging manager initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize messaging manager: {e}")
            raise
    
    async def shutdown(self) -> None:
        """Shutdown all messaging systems."""
        try:
            # Shutdown Redis messaging
            if self._redis_messaging:
                await self._redis_messaging.stop_consuming()
                await self._redis_messaging.disconnect()
                self._redis_messaging = None
            
            # Shutdown WebSocket messaging
            if self._websocket_messaging:
                await self._websocket_messaging.stop_consuming()
                self._websocket_messaging = None
            
            # Shutdown SSE messaging
            if self._sse_messaging:
                await self._sse_messaging.stop_consuming()
                self._sse_messaging = None

            # Shutdown RabbitMQ messaging
            if self._rabbitmq_messaging:
                await self._rabbitmq_messaging.stop_consuming()
                await rabbitmq_manager.disconnect()
                self._rabbitmq_messaging = None
            
            self._is_initialized = False
            logger.info("Messaging manager shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during messaging shutdown: {e}")
    
    async def publish(
        self,
        topic: str,
        payload: Any,
        messaging_type: MessagingType = MessagingType.ALL,
        headers: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, str]:
        """
        Publish message to specified messaging systems.
        
        Args:
            topic: Message topic
            payload: Message payload
            messaging_type: Which messaging systems to use
            headers: Optional message headers
            **kwargs: Additional message options
            
        Returns:
            Dictionary with message IDs from each system
        """
        results = {}
        
        message = Message(
            topic=topic,
            payload=payload,
            headers=headers or {},
            **kwargs
        )
        
        # Publish to Redis
        if (messaging_type in [MessagingType.REDIS, MessagingType.ALL] and 
            self._redis_messaging):
            try:
                await self._redis_messaging.publish_message(message)
                results["redis"] = message.id
            except Exception as e:
                logger.error(f"Failed to publish to Redis: {e}")
        
        # Publish to WebSocket
        if (messaging_type in [MessagingType.WEBSOCKET, MessagingType.ALL] and 
            self._websocket_messaging):
            try:
                await self._websocket_messaging.publish_message(message)
                results["websocket"] = message.id
            except Exception as e:
                logger.error(f"Failed to publish to WebSocket: {e}")
        
        # Publish to SSE
        if (messaging_type in [MessagingType.SSE, MessagingType.ALL] and 
            self._sse_messaging):
            try:
                await self._sse_messaging.publish_message(message)
                results["sse"] = message.id
            except Exception as e:
                logger.error(f"Failed to publish to SSE: {e}")
        
        return results
    
    async def publish_to_websocket_room(
        self,
        room: str,
        payload: Any,
        headers: Optional[Dict[str, Any]] = None,
        exclude_client: Optional[str] = None
    ) -> int:
        """Publish message to WebSocket room."""
        if not self._websocket_messaging:
            return 0
        
        return await self._websocket_messaging.broadcast_to_room(
            room=room,
            payload=payload,
            headers=headers,
            exclude_client=exclude_client
        )
    
    async def send_to_client(
        self,
        client_id: str,
        payload: Any,
        messaging_type: MessagingType = MessagingType.WEBSOCKET,
        headers: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Send message directly to a specific client."""
        if messaging_type == MessagingType.WEBSOCKET and self._websocket_messaging:
            return await self._websocket_messaging.send_to_client(
                client_id=client_id,
                payload=payload,
                headers=headers
            )
        elif messaging_type == MessagingType.SSE and self._sse_messaging:
            return await self._sse_messaging.send_to_client(
                client_id=client_id,
                event_type="direct_message",
                data=payload
            )
        
        return False
    
    def get_messaging(self, messaging_type: MessagingType) -> Optional[MessageInterface]:
        """Get specific messaging implementation."""
        if messaging_type == MessagingType.REDIS:
            return self._redis_messaging
        elif messaging_type == MessagingType.WEBSOCKET:
            return self._websocket_messaging
        elif messaging_type == MessagingType.SSE:
            return self._sse_messaging
        elif messaging_type == MessagingType.RABBITMQ:
            return self._rabbitmq_messaging
        
        return None
    
    async def health_check(self) -> Dict[str, Any]:
        """Check health of all messaging systems."""
        status = {
            "overall": "healthy",
            "systems": {}
        }
        
        unhealthy_count = 0
        
        # Check Redis
        if self._redis_messaging:
            try:
                redis_healthy = await self._redis_messaging.health_check()
                status["systems"]["redis"] = {
                    "status": "healthy" if redis_healthy else "unhealthy",
                    "enabled": True
                }
                if not redis_healthy:
                    unhealthy_count += 1
            except Exception as e:
                status["systems"]["redis"] = {
                    "status": "error",
                    "error": str(e),
                    "enabled": True
                }
                unhealthy_count += 1
        else:
            status["systems"]["redis"] = {"status": "disabled", "enabled": False}
        
        # Check WebSocket
        if self._websocket_messaging:
            connection_count = self._websocket_messaging.get_connection_count()
            status["systems"]["websocket"] = {
                "status": "healthy",
                "enabled": True,
                "connections": connection_count
            }
        else:
            status["systems"]["websocket"] = {"status": "disabled", "enabled": False}
        
        # Check SSE
        if self._sse_messaging:
            connection_count = self._sse_messaging.get_connection_count()
            status["systems"]["sse"] = {
                "status": "healthy",
                "enabled": True,
                "connections": connection_count
            }
        else:
            status["systems"]["sse"] = {"status": "disabled", "enabled": False}
        
        # Determine overall status
        enabled_systems = sum(1 for sys in status["systems"].values() if sys["enabled"])
        if unhealthy_count == enabled_systems and enabled_systems > 0:
            status["overall"] = "unhealthy"
        elif unhealthy_count > 0:
            status["overall"] = "degraded"
        
        return status
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get messaging statistics."""
        stats = {
            "initialized": self._is_initialized,
            "systems": {}
        }
        
        # WebSocket stats
        if self._websocket_messaging:
            stats["systems"]["websocket"] = {
                "connections": self._websocket_messaging.get_connection_count(),
                "enabled": True
            }
        
        # SSE stats
        if self._sse_messaging:
            stats["systems"]["sse"] = {
                "connections": self._sse_messaging.get_connection_count(),
                "enabled": True
            }
        
        # Redis stats
        if self._redis_messaging:
            stats["systems"]["redis"] = {
                "enabled": True,
                "type": "pubsub"
            }
        
        return stats


# Global messaging manager
messaging_manager = MessagingManager()