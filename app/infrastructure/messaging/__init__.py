"""
Messaging infrastructure package.

This module provides a unified interface for different messaging systems
including Redis Pub/Sub, WebSocket, Server-Sent Events, and external
message brokers like RabbitMQ, Kafka, etc.
"""

from .base import (
    MessageInterface,
    MessageBrokerInterface,
    MessageHandler,
    Message,
    MessageSubscriber,
    MessagePublisher,
)
from .redis_messaging import RedisMessaging, redis_messaging_manager
from .websocket_messaging import WebSocketMessaging #, websocket_manager
from .sse_messaging import SSEMessaging, sse_manager
from .manager import MessagingManager, messaging_manager
from .decorators import (
    message_handler,
    publish_event,
    subscribe_to,
    retry_on_failure,
)
from .utils import (
    MessageSerializer,
    MessageValidator,
    create_message_id,
    get_message_routing_key,
)

__version__ = "1.0.0"

__all__ = [
    # Core interfaces
    "MessageInterface",
    "MessageBrokerInterface", 
    "MessageHandler",
    "Message",
    "MessageSubscriber",
    "MessagePublisher",
    
    # Implementations
    "RedisMessaging",
    "redis_messaging_manager",
    "WebSocketMessaging", 
    "websocket_manager",
    "SSEMessaging",
    "sse_manager",
    
    # Manager
    "MessagingManager",
    "messaging_manager",
    
    # Decorators
    "message_handler",
    "publish_event",
    "subscribe_to", 
    "retry_on_failure",
    
    # Utilities
    "MessageSerializer",
    "MessageValidator",
    "create_message_id",
    "get_message_routing_key",
]