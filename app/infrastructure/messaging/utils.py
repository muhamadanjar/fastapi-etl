"""
Messaging utilities and helper functions.
"""

import json
import logging
from typing import Any, Optional, Dict, Callable
from datetime import datetime
import uuid
from dataclasses import asdict

from .base import Message

logger = logging.getLogger(__name__)


class MessageSerializer:
    """
    Message serialization utilities.
    """
    
    @staticmethod
    def serialize(message: Message) -> str:
        """Serialize message to JSON string."""
        try:
            return json.dumps(message.to_dict(), default=str)
        except Exception as e:
            logger.error(f"Failed to serialize message: {e}")
            raise
    
    @staticmethod
    def deserialize(data: str) -> Message:
        """Deserialize JSON string to message."""
        try:
            message_dict = json.loads(data)
            return Message.from_dict(message_dict)
        except Exception as e:
            logger.error(f"Failed to deserialize message: {e}")
            raise
    
    @staticmethod
    def serialize_payload(payload: Any) -> Any:
        """Serialize complex payload objects."""
        if hasattr(payload, 'to_dict'):
            return payload.to_dict()
        elif hasattr(payload, '__dict__'):
            return payload.__dict__
        elif hasattr(payload, '_asdict'):  # namedtuple
            return payload._asdict()
        else:
            return payload


class MessageValidator:
    """
    Message validation utilities.
    """
    
    @staticmethod
    def validate_topic(topic: str) -> bool:
        """Validate topic name."""
        if not topic or not isinstance(topic, str):
            return False
        
        # Topic should not contain certain characters
        invalid_chars = [' ', '\n', '\r', '\t']
        return not any(char in topic for char in invalid_chars)
    
    @staticmethod
    def validate_message(message: Message) -> bool:
        """Validate message object."""
        try:
            # Check required fields
            if not message.id or not isinstance(message.id, str):
                return False
            
            if not MessageValidator.validate_topic(message.topic):
                return False
            
            # Check TTL
            if message.ttl and message.is_expired():
                return False
            
            return True
            
        except Exception:
            return False
    
    @staticmethod
    def sanitize_payload(payload: Any, max_size: int = 1024 * 1024) -> Any:
        """Sanitize payload to prevent issues."""
        try:
            # Convert to JSON and check size
            serialized = json.dumps(payload, default=str)
            
            if len(serialized) > max_size:
                logger.warning(f"Payload size ({len(serialized)}) exceeds limit ({max_size})")
                return {"error": "Payload too large", "size": len(serialized)}
            
            return payload
            
        except Exception as e:
            logger.warning(f"Failed to sanitize payload: {e}")
            return {"error": "Invalid payload", "original_type": str(type(payload))}


def create_message_id() -> str:
    """Create unique message ID."""
    return str(uuid.uuid4())


def get_message_routing_key(topic: str, source: Optional[str] = None) -> str:
    """Generate routing key for message."""
    if source:
        return f"{topic}.{source}"
    return topic


def create_event_message(
    event_type: str,
    entity_id: Optional[str] = None,
    entity_type: Optional[str] = None,
    action: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None,
    **kwargs
) -> Message:
    """
    Create standardized event message.
    
    Args:
        event_type: Type of event (e.g., 'user.created', 'order.updated')
        entity_id: ID of the entity involved
        entity_type: Type of entity (e.g., 'user', 'order')
        action: Action performed (e.g., 'created', 'updated', 'deleted')
        data: Event data
        **kwargs: Additional message options
    """
    payload = {
        "event_type": event_type,
        "timestamp": datetime.utcnow().isoformat(),
    }
    
    if entity_id:
        payload["entity_id"] = entity_id
    
    if entity_type:
        payload["entity_type"] = entity_type
    
    if action:
        payload["action"] = action
    
    if data:
        payload["data"] = data
    
    return Message(
        topic=event_type,
        payload=payload,
        routing_key=get_message_routing_key(event_type),
        **kwargs
    )
