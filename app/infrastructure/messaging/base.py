"""
Base interfaces and data structures for messaging systems.
"""

import asyncio
from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, List, Callable, Union, AsyncGenerator
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
import uuid


class MessagePriority(Enum):
    """Message priority levels."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


class MessageStatus(Enum):
    """Message processing status."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed" 
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD_LETTER = "dead_letter"


@dataclass
class Message:
    """
    Universal message data structure.
    
    Represents a message that can be sent through any messaging system.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    topic: str = ""
    payload: Any = None
    headers: Dict[str, Any] = field(default_factory=dict)
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.utcnow)
    priority: MessagePriority = MessagePriority.NORMAL
    retry_count: int = 0
    max_retries: int = 3
    ttl: Optional[timedelta] = None
    
    # Processing info
    status: MessageStatus = MessageStatus.PENDING
    error_message: Optional[str] = None
    processed_at: Optional[datetime] = None
    
    # Routing
    routing_key: Optional[str] = None
    source: Optional[str] = None
    destination: Optional[str] = None
    correlation_id: Optional[str] = None
    reply_to: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary."""
        return {
            "id": self.id,
            "topic": self.topic,
            "payload": self.payload,
            "headers": self.headers,
            "created_at": self.created_at.isoformat(),
            "priority": self.priority.value,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "ttl": self.ttl.total_seconds() if self.ttl else None,
            "status": self.status.value,
            "error_message": self.error_message,
            "processed_at": self.processed_at.isoformat() if self.processed_at else None,
            "routing_key": self.routing_key,
            "source": self.source,
            "destination": self.destination,
            "correlation_id": self.correlation_id,
            "reply_to": self.reply_to,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Message":
        """Create message from dictionary."""
        message = cls()
        message.id = data.get("id", message.id)
        message.topic = data.get("topic", "")
        message.payload = data.get("payload")
        message.headers = data.get("headers", {})
        
        if "created_at" in data:
            message.created_at = datetime.fromisoformat(data["created_at"])
        
        if "priority" in data:
            message.priority = MessagePriority(data["priority"])
        
        message.retry_count = data.get("retry_count", 0)
        message.max_retries = data.get("max_retries", 3)
        
        if data.get("ttl"):
            message.ttl = timedelta(seconds=data["ttl"])
        
        if "status" in data:
            message.status = MessageStatus(data["status"])
        
        message.error_message = data.get("error_message")
        
        if data.get("processed_at"):
            message.processed_at = datetime.fromisoformat(data["processed_at"])
        
        message.routing_key = data.get("routing_key")
        message.source = data.get("source")
        message.destination = data.get("destination")
        message.correlation_id = data.get("correlation_id")
        message.reply_to = data.get("reply_to")
        
        return message
    
    def is_expired(self) -> bool:
        """Check if message has expired."""
        if self.ttl is None:
            return False
        return datetime.utcnow() > (self.created_at + self.ttl)
    
    def can_retry(self) -> bool:
        """Check if message can be retried."""
        return self.retry_count < self.max_retries and not self.is_expired()
    
    def mark_processing(self) -> None:
        """Mark message as being processed."""
        self.status = MessageStatus.PROCESSING
        self.processed_at = datetime.utcnow()
    
    def mark_completed(self) -> None:
        """Mark message as completed."""
        self.status = MessageStatus.COMPLETED
        self.processed_at = datetime.utcnow()
    
    def mark_failed(self, error: str) -> None:
        """Mark message as failed."""
        self.status = MessageStatus.FAILED
        self.error_message = error
        self.processed_at = datetime.utcnow()
    
    def mark_retry(self) -> None:
        """Mark message for retry."""
        self.retry_count += 1
        self.status = MessageStatus.RETRYING
        self.processed_at = datetime.utcnow()


# Type aliases
MessageHandler = Callable[[Message], Any]
MessageFilter = Callable[[Message], bool]


class MessageSubscriber(ABC):
    """
    Abstract message subscriber interface.
    """
    
    @abstractmethod
    async def subscribe(
        self, 
        topic: str, 
        handler: MessageHandler,
        filters: Optional[List[MessageFilter]] = None
    ) -> str:
        """
        Subscribe to a topic.
        
        Args:
            topic: Topic to subscribe to
            handler: Message handler function
            filters: Optional message filters
            
        Returns:
            Subscription ID
        """
        pass
    
    @abstractmethod
    async def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe from a topic.
        
        Args:
            subscription_id: Subscription ID to remove
            
        Returns:
            True if unsubscribed successfully
        """
        pass
    
    @abstractmethod
    async def start_consuming(self) -> None:
        """Start consuming messages."""
        pass
    
    @abstractmethod
    async def stop_consuming(self) -> None:
        """Stop consuming messages."""
        pass


class MessagePublisher(ABC):
    """
    Abstract message publisher interface.
    """
    
    @abstractmethod
    async def publish(
        self, 
        topic: str, 
        payload: Any,
        headers: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> str:
        """
        Publish a message to a topic.
        
        Args:
            topic: Topic to publish to
            payload: Message payload
            headers: Optional message headers
            **kwargs: Additional message options
            
        Returns:
            Message ID
        """
        pass
    
    @abstractmethod
    async def publish_message(self, message: Message) -> bool:
        """
        Publish a message object.
        
        Args:
            message: Message to publish
            
        Returns:
            True if published successfully
        """
        pass


class MessageInterface(MessageSubscriber, MessagePublisher):
    """
    Combined message interface for both publishing and subscribing.
    """
    pass


class MessageBrokerInterface(ABC):
    """
    Abstract message broker interface for advanced messaging features.
    """
    
    @abstractmethod
    async def connect(self) -> None:
        """Connect to message broker."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from message broker."""
        pass
    
    @abstractmethod
    async def health_check(self) -> bool:
        """Check broker health."""
        pass
    
    @abstractmethod
    async def create_topic(self, topic: str, **config) -> bool:
        """Create a new topic."""
        pass
    
    @abstractmethod
    async def delete_topic(self, topic: str) -> bool:
        """Delete a topic."""
        pass
    
    @abstractmethod
    async def list_topics(self) -> List[str]:
        """List all topics."""
        pass
    
    @abstractmethod
    async def get_topic_info(self, topic: str) -> Dict[str, Any]:
        """Get topic information."""
        pass
    
    # Queue management for persistent messaging
    async def create_queue(self, queue: str, **config) -> bool:
        """Create a message queue."""
        raise NotImplementedError("Queue management not supported")
    
    async def delete_queue(self, queue: str) -> bool:
        """Delete a message queue."""
        raise NotImplementedError("Queue management not supported")
    
    async def purge_queue(self, queue: str) -> int:
        """Purge messages from queue."""
        raise NotImplementedError("Queue management not supported")
    
    # Dead letter queue support
    async def get_dead_letter_messages(self, topic: str) -> List[Message]:
        """Get messages from dead letter queue."""
        return []
    
    async def requeue_dead_letter_message(self, message_id: str) -> bool:
        """Requeue a dead letter message."""
        return False


class MessageMiddleware(ABC):
    """
    Abstract middleware for message processing pipeline.
    """
    
    @abstractmethod
    async def process_incoming(self, message: Message) -> Message:
        """Process incoming message."""
        pass
    
    @abstractmethod
    async def process_outgoing(self, message: Message) -> Message:
        """Process outgoing message."""
        pass

