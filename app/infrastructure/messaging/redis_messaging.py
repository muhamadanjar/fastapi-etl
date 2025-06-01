"""
Redis-based messaging implementation using Pub/Sub.
"""

import asyncio
import logging
import json
from typing import Any, Optional, Dict, List, Callable, Set
from datetime import datetime

import redis.asyncio as redis
from redis.asyncio import ConnectionPool

from .base import (
    Message, 
    MessageInterface, 
    MessageBrokerInterface,
    MessageHandler,
    MessageFilter,
    MessageStatus,
)
from ...core.config import get_settings
from ...core.exceptions import MessagingError

logger = logging.getLogger(__name__)


class RedisMessaging(MessageInterface, MessageBrokerInterface):
    """
    Redis Pub/Sub messaging implementation.
    
    Features:
    - Redis Pub/Sub for real-time messaging
    - Message persistence with Redis Streams
    - Pattern-based subscriptions
    - Message acknowledgment
    - Dead letter queue support
    """
    
    def __init__(
        self,
        url: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        password: Optional[str] = None,
        db: Optional[int] = None,
        max_connections: Optional[int] = None,
        message_ttl: int = 3600,  # 1 hour
        enable_persistence: bool = True,
    ):
        """
        Initialize Redis messaging.
        
        Args:
            url: Redis URL
            host: Redis host
            port: Redis port
            password: Redis password
            db: Redis database
            max_connections: Max connections in pool
            message_ttl: Message TTL in seconds
            enable_persistence: Whether to use Redis Streams for persistence
        """
        settings = get_settings()
        
        self.url = url or getattr(settings, 'REDIS_URL', None)
        self.host = host or getattr(settings, 'REDIS_HOST', 'localhost')
        self.port = port or getattr(settings, 'REDIS_PORT', 6379)
        self.password = password or getattr(settings, 'REDIS_PASSWORD', None)
        self.db = db or getattr(settings, 'REDIS_DB', 0)
        self.max_connections = max_connections or 10
        self.message_ttl = message_ttl
        self.enable_persistence = enable_persistence
        
        # Connection pools
        self._publisher_pool: Optional[ConnectionPool] = None
        self._subscriber_pool: Optional[ConnectionPool] = None
        self._publisher: Optional[redis.Redis] = None
        self._subscriber: Optional[redis.Redis] = None
        self._pubsub: Optional[redis.client.PubSub] = None
        
        # Subscription management
        self._subscriptions: Dict[str, Dict[str, Any]] = {}
        self._subscription_counter = 0
        self._is_consuming = False
        self._consume_task: Optional[asyncio.Task] = None
        
        # Stream names for persistence
        self._stream_prefix = "msg_stream:"
        self._dlq_prefix = "dlq_stream:"
    
    async def connect(self) -> None:
        """Connect to Redis."""
        try:
            # Create connection pools
            if self.url:
                self._publisher_pool = ConnectionPool.from_url(
                    self.url,
                    max_connections=self.max_connections,
                    decode_responses=True,
                )
                self._subscriber_pool = ConnectionPool.from_url(
                    self.url,
                    max_connections=self.max_connections,
                    decode_responses=True,
                )
            else:
                self._publisher_pool = ConnectionPool(
                    host=self.host,
                    port=self.port,
                    password=self.password,
                    db=self.db,
                    max_connections=self.max_connections,
                    decode_responses=True,
                )
                self._subscriber_pool = ConnectionPool(
                    host=self.host,
                    port=self.port,
                    password=self.password,
                    db=self.db,
                    max_connections=self.max_connections,
                    decode_responses=True,
                )
            
            # Create Redis clients
            self._publisher = redis.Redis(connection_pool=self._publisher_pool)
            self._subscriber = redis.Redis(connection_pool=self._subscriber_pool)
            self._pubsub = self._subscriber.pubsub()
            
            # Test connections
            await self._publisher.ping()
            await self._subscriber.ping()
            
            logger.info("Redis messaging connected successfully")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis messaging: {e}")
            raise MessagingError(f"Redis messaging connection failed: {e}")
    
    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        try:
            # Stop consuming
            await self.stop_consuming()
            
            # Close pubsub
            if self._pubsub:
                await self._pubsub.close()
                self._pubsub = None
            
            # Close connections
            if self._publisher:
                await self._publisher.aclose()
                self._publisher = None
            
            if self._subscriber:
                await self._subscriber.aclose()
                self._subscriber = None
            
            # Close pools
            if self._publisher_pool:
                await self._publisher_pool.aclose()
                self._publisher_pool = None
            
            if self._subscriber_pool:
                await self._subscriber_pool.aclose()
                self._subscriber_pool = None
            
            logger.info("Redis messaging disconnected")
            
        except Exception as e:
            logger.error(f"Error disconnecting Redis messaging: {e}")
    
    async def health_check(self) -> bool:
        """Check Redis messaging health."""
        try:
            if not self._publisher or not self._subscriber:
                return False
            
            await self._publisher.ping()
            await self._subscriber.ping()
            return True
            
        except Exception as e:
            logger.error(f"Redis messaging health check failed: {e}")
            return False
    
    async def publish(
        self,
        topic: str,
        payload: Any,
        headers: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> str:
        """Publish message to topic."""
        message = Message(
            topic=topic,
            payload=payload,
            headers=headers or {},
            **kwargs
        )
        
        await self.publish_message(message)
        return message.id
    
    async def publish_message(self, message: Message) -> bool:
        """Publish message object."""
        try:
            if not self._publisher:
                raise MessagingError("Publisher not connected")
            
            # Serialize message
            message_data = json.dumps(message.to_dict(), default=str)
            
            # Publish to Pub/Sub
            await self._publisher.publish(message.topic, message_data)
            
            # Persist to stream if enabled
            if self.enable_persistence:
                stream_name = f"{self._stream_prefix}{message.topic}"
                await self._publisher.xadd(
                    stream_name,
                    message.to_dict(),
                    maxlen=10000,  # Keep last 10k messages
                    approximate=True
                )
                
                # Set TTL for stream
                await self._publisher.expire(stream_name, self.message_ttl)
            
            logger.debug(f"Published message {message.id} to topic {message.topic}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            return False
    
    async def subscribe(
        self,
        topic: str,
        handler: MessageHandler,
        filters: Optional[List[MessageFilter]] = None
    ) -> str:
        """Subscribe to topic."""
        self._subscription_counter += 1
        subscription_id = f"sub_{self._subscription_counter}"
        
        self._subscriptions[subscription_id] = {
            "topic": topic,
            "handler": handler,
            "filters": filters or [],
            "created_at": datetime.utcnow(),
        }
        
        # Subscribe to Redis channel
        await self._pubsub.subscribe(topic)
        
        logger.info(f"Subscribed to topic {topic} with ID {subscription_id}")
        return subscription_id
    
    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from topic."""
        if subscription_id not in self._subscriptions:
            return False
        
        subscription = self._subscriptions.pop(subscription_id)
        topic = subscription["topic"]
        
        # Check if any other subscriptions exist for this topic
        topic_subscriptions = [
            sub for sub in self._subscriptions.values()
            if sub["topic"] == topic
        ]
        
        # Unsubscribe from Redis channel if no more subscriptions
        if not topic_subscriptions:
            await self._pubsub.unsubscribe(topic)
        
        logger.info(f"Unsubscribed from topic {topic} (ID: {subscription_id})")
        return True
    
    async def start_consuming(self) -> None:
        """Start consuming messages."""
        if self._is_consuming:
            return
        
        self._is_consuming = True
        self._consume_task = asyncio.create_task(self._consume_messages())
        logger.info("Started consuming messages")
    
    async def stop_consuming(self) -> None:
        """Stop consuming messages."""
        if not self._is_consuming:
            return
        
        self._is_consuming = False
        
        if self._consume_task:
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
            self._consume_task = None
        
        logger.info("Stopped consuming messages")
    
    async def _consume_messages(self) -> None:
        """Message consumption loop."""
        try:
            while self._is_consuming:
                try:
                    # Get message from pubsub
                    redis_message = await self._pubsub.get_message(
                        ignore_subscribe_messages=True,
                        timeout=1.0
                    )
                    
                    if redis_message and redis_message["type"] == "message":
                        await self._process_redis_message(redis_message)
                        
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"Error consuming message: {e}")
                    await asyncio.sleep(1)
                    
        except asyncio.CancelledError:
            logger.info("Message consumption cancelled")
        except Exception as e:
            logger.error(f"Fatal error in message consumption: {e}")
    
    async def _process_redis_message(self, redis_message: Dict[str, Any]) -> None:
        """Process message from Redis."""
        try:
            # Parse message
            message_data = json.loads(redis_message["data"])
            message = Message.from_dict(message_data)
            topic = redis_message["channel"]
            
            # Find matching subscriptions
            matching_subscriptions = [
                sub for sub in self._subscriptions.values()
                if sub["topic"] == topic
            ]
            
            # Process message for each subscription
            for subscription in matching_subscriptions:
                await self._handle_subscription_message(subscription, message)
                
        except Exception as e:
            logger.error(f"Error processing Redis message: {e}")
    
    async def _handle_subscription_message(
        self, 
        subscription: Dict[str, Any], 
        message: Message
    ) -> None:
        """Handle message for specific subscription."""
        try:
            # Apply filters
            for filter_func in subscription["filters"]:
                if not filter_func(message):
                    return
            
            # Mark message as processing
            message.mark_processing()
            
            # Call handler
            handler = subscription["handler"]
            if asyncio.iscoroutinefunction(handler):
                await handler(message)
            else:
                handler(message)
            
            # Mark as completed
            message.mark_completed()
            
        except Exception as e:
            logger.error(f"Error handling message {message.id}: {e}")
            
            # Mark as failed
            message.mark_failed(str(e))
            
            # Send to dead letter queue if max retries exceeded
            if not message.can_retry():
                await self._send_to_dead_letter_queue(message)
            else:
                message.mark_retry()
                # Could implement retry logic here
    
    async def _send_to_dead_letter_queue(self, message: Message) -> None:
        """Send message to dead letter queue."""
        try:
            if not self.enable_persistence:
                return
            
            dlq_stream = f"{self._dlq_prefix}{message.topic}"
            await self._publisher.xadd(dlq_stream, message.to_dict())
            
            logger.warning(f"Message {message.id} sent to dead letter queue")
            
        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e}")
    
    # Topic management
    async def create_topic(self, topic: str, **config) -> bool:
        """Create topic (Redis channels are created implicitly)."""
        return True
    
    async def delete_topic(self, topic: str) -> bool:
        """Delete topic (Redis channels are removed when no subscribers)."""
        return True
    
    async def list_topics(self) -> List[str]:
        """List active topics."""
        try:
            # Get all pubsub channels
            channels = await self._publisher.pubsub_channels()
            return list(channels.keys())
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            return []
    
    async def get_topic_info(self, topic: str) -> Dict[str, Any]:
        """Get topic information."""
        try:
            # Get subscriber count
            subscribers = await self._publisher.pubsub_numsub(topic)
            subscriber_count = dict(subscribers).get(topic, 0)
            
            info = {
                "topic": topic,
                "subscribers": subscriber_count,
                "type": "redis_pubsub",
            }
            
            # Add stream info if persistence enabled
            if self.enable_persistence:
                stream_name = f"{self._stream_prefix}{topic}"
                try:
                    stream_info = await self._publisher.xinfo_stream(stream_name)
                    info["stream"] = {
                        "length": stream_info.get("length", 0),
                        "last_generated_id": stream_info.get("last-generated-id"),
                    }
                except:
                    info["stream"] = {"exists": False}
            
            return info
            
        except Exception as e:
            logger.error(f"Failed to get topic info: {e}")
            return {"topic": topic, "error": str(e)}
    
    async def get_dead_letter_messages(self, topic: str) -> List[Message]:
        """Get messages from dead letter queue."""
        try:
            if not self.enable_persistence:
                return []
            
            dlq_stream = f"{self._dlq_prefix}{topic}"
            messages = await self._publisher.xrange(dlq_stream)
            
            result = []
            for message_id, fields in messages:
                try:
                    message = Message.from_dict(fields)
                    result.append(message)
                except Exception as e:
                    logger.error(f"Failed to parse DLQ message: {e}")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to get DLQ messages: {e}")
            return []


class RedisMessagingManager:
    """Redis messaging manager with lifecycle management."""
    
    def __init__(self):
        self._messaging: Optional[RedisMessaging] = None
    
    async def connect(self, **config) -> None:
        """Initialize Redis messaging."""
        self._messaging = RedisMessaging(**config)
        await self._messaging.connect()
    
    async def disconnect(self) -> None:
        """Close Redis messaging."""
        if self._messaging:
            await self._messaging.disconnect()
            self._messaging = None
    
    def get_messaging(self) -> Optional[RedisMessaging]:
        """Get messaging instance."""
        return self._messaging
    
    async def health_check(self) -> bool:
        """Check messaging health."""
        if not self._messaging:
            return False
        return await self._messaging.health_check()


# Global Redis messaging manager
redis_messaging_manager = RedisMessagingManager()