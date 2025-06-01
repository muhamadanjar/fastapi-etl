"""
RabbitMQ messaging implementation using AMQP.
"""

import asyncio
import logging
import json
from typing import Any, Optional, Dict, List, Callable, Set
from datetime import datetime, timedelta

import aio_pika
from aio_pika import Message as AMQPMessage, DeliveryMode, ExchangeType
from aio_pika.abc import AbstractConnection, AbstractChannel, AbstractExchange, AbstractQueue

from .base import (
    Message,
    MessageInterface,
    MessageBrokerInterface,
    MessageHandler,
    MessageFilter,
    MessageStatus,
    MessagePriority,
)
from ...core.config import get_settings
from ...core.exceptions import MessagingError

logger = logging.getLogger(__name__)


class RabbitMQMessaging(MessageInterface, MessageBrokerInterface):
    """
    RabbitMQ messaging implementation using AMQP.
    
    Features:
    - AMQP messaging with exchanges and queues
    - Message persistence and durability
    - Dead letter queue support
    - Message acknowledgment
    - Priority queues
    - Routing key support
    - Topic and direct exchanges
    """
    
    def __init__(
        self,
        url: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        virtual_host: Optional[str] = None,
        exchange_name: str = "messages",
        exchange_type: ExchangeType = ExchangeType.TOPIC,
        message_ttl: Optional[int] = None,
        max_retries: int = 3,
        prefetch_count: int = 10,
    ):
        """
        Initialize RabbitMQ messaging.
        
        Args:
            url: RabbitMQ URL (amqp://...)
            host: RabbitMQ host
            port: RabbitMQ port
            username: Username
            password: Password
            virtual_host: Virtual host
            exchange_name: Default exchange name
            exchange_type: Exchange type (topic, direct, fanout)
            message_ttl: Message TTL in seconds
            max_retries: Maximum retry attempts
            prefetch_count: Prefetch count for consumers
        """
        settings = get_settings()
        
        self.url = url or getattr(settings, 'RABBITMQ_URL', None)
        self.host = host or getattr(settings, 'RABBITMQ_HOST', 'localhost')
        self.port = port or getattr(settings, 'RABBITMQ_PORT', 5672)
        self.username = username or getattr(settings, 'RABBITMQ_USERNAME', 'guest')
        self.password = password or getattr(settings, 'RABBITMQ_PASSWORD', 'guest')
        self.virtual_host = virtual_host or getattr(settings, 'RABBITMQ_VHOST', '/')
        
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.message_ttl = message_ttl
        self.max_retries = max_retries
        self.prefetch_count = prefetch_count
        
        # Connection objects
        self._connection: Optional[AbstractConnection] = None
        self._channel: Optional[AbstractChannel] = None
        self._exchange: Optional[AbstractExchange] = None
        
        # Consumer management
        self._consumers: Dict[str, Dict[str, Any]] = {}
        self._queues: Dict[str, AbstractQueue] = {}
        self._is_consuming = False
        self._consume_tasks: List[asyncio.Task] = []
        
        # Dead letter exchange
        self._dlx_name = f"{exchange_name}_dlx"
        self._dlx_exchange: Optional[AbstractExchange] = None
    
    async def connect(self) -> None:
        """Connect to RabbitMQ."""
        try:
            # Build connection URL if not provided
            if not self.url:
                self.url = (
                    f"amqp://{self.username}:{self.password}@"
                    f"{self.host}:{self.port}{self.virtual_host}"
                )
            
            # Establish connection
            self._connection = await aio_pika.connect_robust(
                self.url,
                client_properties={"application": "messaging_service"}
            )
            
            # Create channel
            self._channel = await self._connection.channel()
            await self._channel.set_qos(prefetch_count=self.prefetch_count)
            
            # Declare main exchange
            self._exchange = await self._channel.declare_exchange(
                self.exchange_name,
                self.exchange_type,
                durable=True
            )
            
            # Declare dead letter exchange
            self._dlx_exchange = await self._channel.declare_exchange(
                self._dlx_name,
                ExchangeType.DIRECT,
                durable=True
            )
            
            logger.info(f"Connected to RabbitMQ at {self.host}:{self.port}")
            
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise MessagingError(f"RabbitMQ connection failed: {e}")
    
    async def disconnect(self) -> None:
        """Disconnect from RabbitMQ."""
        try:
            # Stop consuming
            await self.stop_consuming()
            
            # Close channel and connection
            if self._channel:
                await self._channel.close()
                self._channel = None
            
            if self._connection:
                await self._connection.close()
                self._connection = None
            
            self._exchange = None
            self._dlx_exchange = None
            
            logger.info("Disconnected from RabbitMQ")
            
        except Exception as e:
            logger.error(f"Error disconnecting from RabbitMQ: {e}")
    
    async def health_check(self) -> bool:
        """Check RabbitMQ connection health."""
        try:
            if not self._connection or self._connection.is_closed:
                return False
            
            # Try to declare a temporary queue
            temp_queue = await self._channel.declare_queue(
                exclusive=True,
                auto_delete=True
            )
            await temp_queue.delete()
            
            return True
            
        except Exception as e:
            logger.error(f"RabbitMQ health check failed: {e}")
            return False
    
    async def publish(
        self,
        topic: str,
        payload: Any,
        headers: Optional[Dict[str, Any]] = None,
        routing_key: Optional[str] = None,
        priority: Optional[MessagePriority] = None,
        **kwargs
    ) -> str:
        """Publish message to RabbitMQ."""
        message = Message(
            topic=topic,
            payload=payload,
            headers=headers or {},
            routing_key=routing_key or topic,
            priority=priority or MessagePriority.NORMAL,
            **kwargs
        )
        
        await self.publish_message(message)
        return message.id
    
    async def publish_message(self, message: Message) -> bool:
        """Publish message object to RabbitMQ."""
        try:
            if not self._exchange:
                raise MessagingError("Not connected to RabbitMQ")
            
            # Prepare message properties
            properties = {
                "message_id": message.id,
                "timestamp": message.created_at,
                "headers": {
                    **message.headers,
                    "topic": message.topic,
                    "retry_count": message.retry_count,
                    "max_retries": message.max_retries,
                }
            }
            
            # Set TTL
            if message.ttl:
                properties["expiration"] = str(int(message.ttl.total_seconds() * 1000))
            elif self.message_ttl:
                properties["expiration"] = str(self.message_ttl * 1000)
            
            # Set priority
            if message.priority != MessagePriority.NORMAL:
                properties["priority"] = message.priority.value
            
            # Set delivery mode (persistent)
            properties["delivery_mode"] = DeliveryMode.PERSISTENT
            
            # Correlation ID for request-response
            if message.correlation_id:
                properties["correlation_id"] = message.correlation_id
            
            if message.reply_to:
                properties["reply_to"] = message.reply_to
            
            # Create AMQP message
            amqp_message = AMQPMessage(
                body=json.dumps(message.to_dict(), default=str).encode(),
                **properties
            )
            
            # Publish message
            await self._exchange.publish(
                amqp_message,
                routing_key=message.routing_key or message.topic
            )
            
            logger.debug(f"Published message {message.id} to topic {message.topic}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish message to RabbitMQ: {e}")
            return False
    
    async def subscribe(
        self,
        topic: str,
        handler: MessageHandler,
        filters: Optional[List[MessageFilter]] = None,
        queue_name: Optional[str] = None,
        routing_key: Optional[str] = None,
        durable: bool = True,
        auto_delete: bool = False,
        exclusive: bool = False,
    ) -> str:
        """Subscribe to RabbitMQ topic."""
        try:
            # Generate subscription ID
            subscription_id = f"sub_{len(self._consumers) + 1}_{topic}"
            
            # Generate queue name if not provided
            if not queue_name:
                queue_name = f"queue_{topic}_{subscription_id}"
            
            # Declare queue with DLX support
            queue_args = {
                "x-dead-letter-exchange": self._dlx_name,
                "x-dead-letter-routing-key": f"dlq.{topic}",
            }
            
            # Add message TTL if configured
            if self.message_ttl:
                queue_args["x-message-ttl"] = self.message_ttl * 1000
            
            # Support priority queues
            queue_args["x-max-priority"] = 10
            
            queue = await self._channel.declare_queue(
                queue_name,
                durable=durable,
                auto_delete=auto_delete,
                exclusive=exclusive,
                arguments=queue_args
            )
            
            # Bind queue to exchange
            await queue.bind(
                self._exchange,
                routing_key=routing_key or topic
            )
            
            # Store subscription info
            self._consumers[subscription_id] = {
                "topic": topic,
                "handler": handler,
                "filters": filters or [],
                "queue": queue,
                "queue_name": queue_name,
                "routing_key": routing_key or topic,
                "created_at": datetime.utcnow(),
            }
            
            self._queues[queue_name] = queue
            
            logger.info(f"Subscribed to topic {topic} with queue {queue_name}")
            
            # Start consuming if not already started
            if self._is_consuming:
                await self._start_consumer(subscription_id)
            
            return subscription_id
            
        except Exception as e:
            logger.error(f"Failed to subscribe to topic {topic}: {e}")
            raise MessagingError(f"Subscription failed: {e}")
    
    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from RabbitMQ topic."""
        try:
            if subscription_id not in self._consumers:
                return False
            
            consumer_info = self._consumers.pop(subscription_id)
            queue = consumer_info["queue"]
            
            # Cancel consumer
            if hasattr(queue, '_consumer_tag') and queue._consumer_tag:
                await self._channel.basic_cancel(queue._consumer_tag)
            
            # Remove queue from tracking
            queue_name = consumer_info["queue_name"]
            if queue_name in self._queues:
                del self._queues[queue_name]
            
            logger.info(f"Unsubscribed from topic {consumer_info['topic']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to unsubscribe {subscription_id}: {e}")
            return False
    
    async def start_consuming(self) -> None:
        """Start consuming messages from all subscriptions."""
        if self._is_consuming:
            return
        
        self._is_consuming = True
        
        # Start consumers for all existing subscriptions
        for subscription_id in self._consumers:
            await self._start_consumer(subscription_id)
        
        logger.info("Started consuming RabbitMQ messages")
    
    async def stop_consuming(self) -> None:
        """Stop consuming messages."""
        if not self._is_consuming:
            return
        
        self._is_consuming = False
        
        # Cancel all consume tasks
        for task in self._consume_tasks:
            task.cancel()
        
        if self._consume_tasks:
            await asyncio.gather(*self._consume_tasks, return_exceptions=True)
        
        self._consume_tasks.clear()
        
        logger.info("Stopped consuming RabbitMQ messages")
    
    async def _start_consumer(self, subscription_id: str) -> None:
        """Start consumer for specific subscription."""
        consumer_info = self._consumers[subscription_id]
        queue = consumer_info["queue"]
        
        async def message_consumer(amqp_message: aio_pika.IncomingMessage):
            async with amqp_message.process():
                await self._process_amqp_message(amqp_message, consumer_info)
        
        # Start consuming
        await queue.consume(message_consumer)
        
        logger.debug(f"Started consumer for subscription {subscription_id}")
    
    async def _process_amqp_message(
        self,
        amqp_message: aio_pika.IncomingMessage,
        consumer_info: Dict[str, Any]
    ) -> None:
        """Process incoming AMQP message."""
        try:
            # Parse message
            message_data = json.loads(amqp_message.body.decode())
            message = Message.from_dict(message_data)
            
            # Apply filters
            for filter_func in consumer_info["filters"]:
                if not filter_func(message):
                    return
            
            # Mark as processing
            message.mark_processing()
            
            # Call handler
            handler = consumer_info["handler"]
            if asyncio.iscoroutinefunction(handler):
                await handler(message)
            else:
                handler(message)
            
            # Mark as completed
            message.mark_completed()
            
            logger.debug(f"Processed message {message.id}")
            
        except Exception as e:
            logger.error(f"Error processing RabbitMQ message: {e}")
            
            # Message will be rejected and sent to DLQ automatically
            # due to the exception in the async context manager
            raise
    
    # Queue management methods
    async def create_queue(
        self,
        queue_name: str,
        durable: bool = True,
        auto_delete: bool = False,
        exclusive: bool = False,
        **config
    ) -> bool:
        """Create a queue."""
        try:
            queue = await self._channel.declare_queue(
                queue_name,
                durable=durable,
                auto_delete=auto_delete,
                exclusive=exclusive,
                arguments=config.get("arguments", {})
            )
            
            self._queues[queue_name] = queue
            logger.info(f"Created queue {queue_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create queue {queue_name}: {e}")
            return False
    
    async def delete_queue(self, queue_name: str) -> bool:
        """Delete a queue."""
        try:
            if queue_name in self._queues:
                queue = self._queues[queue_name]
                await queue.delete()
                del self._queues[queue_name]
                logger.info(f"Deleted queue {queue_name}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to delete queue {queue_name}: {e}")
            return False
    
    async def purge_queue(self, queue_name: str) -> int:
        """Purge messages from queue."""
        try:
            if queue_name in self._queues:
                queue = self._queues[queue_name]
                result = await queue.purge()
                logger.info(f"Purged {result} messages from queue {queue_name}")
                return result
            
            return 0
            
        except Exception as e:
            logger.error(f"Failed to purge queue {queue_name}: {e}")
            return 0
    
    # Topic management
    async def create_topic(self, topic: str, **config) -> bool:
        """Create topic (exchange in RabbitMQ)."""
        try:
            exchange_name = config.get("exchange_name", f"topic_{topic}")
            exchange_type = config.get("exchange_type", ExchangeType.TOPIC)
            
            await self._channel.declare_exchange(
                exchange_name,
                exchange_type,
                durable=config.get("durable", True)
            )
            
            logger.info(f"Created topic exchange {exchange_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create topic {topic}: {e}")
            return False
    
    async def delete_topic(self, topic: str) -> bool:
        """Delete topic (not commonly done in RabbitMQ)."""
        logger.warning("Deleting exchanges is not recommended in RabbitMQ")
        return False
    
    async def list_topics(self) -> List[str]:
        """List topics (queues in this context)."""
        return list(self._queues.keys())
    
    async def get_topic_info(self, topic: str) -> Dict[str, Any]:
        """Get topic information."""
        try:
            if topic in self._queues:
                queue = self._queues[topic]
                
                # Get queue info (this would require management API in real implementation)
                return {
                    "topic": topic,
                    "queue_name": queue.name,
                    "type": "rabbitmq_queue",
                    "durable": True,  # Assuming durable
                }
            
            return {"topic": topic, "exists": False}
            
        except Exception as e:
            logger.error(f"Failed to get topic info for {topic}: {e}")
            return {"topic": topic, "error": str(e)}
    
    # Dead letter queue support
    async def get_dead_letter_messages(self, topic: str) -> List[Message]:
        """Get messages from dead letter queue."""
        try:
            dlq_name = f"dlq_{topic}"
            
            if dlq_name not in self._queues:
                # Try to declare DLQ
                dlq = await self._channel.declare_queue(
                    dlq_name,
                    durable=True
                )
                await dlq.bind(self._dlx_exchange, f"dlq.{topic}")
                self._queues[dlq_name] = dlq
            
            # This would require getting messages without consuming them
            # In practice, you'd use RabbitMQ Management API
            logger.warning("Getting DLQ messages requires RabbitMQ Management API")
            return []
            
        except Exception as e:
            logger.error(f"Failed to get DLQ messages for {topic}: {e}")
            return []
    
    async def requeue_dead_letter_message(self, message_id: str) -> bool:
        """Requeue a dead letter message."""
        # This would require RabbitMQ Management API
        logger.warning("Requeuing DLQ messages requires RabbitMQ Management API")
        return False


class RabbitMQManager:
    """RabbitMQ manager with lifecycle management."""
    
    def __init__(self):
        self._messaging: Optional[RabbitMQMessaging] = None
    
    async def connect(self, **config) -> None:
        """Initialize RabbitMQ messaging."""
        self._messaging = RabbitMQMessaging(**config)
        await self._messaging.connect()
    
    async def disconnect(self) -> None:
        """Close RabbitMQ messaging."""
        if self._messaging:
            await self._messaging.disconnect()
            self._messaging = None
    
    def get_messaging(self) -> Optional[RabbitMQMessaging]:
        """Get messaging instance."""
        return self._messaging
    
    async def health_check(self) -> bool:
        """Check messaging health."""
        if not self._messaging:
            return False
        return await self._messaging.health_check()


# Global RabbitMQ manager
rabbitmq_manager = RabbitMQManager()
