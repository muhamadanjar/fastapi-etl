"""
RabbitMQ message publisher implementation.

This module provides RabbitMQ publisher functionality with
connection management, routing, and retry logic.
"""

import json
import logging
from typing import Any, Dict, Optional, Union
from datetime import datetime
from dataclasses import dataclass

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError, AMQPChannelError

from ....core.config import get_settings
from ....core.exceptions import MessageBrokerError

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class MessageProperties:
    """Message properties for RabbitMQ."""
    
    delivery_mode: int = 2  # Persistent
    priority: int = 0
    expiration: Optional[str] = None
    message_id: Optional[str] = None
    timestamp: Optional[datetime] = None
    content_type: str = "application/json"
    content_encoding: str = "utf-8"
    headers: Optional[Dict[str, Any]] = None
    
    def to_pika_properties(self) -> pika.BasicProperties:
        """Convert to pika BasicProperties."""
        return pika.BasicProperties(
            delivery_mode=self.delivery_mode,
            priority=self.priority,
            expiration=self.expiration,
            message_id=self.message_id,
            timestamp=self.timestamp,
            content_type=self.content_type,
            content_encoding=self.content_encoding,
            headers=self.headers or {},
        )


class RabbitMQPublisher:
    """
    RabbitMQ message publisher with connection management.
    
    Provides reliable message publishing with automatic reconnection,
    exchange and queue declaration, and error handling.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5672,
        username: str = "guest",
        password: str = "guest",
        virtual_host: str = "/",
        connection_url: Optional[str] = None,
    ):
        """
        Initialize RabbitMQ publisher.
        
        Args:
            host: RabbitMQ host
            port: RabbitMQ port
            username: RabbitMQ username
            password: RabbitMQ password
            virtual_host: RabbitMQ virtual host
            connection_url: Full connection URL (overrides other params)
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.connection_url = connection_url
        
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[BlockingChannel] = None
        
        self._connect()
    
    def _connect(self) -> None:
        """Establish connection to RabbitMQ."""
        try:
            if self.connection_url:
                # Use connection URL
                parameters = pika.URLParameters(self.connection_url)
            else:
                # Use individual parameters
                credentials = pika.PlainCredentials(self.username, self.password)
                parameters = pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    virtual_host=self.virtual_host,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300,
                )
            
            self._connection = pika.BlockingConnection(parameters)
            self._channel = self._connection.channel()
            
            logger.info(f"Connected to RabbitMQ at {self.host}:{self.port}")
            
        except AMQPConnectionError as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise MessageBrokerError(f"RabbitMQ connection failed: {e}")
    
    def _ensure_connection(self) -> None:
        """Ensure connection is active, reconnect if necessary."""
        try:
            if not self._connection or self._connection.is_closed:
                logger.info("Reconnecting to RabbitMQ...")
                self._connect()
            elif not self._channel or self._channel.is_closed:
                logger.info("Reopening RabbitMQ channel...")
                self._channel = self._connection.channel()
        except Exception as e:
            logger.error(f"Failed to ensure RabbitMQ connection: {e}")
            raise MessageBrokerError(f"Connection check failed: {e}")
    
    def declare_exchange(
        self,
        exchange_name: str,
        exchange_type: str = "direct",
        durable: bool = True,
        auto_delete: bool = False,
        arguments: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Declare an exchange.
        
        Args:
            exchange_name: Name of the exchange
            exchange_type: Type of exchange (direct, topic, fanout, headers)
            durable: Whether exchange survives broker restart
            auto_delete: Whether exchange is deleted when no longer used
            arguments: Additional exchange arguments
        """
        try:
            self._ensure_connection()
            
            self._channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=exchange_type,
                durable=durable,
                auto_delete=auto_delete,
                arguments=arguments or {},
            )
            
            logger.debug(f"Declared exchange: {exchange_name} ({exchange_type})")
            
        except AMQPChannelError as e:
            logger.error(f"Failed to declare exchange {exchange_name}: {e}")
            raise MessageBrokerError(f"Exchange declaration failed: {e}")
    
    
    def declare_queue(
        self,
        queue_name: str,
        durable: bool = True,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: Optional[Dict[str, Any]] = None,
    ) -> pika.frame.Method:
        """
        Declare a queue.
        
        Args:
            queue_name: Name of the queue
            durable: Whether queue survives broker restart
            exclusive: Whether queue is exclusive to this connection
            auto_delete: Whether queue is deleted when no longer used
            arguments: Additional queue arguments
            
        Returns:
            Queue declaration result
        """
        try:
            self._ensure_connection()
            
            result = self._channel.queue_declare(
                queue=queue_name,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                arguments=arguments or {},
            )
            
            logger.debug(f"Declared queue: {queue_name}")
            return result
            
        except AMQPChannelError as e:
            logger.error(f"Failed to declare queue {queue_name}: {e}")
            raise MessageBrokerError(f"Queue declaration failed: {e}")
    
    def bind_queue(
        self,
        queue_name: str,
        exchange_name: str,
        routing_key: str = "",
        arguments: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Bind queue to exchange with routing key.
        
        Args:
            queue_name: Name of the queue to bind
            exchange_name: Name of the exchange to bind to
            routing_key: Routing key for binding
            arguments: Additional binding arguments
        """
        try:
            self._ensure_connection()
            
            self._channel.queue_bind(
                exchange=exchange_name,
                queue=queue_name,
                routing_key=routing_key,
                arguments=arguments or {},
            )
            
            logger.debug(f"Bound queue {queue_name} to exchange {exchange_name} with key {routing_key}")
            
        except AMQPChannelError as e:
            logger.error(f"Failed to bind queue {queue_name} to exchange {exchange_name}: {e}")
            raise MessageBrokerError(f"Queue binding failed: {e}")
    
    def publish_message(
        self,
        exchange_name: str,
        routing_key: str,
        message: Union[str, Dict[str, Any], Any],
        properties: Optional[MessageProperties] = None,
        mandatory: bool = False,
        immediate: bool = False,
    ) -> bool:
        """
        Publish message to exchange.
        
        Args:
            exchange_name: Name of the exchange
            routing_key: Routing key for message
            message: Message to publish (will be JSON serialized if not string)
            properties: Message properties
            mandatory: Whether message must be routed to queue
            immediate: Whether message must be delivered immediately
            
        Returns:
            True if message was published successfully
        """
        try:
            self._ensure_connection()
            
            # Serialize message if needed
            if isinstance(message, str):
                body = message
            else:
                body = json.dumps(message, default=str, ensure_ascii=False)
            
            # Use default properties if none provided
            if properties is None:
                properties = MessageProperties(
                    timestamp=datetime.utcnow(),
                    message_id=self._generate_message_id(),
                )
            
            # Publish message
            self._channel.basic_publish(
                exchange=exchange_name,
                routing_key=routing_key,
                body=body.encode('utf-8'),
                properties=properties.to_pika_properties(),
                mandatory=mandatory,
                immediate=immediate,
            )
            
            logger.debug(f"Published message to exchange {exchange_name} with key {routing_key}")
            return True
            
        except AMQPChannelError as e:
            logger.error(f"Failed to publish message to {exchange_name}: {e}")
            raise MessageBrokerError(f"Message publishing failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error publishing message: {e}")
            raise MessageBrokerError(f"Message publishing failed: {e}")
    
    def publish_to_queue(
        self,
        queue_name: str,
        message: Union[str, Dict[str, Any], Any],
        properties: Optional[MessageProperties] = None,
        declare_queue: bool = True,
    ) -> bool:
        """
        Publish message directly to queue (using default exchange).
        
        Args:
            queue_name: Name of the queue
            message: Message to publish
            properties: Message properties
            declare_queue: Whether to declare queue if it doesn't exist
            
        Returns:
            True if message was published successfully
        """
        try:
            if declare_queue:
                self.declare_queue(queue_name)
            
            return self.publish_message(
                exchange_name="",  # Default exchange
                routing_key=queue_name,
                message=message,
                properties=properties,
            )
            
        except Exception as e:
            logger.error(f"Failed to publish message to queue {queue_name}: {e}")
            raise MessageBrokerError(f"Queue publishing failed: {e}")
    
    def publish_rpc_request(
        self,
        queue_name: str,
        message: Union[str, Dict[str, Any], Any],
        reply_to: str,
        correlation_id: str,
        expiration: Optional[str] = None,
    ) -> bool:
        """
        Publish RPC request message.
        
        Args:
            queue_name: Name of the request queue
            message: Request message
            reply_to: Queue name for reply
            correlation_id: Correlation ID for matching request/reply
            expiration: Message expiration time
            
        Returns:
            True if request was published successfully
        """
        try:
            properties = MessageProperties(
                reply_to=reply_to,
                correlation_id=correlation_id,
                expiration=expiration,
                timestamp=datetime.utcnow(),
                message_id=self._generate_message_id(),
            )
            
            # Add reply_to to headers since it's not a standard BasicProperties field
            if not properties.headers:
                properties.headers = {}
            properties.headers['reply_to'] = reply_to
            properties.headers['correlation_id'] = correlation_id
            
            return self.publish_to_queue(
                queue_name=queue_name,
                message=message,
                properties=properties,
            )
            
        except Exception as e:
            logger.error(f"Failed to publish RPC request to {queue_name}: {e}")
            raise MessageBrokerError(f"RPC request publishing failed: {e}")
    
    def publish_delayed_message(
        self,
        exchange_name: str,
        routing_key: str,
        message: Union[str, Dict[str, Any], Any],
        delay_seconds: int,
        properties: Optional[MessageProperties] = None,
    ) -> bool:
        """
        Publish delayed message using RabbitMQ delayed message plugin.
        
        Args:
            exchange_name: Name of the exchange (must be x-delayed-message type)
            routing_key: Routing key for message
            message: Message to publish
            delay_seconds: Delay in seconds
            properties: Message properties
            
        Returns:
            True if message was published successfully
        """
        try:
            if properties is None:
                properties = MessageProperties()
            
            # Set delay header
            if not properties.headers:
                properties.headers = {}
            properties.headers['x-delay'] = delay_seconds * 1000  # Convert to milliseconds
            
            return self.publish_message(
                exchange_name=exchange_name,
                routing_key=routing_key,
                message=message,
                properties=properties,
            )
            
        except Exception as e:
            logger.error(f"Failed to publish delayed message: {e}")
            raise MessageBrokerError(f"Delayed message publishing failed: {e}")
    
    def _generate_message_id(self) -> str:
        """Generate unique message ID."""
        import uuid
        return str(uuid.uuid4())
    
    def get_queue_info(self, queue_name: str) -> Dict[str, Any]:
        """
        Get information about a queue.
        
        Args:
            queue_name: Name of the queue
            
        Returns:
            Dictionary with queue information
        """
        try:
            self._ensure_connection()
            
            result = self._channel.queue_declare(
                queue=queue_name,
                passive=True,  # Only check if queue exists
            )
            
            return {
                "name": queue_name,
                "message_count": result.method.message_count,
                "consumer_count": result.method.consumer_count,
            }
            
        except AMQPChannelError as e:
            logger.error(f"Failed to get queue info for {queue_name}: {e}")
            raise MessageBrokerError(f"Queue info retrieval failed: {e}")
    
    def purge_queue(self, queue_name: str) -> int:
        """
        Purge all messages from a queue.
        
        Args:
            queue_name: Name of the queue to purge
            
        Returns:
            Number of messages purged
        """
        try:
            self._ensure_connection()
            
            result = self._channel.queue_purge(queue=queue_name)
            message_count = result.method.message_count
            
            logger.info(f"Purged {message_count} messages from queue {queue_name}")
            return message_count
            
        except AMQPChannelError as e:
            logger.error(f"Failed to purge queue {queue_name}: {e}")
            raise MessageBrokerError(f"Queue purging failed: {e}")
    
    def delete_queue(
        self,
        queue_name: str,
        if_unused: bool = False,
        if_empty: bool = False,
    ) -> int:
        """
        Delete a queue.
        
        Args:
            queue_name: Name of the queue to delete
            if_unused: Only delete if queue has no consumers
            if_empty: Only delete if queue has no messages
            
        Returns:
            Number of messages in queue when deleted
        """
        try:
            self._ensure_connection()
            
            result = self._channel.queue_delete(
                queue=queue_name,
                if_unused=if_unused,
                if_empty=if_empty,
            )
            
            message_count = result.method.message_count
            logger.info(f"Deleted queue {queue_name} with {message_count} messages")
            return message_count
            
        except AMQPChannelError as e:
            logger.error(f"Failed to delete queue {queue_name}: {e}")
            raise MessageBrokerError(f"Queue deletion failed: {e}")
    
    def is_connected(self) -> bool:
        """
        Check if connection is active.
        
        Returns:
            True if connected, False otherwise
        """
        try:
            return (
                self._connection is not None and
                not self._connection.is_closed and
                self._channel is not None and
                not self._channel.is_closed
            )
        except Exception:
            return False
    
    def close(self) -> None:
        """Close RabbitMQ connection."""
        try:
            if self._channel and not self._channel.is_closed:
                self._channel.close()
                logger.debug("RabbitMQ channel closed")
            
            if self._connection and not self._connection.is_closed:
                self._connection.close()
                logger.info("RabbitMQ connection closed")
                
        except Exception as e:
            logger.error(f"Error closing RabbitMQ connection: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Utility functions for common messaging patterns
def create_work_queue_setup(
    publisher: RabbitMQPublisher,
    queue_name: str,
    durable: bool = True,
) -> None:
    """
    Set up a work queue pattern.
    
    Args:
        publisher: RabbitMQ publisher instance
        queue_name: Name of the work queue
        durable: Whether queue should be durable
    """
    publisher.declare_queue(
        queue_name=queue_name,
        durable=durable,
        exclusive=False,
        auto_delete=False,
    )


def create_pub_sub_setup(
    publisher: RabbitMQPublisher,
    exchange_name: str,
    exchange_type: str = "fanout",
) -> None:
    """
    Set up a publish/subscribe pattern.
    
    Args:
        publisher: RabbitMQ publisher instance
        exchange_name: Name of the exchange
        exchange_type: Type of exchange (usually fanout for pub/sub)
    """
    publisher.declare_exchange(
        exchange_name=exchange_name,
        exchange_type=exchange_type,
        durable=True,
    )


def create_routing_setup(
    publisher: RabbitMQPublisher,
    exchange_name: str,
    queue_bindings: Dict[str, List[str]],
) -> None:
    """
    Set up a routing pattern.
    
    Args:
        publisher: RabbitMQ publisher instance
        exchange_name: Name of the exchange
        queue_bindings: Dict mapping queue names to lists of routing keys
    """
    publisher.declare_exchange(
        exchange_name=exchange_name,
        exchange_type="direct",
        durable=True,
    )
    
    for queue_name, routing_keys in queue_bindings.items():
        publisher.declare_queue(queue_name=queue_name, durable=True)
        
        for routing_key in routing_keys:
            publisher.bind_queue(
                queue_name=queue_name,
                exchange_name=exchange_name,
                routing_key=routing_key,
            )