"""
RabbitMQ message consumer implementation.

This module provides RabbitMQ consumer functionality with
message handling, acknowledgment, and retry logic.
"""

import json
import logging
import signal
import threading
from typing import Any, Callable, Dict, Optional, Union
from datetime import datetime
from dataclasses import dataclass

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError, AMQPChannelError
from pika.spec import Basic, BasicProperties

from ...core.config import get_settings
from ...core.exceptions import MessageBrokerError

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class ConsumedMessage:
    """Wrapper for consumed message with metadata."""
    
    body: bytes
    properties: BasicProperties
    delivery_tag: int
    exchange: str
    routing_key: str
    redelivered: bool
    
    def get_json_body(self) -> Dict[str, Any]:
        """Get message body as JSON."""
        try:
            return json.loads(self.body.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ValueError(f"Failed to decode message body as JSON: {e}")
    
    def get_text_body(self) -> str:
        """Get message body as text."""
        try:
            return self.body.decode('utf-8')
        except UnicodeDecodeError as e:
            raise ValueError(f"Failed to decode message body as text: {e}")
    
    @property
    def message_id(self) -> Optional[str]:
        """Get message ID from properties."""
        return self.properties.message_id
    
    @property
    def correlation_id(self) -> Optional[str]:
        """Get correlation ID from properties."""
        return self.properties.correlation_id
    
    @property
    def reply_to(self) -> Optional[str]:
        """Get reply-to queue from properties."""
        return self.properties.reply_to
    
    @property
    def timestamp(self) -> Optional[datetime]:
        """Get timestamp from properties."""
        return self.properties.timestamp
    
    @property
    def headers(self) -> Dict[str, Any]:
        """Get headers from properties."""
        return self.properties.headers or {}


MessageHandler = Callable[[ConsumedMessage], bool]


class RabbitMQConsumer:
    """
    RabbitMQ message consumer with automatic acknowledgment and retry logic.
    
    Provides reliable message consumption with error handling,
    dead letter queues, and graceful shutdown.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5672,
        username: str = "guest",
        password: str = "guest",
        virtual_host: str = "/",
        connection_url: Optional[str] = None,
        prefetch_count: int = 1,
    ):
        """
        Initialize RabbitMQ consumer.
        
        Args:
            host: RabbitMQ host
            port: RabbitMQ port
            username: RabbitMQ username
            password: RabbitMQ password
            virtual_host: RabbitMQ virtual host
            connection_url: Full connection URL (overrides other params)
            prefetch_count: Number of unacknowledged messages per consumer
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.connection_url = connection_url
        self.prefetch_count = prefetch_count
        
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[BlockingChannel] = None
        self._consuming = False
        self._consumer_tags: Dict[str, str] = {}
        self._message_handlers: Dict[str, MessageHandler] = {}
        self._shutdown_event = threading.Event()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self._connect()
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.stop_consuming()
    
    def _connect(self) -> None:
        """Establish connection to RabbitMQ."""
        try:
            if self.connection_url:
                parameters = pika.URLParameters(self.connection_url)
            else:
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
            self._channel.basic_qos(prefetch_count=self.prefetch_count)
            
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
                self._channel.basic_qos(prefetch_count=self.prefetch_count)
        except Exception as e:
            logger.error(f"Failed to ensure RabbitMQ connection: {e}")
            raise MessageBrokerError(f"Connection check failed: {e}")
    
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
    
    def setup_dead_letter_queue(
        self,
        main_queue: str,
        dead_letter_exchange: str = "",
        dead_letter_routing_key: Optional[str] = None,
        message_ttl: Optional[int] = None,
        max_retries: Optional[int] = None,
    ) -> None:
        """
        Setup dead letter queue for failed messages.
        
        Args:
            main_queue: Main queue name
            dead_letter_exchange: Dead letter exchange name
            dead_letter_routing_key: Dead letter routing key
            message_ttl: Message TTL in milliseconds
            max_retries: Maximum retry attempts
        """
        try:
            dlq_name = f"{main_queue}.dlq"
            
            # Arguments for main queue
            queue_args = {}
            if dead_letter_exchange:
                queue_args["x-dead-letter-exchange"] = dead_letter_exchange
            if dead_letter_routing_key:
                queue_args["x-dead-letter-routing-key"] = dead_letter_routing_key
            else:
                queue_args["x-dead-letter-routing-key"] = dlq_name
            
            if message_ttl:
                queue_args["x-message-ttl"] = message_ttl
            if max_retries:
                queue_args["x-max-retries"] = max_retries
            
            # Declare main queue with DLQ settings
            self.declare_queue(main_queue, arguments=queue_args)
            
            # Declare dead letter queue
            self.declare_queue(dlq_name, durable=True)
            
            logger.info(f"Setup dead letter queue for {main_queue} -> {dlq_name}")
            
        except Exception as e:
            logger.error(f"Failed to setup dead letter queue: {e}")
            raise MessageBrokerError(f"DLQ setup failed: {e}")
    
    def add_message_handler(
        self,
        queue_name: str,
        handler: MessageHandler,
        auto_ack: bool = False,
        exclusive: bool = False,
    ) -> str:
        """
        Add message handler for a queue.
        
        Args:
            queue_name: Queue to consume from
            handler: Message handler function
            auto_ack: Whether to auto-acknowledge messages
            exclusive: Whether to consume exclusively
            
        Returns:
            Consumer tag
        """
        try:
            self._ensure_connection()
            
            # Store handler
            self._message_handlers[queue_name] = handler
            
            # Create callback wrapper
            def callback_wrapper(channel, method, properties, body):
                return self._handle_message(
                    channel, method, properties, body, handler, auto_ack
                )
            
            # Start consuming
            consumer_tag = self._channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback_wrapper,
                auto_ack=auto_ack,
                exclusive=exclusive,
            )
            
            self._consumer_tags[queue_name] = consumer_tag
            
            logger.info(f"Added message handler for queue {queue_name} (tag: {consumer_tag})")
            return consumer_tag
            
        except AMQPChannelError as e:
            logger.error(f"Failed to add message handler for {queue_name}: {e}")
            raise MessageBrokerError(f"Message handler setup failed: {e}")
    
    def _handle_message(
        self,
        channel: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
        handler: MessageHandler,
        auto_ack: bool,
    ) -> None:
        """
        Internal message handler wrapper.
        
        Args:
            channel: RabbitMQ channel
            method: Delivery method
            properties: Message properties
            body: Message body
            handler: User message handler
            auto_ack: Whether auto-acknowledgment is enabled
        """
        try:
            # Create message wrapper
            message = ConsumedMessage(
                body=body,
                properties=properties,
                delivery_tag=method.delivery_tag,
                exchange=method.exchange,
                routing_key=method.routing_key,
                redelivered=method.redelivered,
            )
            
            logger.debug(f"Processing message {properties.message_id} from {method.routing_key}")
            
            # Call user handler
            success = handler(message)
            
            if not auto_ack:
                if success:
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                    logger.debug(f"Message {properties.message_id} acknowledged")
                else:
                    channel.basic_nack(
                        delivery_tag=method.delivery_tag,
                        requeue=not method.redelivered  # Don't requeue if already redelivered
                    )
                    logger.warning(f"Message {properties.message_id} rejected")
            
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            
            if not auto_ack:
                try:
                    channel.basic_nack(
                        delivery_tag=method.delivery_tag,
                        requeue=False  # Send to DLQ on error
                    )
                except Exception as nack_error:
                    logger.error(f"Failed to nack message: {nack_error}")
    
    def start_consuming(self, timeout: Optional[float] = None) -> None:
        """
        Start consuming messages.
        
        Args:
            timeout: Timeout for consuming (None for indefinite)
        """
        try:
            if not self._consumer_tags:
                raise MessageBrokerError("No message handlers configured")
            
            self._consuming = True
            logger.info("Starting message consumption...")
            
            if timeout:
                # Consume with timeout
                self._connection.process_data_events(time_limit=timeout)
            else:
                # Consume indefinitely
                while self._consuming and not self._shutdown_event.is_set():
                    try:
                        self._connection.process_data_events(time_limit=1)
                    except KeyboardInterrupt:
                        logger.info("Received keyboard interrupt, stopping...")
                        break
            
            logger.info("Message consumption stopped")
            
        except Exception as e:
            logger.error(f"Error during message consumption: {e}")
            raise MessageBrokerError(f"Message consumption failed: {e}")
    
    def stop_consuming(self) -> None:
        """Stop consuming messages gracefully."""
        try:
            if self._consuming:
                self._consuming = False
                self._shutdown_event.set()
                
                # Cancel all consumers
                for queue_name, consumer_tag in self._consumer_tags.items():
                    try:
                        self._channel.basic_cancel(consumer_tag)
                        logger.info(f"Cancelled consumer for queue {queue_name}")
                    except Exception as e:
                        logger.error(f"Error cancelling consumer for {queue_name}: {e}")
                
                self._consumer_tags.clear()
                logger.info("Stopped message consumption")
            
        except Exception as e:
            logger.error(f"Error stopping message consumption: {e}")
    
    def publish_reply(
        self,
        reply_to: str,
        message: Union[str, Dict[str, Any]],
        correlation_id: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Publish reply message (for RPC pattern).
        
        Args:
            reply_to: Reply queue name
            message: Reply message
            correlation_id: Correlation ID from original request
            properties: Additional message properties
            
        Returns:
            True if reply was published successfully
        """
        try:
            self._ensure_connection()
            
            # Serialize message if needed
            if isinstance(message, str):
                body = message
            else:
                body = json.dumps(message, default=str, ensure_ascii=False)
            
            # Build properties
            reply_properties = pika.BasicProperties(
                correlation_id=correlation_id,
                timestamp=datetime.utcnow(),
                content_type="application/json",
                content_encoding="utf-8",
            )
            
            if properties:
                for key, value in properties.items():
                    setattr(reply_properties, key, value)
            
            # Publish reply
            self._channel.basic_publish(
                exchange="",  # Default exchange
                routing_key=reply_to,
                body=body.encode('utf-8'),
                properties=reply_properties,
            )
            
            logger.debug(f"Published reply to {reply_to} with correlation_id {correlation_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish reply: {e}")
            return False
    
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
            self.stop_consuming()
            
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


# Utility classes for common messaging patterns
class RPCServer:
    """RPC server using RabbitMQ."""
    
    def __init__(self, consumer: RabbitMQConsumer, request_queue: str):
        """
        Initialize RPC server.
        
        Args:
            consumer: RabbitMQ consumer instance
            request_queue: Queue for receiving RPC requests
        """
        self.consumer = consumer
        self.request_queue = request_queue
        self.request_handlers: Dict[str, Callable] = {}
        
        # Setup request handler
        consumer.add_message_handler(
            queue_name=request_queue,
            handler=self._handle_rpc_request,
            auto_ack=False,
        )
    
    def add_method(self, method_name: str, handler: Callable) -> None:
        """
        Add RPC method handler.
        
        Args:
            method_name: Name of the RPC method
            handler: Function to handle the method call
        """
        self.request_handlers[method_name] = handler
        logger.info(f"Added RPC method: {method_name}")
    
    def _handle_rpc_request(self, message: ConsumedMessage) -> bool:
        """Handle RPC request message."""
        try:
            request_data = message.get_json_body()
            method_name = request_data.get("method")
            params = request_data.get("params", {})
            
            if method_name not in self.request_handlers:
                error_response = {
                    "error": f"Unknown method: {method_name}",
                    "code": -32601
                }
            else:
                try:
                    result = self.request_handlers[method_name](**params)
                    error_response = {"result": result}
                except Exception as e:
                    error_response = {
                        "error": str(e),
                        "code": -32603
                    }
            
            # Send reply if reply_to is specified
            if message.reply_to:
                self.consumer.publish_reply(
                    reply_to=message.reply_to,
                    message=error_response,
                    correlation_id=message.correlation_id,
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Error handling RPC request: {e}")
            return False
    
    def start(self) -> None:
        """Start RPC server."""
        logger.info(f"Starting RPC server on queue {self.request_queue}")
        self.consumer.start_consuming()