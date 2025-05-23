import pika
import logging
from typing import Optional
import aio_pika
from aio_pika.abc import AbstractIncomingMessage

logger = logging.getLogger(__name__)

from abc import ABC, abstractmethod
from typing import Callable, Optional, Awaitable, Union

MessageHandler = Union[Callable[[bytes], None], Callable[[bytes], Awaitable[None]]]

class IRabbitMQBroker(ABC):
    @abstractmethod
    def connect(self) -> None:
        """Connect to RabbitMQ broker (sync or async)."""
        pass

    @abstractmethod
    def declare_queue(self, queue_name: str, durable: bool = True) -> None:
        """Declare queue (sync or async)."""
        pass

    @abstractmethod
    def publish(self, queue_name: str, message: str) -> None:
        """Publish message (sync or async)."""
        pass

    @abstractmethod
    def consume(self, queue_name: str, handler: MessageHandler) -> None:
        """Consume messages with handler (sync or async)."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close connection (sync or async)."""
        pass


class SyncRabbitMQBroker(IRabbitMQBroker):
    def __init__(self, amqp_url: str):
        self.amqp_url = amqp_url
        self.connection: pika.BlockingConnection = None
        self.channel: pika.channel.Channel = None

    def connect(self) -> None:
        parameters = pika.URLParameters(self.amqp_url)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        logging.info("Connected to RabbitMQ (sync)")

    def declare_queue(self, queue_name: str, durable: bool = True) -> None:
        self.channel.queue_declare(queue=queue_name, durable=durable)
        logging.info(f"Declared queue '{queue_name}' (sync)")

    def publish(self, queue_name: str, message: str) -> None:
        self.channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        logging.info(f"Published message to queue '{queue_name}' (sync)")

    def consume(self, queue_name: str, handler: Callable[[bytes], None]) -> None:
        def callback(ch, method, properties, body):
            handler(body)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(queue=queue_name, on_message_callback=callback)
        logging.info(f"Started consuming queue '{queue_name}' (sync)")
        self.channel.start_consuming()

    def close(self) -> None:
        if self.connection and self.connection.is_open:
            self.connection.close()
            logging.info("RabbitMQ connection closed (sync)")


class AsyncRabbitMQBroker(IRabbitMQBroker):
    def __init__(self, amqp_url: str):
        self.amqp_url = amqp_url
        self.connection: aio_pika.RobustConnection = None
        self.channel: aio_pika.RobustChannel = None

    async def connect(self) -> None:
        if self.connection and not self.connection.is_closed:
            return

        try:
            self.connection = await aio_pika.connect_robust(self.amqp_url)
            self.channel = await self.connection.channel()
            logging.info("Connected to RabbitMQ (async)")
        except Exception as e:
            logging.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    async def declare_queue(self, queue_name: str, durable: bool = True) -> None:
        if not self.channel:
            raise RuntimeError("Channel is not established. Call connect() first.")
        await self.channel.declare_queue(queue_name, durable=durable)
        logging.info(f"Declared queue '{queue_name}' (async)")

    async def publish(self, queue_name: str, message: str) -> None:
        if not self.channel:
            raise RuntimeError("Channel is not established. Call connect() first.")
        await self.channel.default_exchange.publish(
            aio_pika.Message(body=message.encode()),
            routing_key=queue_name,
        )
        logging.info(f"Published message to queue '{queue_name}' (async)")

    async def consume(self, queue_name: str, handler: Callable[[aio_pika.abc.AbstractIncomingMessage], Awaitable[None]]) -> None:
        if not self.channel:
            raise RuntimeError("Channel is not established. Call connect() first.")
        queue = await self.channel.declare_queue(queue_name, durable=True)
        await queue.consume(handler)
        logging.info(f"Started consuming queue '{queue_name}' (async)")

    async def close(self) -> None:
        if self.connection:
            await self.connection.close()
            logging.info("RabbitMQ connection closed (async)")


class RabbitMQConnection:
    """
    Utility class to manage connection to RabbitMQ server.
    """

    def __init__(self, amqp_url: str = "amqp://guest:guest@localhost/"):
        self.amqp_url = amqp_url
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.abc.AbstractRobustChannel] = None

    async def connect(self):
        if self.connection and not self.connection.is_closed:
            return

        try:
            self.connection = await aio_pika.connect_robust(self.amqp_url)
            self.channel = await self.connection.channel()
            logger.info("Connected to RabbitMQ (async)")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    async def declare_queue(self, name: str, durable: bool = True) -> aio_pika.abc.AbstractQueue:
        if not self.channel:
            raise RuntimeError("Channel is not established. Call connect() first.")
        queue = await self.channel.declare_queue(name, durable=durable)
        logger.debug(f"Queue declared: {name}")
        return queue

    async def publish(self, queue_name: str, message: str):
        if not self.channel:
            raise RuntimeError("Channel is not established. Call connect() first.")
        await self.channel.default_exchange.publish(
            aio_pika.Message(body=message.encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=queue_name,
        )
        logger.info(f"Published message to queue '{queue_name}'")

    async def consume(self, queue_name: str, callback):
        """
        Consume messages with a user-defined async callback.

        Example:
        async def handler(msg: AbstractIncomingMessage):
            async with msg.process():
                print(msg.body.decode())
        """
        if not self.channel:
            raise RuntimeError("Channel is not established. Call connect() first.")

        queue = await self.declare_queue(queue_name)
        await queue.consume(callback)
        logger.info(f"Consuming from queue '{queue_name}'")

    async def close(self):
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.info("RabbitMQ connection closed (async)")   

