"""
Complete Server-Sent Events (SSE) messaging implementation.

This module provides SSE messaging for one-way server-to-client communication
with features like topic subscriptions, connection management, event streaming,
and automatic reconnection support.
"""

import asyncio
import logging
import json
import time
from typing import Any, Optional, Dict, List, AsyncGenerator, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import defaultdict
from contextlib import asynccontextmanager

from fastapi import Request, HTTPException
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask

from .base import (
    Message,
    MessageInterface,
    MessageHandler,
    MessageFilter,
    MessageStatus,
)

logger = logging.getLogger(__name__)


@dataclass
class SSEConnection:
    """
    Enhanced SSE connection wrapper with comprehensive features.
    """
    client_id: str
    request: Request
    connected_at: datetime
    subscriptions: Set[str] = field(default_factory=set)
    queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=100))
    is_active: bool = True
    last_ping: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    user_agent: Optional[str] = None
    ip_address: Optional[str] = None
    
    def __post_init__(self):
        """Initialize connection metadata."""
        if self.request:
            self.user_agent = self.request.headers.get("user-agent")
            self.ip_address = self.request.client.host if self.request.client else None
        self.last_ping = datetime.utcnow()
    
    async def send_event(
        self, 
        event_type: str, 
        data: Any, 
        event_id: Optional[str] = None,
        retry: Optional[int] = None,
        comment: Optional[str] = None
    ) -> bool:
        """
        Send SSE event to client.
        
        Args:
            event_type: Event type/name
            data: Event data
            event_id: Optional event ID for client tracking
            retry: Retry timeout in milliseconds
            comment: Optional comment
            
        Returns:
            True if event was queued successfully
        """
        try:
            if not self.is_active:
                return False
            
            event = {
                "event": event_type,
                "data": data,
                "id": event_id,
                "retry": retry,
                "comment": comment,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Try to put event in queue (non-blocking)
            try:
                self.queue.put_nowait(event)
                return True
            except asyncio.QueueFull:
                logger.warning(f"SSE queue full for client {self.client_id}, dropping event")
                return False
                
        except Exception as e:
            logger.error(f"Failed to queue SSE event for {self.client_id}: {e}")
            self.is_active = False
            return False
    
    async def send_ping(self) -> bool:
        """Send heartbeat ping to client."""
        self.last_ping = datetime.utcnow()
        return await self.send_event("ping", {"timestamp": self.last_ping.isoformat()})
    
    async def send_message(self, message: Message) -> bool:
        """Send Message object as SSE event."""
        return await self.send_event(
            event_type=message.topic,
            data=message.to_dict(),
            event_id=message.id,
            retry=5000 if message.priority.value >= 3 else None
        )
    
    def close(self) -> None:
        """Mark connection as closed."""
        self.is_active = False
        logger.debug(f"SSE connection {self.client_id} marked as closed")
    
    def is_expired(self, timeout_seconds: int = 300) -> bool:
        """Check if connection has expired based on last ping."""
        if not self.last_ping:
            return True
        
        return (datetime.utcnow() - self.last_ping).total_seconds() > timeout_seconds


class SSEMessaging(MessageInterface):
    """
    Complete Server-Sent Events messaging implementation.
    
    Features:
    - One-way server-to-client communication
    - Topic-based subscriptions
    - Connection management with automatic cleanup
    - Event streaming with proper SSE formatting
    - Heartbeat/ping support
    - Client metadata tracking
    - Queue management per connection
    - Graceful error handling
    - Connection timeout handling
    """
    
    def __init__(
        self,
        max_queue_size: int = 100,
        ping_interval: int = 30,
        connection_timeout: int = 300,
        max_connections: int = 1000,
        enable_cors: bool = True,
    ):
        """
        Initialize SSE messaging.
        
        Args:
            max_queue_size: Maximum events per client queue
            ping_interval: Heartbeat interval in seconds
            connection_timeout: Connection timeout in seconds
            max_connections: Maximum concurrent connections
            enable_cors: Enable CORS headers
        """
        self.max_queue_size = max_queue_size
        self.ping_interval = ping_interval
        self.connection_timeout = connection_timeout
        self.max_connections = max_connections
        self.enable_cors = enable_cors
        
        # Connection management
        self._connections: Dict[str, SSEConnection] = {}
        self._topic_subscriptions: Dict[str, Set[str]] = defaultdict(set)
        self._handlers: Dict[str, MessageHandler] = {}
        
        # Background tasks
        self._is_consuming = False
        self._cleanup_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        
        # Statistics
        self._stats = {
            "connections_created": 0,
            "connections_closed": 0,
            "events_sent": 0,
            "events_dropped": 0,
            "bytes_sent": 0,
        }
        
        logger.info(f"SSE messaging initialized with max_connections={max_connections}")
    
    async def create_connection(
        self,
        request: Request,
        client_id: str,
        topics: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        custom_headers: Optional[Dict[str, str]] = None,
    ) -> StreamingResponse:
        """
        Create SSE connection for a client.
        
        Args:
            request: FastAPI request object
            client_id: Unique client identifier
            topics: List of topics to subscribe to initially
            metadata: Optional client metadata
            custom_headers: Custom response headers
            
        Returns:
            StreamingResponse for SSE
        """
        # Check connection limit
        if len(self._connections) >= self.max_connections:
            logger.warning(f"SSE connection limit reached ({self.max_connections})")
            raise HTTPException(
                status_code=503, 
                detail="Server busy - too many SSE connections"
            )
        
        # Disconnect existing connection for same client
        if client_id in self._connections:
            await self.disconnect_client(client_id)
        
        # Create new connection
        connection = SSEConnection(
            client_id=client_id,
            request=request,
            connected_at=datetime.utcnow(),
            queue=asyncio.Queue(maxsize=self.max_queue_size),
            metadata=metadata or {}
        )
        
        self._connections[client_id] = connection
        self._stats["connections_created"] += 1
        
        # Subscribe to initial topics
        if topics:
            for topic in topics:
                await self.subscribe_client(client_id, topic)
        
        # Send initial connection event
        await connection.send_event(
            event_type="connected",
            data={
                "client_id": client_id,
                "server_time": datetime.utcnow().isoformat(),
                "subscriptions": list(connection.subscriptions),
                "ping_interval": self.ping_interval,
            },
            event_id=f"conn_{client_id}_{int(time.time())}"
        )
        
        logger.info(f"SSE client {client_id} connected from {connection.ip_address}")
        
        # Prepare headers
        headers = {
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "text/event-stream",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        }
        
        # Add CORS headers if enabled
        if self.enable_cors:
            headers.update({
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": "true",
                "Access-Control-Allow-Headers": "Cache-Control",
            })
        
        # Add custom headers
        if custom_headers:
            headers.update(custom_headers)
        
        # Create event stream
        async def event_stream() -> AsyncGenerator[str, None]:
            try:
                while connection.is_active:
                    try:
                        # Wait for event with timeout for heartbeat
                        event = await asyncio.wait_for(
                            connection.queue.get(), 
                            timeout=self.ping_interval
                        )
                        
                        # Format and yield SSE event
                        sse_data = self._format_sse_event(event)
                        self._stats["events_sent"] += 1
                        self._stats["bytes_sent"] += len(sse_data.encode())
                        
                        yield sse_data
                        
                    except asyncio.TimeoutError:
                        # Send heartbeat if no events
                        if connection.is_active:
                            ping_event = {
                                "event": "ping",
                                "data": {"timestamp": datetime.utcnow().isoformat()},
                                "comment": "heartbeat"
                            }
                            yield self._format_sse_event(ping_event)
                            connection.last_ping = datetime.utcnow()
                        
            except asyncio.CancelledError:
                logger.debug(f"SSE stream cancelled for {client_id}")
            except Exception as e:
                logger.error(f"SSE stream error for {client_id}: {e}")
            finally:
                # Ensure cleanup
                await self.disconnect_client(client_id)
        
        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers=headers,
            background=BackgroundTask(self.disconnect_client, client_id)
        )
    
    def _format_sse_event(self, event: Dict[str, Any]) -> str:
        """
        Format event data as SSE.
        
        Args:
            event: Event dictionary
            
        Returns:
            Formatted SSE string
        """
        lines = []
        
        # Add comment if present
        if event.get("comment"):
            lines.append(f": {event['comment']}")
        
        # Add event type
        if event.get("event"):
            lines.append(f"event: {event['event']}")
        
        # Add event ID
        if event.get("id"):
            lines.append(f"id: {event['id']}")
        
        # Add retry timeout
        if event.get("retry"):
            lines.append(f"retry: {event['retry']}")
        
        # Add data (can be multi-line)
        if event.get("data") is not None:
            data = event["data"]
            if isinstance(data, (dict, list)):
                data = json.dumps(data, default=str, separators=(',', ':'))
            elif not isinstance(data, str):
                data = str(data)
            
            # Handle multi-line data
            for line in data.split('\n'):
                lines.append(f"data: {line}")
        
        # Add empty line to complete event
        lines.append("")
        lines.append("")
        
        return '\n'.join(lines)
    
    async def disconnect_client(self, client_id: str) -> bool:
        """
        Disconnect SSE client and cleanup resources.
        
        Args:
            client_id: Client ID to disconnect
            
        Returns:
            True if client was disconnected
        """
        if client_id not in self._connections:
            return False
        
        connection = self._connections.pop(client_id)
        connection.close()
        
        # Remove from all topic subscriptions
        for topic in list(connection.subscriptions):
            self._topic_subscriptions[topic].discard(client_id)
            if not self._topic_subscriptions[topic]:
                del self._topic_subscriptions[topic]
        
        # Update stats
        self._stats["connections_closed"] += 1
        
        # Calculate connection duration
        duration = datetime.utcnow() - connection.connected_at
        
        logger.info(
            f"SSE client {client_id} disconnected "
            f"(duration: {duration.total_seconds():.1f}s, "
            f"events: {connection.queue.qsize()} pending)"
        )
        
        return True
    
    async def publish(
        self,
        topic: str,
        payload: Any,
        headers: Optional[Dict[str, Any]] = None,
        event_type: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Publish message to SSE topic.
        
        Args:
            topic: Topic to publish to
            payload: Message payload
            headers: Optional headers
            event_type: Custom event type (defaults to topic)
            **kwargs: Additional message options
            
        Returns:
            Message ID
        """
        message = Message(
            topic=topic,
            payload=payload,
            headers=headers or {},
            **kwargs
        )
        
        await self.publish_message(message)
        return message.id
    
    async def publish_message(self, message: Message) -> bool:
        """
        Publish message object to SSE subscribers.
        
        Args:
            message: Message to publish
            
        Returns:
            True if message was sent to at least one subscriber
        """
        try:
            subscribers = self._topic_subscriptions.get(message.topic, set())
            
            if not subscribers:
                logger.debug(f"No SSE subscribers for topic {message.topic}")
                return True
            
            # Send to all subscribers
            success_count = 0
            failed_clients = []
            
            for client_id in subscribers.copy():
                if client_id in self._connections:
                    connection = self._connections[client_id]
                    
                    if await connection.send_message(message):
                        success_count += 1
                    else:
                        failed_clients.append(client_id)
                        self._stats["events_dropped"] += 1
                else:
                    # Remove stale subscription
                    failed_clients.append(client_id)
            
            # Clean up failed connections
            for client_id in failed_clients:
                await self.disconnect_client(client_id)
            
            logger.debug(
                f"SSE message sent to {success_count}/{len(subscribers)} subscribers "
                f"(topic: {message.topic}, failed: {len(failed_clients)})"
            )
            
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Failed to publish SSE message: {e}")
            return False
    
    async def subscribe(
        self,
        topic: str,
        handler: MessageHandler,
        filters: Optional[List[MessageFilter]] = None
    ) -> str:
        """
        Subscribe to SSE topic (for internal handlers).
        
        Args:
            topic: Topic to subscribe to
            handler: Message handler function
            filters: Optional message filters
            
        Returns:
            Subscription ID
        """
        subscription_id = f"sse_handler_{topic}_{len(self._handlers)}"
        self._handlers[subscription_id] = handler
        
        logger.info(f"Registered SSE handler for topic {topic}")
        return subscription_id
    
    async def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe from SSE topic.
        
        Args:
            subscription_id: Subscription ID to remove
            
        Returns:
            True if unsubscribed successfully
        """
        if subscription_id in self._handlers:
            del self._handlers[subscription_id]
            logger.info(f"Unsubscribed SSE handler {subscription_id}")
            return True
        return False
    
    async def subscribe_client(self, client_id: str, topic: str) -> bool:
        """
        Subscribe a client to a topic.
        
        Args:
            client_id: Client ID
            topic: Topic to subscribe to
            
        Returns:
            True if subscribed successfully
        """
        if client_id not in self._connections:
            logger.warning(f"Cannot subscribe unknown client {client_id} to {topic}")
            return False
        
        connection = self._connections[client_id]
        connection.subscriptions.add(topic)
        self._topic_subscriptions[topic].add(client_id)
        
        # Notify client of subscription
        await connection.send_event(
            event_type="subscribed",
            data={"topic": topic, "timestamp": datetime.utcnow().isoformat()}
        )
        
        logger.debug(f"SSE client {client_id} subscribed to topic {topic}")
        return True
    
    async def unsubscribe_client(self, client_id: str, topic: str) -> bool:
        """
        Unsubscribe a client from a topic.
        
        Args:
            client_id: Client ID
            topic: Topic to unsubscribe from
            
        Returns:
            True if unsubscribed successfully
        """
        if client_id not in self._connections:
            return False
        
        connection = self._connections[client_id]
        connection.subscriptions.discard(topic)
        self._topic_subscriptions[topic].discard(client_id)
        
        # Clean up empty topic
        if not self._topic_subscriptions[topic]:
            del self._topic_subscriptions[topic]
        
        # Notify client of unsubscription
        await connection.send_event(
            event_type="unsubscribed",
            data={"topic": topic, "timestamp": datetime.utcnow().isoformat()}
        )
        
        logger.debug(f"SSE client {client_id} unsubscribed from topic {topic}")
        return True
    
    async def send_to_client(
        self,
        client_id: str,
        event_type: str,
        data: Any,
        event_id: Optional[str] = None,
        retry: Optional[int] = None
    ) -> bool:
        """
        Send event directly to a specific client.
        
        Args:
            client_id: Target client ID
            event_type: Event type
            data: Event data
            event_id: Optional event ID
            retry: Optional retry timeout
            
        Returns:
            True if event was sent successfully
        """
        if client_id not in self._connections:
            logger.warning(f"Cannot send to unknown SSE client {client_id}")
            return False
        
        connection = self._connections[client_id]
        return await connection.send_event(event_type, data, event_id, retry)
    
    async def broadcast(
        self,
        event_type: str,
        data: Any,
        exclude_clients: Optional[List[str]] = None,
        only_clients: Optional[List[str]] = None
    ) -> int:
        """
        Broadcast event to all or specified clients.
        
        Args:
            event_type: Event type
            data: Event data  
            exclude_clients: Clients to exclude
            only_clients: Send only to these clients
            
        Returns:
            Number of clients that received the event
        """
        exclude_set = set(exclude_clients or [])
        target_clients = set(only_clients) if only_clients else set(self._connections.keys())
        target_clients -= exclude_set
        
        success_count = 0
        failed_clients = []
        
        for client_id in target_clients:
            if client_id in self._connections:
                connection = self._connections[client_id]
                if await connection.send_event(event_type, data):
                    success_count += 1
                else:
                    failed_clients.append(client_id)
            else:
                failed_clients.append(client_id)
        
        # Clean up failed connections
        for client_id in failed_clients:
            await self.disconnect_client(client_id)
        
        logger.debug(f"SSE broadcast sent to {success_count}/{len(target_clients)} clients")
        return success_count
    
    async def start_consuming(self) -> None:
        """Start SSE messaging background tasks."""
        if self._is_consuming:
            return
        
        self._is_consuming = True
        
        # Start cleanup task
        self._cleanup_task = asyncio.create_task(self._cleanup_connections())
        
        # Start ping task
        self._ping_task = asyncio.create_task(self._ping_connections())
        
        logger.info("SSE messaging started")
    
    async def stop_consuming(self) -> None:
        """Stop SSE messaging and cleanup."""
        if not self._is_consuming:
            return
        
        self._is_consuming = False
        
        # Cancel background tasks
        for task in [self._cleanup_task, self._ping_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        self._cleanup_task = None
        self._ping_task = None
        
        # Disconnect all clients
        client_ids = list(self._connections.keys())
        for client_id in client_ids:
            await self.disconnect_client(client_id)
        
        logger.info("SSE messaging stopped")
    
    async def _cleanup_connections(self) -> None:
        """Background task to cleanup expired connections."""
        while self._is_consuming:
            try:
                await asyncio.sleep(60)  # Cleanup every minute
                
                if not self._is_consuming:
                    break
                
                expired_clients = []
                current_time = datetime.utcnow()
                
                for client_id, connection in self._connections.items():
                    # Check if connection has expired
                    if (not connection.is_active or 
                        connection.is_expired(self.connection_timeout)):
                        expired_clients.append(client_id)
                    
                    # Check queue health
                    elif connection.queue.qsize() >= self.max_queue_size * 0.9:
                        logger.warning(f"SSE client {client_id} queue nearly full")
                
                # Remove expired connections
                for client_id in expired_clients:
                    logger.info(f"Cleaning up expired SSE connection: {client_id}")
                    await self.disconnect_client(client_id)
                
                if expired_clients:
                    logger.info(f"Cleaned up {len(expired_clients)} expired SSE connections")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in SSE cleanup: {e}")
    
    async def _ping_connections(self) -> None:
        """Background task to send pings to connections."""
        while self._is_consuming:
            try:
                await asyncio.sleep(self.ping_interval)
                
                if not self._is_consuming:
                    break
                
                # Send pings to all active connections
                ping_tasks = []
                for connection in self._connections.values():
                    if connection.is_active:
                        ping_tasks.append(connection.send_ping())
                
                if ping_tasks:
                    # Execute pings concurrently
                    await asyncio.gather(*ping_tasks, return_exceptions=True)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in SSE ping: {e}")
    
    def get_connection_count(self) -> int:
        """Get number of active connections."""
        return len(self._connections)
    
    def get_topic_subscribers(self, topic: str) -> List[str]:
        """Get list of subscribers for a topic."""
        return list(self._topic_subscriptions.get(topic, set()))
    
    def get_client_info(self, client_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific client."""
        if client_id not in self._connections:
            return None
        
        connection = self._connections[client_id]
        return {
            "client_id": client_id,
            "connected_at": connection.connected_at.isoformat(),
            "subscriptions": list(connection.subscriptions),
            "is_active": connection.is_active,
            "queue_size": connection.queue.qsize(),
            "last_ping": connection.last_ping.isoformat() if connection.last_ping else None,
            "metadata": connection.metadata,
            "user_agent": connection.user_agent,
            "ip_address": connection.ip_address,
        }
    
    def get_all_clients_info(self) -> List[Dict[str, Any]]:
        """Get information about all connected clients."""
        return [
            self.get_client_info(client_id) 
            for client_id in self._connections.keys()
        ]
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get SSE messaging statistics."""
        active_connections = len(self._connections)
        total_queue_size = sum(conn.queue.qsize() for conn in self._connections.values())
        
        return {
            **self._stats,
            "active_connections": active_connections,
            "total_topics": len(self._topic_subscriptions),
            "total_queue_size": total_queue_size,
            "average_queue_size": total_queue_size / active_connections if active_connections > 0 else 0,
            "is_consuming": self._is_consuming,
            "ping_interval": self.ping_interval,
            "connection_timeout": self.connection_timeout,
            "max_connections": self.max_connections,
        }
    
    async def health_check(self) -> bool:
        """Check SSE messaging health."""
        try:
            # Basic health check - verify we can create and cleanup resources
            test_connection_count = len(self._connections)
            
            # Check if background tasks are running
            tasks_healthy = (
                self._is_consuming and
                (not self._cleanup_task or not self._cleanup_task.done()) and
                (not self._ping_task or not self._ping_task.done())
            )
            
            return tasks_healthy and test_connection_count <= self.max_connections
            
        except Exception as e:
            logger.error(f"SSE health check failed: {e}")
            return False


class SSEManager:
    """SSE manager for application lifecycle."""
    
    def __init__(self, **config):
        """
        Initialize SSE manager.
        
        Args:
            **config: SSE configuration options
        """
        self._messaging = SSEMessaging(**config)
    
    async def startup(self) -> None:
        """Initialize SSE messaging."""
        await self._messaging.start_consuming()
        logger.info("SSE manager started")
    
    async def shutdown(self) -> None:
        """Shutdown SSE messaging."""
        await self._messaging.stop_consuming()
        logger.info("SSE manager shutdown")
    
    def get_messaging(self) -> SSEMessaging:
        """Get SSE messaging instance."""
        return self._messaging


# Global SSE manager
sse_manager = SSEManager(
    max_queue_size=100,
    ping_interval=30,
    connection_timeout=300,
    max_connections=1000,
    enable_cors=True
)