"""
Event publisher for publishing events to Redis and handling event distribution.
"""

import json
import asyncio
from typing import Dict, Any, Optional, List
from uuid import UUID
from datetime import datetime
import redis.asyncio as redis

from app.models.events import Event, EventType, EventPriority
from app.core.config import get_settings
from app.utils.logger import get_logger

logger = get_logger(__name__)
settings = get_settings()


class EventPublisher:
    """
    Event publisher for publishing events to Redis pub/sub.
    Supports event filtering, priority-based routing, and webhook notifications.
    """
    
    def __init__(self, redis_url: Optional[str] = None):
        """
        Initialize event publisher.
        
        Args:
            redis_url: Redis connection URL (defaults to settings)
        """
        self.redis_url = redis_url or settings.CELERY_BROKER_URL
        self.redis_client: Optional[redis.Redis] = None
        self.logger = logger
        self._connected = False
    
    async def connect(self):
        """Establish Redis connection"""
        try:
            if not self._connected:
                self.redis_client = await redis.from_url(
                    self.redis_url,
                    encoding="utf-8",
                    decode_responses=True
                )
                self._connected = True
                self.logger.info("Event publisher connected to Redis")
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {str(e)}")
            raise
    
    async def disconnect(self):
        """Close Redis connection"""
        try:
            if self.redis_client and self._connected:
                await self.redis_client.close()
                self._connected = False
                self.logger.info("Event publisher disconnected from Redis")
        except Exception as e:
            self.logger.error(f"Error disconnecting from Redis: {str(e)}")
    
    async def publish(
        self,
        event_type: EventType,
        data: Dict[str, Any],
        source: str,
        priority: EventPriority = EventPriority.MEDIUM,
        metadata: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[UUID] = None
    ) -> Event:
        """
        Publish an event to Redis.
        
        Args:
            event_type: Type of event
            data: Event payload data
            source: Source of the event
            priority: Event priority
            metadata: Additional metadata
            correlation_id: Correlation ID for tracking
            
        Returns:
            Published Event instance
        """
        try:
            # Ensure connection
            if not self._connected:
                await self.connect()
            
            # Create event
            event = Event(
                event_type=event_type,
                source=source,
                priority=priority,
                data=data,
                metadata=metadata or {},
                correlation_id=correlation_id
            )
            
            # Publish to general channel
            channel = f"etl:events:{event_type.value}"
            await self.redis_client.publish(channel, event.model_dump_json())
            
            # Publish to priority channel if high/critical
            if priority in [EventPriority.HIGH, EventPriority.CRITICAL]:
                priority_channel = f"etl:events:priority:{priority.value}"
                await self.redis_client.publish(priority_channel, event.model_dump_json())
            
            # Store event in stream for history
            await self._store_event(event)
            
            self.logger.info(f"Published event: {event_type.value} from {source}")
            return event
            
        except Exception as e:
            self.logger.error(f"Failed to publish event: {str(e)}")
            # Don't raise - event publishing should not break main flow
            return None
    
    async def _store_event(self, event: Event):
        """Store event in Redis stream for history"""
        try:
            stream_key = f"etl:events:stream:{event.event_type.value}"
            await self.redis_client.xadd(
                stream_key,
                {
                    "event_id": str(event.event_id),
                    "data": event.model_dump_json()
                },
                maxlen=10000  # Keep last 10k events per type
            )
        except Exception as e:
            self.logger.warning(f"Failed to store event in stream: {str(e)}")
    
    # Convenience methods for common events
    
    async def publish_job_started(
        self,
        job_id: UUID,
        execution_id: UUID,
        job_name: str,
        **kwargs
    ) -> Event:
        """Publish job started event"""
        return await self.publish(
            event_type=EventType.JOB_STARTED,
            data={
                "job_id": str(job_id),
                "execution_id": str(execution_id),
                "job_name": job_name,
                **kwargs
            },
            source="etl_service",
            priority=EventPriority.MEDIUM
        )
    
    async def publish_job_completed(
        self,
        job_id: UUID,
        execution_id: UUID,
        job_name: str,
        stats: Dict[str, Any],
        **kwargs
    ) -> Event:
        """Publish job completed event"""
        return await self.publish(
            event_type=EventType.JOB_COMPLETED,
            data={
                "job_id": str(job_id),
                "execution_id": str(execution_id),
                "job_name": job_name,
                "stats": stats,
                **kwargs
            },
            source="etl_service",
            priority=EventPriority.MEDIUM
        )
    
    async def publish_job_failed(
        self,
        job_id: UUID,
        execution_id: UUID,
        job_name: str,
        error: str,
        **kwargs
    ) -> Event:
        """Publish job failed event"""
        return await self.publish(
            event_type=EventType.JOB_FAILED,
            data={
                "job_id": str(job_id),
                "execution_id": str(execution_id),
                "job_name": job_name,
                "error": error,
                **kwargs
            },
            source="etl_service",
            priority=EventPriority.HIGH
        )
    
    async def publish_file_uploaded(
        self,
        file_id: UUID,
        filename: str,
        file_size: int,
        **kwargs
    ) -> Event:
        """Publish file uploaded event"""
        return await self.publish(
            event_type=EventType.FILE_UPLOADED,
            data={
                "file_id": str(file_id),
                "filename": filename,
                "file_size": file_size,
                **kwargs
            },
            source="file_service",
            priority=EventPriority.LOW
        )
    
    async def publish_file_processed(
        self,
        file_id: UUID,
        filename: str,
        records_processed: int,
        **kwargs
    ) -> Event:
        """Publish file processed event"""
        return await self.publish(
            event_type=EventType.FILE_PROCESSED,
            data={
                "file_id": str(file_id),
                "filename": filename,
                "records_processed": records_processed,
                **kwargs
            },
            source="file_service",
            priority=EventPriority.MEDIUM
        )
    
    async def publish_dependency_met(
        self,
        job_id: UUID,
        dependency_id: UUID,
        **kwargs
    ) -> Event:
        """Publish dependency met event"""
        return await self.publish(
            event_type=EventType.DEPENDENCY_MET,
            data={
                "job_id": str(job_id),
                "dependency_id": str(dependency_id),
                **kwargs
            },
            source="dependency_service",
            priority=EventPriority.MEDIUM
        )
    
    async def publish_quality_check_failed(
        self,
        execution_id: UUID,
        check_name: str,
        reason: str,
        **kwargs
    ) -> Event:
        """Publish quality check failed event"""
        return await self.publish(
            event_type=EventType.QUALITY_CHECK_FAILED,
            data={
                "execution_id": str(execution_id),
                "check_name": check_name,
                "reason": reason,
                **kwargs
            },
            source="quality_service",
            priority=EventPriority.HIGH
        )
    
    async def publish_error(
        self,
        error_message: str,
        error_type: str,
        severity: str,
        context: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Event:
        """Publish error event"""
        return await self.publish(
            event_type=EventType.ERROR_OCCURRED,
            data={
                "error_message": error_message,
                "error_type": error_type,
                "severity": severity,
                "context": context or {},
                **kwargs
            },
            source="system",
            priority=EventPriority.HIGH if severity in ["HIGH", "CRITICAL"] else EventPriority.MEDIUM
        )
    
    async def publish_system_alert(
        self,
        alert_message: str,
        alert_type: str,
        severity: str = "MEDIUM",
        **kwargs
    ) -> Event:
        """Publish system alert event"""
        return await self.publish(
            event_type=EventType.SYSTEM_ALERT,
            data={
                "alert_message": alert_message,
                "alert_type": alert_type,
                "severity": severity,
                **kwargs
            },
            source="system",
            priority=EventPriority.HIGH if severity in ["HIGH", "CRITICAL"] else EventPriority.MEDIUM
        )


# Global event publisher instance
_event_publisher: Optional[EventPublisher] = None


async def get_event_publisher() -> EventPublisher:
    """Get or create global event publisher instance"""
    global _event_publisher
    if _event_publisher is None:
        _event_publisher = EventPublisher()
        await _event_publisher.connect()
    return _event_publisher


async def cleanup_event_publisher():
    """Cleanup global event publisher"""
    global _event_publisher
    if _event_publisher:
        await _event_publisher.disconnect()
        _event_publisher = None
