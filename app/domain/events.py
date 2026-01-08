"""
Event models for ETL system event publishing.
"""

from enum import Enum
from typing import Dict, Any, Optional
from uuid import UUID, uuid4
from datetime import datetime
from pydantic import BaseModel, Field


class EventType(str, Enum):
    """Types of events that can be published"""
    
    # Job events
    JOB_CREATED = "job.created"
    JOB_STARTED = "job.started"
    JOB_COMPLETED = "job.completed"
    JOB_FAILED = "job.failed"
    JOB_CANCELLED = "job.cancelled"
    
    # File events
    FILE_UPLOADED = "file.uploaded"
    FILE_PROCESSING_STARTED = "file.processing.started"
    FILE_PROCESSED = "file.processed"
    FILE_PROCESSING_FAILED = "file.processing.failed"
    
    # Dependency events
    DEPENDENCY_ADDED = "dependency.added"
    DEPENDENCY_REMOVED = "dependency.removed"
    DEPENDENCY_MET = "dependency.met"
    DEPENDENCY_TRIGGERED = "dependency.triggered"
    
    # Quality events
    QUALITY_CHECK_STARTED = "quality.check.started"
    QUALITY_CHECK_PASSED = "quality.check.passed"
    QUALITY_CHECK_FAILED = "quality.check.failed"
    
    # Error events
    ERROR_OCCURRED = "error.occurred"
    ERROR_RESOLVED = "error.resolved"
    
    # System events
    SYSTEM_HEALTH_CHECK = "system.health.check"
    SYSTEM_ALERT = "system.alert"
    SYSTEM_WARNING = "system.warning"


class EventPriority(str, Enum):
    """Event priority levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class Event(BaseModel):
    """Base event model"""
    
    event_id: UUID = Field(default_factory=uuid4, description="Unique event ID")
    event_type: EventType = Field(..., description="Type of event")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Event timestamp")
    source: str = Field(..., description="Source of the event (service/task name)")
    priority: EventPriority = Field(default=EventPriority.MEDIUM, description="Event priority")
    data: Dict[str, Any] = Field(default_factory=dict, description="Event payload data")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Additional metadata")
    correlation_id: Optional[UUID] = Field(default=None, description="Correlation ID for tracking")
    
    class Config:
        json_schema_extra = {
            "example": {
                "event_id": "550e8400-e29b-41d4-a716-446655440000",
                "event_type": "job.completed",
                "timestamp": "2025-11-23T12:00:00Z",
                "source": "etl_service",
                "priority": "medium",
                "data": {
                    "job_id": "550e8400-e29b-41d4-a716-446655440001",
                    "execution_id": "550e8400-e29b-41d4-a716-446655440002",
                    "records_processed": 1000,
                    "duration_seconds": 120
                },
                "metadata": {
                    "user_id": "user123",
                    "environment": "production"
                }
            }
        }


class JobEvent(Event):
    """Job-specific event"""
    
    def __init__(self, **data):
        super().__init__(**data)
        # Ensure job_id is in data
        if 'job_id' not in self.data:
            raise ValueError("job_id is required in data for JobEvent")


class FileEvent(Event):
    """File-specific event"""
    
    def __init__(self, **data):
        super().__init__(**data)
        # Ensure file_id is in data
        if 'file_id' not in self.data:
            raise ValueError("file_id is required in data for FileEvent")


class ErrorEvent(Event):
    """Error-specific event"""
    
    def __init__(self, **data):
        super().__init__(**data)
        self.priority = EventPriority.HIGH
        # Ensure error details are in data
        if 'error_message' not in self.data:
            raise ValueError("error_message is required in data for ErrorEvent")


class SystemEvent(Event):
    """System-specific event"""
    
    def __init__(self, **data):
        super().__init__(**data)
        self.source = "system"
