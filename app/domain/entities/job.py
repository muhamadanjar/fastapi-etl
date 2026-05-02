from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from uuid import UUID
from datetime import datetime
from enum import Enum


class JobType(str, Enum):
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"
    FULL_ETL = "full_etl"


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"


@dataclass
class Job:
    id: UUID
    job_name: str
    job_type: JobType
    source_type: str
    status: JobStatus = JobStatus.PENDING
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    transformation_rules: Dict[str, Any] = field(default_factory=dict)
    field_mappings: Dict[str, Any] = field(default_factory=dict)
    config: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

    def mark_as_running(self) -> None:
        self.status = JobStatus.RUNNING
        self.updated_at = datetime.utcnow()

    def mark_as_completed(self) -> None:
        self.status = JobStatus.COMPLETED
        self.updated_at = datetime.utcnow()
        self.error_message = None

    def mark_as_failed(self, error: str) -> None:
        self.status = JobStatus.FAILED
        self.updated_at = datetime.utcnow()
        self.error_message = error

    def mark_as_paused(self) -> None:
        self.status = JobStatus.PAUSED
        self.updated_at = datetime.utcnow()
