from typing import Optional, Dict, Any, List
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field
from enum import Enum
from .base import BaseResponse


class JobType(str, Enum):
    """ETL job types."""
    EXTRACT = "EXTRACT"
    TRANSFORM = "TRANSFORM"
    LOAD = "LOAD"
    VALIDATE = "VALIDATE"
    AGGREGATE = "AGGREGATE"
    CLEANUP = "CLEANUP"


class JobCategory(str, Enum):
    """ETL job categories."""
    FILE_PROCESSING = "FILE_PROCESSING"
    DATA_CLEANING = "DATA_CLEANING"
    AGGREGATION = "AGGREGATION"
    VALIDATION = "VALIDATION"
    MONITORING = "MONITORING"
    MAINTENANCE = "MAINTENANCE"


class SourceType(str, Enum):
    """Source types for ETL jobs."""
    FILE = "FILE"
    API = "API"
    DATABASE = "DATABASE"
    STREAM = "STREAM"


class JobStatus(str, Enum):
    """Job execution status."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    RETRYING = "RETRYING"


class JobCreate(BaseModel):
    """Schema for creating ETL jobs."""
    job_name: str = Field(min_length=1, max_length=100)
    job_type: JobType
    job_category: JobCategory
    source_type: SourceType
    target_schema: Optional[str] = Field(default=None, max_length=50)
    target_table: Optional[str] = Field(default=None, max_length=100)
    job_config: Optional[Dict[str, Any]] = Field(default=None, description="Job configuration in JSON")
    schedule_expression: Optional[str] = Field(default=None, max_length=100, description="Cron expression")
    is_active: bool = True
    description: Optional[str] = Field(default=None, max_length=500)
    timeout_minutes: Optional[int] = Field(default=60, ge=1, le=1440)
    max_retries: int = Field(default=3, ge=0, le=10)
    priority: int = Field(default=1, ge=1, le=5)


class JobRead(BaseModel):
    """Schema for reading ETL jobs."""
    job_id: int
    job_name: str
    job_type: JobType
    job_category: JobCategory
    source_type: SourceType
    target_schema: Optional[str] = None
    target_table: Optional[str] = None
    job_config: Optional[Dict[str, Any]] = None
    schedule_expression: Optional[str] = None
    is_active: bool
    description: Optional[str] = None
    timeout_minutes: int
    max_retries: int
    priority: int
    created_at: datetime
    last_execution: Optional[datetime] = None
    next_execution: Optional[datetime] = None
    execution_count: int = 0
    success_count: int = 0
    failure_count: int = 0


class JobUpdate(BaseModel):
    """Schema for updating ETL jobs."""
    job_name: Optional[str] = Field(default=None, min_length=1, max_length=100)
    job_type: Optional[JobType] = None
    job_category: Optional[JobCategory] = None
    source_type: Optional[SourceType] = None
    target_schema: Optional[str] = Field(default=None, max_length=50)
    target_table: Optional[str] = Field(default=None, max_length=100)
    job_config: Optional[Dict[str, Any]] = None
    schedule_expression: Optional[str] = Field(default=None, max_length=100)
    is_active: Optional[bool] = None
    description: Optional[str] = Field(default=None, max_length=500)
    timeout_minutes: Optional[int] = Field(default=None, ge=1, le=1440)
    max_retries: Optional[int] = Field(default=None, ge=0, le=10)
    priority: Optional[int] = Field(default=None, ge=1, le=5)


class JobExecutionCreate(BaseModel):
    """Schema for creating job executions."""
    job_id: int
    batch_id: Optional[str] = Field(default=None, max_length=50)
    execution_context: Optional[Dict[str, Any]] = Field(default=None, description="Execution context")
    scheduled_time: Optional[datetime] = None
    priority_override: Optional[int] = Field(default=None, ge=1, le=5)


class JobExecutionRead(BaseModel):
    """Schema for reading job executions."""
    execution_id: int
    job_id: int
    job_name: str
    batch_id: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: JobStatus
    records_processed: int = 0
    records_successful: int = 0
    records_failed: int = 0
    execution_log: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    performance_metrics: Optional[Dict[str, Any]] = None
    created_at: datetime
    duration_seconds: Optional[int] = None
    retry_count: int = 0


class JobExecutionUpdate(BaseModel):
    """Schema for updating job executions."""
    status: Optional[JobStatus] = None
    records_processed: Optional[int] = None
    records_successful: Optional[int] = None
    records_failed: Optional[int] = None
    execution_log: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    performance_metrics: Optional[Dict[str, Any]] = None
    end_time: Optional[datetime] = None


class JobExecutionRequest(BaseModel):
    """Schema for job execution request."""
    job_id: int
    batch_id: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    force_run: bool = Field(default=False, description="Force run even if job is disabled")
    async_execution: bool = Field(default=True, description="Execute job asynchronously")


class JobExecutionResponse(BaseResponse):
    """Schema for job execution response."""
    execution_id: int
    job_id: int
    status: JobStatus
    estimated_completion: Optional[datetime] = None
    

class JobScheduleRequest(BaseModel):
    """Schema for job scheduling request."""
    job_id: int
    schedule_expression: str = Field(description="Cron expression for scheduling")
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    timezone: str = Field(default="UTC", description="Timezone for scheduling")


class JobStatistics(BaseModel):
    """Schema for job statistics."""
    job_id: int
    job_name: str
    total_executions: int
    successful_executions: int
    failed_executions: int
    average_duration_seconds: float
    last_execution_time: Optional[datetime] = None
    next_scheduled_time: Optional[datetime] = None
    success_rate: float = Field(ge=0, le=1, description="Success rate as percentage")


class JobPerformanceMetrics(BaseModel):
    """Schema for job performance metrics."""
    execution_id: int
    cpu_usage_percent: Optional[float] = None
    memory_usage_mb: Optional[float] = None
    disk_io_mb: Optional[float] = None
    network_io_mb: Optional[float] = None
    processing_rate_per_second: Optional[float] = None
    queue_time_seconds: Optional[float] = None
    execution_time_seconds: Optional[float] = None


class JobDependency(BaseModel):
    """Schema for job dependencies."""
    parent_job_id: int
    child_job_id: int
    dependency_type: str = Field(description="Type of dependency: success, completion, failure")
    is_active: bool = True


class JobBatchOperation(BaseModel):
    """Schema for batch job operations."""
    job_ids: List[int]
    operation: str = Field(description="Operation: start, stop, pause, resume, delete")
    parameters: Optional[Dict[str, Any]] = None
    force: bool = Field(default=False, description="Force operation even if jobs are running")


class JobBatchOperationResponse(BaseResponse):
    """Schema for batch job operation response."""
    total_jobs: int
    successful_operations: int
    failed_operations: int
    operation_details: List[Dict[str, Any]]

class JobResponse(BaseResponse):
    """Schema for job response."""
    job_id: UUID
    job_name: str
    status: JobStatus
    message: Optional[str] = None
    data: Optional[Dict[str, Any]] = None

class JobScheduleCreate(BaseModel):
    """Schema for creating a job schedule."""
    job_id: UUID
    cron_expression: str = Field(description="Cron expression for scheduling the job")
    timezone: str = Field(default="UTC", description="Timezone for the schedule")
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    is_active: bool = True

class JobConfigUpdate(BaseModel):
    """Schema for updating job configuration."""
    job_id: UUID
    job_config: Dict[str, Any] = Field(description="Updated job configuration in JSON format")
    is_active: Optional[bool] = None
    timeout_minutes: Optional[int] = Field(default=None, ge=1, le=1440)
    max_retries: Optional[int] = Field(default=None, ge=0, le=10)
    priority: Optional[int] = Field(default=None, ge=1, le=5)
    
    