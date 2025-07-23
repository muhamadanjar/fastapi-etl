from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum
from uuid import UUID

from sqlmodel import SQLModel, Field, Column, Relationship
from sqlalchemy.dialects.postgresql import JSONB

from app.infrastructure.db.models.base import BaseModel


class ExecutionStatus(str, Enum):
    """Enum untuk execution status"""
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    PENDING = "PENDING"
    TIMEOUT = "TIMEOUT"


class JobExecutionBase(BaseModel):
    """Base model untuk JobExecution dengan field-field umum"""
    job_id: UUID = Field(foreign_key="etl_control.etl_jobs.id", index=True)
    batch_id: Optional[str] = Field(default=None, max_length=50, index=True)
    start_time: Optional[datetime] = Field(default=None)
    end_time: Optional[datetime] = Field(default=None)
    status: ExecutionStatus = Field(default=ExecutionStatus.PENDING, index=True)
    records_processed: Optional[int] = Field(default=0)
    records_successful: Optional[int] = Field(default=0)
    records_failed: Optional[int] = Field(default=0)
    execution_log: Optional[str] = Field(default=None)
    error_details: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB))
    performance_metrics: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB))


class JobExecution(JobExecutionBase, table=True):
    """Model untuk tabel etl_control.job_executions"""
    __tablename__ = "job_executions"
    __table_args__ = (
        {"schema": "etl_control"},
    )
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Relationships
    job: Optional["EtlJob"] = Relationship(back_populates="executions")
    
    quality_check_results: List["QualityCheckResult"] = Relationship(back_populates="execution")
    
    class Config:
        schema_extra = {
            "example": {
                "job_id": 1,
                "batch_id": "batch_2024_01_001",
                "start_time": "2024-01-15T10:00:00",
                "end_time": "2024-01-15T10:30:00",
                "status": "SUCCESS",
                "records_processed": 1000,
                "records_successful": 995,
                "records_failed": 5,
                "execution_log": "Job started successfully...\nProcessing file customer_data.csv...\nValidation completed...\nTransformation applied...\nJob completed successfully.",
                "error_details": {
                    "failed_records": [
                        {
                            "record_id": 123,
                            "error": "Invalid email format",
                            "data": {"email": "invalid-email"}
                        }
                    ]
                },
                "performance_metrics": {
                    "duration_seconds": 1800,
                    "records_per_second": 0.56,
                    "memory_usage_mb": 256,
                    "cpu_usage_percent": 45,
                    "disk_io_mb": 128,
                    "network_io_mb": 64
                }
            }
        }


class JobExecutionCreate(JobExecutionBase):
    """Schema untuk create job execution"""
    pass


class JobExecutionUpdate(SQLModel):
    """Schema untuk update job execution"""
    batch_id: Optional[str] = Field(default=None, max_length=50)
    start_time: Optional[datetime] = Field(default=None)
    end_time: Optional[datetime] = Field(default=None)
    status: Optional[ExecutionStatus] = Field(default=None)
    records_processed: Optional[int] = Field(default=None)
    records_successful: Optional[int] = Field(default=None)
    records_failed: Optional[int] = Field(default=None)
    execution_log: Optional[str] = Field(default=None)
    error_details: Optional[Dict[str, Any]] = Field(default=None)
    performance_metrics: Optional[Dict[str, Any]] = Field(default=None)


class JobExecutionRead(JobExecutionBase):
    """Schema untuk read job execution"""
    execution_id: int
    created_at: datetime


class JobExecutionReadWithJob(JobExecutionRead):
    """Schema untuk read job execution dengan job detail"""
    job: Optional["EtlJobRead"] = Field(default=None)


class JobExecutionReadWithQualityResults(JobExecutionRead):
    """Schema untuk read job execution dengan quality check results"""
    quality_check_results: Optional[List["QualityCheckResultRead"]] = Field(default=None)


class JobExecutionFilter(SQLModel):
    """Schema untuk filter job execution"""
    job_id: Optional[int] = Field(default=None)
    batch_id: Optional[str] = Field(default=None)
    status: Optional[ExecutionStatus] = Field(default=None)
    start_time_from: Optional[datetime] = Field(default=None)
    start_time_to: Optional[datetime] = Field(default=None)
    end_time_from: Optional[datetime] = Field(default=None)
    end_time_to: Optional[datetime] = Field(default=None)
    records_processed_min: Optional[int] = Field(default=None)
    records_processed_max: Optional[int] = Field(default=None)
    success_rate_min: Optional[float] = Field(default=None)
    success_rate_max: Optional[float] = Field(default=None)


class JobExecutionSummary(SQLModel):
    """Schema untuk summary job execution"""
    job_id: int
    job_name: str
    total_executions: int
    successful_executions: int
    failed_executions: int
    success_rate: float
    avg_duration_seconds: Optional[float] = Field(default=None)
    avg_records_processed: Optional[float] = Field(default=None)
    last_execution: Optional[datetime] = Field(default=None)
    last_success: Optional[datetime] = Field(default=None)


class JobExecutionMetrics(SQLModel):
    """Schema untuk metrics job execution"""
    execution_id: int
    duration_seconds: Optional[float] = Field(default=None)
    records_per_second: Optional[float] = Field(default=None)
    success_rate: Optional[float] = Field(default=None)
    memory_usage_mb: Optional[float] = Field(default=None)
    cpu_usage_percent: Optional[float] = Field(default=None)
    disk_io_mb: Optional[float] = Field(default=None)
    network_io_mb: Optional[float] = Field(default=None)
    error_rate: Optional[float] = Field(default=None)


class JobExecutionLog(SQLModel):
    """Schema untuk log job execution"""
    execution_id: int
    log_level: str  # 'INFO', 'WARNING', 'ERROR', 'DEBUG'
    log_message: str
    log_timestamp: datetime = Field(default_factory=datetime.utcnow)
    log_context: Optional[Dict[str, Any]] = Field(default=None)


class JobExecutionRestart(SQLModel):
    """Schema untuk restart job execution"""
    execution_id: int
    restart_from_step: Optional[str] = Field(default=None)
    restart_reason: str
    preserve_batch_id: bool = Field(default=True)
    reset_counters: bool = Field(default=False)


class JobExecutionCancel(SQLModel):
    """Schema untuk cancel job execution"""
    execution_id: int
    cancel_reason: str
    force_cancel: bool = Field(default=False)
    cleanup_resources: bool = Field(default=True)


class JobExecutionRetry(SQLModel):
    """Schema untuk retry job execution"""
    execution_id: int
    retry_count: int = Field(default=1)
    retry_delay_minutes: int = Field(default=5)
    retry_reason: str
    max_retries: int = Field(default=3)


class JobExecutionAlert(SQLModel):
    """Schema untuk alert job execution"""
    execution_id: int
    alert_type: str  # 'failure', 'timeout', 'performance', 'data_quality'
    alert_severity: str  # 'low', 'medium', 'high', 'critical'
    alert_message: str
    alert_timestamp: datetime = Field(default_factory=datetime.utcnow)
    is_acknowledged: bool = Field(default=False)
    acknowledged_by: Optional[str] = Field(default=None)


# Import untuk menghindari circular imports
from .etl_jobs import EtlJobRead
from .quality_check_results import QualityCheckResultRead