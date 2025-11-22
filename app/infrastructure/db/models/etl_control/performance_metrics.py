from datetime import datetime
from typing import Optional
from decimal import Decimal
from uuid import UUID

from sqlmodel import SQLModel, Field, Relationship
from app.infrastructure.db.models.base import BaseModel


class PerformanceMetricBase(BaseModel):
    """Base model for PerformanceMetric"""
    execution_id: UUID = Field(
        foreign_key="etl_control.job_executions.id",
        index=True,
        description="Related job execution ID"
    )
    records_per_second: Optional[Decimal] = Field(
        default=None,
        max_digits=10,
        decimal_places=2,
        description="Processing rate in records per second"
    )
    memory_usage_mb: Optional[Decimal] = Field(
        default=None,
        max_digits=10,
        decimal_places=2,
        description="Memory usage in megabytes"
    )
    cpu_usage_percent: Optional[Decimal] = Field(
        default=None,
        max_digits=5,
        decimal_places=2,
        description="CPU usage percentage"
    )
    disk_io_mb: Optional[Decimal] = Field(
        default=None,
        max_digits=10,
        decimal_places=2,
        description="Disk I/O in megabytes"
    )
    network_io_mb: Optional[Decimal] = Field(
        default=None,
        max_digits=10,
        decimal_places=2,
        description="Network I/O in megabytes"
    )
    duration_seconds: Optional[int] = Field(
        default=None,
        description="Total duration in seconds"
    )
    peak_memory_mb: Optional[Decimal] = Field(
        default=None,
        max_digits=10,
        decimal_places=2,
        description="Peak memory usage in megabytes"
    )
    avg_cpu_percent: Optional[Decimal] = Field(
        default=None,
        max_digits=5,
        decimal_places=2,
        description="Average CPU usage percentage"
    )
    cache_hit_rate: Optional[Decimal] = Field(
        default=None,
        max_digits=5,
        decimal_places=2,
        description="Cache hit rate percentage"
    )
    error_rate: Optional[Decimal] = Field(
        default=None,
        max_digits=5,
        decimal_places=2,
        description="Error rate percentage"
    )


class PerformanceMetric(PerformanceMetricBase, table=True):
    """Model for etl_control.performance_metrics table"""
    __tablename__ = "performance_metrics"
    __table_args__ = (
        {"schema": "etl_control"},
    )
    
    recorded_at: datetime = Field(
        default_factory=datetime.utcnow,
        index=True,
        description="When the metrics were recorded"
    )
    
    # Relationships
    job_execution: Optional["JobExecution"] = Relationship(
        back_populates="performance_metrics_records",
        sa_relationship_kwargs={"lazy": "joined"}
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "execution_id": "550e8400-e29b-41d4-a716-446655440000",
                "records_per_second": 125.50,
                "memory_usage_mb": 512.75,
                "cpu_usage_percent": 45.30,
                "disk_io_mb": 256.00,
                "network_io_mb": 128.50,
                "duration_seconds": 1800,
                "peak_memory_mb": 768.25,
                "avg_cpu_percent": 42.15,
                "cache_hit_rate": 85.50,
                "error_rate": 0.05
            }
        }


class PerformanceMetricCreate(PerformanceMetricBase):
    """Schema for creating performance metric"""
    pass


class PerformanceMetricUpdate(SQLModel):
    """Schema for updating performance metric"""
    records_per_second: Optional[Decimal] = Field(default=None, max_digits=10, decimal_places=2)
    memory_usage_mb: Optional[Decimal] = Field(default=None, max_digits=10, decimal_places=2)
    cpu_usage_percent: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=2)
    disk_io_mb: Optional[Decimal] = Field(default=None, max_digits=10, decimal_places=2)
    network_io_mb: Optional[Decimal] = Field(default=None, max_digits=10, decimal_places=2)
    duration_seconds: Optional[int] = Field(default=None)


class PerformanceMetricRead(PerformanceMetricBase):
    """Schema for reading performance metric"""
    metric_id: UUID
    recorded_at: datetime


class PerformanceMetricSummary(SQLModel):
    """Schema for performance metrics summary"""
    execution_id: UUID
    avg_records_per_second: Decimal
    avg_memory_usage_mb: Decimal
    avg_cpu_usage_percent: Decimal
    total_duration_seconds: int
    total_records_processed: int


# Import to avoid circular imports
from app.infrastructure.db.models.etl_control.job_executions import JobExecution
