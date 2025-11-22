from datetime import datetime
from typing import Optional, Dict, Any
from uuid import UUID

from sqlmodel import SQLModel, Field, Column, Relationship
from sqlalchemy.dialects.postgresql import JSONB
from enum import Enum
from app.infrastructure.db.models.base import BaseModel


class ErrorSeverity(str, Enum):
    """Enum for error severity levels"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class ErrorType(str, Enum):
    """Enum for error types"""
    VALIDATION_ERROR = "VALIDATION_ERROR"
    PROCESSING_ERROR = "PROCESSING_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    NETWORK_ERROR = "NETWORK_ERROR"
    TIMEOUT_ERROR = "TIMEOUT_ERROR"
    CONFIGURATION_ERROR = "CONFIGURATION_ERROR"
    SYSTEM_ERROR = "SYSTEM_ERROR"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"


class ErrorLogBase(BaseModel):
    """Base model for ErrorLog"""
    job_execution_id: Optional[UUID] = Field(
        default=None,
        foreign_key="etl_control.job_executions.id",
        index=True,
        description="Related job execution ID"
    )
    error_type: ErrorType = Field(
        default=ErrorType.UNKNOWN_ERROR,
        index=True,
        description="Type of error that occurred"
    )
    error_severity: ErrorSeverity = Field(
        default=ErrorSeverity.MEDIUM,
        index=True,
        description="Severity level of the error"
    )
    error_message: str = Field(
        description="Error message"
    )
    error_details: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSONB),
        description="Additional error details in JSON format"
    )
    stack_trace: Optional[str] = Field(
        default=None,
        description="Full stack trace of the error"
    )
    context: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSONB),
        description="Context information when error occurred"
    )
    is_resolved: bool = Field(
        default=False,
        index=True,
        description="Whether this error has been resolved"
    )
    resolved_at: Optional[datetime] = Field(
        default=None,
        description="When the error was resolved"
    )
    resolved_by: Optional[UUID] = Field(
        default=None,
        foreign_key="users.id",
        description="User who resolved the error"
    )


class ErrorLog(ErrorLogBase, table=True):
    """Model for etl_control.error_logs table"""
    __tablename__ = "error_logs"
    __table_args__ = (
        {"schema": "etl_control"},
    )
    
    occurred_at: datetime = Field(
        default_factory=datetime.utcnow,
        index=True,
        description="When the error occurred"
    )
    
    # Relationships
    job_execution: Optional["JobExecution"] = Relationship(
        back_populates="error_logs",
        sa_relationship_kwargs={"lazy": "joined"}
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "job_execution_id": "550e8400-e29b-41d4-a716-446655440000",
                "error_type": "VALIDATION_ERROR",
                "error_severity": "HIGH",
                "error_message": "Invalid email format in row 123",
                "error_details": {
                    "row_number": 123,
                    "field": "email",
                    "value": "invalid-email",
                    "expected_format": "email@domain.com"
                },
                "stack_trace": "Traceback (most recent call last):\\n  File ...",
                "context": {
                    "file_id": "file-123",
                    "batch_id": "batch-001",
                    "processor": "CSVProcessor"
                },
                "is_resolved": False
            }
        }


class ErrorLogCreate(ErrorLogBase):
    """Schema for creating error log"""
    pass


class ErrorLogUpdate(SQLModel):
    """Schema for updating error log"""
    is_resolved: Optional[bool] = Field(default=None)
    resolved_at: Optional[datetime] = Field(default=None)
    resolved_by: Optional[UUID] = Field(default=None)


class ErrorLogRead(ErrorLogBase):
    """Schema for reading error log"""
    error_id: UUID
    occurred_at: datetime


class ErrorLogSummary(SQLModel):
    """Schema for error log summary"""
    error_type: ErrorType
    error_severity: ErrorSeverity
    total_count: int
    unresolved_count: int
    last_occurrence: datetime


# Import to avoid circular imports
from app.infrastructure.db.models.etl_control.job_executions import JobExecution
