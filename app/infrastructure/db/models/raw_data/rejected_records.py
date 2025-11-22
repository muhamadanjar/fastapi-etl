from datetime import datetime
from typing import Optional, Dict, Any, List
from uuid import UUID

from sqlmodel import SQLModel, Field, Column
from sqlalchemy.dialects.postgresql import JSONB
from app.infrastructure.db.models.base import BaseModel


class RejectedRecordBase(BaseModel):
    """Base model for RejectedRecord"""
    source_file_id: UUID = Field(
        foreign_key="raw_data.file_registry.id",
        index=True,
        description="Source file ID"
    )
    source_record_id: Optional[UUID] = Field(
        default=None,
        foreign_key="raw_data.raw_records.id",
        index=True,
        description="Original raw record ID if available"
    )
    row_number: Optional[int] = Field(
        default=None,
        description="Row number in source file"
    )
    raw_data: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSONB),
        description="Raw data that was rejected"
    )
    rejection_reason: str = Field(
        description="Primary reason for rejection"
    )
    validation_errors: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        sa_column=Column(JSONB),
        description="Detailed validation errors"
    )
    can_retry: bool = Field(
        default=True,
        index=True,
        description="Whether this record can be retried after correction"
    )
    retry_count: int = Field(
        default=0,
        description="Number of times this record has been retried"
    )
    last_retry_at: Optional[datetime] = Field(
        default=None,
        description="Last retry timestamp"
    )
    is_resolved: bool = Field(
        default=False,
        index=True,
        description="Whether this rejection has been resolved"
    )
    resolved_at: Optional[datetime] = Field(
        default=None,
        description="When the rejection was resolved"
    )
    batch_id: Optional[str] = Field(
        default=None,
        max_length=50,
        index=True,
        description="Batch identifier"
    )


class RejectedRecord(RejectedRecordBase, table=True):
    """Model for raw_data.rejected_records table"""
    __tablename__ = "rejected_records"
    __table_args__ = (
        {"schema": "raw_data"},
    )
    
    rejected_at: datetime = Field(
        default_factory=datetime.utcnow,
        index=True,
        description="When the record was rejected"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "source_file_id": "550e8400-e29b-41d4-a716-446655440000",
                "source_record_id": "550e8400-e29b-41d4-a716-446655440001",
                "row_number": 123,
                "raw_data": {
                    "name": "John Doe",
                    "email": "invalid-email",
                    "age": "not-a-number"
                },
                "rejection_reason": "Multiple validation errors",
                "validation_errors": [
                    {
                        "field": "email",
                        "error": "Invalid email format",
                        "value": "invalid-email"
                    },
                    {
                        "field": "age",
                        "error": "Must be a number",
                        "value": "not-a-number"
                    }
                ],
                "can_retry": True,
                "retry_count": 0,
                "is_resolved": False,
                "batch_id": "batch_2024_01_001"
            }
        }


class RejectedRecordCreate(RejectedRecordBase):
    """Schema for creating rejected record"""
    pass


class RejectedRecordUpdate(SQLModel):
    """Schema for updating rejected record"""
    can_retry: Optional[bool] = Field(default=None)
    retry_count: Optional[int] = Field(default=None)
    last_retry_at: Optional[datetime] = Field(default=None)
    is_resolved: Optional[bool] = Field(default=None)
    resolved_at: Optional[datetime] = Field(default=None)


class RejectedRecordRead(RejectedRecordBase):
    """Schema for reading rejected record"""
    rejection_id: UUID
    rejected_at: datetime


class RejectedRecordSummary(SQLModel):
    """Schema for rejected records summary"""
    source_file_id: UUID
    total_rejected: int
    can_retry_count: int
    resolved_count: int
    common_rejection_reasons: List[Dict[str, Any]]
