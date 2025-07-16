from datetime import datetime
from typing import Optional, Dict, Any, List
from sqlmodel import UUID, SQLModel, Field, Column, JSON, ARRAY, String, ForeignKey

from app.core.enums import ValidationStatus

from ..base import BaseModel





class RawRecords(BaseModel, table=True):
    """
    Model for storing raw data records in flexible JSON format
    """
    __tablename__ = "raw_records"
    __table_args__ = {"schema": "raw_data"}
    
    
    file_id: int = Field(
        foreign_key="raw_data.file_registry.id",
        description="Reference to the source file"
    )
    
    sheet_name: Optional[str] = Field(
        default=None,
        max_length=100,
        description="Sheet name for Excel files with multiple sheets"
    )
    
    row_number: Optional[int] = Field(
        default=None,
        description="Original row number in the source file"
    )
    
    column_mapping: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSON),
        description="Mapping of original column names to standardized names"
    )
    
    raw_data: Dict[str, Any] = Field(
        sa_column=Column(JSON),
        description="Raw data in JSON format"
    )
    
    data_hash: Optional[str] = Field(
        default=None,
        max_length=64,
        description="Hash of the raw data for duplicate detection"
    )
    
    validation_status: ValidationStatus = Field(
        default=ValidationStatus.UNVALIDATED,
        description="Current validation status of the record"
    )
    
    validation_errors: Optional[List[str]] = Field(
        default=None,
        sa_column=Column(ARRAY(String)),
        description="List of validation errors if any"
    )
    
    batch_id: Optional[str] = Field(
        default=None,
        max_length=50,
        description="Batch identifier for grouping related records"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "file_id": 1,
                "sheet_name": "Sheet1",
                "row_number": 2,
                "column_mapping": {
                    "Name": "customer_name",
                    "Email": "email_address",
                    "Phone": "phone_number"
                },
                "raw_data": {
                    "customer_name": "John Doe",
                    "email_address": "john.doe@email.com",
                    "phone_number": "+1234567890",
                    "purchase_date": "2024-01-15"
                },
                "data_hash": "abc123def456...",
                "validation_status": "VALID",
                "validation_errors": None,
                "batch_id": "BATCH_20240101_001"
            }
        }


class RawRecordsCreate(SQLModel):
    """Schema for creating a new raw record"""
    id: str
    sheet_name: Optional[str] = Field(default=None, max_length=100)
    row_number: Optional[int] = None
    column_mapping: Optional[Dict[str, Any]] = None
    raw_data: Dict[str, Any]
    data_hash: Optional[str] = Field(default=None, max_length=64)
    validation_status: ValidationStatus = ValidationStatus.UNVALIDATED
    validation_errors: Optional[List[str]] = None
    batch_id: Optional[str] = Field(default=None, max_length=50)


class RawRecordsUpdate(SQLModel):
    """Schema for updating a raw record"""
    sheet_name: Optional[str] = Field(default=None, max_length=100)
    row_number: Optional[int] = None
    column_mapping: Optional[Dict[str, Any]] = None
    raw_data: Optional[Dict[str, Any]] = None
    data_hash: Optional[str] = Field(default=None, max_length=64)
    validation_status: Optional[ValidationStatus] = None
    validation_errors: Optional[List[str]] = None
    batch_id: Optional[str] = Field(default=None, max_length=50)


class RawRecordsRead(SQLModel):
    """Schema for reading raw record data"""
    record_id: int
    file_id: str
    sheet_name: Optional[str]
    row_number: Optional[int]
    column_mapping: Optional[Dict[str, Any]]
    raw_data: Dict[str, Any]
    data_hash: Optional[str]
    validation_status: ValidationStatus
    validation_errors: Optional[List[str]]
    batch_id: Optional[str]
    created_at: datetime


class RawRecordsBulkCreate(SQLModel):
    """Schema for bulk creating raw records"""
    records: List[RawRecordsCreate]
    
    class Config:
        schema_extra = {
            "example": {
                "records": [
                    {
                        "file_id": 1,
                        "row_number": 2,
                        "raw_data": {
                            "name": "John Doe",
                            "email": "john@email.com"
                        },
                        "batch_id": "BATCH_001"
                    },
                    {
                        "file_id": 1,
                        "row_number": 3,
                        "raw_data": {
                            "name": "Jane Smith",
                            "email": "jane@email.com"
                        },
                        "batch_id": "BATCH_001"
                    }
                ]
            }
        }


class RawRecordsFilter(SQLModel):
    """Schema for filtering raw records"""
    file_id: Optional[int] = None
    sheet_name: Optional[str] = None
    validation_status: Optional[ValidationStatus] = None
    batch_id: Optional[str] = None
    has_errors: Optional[bool] = None
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None
    limit: Optional[int] = Field(default=100, le=1000)
    offset: Optional[int] = Field(default=0, ge=0)
