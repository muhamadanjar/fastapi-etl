from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel

class ColumnStructureCreate(BaseModel):
    file_id: int
    column_name: str
    column_position: int
    data_type: str
    sample_values: Optional[List[str]] = []
    null_count: Optional[int] = None
    unique_count: Optional[int] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None

class ColumnStructureRead(ColumnStructureCreate):
    structure_id: int
    created_at: datetime






class RawRecordCreate(BaseModel):
    file_id: int
    sheet_name: Optional[str]
    row_number: Optional[int]
    column_mapping: Optional[dict] = {}
    raw_data: dict
    data_hash: Optional[str]
    validation_status: Optional[str] = "UNVALIDATED"
    validation_errors: Optional[List[str]] = []
    batch_id: Optional[str]

class RawRecordRead(RawRecordCreate):
    record_id: int
    created_at: datetime

class SuccessResponse(BaseModel):
    """Base schema for successful responses."""
    status: str = "success"
    message: Optional[str] = None
    data: Optional[dict] = None

class ErrorResponseDetail(BaseModel):
    """Schema for error details in responses."""
    code: str
    message: str
    details: Optional[str] = None

class ValidationErrorResponse(BaseModel):
    """Schema for validation error responses."""
    status: str = "error"
    message: str = "Validation failed"
    errors: List[ErrorResponseDetail]

class ProcessingStatusResponse(BaseModel):
    """Schema for processing status responses."""
    status: str = "processing"
    message: Optional[str] = None
    progress: Optional[float] = None  # Percentage of completion
    estimated_time_remaining: Optional[int] = None  # In seconds

class DashboardResponse(BaseModel):
    """Schema for dashboard data responses."""
    status: str = "success"
    message: Optional[str] = None
    data: dict