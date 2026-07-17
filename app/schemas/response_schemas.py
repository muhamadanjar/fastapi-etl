from typing import Optional, List, Dict, Any
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field

class ColumnStructureCreate(BaseModel):
    file_id: UUID
    column_name: str
    column_position: int
    data_type: str
    sample_values: Optional[List[str]] = []
    null_count: Optional[int] = None
    unique_count: Optional[int] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None


class ColumnStructureRead(ColumnStructureCreate):
    structure_id: UUID
    created_at: datetime


class RawRecordCreate(BaseModel):
    file_id: UUID
    sheet_name: Optional[str]
    row_number: Optional[int]
    column_mapping: Optional[dict] = {}
    raw_data: dict
    data_hash: Optional[str]
    validation_status: Optional[str] = "UNVALIDATED"
    validation_errors: Optional[List[str]] = []
    batch_id: Optional[str]


class RawRecordRead(RawRecordCreate):
    record_id: UUID
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


class ReportRequest(BaseModel):
    """Schema for report generation request."""
    name: str = Field(..., description="Report name")
    report_type: str = Field(..., description="Type of report (summary, detailed, analytics)")
    period: Optional[str] = Field("30d", description="Time period (7d, 30d, 90d)")
    filters: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Filters to apply")
    include_charts: Optional[bool] = Field(True, description="Include charts in report")
    include_raw_data: Optional[bool] = Field(False, description="Include raw data export")

    class Config:
        json_schema_extra = {
            "example": {
                "name": "Monthly ETL Report",
                "report_type": "summary",
                "period": "30d",
                "filters": {"job_type": "transformation"},
                "include_charts": True,
                "include_raw_data": False
            }
        }


class ReportResponse(BaseModel):
    """Schema for report response."""
    id: UUID = Field(..., description="Report ID")
    name: str
    report_type: str
    period: str
    created_at: datetime
    created_by: UUID
    status: str = Field(default="completed", description="Report status")
    file_path: Optional[str] = Field(None, description="Path to generated report file")
    format: Optional[str] = Field("pdf", description="Report format")
    size: Optional[int] = Field(None, description="File size in bytes")
    filters: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "name": "Monthly ETL Report",
                "report_type": "summary",
                "period": "30d",
                "created_at": "2026-05-21T10:30:00",
                "created_by": "550e8400-e29b-41d4-a716-446655440001",
                "status": "completed",
                "file_path": "/reports/report_550e8400.pdf",
                "format": "pdf",
                "size": 1024000
            }
        }
    