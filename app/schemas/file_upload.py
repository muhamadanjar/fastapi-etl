from typing import Optional, List, Dict, Any
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field
from enum import Enum

from app.core.enums import FileTypeEnum, ProcessingStatus
from .base import BaseResponse, PaginatedMetaDataResponse


class ValidationStatus(str, Enum):
    """Data validation status."""
    UNVALIDATED = "UNVALIDATED"
    VALID = "VALID"
    INVALID = "INVALID"
    WARNING = "WARNING"


class FileUploadRequest(BaseModel):
    """Schema for file upload request."""
    source_system: Optional[str] = Field(default=None, max_length=100)
    batch_id: Optional[str] = Field(default=None, max_length=50)
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Additional file metadata")
    auto_process: bool = Field(default=True, description="Whether to auto-process the file")
    validation_rules: Optional[List[str]] = Field(default=None, description="Validation rules to apply")


class FileUploadResponse(BaseResponse):
    """Schema for file upload response."""
    file_id: UUID
    file_name: str
    file_type: FileTypeEnum
    file_size: int
    batch_id: str
    processing_status: ProcessingStatus
    upload_date: datetime


class FileProcessingStatus(BaseModel):
    """Schema for file processing status."""
    file_id: UUID
    file_name: str
    processing_status: ProcessingStatus
    progress_percentage: float = Field(ge=0, le=100, description="Processing progress percentage")
    records_processed: int = 0
    records_successful: int = 0
    records_failed: int = 0
    validation_errors: List[str] = []
    processing_errors: List[str] = []
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    estimated_completion: Optional[datetime] = None


class FileMetadata(BaseModel):
    """Schema for file metadata."""
    file_id: UUID
    file_name: str
    file_path: str
    file_type: FileTypeEnum
    file_size: int
    source_system: Optional[str] = None
    upload_date: datetime
    processing_status: ProcessingStatus
    batch_id: str
    created_by: UUID
    metadata: Optional[Dict[str, Any]] = None


class ColumnStructure(BaseModel):
    """Schema for column structure information."""
    column_name: str
    column_position: int
    data_type: str
    sample_values: List[str] = []
    null_count: int = 0
    unique_count: int = 0
    min_length: Optional[int] = None
    max_length: Optional[int] = None


class FileStructureAnalysis(BaseModel):
    """Schema for file structure analysis."""
    file_id: UUID
    total_rows: int
    total_columns: int
    columns: List[ColumnStructure]
    data_quality_score: float = Field(ge=0, le=1, description="Overall data quality score")
    issues_found: List[str] = []
    recommendations: List[str] = []


class FilePreview(BaseModel):
    """Schema for file preview."""
    file_id: UUID
    headers: List[str]
    sample_data: List[Dict[str, Any]]
    total_rows: int
    preview_rows: int


class BatchProcessingRequest(BaseModel):
    """Schema for batch processing request."""
    file_ids: List[int]
    processing_options: Optional[Dict[str, Any]] = None
    priority: int = Field(default=1, ge=1, le=5, description="Processing priority")
    schedule_time: Optional[datetime] = Field(default=None, description="Schedule processing for later")


class BatchProcessingResponse(BaseResponse):
    """Schema for batch processing response."""
    batch_id: str
    total_files: int
    files_queued: int
    files_failed: int
    estimated_completion: datetime


class FileDownloadRequest(BaseModel):
    """Schema for file download request."""
    file_id: int
    format: Optional[str] = Field(default="original", description="Download format")
    include_metadata: bool = Field(default=False, description="Include metadata in download")


class FileValidationResult(BaseModel):
    """Schema for file validation result."""
    file_id: int
    validation_status: ValidationStatus
    total_records: int
    valid_records: int
    invalid_records: int
    warnings: int
    validation_errors: List[Dict[str, Any]] = []
    quality_score: float = Field(ge=0, le=1)


class FileExportRequest(BaseModel):
    """Schema for file export request."""
    file_ids: List[int]
    export_format: str = Field(description="Export format: csv, excel, json")
    include_raw_data: bool = Field(default=True)
    include_processed_data: bool = Field(default=False)
    filters: Optional[Dict[str, Any]] = None
    columns: Optional[List[str]] = None


class FileExportResponse(BaseResponse):
    """Schema for file export response."""
    export_id: str
    download_url: str
    expires_at: datetime
    file_size: int
    format: str

class FileListResponse(BaseResponse):
    """Schema for file list response."""
    data: List[FileMetadata]
    metas: Optional[PaginatedMetaDataResponse] = None

class FileDetailResponse(BaseResponse):
    """Schema for file detail response."""
    file: FileMetadata
    structure_analysis: Optional[FileStructureAnalysis] = None
    preview: Optional[FilePreview] = None
    processing_status: Optional[FileProcessingStatus] = None
    validation_result: Optional[FileValidationResult] = None
    