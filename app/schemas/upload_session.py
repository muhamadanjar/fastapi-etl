from uuid import UUID
from datetime import datetime
from typing import Optional, Dict
from pydantic import BaseModel, Field, validator
from app.core.enums import FileTypeEnum


_FILE_TYPE_MAP = {
    "csv": "CSV",
    "xlsx": "EXCEL",
    "xls": "EXCEL",
    "json": "JSON",
    "xml": "XML",
    "api": "API",
}


class InitUploadSessionRequest(BaseModel):
    """Request to initiate chunked upload session"""
    file_name: str = Field(..., description="Original file name with extension")
    file_size: int = Field(..., ge=1, description="Total file size in bytes")
    file_type: FileTypeEnum = Field(..., description="File type: CSV, EXCEL, JSON, XML, API")
    source_system: str = Field(..., description="Source system identifier")
    batch_id: Optional[str] = Field(None, description="Batch ID for grouping files")
    metadata: Optional[str] = Field(None, description="Additional metadata as JSON string")

    @validator('file_type', pre=True)
    def normalize_file_type(cls, v):
        """Normalize file_type input: 'csv' → 'CSV', 'xlsx' → 'EXCEL', etc."""
        normalized = _FILE_TYPE_MAP.get(str(v).lower().strip(), str(v).upper().strip())
        return normalized


class InitUploadSessionResponse(BaseModel):
    """Response from initiating chunked upload session"""
    session_id: UUID
    chunk_size: int = Field(..., description="Bytes per chunk to upload")
    total_chunks: int = Field(..., description="Total number of chunks")
    expires_at: datetime = Field(..., description="Session expiration timestamp")
    status: str = Field(default="pending")

    class Config:
        from_attributes = True


class ChunkUploadResponse(BaseModel):
    """Response after uploading a single chunk"""
    session_id: UUID
    status: str = Field(..., description="Current session status")
    received_bytes: int = Field(..., description="Total bytes received so far")
    uploaded_chunks: int = Field(..., description="Number of chunks received")
    total_chunks: int = Field(..., description="Total chunks expected")
    progress_percent: float = Field(..., description="Upload progress 0.0-100.0")

    class Config:
        from_attributes = True


class UploadSessionStatusResponse(BaseModel):
    """Response for getting session status (used for resume)"""
    session_id: UUID = Field(alias="id")
    status: str
    file_name: str
    file_size: int
    received_bytes: int
    uploaded_chunks: int
    total_chunks: int
    chunk_size: int
    chunk_map: Optional[Dict] = Field(None, description="Map of received chunks {index: true}")
    progress_percent: float
    file_registry_id: Optional[UUID] = Field(None, description="Set after completion")
    expires_at: datetime

    class Config:
        from_attributes = True
        populate_by_name = True
