from sqlmodel import SQLModel, Field, Relationship, Column, JSON
from datetime import datetime
from typing import Optional, List, Dict, Any
from app.models.base import BaseModel
from app.core.enums import ProcessingStatus, FileTypeEnum


class FileRegistry(BaseModel, table=True):
    """
    Model for storing file metadata and processing information
    """
    __tablename__ = "file_registry"
    __table_args__ = {"schema": "raw_data"}
    
    file_id: Optional[int] = Field(
        default=None, 
        primary_key=True,
        description="Unique identifier for the file"
    )
    
    file_name: str = Field(
        max_length=255,
        description="Original name of the uploaded file"
    )
    
    file_path: Optional[str] = Field(
        default=None,
        max_length=500,
        description="Path where the file is stored"
    )
    
    file_type: FileTypeEnum = Field(
        description="Type of the file (CSV, EXCEL, JSON, XML, API)"
    )
    
    file_size: Optional[int] = Field(
        default=None,
        description="Size of the file in bytes"
    )
    
    source_system: Optional[str] = Field(
        default=None,
        max_length=100,
        description="Source system that provided the file"
    )
    
    upload_date: datetime = Field(
        default_factory=datetime.utcnow,
        description="Date and time when the file was uploaded"
    )
    
    processing_status: ProcessingStatus = Field(
        default=ProcessingStatus.PENDING,
        description="Current processing status of the file"
    )
    
    batch_id: Optional[str] = Field(
        default=None,
        max_length=50,
        description="Batch identifier for grouping related files"
    )
    
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSON),
        description="Additional metadata about the file in JSON format"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "file_name": "sales_data_2024.csv",
                "file_path": "/storage/uploads/sales_data_2024.csv",
                "file_type": "CSV",
                "file_size": 1024000,
                "source_system": "SalesForce",
                "processing_status": "PENDING",
                "batch_id": "BATCH_20240101_001",
                "created_by": "admin@company.com",
                "metadata": {
                    "delimiter": ",",
                    "encoding": "utf-8",
                    "has_header": True
                }
            }
        }


class FileRegistryCreate(SQLModel):
    """Schema for creating a new file registry entry"""
    file_name: str = Field(max_length=255)
    file_path: Optional[str] = Field(default=None, max_length=500)
    file_type: FileTypeEnum
    file_size: Optional[int] = None
    source_system: Optional[str] = Field(default=None, max_length=100)
    batch_id: Optional[str] = Field(default=None, max_length=50)
    created_by: Optional[str] = Field(default=None, max_length=100)
    metadata: Optional[Dict[str, Any]] = None


class FileRegistryUpdate(SQLModel):
    """Schema for updating a file registry entry"""
    file_name: Optional[str] = Field(default=None, max_length=255)
    file_path: Optional[str] = Field(default=None, max_length=500)
    file_type: Optional[FileTypeEnum] = None
    file_size: Optional[int] = None
    source_system: Optional[str] = Field(default=None, max_length=100)
    processing_status: Optional[ProcessingStatus] = None
    batch_id: Optional[str] = Field(default=None, max_length=50)
    created_by: Optional[str] = Field(default=None, max_length=100)
    metadata: Optional[Dict[str, Any]] = None


class FileRegistryRead(SQLModel):
    """Schema for reading file registry data"""
    file_id: int
    file_name: str
    file_path: Optional[str]
    file_type: FileTypeEnum
    file_size: Optional[int]
    source_system: Optional[str]
    upload_date: datetime
    processing_status: ProcessingStatus
    batch_id: Optional[str]
    created_by: Optional[str]
    metadata: Optional[Dict[str, Any]]
    created_at: datetime