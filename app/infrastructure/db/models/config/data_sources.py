from datetime import datetime
from typing import Optional, Dict, Any
from uuid import UUID

from sqlmodel import SQLModel, Field, Column
from sqlalchemy.dialects.postgresql import JSONB
from enum import Enum
from app.infrastructure.db.models.base import BaseModel


class SourceType(str, Enum):
    """Enum for data source types"""
    DATABASE = "DATABASE"
    API = "API"
    FILE = "FILE"
    STREAM = "STREAM"
    FTP = "FTP"
    SFTP = "SFTP"
    S3 = "S3"
    AZURE_BLOB = "AZURE_BLOB"
    GCS = "GCS"


class ConnectionStatus(str, Enum):
    """Enum for connection status"""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    ERROR = "ERROR"
    TESTING = "TESTING"


class DataSourceBase(BaseModel):
    """Base model for DataSource"""
    source_name: str = Field(
        max_length=100,
        unique=True,
        index=True,
        description="Unique name for the data source"
    )
    source_type: SourceType = Field(
        index=True,
        description="Type of data source"
    )
    description: Optional[str] = Field(
        default=None,
        max_length=500,
        description="Description of the data source"
    )
    connection_config: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSONB),
        description="Connection configuration (host, port, database, etc.)"
    )
    credentials_encrypted: Optional[str] = Field(
        default=None,
        description="Encrypted credentials for authentication"
    )
    is_active: bool = Field(
        default=True,
        index=True,
        description="Whether this data source is currently active"
    )
    connection_status: ConnectionStatus = Field(
        default=ConnectionStatus.INACTIVE,
        index=True,
        description="Current connection status"
    )
    last_connection_test: Optional[datetime] = Field(
        default=None,
        description="Last time connection was tested"
    )
    connection_pool_size: Optional[int] = Field(
        default=5,
        description="Connection pool size for database sources"
    )
    timeout_seconds: Optional[int] = Field(
        default=30,
        description="Connection timeout in seconds"
    )
    retry_attempts: Optional[int] = Field(
        default=3,
        description="Number of retry attempts for failed connections"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSONB),
        description="Additional metadata"
    )


class DataSource(DataSourceBase, table=True):
    """Model for config.data_sources table"""
    __tablename__ = "data_sources"
    __table_args__ = (
        {"schema": "config"},
    )
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: Optional[UUID] = Field(
        default=None,
        foreign_key="users.id",
        description="User who created this data source"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "source_name": "production_postgres",
                "source_type": "DATABASE",
                "description": "Production PostgreSQL database",
                "connection_config": {
                    "host": "db.example.com",
                    "port": 5432,
                    "database": "production",
                    "ssl_mode": "require",
                    "schema": "public"
                },
                "credentials_encrypted": "encrypted_string_here",
                "is_active": True,
                "connection_status": "ACTIVE",
                "connection_pool_size": 10,
                "timeout_seconds": 30,
                "retry_attempts": 3,
                "metadata": {
                    "environment": "production",
                    "region": "us-east-1",
                    "backup_enabled": True
                }
            }
        }


class DataSourceCreate(DataSourceBase):
    """Schema for creating data source"""
    pass


class DataSourceUpdate(SQLModel):
    """Schema for updating data source"""
    source_name: Optional[str] = Field(default=None, max_length=100)
    source_type: Optional[SourceType] = Field(default=None)
    description: Optional[str] = Field(default=None, max_length=500)
    connection_config: Optional[Dict[str, Any]] = Field(default=None)
    credentials_encrypted: Optional[str] = Field(default=None)
    is_active: Optional[bool] = Field(default=None)
    connection_status: Optional[ConnectionStatus] = Field(default=None)
    connection_pool_size: Optional[int] = Field(default=None)
    timeout_seconds: Optional[int] = Field(default=None)
    retry_attempts: Optional[int] = Field(default=None)
    metadata: Optional[Dict[str, Any]] = Field(default=None)


class DataSourceRead(DataSourceBase):
    """Schema for reading data source"""
    source_id: UUID
    created_at: datetime
    updated_at: datetime
    created_by: Optional[UUID]


class DataSourceConnectionTest(SQLModel):
    """Schema for connection test result"""
    source_id: UUID
    is_successful: bool
    response_time_ms: Optional[int]
    error_message: Optional[str]
    tested_at: datetime = Field(default_factory=datetime.utcnow)
