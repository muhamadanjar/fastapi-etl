'use client'

from enum import Enum
from datetime import datetime
from typing import Optional, Dict
from uuid import UUID
from sqlmodel import SQLModel, Field, Column
from sqlalchemy import BigInteger, String
from sqlalchemy.dialects.postgresql import JSONB
from app.infrastructure.db.models.base import BaseModelWithTimestamp
from app.core.enums import FileTypeEnum


class UploadSessionStatus(str, Enum):
    PENDING = "pending"
    UPLOADING = "uploading"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"


class UploadSession(BaseModelWithTimestamp, table=True):
    __tablename__ = "upload_sessions"
    __table_args__ = {"schema": "raw_data"}

    # File identity
    file_name: str = Field(max_length=255)
    file_path: str = Field(max_length=500)   # HANYA "{uuid}.{ext}" — tanpa base path
    file_size: int = Field(sa_column=Column(BigInteger, nullable=False))
    file_type: FileTypeEnum = Field(sa_column=Column(String(50), nullable=False))

    # Chunk tracking
    chunk_size: int                           # bytes per chunk (from config)
    total_chunks: int                         # ceil(file_size / chunk_size)
    uploaded_chunks: int = Field(default=0)  # jumlah chunk diterima
    received_bytes: int = Field(
        default=0,
        sa_column=Column(BigInteger, nullable=False, server_default="0"),
    )
    chunk_map: Optional[Dict] = Field(        # {chunk_index_str: True} — chunk mana saja sudah diterima
        default=None,
        sa_column=Column(JSONB)
    )

    # Status
    status: str = Field(default=UploadSessionStatus.PENDING, max_length=20, index=True)

    # Upload context (dibutuhkan untuk buat FileRegistry setelah selesai)
    source_system: str = Field(max_length=100)
    batch_id: Optional[str] = Field(default=None, max_length=100)
    file_metadata: Optional[Dict] = Field(default=None, sa_column=Column(JSONB))

    # Relations
    created_by: Optional[UUID] = Field(default=None, index=True)
    file_registry_id: Optional[UUID] = Field(      # diset setelah upload selesai
        default=None,
        foreign_key="raw_data.file_registry.id",
        ondelete="SET NULL",
    )

    # Expiry
    expires_at: datetime = Field(..., index=True)  # created_at + expire_hours
