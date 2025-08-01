from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from sqlmodel import SQLModel, Field
from sqlalchemy import Column, DateTime, func, text


class BaseModel(SQLModel):
    """
    Base model with common fields for all database models.
    """
    
    id: UUID = Field(
        default_factory=uuid4,
        sa_column_kwargs={
            "server_default": text("gen_random_uuid()")  # atau uuid_generate_v4()
        },
        primary_key=True,
        index=True,
        nullable=False,
        description="Unique identifier"
    )

class AuditMixin(SQLModel):
    created_by: Optional[UUID] = Field(default=None, foreign_key="users.id")
    updated_by: Optional[UUID] = Field(default=None, foreign_key="users.id")


class TimestampMixin(SQLModel):
    """
    Mixin for models that need timestamp fields.
    """
    
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column_kwargs={
            "server_default": func.now(),
            "nullable": False,
        },
        description="Record creation timestamp"
    )

    updated_at: Optional[datetime] = Field(
        default=None,
        sa_column_kwargs={
            "onupdate": func.now(),
            "nullable": True,
        },
        description="Record last update timestamp"
    )

class SoftDeleteMixin:
    """Mixin for soft delete functionality"""
    is_deleted: bool = Field(default=False)
    deleted_at: Optional[datetime] = Field(default=None)
    deleted_by: Optional[UUID] = Field(default=None, foreign_key="users.id")

class BaseModelWithTimestamp(BaseModel, TimestampMixin):
    """
    Base model with ID and timestamp fields.
    """
    pass
