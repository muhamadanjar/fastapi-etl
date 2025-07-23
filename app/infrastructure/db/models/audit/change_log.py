from datetime import datetime
from typing import Optional, Dict, Any
from sqlmodel import SQLModel, Field, Column, JSON

from app.infrastructure.db.models.base import BaseModel


class ChangeLogBase(BaseModel):
    """Base model for change log."""
    table_name: Optional[str] = Field(default=None, max_length=100, description="Name of the table that changed")
    record_id: Optional[str] = Field(default=None, max_length=50, description="ID of the record that changed")
    operation: Optional[str] = Field(
        default=None, 
        max_length=10, 
        description="Type of operation: INSERT, UPDATE, DELETE"
    )
    old_values: Optional[Dict[str, Any]] = Field(
        default=None, 
        sa_column=Column(JSON),
        description="Old values before change in JSON format"
    )
    new_values: Optional[Dict[str, Any]] = Field(
        default=None, 
        sa_column=Column(JSON),
        description="New values after change in JSON format"
    )
    changed_by: Optional[str] = Field(default=None, max_length=100, description="User who made the change")
    change_reason: Optional[str] = Field(default=None, max_length=255, description="Reason for the change")


class ChangeLog(ChangeLogBase, table=True):
    """Change log model for database storage."""
    __tablename__ = "change_log"
    __table_args__ = {"schema": "audit"}
    
    # id: Optional[str] = Field(default=None, primary_key=True, description="Unique identifier")
    changed_at: datetime = Field(default_factory=datetime.utcnow, description="Timestamp of the change")


class ChangeLogCreate(ChangeLogBase):
    """Schema for creating change log records."""
    pass


class ChangeLogRead(ChangeLogBase):
    """Schema for reading change log records."""
    change_id: int
    changed_at: datetime


class ChangeLogUpdate(SQLModel):
    """Schema for updating change log records."""
    table_name: Optional[str] = Field(default=None, max_length=100)
    record_id: Optional[str] = Field(default=None, max_length=50)
    operation: Optional[str] = Field(default=None, max_length=10)
    old_values: Optional[Dict[str, Any]] = Field(default=None)
    new_values: Optional[Dict[str, Any]] = Field(default=None)
    changed_by: Optional[str] = Field(default=None, max_length=100)
    change_reason: Optional[str] = Field(default=None, max_length=255)