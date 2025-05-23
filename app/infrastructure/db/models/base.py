"""
Base SQLModel models with common functionality.

This module provides base classes for all database models
with common fields like timestamps, soft deletes, and UUIDs.
"""

from datetime import datetime
from typing import Optional, Any, Dict
from uuid import UUID, uuid4

from sqlmodel import SQLModel, Field
from sqlalchemy import Column, DateTime, func


class BaseModel(SQLModel):
    """
    Base model class with common fields and functionality.
    
    Provides:
    - Primary key with UUID
    - Created and updated timestamps
    - Soft delete functionality
    - Version field for optimistic locking
    """
    
    # Primary key
    id: Optional[UUID] = Field(
        default_factory=uuid4,
        primary_key=True,
        index=True,
        nullable=False
    )
    
    # Timestamps
    created_at: Optional[datetime] = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(DateTime(timezone=True), server_default=func.now()),
        nullable=False
    )
    
    updated_at: Optional[datetime] = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
        nullable=False
    )
    
    # Soft delete
    is_deleted: bool = Field(default=False, index=True)
    deleted_at: Optional[datetime] = Field(default=None, index=True)
    
    # Optimistic locking
    version: int = Field(default=1, nullable=False)
    
    def mark_as_deleted(self) -> None:
        """Mark record as soft deleted."""
        self.is_deleted = True
        self.deleted_at = datetime.utcnow()
        self.version += 1
    
    def restore(self) -> None:
        """Restore soft deleted record."""
        self.is_deleted = False
        self.deleted_at = None
        self.version += 1
    
    def update_version(self) -> None:
        """Increment version for optimistic locking."""
        self.version += 1
    
    def to_dict(self, exclude: Optional[list] = None) -> Dict[str, Any]:
        """
        Convert model to dictionary.
        
        Args:
            exclude: List of fields to exclude from dictionary
            
        Returns:
            Dictionary representation of model
        """
        exclude = exclude or []
        result = {}
        
        for field_name, field_info in self.__fields__.items():
            if field_name not in exclude:
                value = getattr(self, field_name)
                
                # Handle UUID serialization
                if isinstance(value, UUID):
                    value = str(value)
                # Handle datetime serialization
                elif isinstance(value, datetime):
                    value = value.isoformat()
                
                result[field_name] = value
        
        return result
    
    class Config:
        # Enable ORM mode for FastAPI response models
        from_attributes = True
        # Generate schema with examples
        schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174000",
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
                "is_deleted": False,
                "version": 1
            }
        }


class TimestampMixin(SQLModel):
    """
    Mixin for models that only need timestamp fields.
    
    Useful for models that don't need full BaseModel functionality.
    """
    
    created_at: Optional[datetime] = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(DateTime(timezone=True), server_default=func.now()),
        nullable=False
    )
    
    updated_at: Optional[datetime] = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
        nullable=False
    )


class SoftDeleteMixin(SQLModel):
    """
    Mixin for soft delete functionality.
    
    Can be added to models that need soft delete without BaseModel.
    """
    
    is_deleted: bool = Field(default=False, index=True)
    deleted_at: Optional[datetime] = Field(default=None, index=True)
    
    def mark_as_deleted(self) -> None:
        """Mark record as soft deleted."""
        self.is_deleted = True
        self.deleted_at = datetime.utcnow()
    
    def restore(self) -> None:
        """Restore soft deleted record."""
        self.is_deleted = False
        self.deleted_at = None


class AuditMixin(SQLModel):
    """
    Mixin for auditable models with created_by and updated_by fields.
    """
    
    created_by: Optional[UUID] = Field(default=None, index=True)
    updated_by: Optional[UUID] = Field(default=None, index=True)
    
    def update_audit_fields(self, user_id: Optional[UUID]) -> None:
        """
        Update audit fields with user information.
        
        Args:
            user_id: ID of user making the change
        """
        if not self.created_by:
            self.created_by = user_id
        
        self.updated_by = user_id