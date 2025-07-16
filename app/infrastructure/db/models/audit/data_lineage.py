from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field, Relationship

from app.models.base import BaseModel


class DataLineageBase(BaseModel):
    """Base model for data lineage."""
    source_entity: Optional[str] = Field(default=None, max_length=100, description="Source entity name")
    source_field: Optional[str] = Field(default=None, max_length=100, description="Source field name")
    target_entity: Optional[str] = Field(default=None, max_length=100, description="Target entity name")
    target_field: Optional[str] = Field(default=None, max_length=100, description="Target field name")
    transformation_applied: Optional[str] = Field(default=None, description="Transformation logic applied")
    execution_id: Optional[int] = Field(
        default=None, 
        foreign_key="etl_control.job_executions.execution_id",
        description="Reference to job execution"
    )


class DataLineage(DataLineageBase, table=True):
    """Data lineage model for database storage."""
    __tablename__ = "data_lineage"
    __table_args__ = {"schema": "audit"}
    
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation timestamp")


class DataLineageCreate(DataLineageBase):
    """Schema for creating data lineage records."""
    pass


class DataLineageRead(DataLineageBase):
    """Schema for reading data lineage records."""
    lineage_id: int
    created_at: datetime


class DataLineageUpdate(SQLModel):
    """Schema for updating data lineage records."""
    source_entity: Optional[str] = Field(default=None, max_length=100)
    source_field: Optional[str] = Field(default=None, max_length=100)
    target_entity: Optional[str] = Field(default=None, max_length=100)
    target_field: Optional[str] = Field(default=None, max_length=100)
    transformation_applied: Optional[str] = Field(default=None)
    execution_id: Optional[int] = Field(default=None)