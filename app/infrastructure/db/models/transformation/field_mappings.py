from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field

from app.models.base import BaseModel


class FieldMappingBase(BaseModel):
    """Base model for field mappings."""
    source_entity: Optional[str] = Field(default=None, max_length=100, description="Source entity name")
    source_field: Optional[str] = Field(default=None, max_length=100, description="Source field name")
    target_entity: Optional[str] = Field(default=None, max_length=100, description="Target entity name")
    target_field: Optional[str] = Field(default=None, max_length=100, description="Target field name")
    mapping_type: Optional[str] = Field(
        default=None, 
        max_length=50, 
        description="Type of mapping: DIRECT, CALCULATED, LOOKUP"
    )
    mapping_expression: Optional[str] = Field(default=None, description="Expression for the mapping")
    data_type: Optional[str] = Field(default=None, max_length=50, description="Data type of the field")
    is_required: bool = Field(default=False, description="Whether the field is required")
    default_value: Optional[str] = Field(default=None, max_length=255, description="Default value for the field")


class FieldMapping(FieldMappingBase, table=True):
    """Field mapping model for database storage."""
    __tablename__ = "field_mappings"
    __table_args__ = {"schema": "transformation"}
    
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation timestamp")


class FieldMappingCreate(FieldMappingBase):
    """Schema for creating field mappings."""
    pass


class FieldMappingRead(FieldMappingBase):
    """Schema for reading field mappings."""
    mapping_id: int
    created_at: datetime


class FieldMappingUpdate(SQLModel):
    """Schema for updating field mappings."""
    source_entity: Optional[str] = Field(default=None, max_length=100)
    source_field: Optional[str] = Field(default=None, max_length=100)
    target_entity: Optional[str] = Field(default=None, max_length=100)
    target_field: Optional[str] = Field(default=None, max_length=100)
    mapping_type: Optional[str] = Field(default=None, max_length=50)
    mapping_expression: Optional[str] = Field(default=None)
    data_type: Optional[str] = Field(default=None, max_length=50)
    is_required: Optional[bool] = Field(default=None)
    default_value: Optional[str] = Field(default=None, max_length=255)