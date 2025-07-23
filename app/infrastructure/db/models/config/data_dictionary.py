from datetime import datetime
from typing import Optional, List
from uuid import UUID
from sqlmodel import SQLModel, Field, Column
from sqlalchemy import ARRAY, String

from app.infrastructure.db.models.base import BaseModel, TimestampMixin


class DataDictionaryBase(BaseModel):
    """Base model for data dictionary."""
    entity_name: Optional[str] = Field(default=None, max_length=100, description="Name of the entity")
    field_name: Optional[str] = Field(default=None, max_length=100, description="Name of the field")
    field_type: Optional[str] = Field(default=None, max_length=50, description="Type of the field")
    field_description: Optional[str] = Field(default=None, description="Description of the field")
    business_rules: Optional[str] = Field(default=None, description="Business rules for the field")
    sample_values: Optional[List[str]] = Field(
        default=None, 
        sa_column=Column(ARRAY(String)),
        description="Sample values for the field"
    )


class DataDictionary(DataDictionaryBase, TimestampMixin, table=True):
    """Data dictionary model for database storage."""
    __tablename__ = "data_dictionary"
    __table_args__ = {"schema": "config"}
    


class DataDictionaryCreate(DataDictionaryBase):
    """Schema for creating data dictionary entries."""
    pass


class DataDictionaryRead(DataDictionaryBase):
    """Schema for reading data dictionary entries."""
    dict_id: UUID
    created_at: datetime


class DataDictionaryUpdate(SQLModel):
    """Schema for updating data dictionary entries."""
    entity_name: Optional[str] = Field(default=None, max_length=100)
    field_name: Optional[str] = Field(default=None, max_length=100)
    field_type: Optional[str] = Field(default=None, max_length=50)
    field_description: Optional[str] = Field(default=None)
    business_rules: Optional[str] = Field(default=None)
    sample_values: Optional[List[str]] = Field(default=None)
