from datetime import datetime
from typing import Optional, List, Dict, Any
from decimal import Decimal
from app.models.base import BaseModel
from sqlmodel import SQLModel, Field, JSON, Column
from sqlalchemy import ARRAY, Integer, text
from sqlalchemy.dialects.postgresql import JSONB


class EntityBase(BaseModel):
    """Base model untuk Entity dengan field-field umum"""
    entity_type: str = Field(max_length=100, index=True)
    entity_key: str = Field(max_length=255, index=True)
    entity_data: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB))
    source_files: Optional[List[int]] = Field(default=None, sa_column=Column(ARRAY(Integer)))
    confidence_score: Optional[Decimal] = Field(default=None, max_digits=3, decimal_places=2)
    version: int = Field(default=1)
    is_active: bool = Field(default=True)


class Entity(EntityBase, table=True):
    """Model untuk tabel processed.entities"""
    __tablename__ = "entities"
    __table_args__ = (
        {"schema": "processed"},
    )
    
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        schema_extra = {
            "example": {
                "entity_type": "PERSON",
                "entity_key": "john_doe_001",
                "entity_data": {
                    "name": "John Doe",
                    "email": "john.doe@example.com",
                    "age": 30,
                    "address": {
                        "street": "123 Main St",
                        "city": "New York",
                        "country": "USA"
                    }
                },
                "source_files": [1, 2, 3],
                "confidence_score": 0.95,
                "version": 1,
                "is_active": True
            }
        }


class EntityCreate(EntityBase):
    """Schema untuk create entity"""
    pass


class EntityUpdate(SQLModel):
    """Schema untuk update entity"""
    entity_type: Optional[str] = Field(default=None, max_length=100)
    entity_key: Optional[str] = Field(default=None, max_length=255)
    entity_data: Optional[Dict[str, Any]] = Field(default=None)
    source_files: Optional[List[int]] = Field(default=None)
    confidence_score: Optional[Decimal] = Field(default=None, max_digits=3, decimal_places=2)
    version: Optional[int] = Field(default=None)
    is_active: Optional[bool] = Field(default=None)


class EntityRead(EntityBase):
    """Schema untuk read entity"""
    last_updated: datetime


class EntityReadWithRelations(EntityRead):
    """Schema untuk read entity dengan relasi"""
    # Akan digunakan untuk include relationships jika diperlukan
    relationships_from: Optional[List["EntityRelationship"]] = Field(default=None)
    relationships_to: Optional[List["EntityRelationship"]] = Field(default=None)