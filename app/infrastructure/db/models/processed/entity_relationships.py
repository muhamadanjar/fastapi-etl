from datetime import datetime
from typing import Optional, Dict, Any
from decimal import Decimal
from uuid import UUID

from sqlmodel import SQLModel, Field, Relationship, Column
from sqlalchemy.dialects.postgresql import JSONB

from app.infrastructure.db.models.base import BaseModel


class EntityRelationshipBase(BaseModel):
    """Base model untuk EntityRelationship dengan field-field umum"""
    entity_from: UUID = Field(foreign_key="processed.entities.id")
    entity_to: UUID = Field(foreign_key="processed.entities.id")
    relationship_type: str = Field(max_length=100)
    relationship_strength: Optional[Decimal] = Field(default=None, max_digits=3, decimal_places=2)
    relationship_metadata: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB))


class EntityRelationship(EntityRelationshipBase, table=True):
    """Model untuk tabel processed.entity_relationships"""
    __tablename__ = "entity_relationships"
    __table_args__ = (
        {"schema": "processed"},
    )
    
    created_at: datetime = Field(default_factory=datetime.now)
    
    # Relationships - akan digunakan jika diperlukan joins
    from_entity: Optional["Entity"] = Relationship(
        back_populates="relationships_from",
        sa_relationship_kwargs={"foreign_keys": "[EntityRelationship.entity_from]"},
    )
    to_entity: Optional["Entity"] = Relationship(
        back_populates="relationships_to",
        sa_relationship_kwargs={"foreign_keys": "[EntityRelationship.entity_to]"},
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "entity_from": 1,
                "entity_to": 2,
                "relationship_type": "OWNS",
                "relationship_strength": 0.85,
                "relationship_metadata": {
                    "relationship_context": "business_ownership",
                    "confidence_factors": ["document_evidence", "public_records"],
                    "source_documents": ["doc_001", "doc_002"],
                    "verification_date": "2024-01-15"
                }
            }
        }


class EntityRelationshipCreate(EntityRelationshipBase):
    """Schema untuk create entity relationship"""
    pass


class EntityRelationshipUpdate(SQLModel):
    """Schema untuk update entity relationship"""
    entity_from: Optional[UUID] = Field(default=None)
    entity_to: Optional[UUID] = Field(default=None)
    relationship_type: Optional[str] = Field(default=None, max_length=100)
    relationship_strength: Optional[Decimal] = Field(default=None, max_digits=3, decimal_places=2)
    relationship_metadata: Optional[Dict[str, Any]] = Field(default=None)


class EntityRelationshipRead(EntityRelationshipBase):
    """Schema untuk read entity relationship"""
    relationship_id: UUID
    created_at: datetime


class EntityRelationshipReadWithEntities(EntityRelationshipRead):
    """Schema untuk read entity relationship dengan detail entitas"""
    from_entity: Optional["EntityRead"] = Field(default=None)
    to_entity: Optional["EntityRead"] = Field(default=None)


# Import untuk menghindari circular imports
from .entities import Entity, EntityRead