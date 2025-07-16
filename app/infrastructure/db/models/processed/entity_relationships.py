from datetime import datetime
from typing import Optional, Dict, Any
from decimal import Decimal

from sqlmodel import SQLModel, Field, Relationship, Column
from sqlalchemy.dialects.postgresql import JSONB

from app.models.base import BaseModel


class EntityRelationshipBase(BaseModel):
    """Base model untuk EntityRelationship dengan field-field umum"""
    entity_from: int = Field(foreign_key="processed.entities.entity_id")
    entity_to: int = Field(foreign_key="processed.entities.entity_id")
    relationship_type: str = Field(max_length=100)
    relationship_strength: Optional[Decimal] = Field(default=None, max_digits=3, decimal_places=2)
    metadata: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB))


class EntityRelationship(EntityRelationshipBase, table=True):
    """Model untuk tabel processed.entity_relationships"""
    __tablename__ = "entity_relationships"
    __table_args__ = (
        {"schema": "processed"},
    )
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Relationships - akan digunakan jika diperlukan joins
    from_entity: Optional["Entity"] = Relationship(
        foreign_keys=[entity_from],
        sa_relationship_kwargs={"foreign_keys": "EntityRelationship.entity_from"}
    )
    to_entity: Optional["Entity"] = Relationship(
        foreign_keys=[entity_to],
        sa_relationship_kwargs={"foreign_keys": "EntityRelationship.entity_to"}
    )
    
    class Config:
        schema_extra = {
            "example": {
                "entity_from": 1,
                "entity_to": 2,
                "relationship_type": "OWNS",
                "relationship_strength": 0.85,
                "metadata": {
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
    entity_from: Optional[int] = Field(default=None)
    entity_to: Optional[int] = Field(default=None)
    relationship_type: Optional[str] = Field(default=None, max_length=100)
    relationship_strength: Optional[Decimal] = Field(default=None, max_digits=3, decimal_places=2)
    metadata: Optional[Dict[str, Any]] = Field(default=None)


class EntityRelationshipRead(EntityRelationshipBase):
    """Schema untuk read entity relationship"""
    relationship_id: int
    created_at: datetime


class EntityRelationshipReadWithEntities(EntityRelationshipRead):
    """Schema untuk read entity relationship dengan detail entitas"""
    from_entity: Optional["EntityRead"] = Field(default=None)
    to_entity: Optional["EntityRead"] = Field(default=None)


# Import untuk menghindari circular imports
from .entities import Entity, EntityRead