from typing import Optional, Dict, Any, List
from datetime import datetime
from pydantic import BaseModel, Field
from enum import Enum

from app.core.enums import EntityType, RelationshipType
from .base import BaseResponse




class EntityCreate(BaseModel):
    """Schema for creating entities."""
    entity_type: EntityType
    entity_key: str = Field(min_length=1, max_length=255, description="Unique identifier for the entity")
    entity_data: Dict[str, Any] = Field(description="Entity data in JSON format")
    source_files: Optional[List[int]] = Field(default=None, description="Source file IDs")
    confidence_score: Optional[float] = Field(default=1.0, ge=0, le=1, description="Confidence score for entity")
    tags: Optional[List[str]] = Field(default=None, description="Tags for categorization")
    description: Optional[str] = Field(default=None, max_length=1000)
    external_id: Optional[str] = Field(default=None, max_length=255, description="External system ID")


class EntityRead(BaseModel):
    """Schema for reading entities."""
    entity_id: int
    entity_type: EntityType
    entity_key: str
    entity_data: Dict[str, Any]
    source_files: Optional[List[int]] = None
    confidence_score: float
    last_updated: datetime
    version: int
    is_active: bool
    tags: Optional[List[str]] = None
    description: Optional[str] = None
    external_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    

class EntityUpdate(BaseModel):
    """Schema for updating entities."""
    entity_data: Optional[Dict[str, Any]] = None
    source_files: Optional[List[int]] = None
    confidence_score: Optional[float] = Field(default=None, ge=0, le=1)
    is_active: Optional[bool] = None
    tags: Optional[List[str]] = None
    description: Optional[str] = Field(default=None, max_length=1000)
    external_id: Optional[str] = Field(default=None, max_length=255)


class EntityRelationshipCreate(BaseModel):
    """Schema for creating entity relationships."""
    entity_from: int = Field(description="Source entity ID")
    entity_to: int = Field(description="Target entity ID")
    relationship_type: RelationshipType
    relationship_strength: Optional[float] = Field(default=1.0, ge=0, le=1, description="Strength of relationship")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Additional relationship metadata")
    description: Optional[str] = Field(default=None, max_length=500)
    is_bidirectional: bool = Field(default=False, description="Whether relationship is bidirectional")


class EntityRelationshipRead(BaseModel):
    """Schema for reading entity relationships."""
    relationship_id: int
    entity_from: int
    entity_to: int
    relationship_type: RelationshipType
    relationship_strength: float
    metadata: Optional[Dict[str, Any]] = None
    description: Optional[str] = None
    is_bidirectional: bool
    created_at: datetime
    updated_at: datetime


class EntityRelationshipUpdate(BaseModel):
    """Schema for updating entity relationships."""
    relationship_type: Optional[RelationshipType] = None
    relationship_strength: Optional[float] = Field(default=None, ge=0, le=1)
    metadata: Optional[Dict[str, Any]] = None
    description: Optional[str] = Field(default=None, max_length=500)
    is_bidirectional: Optional[bool] = None


class EntitySearch(BaseModel):
    """Schema for entity search."""
    entity_type: Optional[EntityType] = None
    search_query: Optional[str] = Field(default=None, min_length=1, max_length=500)
    fields: Optional[List[str]] = Field(default=None, description="Fields to search in")
    confidence_threshold: Optional[float] = Field(default=0.0, ge=0, le=1)
    tags: Optional[List[str]] = None
    source_files: Optional[List[int]] = None
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None
    include_inactive: bool = Field(default=False, description="Include inactive entities")
    fuzzy_search: bool = Field(default=False, description="Enable fuzzy search")
    similarity_threshold: Optional[float] = Field(default=0.8, ge=0, le=1)


class EntitySearchResult(BaseModel):
    """Schema for entity search results."""
    entity: EntityRead
    # relevance_score