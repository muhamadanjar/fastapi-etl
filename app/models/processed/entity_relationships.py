from sqlmodel import SQLModel, Field
from typing import Optional, Dict
from datetime import datetime

class EntityRelationship(SQLModel, table=True):
    __tablename__ = "entity_relationships"
    __table_args__ = {"schema": "processed"}

    relationship_id: Optional[int] = Field(default=None, primary_key=True)
    entity_from: int = Field(foreign_key="processed.entities.entity_id")
    entity_to: int = Field(foreign_key="processed.entities.entity_id")
    relationship_type: str
    relationship_strength: Optional[float]
    metadata: Optional[Dict] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
