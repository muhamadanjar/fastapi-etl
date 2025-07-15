from sqlmodel import SQLModel, Field
from typing import Optional, List
from datetime import datetime

class Entity(SQLModel, table=True):
    __tablename__ = "entities"
    __table_args__ = {"schema": "processed"}

    entity_id: Optional[int] = Field(default=None, primary_key=True)
    entity_type: str
    entity_key: str
    entity_data: dict
    source_files: Optional[List[int]] = Field(default_factory=list)
    confidence_score: Optional[float]
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    version: int = 1
    is_active: bool = True