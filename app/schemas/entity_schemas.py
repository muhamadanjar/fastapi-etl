


# app/schemas/entity_schemas.py
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel

class EntityCreate(BaseModel):
    entity_type: str
    entity_key: str
    entity_data: dict
    source_files: Optional[List[int]] = []
    confidence_score: Optional[float] = None
    version: Optional[int] = 1
    is_active: Optional[bool] = True

class EntityRead(EntityCreate):
    entity_id: int
    last_updated: datetime