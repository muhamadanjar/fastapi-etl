from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime

class FieldMapping(SQLModel, table=True):
    __tablename__ = "field_mappings"
    __table_args__ = {"schema": "transformation"}

    mapping_id: Optional[int] = Field(default=None, primary_key=True)
    source_entity: str
    source_field: str
    target_entity: str
    target_field: str
    mapping_type: Optional[str]
    mapping_expression: Optional[str]
    data_type: Optional[str]
    is_required: bool = False
    default_value: Optional[str]
    created_at: datetime = Field(default_factory=datetime.utcnow)
