from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime

class TransformationRule(SQLModel, table=True):
    __tablename__ = "transformation_rules"
    __table_args__ = {"schema": "transformation"}

    rule_id: Optional[int] = Field(default=None, primary_key=True)
    rule_name: str
    source_format: Optional[str]
    target_format: Optional[str]
    transformation_type: Optional[str]
    rule_logic: Optional[str]
    rule_parameters: Optional[dict] = Field(default_factory=dict)
    priority: int = 1
    is_active: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)
