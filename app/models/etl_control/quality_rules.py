from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime

class QualityRule(SQLModel, table=True):
    __tablename__ = "quality_rules"
    __table_args__ = {"schema": "etl_control"}

    rule_id: Optional[int] = Field(default=None, primary_key=True)
    rule_name: str
    rule_type: Optional[str]
    entity_type: Optional[str]
    field_name: Optional[str]
    rule_expression: Optional[str]
    error_threshold: Optional[float]
    is_active: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)