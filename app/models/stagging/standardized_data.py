from sqlmodel import SQLModel, Field
from typing import Optional, List
from datetime import datetime

class StandardizedData(SQLModel, table=True):
    __tablename__ = "standardized_data"
    __table_args__ = {"schema": "staging"}

    staging_id: Optional[int] = Field(default=None, primary_key=True)
    source_file_id: int = Field(foreign_key="raw_data.file_registry.file_id")
    source_record_id: int = Field(foreign_key="raw_data.raw_records.record_id")
    entity_type: Optional[str]
    standardized_data: dict
    quality_score: Optional[float]
    transformation_rules_applied: Optional[List[str]] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    batch_id: Optional[str]
    