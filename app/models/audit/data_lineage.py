from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime

class DataLineage(SQLModel, table=True):
    __tablename__ = "data_lineage"
    __table_args__ = {"schema": "audit"}

    lineage_id: Optional[int] = Field(default=None, primary_key=True)
    source_entity: str
    source_field: str
    target_entity: str
    target_field: str
    transformation_applied: Optional[str]
    execution_id: int = Field(foreign_key="etl_control.job_executions.execution_id")
    created_at: datetime = Field(default_factory=datetime.utcnow)
