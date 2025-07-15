from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime

class ETLJob(SQLModel, table=True):
    __tablename__ = "etl_jobs"
    __table_args__ = {"schema": "etl_control"}

    job_id: Optional[int] = Field(default=None, primary_key=True)
    job_name: str
    job_type: Optional[str]
    job_category: Optional[str]
    source_type: Optional[str]
    target_schema: Optional[str]
    target_table: Optional[str]
    job_config: Optional[dict] = Field(default_factory=dict)
    schedule_expression: Optional[str]
    is_active: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)