from sqlmodel import SQLModel, Field, Relationship
from typing import Optional
from datetime import datetime

class JobExecution(SQLModel, table=True):
    __tablename__ = "job_executions"
    __table_args__ = {"schema": "etl_control"}

    execution_id: Optional[int] = Field(default=None, primary_key=True)
    job_id: int = Field(foreign_key="etl_control.etl_jobs.job_id")
    batch_id: Optional[str]
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    status: Optional[str]
    records_processed: Optional[int] = 0
    records_successful: Optional[int] = 0
    records_failed: Optional[int] = 0
    execution_log: Optional[str]
    error_details: Optional[dict] = Field(default_factory=dict)
    performance_metrics: Optional[dict] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)