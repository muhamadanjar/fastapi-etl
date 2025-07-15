from typing import Optional
from datetime import datetime
from pydantic import BaseModel


class ETLJobCreate(BaseModel):
    job_name: str
    job_type: Optional[str] = None
    job_category: Optional[str] = None
    source_type: Optional[str] = None
    target_schema: Optional[str] = None
    target_table: Optional[str] = None
    job_config: Optional[dict] = {}
    schedule_expression: Optional[str] = None
    is_active: Optional[bool] = True


class ETLJobRead(ETLJobCreate):
    job_id: int
    created_at: datetime


class JobExecutionCreate(BaseModel):
    job_id: int
    batch_id: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: Optional[str] = None
    records_processed: Optional[int] = 0
    records_successful: Optional[int] = 0
    records_failed: Optional[int] = 0
    execution_log: Optional[str] = None
    error_details: Optional[dict] = {}
    performance_metrics: Optional[dict] = {}

class JobExecutionRead(JobExecutionCreate):
    execution_id: int
    created_at: datetime