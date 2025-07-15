from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime

class QualityCheckResult(SQLModel, table=True):
    __tablename__ = "quality_check_results"
    __table_args__ = {"schema": "etl_control"}

    check_id: Optional[int] = Field(default=None, primary_key=True)
    execution_id: int = Field(foreign_key="etl_control.job_executions.execution_id")
    rule_id: int = Field(foreign_key="etl_control.quality_rules.rule_id")
    check_result: Optional[str]
    records_checked: Optional[int]
    records_passed: Optional[int]
    records_failed: Optional[int]
    failure_details: Optional[dict] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    