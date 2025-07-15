from typing import Optional
from datetime import datetime
from pydantic import BaseModel

class QualityRuleCreate(BaseModel):
    rule_name: str
    rule_type: Optional[str] = None
    entity_type: Optional[str] = None
    field_name: Optional[str] = None
    rule_expression: Optional[str] = None
    error_threshold: Optional[float] = None
    is_active: Optional[bool] = True

class QualityRuleRead(QualityRuleCreate):
    rule_id: int
    created_at: datetime


# app/schemas/quality_check_result_schemas.py
from typing import Optional
from datetime import datetime
from pydantic import BaseModel

class QualityCheckResultCreate(BaseModel):
    execution_id: int
    rule_id: int
    check_result: Optional[str] = None
    records_checked: Optional[int] = None
    records_passed: Optional[int] = None
    records_failed: Optional[int] = None
    failure_details: Optional[dict] = {}

class QualityCheckResultRead(QualityCheckResultCreate):
    check_id: int
    created_at: datetime