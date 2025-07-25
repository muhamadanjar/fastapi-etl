from typing import Optional
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel

class QualityRuleResponse(BaseModel):
    """Schema for quality rule response."""
    rule_id: UUID
    rule_name: str
    rule_type: Optional[str] = None
    entity_type: Optional[str] = None
    field_name: Optional[str] = None
    rule_expression: Optional[str] = None
    error_threshold: Optional[float] = None
    is_active: bool = True
    created_at: datetime

class QualityRuleCreate(BaseModel):
    rule_name: str
    rule_type: Optional[str] = None
    entity_type: Optional[str] = None
    field_name: Optional[str] = None
    rule_expression: Optional[str] = None
    error_threshold: Optional[float] = None
    is_active: Optional[bool] = True

class QualityRuleRead(QualityRuleCreate):
    rule_id: UUID
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
    check_id: UUID
    created_at: datetime


class QualityRuleUpdate(BaseModel):
    rule_name: Optional[str] = None
    rule_type: Optional[str] = None
    entity_type: Optional[str] = None
    field_name: Optional[str] = None
    rule_expression: Optional[str] = None
    error_threshold: Optional[float] = None
    is_active: Optional[bool] = None


class QualityReport(BaseModel):
    """Schema for quality report."""
    execution_id: int
    rule_id: int
    check_result: str
    records_checked: int
    records_passed: int
    records_failed: int
    failure_details: Optional[dict] = {}
    created_at: datetime

    class Config:
        json_schema_extra = {
            "example": {
                "execution_id": 1,
                "rule_id": 1,
                "check_result": "FAIL",
                "records_checked": 1000,
                "records_passed": 950,
                "records_failed": 50,
                "failure_details": {}
            }
        }
