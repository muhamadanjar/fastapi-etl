from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum
from uuid import UUID

from sqlmodel import SQLModel, Field, Column, Relationship
from sqlalchemy.dialects.postgresql import JSONB


class CheckResult(str, Enum):
    """Enum untuk check result"""
    PASS = "PASS"
    FAIL = "FAIL"
    WARNING = "WARNING"
    SKIP = "SKIP"
    ERROR = "ERROR"


class QualityCheckResultBase(SQLModel):
    """Base model untuk QualityCheckResult dengan field-field umum"""
    execution_id: UUID = Field(foreign_key="etl_control.job_executions.id", index=True)
    rule_id: UUID = Field(foreign_key="etl_control.quality_rules.id", index=True)
    check_result: CheckResult = Field(index=True)
    records_checked: Optional[int] = Field(default=0)
    records_passed: Optional[int] = Field(default=0)
    records_failed: Optional[int] = Field(default=0)
    failure_details: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB))


class QualityCheckResult(QualityCheckResultBase, table=True):
    """Model untuk tabel etl_control.quality_check_results"""
    __tablename__ = "quality_check_results"
    __table_args__ = (
        {"schema": "etl_control"},
    )
    
    check_id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.now)
    rule_id: UUID = Field(foreign_key="etl_control.quality_rules.id", index=True)
    execution_id: UUID = Field(foreign_key="etl_control.job_executions.id", index=True)
    
    # Relationships
    execution: Optional["JobExecution"] = Relationship(back_populates="quality_check_results")
    rule: Optional["QualityRule"] = Relationship(back_populates="quality_check_results")
    
    class Config:
        schema_extra = {
            "example": {
                "execution_id": 1,
                "rule_id": 1,
                "check_result": "FAIL",
                "records_checked": 1000,
                "records_passed": 950,
                "records_failed": 50,
                "failure_details": {
                    "failure_rate": 0.05,
                    "threshold_exceeded": True,
                    "error_threshold": 0.02,
                    "failed_records": [
                        {
                            "record_id": 123,
                            "field": "email",
                            "value": "invalid-email",
                            "error": "Invalid email format"
                        },
                        {
                            "record_id": 456,
                            "field": "email",
                            "value": "test@",
                            "error": "Incomplete email domain"
                        }
                    ],
                    "error_categories": {
                        "invalid_format": 30,
                        "incomplete_data": 20
                    },
                    "recommendations": [
                        "Review email validation rules",
                        "Implement data cleansing before validation"
                    ]
                }
            }
        }


class QualityCheckResultCreate(QualityCheckResultBase):
    """Schema untuk create quality check result"""

class QualityCheckResultUpdate(SQLModel):
    """Schema untuk update quality check result"""
    check_result: Optional[CheckResult] = Field(default=None)
    records_checked: Optional[int] = Field(default=None)
    records_passed: Optional[int] = Field(default=None)
    records_failed: Optional[int] = Field(default=None)
    failure_details: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB))
    execution_id: Optional[UUID] = Field(default=None, foreign_key="etl_control.job_executions.execution_id")
    rule_id: Optional[int] = Field(default=None, foreign_key="etl_control.quality_rules.rule_id")
    created_at: Optional[datetime] = Field(default=None)
    check_id: Optional[int] = Field(default=None, primary_key=True)

    class Config:
        schema_extra = {
            "example": {
                "check_result": "PASS",
                "records_checked": 1000,
                "records_passed": 980,
                "records_failed": 20,
                "failure_details": {
                    "error_categories": {
                        "missing_data": 10,
                        "format_errors": 5,
                        "duplicate_records": 5
                    },
                    "recommendations": [
                        "Improve data entry validation",
                        "Implement deduplication logic"
                    ]
                },
                "execution_id": 1,
                "rule_id": 1,
                "created_at": "2024-01-15T10:30:00",
                "check_id": 1
            }
        }
    
class QualityCheckResultRead(QualityCheckResultBase):
    """Schema untuk read quality check result"""
    check_id: int = Field(primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        schema_extra = {
            "example": {
                "check_id": 1,
                "execution_id": 1,
                "rule_id": 1,
                "check_result": "FAIL",
                "records_checked": 1000,
                "records_passed": 950,
                "records_failed": 50,
                "failure_details": {
                    "failure_rate": 0.05,
                    "threshold_exceeded": True,
                    "error_threshold": 0.02,
                    "failed_records": [
                        {
                            "record_id": 123,
                            "field": "email",
                            "value": "invalid-email",
                            "error": "Invalid email format"
                        }
                    ],
                    "error_categories": {
                        "invalid_format": 30,
                        "incomplete_data": 20
                    },
                    "recommendations": [
                        "Review email validation rules",
                        "Implement data cleansing before validation"
                    ]
                },
                "created_at": "2024-01-15T10:30:00"
            }
        }
