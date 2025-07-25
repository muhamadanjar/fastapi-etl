from datetime import datetime
from typing import Optional, List, Dict, Any
from decimal import Decimal
from uuid import UUID
from enum import Enum
from sqlmodel import SQLModel, Field, Relationship
from app.core.enums import QualityRuleType
from app.infrastructure.db.models.base import BaseModel




class QualityRuleBase(BaseModel):
    """Base model untuk QualityRule dengan field-field umum"""
    rule_name: str = Field(max_length=100, index=True, unique=True)
    rule_type: QualityRuleType = Field(index=True)
    entity_type: Optional[str] = Field(default=None, max_length=100, index=True)
    field_name: Optional[str] = Field(default=None, max_length=100, index=True)
    rule_expression: str = Field(description="SQL expression atau rule logic")
    error_threshold: Optional[Decimal] = Field(default=None, max_digits=3, decimal_places=2)
    is_active: bool = Field(default=True, index=True)


class QualityRule(QualityRuleBase, table=True):
    """Model untuk tabel etl_control.quality_rules"""
    __tablename__ = "quality_rules"
    __table_args__ = (
        {"schema": "etl_control"},
    )
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Relationships
    quality_check_results: List["QualityCheckResult"] = Relationship(back_populates="rule")
    
    class Config:
        json_schema_extra = {
            "example": {
                "rule_name": "email_format_validation",
                "rule_type": "VALIDITY",
                "entity_type": "PERSON",
                "field_name": "email",
                "rule_expression": "email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'",
                "error_threshold": 0.05,
                "is_active": True
            }
        }


class QualityRuleCreate(QualityRuleBase):
    """Schema untuk create quality rule"""
    pass


class QualityRuleUpdate(SQLModel):
    """Schema untuk update quality rule"""
    rule_name: Optional[str] = Field(default=None, max_length=100)
    rule_type: Optional[QualityRuleType] = Field(default=None)
    entity_type: Optional[str] = Field(default=None, max_length=100)
    field_name: Optional[str] = Field(default=None, max_length=100)
    rule_expression: Optional[str] = Field(default=None)
    error_threshold: Optional[Decimal] = Field(default=None, max_digits=3, decimal_places=2)
    is_active: Optional[bool] = Field(default=None)


class QualityRuleRead(QualityRuleBase):
    """Schema untuk read quality rule"""
    rule_id: UUID
    created_at: datetime


class QualityRuleReadWithResults(QualityRuleRead):
    """Schema untuk read quality rule dengan check results"""
    quality_check_results: Optional[List["QualityCheckResultRead"]] = Field(default=None)


class QualityRuleFilter(SQLModel):
    """Schema untuk filter quality rule"""
    rule_name: Optional[str] = Field(default=None)
    rule_type: Optional[QualityRuleType] = Field(default=None)
    entity_type: Optional[str] = Field(default=None)
    field_name: Optional[str] = Field(default=None)
    is_active: Optional[bool] = Field(default=None)
    search_term: Optional[str] = Field(default=None)


class QualityRuleSummary(SQLModel):
    """Schema untuk summary quality rule"""
    rule_type: QualityRuleType
    total_rules: int
    active_rules: int
    inactive_rules: int
    avg_error_threshold: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=2)
    most_common_entity_types: Optional[List[str]] = Field(default=None)


class QualityRuleValidation(SQLModel):
    """Schema untuk validasi quality rule"""
    rule_id: int
    validation_type: str  # 'syntax', 'logic', 'performance'
    is_valid: bool
    validation_errors: Optional[List[str]] = Field(default=None)
    validation_warnings: Optional[List[str]] = Field(default=None)
    estimated_performance: Optional[str] = Field(default=None)  # 'fast', 'medium', 'slow'
    validated_at: datetime = Field(default_factory=datetime.utcnow)


class QualityRuleTemplate(SQLModel):
    """Schema untuk template quality rule"""
    template_name: str = Field(max_length=100)
    template_description: Optional[str] = Field(default=None)
    rule_type: QualityRuleType
    template_expression: str
    parameters: Optional[Dict[str, Any]] = Field(default=None)
    applicable_fields: Optional[List[str]] = Field(default=None)
    applicable_entities: Optional[List[str]] = Field(default=None)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class QualityRuleBulkCreate(SQLModel):
    """Schema untuk bulk create quality rule"""
    rules: List[QualityRuleCreate]
    apply_to_entity_types: Optional[List[str]] = Field(default=None)
    default_error_threshold: Optional[Decimal] = Field(default=0.05, max_digits=3, decimal_places=2)


class QualityRuleBulkUpdate(SQLModel):
    """Schema untuk bulk update quality rule"""
    rule_ids: List[int]
    updates: QualityRuleUpdate
    apply_to_matching: Optional[Dict[str, Any]] = Field(default=None)  # filter criteria


class QualityRuleTest(SQLModel):
    """Schema untuk test quality rule"""
    rule_id: int
    test_data: Dict[str, Any]
    expected_result: bool
    test_description: Optional[str] = Field(default=None)


class QualityRuleTestResult(SQLModel):
    """Schema untuk hasil test quality rule"""
    rule_id: int
    test_passed: bool
    actual_result: bool
    expected_result: bool
    test_data: Dict[str, Any]
    execution_time_ms: Optional[float] = Field(default=None)
    error_message: Optional[str] = Field(default=None)
    tested_at: datetime = Field(default_factory=datetime.utcnow)


class QualityRulePerformance(SQLModel):
    """Schema untuk performance quality rule"""
    rule_id: int
    avg_execution_time_ms: float
    max_execution_time_ms: float
    min_execution_time_ms: float
    total_executions: int
    performance_rating: str  # 'excellent', 'good', 'poor', 'critical'
    last_measured: datetime = Field(default_factory=datetime.utcnow)


class QualityRuleUsage(SQLModel):
    """Schema untuk usage quality rule"""
    rule_id: int
    rule_name: str
    usage_count: int
    last_used: Optional[datetime] = Field(default=None)
    used_by_jobs: Optional[List[str]] = Field(default=None)
    success_rate: Optional[float] = Field(default=None)
    avg_records_checked: Optional[float] = Field(default=None)


class QualityRuleClone(SQLModel):
    """Schema untuk clone quality rule"""
    source_rule_id: int
    new_rule_name: str = Field(max_length=100)
    modify_expression: Optional[str] = Field(default=None)
    modify_threshold: Optional[Decimal] = Field(default=None, max_digits=3, decimal_places=2)
    apply_to_entity_type: Optional[str] = Field(default=None)
    apply_to_field: Optional[str] = Field(default=None)


# Import untuk menghindari circular imports
from .quality_check_results import QualityCheckResultRead