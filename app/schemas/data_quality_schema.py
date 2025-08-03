"""
Data Quality schemas for request/response validation.
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, validator


# ==============================================
# QUALITY RULE SCHEMAS
# ==============================================

class QualityRuleBase(BaseModel):
    """Base quality rule schema"""
    rule_name: str
    rule_type: str
    entity_type: str
    field_name: Optional[str] = None
    rule_expression: Optional[str] = None
    error_threshold: float = 0.0
    severity: str = "MEDIUM"
    is_active: bool = True
    rule_config: Optional[Dict[str, Any]] = None
    description: Optional[str] = None


class QualityRuleCreate(QualityRuleBase):
    """Schema for creating quality rule"""
    
    @validator('rule_type')
    def validate_rule_type(cls, v):
        allowed_types = ['COMPLETENESS', 'UNIQUENESS', 'VALIDITY', 'CONSISTENCY', 'ACCURACY', 'INTEGRITY', 'TIMELINESS', 'CUSTOM']
        if v not in allowed_types:
            raise ValueError(f'rule_type must be one of: {allowed_types}')
        return v
    
    @validator('severity')
    def validate_severity(cls, v):
        allowed_severities = ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
        if v not in allowed_severities:
            raise ValueError(f'severity must be one of: {allowed_severities}')
        return v
    
    @validator('error_threshold')
    def validate_error_threshold(cls, v):
        if v < 0 or v > 1:
            raise ValueError('error_threshold must be between 0 and 1')
        return v
    
    @validator('rule_expression')
    def validate_rule_expression(cls, v, values):
        rule_type = values.get('rule_type')
        if rule_type in ['VALIDITY', 'CUSTOM'] and not v:
            raise ValueError(f'rule_expression is required for {rule_type} rule type')
        return v


class QualityRuleUpdate(BaseModel):
    """Schema for updating quality rule"""
    rule_name: Optional[str] = None
    rule_type: Optional[str] = None
    entity_type: Optional[str] = None
    field_name: Optional[str] = None
    rule_expression: Optional[str] = None
    error_threshold: Optional[float] = None
    severity: Optional[str] = None
    is_active: Optional[bool] = None
    rule_config: Optional[Dict[str, Any]] = None
    description: Optional[str] = None
    
    @validator('rule_type')
    def validate_rule_type(cls, v):
        if v is not None:
            allowed_types = ['COMPLETENESS', 'UNIQUENESS', 'VALIDITY', 'CONSISTENCY', 'ACCURACY', 'INTEGRITY', 'TIMELINESS', 'CUSTOM']
            if v not in allowed_types:
                raise ValueError(f'rule_type must be one of: {allowed_types}')
        return v
    
    @validator('severity')
    def validate_severity(cls, v):
        if v is not None:
            allowed_severities = ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
            if v not in allowed_severities:
                raise ValueError(f'severity must be one of: {allowed_severities}')
        return v


class QualityRuleRead(QualityRuleBase):
    """Schema for quality rule response"""
    rule_id: int
    created_at: datetime
    usage_stats: Optional[Dict[str, Any]] = None
    
    class Config:
        from_attributes = True


# ==============================================
# QUALITY CHECK SCHEMAS
# ==============================================

class QualityCheckRequest(BaseModel):
    """Schema for quality check request"""
    data_batch: List[Dict[str, Any]]
    entity_type: str
    rule_ids: Optional[List[int]] = None
    check_config: Optional[Dict[str, Any]] = None
    
    @validator('data_batch')
    def validate_data_batch(cls, v):
        if not v:
            raise ValueError('data_batch cannot be empty')
        if len(v) > 10000:
            raise ValueError('data_batch cannot contain more than 10,000 records')
        return v


class QualityViolation(BaseModel):
    """Schema for quality violation"""
    record_index: int
    rule_name: str
    field_name: Optional[str] = None
    field_value: Any = None
    violation_type: str
    message: str


class QualityRuleResult(BaseModel):
    """Schema for quality rule result"""
    rule_name: str
    rule_type: str
    records_checked: int
    records_passed: int
    records_failed: int
    pass_rate: float
    violations: List[QualityViolation]
    severity: str


class QualityCheckResponse(BaseModel):
    """Schema for quality check response"""
    total_records: int
    quality_score: float
    rules_checked: int
    total_violations: int
    rule_results: Dict[int, QualityRuleResult]
    summary: Dict[str, Any]


# ==============================================
# VALIDATION SCHEMAS
# ==============================================

class ValidationRule(BaseModel):
    """Schema for validation rule"""
    field_name: str
    rule_type: str
    rule_config: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    
    @validator('rule_type')
    def validate_rule_type(cls, v):
        allowed_types = ['required', 'type', 'range', 'pattern', 'custom']
        if v not in allowed_types:
            raise ValueError(f'rule_type must be one of: {allowed_types}')
        return v


class ValidationRequest(BaseModel):
    """Schema for validation request"""
    data_batch: List[Dict[str, Any]]
    validation_rules: List[ValidationRule]
    validation_config: Optional[Dict[str, Any]] = None
    
    @validator('data_batch')
    def validate_data_batch(cls, v):
        if not v:
            raise ValueError('data_batch cannot be empty')
        if len(v) > 5000:
            raise ValueError('data_batch cannot contain more than 5,000 records for validation')
        return v


class ValidationError(BaseModel):
    """Schema for validation error"""
    field_name: str
    field_value: Any
    error_type: str
    error_message: str


class ValidationResult(BaseModel):
    """Schema for validation result"""
    record_index: int
    is_valid: bool
    errors: List[ValidationError]


class ValidationResponse(BaseModel):
    """Schema for validation response"""
    total_records: int
    valid_records: int
    invalid_records: int
    validation_score: float
    total_errors: int
    validation_results: List[ValidationResult]


# ==============================================
# QUALITY REPORT SCHEMAS
# ==============================================

class QualityReportRequest(BaseModel):
    """Schema for quality report request"""
    entity_type: Optional[str] = None
    date_range: Optional[Dict[str, str]] = None
    report_config: Optional[Dict[str, Any]] = None
    
    @validator('date_range')
    def validate_date_range(cls, v):
        if v and ('start_date' not in v or 'end_date' not in v):
            raise ValueError('date_range must contain start_date and end_date')
        return v


class QualityStatistics(BaseModel):
    """Schema for quality statistics"""
    total_quality_checks: int
    passed_checks: int
    failed_checks: int
    overall_pass_rate: float


class RuleTypeStatistics(BaseModel):
    """Schema for rule type statistics"""
    total_checks: int
    passed_checks: int
    failed_checks: int
    pass_rate: float


class QualityReportResponse(BaseModel):
    """Schema for quality report response"""
    report_period: Dict[str, str]
    entity_type: Optional[str]
    overall_statistics: QualityStatistics
    rule_type_statistics: Dict[str, RuleTypeStatistics]
    generated_at: str


# ==============================================
# QUALITY ALERT SCHEMAS
# ==============================================

class QualityAlert(BaseModel):
    """Schema for quality alert"""
    alert_id: int
    rule_name: str
    entity_type: str
    severity: str
    records_failed: int
    failure_rate: float
    created_at: datetime
    is_resolved: bool


class AlertResolution(BaseModel):
    """Schema for alert resolution"""
    resolution_notes: Optional[str] = None


# ==============================================
# QUALITY METRICS SCHEMAS
# ==============================================

class QualityMetrics(BaseModel):
    """Schema for quality metrics"""
    total_checks_last_7_days: int
    passed_checks: int
    failed_checks: int
    pass_rate: float
    entity_type: Optional[str]
    last_updated: str


class QualityTrendData(BaseModel):
    """Schema for quality trend data"""
    date: str
    quality_score: float
    checks_performed: int
    total_records: int


class QualityTrends(BaseModel):
    """Schema for quality trends"""
    entity_type: Optional[str]
    period: str
    trend_data: List[QualityTrendData]


# ==============================================
# SCHEDULE SCHEMAS
# ==============================================

class QualityCheckSchedule(BaseModel):
    """Schema for quality check schedule"""
    entity_type: str
    schedule_expression: str  # Cron expression
    rule_ids: Optional[List[int]] = None
    check_config: Optional[Dict[str, Any]] = None
    is_active: bool = True
    
    @validator('schedule_expression')
    def validate_schedule_expression(cls, v):
        # Basic validation for cron expression
        parts = v.split()
        if len(parts) != 5:
            raise ValueError('schedule_expression must be a valid cron expression (5 parts)')
        return v


class ScheduleRequest(BaseModel):
    """Schema for schedule request"""
    entity_type: str
    schedule_config: Dict[str, Any]


# ==============================================
# QUALITY SUMMARY SCHEMAS
# ==============================================

class QualitySummary(BaseModel):
    """Schema for quality summary"""
    total_quality_checks: int
    total_records_checked: int
    total_records_passed: int
    total_records_failed: int
    overall_pass_rate: float
    entity_type: Optional[str]
    period: Dict[str, Optional[str]]


# ==============================================
# FILE AND ENTITY QUALITY SCHEMAS
# ==============================================

class FileQualityCheck(BaseModel):
    """Schema for file quality check"""
    file_id: int
    validation_rules: Optional[List[ValidationRule]] = None


class EntityQualityCheck(BaseModel):
    """Schema for entity quality check"""
    entity_type: str
    entity_ids: Optional[List[int]] = None
    quality_config: Optional[Dict[str, Any]] = None


class JobQualityCheck(BaseModel):
    """Schema for job quality check"""
    job_id: int
    execution_id: Optional[int] = None
    quality_config: Optional[Dict[str, Any]] = None


# ==============================================
# QUALITY CONFIGURATION SCHEMAS
# ==============================================

class QualityConfiguration(BaseModel):
    """Schema for quality configuration"""
    entity_type: str
    default_rules: List[int]
    quality_thresholds: Dict[str, float]
    notification_settings: Optional[Dict[str, Any]] = None
    auto_remediation: bool = False


class QualityThreshold(BaseModel):
    """Schema for quality threshold"""
    rule_type: str
    warning_threshold: float
    error_threshold: float
    critical_threshold: float
    
    @validator('warning_threshold', 'error_threshold', 'critical_threshold')
    def validate_thresholds(cls, v):
        if v < 0 or v > 100:
            raise ValueError('Threshold must be between 0 and 100')
        return v


# ==============================================
# QUALITY PROFILE SCHEMAS
# ==============================================

class QualityProfile(BaseModel):
    """Schema for quality profile"""
    profile_name: str
    entity_types: List[str]
    quality_rules: List[int]
    thresholds: Dict[str, QualityThreshold]
    is_default: bool = False


class QualityProfileCreate(QualityProfile):
    """Schema for creating quality profile"""
    pass


class QualityProfileRead(QualityProfile):
    """Schema for quality profile response"""
    profile_id: int
    created_at: datetime
    updated_at: Optional[datetime]
    created_by: Optional[int]
    
    class Config:
        from_attributes = True


# ==============================================
# BATCH PROCESSING SCHEMAS
# ==============================================

class BatchQualityRequest(BaseModel):
    """Schema for batch quality processing"""
    entity_types: List[str]
    quality_profile_id: Optional[int] = None
    processing_config: Optional[Dict[str, Any]] = None
    notification_settings: Optional[Dict[str, Any]] = None


class BatchQualityResponse(BaseModel):
    """Schema for batch quality response"""
    batch_id: str
    total_entity_types: int
    processing_status: str
    estimated_completion: Optional[str] = None
    started_at: str
    