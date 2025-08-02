"""
Transformation schemas for request/response validation.
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, validator


# ==============================================
# TRANSFORMATION RULE SCHEMAS
# ==============================================

class TransformationRuleBase(BaseModel):
    """Base transformation rule schema"""
    rule_name: str
    source_format: str
    target_format: str
    transformation_type: str
    rule_logic: Optional[str] = None
    rule_parameters: Optional[Dict[str, Any]] = None
    priority: int = 1
    is_active: bool = True


class TransformationRuleCreate(TransformationRuleBase):
    """Schema for creating transformation rule"""
    
    @validator('transformation_type')
    def validate_transformation_type(cls, v):
        allowed_types = ['MAPPING', 'CALCULATION', 'VALIDATION', 'ENRICHMENT', 'CUSTOM']
        if v not in allowed_types:
            raise ValueError(f'transformation_type must be one of: {allowed_types}')
        return v
    
    @validator('priority')
    def validate_priority(cls, v):
        if v < 1:
            raise ValueError('priority must be greater than 0')
        return v


class TransformationRuleUpdate(BaseModel):
    """Schema for updating transformation rule"""
    rule_name: Optional[str] = None
    source_format: Optional[str] = None
    target_format: Optional[str] = None
    transformation_type: Optional[str] = None
    rule_logic: Optional[str] = None
    rule_parameters: Optional[Dict[str, Any]] = None
    priority: Optional[int] = None
    is_active: Optional[bool] = None
    
    @validator('transformation_type')
    def validate_transformation_type(cls, v):
        if v is not None:
            allowed_types = ['MAPPING', 'CALCULATION', 'VALIDATION', 'ENRICHMENT', 'CUSTOM']
            if v not in allowed_types:
                raise ValueError(f'transformation_type must be one of: {allowed_types}')
        return v


class TransformationRuleRead(TransformationRuleBase):
    """Schema for transformation rule response"""
    rule_id: int
    created_at: datetime
    usage_stats: Optional[Dict[str, Any]] = None
    
    class Config:
        from_attributes = True


# ==============================================
# FIELD MAPPING SCHEMAS
# ==============================================

class FieldMappingBase(BaseModel):
    """Base field mapping schema"""
    source_entity: str
    source_field: str
    target_entity: str
    target_field: str
    mapping_type: str = "DIRECT"
    mapping_expression: Optional[str] = None
    data_type: Optional[str] = None
    is_required: bool = False
    default_value: Optional[str] = None


class FieldMappingCreate(FieldMappingBase):
    """Schema for creating field mapping"""
    
    @validator('mapping_type')
    def validate_mapping_type(cls, v):
        allowed_types = ['DIRECT', 'CALCULATED', 'LOOKUP']
        if v not in allowed_types:
            raise ValueError(f'mapping_type must be one of: {allowed_types}')
        return v
    
    @validator('mapping_expression')
    def validate_mapping_expression(cls, v, values):
        mapping_type = values.get('mapping_type')
        if mapping_type in ['CALCULATED', 'LOOKUP'] and not v:
            raise ValueError(f'mapping_expression is required for {mapping_type} mapping')
        return v


class FieldMappingUpdate(BaseModel):
    """Schema for updating field mapping"""
    source_entity: Optional[str] = None
    source_field: Optional[str] = None
    target_entity: Optional[str] = None
    target_field: Optional[str] = None
    mapping_type: Optional[str] = None
    mapping_expression: Optional[str] = None
    data_type: Optional[str] = None
    is_required: Optional[bool] = None
    default_value: Optional[str] = None
    
    @validator('mapping_type')
    def validate_mapping_type(cls, v):
        if v is not None:
            allowed_types = ['DIRECT', 'CALCULATED', 'LOOKUP']
            if v not in allowed_types:
                raise ValueError(f'mapping_type must be one of: {allowed_types}')
        return v


class FieldMappingRead(FieldMappingBase):
    """Schema for field mapping response"""
    mapping_id: int
    created_at: datetime
    
    class Config:
        from_attributes = True


# ==============================================
# DATA TRANSFORMATION SCHEMAS
# ==============================================

class DataTransformRequest(BaseModel):
    """Schema for data transformation request"""
    data_batch: List[Dict[str, Any]]
    source_entity: str
    target_entity: str
    
    @validator('data_batch')
    def validate_data_batch(cls, v):
        if not v:
            raise ValueError('data_batch cannot be empty')
        if len(v) > 1000:
            raise ValueError('data_batch cannot contain more than 1000 records')
        return v


class TransformationMetadata(BaseModel):
    """Transformation metadata schema"""
    mappings_applied: int
    rules_applied: int
    transformed_at: datetime


class TransformedRecord(BaseModel):
    """Schema for transformed record"""
    source_row: int
    transformed_data: Dict[str, Any]
    transformation_metadata: TransformationMetadata


class FailedRecord(BaseModel):
    """Schema for failed transformation record"""
    source_row: int
    source_data: Dict[str, Any]
    error: str


class DataTransformResponse(BaseModel):
    """Schema for data transformation response"""
    total_records: int
    successful_records: int
    failed_records: int
    success_rate: float
    transformed_data: List[TransformedRecord]
    failed_data: List[FailedRecord]
    mappings_used: int
    rules_used: int


class CustomTransformRequest(BaseModel):
    """Schema for custom transformation request"""
    data_batch: List[Dict[str, Any]]
    transformation_logic: str
    parameters: Optional[Dict[str, Any]] = None
    
    @validator('data_batch')
    def validate_data_batch(cls, v):
        if not v:
            raise ValueError('data_batch cannot be empty')
        if len(v) > 500:  # Smaller limit for custom transformations
            raise ValueError('data_batch cannot contain more than 500 records for custom transformations')
        return v
    
    @validator('transformation_logic')
    def validate_transformation_logic(cls, v):
        if not v.strip():
            raise ValueError('transformation_logic cannot be empty')
        return v


# ==============================================
# TEST TRANSFORMATION SCHEMAS
# ==============================================

class TestTransformRequest(BaseModel):
    """Schema for test transformation request"""
    sample_data: List[Dict[str, Any]]
    transformation_config: Dict[str, Any]
    
    @validator('sample_data')
    def validate_sample_data(cls, v):
        if not v:
            raise ValueError('sample_data cannot be empty')
        if len(v) > 50:  # Small limit for testing
            raise ValueError('sample_data cannot contain more than 50 records for testing')
        return v


class TransformedSample(BaseModel):
    """Schema for transformed sample"""
    original: Dict[str, Any]
    transformed: Dict[str, Any]


class TransformationError(BaseModel):
    """Schema for transformation error"""
    record_index: int
    error: str


class TestTransformResponse(BaseModel):
    """Schema for test transformation response"""
    sample_size: int
    transformed_samples: List[TransformedSample]
    errors: List[TransformationError]
    warnings: List[str]


# ==============================================
# LOOKUP TABLE SCHEMAS
# ==============================================

class LookupTableBase(BaseModel):
    """Base lookup table schema"""
    lookup_name: str
    lookup_key: str
    lookup_value: str
    lookup_category: Optional[str] = None
    is_active: bool = True


class LookupTableCreate(LookupTableBase):
    """Schema for creating lookup table entry"""
    pass


class LookupTableRead(LookupTableBase):
    """Schema for lookup table response"""
    lookup_id: int
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


# ==============================================
# TRANSFORMATION PIPELINE SCHEMAS
# ==============================================

class TransformationStep(BaseModel):
    """Schema for transformation pipeline step"""
    step_type: str  # 'mapping', 'rule', 'validation'
    step_config: Dict[str, Any]
    step_order: int
    is_optional: bool = False


class TransformationPipeline(BaseModel):
    """Schema for transformation pipeline"""
    pipeline_name: str
    source_entity: str
    target_entity: str
    steps: List[TransformationStep]
    is_active: bool = True
    
    @validator('steps')
    def validate_steps(cls, v):
        if not v:
            raise ValueError('Pipeline must have at least one step')
        
        # Check step order
        orders = [step.step_order for step in v]
        if len(orders) != len(set(orders)):
            raise ValueError('Step orders must be unique')
        
        return sorted(v, key=lambda x: x.step_order)


class PipelineExecutionRequest(BaseModel):
    """Schema for pipeline execution request"""
    pipeline_name: str
    data_batch: List[Dict[str, Any]]
    execution_config: Optional[Dict[str, Any]] = None


class PipelineExecutionResponse(BaseModel):
    """Schema for pipeline execution response"""
    pipeline_name: str
    execution_id: str
    total_records: int
    successful_records: int
    failed_records: int
    steps_executed: int
    execution_time_seconds: float
    results: List[Dict[str, Any]]


# ==============================================
# VALIDATION SCHEMAS
# ==============================================

class ValidationRule(BaseModel):
    """Schema for data validation rule"""
    field_name: str
    rule_type: str  # 'required', 'type', 'range', 'pattern', 'custom'
    rule_config: Dict[str, Any]
    error_message: Optional[str] = None


class ValidationRequest(BaseModel):
    """Schema for data validation request"""
    data_batch: List[Dict[str, Any]]
    validation_rules: List[ValidationRule]
    stop_on_first_error: bool = False


class ValidationError(BaseModel):
    """Schema for validation error"""
    record_index: int
    field_name: str
    error_type: str
    error_message: str
    field_value: Any


class ValidationResponse(BaseModel):
    """Schema for validation response"""
    total_records: int
    valid_records: int
    invalid_records: int
    validation_errors: List[ValidationError]
    validation_summary: Dict[str, Any]