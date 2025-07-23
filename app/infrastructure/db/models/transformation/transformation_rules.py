from datetime import datetime
from typing import Optional, Dict, Any
from sqlmodel import SQLModel, Field, Column, JSON

from app.infrastructure.db.models.base import BaseModel


class TransformationRuleBase(BaseModel):
    """Base model for transformation rules."""
    rule_name: str = Field(max_length=100, description="Name of the transformation rule")
    source_format: Optional[str] = Field(default=None, max_length=50, description="Source data format")
    target_format: Optional[str] = Field(default=None, max_length=50, description="Target data format")
    transformation_type: Optional[str] = Field(
        default=None, 
        max_length=50, 
        description="Type of transformation: MAPPING, CALCULATION, VALIDATION, ENRICHMENT"
    )
    rule_logic: Optional[str] = Field(default=None, description="SQL or script logic for the rule")
    rule_parameters: Optional[Dict[str, Any]] = Field(
        default=None, 
        sa_column=Column(JSON),
        description="Parameters for the transformation rule in JSON format"
    )
    priority: int = Field(default=1, description="Priority of the rule execution")
    is_active: bool = Field(default=True, description="Whether the rule is active")


class TransformationRule(TransformationRuleBase, table=True):
    """Transformation rule model for database storage."""
    __tablename__ = "transformation_rules"
    __table_args__ = {"schema": "transformation"}
    
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation timestamp")


class TransformationRuleCreate(TransformationRuleBase):
    """Schema for creating transformation rules."""
    pass


class TransformationRuleRead(TransformationRuleBase):
    """Schema for reading transformation rules."""
    rule_id: str
    created_at: datetime


class TransformationRuleUpdate(SQLModel):
    """Schema for updating transformation rules."""
    rule_name: Optional[str] = Field(default=None, max_length=100)
    source_format: Optional[str] = Field(default=None, max_length=50)
    target_format: Optional[str] = Field(default=None, max_length=50)
    transformation_type: Optional[str] = Field(default=None, max_length=50)
    rule_logic: Optional[str] = Field(default=None)
    rule_parameters: Optional[Dict[str, Any]] = Field(default=None)
    priority: Optional[int] = Field(default=None)
    is_active: Optional[bool] = Field(default=None)