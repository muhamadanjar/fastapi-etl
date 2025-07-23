from datetime import datetime
from typing import Optional, Dict, Any, List
from decimal import Decimal
from uuid import UUID

from sqlmodel import SQLModel, Field, Column
from sqlalchemy import ARRAY, String, text
from sqlalchemy.dialects.postgresql import JSONB
from app.infrastructure.db.models.base import BaseModel


class StandardizedDataBase(BaseModel):
    """Base model untuk StandardizedData dengan field-field umum"""
    source_file_id: UUID = Field(foreign_key="raw_data.file_registry.id", index=True)
    source_record_id: UUID = Field(foreign_key="raw_data.raw_records.id", index=True)
    entity_type: str = Field(max_length=100, index=True)  # 'PERSON', 'PRODUCT', 'TRANSACTION', 'EVENT'
    standardized_data: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB))
    quality_score: Optional[Decimal] = Field(default=None, max_digits=3, decimal_places=2)
    transformation_rules_applied: Optional[List[str]] = Field(default=None, sa_column=Column(ARRAY(String)))
    batch_id: Optional[str] = Field(default=None, max_length=50, index=True)


class StandardizedData(StandardizedDataBase, table=True):
    """Model untuk tabel staging.standardized_data"""
    __tablename__ = "standardized_data"
    __table_args__ = (
        {"schema": "staging"},
    )
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        schema_extra = {
            "example": {
                "source_file_id": 1,
                "source_record_id": 123,
                "entity_type": "PERSON",
                "standardized_data": {
                    "full_name": "John Doe",
                    "email": "john.doe@example.com",
                    "phone": "+1-555-0123",
                    "birth_date": "1990-01-15",
                    "address": {
                        "street": "123 Main St",
                        "city": "New York",
                        "state": "NY",
                        "zip_code": "10001",
                        "country": "USA"
                    },
                    "employment": {
                        "company": "Tech Corp",
                        "position": "Software Engineer",
                        "department": "Engineering"
                    }
                },
                "quality_score": 0.95,
                "transformation_rules_applied": [
                    "name_standardization",
                    "email_validation",
                    "phone_formatting",
                    "address_geocoding"
                ],
                "batch_id": "batch_2024_01_001"
            }
        }


class StandardizedDataCreate(StandardizedDataBase):
    """Schema untuk create standardized data"""
    pass


class StandardizedDataUpdate(SQLModel):
    """Schema untuk update standardized data"""
    source_file_id: Optional[int] = Field(default=None)
    source_record_id: Optional[int] = Field(default=None)
    entity_type: Optional[str] = Field(default=None, max_length=100)
    standardized_data: Optional[Dict[str, Any]] = Field(default=None)
    quality_score: Optional[Decimal] = Field(default=None, max_digits=3, decimal_places=2)
    transformation_rules_applied: Optional[List[str]] = Field(default=None)
    batch_id: Optional[str] = Field(default=None, max_length=50)


class StandardizedDataRead(StandardizedDataBase):
    """Schema untuk read standardized data"""
    staging_id: int
    created_at: datetime


class StandardizedDataFilter(SQLModel):
    """Schema untuk filter standardized data"""
    entity_type: Optional[str] = Field(default=None)
    source_file_id: Optional[int] = Field(default=None)
    batch_id: Optional[str] = Field(default=None)
    quality_score_min: Optional[Decimal] = Field(default=None, max_digits=3, decimal_places=2)
    quality_score_max: Optional[Decimal] = Field(default=None, max_digits=3, decimal_places=2)
    transformation_rule: Optional[str] = Field(default=None)
    date_from: Optional[datetime] = Field(default=None)
    date_to: Optional[datetime] = Field(default=None)


class StandardizedDataSummary(SQLModel):
    """Schema untuk summary standardized data"""
    entity_type: str
    total_records: int
    avg_quality_score: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=2)
    latest_batch: Optional[str] = Field(default=None)
    latest_update: Optional[datetime] = Field(default=None)
    unique_source_files: int
    common_transformation_rules: Optional[List[str]] = Field(default=None)


class StandardizedDataBulkCreate(SQLModel):
    """Schema untuk bulk create standardized data"""
    records: List[StandardizedDataCreate]
    batch_id: str = Field(max_length=50)


class StandardizedDataQualityReport(SQLModel):
    """Schema untuk quality report standardized data"""
    entity_type: str
    total_records: int
    quality_distribution: Dict[str, int]  # {"high": 100, "medium": 50, "low": 10}
    avg_quality_score: Decimal
    transformation_rules_usage: Dict[str, int]  # {"rule_name": usage_count}
    batch_quality_trends: Optional[List[Dict[str, Any]]] = Field(default=None)