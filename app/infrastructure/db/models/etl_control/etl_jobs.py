from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum

from sqlmodel import SQLModel, Field, Column, Relationship
from sqlalchemy.dialects.postgresql import JSONB

from app.models.base import BaseModel


class JobType(str, Enum):
    """Enum untuk job type"""
    EXTRACT = "EXTRACT"
    TRANSFORM = "TRANSFORM"
    LOAD = "LOAD"
    VALIDATE = "VALIDATE"


class JobCategory(str, Enum):
    """Enum untuk job category"""
    FILE_PROCESSING = "FILE_PROCESSING"
    DATA_CLEANING = "DATA_CLEANING"
    AGGREGATION = "AGGREGATION"
    VALIDATION = "VALIDATION"
    EXPORT = "EXPORT"


class SourceType(str, Enum):
    """Enum untuk source type"""
    FILE = "FILE"
    API = "API"
    DATABASE = "DATABASE"
    STREAM = "STREAM"


class EtlJobBase(BaseModel):
    """Base model untuk EtlJob dengan field-field umum"""
    job_name: str = Field(max_length=100, index=True, unique=True)
    job_type: JobType = Field(index=True)
    job_category: JobCategory = Field(index=True)
    source_type: SourceType = Field(index=True)
    target_schema: Optional[str] = Field(default=None, max_length=50)
    target_table: Optional[str] = Field(default=None, max_length=100)
    job_config: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB))
    schedule_expression: Optional[str] = Field(default=None, max_length=100)  # Cron expression
    is_active: bool = Field(default=True, index=True)


class EtlJob(EtlJobBase, table=True):
    """Model untuk tabel etl_control.etl_jobs"""
    __tablename__ = "etl_jobs"
    __table_args__ = (
        {"schema": "etl_control"},
    )
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Relationships
    executions: List["JobExecution"] = Relationship(back_populates="job")
    
    class Config:
        schema_extra = {
            "example": {
                "job_name": "process_customer_data",
                "job_type": "TRANSFORM",
                "job_category": "DATA_CLEANING",
                "source_type": "FILE",
                "target_schema": "staging",
                "target_table": "standardized_data",
                "job_config": {
                    "input_format": "csv",
                    "delimiter": ",",
                    "encoding": "utf-8",
                    "transformations": [
                        {
                            "type": "name_standardization",
                            "fields": ["first_name", "last_name"]
                        },
                        {
                            "type": "email_validation",
                            "fields": ["email"]
                        }
                    ],
                    "validation_rules": ["not_null", "unique_email"],
                    "batch_size": 1000,
                    "parallel_processing": True
                },
                "schedule_expression": "0 2 * * *",
                "is_active": True
            }
        }


class EtlJobCreate(EtlJobBase):
    """Schema untuk create ETL job"""
    pass


class EtlJobUpdate(SQLModel):
    """Schema untuk update ETL job"""
    job_name: Optional[str] = Field(default=None, max_length=100)
    job_type: Optional[JobType] = Field(default=None)
    job_category: Optional[JobCategory] = Field(default=None)
    source_type: Optional[SourceType] = Field(default=None)
    target_schema: Optional[str] = Field(default=None, max_length=50)
    target_table: Optional[str] = Field(default=None, max_length=100)
    job_config: Optional[Dict[str, Any]] = Field(default=None)
    schedule_expression: Optional[str] = Field(default=None, max_length=100)
    is_active: Optional[bool] = Field(default=None)


class EtlJobRead(EtlJobBase):
    """Schema untuk read ETL job"""
    job_id: str
    created_at: datetime


class EtlJobReadWithExecutions(EtlJobRead):
    """Schema untuk read ETL job dengan executions"""
    executions: Optional[List["JobExecutionRead"]] = Field(default=None)


class EtlJobFilter(SQLModel):
    """Schema untuk filter ETL job"""
    job_name: Optional[str] = Field(default=None)
    job_type: Optional[JobType] = Field(default=None)
    job_category: Optional[JobCategory] = Field(default=None)
    source_type: Optional[SourceType] = Field(default=None)
    target_schema: Optional[str] = Field(default=None)
    target_table: Optional[str] = Field(default=None)
    is_active: Optional[bool] = Field(default=None)
    search_term: Optional[str] = Field(default=None)


class EtlJobSummary(SQLModel):
    """Schema untuk summary ETL job"""
    job_type: JobType
    job_category: JobCategory
    total_jobs: int
    active_jobs: int
    inactive_jobs: int
    scheduled_jobs: int
    last_execution: Optional[datetime] = Field(default=None)


class EtlJobSchedule(SQLModel):
    """Schema untuk schedule ETL job"""
    job_id: str
    schedule_expression: str = Field(max_length=100)
    timezone: Optional[str] = Field(default="UTC", max_length=50)
    is_enabled: bool = Field(default=True)
    next_run: Optional[datetime] = Field(default=None)


class EtlJobValidation(SQLModel):
    """Schema untuk validasi ETL job"""
    job_id: str
    validation_type: str  # 'config', 'dependencies', 'permissions'
    is_valid: bool
    validation_errors: Optional[List[str]] = Field(default=None)
    validation_warnings: Optional[List[str]] = Field(default=None)
    validated_at: datetime = Field(default_factory=datetime.utcnow)


class EtlJobDependency(SQLModel):
    """Schema untuk dependency ETL job"""
    job_id: str
    depends_on_job_id: int
    dependency_type: str  # 'success', 'completion', 'data_availability'
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class EtlJobTemplate(SQLModel):
    """Schema untuk template ETL job"""
    template_name: str = Field(max_length=100)
    template_description: Optional[str] = Field(default=None)
    job_type: JobType
    job_category: JobCategory
    source_type: SourceType
    default_config: Dict[str, Any]
    required_fields: List[str]
    optional_fields: List[str]
    created_at: datetime = Field(default_factory=datetime.utcnow)


class EtlJobClone(SQLModel):
    """Schema untuk clone ETL job"""
    source_job_id: int
    new_job_name: str = Field(max_length=100)
    copy_executions: bool = Field(default=False)
    copy_schedule: bool = Field(default=True)
    modify_config: Optional[Dict[str, Any]] = Field(default=None)


# Import untuk menghindari circular imports
from .job_executions import JobExecutionRead