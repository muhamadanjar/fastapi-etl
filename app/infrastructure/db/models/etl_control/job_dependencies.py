from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlmodel import SQLModel, Field, Relationship
from enum import Enum
from app.infrastructure.db.models.base import BaseModel


class DependencyType(str, Enum):
    """Enum for dependency types"""
    SUCCESS = "SUCCESS"  # Child job runs only if parent succeeds
    COMPLETION = "COMPLETION"  # Child job runs when parent completes (success or failure)
    DATA_AVAILABILITY = "DATA_AVAILABILITY"  # Child job runs when parent data is available


class JobDependencyBase(BaseModel):
    """Base model for JobDependency"""
    parent_job_id: UUID = Field(
        foreign_key="etl_control.etl_jobs.id",
        index=True,
        description="Parent job that must complete first"
    )
    child_job_id: UUID = Field(
        foreign_key="etl_control.etl_jobs.id",
        index=True,
        description="Child job that depends on parent"
    )
    dependency_type: DependencyType = Field(
        default=DependencyType.SUCCESS,
        description="Type of dependency relationship"
    )
    is_active: bool = Field(
        default=True,
        index=True,
        description="Whether this dependency is currently active"
    )
    description: Optional[str] = Field(
        default=None,
        max_length=500,
        description="Description of the dependency relationship"
    )


class JobDependency(JobDependencyBase, table=True):
    """Model for etl_control.job_dependencies table"""
    __tablename__ = "job_dependencies"
    __table_args__ = (
        {"schema": "etl_control"},
    )
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Relationships
    parent_job: Optional["EtlJob"] = Relationship(
        sa_relationship_kwargs={
            "foreign_keys": "[JobDependency.parent_job_id]",
            "lazy": "joined"
        }
    )
    child_job: Optional["EtlJob"] = Relationship(
        sa_relationship_kwargs={
            "foreign_keys": "[JobDependency.child_job_id]",
            "lazy": "joined"
        }
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "parent_job_id": "550e8400-e29b-41d4-a716-446655440000",
                "child_job_id": "550e8400-e29b-41d4-a716-446655440001",
                "dependency_type": "SUCCESS",
                "is_active": True,
                "description": "Customer data must be processed before running aggregation"
            }
        }


class JobDependencyCreate(JobDependencyBase):
    """Schema for creating job dependency"""
    pass


class JobDependencyUpdate(SQLModel):
    """Schema for updating job dependency"""
    dependency_type: Optional[DependencyType] = Field(default=None)
    is_active: Optional[bool] = Field(default=None)
    description: Optional[str] = Field(default=None, max_length=500)


class JobDependencyRead(JobDependencyBase):
    """Schema for reading job dependency"""
    dependency_id: UUID
    created_at: datetime
    updated_at: datetime


class JobDependencyWithJobs(JobDependencyRead):
    """Schema for reading job dependency with job details"""
    parent_job: Optional["EtlJobRead"] = Field(default=None)
    child_job: Optional["EtlJobRead"] = Field(default=None)


# Import to avoid circular imports
from app.infrastructure.db.models.etl_control.etl_jobs import EtlJob, EtlJobRead
