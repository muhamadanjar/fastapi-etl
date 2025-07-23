"""
Column Structure model for storing detected column metadata
"""

from datetime import datetime
from typing import Optional, List
from uuid import UUID
from sqlmodel import SQLModel, Field, Column, ARRAY, String, ForeignKey

from app.core.enums import DataType

from ..base import BaseModel


class ColumnStructure(BaseModel, table=True):
    """
    Model for storing detected column structure and metadata
    """
    __tablename__ = "column_structure"
    __table_args__ = {"schema": "raw_data"}
    
    
    file_id: UUID = Field(
        foreign_key="raw_data.file_registry.id",
        description="Reference to the source file"
    )
    
    column_name: str = Field(
        max_length=255,
        description="Name of the column"
    )
    
    column_position: Optional[int] = Field(
        default=None,
        description="Position of the column in the source file"
    )
    
    data_type: DataType = Field(
        description="Detected data type of the column"
    )
    
    sample_values: Optional[List[str]] = Field(
        default=None,
        sa_column=Column(ARRAY(String)),
        description="Sample values from the column"
    )
    
    null_count: Optional[int] = Field(
        default=None,
        description="Number of null/empty values in the column"
    )
    
    unique_count: Optional[int] = Field(
        default=None,
        description="Number of unique values in the column"
    )
    
    min_length: Optional[int] = Field(
        default=None,
        description="Minimum length of values in the column"
    )
    
    max_length: Optional[int] = Field(
        default=None,
        description="Maximum length of values in the column"
    )
    
    # Additional statistical fields
    total_count: Optional[int] = Field(
        default=None,
        description="Total number of values in the column"
    )
    
    distinct_count: Optional[int] = Field(
        default=None,
        description="Number of distinct values in the column"
    )
    
    completeness_ratio: Optional[float] = Field(
        default=None,
        description="Ratio of non-null values (completeness)"
    )
    
    uniqueness_ratio: Optional[float] = Field(
        default=None,
        description="Ratio of unique values (uniqueness)"
    )
    
    # Pattern analysis
    common_patterns: Optional[List[str]] = Field(
        default=None,
        sa_column=Column(ARRAY(String)),
        description="Common patterns found in the column values"
    )
    
    validation_rules: Optional[List[str]] = Field(
        default=None,
        sa_column=Column(ARRAY(String)),
        description="Suggested validation rules for the column"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "file_id": 1,
                "column_name": "email_address",
                "column_position": 3,
                "data_type": "EMAIL",
                "sample_values": [
                    "john.doe@email.com",
                    "jane.smith@company.com",
                    "user@domain.org"
                ],
                "null_count": 5,
                "unique_count": 95,
                "min_length": 10,
                "max_length": 50,
                "total_count": 100,
                "distinct_count": 95,
                "completeness_ratio": 0.95,
                "uniqueness_ratio": 0.95,
                "common_patterns": [
                    "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
                ],
                "validation_rules": [
                    "EMAIL_FORMAT",
                    "NOT_NULL",
                    "MAX_LENGTH_50"
                ]
            }
        }


class ColumnStructureCreate(SQLModel):
    """Schema for creating a new column structure entry"""
    file_id: str
    column_name: str = Field(max_length=255)
    column_position: Optional[int] = None
    data_type: DataType
    sample_values: Optional[List[str]] = None
    null_count: Optional[int] = None
    unique_count: Optional[int] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    total_count: Optional[int] = None
    distinct_count: Optional[int] = None
    completeness_ratio: Optional[float] = None
    uniqueness_ratio: Optional[float] = None
    common_patterns: Optional[List[str]] = None
    validation_rules: Optional[List[str]] = None


class ColumnStructureUpdate(SQLModel):
    """Schema for updating a column structure entry"""
    column_name: Optional[str] = Field(default=None, max_length=255)
    column_position: Optional[int] = None
    data_type: Optional[DataType] = None
    sample_values: Optional[List[str]] = None
    null_count: Optional[int] = None
    unique_count: Optional[int] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    total_count: Optional[int] = None
    distinct_count: Optional[int] = None
    completeness_ratio: Optional[float] = None
    uniqueness_ratio: Optional[float] = None
    common_patterns: Optional[List[str]] = None
    validation_rules: Optional[List[str]] = None


class ColumnStructureRead(SQLModel):
    """Schema for reading column structure data"""
    structure_id: str
    file_id: str
    column_name: str
    column_position: Optional[int]
    data_type: DataType
    sample_values: Optional[List[str]]
    null_count: Optional[int]
    unique_count: Optional[int]
    min_length: Optional[int]
    max_length: Optional[int]
    total_count: Optional[int]
    distinct_count: Optional[int]
    completeness_ratio: Optional[float]
    uniqueness_ratio: Optional[float]
    common_patterns: Optional[List[str]]
    validation_rules: Optional[List[str]]
    created_at: datetime


class ColumnStructureBulkCreate(SQLModel):
    """Schema for bulk creating column structures"""
    structures: List[ColumnStructureCreate]
    
    class Config:
        schema_extra = {
            "example": {
                "structures": [
                    {
                        "file_id": 1,
                        "column_name": "customer_name",
                        "column_position": 1,
                        "data_type": "STRING",
                        "sample_values": ["John Doe", "Jane Smith"],
                        "null_count": 0,
                        "unique_count": 100
                    },
                    {
                        "file_id": 1,
                        "column_name": "email",
                        "column_position": 2,
                        "data_type": "EMAIL",
                        "sample_values": ["john@email.com", "jane@email.com"],
                        "null_count": 2,
                        "unique_count": 98
                    }
                ]
            }
        }


class ColumnStructureAnalysis(SQLModel):
    """Schema for column structure analysis results"""
    file_id: int
    total_columns: int
    analyzed_columns: int
    data_quality_score: float
    completeness_score: float
    uniqueness_score: float
    column_summary: List[ColumnStructureRead]
    recommendations: List[str]
    
    class Config:
        schema_extra = {
            "example": {
                "file_id": 1,
                "total_columns": 5,
                "analyzed_columns": 5,
                "data_quality_score": 0.85,
                "completeness_score": 0.90,
                "uniqueness_score": 0.80,
                "column_summary": [],
                "recommendations": [
                    "Column 'phone' has inconsistent format",
                    "Column 'date' contains invalid date values",
                    "Column 'email' has 95% completeness"
                ]
            }
        }