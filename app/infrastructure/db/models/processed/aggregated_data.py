from datetime import datetime
from typing import Optional, Dict, Any

from sqlmodel import SQLModel, Field, Column
from sqlalchemy.dialects.postgresql import JSONB

from app.models.base import BaseModel


class AggregatedDataBase(BaseModel):
    """Base model untuk AggregatedData dengan field-field umum"""
    aggregation_name: str = Field(max_length=100, index=True)
    aggregation_type: str = Field(max_length=50, index=True)  # 'SUM', 'COUNT', 'AVG', 'GROUP_BY'
    dimension_keys: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB))
    measure_values: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB))
    time_period: Optional[str] = Field(default=None, max_length=50)
    batch_id: Optional[str] = Field(default=None, max_length=50, index=True)


class AggregatedData(AggregatedDataBase, table=True):
    """Model untuk tabel processed.aggregated_data"""
    __tablename__ = "aggregated_data"
    __table_args__ = (
        {"schema": "processed"},
    )
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        schema_extra = {
            "example": {
                "aggregation_name": "monthly_sales_summary",
                "aggregation_type": "SUM",
                "dimension_keys": {
                    "region": "north_america",
                    "product_category": "electronics",
                    "year": 2024,
                    "month": 1
                },
                "measure_values": {
                    "total_sales": 125000.50,
                    "total_quantity": 450,
                    "avg_order_value": 277.78,
                    "unique_customers": 89,
                    "total_orders": 156
                },
                "time_period": "2024-01",
                "batch_id": "batch_2024_01_001"
            }
        }


class AggregatedDataCreate(AggregatedDataBase):
    """Schema untuk create aggregated data"""
    pass


class AggregatedDataUpdate(SQLModel):
    """Schema untuk update aggregated data"""
    aggregation_name: Optional[str] = Field(default=None, max_length=100)
    aggregation_type: Optional[str] = Field(default=None, max_length=50)
    dimension_keys: Optional[Dict[str, Any]] = Field(default=None)
    measure_values: Optional[Dict[str, Any]] = Field(default=None)
    time_period: Optional[str] = Field(default=None, max_length=50)
    batch_id: Optional[str] = Field(default=None, max_length=50)


class AggregatedDataRead(AggregatedDataBase):
    """Schema untuk read aggregated data"""
    aggregation_id: int
    created_at: datetime


class AggregatedDataFilter(SQLModel):
    """Schema untuk filter aggregated data"""
    aggregation_name: Optional[str] = Field(default=None)
    aggregation_type: Optional[str] = Field(default=None)
    time_period: Optional[str] = Field(default=None)
    batch_id: Optional[str] = Field(default=None)
    dimension_filter: Optional[Dict[str, Any]] = Field(default=None)
    measure_filter: Optional[Dict[str, Any]] = Field(default=None)
    date_from: Optional[datetime] = Field(default=None)
    date_to: Optional[datetime] = Field(default=None)


class AggregatedDataSummary(SQLModel):
    """Schema untuk summary aggregated data"""
    aggregation_type: str
    total_records: int
    latest_batch: Optional[str] = Field(default=None)
    latest_update: Optional[datetime] = Field(default=None)
    time_periods: Optional[list] = Field(default=None)