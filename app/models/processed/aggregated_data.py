from sqlmodel import SQLModel, Field
from typing import Optional, Dict
from datetime import datetime

class AggregatedData(SQLModel, table=True):
    __tablename__ = "aggregated_data"
    __table_args__ = {"schema": "processed"}

    aggregation_id: Optional[int] = Field(default=None, primary_key=True)
    aggregation_name: str
    aggregation_type: Optional[str]
    dimension_keys: Optional[Dict] = Field(default_factory=dict)
    measure_values: Optional[Dict] = Field(default_factory=dict)
    time_period: Optional[str]
    created_at: datetime = Field(default_factory=datetime.utcnow)
    batch_id: Optional[str]
