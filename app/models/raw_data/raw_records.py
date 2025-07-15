from sqlmodel import SQLModel, Field
from typing import Optional, List
from datetime import datetime

class RawRecord(SQLModel, table=True):
    __tablename__ = "raw_records"
    __table_args__ = {"schema": "raw_data"}

    record_id: Optional[int] = Field(default=None, primary_key=True)
    file_id: int = Field(foreign_key="raw_data.file_registry.file_id")
    sheet_name: Optional[str]
    row_number: Optional[int]
    column_mapping: Optional[dict] = Field(default_factory=dict)
    raw_data: dict
    data_hash: Optional[str]
    validation_status: str = Field(default="UNVALIDATED")
    validation_errors: Optional[List[str]] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    batch_id: Optional[str]
    