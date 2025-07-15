from sqlmodel import SQLModel, Field
from typing import Optional, List
from datetime import datetime

class ColumnStructure(SQLModel, table=True):
    __tablename__ = "column_structure"
    __table_args__ = {"schema": "raw_data"}

    structure_id: Optional[int] = Field(default=None, primary_key=True)
    file_id: int = Field(foreign_key="raw_data.file_registry.file_id")
    column_name: str
    column_position: int
    data_type: str
    sample_values: Optional[List[str]] = Field(default_factory=list)
    null_count: Optional[int]
    unique_count: Optional[int]
    min_length: Optional[int]
    max_length: Optional[int]
    created_at: datetime = Field(default_factory=datetime.utcnow)
    