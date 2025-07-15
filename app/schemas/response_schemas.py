from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel

class ColumnStructureCreate(BaseModel):
    file_id: int
    column_name: str
    column_position: int
    data_type: str
    sample_values: Optional[List[str]] = []
    null_count: Optional[int] = None
    unique_count: Optional[int] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None

class ColumnStructureRead(ColumnStructureCreate):
    structure_id: int
    created_at: datetime






class RawRecordCreate(BaseModel):
    file_id: int
    sheet_name: Optional[str]
    row_number: Optional[int]
    column_mapping: Optional[dict] = {}
    raw_data: dict
    data_hash: Optional[str]
    validation_status: Optional[str] = "UNVALIDATED"
    validation_errors: Optional[List[str]] = []
    batch_id: Optional[str]

class RawRecordRead(RawRecordCreate):
    record_id: int
    created_at: datetime