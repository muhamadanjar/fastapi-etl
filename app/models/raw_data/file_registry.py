from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime

class FileRegistry(SQLModel, table=True):
    __tablename__ = "file_registry"
    __table_args__ = {"schema": "raw_data"}

    file_id: Optional[int] = Field(default=None, primary_key=True)
    file_name: str
    file_path: Optional[str]
    file_type: Optional[str]
    file_size: Optional[int]
    source_system: Optional[str]
    upload_date: datetime = Field(default_factory=datetime.utcnow)
    processing_status: str = Field(default="PENDING")
    batch_id: Optional[str]
    created_by: Optional[str]
    metadata: Optional[dict] = Field(default_factory=dict)
