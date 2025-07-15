from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime

class ChangeLog(SQLModel, table=True):
    __tablename__ = "change_log"
    __table_args__ = {"schema": "audit"}

    change_id: Optional[int] = Field(default=None, primary_key=True)
    table_name: str
    record_id: str
    operation: str
    old_values: Optional[dict] = Field(default_factory=dict)
    new_values: Optional[dict] = Field(default_factory=dict)
    changed_by: Optional[str]
    change_reason: Optional[str]
    changed_at: datetime = Field(default_factory=datetime.utcnow)