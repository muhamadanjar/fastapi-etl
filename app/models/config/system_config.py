from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime

class SystemConfig(SQLModel, table=True):
    __tablename__ = "system_config"
    __table_args__ = {"schema": "config"}

    config_id: Optional[int] = Field(default=None, primary_key=True)
    config_category: Optional[str]
    config_key: str
    config_value: str
    config_type: Optional[str]
    description: Optional[str]
    is_encrypted: bool = False
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
