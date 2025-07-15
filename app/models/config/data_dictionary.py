from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime

class DataDictionary(SQLModel, table=True):
    __tablename__ = "data_dictionary"
    __table_args__ = {"schema": "config"}

    dict_id: Optional[int] = Field(default=None, primary_key=True)
    entity_name: str
    field_name: str
    field_type: Optional[str]
    field_description: Optional[str]
    business_rules: Optional[str]
    sample_values: Optional[list] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    