from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlmodel import SQLModel, Field

from app.models.base import BaseModel


class LookupTableBase(BaseModel):
    """Base model untuk LookupTable dengan field-field umum"""
    lookup_name: str = Field(max_length=100, index=True)
    lookup_key: str = Field(max_length=100, index=True)
    lookup_value: str = Field(max_length=500)
    lookup_category: Optional[str] = Field(default=None, max_length=100, index=True)
    is_active: bool = Field(default=True, index=True)


class LookupTable(LookupTableBase, table=True):
    """Model untuk tabel staging.lookup_tables"""
    __tablename__ = "lookup_tables"
    __table_args__ = (
        {"schema": "staging"},
    )
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        schema_extra = {
            "example": {
                "lookup_name": "country_codes",
                "lookup_key": "US",
                "lookup_value": "United States",
                "lookup_category": "geography",
                "is_active": True
            }
        }


class LookupTableCreate(LookupTableBase):
    """Schema untuk create lookup table"""
    pass


class LookupTableUpdate(SQLModel):
    """Schema untuk update lookup table"""
    lookup_name: Optional[str] = Field(default=None, max_length=100)
    lookup_key: Optional[str] = Field(default=None, max_length=100)
    lookup_value: Optional[str] = Field(default=None, max_length=500)
    lookup_category: Optional[str] = Field(default=None, max_length=100)
    is_active: Optional[bool] = Field(default=None)


class LookupTableRead(LookupTableBase):
    """Schema untuk read lookup table"""
    created_at: datetime
    updated_at: datetime


class LookupTableFilter(SQLModel):
    """Schema untuk filter lookup table"""
    lookup_name: Optional[str] = Field(default=None)
    lookup_category: Optional[str] = Field(default=None)
    lookup_key: Optional[str] = Field(default=None)
    lookup_value: Optional[str] = Field(default=None)
    is_active: Optional[bool] = Field(default=None)
    search_term: Optional[str] = Field(default=None)  # untuk search di key atau value


class LookupTableBulkCreate(SQLModel):
    """Schema untuk bulk create lookup table"""
    lookup_name: str = Field(max_length=100)
    lookup_category: str = Field(max_length=100)
    entries: List[Dict[str, str]]  # [{"key": "US", "value": "United States"}, ...]
    replace_existing: bool = Field(default=False)


class LookupTableBulkUpdate(SQLModel):
    """Schema untuk bulk update lookup table"""
    lookup_name: str = Field(max_length=100)
    lookup_category: Optional[str] = Field(default=None, max_length=100)
    entries: List[Dict[str, Any]]  # [{"key": "US", "value": "United States", "is_active": True}, ...]


class LookupTableSummary(SQLModel):
    """Schema untuk summary lookup table"""
    lookup_name: str
    lookup_category: Optional[str] = Field(default=None)
    total_entries: int
    active_entries: int
    inactive_entries: int
    last_updated: datetime
    sample_entries: Optional[List[Dict[str, str]]] = Field(default=None)


class LookupTableCategory(SQLModel):
    """Schema untuk kategori lookup table"""
    category_name: str
    total_lookup_tables: int
    total_entries: int
    active_entries: int
    lookup_tables: List[str]


class LookupTableImport(SQLModel):
    """Schema untuk import lookup table dari file"""
    lookup_name: str = Field(max_length=100)
    lookup_category: str = Field(max_length=100)
    file_type: str = Field(max_length=10)  # 'csv', 'json', 'xlsx'
    key_column: str = Field(max_length=100)
    value_column: str = Field(max_length=100)
    has_header: bool = Field(default=True)
    delimiter: Optional[str] = Field(default=",", max_length=5)
    replace_existing: bool = Field(default=False)


class LookupTableExport(SQLModel):
    """Schema untuk export lookup table"""
    lookup_name: Optional[str] = Field(default=None)
    lookup_category: Optional[str] = Field(default=None)
    file_format: str = Field(max_length=10)  # 'csv', 'json', 'xlsx'
    include_inactive: bool = Field(default=False)
    include_metadata: bool = Field(default=False)


class LookupTableValidation(SQLModel):
    """Schema untuk validasi lookup table"""
    lookup_name: str
    validation_type: str  # 'duplicate_keys', 'empty_values', 'consistency'
    validation_result: bool
    issues_found: Optional[List[str]] = Field(default=None)
    total_issues: int = Field(default=0)
    validated_at: datetime = Field(default_factory=datetime.utcnow)


class LookupTableUsage(SQLModel):
    """Schema untuk tracking penggunaan lookup table"""
    lookup_name: str
    lookup_category: Optional[str] = Field(default=None)
    usage_count: int = Field(default=0)
    last_used: Optional[datetime] = Field(default=None)
    used_by_entities: Optional[List[str]] = Field(default=None)
    used_by_transformations: Optional[List[str]] = Field(default=None)
