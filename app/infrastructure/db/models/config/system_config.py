from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field

from app.models.base import BaseModel


class SystemConfigBase(BaseModel):
    """Base model for system configuration."""
    config_category: Optional[str] = Field(default=None, max_length=100, description="Category of configuration")
    config_key: Optional[str] = Field(default=None, max_length=100, description="Configuration key")
    config_value: Optional[str] = Field(default=None, description="Configuration value")
    config_type: Optional[str] = Field(
        default=None, 
        max_length=50, 
        description="Type of configuration: STRING, NUMBER, JSON, BOOLEAN"
    )
    description: Optional[str] = Field(default=None, description="Description of the configuration")
    is_encrypted: bool = Field(default=False, description="Whether the value is encrypted")


class SystemConfig(SystemConfigBase, table=True):
    """System configuration model for database storage."""
    __tablename__ = "system_config"
    __table_args__ = {"schema": "config"}
    
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation timestamp")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Last update timestamp")


class SystemConfigCreate(SystemConfigBase):
    """Schema for creating system configuration."""
    pass


class SystemConfigRead(SystemConfigBase):
    """Schema for reading system configuration."""
    config_id: str
    created_at: datetime
    updated_at: datetime


class SystemConfigUpdate(SQLModel):
    """Schema for updating system configuration."""
    config_category: Optional[str] = Field(default=None, max_length=100)
    config_key: Optional[str] = Field(default=None, max_length=100)
    config_value: Optional[str] = Field(default=None)
    config_type: Optional[str] = Field(default=None, max_length=50)
    description: Optional[str] = Field(default=None)
    is_encrypted: Optional[bool] = Field(default=None)
    updated_at: datetime = Field(default_factory=datetime.utcnow)