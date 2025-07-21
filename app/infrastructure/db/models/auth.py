import uuid
from sqlmodel import SQLModel, Field, Relationship
from datetime import datetime
from typing import Optional, List
from pydantic import EmailStr

from app.infrastructure.db.models.base import BaseModel


class UserBase(SQLModel):
    """User base model"""
    username: str = Field(unique=True, index=True)
    email: EmailStr = Field(unique=True, index=True)
    full_name: Optional[str] = None
    is_active: bool = Field(default=True)
    is_superuser: bool = Field(default=False)

class User(UserBase, table=True):
    """User model"""
    __tablename__ = "users"
    
    id: Optional[str] = Field(
        default_factory=lambda: str(uuid.uuid4()),
        primary_key=True
    )
    password: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(default=None)
    last_login: Optional[datetime] = Field(default=None)
    
    # Relationships
    created_jobs: List["ETLJob"] = Relationship(back_populates="created_by_user")
    file_uploads: List["FileRegistry"] = Relationship(back_populates="uploaded_by_user")

class UserCreate(UserBase):
    """User creation model"""
    password: str

class UserUpdate(SQLModel):
    """User update model"""
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    is_active: Optional[bool] = None
    is_admin: Optional[bool] = None

class UserResponse(UserBase):
    """User response model"""
    id: str
    created_at: datetime
    last_login: Optional[datetime]

class Token(SQLModel):
    """Token model"""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int

class TokenData(SQLModel):
    """Token data model"""
    user_id: Optional[str] = None
    username: Optional[str] = None


class UserProfile(BaseModel):
    """User profile model"""
    user_id: str = Field(foreign_key="users.id", primary_key=True)
    bio: Optional[str] = None
    profile_picture: Optional[str] = None
    # user: User = Relationship(back_populates="profile")