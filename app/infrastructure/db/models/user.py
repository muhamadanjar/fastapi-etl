from datetime import datetime
from typing import Optional, List
from uuid import UUID

from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import Column, String, Boolean, DateTime, Text

from .base import BaseModelWithTimestamp


class UserBase(SQLModel):
    """Base user model with shared fields."""
    
    email: str = Field(
        sa_column=Column(String(255), unique=True, index=True, nullable=False),
        description="User email address"
    )
    username: str = Field(
        sa_column=Column(String(50), unique=True, index=True, nullable=False),
        description="Unique username"
    )
    full_name: Optional[str] = Field(
        default=None,
        sa_column=Column(String(255)),
        description="User full name"
    )
    is_active: bool = Field(
        default=True,
        sa_column=Column(Boolean, default=True, nullable=False),
        description="User active status"
    )
    is_verified: bool = Field(
        default=False,
        sa_column=Column(Boolean, default=False, nullable=False),
        description="Email verification status"
    )


class User(UserBase, BaseModelWithTimestamp, table=True):
    """User database model."""
    
    __tablename__ = "users"
    
    password: str = Field(
        sa_column=Column(String(255), nullable=False),
        description="Hashed password"
    )
    is_superuser: bool = Field(
        default=False,
        sa_column=Column(Boolean, default=False, nullable=False),
        description="Superuser status"
    )
    last_login: Optional[datetime] = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True)),
        description="Last login timestamp"
    )
    
    # Relationships
    profile: Optional["UserProfile"] = Relationship(
        back_populates="user",
        sa_relationship_kwargs={"uselist": False, "cascade": "all, delete-orphan"}
    )


class UserProfileBase(SQLModel):
    """Base user profile model."""
    
    bio: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="User biography"
    )
    avatar_url: Optional[str] = Field(
        default=None,
        sa_column=Column(String(500)),
        description="Avatar image URL"
    )
    phone: Optional[str] = Field(
        default=None,
        sa_column=Column(String(20)),
        description="Phone number"
    )
    location: Optional[str] = Field(
        default=None,
        sa_column=Column(String(255)),
        description="User location"
    )
    website: Optional[str] = Field(
        default=None,
        sa_column=Column(String(500)),
        description="Personal website URL"
    )


class UserProfile(UserProfileBase, BaseModelWithTimestamp, table=True):
    """User profile database model."""
    
    __tablename__ = "user_profiles"
    
    user_id: UUID = Field(
        foreign_key="users.id",
        nullable=False,
        index=True,
        description="Reference to user"
    )
    
    # Relationships
    user: User = Relationship(back_populates="profile")
