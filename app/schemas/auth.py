"""
Authentication schemas for request/response validation.
"""

from typing import Optional
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, EmailStr, validator


class UserBase(BaseModel):
    """Base user schema with common fields."""
    username: str
    email: EmailStr
    full_name: Optional[str] = None
    is_active: bool = True


class UserCreate(UserBase):
    """Schema for user creation."""
    password: str
    
    @validator('password')
    def validate_password(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        if not any(c.isupper() for c in v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not any(c.islower() for c in v):
            raise ValueError('Password must contain at least one lowercase letter')
        if not any(c.isdigit() for c in v):
            raise ValueError('Password must contain at least one digit')
        return v
    
    @validator('username')
    def validate_username(cls, v):
        if len(v) < 3:
            raise ValueError('Username must be at least 3 characters long')
        if not v.isalnum():
            raise ValueError('Username must contain only alphanumeric characters')
        return v.lower()


class UserRead(UserBase):
    """Schema for user response."""
    id: UUID
    is_superuser: bool = False
    created_at: datetime
    updated_at: Optional[datetime] = None
    last_login: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class UserUpdate(BaseModel):
    """Schema for user update."""
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    is_active: Optional[bool] = None


class Token(BaseModel):
    """Schema for token response."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class TokenData(BaseModel):
    """Schema for token data."""
    username: Optional[str] = None
    user_id: Optional[UUID] = None


class PasswordChange(BaseModel):
    """Schema for password change."""
    current_password: str
    new_password: str
    
    @validator('new_password')
    def validate_new_password(cls, v):
        if len(v) < 8:
            raise ValueError('New password must be at least 8 characters long')
        if not any(c.isupper() for c in v):
            raise ValueError('New password must contain at least one uppercase letter')
        if not any(c.islower() for c in v):
            raise ValueError('New password must contain at least one lowercase letter')
        if not any(c.isdigit() for c in v):
            raise ValueError('New password must contain at least one digit')
        return v


class PasswordResetRequest(BaseModel):
    """Schema for password reset request."""
    email: EmailStr


class PasswordResetConfirm(BaseModel):
    """Schema for password reset confirmation."""
    token: str
    new_password: str
    
    @validator('new_password')
    def validate_new_password(cls, v):
        if len(v) < 8:
            raise ValueError('New password must be at least 8 characters long')
        if not any(c.isupper() for c in v):
            raise ValueError('New password must contain at least one uppercase letter')
        if not any(c.islower() for c in v):
            raise ValueError('New password must contain at least one lowercase letter')
        if not any(c.isdigit() for c in v):
            raise ValueError('New password must contain at least one digit')
        return v


class LoginRequest(BaseModel):
    """Schema for login request."""
    username: str
    password: str


class RefreshTokenRequest(BaseModel):
    """Schema for refresh token request."""
    refresh_token: str


class UserProfile(BaseModel):
    """Schema for user profile information."""
    id: UUID
    username: str
    email: EmailStr
    full_name: Optional[str] = None
    is_active: bool
    is_superuser: bool
    created_at: datetime
    last_login: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class UserList(BaseModel):
    """Schema for user list response."""
    users: list[UserRead]
    total: int
    page: int
    size: int
    has_next: bool
    has_prev: bool


class AuthResponse(BaseModel):
    """Schema for authentication response."""
    user: UserRead
    token: Token
    message: str = "Authentication successful"
    