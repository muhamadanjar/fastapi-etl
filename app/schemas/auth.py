from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, EmailStr, Field
from .base import BaseResponse


class UserLogin(BaseModel):
    """Schema for user login request."""
    username: str = Field(min_length=3, max_length=100)
    password: str = Field(min_length=6, max_length=128)
    remember_me: bool = False


class UserCreate(BaseModel):
    """Schema for user creation."""
    username: str = Field(min_length=3, max_length=100)
    email: EmailStr
    password: str = Field(min_length=6, max_length=128)
    full_name: Optional[str] = Field(default=None, max_length=200)
    is_active: bool = True
    roles: List[str] = Field(default=["user"], description="User roles")


class UserRead(BaseModel):
    """Schema for user response."""
    user_id: int
    username: str
    email: EmailStr
    full_name: Optional[str] = None
    is_active: bool
    roles: List[str]
    created_at: datetime
    last_login: Optional[datetime] = None
    

class UserUpdate(BaseModel):
    """Schema for user update."""
    email: Optional[EmailStr] = None
    full_name: Optional[str] = Field(default=None, max_length=200)
    is_active: Optional[bool] = None
    roles: Optional[List[str]] = None


class PasswordChange(BaseModel):
    """Schema for password change."""
    current_password: str = Field(min_length=6, max_length=128)
    new_password: str = Field(min_length=6, max_length=128)
    confirm_password: str = Field(min_length=6, max_length=128)


class PasswordReset(BaseModel):
    """Schema for password reset request."""
    email: EmailStr


class PasswordResetConfirm(BaseModel):
    """Schema for password reset confirmation."""
    token: str
    new_password: str = Field(min_length=6, max_length=128)
    confirm_password: str = Field(min_length=6, max_length=128)


class Token(BaseModel):
    """Schema for authentication token."""
    access_token: str
    token_type: str = "bearer"
    expires_in: int = Field(description="Token expiration time in seconds")
    refresh_token: Optional[str] = None


class TokenData(BaseModel):
    """Schema for token payload data."""
    user_id: int
    username: str
    roles: List[str]
    exp: datetime


class RefreshToken(BaseModel):
    """Schema for token refresh request."""
    refresh_token: str


class RoleCreate(BaseModel):
    """Schema for role creation."""
    name: str = Field(min_length=2, max_length=50)
    description: Optional[str] = Field(default=None, max_length=255)
    permissions: List[str] = Field(default=[], description="List of permissions")


class RoleRead(BaseModel):
    """Schema for role response."""
    role_id: int
    name: str
    description: Optional[str] = None
    permissions: List[str]
    created_at: datetime
    is_active: bool


class RoleUpdate(BaseModel):
    """Schema for role update."""
    description: Optional[str] = Field(default=None, max_length=255)
    permissions: Optional[List[str]] = None
    is_active: Optional[bool] = None


class PermissionSchema(BaseModel):
    """Schema for permission."""
    name: str
    description: Optional[str] = None
    resource: str = Field(description="Resource the permission applies to")
    action: str = Field(description="Action the permission allows")


class LoginResponse(BaseResponse):
    """Schema for login response."""
    user: UserRead
    token: Token


class UserSession(BaseModel):
    """Schema for user session information."""
    session_id: str
    user_id: int
    username: str
    ip_address: str
    user_agent: str
    created_at: datetime
    last_activity: datetime
    is_active: bool