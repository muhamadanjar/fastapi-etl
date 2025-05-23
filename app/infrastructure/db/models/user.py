"""
User SQLModel for database operations.

This module contains the SQLModel for users with simplified
database operations and automatic type safety.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlmodel import SQLModel, Field, Relationship, Index
from sqlalchemy import Column, String, Text

from .base import BaseModel, AuditMixin


class UserBase(SQLModel):
    """Base user model with shared fields."""
    
    email: str = Field(
        max_length=254,
        unique=True,
        index=True,
        regex=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        description="User's email address"
    )
    first_name: str = Field(max_length=50, description="User's first name")
    last_name: str = Field(max_length=50, description="User's last name")
    role: str = Field(default="user", max_length=20, index=True, description="User role")
    is_active: bool = Field(default=True, index=True, description="Whether user is active")
    is_verified: bool = Field(default=False, index=True, description="Whether email is verified")
    
    # Optional contact information
    phone_number: Optional[str] = Field(default=None, max_length=20, description="Phone number")
    phone_country_code: Optional[str] = Field(default=None, max_length=5, description="Phone country code")
    
    # Profile information
    avatar_url: Optional[str] = Field(default=None, max_length=500, description="Avatar URL")
    bio: Optional[str] = Field(default=None, sa_column=Column(Text), description="User bio")
    timezone: str = Field(default="UTC", max_length=50, description="User timezone")
    locale: str = Field(default="en", max_length=10, description="User locale")
    
    class Config:
        schema_extra = {
            "example": {
                "email": "user@example.com",
                "first_name": "John",
                "last_name": "Doe",
                "role": "user",
                "is_active": True,
                "is_verified": False,
                "phone_number": "+1234567890",
                "timezone": "UTC",
                "locale": "en"
            }
        }


class User(BaseModel, AuditMixin, UserBase, table=True):
    """
    User database model with all fields for persistence.
    """
    
    __tablename__ = "users"
    
    # Authentication fields
    password_hash: str = Field(max_length=255, description="Hashed password")
    
    # Security and authentication tracking
    last_login_at: Optional[datetime] = Field(default=None, index=True, description="Last login timestamp")
    failed_login_attempts: int = Field(default=0, description="Failed login attempts count")
    locked_until: Optional[datetime] = Field(default=None, index=True, description="Account locked until")
    password_changed_at: Optional[datetime] = Field(
        default_factory=datetime.utcnow,
        description="When password was last changed"
    )
    
    # Email verification
    email_verification_token: Optional[str] = Field(default=None, max_length=255, index=True)
    email_verification_token_expires: Optional[datetime] = Field(default=None)
    email_verified_at: Optional[datetime] = Field(default=None)
    
    # Password reset
    password_reset_token: Optional[str] = Field(default=None, max_length=255, index=True)
    password_reset_token_expires: Optional[datetime] = Field(default=None)
    
    # Preferences and settings
    preferences: Optional[str] = Field(default=None, sa_column=Column(Text), description="JSON preferences")
    marketing_emails_enabled: bool = Field(default=True, description="Marketing emails enabled")
    notifications_enabled: bool = Field(default=True, description="Notifications enabled")
    
    # Terms and privacy
    terms_accepted_at: Optional[datetime] = Field(default=None, description="Terms acceptance timestamp")
    privacy_policy_accepted_at: Optional[datetime] = Field(default=None, description="Privacy policy acceptance")
    
    # Account deactivation
    deactivated_at: Optional[datetime] = Field(default=None, description="Deactivation timestamp")
    deactivation_reason: Optional[str] = Field(default=None, max_length=500, description="Deactivation reason")
    
    # Computed properties
    @property
    def full_name(self) -> str:
        """Get user's full name."""
        return f"{self.first_name} {self.last_name}".strip()
    
    @property
    def is_locked(self) -> bool:
        """Check if user account is currently locked."""
        if not self.locked_until:
            return False
        return self.locked_until > datetime.utcnow()
    
    @property
    def can_login(self) -> bool:
        """Check if user can currently login."""
        return (
            self.is_active and
            self.is_verified and
            not self.is_locked and
            not self.is_deleted
        )
    
    # Utility methods
    def set_preferences(self, preferences: dict) -> None:
        """Set user preferences as JSON string."""
        import json
        self.preferences = json.dumps(preferences, default=str)
    
    def get_preferences(self) -> dict:
        """Get user preferences as dictionary."""
        if not self.preferences:
            return {}
        
        import json
        try:
            return json.loads(self.preferences)
        except (json.JSONDecodeError, TypeError):
            return {}
    
    def generate_email_verification_token(self) -> str:
        """Generate email verification token."""
        import secrets
        from datetime import timedelta
        
        token = secrets.token_urlsafe(32)
        self.email_verification_token = token
        self.email_verification_token_expires = datetime.utcnow() + timedelta(hours=24)
        
        return token
    
    def generate_password_reset_token(self) -> str:
        """Generate password reset token."""
        import secrets
        from datetime import timedelta
        
        token = secrets.token_urlsafe(32)
        self.password_reset_token = token
        self.password_reset_token_expires = datetime.utcnow() + timedelta(hours=1)
        
        return token
    
    def clear_email_verification_token(self) -> None:
        """Clear email verification token after successful verification."""
        self.email_verification_token = None
        self.email_verification_token_expires = None
        self.email_verified_at = datetime.utcnow()
        self.is_verified = True
    
    def clear_password_reset_token(self) -> None:
        """Clear password reset token after successful reset."""
        self.password_reset_token = None
        self.password_reset_token_expires = None
        self.password_changed_at = datetime.utcnow()
    
    def record_login_attempt(self, success: bool) -> None:
        """Record login attempt."""
        if success:
            self.last_login_at = datetime.utcnow()
            self.failed_login_attempts = 0
            self.locked_until = None
        else:
            self.failed_login_attempts += 1
            
            # Lock account after 5 failed attempts
            if self.failed_login_attempts >= 5:
                from datetime import timedelta
                self.locked_until = datetime.utcnow() + timedelta(minutes=30)
    
    def unlock_account(self) -> None:
        """Manually unlock user account."""
        self.locked_until = None
        self.failed_login_attempts = 0
    
    def deactivate_account(self, reason: Optional[str] = None) -> None:
        """Deactivate user account."""
        self.is_active = False
        self.deactivated_at = datetime.utcnow()
        self.deactivation_reason = reason
    
    def reactivate_account(self) -> None:
        """Reactivate user account."""
        self.is_active = True
        self.deactivated_at = None
        self.deactivation_reason = None
        self.locked_until = None
        self.failed_login_attempts = 0


class UserCreate(UserBase):
    """Model for creating users."""
    password: str = Field(min_length=8, max_length=128, description="User password")


class UserUpdate(SQLModel):
    """Model for updating users."""
    first_name: Optional[str] = Field(default=None, max_length=50)
    last_name: Optional[str] = Field(default=None, max_length=50)
    phone_number: Optional[str] = Field(default=None, max_length=20)
    phone_country_code: Optional[str] = Field(default=None, max_length=5)
    avatar_url: Optional[str] = Field(default=None, max_length=500)
    bio: Optional[str] = Field(default=None)
    timezone: Optional[str] = Field(default=None, max_length=50)
    locale: Optional[str] = Field(default=None, max_length=10)
    marketing_emails_enabled: Optional[bool] = Field(default=None)
    notifications_enabled: Optional[bool] = Field(default=None)


class UserPublic(UserBase):
    """Public user model for API responses."""
    id: UUID
    created_at: datetime
    updated_at: datetime
    
    # Exclude sensitive fields
    class Config:
        exclude = {
            'password_hash', 'email_verification_token', 'password_reset_token',
            'failed_login_attempts', 'locked_until', 'deactivated_at'
        }


class UserAdmin(UserPublic):
    """Admin user model with additional fields."""
    last_login_at: Optional[datetime]
    failed_login_attempts: int
    locked_until: Optional[datetime]
    deactivated_at: Optional[datetime]
    deactivation_reason: Optional[str]
    version: int


# Database indexes for optimized queries
__table_args__ = (
    Index('idx_users_email_active', 'email', 'is_active'),
    Index('idx_users_role_active', 'role', 'is_active'),
    Index('idx_users_created_at_desc', 'created_at'),
    Index('idx_users_verification', 'is_verified', 'email_verification_token'),
    Index('idx_users_password_reset', 'password_reset_token', 'password_reset_token_expires'),
)