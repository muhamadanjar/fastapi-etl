from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime
from app.domain.value_objects.email import Email
from uuid import UUID


@dataclass
class User:
    """
    User domain entity.
    
    This represents the core user business entity with all business rules.
    """
    
    id: UUID
    email: Email
    username: str
    full_name: Optional[str] = None
    hashed_password: str = ""
    is_active: bool = True
    is_verified: bool = False
    is_superuser: bool = False
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None
    last_login: Optional[datetime] = None
    profile: Optional["UserProfile"] = None
    
    def __post_init__(self):
        """Validate entity after initialization."""
        self._validate()
    
    def _validate(self) -> None:
        """Validate user entity business rules."""
        if not self.username or len(self.username) < 3:
            raise ValueError("Username must be at least 3 characters long")
        
        if not self.username.isalnum() and "_" not in self.username:
            raise ValueError("Username can only contain alphanumeric characters and underscores")
        
        if self.full_name and len(self.full_name) > 255:
            raise ValueError("Full name cannot exceed 255 characters")
    
    def activate(self) -> None:
        """Activate user account."""
        self.is_active = True
        self.is_verified = True
        self.updated_at = datetime.utcnow()
    
    def deactivate(self) -> None:
        """Deactivate user account."""
        self.is_active = False
        self.updated_at = datetime.utcnow()
    
    def verify_email(self) -> None:
        """Mark email as verified."""
        self.is_verified = True
        self.updated_at = datetime.utcnow()
    
    def update_last_login(self) -> None:
        """Update last login timestamp."""
        self.last_login = datetime.utcnow()
        self.updated_at = datetime.utcnow()
    
    def change_email(self, new_email: Email) -> None:
        """Change user email address."""
        self.email = new_email
        self.is_verified = False  # Re-verification required
        self.updated_at = datetime.utcnow()
    
    def change_password(self, new_hashed_password: str) -> None:
        """Change user password."""
        if not new_hashed_password:
            raise ValueError("Password hash cannot be empty")
        
        self.hashed_password = new_hashed_password
        self.updated_at = datetime.utcnow()
    
    def update_profile_info(self, full_name: Optional[str] = None) -> None:
        """Update basic profile information."""
        if full_name is not None:
            if len(full_name) > 255:
                raise ValueError("Full name cannot exceed 255 characters")
            self.full_name = full_name
        
        self.updated_at = datetime.utcnow()
    
    def can_login(self) -> bool:
        """Check if user can login."""
        return self.is_active and self.is_verified
    
    def is_admin(self) -> bool:
        """Check if user is admin/superuser."""
        return self.is_superuser and self.is_active
    

@dataclass
class UserProfile:
    """
    User profile domain entity.
    
    Extended user information and preferences.
    """
    
    id: UUID
    user_id: UUID
    bio: Optional[str] = None
    avatar_url: Optional[str] = None
    phone: Optional[str] = None
    location: Optional[str] = None
    website: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Validate entity after initialization."""
        self._validate()
    
    def _validate(self) -> None:
        """Validate profile entity business rules."""
        if self.bio and len(self.bio) > 1000:
            raise ValueError("Bio cannot exceed 1000 characters")
        
        if self.phone and len(self.phone) > 20:
            raise ValueError("Phone number cannot exceed 20 characters")
        
        if self.location and len(self.location) > 255:
            raise ValueError("Location cannot exceed 255 characters")
        
        if self.website and len(self.website) > 500:
            raise ValueError("Website URL cannot exceed 500 characters")
        
        if self.avatar_url and len(self.avatar_url) > 500:
            raise ValueError("Avatar URL cannot exceed 500 characters")
    
    def update_bio(self, bio: str) -> None:
        """Update user bio."""
        if len(bio) > 1000:
            raise ValueError("Bio cannot exceed 1000 characters")
        
        self.bio = bio
        self.updated_at = datetime.utcnow()
    
    def update_avatar(self, avatar_url: str) -> None:
        """Update user avatar."""
        if len(avatar_url) > 500:
            raise ValueError("Avatar URL cannot exceed 500 characters")
        
        self.avatar_url = avatar_url
        self.updated_at = datetime.utcnow()
    
    def update_contact_info(
        self,
        phone: Optional[str] = None,
        location: Optional[str] = None,
        website: Optional[str] = None
    ) -> None:
        """Update contact information."""
        if phone is not None:
            if len(phone) > 20:
                raise ValueError("Phone number cannot exceed 20 characters")
            self.phone = phone
        
        if location is not None:
            if len(location) > 255:
                raise ValueError("Location cannot exceed 255 characters")
            self.location = location
        
        if website is not None:
            if len(website) > 500:
                raise ValueError("Website URL cannot exceed 500 characters")
            self.website = website
        
        self.updated_at = datetime.utcnow()
        