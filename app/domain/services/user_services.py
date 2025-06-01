import logging
from typing import Optional
from uuid import UUID

from ..entities.user_entity import User
from ..value_objects.email import Email
from ..repositories.user_repository import UserRepositoryInterface
from ..exceptions.user_exceptions import (
    UserAlreadyExistsError,
    UserNotFoundError,
    InvalidCredentialsError
)

logger = logging.getLogger(__name__)


class UserDomainService:
    """
    User domain service.
    
    Contains business logic that doesn't naturally fit in entities.
    """
    
    def __init__(self, user_repository: UserRepositoryInterface):
        self.user_repository = user_repository
    
    async def ensure_user_unique(self, email: Email, username: str, exclude_id: Optional[UUID] = None) -> None:
        """
        Ensure user email and username are unique.
        
        Args:
            email: Email to check
            username: Username to check
            exclude_id: User ID to exclude from check (for updates)
            
        Raises:
            UserAlreadyExistsError: If email or username already exists
        """
        # Check email uniqueness
        existing_user = await self.user_repository.get_by_email(str(email))
        if existing_user and (exclude_id is None or existing_user.id != exclude_id):
            raise UserAlreadyExistsError(f"User with email {email} already exists")
        
        # Check username uniqueness
        existing_user = await self.user_repository.get_by_username(username)
        if existing_user and (exclude_id is None or existing_user.id != exclude_id):
            raise UserAlreadyExistsError(f"User with username {username} already exists")
    
    async def validate_user_credentials(self, email: Email, password_hash: str) -> User:
        """
        Validate user credentials for login.
        
        Args:
            email: User email
            password_hash: Hashed password to verify
            
        Returns:
            User entity if credentials are valid
            
        Raises:
            InvalidCredentialsError: If credentials are invalid
            UserNotFoundError: If user doesn't exist
        """
        user = await self.user_repository.get_by_email(str(email))
        if not user:
            raise UserNotFoundError(f"User with email {email} not found")
        
        # In a real implementation, you would verify the password hash here
        # This is just a placeholder for the business logic
        if user.hashed_password != password_hash:
            raise InvalidCredentialsError("Invalid password")
        
        if not user.can_login():
            raise InvalidCredentialsError("User account is inactive or not verified")
        
        return user
    
    async def can_user_be_deleted(self, user_id: UUID) -> bool:
        """
        Check if user can be safely deleted.
        
        Args:
            user_id: User ID
            
        Returns:
            True if user can be deleted
        """
        user = await self.user_repository.get(user_id)
        if not user:
            return False
        
        # Business rules for user deletion
        # For example: superusers cannot be deleted, users with active orders, etc.
        if user.is_superuser:
            logger.warning(f"Attempted to delete superuser {user_id}")
            return False
        
        # Add more business rules as needed
        return True
    
    async def get_user_statistics(self) -> dict:
        """
        Get user statistics.
        
        Returns:
            Dictionary with user statistics
        """
        total_users = await self.user_repository.count()
        active_users = await self.user_repository.count(is_active=True)
        verified_users = await self.user_repository.count(is_verified=True)
        superusers = await self.user_repository.count(is_superuser=True)
        
        return {
            "total_users": total_users,
            "active_users": active_users,
            "verified_users": verified_users,
            "superusers": superusers,
            "inactive_users": total_users - active_users,
            "unverified_users": total_users - verified_users,
        }