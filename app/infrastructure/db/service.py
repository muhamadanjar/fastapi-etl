"""
Database service layer for simplified operations.

This module provides high-level database operations using SQLModel
with automatic session management and simplified APIs.
"""

import logging
from typing import Type, TypeVar, Optional, List, Dict, Any
from contextlib import contextmanager
from uuid import UUID

from sqlmodel import SQLModel

from .connection import get_session
from .repository import Repository, UserRepository
from .models.user import User, UserCreate, UserUpdate
from ...core.exceptions import DatabaseError

logger = logging.getLogger(__name__)

ModelType = TypeVar("ModelType", bound=SQLModel)


class DatabaseService:
    """
    High-level database service with automatic session management.
    
    Provides simplified database operations without requiring
    manual session handling.
    """
    
    @contextmanager
    def get_repository(self, model: Type[ModelType]):
        """
        Get repository with automatic session management.
        
        Args:
            model: SQLModel class
            
        Yields:
            Repository instance
        """
        with get_session() as session:
            yield Repository(session, model)
    
    @contextmanager
    def get_user_repository(self):
        """
        Get user repository with automatic session management.
        
        Yields:
            UserRepository instance
        """
        with get_session() as session:
            yield UserRepository(session)
    
    def create(self, model: Type[ModelType], data: Dict[str, Any]) -> ModelType:
        """
        Create new record.
        
        Args:
            model: SQLModel class
            data: Data for new record
            
        Returns:
            Created model instance
        """
        with self.get_repository(model) as repo:
            obj = model(**data)
            return repo.create(obj)
    
    def get(self, model: Type[ModelType], id: UUID) -> Optional[ModelType]:
        """
        Get record by ID.
        
        Args:
            model: SQLModel class
            id: Record ID
            
        Returns:
            Model instance if found, None otherwise
        """
        with self.get_repository(model) as repo:
            return repo.get(id)
    
    def get_or_404(self, model: Type[ModelType], id: UUID) -> ModelType:
        """
        Get record by ID or raise NotFoundError.
        
        Args:
            model: SQLModel class
            id: Record ID
            
        Returns:
            Model instance
        """
        with self.get_repository(model) as repo:
            return repo.get_or_404(id)
    
    def update(self, model: Type[ModelType], id: UUID, data: Dict[str, Any]) -> ModelType:
        """
        Update record.
        
        Args:
            model: SQLModel class
            id: Record ID
            data: Update data
            
        Returns:
            Updated model instance
        """
        with self.get_repository(model) as repo:
            return repo.update(id, data)
    
    def delete(self, model: Type[ModelType], id: UUID, soft_delete: bool = True) -> bool:
        """
        Delete record.
        
        Args:
            model: SQLModel class
            id: Record ID
            soft_delete: Whether to use soft delete
            
        Returns:
            True if deleted, False if not found
        """
        with self.get_repository(model) as repo:
            return repo.delete(id, soft_delete)
    
    def list(
        self,
        model: Type[ModelType],
        offset: int = 0,
        limit: int = 100,
        order_by: Optional[str] = None,
        order_desc: bool = False,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[ModelType]:
        """
        List records with pagination and filtering.
        
        Args:
            model: SQLModel class
            offset: Number of records to skip
            limit: Maximum records to return
            order_by: Field to order by
            order_desc: Whether to order descending
            filters: Filters to apply
            
        Returns:
            List of model instances
        """
        with self.get_repository(model) as repo:
            return repo.list(offset, limit, order_by, order_desc, filters)
    
    def count(self, model: Type[ModelType], filters: Optional[Dict[str, Any]] = None) -> int:
        """
        Count records.
        
        Args:
            model: SQLModel class
            filters: Filters to apply
            
        Returns:
            Number of records
        """
        with self.get_repository(model) as repo:
            return repo.count(filters)
    
    def find_by(self, model: Type[ModelType], **kwargs) -> Optional[ModelType]:
        """
        Find single record by field values.
        
        Args:
            model: SQLModel class
            **kwargs: Field values to search for
            
        Returns:
            Model instance if found, None otherwise
        """
        with self.get_repository(model) as repo:
            return repo.find_by(**kwargs)


class UserService:
    """
    Simplified user service with common operations.
    
    Provides high-level user operations without requiring
    repository management.
    """
    
    def __init__(self):
        self.db = DatabaseService()
    
    def create_user(self, user_data: UserCreate) -> User:
        """
        Create new user.
        
        Args:
            user_data: User creation data
            
        Returns:
            Created user
        """
        # Hash password before storing
        from ...core.security import get_password_hash
        
        hashed_password = get_password_hash(user_data.password)
        
        # Convert to dict and replace password with hash
        data = user_data.dict()
        data['password_hash'] = hashed_password
        del data['password']  # Remove plain password
        
        with self.db.get_user_repository() as repo:
            user = User(**data)
            return repo.create(user)
    
    def get_user(self, user_id: UUID) -> Optional[User]:
        """Get user by ID."""
        return self.db.get(User, user_id)
    
    def get_user_or_404(self, user_id: UUID) -> User:
        """Get user by ID or raise NotFoundError."""
        return self.db.get_or_404(User, user_id)
    
    def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email."""
        with self.db.get_user_repository() as repo:
            return repo.find_by_email(email)
    
    def get_user_by_email_or_404(self, email: str) -> User:
        """Get user by email or raise NotFoundError."""
        with self.db.get_user_repository() as repo:
            return repo.find_by_email_or_404(email)
    
    def update_user(self, user_id: UUID, user_data: UserUpdate) -> User:
        """
        Update user.
        
        Args:
            user_id: User ID
            user_data: Update data
            
        Returns:
            Updated user
        """
        # Only include non-None values
        data = {k: v for k, v in user_data.dict().items() if v is not None}
        return self.db.update(User, user_id, data)
    
    def delete_user(self, user_id: UUID, soft_delete: bool = True) -> bool:
        """Delete user."""
        return self.db.delete(User, user_id, soft_delete)
    
    def list_users(
        self,
        offset: int = 0,
        limit: int = 100,
        active_only: bool = True,
        verified_only: bool = False,
        role: Optional[str] = None,
    ) -> List[User]:
        """
        List users with filters.
        
        Args:
            offset: Number of records to skip
            limit: Maximum records to return
            active_only: Only return active users
            verified_only: Only return verified users
            role: Filter by role
            
        Returns:
            List of users
        """
        filters = {}
        
        if active_only:
            filters['is_active'] = True
        
        if verified_only:
            filters['is_verified'] = True
        
        if role:
            filters['role'] = role
        
        return self.db.list(
            User,
            offset=offset,
            limit=limit,
            filters=filters,
            order_by='created_at',
            order_desc=True
        )
    
    def search_users(self, query: str, limit: int = 100) -> List[User]:
        """Search users by name or email."""
        with self.db.get_user_repository() as repo:
            return repo.search_users(query, limit)
    
    def get_user_stats(self) -> Dict[str, Any]:
        """Get user statistics."""
        with self.db.get_user_repository() as repo:
            return repo.get_user_stats()
    
    def verify_user_email(self, user_id: UUID) -> User:
        """Mark user email as verified."""
        user = self.get_user_or_404(user_id)
        
        with self.db.get_user_repository() as repo:
            user.clear_email_verification_token()
            return repo.update(user_id, {
                'is_verified': True,
                'email_verified_at': user.email_verified_at,
                'email_verification_token': None,
                'email_verification_token_expires': None,
            })
    
    def change_user_password(self, user_id: UUID, new_password: str) -> User:
        """Change user password."""
        from ...core.security import get_password_hash
        
        hashed_password = get_password_hash(new_password)
        
        return self.db.update(User, user_id, {
            'password_hash': hashed_password,
            'password_changed_at': datetime.utcnow(),
            'password_reset_token': None,
            'password_reset_token_expires': None,
        })
    
    def activate_user(self, user_id: UUID) -> User:
        """Activate user account."""
        return self.db.update(User, user_id, {
            'is_active': True,
            'deactivated_at': None,
            'deactivation_reason': None,
        })
    
    def deactivate_user(self, user_id: UUID, reason: Optional[str] = None) -> User:
        """Deactivate user account."""
        from datetime import datetime
        
        return self.db.update(User, user_id, {
            'is_active': False,
            'deactivated_at': datetime.utcnow(),
            'deactivation_reason': reason,
        })
    
    def unlock_user(self, user_id: UUID) -> User:
        """Unlock user account."""
        return self.db.update(User, user_id, {
            'locked_until': None,
            'failed_login_attempts': 0,
        })
    
    def record_login_attempt(self, user_id: UUID, success: bool) -> User:
        """Record login attempt."""
        user = self.get_user_or_404(user_id)
        
        with self.db.get_user_repository() as repo:
            user.record_login_attempt(success)
            return repo.update(user_id, {
                'last_login_at': user.last_login_at,
                'failed_login_attempts': user.failed_login_attempts,
                'locked_until': user.locked_until,
            })


# Global service instances
db_service = DatabaseService()
user_service = UserService()

# Export for easy importing
__all__ = [
    'DatabaseService',
    'UserService',
    'db_service',
    'user_service',
]