from abc import ABC, abstractmethod
from typing import Optional, List
from uuid import UUID

from ..entities.user_entity import User, UserProfile


class UserRepositoryInterface(ABC):
    """
    User repository interface.
    
    Defines the contract for user data persistence.
    """
    
    @abstractmethod
    async def create(self, user: User) -> User:
        """
        Create a new user.
        
        Args:
            user: User entity to create
            
        Returns:
            Created user entity
        """
        pass
    
    @abstractmethod
    async def get(self, user_id: UUID) -> Optional[User]:
        """
        Get user by ID.
        
        Args:
            user_id: User ID
            
        Returns:
            User entity or None if not found
        """
        pass
    
    @abstractmethod
    async def get_by_email(self, email: str) -> Optional[User]:
        """
        Get user by email address.
        
        Args:
            email: Email address
            
        Returns:
            User entity or None if not found
        """
        pass
    
    @abstractmethod
    async def get_by_username(self, username: str) -> Optional[User]:
        """
        Get user by username.
        
        Args:
            username: Username
            
        Returns:
            User entity or None if not found
        """
        pass
    
    @abstractmethod
    async def get_with_profile(self, user_id: UUID) -> Optional[User]:
        """
        Get user with profile information.
        
        Args:
            user_id: User ID
            
        Returns:
            User entity with profile or None if not found
        """
        pass
    
    @abstractmethod
    async def update(self, user: User) -> User:
        """
        Update existing user.
        
        Args:
            user: User entity with updated data
            
        Returns:
            Updated user entity
        """
        pass
    
    @abstractmethod
    async def delete(self, user_id: UUID) -> bool:
        """
        Delete user.
        
        Args:
            user_id: User ID
            
        Returns:
            True if deleted, False if not found
        """
        pass
    
    @abstractmethod
    async def get_active_users(
        self,
        *,
        skip: int = 0,
        limit: int = 100
    ) -> List[User]:
        """
        Get active users.
        
        Args:
            skip: Number of records to skip
            limit: Maximum number of records to return
            
        Returns:
            List of active users
        """
        pass
    
    @abstractmethod
    async def search_users(
        self,
        query: str,
        *,
        skip: int = 0,
        limit: int = 100
    ) -> List[User]:
        """
        Search users by query.
        
        Args:
            query: Search query
            skip: Number of records to skip
            limit: Maximum number of records to return
            
        Returns:
            List of matching users
        """
        pass
    
    @abstractmethod
    async def count(self, **filters) -> int:
        """
        Count users with optional filters.
        
        Args:
            **filters: Filter criteria
            
        Returns:
            Number of users
        """
        pass
    
    @abstractmethod
    async def exists(self, user_id: UUID) -> bool:
        """
        Check if user exists.
        
        Args:
            user_id: User ID
            
        Returns:
            True if exists, False otherwise
        """
        pass
