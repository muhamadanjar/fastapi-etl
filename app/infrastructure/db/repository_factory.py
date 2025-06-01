"""
Repository factory for dependency injection.
"""

from sqlmodel import Session
from typing import TypeVar, Type, Generic

from .repositories.user_repository import UserRepository, UserProfileRepository
from .repositories.base import BaseRepository

T = TypeVar('T', bound=BaseRepository)


class RepositoryFactory:
    """
    Factory for creating repository instances with proper session injection.
    """
    
    def __init__(self, session: Session):
        self.session = session
    
    def get_user_repository(self) -> UserRepository:
        """Get user repository instance."""
        return UserRepository(self.session)
    
    def get_user_profile_repository(self) -> UserProfileRepository:
        """Get user profile repository instance."""
        return UserProfileRepository(self.session)
    
    def create_repository(self, repository_class: Type[T]) -> T:
        """
        Create repository instance of given class.
        
        Args:
            repository_class: Repository class to instantiate
            
        Returns:
            Repository instance
        """
        return repository_class(self.session)


def get_repository_factory(session: Session) -> RepositoryFactory:
    """
    Get repository factory instance.
    
    Args:
        session: Database session
        
    Returns:
        Repository factory instance
    """
    return RepositoryFactory(session)
