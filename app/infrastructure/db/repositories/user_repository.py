import logging
from typing import Optional, List, Dict, Any
from uuid import UUID

from sqlmodel import Session, select
from sqlalchemy.orm import selectinload

from .base import BaseRepository
from ..models.auth import User, UserProfile
from ....core.exceptions import DatabaseError
from ....infrastructure.db.models import User as UserEntity, UserProfile as UserProfileEntity
from ....domain.value_objects.email import Email

logger = logging.getLogger(__name__)


class UserRepository(BaseRepository[User]):
    """
    User repository implementation.
    """
    
    def __init__(self, session: Session):
        super().__init__(User, session)
    
    def _db_to_entity(self, db_user: User) -> UserEntity:
        """
        Convert database model to domain entity.
        
        Args:
            db_user: Database user model
            
        Returns:
            User domain entity
        """
        profile_entity = None
        if db_user.profile:
            profile_entity = UserProfileEntity(
                id=db_user.profile.id,
                user_id=db_user.profile.user_id,
                bio=db_user.profile.bio,
                avatar_url=db_user.profile.avatar_url,
                phone=db_user.profile.phone,
                location=db_user.profile.location,
                website=db_user.profile.website,
                created_at=db_user.profile.created_at,
                updated_at=db_user.profile.updated_at,
            )
        
        return UserEntity(
            id=db_user.id,
            email=Email(db_user.email),
            username=db_user.username,
            full_name=db_user.full_name,
            hashed_password=db_user.hashed_password,
            is_active=db_user.is_active,
            is_verified=db_user.is_verified,
            is_superuser=db_user.is_superuser,
            created_at=db_user.created_at,
            updated_at=db_user.updated_at,
            last_login=db_user.last_login,
            profile=profile_entity,
        )
    
    def _entity_to_db(self, user_entity: UserEntity) -> Dict[str, Any]:
        """
        Convert domain entity to database model data.
        
        Args:
            user_entity: User domain entity
            
        Returns:
            Dictionary with database model data
        """
        return {
            "id": user_entity.id,
            "email": str(user_entity.email),
            "username": user_entity.username,
            "full_name": user_entity.full_name,
            "hashed_password": user_entity.hashed_password,
            "is_active": user_entity.is_active,
            "is_verified": user_entity.is_verified,
            "is_superuser": user_entity.is_superuser,
            "created_at": user_entity.created_at,
            "updated_at": user_entity.updated_at,
            "last_login": user_entity.last_login,
        }
    
    async def create(self, user_entity: UserEntity) -> UserEntity:
        """
        Create a new user.
        
        Args:
            user_entity: User entity to create
            
        Returns:
            Created user entity
        """
        try:
            user_data = self._entity_to_db(user_entity)
            db_user = await super().create(user_data)
            return self._db_to_entity(db_user)
            
        except Exception as e:
            logger.error(f"Failed to create user: {e}")
            raise DatabaseError(f"Failed to create user: {str(e)}")
    
    async def get(self, user_id: UUID) -> Optional[UserEntity]:
        """
        Get user by ID.
        
        Args:
            user_id: User ID
            
        Returns:
            User entity or None if not found
        """
        try:
            db_user = await super().get(user_id)
            return self._db_to_entity(db_user) if db_user else None
            
        except Exception as e:
            logger.error(f"Failed to get user by ID {user_id}: {e}")
            raise DatabaseError(f"Failed to get user: {str(e)}")
    
    async def update(self, user_entity: UserEntity) -> UserEntity:
        """
        Update existing user.
        
        Args:
            user_entity: User entity with updated data
            
        Returns:
            Updated user entity
        """
        try:
            user_data = self._entity_to_db(user_entity)
            db_user = await super().update(id=user_entity.id, obj_in=user_data)
            if not db_user:
                raise DatabaseError(f"User {user_entity.id} not found for update")
            return self._db_to_entity(db_user)
            
        except Exception as e:
            logger.error(f"Failed to update user {user_entity.id}: {e}")
            raise DatabaseError(f"Failed to update user: {str(e)}")
    
    async def get_active_users(
        self,
        *,
        skip: int = 0,
        limit: int = 100
    ) -> List[UserEntity]:
        """
        Get active users.
        
        Args:
            skip: Number of records to skip
            limit: Maximum number of records to return
            
        Returns:
            List of active user entities
        """
        try:
            statement = (
                select(User)
                .where(User.is_active == True)
                .offset(skip)
                .limit(limit)
                .order_by(User.created_at.desc())
            )
            result = self.session.exec(statement)
            db_users = result.all()
            return [self._db_to_entity(db_user) for db_user in db_users]
            
        except Exception as e:
            logger.error(f"Failed to get active users: {e}")
            raise DatabaseError(f"Failed to get active users: {str(e)}")
        
    async def get_by_email(self, email: str) -> Optional[UserEntity]:
        """
        Get user by email address.
        
        Args:
            email: User email address
            
        Returns:
            User entity or None if not found
        """
        try:
            statement = select(User).where(User.email == email)
            result = self.session.exec(statement)
            db_user = result.first()
            return self._db_to_entity(db_user) if db_user else None
            
        except Exception as e:
            logger.error(f"Failed to get user by email {email}: {e}")
            raise DatabaseError(f"Failed to get user by email: {str(e)}")
    
    async def get_by_username(self, username: str) -> Optional[UserEntity]:
        """
        Get user by username.
        
        Args:
            username: Username
            
        Returns:
            User entity or None if not found
        """
        try:
            statement = select(User).where(User.username == username)
            result = self.session.exec(statement)
            db_user = result.first()
            return self._db_to_entity(db_user) if db_user else None
            
        except Exception as e:
            logger.error(f"Failed to get user by username {username}: {e}")
            raise DatabaseError(f"Failed to get user by username: {str(e)}")
    
    async def get_with_profile(self, user_id: UUID) -> Optional[UserEntity]:
        """
        Get user with profile information.
        
        Args:
            user_id: User ID
            
        Returns:
            User entity with profile or None if not found
        """
        try:
            statement = (
                select(User)
                .options(selectinload(User.profile))
                .where(User.id == user_id)
            )
            result = self.session.exec(statement)
            db_user = result.first()
            return self._db_to_entity(db_user) if db_user else None
            
        except Exception as e:
            logger.error(f"Failed to get user with profile {user_id}: {e}")
            raise DatabaseError(f"Failed to get user with profile: {str(e)}")
        
    async def get_by_email(self, email: str) -> Optional[User]:
        """
        Get user by email address.
        
        Args:
            email: User email address
            
        Returns:
            User instance or None if not found
        """
        try:
            statement = select(User).where(User.email == email)
            result = self.session.exec(statement)
            return result.first()
            
        except Exception as e:
            logger.error(f"Failed to get user by email {email}: {e}")
            raise DatabaseError(f"Failed to get user by email: {str(e)}")
    
    async def get_by_username(self, username: str) -> Optional[User]:
        """
        Get user by username.
        
        Args:
            username: Username
            
        Returns:
            User instance or None if not found
        """
        try:
            statement = select(User).where(User.username == username)
            result = self.session.exec(statement)
            return result.first()
            
        except Exception as e:
            logger.error(f"Failed to get user by username {username}: {e}")
            raise DatabaseError(f"Failed to get user by username: {str(e)}")
    
    async def get_with_profile(self, user_id: UUID) -> Optional[User]:
        """
        Get user with profile information.
        
        Args:
            user_id: User ID
            
        Returns:
            User instance with profile or None if not found
        """
        try:
            statement = (
                select(User)
                .options(selectinload(User.profile))
                .where(User.id == user_id)
            )
            result = self.session.exec(statement)
            return result.first()
            
        except Exception as e:
            logger.error(f"Failed to get user with profile {user_id}: {e}")
            raise DatabaseError(f"Failed to get user with profile: {str(e)}")
    
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
        try:
            statement = (
                select(User)
                .where(User.is_active == True)
                .offset(skip)
                .limit(limit)
                .order_by(User.created_at.desc())
            )
            result = self.session.exec(statement)
            return list(result.all())
            
        except Exception as e:
            logger.error(f"Failed to get active users: {e}")
            raise DatabaseError(f"Failed to get active users: {str(e)}")
    
    async def search_users(
        self,
        query: str,
        *,
        skip: int = 0,
        limit: int = 100
    ) -> List[User]:
        """
        Search users by email, username, or full name.
        
        Args:
            query: Search query
            skip: Number of records to skip
            limit: Maximum number of records to return
            
        Returns:
            List of matching users
        """
        try:
            search_term = f"%{query}%"
            statement = (
                select(User)
                .where(
                    (User.email.ilike(search_term)) |
                    (User.username.ilike(search_term)) |
                    (User.full_name.ilike(search_term))
                )
                .offset(skip)
                .limit(limit)
                .order_by(User.created_at.desc())
            )
            result = self.session.exec(statement)
            return list(result.all())
            
        except Exception as e:
            logger.error(f"Failed to search users with query '{query}': {e}")
            raise DatabaseError(f"Failed to search users: {str(e)}")
    
    async def update_last_login(self, user_id: UUID) -> bool:
        """
        Update user's last login timestamp.
        
        Args:
            user_id: User ID
            
        Returns:
            True if updated successfully
        """
        try:
            from datetime import datetime
            
            user = await self.get(user_id)
            if not user:
                return False
            
            user.last_login = datetime.utcnow()
            self.session.add(user)
            self.session.flush()
            
            logger.debug(f"Updated last login for user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update last login for user {user_id}: {e}")
            raise DatabaseError(f"Failed to update last login: {str(e)}")
    
    async def activate_user(self, user_id: UUID) -> bool:
        """
        Activate a user account.
        
        Args:
            user_id: User ID
            
        Returns:
            True if activated successfully
        """
        try:
            user = await self.get(user_id)
            if not user:
                return False
            
            user.is_active = True
            user.is_verified = True
            self.session.add(user)
            self.session.flush()
            
            logger.debug(f"Activated user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to activate user {user_id}: {e}")
            raise DatabaseError(f"Failed to activate user: {str(e)}")
    
    async def deactivate_user(self, user_id: UUID) -> bool:
        """
        Deactivate a user account.
        
        Args:
            user_id: User ID
            
        Returns:
            True if deactivated successfully
        """
        try:
            user = await self.get(user_id)
            if not user:
                return False
            
            user.is_active = False
            self.session.add(user)
            self.session.flush()
            
            logger.debug(f"Deactivated user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to deactivate user {user_id}: {e}")
            raise DatabaseError(f"Failed to deactivate user: {str(e)}")


class UserProfileRepository(BaseRepository[UserProfile]):
    """
    User profile repository implementation.
    """
    
    def __init__(self, session: Session):
        super().__init__(UserProfile, session)
    
    def _db_to_entity(self, db_profile: UserProfile) -> UserProfileEntity:
        """Convert database model to domain entity."""
        return UserProfileEntity(
            id=db_profile.id,
            user_id=db_profile.user_id,
            bio=db_profile.bio,
            avatar_url=db_profile.avatar_url,
            phone=db_profile.phone,
            location=db_profile.location,
            website=db_profile.website,
            created_at=db_profile.created_at,
            updated_at=db_profile.updated_at,
        )
    
    def _entity_to_db(self, profile_entity: UserProfileEntity) -> Dict[str, Any]:
        """Convert domain entity to database model data."""
        return {
            "id": profile_entity.id,
            "user_id": profile_entity.user_id,
            "bio": profile_entity.bio,
            "avatar_url": profile_entity.avatar_url,
            "phone": profile_entity.phone,
            "location": profile_entity.location,
            "website": profile_entity.website,
            "created_at": profile_entity.created_at,
            "updated_at": profile_entity.updated_at,
        }
    
    async def get_by_user_id(self, user_id: UUID) -> Optional[UserProfileEntity]:
        """
        Get user profile by user ID.
        
        Args:
            user_id: User ID
            
        Returns:
            UserProfile entity or None if not found
        """
        try:
            statement = select(UserProfile).where(UserProfile.user_id == user_id)
            result = self.session.exec(statement)
            db_profile = result.first()
            return self._db_to_entity(db_profile) if db_profile else None
            
        except Exception as e:
            logger.error(f"Failed to get user profile by user_id {user_id}: {e}")
            raise DatabaseError(f"Failed to get user profile: {str(e)}")
    
    async def create_profile(self, profile_entity: UserProfileEntity) -> UserProfileEntity:
        """
        Create user profile.
        
        Args:
            profile_entity: Profile entity to create
            
        Returns:
            Created profile entity
        """
        try:
            profile_data = self._entity_to_db(profile_entity)
            db_profile = await super().create(profile_data)
            return self._db_to_entity(db_profile)
            
        except Exception as e:
            logger.error(f"Failed to create profile for user {profile_entity.user_id}: {e}")
            raise DatabaseError(f"Failed to create profile: {str(e)}")
    
    async def update_profile(self, profile_entity: UserProfileEntity) -> UserProfileEntity:
        """
        Update user profile.
        
        Args:
            profile_entity: Profile entity with updated data
            
        Returns:
            Updated profile entity
        """
        try:
            profile_data = self._entity_to_db(profile_entity)
            db_profile = await super().update(id=profile_entity.id, obj_in=profile_data)
            if not db_profile:
                raise DatabaseError(f"Profile {profile_entity.id} not found for update")
            return self._db_to_entity(db_profile)
            
        except Exception as e:
            logger.error(f"Failed to update profile {profile_entity.id}: {e}")
            raise DatabaseError(f"Failed to update profile: {str(e)}")
    
    async def create_or_update_profile(
        self,
        user_id: UUID,
        profile_data: dict
    ) -> UserProfileEntity:
        """
        Create or update user profile.
        
        Args:
            user_id: User ID
            profile_data: Profile data dictionary
            
        Returns:
            UserProfile entity
        """
        try:
            # Check if profile exists
            existing_profile = await self.get_by_user_id(user_id)
            
            if existing_profile:
                # Update existing profile
                for field, value in profile_data.items():
                    if hasattr(existing_profile, field):
                        setattr(existing_profile, field, value)
                
                return await self.update_profile(existing_profile)
            else:
                # Create new profile
                from uuid import uuid4
                from datetime import datetime
                
                profile_entity = UserProfileEntity(
                    id=uuid4(),
                    user_id=user_id,
                    bio=profile_data.get('bio'),
                    avatar_url=profile_data.get('avatar_url'),
                    phone=profile_data.get('phone'),
                    location=profile_data.get('location'),
                    website=profile_data.get('website'),
                    created_at=datetime.utcnow(),
                )
                
                return await self.create_profile(profile_entity)
                
        except Exception as e:
            logger.error(f"Failed to create/update profile for user {user_id}: {e}")
            raise DatabaseError(f"Failed to create/update profile: {str(e)}")
            