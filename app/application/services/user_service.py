"""
User service for managing user operations.
"""

import asyncio
from typing import Optional, List, Dict, Any
from uuid import UUID
from datetime import datetime
import aiohttp

from app.application.services.base import BaseService
from app.infrastructure.db.models.auth import User, UserCreate, UserUpdate
from app.infrastructure.db.repositories.user_repository import UserRepository
from app.core.security import get_password_hash
from app.core.config import get_settings
from app.infrastructure.cache import cache_manager
from app.core.exceptions import ServiceError

settings = get_settings()
class UserService(BaseService):
    """
    User service for managing user operations.
    """
    
    def __init__(self, db_session):
        super().__init__(db_session)
        self.user_repo = UserRepository(db_session)
    
    def get_service_name(self) -> str:
        return "UserService"
    
    def create_user(self, user_data: UserCreate) -> User:
        """Create new user."""
        hashed_password = get_password_hash(user_data.password)
        
        # Convert to dict and replace password with hash
        data = user_data.dict()
        data['password_hash'] = hashed_password
        del data['password']  # Remove plain password
        
        return self.user_repo.create(User(**data))
    
    def get_user(self, user_id: UUID) -> Optional[User]:
        """Get user by ID."""
        return self.user_repo.get(user_id)
    
    def get_user_or_404(self, user_id: UUID) -> User:
        """Get user by ID or raise NotFoundError."""
        return self.user_repo.get_or_404(user_id)
    
    def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email."""
        return self.user_repo.find_by_email(email)
    
    def get_user_by_email_or_404(self, email: str) -> User:
        """Get user by email or raise NotFoundError."""
        return self.user_repo.find_by_email_or_404(email)
    
    def update_user(self, user_id: UUID, user_data: UserUpdate) -> User:
        """Update user."""
        # Only include non-None values
        data = {k: v for k, v in user_data.dict().items() if v is not None}
        return self.user_repo.update(user_id, data)
    
    def delete_user(self, user_id: UUID, soft_delete: bool = True) -> bool:
        """Delete user."""
        return self.user_repo.delete(user_id, soft_delete)
    
    def list_users(
        self,
        offset: int = 0,
        limit: int = 100,
        active_only: bool = True,
        verified_only: bool = False,
        role: Optional[str] = None,
    ) -> List[User]:
        """List users with filters."""
        filters = {}
        
        if active_only:
            filters['is_active'] = True
        
        if verified_only:
            filters['is_verified'] = True
        
        if role:
            filters['role'] = role
        
        return self.user_repo.list(
            offset=offset,
            limit=limit,
            filters=filters,
            order_by='created_at',
            order_desc=True
        )
    
    def search_users(self, query: str, limit: int = 100) -> List[User]:
        """Search users by name or email."""
        return self.user_repo.search_users(query, limit)
    
    def get_user_stats(self) -> Dict[str, Any]:
        """Get user statistics."""
        return self.user_repo.get_user_stats()
    
    def verify_user_email(self, user_id: UUID) -> User:
        """Mark user email as verified."""
        user = self.get_user_or_404(user_id)
        
        user.clear_email_verification_token()
        return self.user_repo.update(user_id, {
            'is_verified': True,
            'email_verified_at': user.email_verified_at,
            'email_verification_token': None,
            'email_verification_token_expires': None,
        })
    
    def change_user_password(self, user_id: UUID, new_password: str) -> User:
        """Change user password."""
        hashed_password = get_password_hash(new_password)
        
        return self.user_repo.update(user_id, {
            'password_hash': hashed_password,
            'password_changed_at': datetime.utcnow(),
            'password_reset_token': None,
            'password_reset_token_expires': None,
        })
    
    def activate_user(self, user_id: UUID) -> User:
        """Activate user account."""
        return self.user_repo.update(user_id, {
            'is_active': True,
            'deactivated_at': None,
            'deactivation_reason': None,
        })
    
    def deactivate_user(self, user_id: UUID, reason: Optional[str] = None) -> User:
        """Deactivate user account."""
        return self.user_repo.update(user_id, {
            'is_active': False,
            'deactivated_at': datetime.utcnow(),
            'deactivation_reason': reason,
        })
    
    def unlock_user(self, user_id: UUID) -> User:
        """Unlock user account."""
        return self.user_repo.update(user_id, {
            'locked_until': None,
            'failed_login_attempts': 0,
        })
    
    def record_login_attempt(self, user_id: UUID, success: bool) -> User:
        """Record login attempt."""
        user = self.get_user_or_404(user_id)

        user.record_login_attempt(success)
        return self.user_repo.update(user_id, {
            'last_login_at': user.last_login_at,
            'failed_login_attempts': user.failed_login_attempts,
            'locked_until': user.locked_until,
        })

    # Remote user validation methods (cache + API)

    @staticmethod
    async def validate_user_exists_remote(user_id: UUID) -> bool:
        """
        Validate user exists di usermanagement API.
        Cache strategy: 5 min TTL, remote call fallback.
        """
        cache_key = f"user_exists:{user_id}"

        # Try cache first
        cache = await cache_manager.get_cache()
        if cache:
            try:
                cached = await cache.get(cache_key)
                if cached is not None:
                    return cached.get("exists", False)
            except Exception:
                pass

        # Remote call
        user_exists = await UserService._fetch_user_exists(user_id)

        # Cache result
        if cache:
            try:
                await cache.set(
                    cache_key,
                    {"exists": user_exists},
                    ttl=300
                )
            except Exception:
                pass

        return user_exists

    @staticmethod
    async def get_user_data_remote(user_id: UUID) -> Optional[Dict[str, Any]]:
        """Get user data dari usermanagement API dengan cache."""
        cache_key = f"user_data:{user_id}"

        # Try cache
        cache = await cache_manager.get_cache()
        if cache:
            try:
                cached = await cache.get(cache_key)
                if cached:
                    return cached
            except Exception:
                pass

        # Remote call
        user_data = await UserService._fetch_user_data(user_id)

        # Cache if found
        if user_data and cache:
            try:
                await cache.set(cache_key, user_data, ttl=300)
            except Exception:
                pass

        return user_data

    @staticmethod
    async def _fetch_user_exists(user_id: UUID) -> bool:
        """Remote API call to check user exists."""
        try:
            url = f"{settings.security.usermanagement_api_url}/users/{user_id}"
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    if resp.status == 200:
                        return True
                    elif resp.status == 404:
                        return False
                    elif resp.status >= 500:
                        raise ServiceError(f"User API unavailable: {resp.status}")
                    return False
        except asyncio.TimeoutError:
            raise ServiceError("User API timeout")
        except ServiceError:
            raise
        except Exception as e:
            raise ServiceError(f"Failed to validate user: {str(e)}")

    @staticmethod
    async def _fetch_user_data(user_id: UUID) -> Optional[Dict[str, Any]]:
        """Remote API call to get user data."""
        try:
            url = f"{settings.security.usermanagement_api_url}/users/{user_id}"
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    if resp.status == 200:
                        body = await resp.json()
                        return body.get("data") or body
                    return None
        except Exception:
            return None
