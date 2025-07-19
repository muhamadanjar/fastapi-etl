"""
Authentication service for user management and authentication.
"""

from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from passlib.context import CryptContext
from jose import JWTError, jwt
from app.services.base import BaseService
from app.core.exceptions import AuthenticationError, ServiceError
from app.core.constants import JWT_SECRET_KEY, JWT_ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES
from app.utils.security import verify_password, create_access_token


class AuthService(BaseService):
    """Service for handling authentication operations."""
    
    def __init__(self, db_session: Session):
        super().__init__(db_session)
        self.pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    
    def get_service_name(self) -> str:
        return "AuthService"
    
    async def authenticate_user(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """Authenticate user with username and password."""
        try:
            self.log_operation("authenticate_user", {"username": username})
            
            # Get user from database (implement based on your user model)
            user = await self._get_user_by_username(username)
            if not user:
                return None
            
            # Verify password
            if not verify_password(password, user.hashed_password):
                return None
            
            return {
                "user_id": user.id,
                "username": user.username,
                "email": user.email,
                "roles": user.roles,
                "is_active": user.is_active
            }
            
        except Exception as e:
            self.handle_error(e, "authenticate_user")
    
    async def create_user(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new user."""
        try:
            self.validate_input(user_data, ["username", "email", "password"])
            self.log_operation("create_user", {"username": user_data["username"]})
            
            # Check if user already exists
            existing_user = await self._get_user_by_username(user_data["username"])
            if existing_user:
                raise ServiceError("Username already exists")
            
            # Hash password
            hashed_password = self.pwd_context.hash(user_data["password"])
            
            # Create user (implement based on your user model)
            user = await self._create_user_record({
                **user_data,
                "hashed_password": hashed_password,
                "created_at": datetime.utcnow()
            })
            
            return {
                "user_id": user.id,
                "username": user.username,
                "email": user.email,
                "created_at": user.created_at
            }
            
        except Exception as e:
            self.handle_error(e, "create_user")
    
    async def generate_access_token(self, user_data: Dict[str, Any]) -> str:
        """Generate access token for user."""
        try:
            self.log_operation("generate_access_token", {"user_id": user_data["user_id"]})
            
            token_data = {
                "sub": user_data["username"],
                "user_id": user_data["user_id"],
                "roles": user_data.get("roles", []),
                "exp": datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
            }
            
            return create_access_token(token_data)
            
        except Exception as e:
            self.handle_error(e, "generate_access_token")
    
    async def verify_token(self, token: str) -> Dict[str, Any]:
        """Verify and decode JWT token."""
        try:
            payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
            username = payload.get("sub")
            user_id = payload.get("user_id")
            
            if username is None or user_id is None:
                raise AuthenticationError("Invalid token")
            
            return {
                "username": username,
                "user_id": user_id,
                "roles": payload.get("roles", [])
            }
            
        except JWTError as e:
            raise AuthenticationError("Invalid token") from e
    
    async def refresh_token(self, refresh_token: str) -> str:
        """Refresh access token using refresh token."""
        try:
            # Verify refresh token
            payload = jwt.decode(refresh_token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
            username = payload.get("sub")
            
            if username is None:
                raise AuthenticationError("Invalid refresh token")
            
            # Get user data
            user = await self._get_user_by_username(username)
            if not user or not user.is_active:
                raise AuthenticationError("User not found or inactive")
            
            # Generate new access token
            user_data = {
                "user_id": user.id,
                "username": user.username,
                "roles": user.roles
            }
            
            return await self.generate_access_token(user_data)
            
        except JWTError as e:
            raise AuthenticationError("Invalid refresh token") from e
    
    async def change_password(self, user_id: int, old_password: str, new_password: str) -> bool:
        """Change user password."""
        try:
            self.log_operation("change_password", {"user_id": user_id})
            
            # Get user
            user = await self._get_user_by_id(user_id)
            if not user:
                raise ServiceError("User not found")
            
            # Verify old password
            if not verify_password(old_password, user.hashed_password):
                raise ServiceError("Invalid old password")
            
            # Hash new password
            new_hashed_password = self.pwd_context.hash(new_password)
            
            # Update password
            await self._update_user_password(user_id, new_hashed_password)
            
            return True
            
        except Exception as e:
            self.handle_error(e, "change_password")
    
    async def reset_password(self, email: str) -> str:
        """Reset user password and return temporary password."""
        try:
            self.log_operation("reset_password", {"email": email})
            
            # Get user by email
            user = await self._get_user_by_email(email)
            if not user:
                raise ServiceError("User not found")
            
            # Generate temporary password
            temp_password = self._generate_temporary_password()
            hashed_temp_password = self.pwd_context.hash(temp_password)
            
            # Update password
            await self._update_user_password(user.id, hashed_temp_password)
            
            return temp_password
            
        except Exception as e:
            self.handle_error(e, "reset_password")
    
    # Private helper methods (implement based on your user model)
    async def _get_user_by_username(self, username: str):
        """Get user by username from database."""
        # Implement database query
        pass
    
    async def _get_user_by_id(self, user_id: int):
        """Get user by ID from database."""
        # Implement database query
        pass
    
    async def _get_user_by_email(self, email: str):
        """Get user by email from database."""
        # Implement database query
        pass
    
    async def _create_user_record(self, user_data: Dict[str, Any]):
        """Create user record in database."""
        # Implement database insert
        pass
    
    async def _update_user_password(self, user_id: int, hashed_password: str):
        """Update user password in database."""
        # Implement database update
        pass
    
    def _generate_temporary_password(self) -> str:
        """Generate temporary password."""
        import secrets
        import string
        alphabet = string.ascii_letters + string.digits
        return ''.join(secrets.choice(alphabet) for i in range(12))