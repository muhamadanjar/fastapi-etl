"""
Authentication service for user management and authentication.
"""

from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import select
from passlib.context import CryptContext
from jose import JWTError, jwt

from app.application.services.base import BaseService
from app.infrastructure.db.models.auth import User
from app.schemas.auth import UserCreate, Token, PasswordChange
from app.core.exceptions import ServiceError, UnauthorizedError
from app.core.constants import JWT_SECRET_KEY, JWT_ALGORITHM, ACCESS_TOKEN_EXPIRE
from app.utils.security import create_access_token, create_refresh_token


class AuthService(BaseService):
    """Service for handling authentication operations."""
    
    def __init__(self, db_session: Session):
        super().__init__(db_session)
        self.pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    
    def get_service_name(self) -> str:
        return "AuthService"
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify password against hash."""
        return self.pwd_context.verify(plain_password, hashed_password)
    
    def get_password_hash(self, password: str) -> str:
        """Hash a password."""
        return self.pwd_context.hash(password)
    
    async def create_user(self, user_data: UserCreate) -> User:
        """Create a new user."""
        try:
            self.log_operation("create_user", {"username": user_data.username, "email": user_data.email})
            
            # Check if user already exists by username
            existing_user = await self._get_user_by_username(user_data.username)
            if existing_user:
                raise ServiceError("Username already exists")
            
            # Check if user already exists by email
            existing_email = await self._get_user_by_email(user_data.email)
            if existing_email:
                raise ServiceError("Email already exists")
            
            # Hash password
            hashed_password = self.get_password_hash(user_data.password)
            
            # Create user
            user = User(
                username=user_data.username,
                email=user_data.email,
                full_name=user_data.full_name,
                password=hashed_password,
                is_active=True,
                is_superuser=False,
                created_at=datetime.utcnow()
            )
            
            self.db.add(user)
            self.db.commit()
            self.db.refresh(user)
            
            return user
            
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "create_user")
    
    async def authenticate_user(self, username: str, password: str) -> Optional[Token]:
        """Authenticate user and return tokens."""
        try:
            self.log_operation("authenticate_user", {"username": username})
            
            # Get user from database
            user = await self._get_user_by_username(username)
            if not user:
                return None
            
            # Check if user is active
            if not user.is_active:
                raise ServiceError("User account is deactivated")
            
            # Verify password
            if not self.verify_password(password, user.hashed_password):
                return None
            
            # Update last login
            user.last_login = datetime.utcnow()
            self.db.commit()
            
            # Create tokens
            access_token = create_access_token(data={"sub": user.username, "user_id": user.id})
            refresh_token = create_refresh_token(data={"sub": user.username, "user_id": user.id})
            
            return Token(
                access_token=access_token,
                refresh_token=refresh_token,
                token_type="bearer"
            )
            
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "authenticate_user")
    
    async def refresh_token(self, current_user: User) -> Token:
        """Generate new access token for authenticated user."""
        try:
            self.log_operation("refresh_token", {"user_id": current_user.id})
            
            if not current_user.is_active:
                raise UnauthorizedError("User account is deactivated")
            
            # Create new tokens
            access_token = create_access_token(data={"sub": current_user.username, "user_id": current_user.id})
            refresh_token = create_refresh_token(data={"sub": current_user.username, "user_id": current_user.id})
            
            return Token(
                access_token=access_token,
                refresh_token=refresh_token,
                token_type="bearer"
            )
            
        except Exception as e:
            self.handle_error(e, "refresh_token")
    
    async def change_password(self, current_user: User, password_data: PasswordChange) -> bool:
        """Change user password."""
        try:
            self.log_operation("change_password", {"user_id": current_user.id})
            
            # Verify current password
            if not self.verify_password(password_data.current_password, current_user.hashed_password):
                raise ServiceError("Current password is incorrect")
            
            # Validate new password is different
            if password_data.current_password == password_data.new_password:
                raise ServiceError("New password must be different from current password")
            
            # Hash new password
            new_hashed_password = self.get_password_hash(password_data.new_password)
            
            # Update password
            current_user.hashed_password = new_hashed_password
            current_user.updated_at = datetime.utcnow()
            
            self.db.commit()
            
            return True
            
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "change_password")
    
    async def logout_user(self, current_user: User) -> bool:
        """Logout user (can be used for token blacklisting in the future)."""
        try:
            self.log_operation("logout_user", {"user_id": current_user.id})
            
            # Update last logout time
            current_user.last_logout = datetime.utcnow()
            current_user.updated_at = datetime.utcnow()
            
            self.db.commit()
            
            # Note: In a production environment, you might want to:
            # 1. Add the token to a blacklist
            # 2. Store logout timestamp for security auditing
            # 3. Invalidate refresh tokens
            
            return True
            
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "logout_user")
    
    async def get_user_by_id(self, user_id: int) -> Optional[User]:
        """Get user by ID."""
        try:
            self.log_operation("get_user_by_id", {"user_id": user_id})
            return self.db.get(User, user_id)
        except Exception as e:
            self.handle_error(e, "get_user_by_id")
    
    async def get_user_by_username(self, username: str) -> Optional[User]:
        """Get user by username."""
        try:
            return await self._get_user_by_username(username)
        except Exception as e:
            self.handle_error(e, "get_user_by_username")
    
    async def verify_token(self, token: str) -> Dict[str, Any]:
        """Verify and decode JWT token."""
        try:
            payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
            username = payload.get("sub")
            user_id = payload.get("user_id")
            
            if username is None or user_id is None:
                raise UnauthorizedError("Invalid token")
            
            # Verify user still exists and is active
            user = await self._get_user_by_username(username)
            if not user or not user.is_active:
                raise UnauthorizedError("User not found or inactive")
            
            return {
                "username": username,
                "user_id": user_id,
                "user": user
            }
            
        except JWTError as e:
            raise UnauthorizedError("Invalid token") from e
    
    async def reset_password_request(self, email: str) -> str:
        """Generate password reset token for user."""
        try:
            self.log_operation("reset_password_request", {"email": email})
            
            user = await self._get_user_by_email(email)
            if not user:
                # Don't reveal if email exists for security
                return "If the email exists, a reset link has been sent"
            
            # Generate reset token (expires in 30 minutes)
            reset_data = {
                "sub": user.username,
                "user_id": user.id,
                "type": "password_reset",
                "exp": datetime.utcnow() + timedelta(minutes=30)
            }
            
            reset_token = jwt.encode(reset_data, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
            
            # In production, send email with reset link
            # For now, return the token (remove this in production)
            return f"Password reset token: {reset_token}"
            
        except Exception as e:
            self.handle_error(e, "reset_password_request")
    
    async def reset_password_confirm(self, token: str, new_password: str) -> bool:
        """Reset password using reset token."""
        try:
            self.log_operation("reset_password_confirm", {})
            
            # Verify reset token
            try:
                payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
                if payload.get("type") != "password_reset":
                    raise UnauthorizedError("Invalid reset token")
                
                user_id = payload.get("user_id")
                if not user_id:
                    raise UnauthorizedError("Invalid reset token")
                
            except JWTError:
                raise UnauthorizedError("Invalid or expired reset token")
            
            # Get user
            user = self.db.get(User, user_id)
            if not user or not user.is_active:
                raise ServiceError("User not found or inactive")
            
            # Update password
            user.password = self.get_password_hash(new_password)
            user.updated_at = datetime.utcnow()
            
            self.db.commit()
            
            return True
            
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "reset_password_confirm")
    
    async def deactivate_user(self, user_id: int) -> bool:
        """Deactivate user account."""
        try:
            self.log_operation("deactivate_user", {"user_id": user_id})
            
            user = self.db.get(User, user_id)
            if not user:
                raise ServiceError("User not found")
            
            user.is_active = False
            user.updated_at = datetime.utcnow()
            
            self.db.commit()
            
            return True
            
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "deactivate_user")
    
    async def activate_user(self, user_id: int) -> bool:
        """Activate user account."""
        try:
            self.log_operation("activate_user", {"user_id": user_id})
            
            user = self.db.get(User, user_id)
            if not user:
                raise ServiceError("User not found")
            
            user.is_active = True
            user.updated_at = datetime.utcnow()
            
            self.db.commit()
            
            return True
            
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "activate_user")
    
    # Private helper methods
    async def _get_user_by_username(self, username: str) -> Optional[User]:
        """Get user by username from database."""
        stmt = select(User).where(User.username == username)
        result = self.db.execute(stmt)
        return result.scalar_one_or_none()
    
    async def _get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email from database."""
        stmt = select(User).where(User.email == email)
        result = self.db.execute(stmt)
        return result.scalar_one_or_none()