import jwt
from fastapi import Request, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from sqlmodel import Session, select
from typing import Optional, List
import time
from datetime import datetime, timedelta

from app.core.exceptions import AuthenticationException, AuthorizationException
from app.infrastructure.db.models import User
from app.interfaces.dependencies import get_db
from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

class AuthMiddleware(BaseHTTPMiddleware):
    """
    Authentication middleware for validating JWT tokens and user permissions
    """
    
    def __init__(self, app, exclude_paths: Optional[List[str]] = None):
        super().__init__(app)
        self.exclude_paths = exclude_paths or [
            "/docs",
            "/redoc", 
            "/openapi.json",
            "/health",
            "/api/auth/login",
            "/api/auth/register",
            "/api/monitoring/health"
        ]
        self.bearer = HTTPBearer(auto_error=False)
    
    async def dispatch(self, request: Request, call_next):
        # Skip authentication for excluded paths
        if self._should_skip_auth(request.url.path):
            return await call_next(request)
        
        try:
            # Extract and validate token
            token = await self._extract_token(request)
            if not token:
                raise AuthenticationException("Missing authentication token")
            
            # Validate token and get user
            user = await self._validate_token(token, request)
            if not user:
                raise AuthenticationException("Invalid authentication token")
            
            # Add user to request state
            request.state.current_user = user
            request.state.user_id = user.id
            
            # Check user permissions for the endpoint
            await self._check_permissions(request, user)
            
            # Log authentication success
            logger.info(
                f"Authentication successful for user {user.id} on {request.method} {request.url.path}"
            )
            
            response = await call_next(request)
            
            # Add security headers
            self._add_security_headers(response)
            
            return response
            
        except AuthenticationException as e:
            logger.warning(f"Authentication failed: {str(e)} for {request.url.path}")
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": str(e), "type": "authentication_error"}
            )
        except AuthorizationException as e:
            logger.warning(f"Authorization failed: {str(e)} for user on {request.url.path}")
            return JSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={"detail": str(e), "type": "authorization_error"}
            )
        except Exception as e:
            logger.error(f"Auth middleware error: {str(e)}")
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"detail": "Internal authentication error"}
            )
    
    def _should_skip_auth(self, path: str) -> bool:
        """Check if path should skip authentication"""
        return any(excluded in path for excluded in self.exclude_paths)
    
    async def _extract_token(self, request: Request) -> Optional[str]:
        """Extract JWT token from request headers"""
        authorization = request.headers.get("Authorization")
        if not authorization:
            return None
        
        try:
            scheme, token = authorization.split()
            if scheme.lower() != "bearer":
                return None
            return token
        except ValueError:
            return None
    
    async def _validate_token(self, token: str, request: Request) -> Optional[User]:
        """Validate JWT token and return user"""
        try:
            # Decode JWT token
            payload = jwt.decode(
                token, 
                settings.security.secret_key, 
                algorithms=[settings.security.algorithm]
            )
            
            # Extract user information
            user_id: str = payload.get("sub")
            if user_id is None:
                raise AuthenticationException("Invalid token payload")
            
            # Check token expiration
            exp = payload.get("exp")
            if exp and datetime.utcfromtimestamp(exp) < datetime.utcnow():
                raise AuthenticationException("Token has expired")
            
            # Get user from database
            db = next(get_db())
            try:
                statement = select(User).where(User.id == user_id, User.is_active == True)
                user = db.exec(statement).first()
                
                if not user:
                    raise AuthenticationException("User not found or inactive")
                
                # Update last activity
                user.last_activity = datetime.utcnow()
                db.add(user)
                db.commit()
                
                return user
            finally:
                db.close()
                
        except jwt.ExpiredSignatureError:
            raise AuthenticationException("Token has expired")
        except jwt.InvalidTokenError:
            raise AuthenticationException("Invalid token")
        except Exception as e:
            logger.error(f"Token validation error: {str(e)}")
            raise AuthenticationException("Token validation failed")
    
    async def _check_permissions(self, request: Request, user: User):
        """Check user permissions for the requested endpoint"""
        path = request.url.path
        method = request.method
        
        # Admin users have access to everything
        if user.is_superuser:
            return
        
        # Define role-based permissions
        role_permissions = {
            "admin": ["*"],  # All permissions
            "analyst": [
                "GET:/api/files/*",
                "GET:/api/jobs/*", 
                "GET:/api/entities/*",
                "GET:/api/reports/*",
                "GET:/api/monitoring/*",
                "GET:/api/data-quality/*"
            ],
            "operator": [
                "GET:/api/files/*",
                "POST:/api/files/upload",
                "POST:/api/jobs/*/execute",
                "GET:/api/jobs/*",
                "GET:/api/monitoring/*"
            ],
            "viewer": [
                "GET:/api/reports/*",
                "GET:/api/monitoring/dashboard",
                "GET:/api/entities/*"
            ]
        }
        
        user_permissions = role_permissions.get(user.role, [])
        
        # Check if user has permission
        permission_key = f"{method}:{path}"
        
        # Check exact match or wildcard permissions
        has_permission = any(
            perm == "*" or 
            perm == permission_key or
            (perm.endswith("/*") and path.startswith(perm[:-1]))
            for perm in user_permissions
        )
        
        if not has_permission:
            raise AuthorizationException(
                f"Insufficient permissions for {method} {path}"
            )
    
    def _add_security_headers(self, response):
        """Add security headers to response"""
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
