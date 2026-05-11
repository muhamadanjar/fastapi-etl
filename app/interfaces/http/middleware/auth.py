import aiohttp
from fastapi import Request, status
from fastapi.security import HTTPBearer
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from typing import Optional, List

from app.core.exceptions import AuthenticationException, AuthorizationException
from app.core.config import settings
from app.infrastructure.cache import cache_manager
from app.schemas.remote_user import RemoteUserInfo
from app.utils.logger import get_logger

logger = get_logger(__name__)


class AuthMiddleware(BaseHTTPMiddleware):
    """Validates Bearer tokens by delegating to usermanagement_api."""

    def __init__(self, app, exclude_paths: Optional[List[str]] = None):
        super().__init__(app)
        self.exclude_paths = exclude_paths or [
            "/docs",
            "/redoc",
            "/openapi.json",
            "/health",
            "/api/monitoring/health",
        ]
        self.bearer = HTTPBearer(auto_error=False)

    async def dispatch(self, request: Request, call_next):
        if self._should_skip_auth(request.url.path):
            return await call_next(request)

        try:
            token = self._extract_token(request)
            if not token:
                raise AuthenticationException("Missing authentication token")

            user = await self._validate_token(token)
            if not user:
                raise AuthenticationException("Invalid authentication token")

            request.state.current_user = user
            request.state.user_id = user.id

            logger.info(f"Auth ok: user={user.id} {request.method} {request.url.path}")

            response = await call_next(request)
            self._add_security_headers(response)
            return response

        except AuthenticationException as e:
            logger.warning(f"Auth failed: {e} — {request.url.path}")
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": str(e), "type": "authentication_error"},
            )
        except AuthorizationException as e:
            logger.warning(f"Authz failed: {e} — {request.url.path}")
            return JSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={"detail": str(e), "type": "authorization_error"},
            )
        except Exception as e:
            logger.error(f"Auth middleware error: {e}")
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"detail": "Internal authentication error"},
            )

    def _should_skip_auth(self, path: str) -> bool:
        return any(excluded in path for excluded in self.exclude_paths)

    def _extract_token(self, request: Request) -> Optional[str]:
        authorization = request.headers.get("Authorization")
        if not authorization:
            return None
        try:
            scheme, token = authorization.split()
            return token if scheme.lower() == "bearer" else None
        except ValueError:
            return None

    async def _validate_token(self, token: str) -> Optional[RemoteUserInfo]:
        cache_key = f"auth_gateway:{token[:32]}"
        cache = await cache_manager.get_cache()
        if cache:
            cached = await cache.get(cache_key)
            if cached:
                return RemoteUserInfo(**cached)

        url = f"{settings.security.usermanagement_api_url}/auth/info"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status == 401:
                        raise AuthenticationException("Invalid or expired token")
                    if resp.status != 200:
                        raise AuthenticationException("Auth service unavailable")
                    body = await resp.json()
        except AuthenticationException:
            raise
        except Exception as e:
            logger.error(f"Auth gateway error: {e}")
            raise AuthenticationException("Auth service unreachable")

        user_data = body.get("data") or body
        user = RemoteUserInfo(**user_data)

        if cache:
            await cache.set(cache_key, user_data, ttl=60)

        return user

    def _add_security_headers(self, response):
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
