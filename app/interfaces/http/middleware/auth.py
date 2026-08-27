import aiohttp
from fastapi import Request, status
from fastapi.security import HTTPBearer
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from typing import Optional, List

from app.core.exceptions import AuthenticationException, AuthorizationException
from app.core.config import settings
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

            user = await self._authorize(token, self._required_permission(request))
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
                content={"error": {"code": "AUTHENTICATION_ERROR", "message": str(e), "details": {}}},
            )
        except AuthorizationException as e:
            logger.warning(f"Authz failed: {e} — {request.url.path}")
            return JSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={"error": {"code": "AUTHORIZATION_ERROR", "message": str(e), "details": {}}},
            )
        except RuntimeError as e:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"error": {"code": "AUTHORIZATION_UNAVAILABLE", "message": str(e), "details": {}}},
            )
        except Exception as e:
            logger.error(f"Auth middleware error: {e}")
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"error": {"code": "AUTHENTICATION_INTERNAL_ERROR", "message": "Internal authentication error", "details": {}}},
            )

    def _should_skip_auth(self, path: str) -> bool:
        normalized = path.rstrip("/") or "/"
        return any(
            normalized == excluded.rstrip("/")
            or normalized.startswith(f"{excluded.rstrip('/')}/")
            for excluded in self.exclude_paths
        )

    def _extract_token(self, request: Request) -> Optional[str]:
        authorization = request.headers.get("Authorization")
        if not authorization:
            return None
        try:
            scheme, token = authorization.split()
            return token if scheme.lower() == "bearer" else None
        except ValueError:
            return None

    @staticmethod
    def _required_permission(request: Request) -> str:
        path = request.url.path.rstrip("/")
        method = request.method

        if path.startswith("/api/v1/sources"):
            if "/ingestions/" in path:
                return "etl.sources.ingest"
            return "etl.sources.read" if method in {"GET", "HEAD"} else "etl.sources.write"

        if path.startswith("/api/v1/datasets"):
            if path.endswith("/download"):
                return "etl.datasets.download"
            if method == "DELETE":
                return "etl.datasets.delete"
            return "etl.datasets.read"

        if path.startswith("/api/v1/recipes"):
            if path.endswith("/runs"):
                return "etl.recipes.run"
            return "etl.recipes.read" if method in {"GET", "HEAD"} else "etl.recipes.write"

        if path.startswith(("/api/v1/runs", "/api/v1/overview")):
            return "etl.runs.read"

        raise AuthorizationException("No permission mapping exists for this endpoint")

    async def _authorize(self, token: str, permission: str) -> RemoteUserInfo:
        url = f"{settings.security.usermanagement_api_url}/auth/authorize"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    headers={"Authorization": f"Bearer {token}"},
                    json={"permission": permission},
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status == 401:
                        raise AuthenticationException("Invalid or expired token")
                    if resp.status == 403:
                        raise AuthorizationException("Permission denied")
                    if resp.status != 200:
                        raise RuntimeError("Authorization service unavailable")
                    body = await resp.json()
        except AuthenticationException:
            raise
        except AuthorizationException:
            raise
        except Exception as e:
            logger.error(f"Auth gateway error: {e}")
            raise RuntimeError("Authorization service unreachable")

        decision = body.get("data") or body
        if not decision.get("allowed"):
            raise AuthorizationException("Permission denied")
        principal = decision.get("principal") or {}
        return RemoteUserInfo(
            id=str(principal["id"]),
            username=principal["username"],
            email=principal["email"],
            name=principal["name"],
            is_active=True,
            is_superuser=principal.get("is_superuser", False),
        )

    def _add_security_headers(self, response):
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
