"""
Rate limiting middleware menggunakan Redis dengan memory fallback.
Strategi: per-user limits + endpoint-specific rules.
"""

import time
from typing import Callable, Optional
from fastapi import Request, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from app.infrastructure.cache import cache_manager
from app.schemas.remote_user import RemoteUserInfo
import logging

logger = logging.getLogger(__name__)


class RateLimitConfig:
    """Rate limit configuration per endpoint pattern."""

    def __init__(self):
        # Default limits (requests per minute)
        self.default_limit = 100  # 100 req/min globally
        self.default_window = 60  # 1 minute

        # Endpoint-specific limits
        self.limits = {
            "/upload": {"limit": 20, "window": 60},  # 20 uploads/min
            "/batch-upload": {"limit": 5, "window": 60},  # 5 batch uploads/min
            "/process": {"limit": 30, "window": 60},  # 30 process/min
            "/download": {"limit": 50, "window": 60},  # 50 downloads/min
            "default": {"limit": self.default_limit, "window": self.default_window},
        }

    def get_limit(self, path: str) -> tuple:
        """Get limit and window for path."""
        for pattern, config in self.limits.items():
            if pattern != "default" and pattern in path:
                return config["limit"], config["window"]
        return self.limits["default"]["limit"], self.limits["default"]["window"]


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Rate limit middleware dengan Redis + fallback."""

    def __init__(self, app, config: Optional[RateLimitConfig] = None):
        super().__init__(app)
        self.config = config or RateLimitConfig()

    async def dispatch(self, request: Request, call_next: Callable):
        # Skip rate limit untuk health checks dan metrics
        if request.url.path in ["/health", "/metrics", "/docs", "/openapi.json"]:
            return await call_next(request)

        # Get rate limit key (user_id atau IP)
        rate_limit_key = await self._get_rate_limit_key(request)
        if not rate_limit_key:
            # No user info dan no IP - skip (shouldn't happen)
            return await call_next(request)

        # Get limit config
        limit, window = self.config.get_limit(request.url.path)

        # Check rate limit
        is_limited, remaining = await self._check_rate_limit(
            rate_limit_key, limit, window
        )

        if is_limited:
            logger.warning(
                f"Rate limit exceeded for {rate_limit_key} on {request.url.path}"
            )
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "detail": "Rate limit exceeded. Try again later.",
                    "retry_after": window,
                },
                headers={"Retry-After": str(window)},
            )

        # Add rate limit headers
        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(limit)
        response.headers["X-RateLimit-Remaining"] = str(max(0, remaining - 1))
        response.headers["X-RateLimit-Reset"] = str(
            int(time.time()) + window
        )

        return response

    async def _get_rate_limit_key(self, request: Request) -> Optional[str]:
        """Get rate limit key dari user atau IP."""
        # Try get user dari request state (added by auth middleware)
        if hasattr(request.state, "user") and request.state.user:
            user: RemoteUserInfo = request.state.user
            return f"rl:user:{user.id}"

        # Fallback ke IP-based rate limiting
        client_ip = request.client.host if request.client else "unknown"
        return f"rl:ip:{client_ip}"

    async def _check_rate_limit(
        self, key: str, limit: int, window: int
    ) -> tuple:
        """
        Check rate limit dan update counter.
        Return (is_limited, remaining).
        """
        cache = await cache_manager.get_cache()
        if not cache:
            logger.warning("Cache unavailable - skipping rate limit check")
            return False, limit  # Don't limit if cache down

        try:
            current = await cache.get(key)
            if current is None:
                # First request in window
                await cache.set(key, 1, ttl=window)
                return False, limit - 1

            count = current.get("count", 0) if isinstance(current, dict) else current
            if count >= limit:
                return True, 0

            # Increment
            await cache.set(key, {"count": count + 1}, ttl=window)
            return False, limit - count - 1

        except Exception as e:
            logger.error(f"Rate limit check error: {str(e)}")
            return False, limit  # Don't fail closed
