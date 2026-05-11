import aiohttp
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.infrastructure.db.manager import get_session_dependency, get_async_session_dependency
from app.infrastructure.cache import cache_manager
from app.schemas.remote_user import RemoteUserInfo

security = HTTPBearer()

get_db = get_session_dependency


async def _fetch_user_info(token: str) -> RemoteUserInfo:
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
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Invalid or expired token",
                        headers={"WWW-Authenticate": "Bearer"},
                    )
                if resp.status != 200:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail="Auth service unavailable",
                    )
                body = await resp.json()
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Auth service unreachable",
        )

    user_data = body.get("data") or body
    if cache:
        await cache.set(cache_key, user_data, ttl=60)
    return RemoteUserInfo(**user_data)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> RemoteUserInfo:
    return await _fetch_user_info(credentials.credentials)


async def get_current_active_user(
    current_user: RemoteUserInfo = Depends(get_current_user),
) -> RemoteUserInfo:
    if not current_user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user",
        )
    return current_user


async def get_admin_user(
    current_user: RemoteUserInfo = Depends(get_current_active_user),
) -> RemoteUserInfo:
    if not current_user.is_superuser:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions",
        )
    return current_user
