import aiohttp
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy import insert, literal
from sqlalchemy.dialects.postgresql import insert as pg_insert
import uuid

from app.core.config import settings
from app.infrastructure.db.manager import get_session_dependency, database_manager
from app.infrastructure.cache import cache_manager
from app.schemas.remote_user import RemoteUserInfo
from app.infrastructure.db.models.auth import User

security = HTTPBearer()

get_db = get_session_dependency


async def _sync_user_to_db(user_info: RemoteUserInfo) -> None:
    """Sync fetched user to database via UPSERT. Runs once per unique user (non-cached fetch)."""
    try:
        async with database_manager.get_async_session() as session:
            user_id = uuid.UUID(user_info.id) if isinstance(user_info.id, str) else user_info.id

            # UPSERT: insert if not exist, skip if already there (single atomic query)
            stmt = pg_insert(User).values(
                id=user_id,
                username=user_info.username,
                email=user_info.email,
                full_name=user_info.full_name or user_info.name,
                is_active=user_info.is_active,
                is_superuser=user_info.is_superuser,
                password="",
                created_at=literal(None),
            ).on_conflict_do_nothing(index_elements=["email"])

            await session.execute(stmt)
            await session.commit()
    except Exception as e:
        # Log error but don't block auth flow - user fetch succeeded even if sync failed
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Failed to sync user {user_info.email} to database: {e}")


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
    user_info = RemoteUserInfo(**user_data)

    # Sync user to database (only on fresh API fetch, not from cache)
    await _sync_user_to_db(user_info)

    if cache:
        await cache.set(cache_key, user_data, ttl=60)
    return user_info


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
