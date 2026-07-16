import aiohttp
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy import insert, literal
from sqlalchemy.dialects.postgresql import insert as pg_insert
import uuid

from app.core.rbac import user_has_admin_role, extract_role_names
from app.infrastructure.db.manager import get_session_dependency, database_manager
from app.infrastructure.cache import cache_manager
from app.schemas.remote_user import RemoteUserInfo
from app.infrastructure.db.models.auth import User
from app.core.config import get_settings



security = HTTPBearer()

get_db = get_session_dependency
settings = get_settings()


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
    import logging
    logger = logging.getLogger(__name__)

    cache_key = f"auth_gateway:{token[:32]}"
    cache = await cache_manager.get_cache()
    print("CACHE DATA", cache)
    if cache:
        cached = await cache.get(cache_key)
        if cached:
            return RemoteUserInfo(**cached)

    url = f"{settings.security.usermanagement_api_url}/auth/info"

    # Narrow the try/except to network I/O only — do not let post-response
    # logic (NameError, ValidationError, etc.) be swallowed as "unreachable".
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
                    logger.error(
                        "Auth Service returned unexpected status %s for %s",
                        resp.status,
                        url,
                    )
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail="Auth Service unavailable",
                    )
                body = await resp.json()
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Auth Service unreachable at %s: %s", url, exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Auth service unreachable",
        )

    # Post-response processing is intentionally outside the try/except so that
    # programming errors (bad response shape, ValidationError) surface as 500s,
    # not as misleading 503s.
    user_data = body.get("data") or body
    user_info = RemoteUserInfo(**user_data)

    # Sync user to database (only on fresh API fetch, not from cache)
    await _sync_user_to_db(user_info)

    if cache:
        await cache.set(cache_key, user_data, ttl=30)
    return user_info


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> RemoteUserInfo:
    user = await _fetch_user_info(credentials.credentials)
    # Enforce active status globally (covers all 144 endpoints that depend on
    # get_current_user). Inactive users from the usermanagement service must
    # not be able to use the API even with a still-valid token.
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Inactive user",
        )
    return user


async def get_current_active_user(
    current_user: RemoteUserInfo = Depends(get_current_user),
) -> RemoteUserInfo:
    # is_active is already enforced in get_current_user; this wrapper exists
    # for semantic clarity / future per-route use.
    return current_user


async def get_admin_user(
    current_user: RemoteUserInfo = Depends(get_current_user),
) -> RemoteUserInfo:
    if not current_user.is_superuser:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions",
        )
    return current_user


def require_roles(*required_roles: str):
    """Dependency factory: require the user to have at least one of the
    given roles. Superusers always pass.

    NOTE: because the system is fully dynamic (roles managed in
    fastapi_usermanagement), prefer calling ``require_roles()`` *without*
    arguments — it then enforces the configured admin role set
    (``SecuritySettings.admin_roles`` / env ``ADMIN_ROLES``) instead of
    hardcoding role names in endpoint code.
    """

    async def _checker(
        current_user: RemoteUserInfo = Depends(get_current_user),
    ) -> RemoteUserInfo:
        if current_user.is_superuser:
            return current_user
        # Dynamic mode: no args supplied -> use the configurable admin role set.
        if not required_roles:
            if user_has_admin_role(current_user.roles):
                return current_user
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Requires an administrative role",
            )
        # Explicit (static) mode: match against supplied role names.
        user_roles = extract_role_names(current_user.roles)
        if not user_roles.intersection({r.lower() for r in required_roles}):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Requires one of roles: {', '.join(required_roles)}",
            )
        return current_user

    return _checker


def require_privileges(*required_privileges: str):
    """Dependency factory: require the user to have all of the given
    privileges. Superusers always pass.

    The usermanagement ``/auth/info`` endpoint currently returns *menus* under
    ``privileges`` (not permission tokens). Until that service emits explicit
    permission strings, call this *without* arguments to deny non-superusers,
    or supply concrete permission tokens once available.
    """

    async def _checker(
        current_user: RemoteUserInfo = Depends(get_current_user),
    ) -> RemoteUserInfo:
        if current_user.is_superuser:
            return current_user
        if not required_privileges:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Requires explicit privileges",
            )
        user_privs = extract_role_names(current_user.privileges)
        if not set(required_privileges).issubset(user_privs):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Missing required privileges: {', '.join(required_privileges)}",
            )
        return current_user

    return _checker
