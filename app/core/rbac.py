"""
Dynamic RBAC helpers for fastapi-etl.

The system is role-based and fully dynamic: roles (and their membership) are
managed in fastapi_usermanagement. Therefore this module NEVER hardcodes role
names in endpoint code — the set of privileged roles is read from configuration
(``SecuritySettings.admin_roles`` / env ``ADMIN_ROLES``).

The /auth/info endpoint returns roles as objects (``{"id": ..., "name": ...}``)
and privileges as menu objects, so the helpers below normalise both dicts,
Pydantic models, and plain strings into a set of role *names*.
"""

from typing import Any, List, Set

from app.core.config import get_settings

settings = get_settings()


def get_admin_roles() -> Set[str]:
    """Return the configured set of privileged role names (lowercased).

    Configurable via env ``ADMIN_ROLES`` (comma-separated). Defaults to
    ``admin,superuser``.
    """
    raw = getattr(settings.security, "admin_roles", "admin,superuser") or ""
    return {r.strip().lower() for r in raw.split(",") if r.strip()}


def extract_role_names(roles: Any) -> Set[str]:
    """Normalise a roles payload (from /auth/info) into a set of lowercase names.

    Handles all shapes the usermanagement service may return:
      - ``[{"id": ..., "name": "admin"}, ...]``  (dict)
      - ``[RoleModel(id=..., name="admin"), ...]`` (object with ``.name``)
      - ``["admin", "manager"]``  (plain strings)
      - ``None`` / empty
    """
    names: Set[str] = set()
    if not roles:
        return names

    items: List[Any] = roles if isinstance(roles, (list, tuple, set)) else [roles]
    for item in items:
        name = None
        if isinstance(item, str):
            name = item
        elif isinstance(item, dict):
            name = item.get("name") or item.get("slug")
        else:
            # Pydantic model / arbitrary object
            name = getattr(item, "name", None) or getattr(item, "slug", None)
        if name:
            names.add(str(name).strip().lower())
    return names


def user_has_admin_role(roles: Any) -> bool:
    """True if the user holds at least one configured admin role."""
    admin_roles = get_admin_roles()
    if not admin_roles:
        return False
    return bool(extract_role_names(roles).intersection(admin_roles))
