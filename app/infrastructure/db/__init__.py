from .connection import DatabaseConnection
from .manager import (
    DatabaseManager,
    database_manager,
    get_engine,
    get_session,
    get_session_dependency,
    get_async_session_dependency,
    init_database,
    drop_database,
    check_database_health,
    close_database_connections,
    on_startup,
    on_shutdown,
)
from .dependencies import get_async_session, get_db, get_db_session
from .repositories.base import BaseRepository
from .models.base import BaseModel, TimestampMixin

__all__ = [
    # Core classes
    "DatabaseConnection",
    "DatabaseManager",
    "database_manager",
    # Engine
    "get_engine",
    # Session (sync)
    "get_session",
    "get_session_dependency",
    # Session (async)
    "get_async_session",
    "get_async_session_dependency",
    "get_db",
    "get_db_session",
    # Lifecycle
    "init_database",
    "drop_database",
    "check_database_health",
    "close_database_connections",
    "on_startup",
    "on_shutdown",
    # Repository
    "BaseRepository",
    # Models
    "BaseModel",
    "TimestampMixin",
]
