"""
Thin backward-compatibility adapter.

All database logic has been migrated to:
  app.infrastructure.db.connection  (DatabaseManager, engines, sessions)
  app.config.database               (DatabaseSettings)

This module re-exports the symbols that legacy code may import from
app.core.database so that no import sites need to be updated.
"""
from typing import Generator, AsyncGenerator

from sqlalchemy.orm import declarative_base
from sqlmodel import SQLModel

from app.infrastructure.db.manager import (
    database_manager,
    get_engine,
    get_session as _get_session_ctx,
)

# Legacy Base — kept for any sync SQLAlchemy code that imported it directly.
# New models should use SQLModel directly.
Base = declarative_base()


def init_db():
    """Create all tables (sync, via sync engine). Legacy entry point."""
    SQLModel.metadata.create_all(get_engine())


def get_session() -> Generator:
    """Sync session dependency (backward compat). Delegates to database_manager."""
    with database_manager.get_session() as session:
        yield session


async def get_async_session() -> AsyncGenerator:
    """Async session dependency (backward compat). Delegates to database_manager."""
    async with database_manager.get_async_session() as session:
        yield session


# Alias kept for code that does `from app.core.database import get_db`
get_db = get_session
