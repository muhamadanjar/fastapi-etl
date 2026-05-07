import logging
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncIterator, Dict, Generator, Iterator

from sqlalchemy import Engine
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlmodel import Session
from sqlmodel.ext.asyncio.session import AsyncSession

from app.core.config import get_settings
from app.core.exceptions import DatabaseError
from app.infrastructure.db.connection import DatabaseConnection

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Registry manager for multiple named database connections."""

    def __init__(self):
        self.settings = get_settings()
        self.connections: Dict[str, DatabaseConnection] = {}
        self.connections["default"] = DatabaseConnection("default", self.settings.database)
        self._is_connected = False

    def get_connection(self, db_name: str = "default") -> DatabaseConnection:
        if db_name not in self.connections:
            raise DatabaseError(f"Database '{db_name}' not configured.")
        return self.connections[db_name]

    def get_engine(self, db_name: str = "default") -> Engine:
        return self.get_connection(db_name).get_engine()

    async def get_async_engine(self, db_name: str = "default") -> AsyncEngine:
        return await self.get_connection(db_name).get_async_engine()

    @contextmanager
    def get_session(self, db_name: str = "default") -> Iterator[Session]:
        with self.get_connection(db_name).get_session() as session:
            yield session

    @asynccontextmanager
    async def get_async_session(self, db_name: str = "default") -> AsyncIterator[AsyncSession]:
        async with self.get_connection(db_name).get_async_session() as session:
            yield session

    async def connect(self, db_name: str = None) -> None:
        targets = [self.get_connection(db_name)] if db_name else self.connections.values()
        for conn in targets:
            try:
                await conn.connect()
                if not await conn.health_check():
                    logger.warning(f"Health check failed for '{conn.name}' after connect.")
            except Exception as e:
                logger.error(f"Failed to connect '{conn.name}': {e}")
                if conn.name == "default":
                    raise
        self._is_connected = True

    async def disconnect(self, db_name: str = None) -> None:
        targets = [self.get_connection(db_name)] if db_name else self.connections.values()
        for conn in targets:
            await conn.disconnect()
        if not db_name:
            self._is_connected = False

    async def health_check(self, db_name: str = "default") -> bool:
        return await self.get_connection(db_name).health_check()

    async def create_tables(self, db_name: str = "default") -> None:
        await self.get_connection(db_name).create_tables()

    async def create_async_session(self, db_name: str = "default") -> AsyncSession:
        return await self.get_connection(db_name).create_async_session()

    def create_session(self, db_name: str = "default") -> Session:
        return self.get_connection(db_name).create_session()

    @property
    def is_connected(self) -> bool:
        return self._is_connected


database_manager = DatabaseManager()


# ---------------------------------------------------------------------------
# Backward-compat helpers (imported by routes, tasks, handlers)
# ---------------------------------------------------------------------------

def get_engine() -> Engine:
    return database_manager.get_engine()


@contextmanager
def get_session() -> Iterator[Session]:
    with database_manager.get_session() as session:
        yield session


def get_session_dependency() -> Generator[Session, None, None]:
    with database_manager.get_session() as session:
        yield session


async def get_async_session_dependency() -> AsyncIterator[AsyncSession]:
    async with database_manager.get_async_session() as session:
        yield session


async def init_database() -> None:
    await database_manager.connect()


async def drop_database() -> None:
    engine = await database_manager.get_async_engine()
    from sqlmodel import SQLModel
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.drop_all)


async def check_database_health() -> bool:
    return await database_manager.health_check()


async def close_database_connections() -> None:
    await database_manager.disconnect()


async def on_startup() -> None:
    logger.info("Initializing database connections...")
    await database_manager.connect()


async def on_shutdown() -> None:
    logger.info("Closing database connections...")
    await database_manager.disconnect()
