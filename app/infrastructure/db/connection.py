import asyncio
import logging
from typing import AsyncIterator, Iterator, Optional
from contextlib import asynccontextmanager, contextmanager

from sqlalchemy import Engine
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncEngine
from sqlalchemy.pool import QueuePool, StaticPool
from sqlmodel import create_engine, Session, SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.config.database import DatabaseSettings
from app.core.exceptions import AppException, DatabaseError

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Manages a single named database connection (sync & async)."""

    def __init__(self, name: str, config: DatabaseSettings):
        self.name = name
        self.config = config
        self._engine: Optional[Engine] = None
        self._async_engine: Optional[AsyncEngine] = None
        self._async_session_maker: Optional[async_sessionmaker[AsyncSession]] = None
        self._is_connected = False
        self._lock = asyncio.Lock()

    def _get_sync_engine_config(self) -> dict:
        config_dict = self.config.engine_kwargs.copy()
        db_url = self.config.get_database_url(sync=True)

        if "sqlite" in db_url:
            config_dict["connect_args"] = {"check_same_thread": False}
            config_dict["poolclass"] = StaticPool if ":memory:" in db_url else QueuePool
        else:
            config_dict["poolclass"] = QueuePool
            config_dict["connect_args"] = {
                "connect_timeout": getattr(self.config, "connect_timeout", 10)
            }

        return config_dict

    def _get_async_engine_config(self) -> dict:
        config_dict = self.config.async_engine_kwargs.copy()
        db_url = self.config.get_database_url(sync=True)

        if "sqlite" in db_url:
            config_dict["connect_args"] = {"check_same_thread": False}
        elif "postgresql" in db_url:
            config_dict["connect_args"] = {
                "timeout": getattr(self.config, "connect_timeout", 10)
            }
        elif "mysql" in db_url:
            config_dict["connect_args"] = {
                "connect_timeout": getattr(self.config, "connect_timeout", 10)
            }

        return config_dict

    def _create_sync_engine(self) -> Engine:
        try:
            sync_url = self.config.get_database_url(sync=True)
            engine = create_engine(sync_url, **self._get_sync_engine_config())
            logger.info(f"Sync engine '{self.name}' created.")
            return engine
        except Exception as e:
            logger.error(f"Failed to create sync engine '{self.name}': {e}")
            raise DatabaseError(f"Sync engine creation failed for '{self.name}': {e}")

    def _create_async_engine(self) -> AsyncEngine:
        try:
            async_url = self.config.get_database_url(sync=False)
            engine = create_async_engine(async_url, **self._get_async_engine_config())
            logger.info(f"Async engine '{self.name}' created.")
            return engine
        except Exception as e:
            logger.error(f"Failed to create async engine '{self.name}': {e}")
            raise DatabaseError(f"Async engine creation failed for '{self.name}': {e}")

    def get_engine(self) -> Engine:
        if self._engine is None:
            self._engine = self._create_sync_engine()
        return self._engine

    async def get_async_engine(self) -> AsyncEngine:
        if self._async_engine is None:
            async with self._lock:
                if self._async_engine is None:
                    self._async_engine = self._create_async_engine()
                    self._async_session_maker = async_sessionmaker(
                        self._async_engine,
                        class_=AsyncSession,
                        expire_on_commit=False,
                    )
        return self._async_engine

    @contextmanager
    def get_session(self) -> Iterator[Session]:
        engine = self.get_engine()
        session = Session(engine)
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise DatabaseError(f"Session error '{self.name}': {e}")
        finally:
            session.close()

    @asynccontextmanager
    async def get_async_session(self) -> AsyncIterator[AsyncSession]:
        if self._async_session_maker is None:
            await self.get_async_engine()

        async with self._async_session_maker() as session:
            try:
                yield session
                await session.commit()
            except Exception as e:
                await session.rollback()
                if isinstance(e, AppException):
                    raise
                raise DatabaseError(
                    f"Database operation failed: {str(e)}",
                    details={"original_error": type(e).__name__},
                )

    async def connect(self) -> None:
        self.get_engine()
        await self.get_async_engine()
        self._is_connected = True

    async def disconnect(self) -> None:
        if self._async_engine:
            await self._async_engine.dispose()
        if self._engine:
            self._engine.dispose()
        self._engine = None
        self._async_engine = None
        self._async_session_maker = None
        self._is_connected = False

    async def health_check(self) -> bool:
        try:
            async with self.get_async_session() as session:
                result = await session.execute(select(1))
                return result.scalar() == 1
        except Exception:
            return False

    async def create_tables(self) -> None:
        engine = await self.get_async_engine()
        async with engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)

    async def create_async_session(self) -> AsyncSession:
        """
        Create async session without context manager.
        WARNING: You must call session.close() manually!
        """
        if self._async_session_maker is None:
            await self.get_async_engine()
        return self._async_session_maker()

    def create_session(self) -> Session:
        """
        Create sync session without context manager.
        WARNING: You must call session.close() manually!
        """
        return Session(self.get_engine())
