# database/manager.py - Improved version
import asyncio
import logging
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncIterator, Iterator, Optional, Generator

from sqlmodel import create_engine, Session, SQLModel, select
from sqlalchemy.pool import QueuePool, StaticPool
from sqlalchemy.ext.asyncio import (
    create_async_engine, 
    AsyncSession, 
    async_sessionmaker,
    AsyncEngine
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from ...core.config import get_settings
from ...core.exceptions import DatabaseError

logger = logging.getLogger(__name__)

# Import all models here to ensure they're registered with SQLModel.metadata
from . import models  # Import the models module (adjust as needed)


class DatabaseManager:
    """
    Database connection manager with both sync and async support.
    Improved version with better type hints and session management.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self._engine: Optional[Engine] = None
        self._async_engine: Optional[AsyncEngine] = None
        self._async_session_maker: Optional[async_sessionmaker[AsyncSession]] = None
        self._is_connected = False
        self._lock = asyncio.Lock()
    
    def _get_database_config(self) -> dict:
        """Get database configuration based on URL."""
        config = {
            "echo": self.settings.DATABASE_ECHO,
            "pool_size": self.settings.DATABASE_POOL_SIZE,
            "max_overflow": self.settings.DATABASE_MAX_OVERFLOW,
            "pool_timeout": self.settings.DATABASE_POOL_TIMEOUT,
            "pool_recycle": self.settings.DATABASE_POOL_RECYCLE,
            "pool_pre_ping": True,
        }
        
        # SQLite specific configuration
        if "sqlite" in self.settings.DATABASE_URL:
            config["connect_args"] = {"check_same_thread": False}
            if ":memory:" in self.settings.DATABASE_URL:
                config["poolclass"] = StaticPool
            else:
                config["poolclass"] = QueuePool
        else:
            config["poolclass"] = QueuePool
            
        return config
    
    def _create_sync_engine(self) -> Engine:
        """Create synchronous database engine."""
        try:
            config = self._get_database_config()
            
            engine = create_engine(
                self.settings.DATABASE_URL,
                **config
            )
            
            logger.info(f"Sync database engine created: {self.settings.DATABASE_URL}")
            return engine
            
        except Exception as e:
            logger.error(f"Failed to create sync database engine: {e}")
            raise DatabaseError(f"Database engine creation failed: {e}")
    
    def _create_async_engine(self) -> AsyncEngine:
        """Create asynchronous database engine."""
        try:
            # Convert sync URL to async URL if needed
            async_url = self._convert_to_async_url(self.settings.DATABASE_URL)
            
            # Remove sync-specific configs for async engine
            config = self._get_database_config()
            config.pop("poolclass", None)  # async engines handle pooling differently
            
            engine = create_async_engine(
                async_url,
                **config
            )
            
            logger.info(f"Async database engine created: {async_url}")
            return engine
            
        except Exception as e:
            logger.error(f"Failed to create async database engine: {e}")
            raise DatabaseError(f"Async database engine creation failed: {e}")
    
    def _convert_to_async_url(self, url: str) -> str:
        """Convert sync database URL to async URL."""
        if url.startswith("postgresql://"):
            return url.replace("postgresql://", "postgresql+asyncpg://")
        elif url.startswith("mysql://"):
            return url.replace("mysql://", "mysql+aiomysql://")
        elif url.startswith("sqlite:///"):
            return url.replace("sqlite:///", "sqlite+aiosqlite:///")
        return url
    
    def get_engine(self) -> Engine:
        """Get or create synchronous database engine."""
        if self._engine is None:
            self._engine = self._create_sync_engine()
        return self._engine
    
    async def get_async_engine(self) -> AsyncEngine:
        """Get or create asynchronous database engine."""
        if self._async_engine is None:
            async with self._lock:
                if self._async_engine is None:  # Double-check locking
                    self._async_engine = self._create_async_engine()
                    self._async_session_maker = async_sessionmaker(
                        self._async_engine,
                        class_=AsyncSession,
                        expire_on_commit=False
                    )
        return self._async_engine
    
    @contextmanager
    def get_session(self) -> Iterator[Session]:
        """Get database session with automatic cleanup."""
        engine = self.get_engine()
        session = Session(engine)
        
        try:
            logger.debug(f"Database session created: {id(session)}")
            yield session
            session.commit()
            logger.debug(f"Database session committed: {id(session)}")
            
        except Exception as e:
            logger.error(f"Database session error: {e}")
            session.rollback()
            logger.debug(f"Database session rolled back: {id(session)}")
            raise DatabaseError(f"Database operation failed: {e}")
            
        finally:
            session.close()
            logger.debug(f"Database session closed: {id(session)}")
    
    @asynccontextmanager
    async def get_async_session(self) -> AsyncIterator[AsyncSession]:
        """Get async database session with automatic cleanup."""
        if self._async_session_maker is None:
            await self.get_async_engine()  # Initialize async engine and session maker
        
        async with self._async_session_maker() as session:
            try:
                logger.debug(f"Async database session created: {id(session)}")
                yield session
                await session.commit()
                logger.debug(f"Async database session committed: {id(session)}")
                
            except Exception as e:
                logger.error(f"Async database session error: {e}")
                await session.rollback()
                logger.debug(f"Async database session rolled back: {id(session)}")
                raise DatabaseError(f"Async database operation failed: {e}")
    
    async def connect(self) -> None:
        """Initialize database connection and create tables."""
        try:
            logger.info("Connecting to database...")
            
            # Initialize engines
            self.get_engine()
            await self.get_async_engine()
            
            # Create tables
            await self.create_tables()
            
            # Health check
            if not await self.health_check():
                raise DatabaseError("Database health check failed after connection")
            
            self._is_connected = True
            logger.info("Database connected successfully")
            
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise DatabaseError(f"Database connection failed: {e}")
    
    async def disconnect(self) -> None:
        """Close database connections."""
        try:
            logger.info("Disconnecting from database...")
            
            if self._async_engine:
                await self._async_engine.dispose()
                logger.debug("Async database engine disposed")
            
            if self._engine:
                self._engine.dispose()
                logger.debug("Sync database engine disposed")
            
            self._engine = None
            self._async_engine = None
            self._async_session_maker = None
            self._is_connected = False
            
            logger.info("Database disconnected successfully")
            
        except Exception as e:
            logger.error(f"Database disconnection failed: {e}")
            # Don't raise exception during cleanup
            
    async def create_tables(self) -> None:
        """Create all database tables."""
        logger.info("Creating database tables...")
        
        try:
            # Use async engine for table creation if available
            if self._async_engine:
                async with self._async_engine.begin() as conn:
                    await conn.run_sync(SQLModel.metadata.create_all)
            else:
                engine = self.get_engine()
                SQLModel.metadata.create_all(bind=engine)
            
            logger.info("Database tables created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create database tables: {e}")
            raise DatabaseError(f"Database table creation failed: {e}")
    
    async def drop_tables(self) -> None:
        """Drop all database tables."""
        try:
            logger.warning("Dropping all database tables...")
            
            if self._async_engine:
                async with self._async_engine.begin() as conn:
                    await conn.run_sync(SQLModel.metadata.drop_all)
            else:
                engine = self.get_engine()
                SQLModel.metadata.drop_all(bind=engine)
            
            logger.warning("All database tables dropped")
            
        except Exception as e:
            logger.error(f"Failed to drop database tables: {e}")
            raise DatabaseError(f"Database table drop failed: {e}")
    
    async def health_check(self) -> bool:
        """Check database health and connectivity."""
        try:
            async with self.get_async_session() as session:
                result = await session.execute(select(1))
                return result.scalar() == 1
                
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
    
    @property
    def is_connected(self) -> bool:
        """Check if database is connected."""
        return self._is_connected
    
    async def execute_raw_sql(self, sql: str) -> any:
        """Execute raw SQL query (for admin operations)."""
        try:
            async with self.get_async_session() as session:
                result = await session.execute(sql)
                return result.fetchall()
        except Exception as e:
            logger.error(f"Raw SQL execution failed: {e}")
            raise DatabaseError(f"Raw SQL execution failed: {e}")


# Global database manager instance
database_manager = DatabaseManager()


# Improved FastAPI dependencies
def get_session_dependency() -> Generator[Session, None, None]:
    """FastAPI dependency for getting database session."""
    with database_manager.get_session() as session:
        yield session


async def get_async_session_dependency() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency for getting async database session."""
    async with database_manager.get_async_session() as session:
        yield session


# Backward compatibility functions
def get_engine() -> Engine:
    """Get database engine (backward compatibility)."""
    return database_manager.get_engine()


@contextmanager
def get_session() -> Iterator[Session]:
    """Get database session (backward compatibility)."""
    with database_manager.get_session() as session:
        yield session


async def init_database() -> None:
    """Initialize database (async version)."""
    await database_manager.connect()


async def drop_database() -> None:
    """Drop database (async version)."""
    await database_manager.drop_tables()


async def check_database_health() -> bool:
    """Check database health (async version)."""
    return await database_manager.health_check()


async def close_database_connections() -> None:
    """Close database connections (async version)."""
    await database_manager.disconnect()


# Sync versions for backward compatibility
def init_database_sync() -> None:
    """Initialize database (sync version)."""
    asyncio.run(database_manager.connect())


def drop_database_sync() -> None:
    """Drop database (sync version)."""
    asyncio.run(database_manager.drop_tables())


def check_database_health_sync() -> bool:
    """Check database health (sync version)."""
    return asyncio.run(database_manager.health_check())


def close_database_connections_sync() -> None:
    """Close database connections (sync version)."""
    asyncio.run(database_manager.disconnect())


# FastAPI event handlers
async def on_startup():
    """Database startup handler."""
    logger.info("Initializing database connections...")
    await database_manager.connect()


async def on_shutdown():
    """Database shutdown handler."""
    logger.info("Closing database connections...")
    await database_manager.disconnect()
    