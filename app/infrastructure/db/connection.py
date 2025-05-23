"""
SQLModel database connection and session management.

This module provides simplified database operations using SQLModel
for better type safety and easier maintenance.
"""

import logging
from contextlib import contextmanager
from typing import Iterator, Optional, Generator

from sqlmodel import create_engine, Session, SQLModel
from sqlalchemy.pool import QueuePool

from ...core.config import get_settings
from ...core.exceptions import DatabaseError

logger = logging.getLogger(__name__)
settings = get_settings()

# Global engine instance
_engine: Optional[object] = None


def create_database_engine():
    """
    Create and configure SQLModel engine.
    
    Returns:
        SQLModel engine instance
    """
    try:
        engine = create_engine(
            settings.database.url,
            echo=settings.database.echo,
            poolclass=QueuePool,
            pool_size=settings.database.pool_size,
            max_overflow=settings.database.max_overflow,
            pool_timeout=settings.database.pool_timeout,
            pool_recycle=settings.database.pool_recycle,
            pool_pre_ping=True,
            connect_args={
                "check_same_thread": False,  # For SQLite
            } if "sqlite" in settings.database.url else {},
        )
        
        logger.info(f"Database engine created: {settings.database.url}")
        return engine
        
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        raise DatabaseError(f"Database engine creation failed: {e}")


def get_engine():
    """
    Get or create database engine (singleton pattern).
    
    Returns:
        SQLModel engine instance
    """
    global _engine
    if _engine is None:
        _engine = create_database_engine()
    return _engine


@contextmanager
def get_session() -> Iterator[Session]:
    """
    Get database session with automatic cleanup.
    
    Yields:
        SQLModel Session instance
        
    Raises:
        DatabaseError: If session operation fails
    """
    engine = get_engine()
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


def get_session_dependency() -> Generator[Session, None, None]:
    """
    FastAPI dependency for getting database session.
    
    Yields:
        SQLModel Session instance
    """
    with get_session() as session:
        yield session


def init_database() -> None:
    """
    Initialize database by creating all tables.
    
    Raises:
        DatabaseError: If database initialization fails
    """
    try:
        engine = get_engine()
        logger.info("Creating database tables...")
        
        # Create all tables
        SQLModel.metadata.create_all(bind=engine)
        
        logger.info("Database tables created successfully")
        
        # Verify connection
        with get_session() as session:
            result = session.exec("SELECT 1").first()
            if result != 1:
                raise DatabaseError("Database connection verification failed")
        
        logger.info("Database initialization completed successfully")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise DatabaseError(f"Database initialization failed: {e}")


def drop_database() -> None:
    """
    Drop all database tables (for testing purposes).
    
    Warning:
        This will delete all data in the database!
        
    Raises:
        DatabaseError: If database drop fails
    """
    try:
        engine = get_engine()
        logger.warning("Dropping all database tables...")
        
        SQLModel.metadata.drop_all(bind=engine)
        
        logger.warning("All database tables dropped")
        
    except Exception as e:
        logger.error(f"Database drop failed: {e}")
        raise DatabaseError(f"Database drop failed: {e}")


def check_database_health() -> bool:
    """
    Check database health and connectivity.
    
    Returns:
        True if database is healthy, False otherwise
    """
    try:
        with get_session() as session:
            result = session.exec("SELECT 1").first()
            return result == 1
            
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False


def close_database_connections() -> None:
    """
    Close all database connections and dispose engine.
    
    This should be called during application shutdown.
    """
    global _engine
    
    try:
        if _engine:
            _engine.dispose()
            logger.info("Database engine disposed")
        
        _engine = None
        logger.info("Database connections closed")
        
    except Exception as e:
        logger.error(f"Error closing database connections: {e}")


# Database event handlers for application lifecycle
def on_startup():
    """Database startup handler."""
    logger.info("Initializing database connections...")
    init_database()


def on_shutdown():
    """Database shutdown handler."""
    logger.info("Closing database connections...")
    close_database_connections()