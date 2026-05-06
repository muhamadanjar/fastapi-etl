from typing import Generator, AsyncGenerator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()

_engine = None
_async_engine = None


def _get_settings():
    from app.core.config import settings
    return settings


def get_engine():
    global _engine
    if _engine is None:
        s = _get_settings()
        _engine = create_engine(
            s.database_url,
            pool_size=s.database_pool_size,
            max_overflow=s.database_max_overflow,
            pool_timeout=s.database_pool_timeout,
            pool_recycle=s.database_pool_recycle,
            pool_pre_ping=True,
            echo=s.database_echo,
        )
        logger.info("Sync DB engine created")
    return _engine


def get_async_engine():
    global _async_engine
    if _async_engine is None:
        s = _get_settings()
        async_url = s.database_url
        if async_url.startswith("postgresql://"):
            async_url = async_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        elif async_url.startswith("postgresql+psycopg2://"):
            async_url = async_url.replace("postgresql+psycopg2://", "postgresql+asyncpg://", 1)
        _async_engine = create_async_engine(
            async_url,
            pool_size=s.database_pool_size,
            max_overflow=s.database_max_overflow,
            pool_timeout=s.database_pool_timeout,
            pool_recycle=s.database_pool_recycle,
            pool_pre_ping=True,
            echo=s.database_echo,
        )
        logger.info("Async DB engine created")
    return _async_engine


def init_db():
    SQLModel.metadata.create_all(get_engine())


def get_session() -> Generator:
    Session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
    )
    try:
        yield Session
    finally:
        Session.remove()


async def get_async_session() -> AsyncGenerator:
    AsyncSessionLocal = async_sessionmaker(
        get_async_engine(), class_=AsyncSession, expire_on_commit=False
    )
    async with AsyncSessionLocal() as session:
        yield session


# Aliases for backward compatibility
get_db = get_session
