from typing import AsyncGenerator, Callable, Generator

from sqlmodel import Session
from sqlmodel.ext.asyncio.session import AsyncSession

from app.infrastructure.db.manager import database_manager


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Default async database session dependency.
    Usage: session: AsyncSession = Depends(get_async_session)
    """
    async with database_manager.get_async_session("default") as session:
        yield session


def get_session() -> Generator[Session, None, None]:
    """
    Default sync database session dependency.
    Usage: session: Session = Depends(get_session)
    """
    with database_manager.get_session("default") as session:
        yield session


def get_db_session(db_name: str = "default") -> Callable[[], AsyncGenerator[AsyncSession, None]]:
    """
    Factory for named-database session dependency.
    Usage: session: AsyncSession = Depends(get_db_session("secondary"))
    """
    async def _get_session() -> AsyncGenerator[AsyncSession, None]:
        async with database_manager.get_async_session(db_name) as session:
            yield session
    return _get_session


get_db = get_async_session
