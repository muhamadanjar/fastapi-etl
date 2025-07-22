from typing import Generator, AsyncGenerator
from sqlmodel import create_engine, SQLModel

from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession
from app.core.config import settings
import logging


engine = create_engine(settings.database_url)
# async_engine = create_async_engine(
#     settings.database_url,
#     echo=settings.DEBUG if hasattr(settings, 'DEBUG') else False,
#     pool_pre_ping=True,
# )
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
logger = logging.getLogger(__name__)

# AsyncSessionLocal = sessionmaker(
#     bind=async_engine,
#     class_=AsyncSession,
#     expire_on_commit=False,
#     autocommit=False,
#     autoflush=False,
# )

def init_db():
    SQLModel.metadata.create_all(engine)


def get_session() -> Generator:
    with SessionLocal() as session:
        yield session

# async def get_async_session() ->  AsyncGenerator:
#     async with AsyncSessionLocal() as session:
#         yield session
        