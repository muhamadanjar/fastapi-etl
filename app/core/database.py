from sqlmodel import create_engine, SQLModel

from sqlalchemy.orm import sessionmaker, declarative_base
from app.core.config import settings
import logging


engine = create_engine(settings.database_url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
logger = logging.getLogger(__name__)

def init_db():
    SQLModel.metadata.create_all(engine)
