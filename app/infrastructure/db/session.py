from sqlalchemy.orm import sessionmaker
from sqlmodel import create_engine

from app.core.config import get_settings

settings = get_settings()

db_url = settings.database.get_database_url(sync=True)
engine = create_engine(db_url, **settings.database.engine_kwargs)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
