
from app.infrastructure.db.models.raw_data.file_registry import FileRegistry
from app.infrastructure.db.repositories.base import BaseRepository

from sqlmodel import Session, select

class FileRegistryRepository(BaseRepository[FileRegistry]):

    def __init__(self, session: Session):
        super().__init__(FileRegistry, session)
