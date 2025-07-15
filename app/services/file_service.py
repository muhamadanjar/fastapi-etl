from app.services.base import BaseService
from app.models.raw_data import file_registry
from sqlmodel import select

class FileService(BaseService):
    def list_pending_files(self):
        return self.session.exec(
            select(file_registry.FileRegistry).where(
                file_registry.FileRegistry.processing_status == "PENDING"
            )
        ).all()