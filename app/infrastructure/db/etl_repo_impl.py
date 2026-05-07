from app.domain.repositories.etl_repo import ETLRepository
from app.infrastructure.db.models import ETLResult
from app.infrastructure.db.manager import database_manager

class ETLRepositoryImpl(ETLRepository):
    def save_result(self, source: str, result: str):
        with database_manager.get_session() as db:
            db.add(ETLResult(source=source, result=result))

    def get_result(self, job_id: str):
        with database_manager.get_session() as db:
            return db.query(ETLResult).filter(ETLResult.id == job_id).first()