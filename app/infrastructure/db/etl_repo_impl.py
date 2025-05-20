from app.domain.repositories.etl_repo import ETLRepository
from app.infrastructure.db.models import ETLResult
from app.core.database import SessionLocal

class ETLRepositoryImpl(ETLRepository):
    def save_result(self, source: str, result: str):
        db = SessionLocal()
        db.add(ETLResult(source=source, result=result))
        db.commit()
        db.close()

    def get_result(self, job_id: str):
        db = SessionLocal()
        result = db.query(ETLResult).filter(ETLResult.id == job_id).first()
        db.close()
        return result