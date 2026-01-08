from typing import List, Optional, Dict, Any
from uuid import UUID
from sqlalchemy.orm import Session

from app.infrastructure.db.repositories.base import BaseRepository
from app.infrastructure.db.models.etl_control.etl_jobs import EtlJob
from app.domain.repositories.job_repository import IJobRepository

class JobRepository(BaseRepository[EtlJob], IJobRepository):
    def __init__(self, session: Session):
        super().__init__(EtlJob, session)

    def create(self, job_data: Dict[str, Any]) -> EtlJob:
        return super().create(job_data)

    def get(self, job_id: UUID) -> Optional[EtlJob]:
        return super().get(job_id)

    def list(self, filters: Dict[str, Any] = None) -> List[EtlJob]:
        # Using get_multi from BaseRepository which supports filters kwargs
        return super().get_multi(**(filters or {}))

    def update(self, job_id: UUID, job_data: Dict[str, Any]) -> EtlJob:
        return super().update(id=job_id, obj_in=job_data)

    def delete(self, job_id: UUID) -> bool:
        return super().delete(job_id)
