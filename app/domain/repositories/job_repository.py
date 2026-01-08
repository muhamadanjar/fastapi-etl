from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from uuid import UUID

from app.domain.entities.job import Job  # Assuming Job entity exists or will be created, or use generic
# For now, we might rely on models if entities aren't fully separated yet, but Clean Architecture prefers Entities.
# Given the current state, I'll define the interface using types that are likely available or generic.

class IJobRepository(ABC):
    @abstractmethod
    def create(self, job_data: Dict[str, Any]) -> Any:
        pass

    @abstractmethod
    def get(self, job_id: UUID) -> Optional[Any]:
        pass

    @abstractmethod
    def list(self, filters: Dict[str, Any] = None) -> List[Any]:
        pass

    @abstractmethod
    def update(self, job_id: UUID, job_data: Dict[str, Any]) -> Any:
        pass

    @abstractmethod
    def delete(self, job_id: UUID) -> bool:
        pass
