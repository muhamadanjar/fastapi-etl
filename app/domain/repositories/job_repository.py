from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, TypeVar, Generic
from uuid import UUID

T = TypeVar('T')

class IJobRepository(ABC, Generic[T]):
    @abstractmethod
    def create(self, job_data: Dict[str, Any]) -> T:
        pass

    @abstractmethod
    def get(self, job_id: UUID) -> Optional[T]:
        pass

    @abstractmethod
    def list(self, filters: Dict[str, Any] = None) -> List[T]:
        pass

    @abstractmethod
    def update(self, job_id: UUID, job_data: Dict[str, Any]) -> T:
        pass

    @abstractmethod
    def delete(self, job_id: UUID) -> bool:
        pass
