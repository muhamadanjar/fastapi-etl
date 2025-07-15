from abc import ABC, abstractmethod
from typing import Dict, Any

class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single data record."""
        pass