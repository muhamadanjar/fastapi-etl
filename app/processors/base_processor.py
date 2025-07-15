from abc import ABC, abstractmethod
from typing import Any, List, Dict

class BaseProcessor(ABC):
    def __init__(self, file_path: str):
        self.file_path = file_path

    @abstractmethod
    def extract(self) -> List[Dict[str, Any]]:
        """Extract raw records from the source file."""
        pass

    @abstractmethod
    def detect_structure(self) -> List[Dict[str, Any]]:
        """Detect column structure and metadata."""
        pass