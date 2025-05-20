from abc import ABC, abstractmethod

class ETLRepository(ABC):
    @abstractmethod
    def save_result(self, source: str, result: str):
        pass