"""
Base service class providing common functionality for all services.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Union
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from app.utils.logger import get_logger
from app.core.exceptions import ServiceError


class BaseService(ABC):
    """Base class for all services."""
    
    def __init__(self, db_session: Union[Session, AsyncSession]):
        self.db = db_session
        self.logger = get_logger(self.__class__.__name__)
    
    def log_operation(self, operation: str, details: Optional[Dict[str, Any]] = None) -> None:
        """Log service operation."""
        log_msg = f"Service operation: {operation}"
        if details:
            log_msg += f" - Details: {details}"
        self.logger.info(log_msg)
    
    def handle_error(self, error: Exception, operation: str) -> None:
        """Handle service errors."""
        error_msg = f"Error in {operation}: {str(error)}"
        self.logger.error(error_msg)
        raise ServiceError(error_msg) from error
    
    def validate_input(self, data: Dict[str, Any], required_fields: list) -> None:
        """Validate input data."""
        missing_fields = [field for field in required_fields if field not in data or data[field] is None]
        if missing_fields:
            raise ServiceError(f"Missing required fields: {', '.join(missing_fields)}")
    
    @abstractmethod
    def get_service_name(self) -> str:
        """Return the service name."""
        pass