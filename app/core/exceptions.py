from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.status import HTTP_400_BAD_REQUEST
from typing import Any, Dict, Optional, Union


class AppException(Exception):
    def __init__(self, message: str, 
                error_code: Optional[str] = None,
                 status_code: int = HTTP_400_BAD_REQUEST, 
                 details: Optional[Dict[str, Any]] = None,):
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.status_code = status_code
        self.details = details or {}
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.status_code}: {self.message}"
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(message='{self.message}', status_code={self.status_code})"
    

class ValidationException(AppException):
    def __init__(
        self,
        message: str = "Validation failed",
        field: Optional[str] = None,
        value: Optional[Any] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.field = field
        self.value = value
        
        exception_details = details or {}
        if field:
            exception_details["field"] = field
        if value is not None:
            exception_details["value"] = value
        
        super().__init__(
            message=message,
            error_code="VALIDATION_ERROR",
            details=exception_details,
            status_code=422,
        )

class NotFoundError(AppException):
    """Exception raised when a resource is not found."""
    
    def __init__(
        self,
        resource: str = "Resource",
        resource_id: Optional[Union[str, int]] = None,
        message: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.resource = resource
        self.resource_id = resource_id
        
        if not message:
            if resource_id:
                message = f"{resource} with ID '{resource_id}' not found"
            else:
                message = f"{resource} not found"
        
        exception_details = details or {}
        exception_details.update({
            "resource": resource,
            "resource_id": resource_id,
        })
        
        super().__init__(
            message=message,
            error_code="NOT_FOUND",
            details=exception_details,
            status_code=404,
        )


class UnauthorizedError(AppException):
    """Exception raised when authentication fails."""
    
    def __init__(
        self,
        message: str = "Authentication required",
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            message=message,
            error_code="UNAUTHORIZED",
            details=details,
            status_code=401,
        )


class ForbiddenError(AppException):
    """Exception raised when access is forbidden."""
    
    def __init__(
        self,
        message: str = "Access forbidden",
        resource: Optional[str] = None,
        action: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.resource = resource
        self.action = action
        
        exception_details = details or {}
        if resource:
            exception_details["resource"] = resource
        if action:
            exception_details["action"] = action
        
        super().__init__(
            message=message,
            error_code="FORBIDDEN",
            details=exception_details,
            status_code=403,
        )


class ConflictError(AppException):
    """Exception raised when there's a conflict with current state."""
    
    def __init__(
        self,
        message: str = "Conflict with current state",
        resource: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.resource = resource
        
        exception_details = details or {}
        if resource:
            exception_details["resource"] = resource
        
        super().__init__(
            message=message,
            error_code="CONFLICT",
            details=exception_details,
            status_code=409,
        )


class BadRequestError(AppException):
    """Exception raised for bad requests."""
    
    def __init__(
        self,
        message: str = "Bad request",
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            message=message,
            error_code="BAD_REQUEST",
            details=details,
            status_code=400,
        )


class InternalServerError(AppException):
    """Exception raised for internal server errors."""
    
    def __init__(
        self,
        message: str = "Internal server error",
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            message=message,
            error_code="INTERNAL_SERVER_ERROR",
            details=details,
            status_code=500,
        )


class ServiceUnavailableError(AppException):
    """Exception raised when a service is unavailable."""
    
    def __init__(
        self,
        service: str,
        message: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.service = service
        
        if not message:
            message = f"Service '{service}' is currently unavailable"
        
        exception_details = details or {}
        exception_details["service"] = service
        
        super().__init__(
            message=message,
            error_code="SERVICE_UNAVAILABLE",
            details=exception_details,
            status_code=503,
        )


class RateLimitExceededError(AppException):
    """Exception raised when rate limit is exceeded."""
    
    def __init__(
        self,
        message: str = "Rate limit exceeded",
        limit: Optional[int] = None,
        window: Optional[int] = None,
        retry_after: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.limit = limit
        self.window = window
        self.retry_after = retry_after
        
        exception_details = details or {}
        if limit:
            exception_details["limit"] = limit
        if window:
            exception_details["window"] = window
        if retry_after:
            exception_details["retry_after"] = retry_after
        
        super().__init__(
            message=message,
            error_code="RATE_LIMIT_EXCEEDED",
            details=exception_details,
            status_code=429,
        )


class DatabaseError(AppException):
    """Exception raised for database-related errors."""
    
    def __init__(
        self,
        message: str = "Database error occurred",
        operation: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.operation = operation
        
        exception_details = details or {}
        if operation:
            exception_details["operation"] = operation
        
        super().__init__(
            message=message,
            error_code="DATABASE_ERROR",
            details=exception_details,
            status_code=500,
        )


class CacheError(AppException):
    """Exception raised for cache-related errors."""
    
    def __init__(
        self,
        message: str = "Cache error occurred",
        operation: Optional[str] = None,
        key: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.operation = operation
        self.key = key
        
        exception_details = details or {}
        if operation:
            exception_details["operation"] = operation
        if key:
            exception_details["key"] = key
        
        super().__init__(
            message=message,
            error_code="CACHE_ERROR",
            details=exception_details,
            status_code=500,
        )


class MessageBrokerError(AppException):
    """Exception raised for message broker-related errors."""
    
    def __init__(
        self,
        message: str = "Message broker error occurred",
        operation: Optional[str] = None,
        queue: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.operation = operation
        self.queue = queue
        
        exception_details = details or {}
        if operation:
            exception_details["operation"] = operation
        if queue:
            exception_details["queue"] = queue
        
        super().__init__(
            message=message,
            error_code="MESSAGE_BROKER_ERROR",
            details=exception_details,
            status_code=500,
        )


class EmailError(AppException):
    """Exception raised for email-related errors."""
    
    def __init__(
        self,
        message: str = "Email error occurred",
        recipient: Optional[str] = None,
        template: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.recipient = recipient
        self.template = template
        
        exception_details = details or {}
        if recipient:
            exception_details["recipient"] = recipient
        if template:
            exception_details["template"] = template
        
        super().__init__(
            message=message,
            error_code="EMAIL_ERROR",
            details=exception_details,
            status_code=500,
        )


class FileStorageError(AppException):
    """Exception raised for file storage-related errors."""
    
    def __init__(
        self,
        message: str = "File storage error occurred",
        operation: Optional[str] = None,
        file_path: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.operation = operation
        self.file_path = file_path
        
        exception_details = details or {}
        if operation:
            exception_details["operation"] = operation
        if file_path:
            exception_details["file_path"] = file_path
        
        super().__init__(
            message=message,
            error_code="FILE_STORAGE_ERROR",
            details=exception_details,
            status_code=500,
        )


def app_exception_handler(request: Request, exc: AppException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.message}
    )
