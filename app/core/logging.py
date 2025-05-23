import logging
import sys
import os
from pathlib import Path
from typing import Any, Dict, Optional
from .config import get_settings

settings = get_settings()

class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hostname = self._get_hostname()
    
    def _get_hostname(self) -> str:
        """Get the hostname of the current machine."""
        import socket
        try:
            return socket.gethostname()
        except Exception:
            return "unknown"
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "hostname": self.hostname,
            "process_id": record.process,
            "thread_id": record.thread,
        }
        
        # Add exception information if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields if present
        if hasattr(record, "extra_fields"):
            log_entry.update(record.extra_fields)
        
        # Add request context if available
        if hasattr(record, "request_id"):
            log_entry["request_id"] = record.request_id
        
        if hasattr(record, "user_id"):
            log_entry["user_id"] = record.user_id
        
        if hasattr(record, "correlation_id"):
            log_entry["correlation_id"] = record.correlation_id
        
        return json.dumps(log_entry, default=str, ensure_ascii=False)

class ContextFilter(logging.Filter):
    """Filter to add context information to log records."""
    
    def __init__(self, context: Optional[Dict[str, Any]] = None):
        super().__init__()
        self.context = context or {}
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Add context information to the log record."""
        for key, value in self.context.items():
            setattr(record, key, value)
        return True

class RequestContextFilter(logging.Filter):
    """Filter to add request context to log records."""
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Add request context information to the log record."""
        import contextvars
        
        # Try to get request context from context variables
        try:
            # These would be set by middleware
            request_id = contextvars.copy_context().get('request_id', None)
            user_id = contextvars.copy_context().get('user_id', None)
            correlation_id = contextvars.copy_context().get('correlation_id', None)
            
            if request_id:
                record.request_id = request_id
            if user_id:
                record.user_id = user_id
            if correlation_id:
                record.correlation_id = correlation_id
                
        except Exception:
            # If context variables are not available, continue without them
            pass
        
        return True    


def setup_logging() -> None:
    """Setup application logging configuration."""
    
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, settings.logging.level.upper()))
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create formatters
    if settings.logging.json_format:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            fmt=settings.logging.format,
            datefmt="%Y-%m-%d %H:%M:%S"
        )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(RequestContextFilter())
    root_logger.addHandler(console_handler)
    
    # File handler (if file path is specified)
    if settings.logging.file_path:
        file_path = Path(settings.logging.file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            filename=file_path,
            maxBytes=settings.logging.max_bytes,
            backupCount=settings.logging.backup_count,
            encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        file_handler.addFilter(RequestContextFilter())
        root_logger.addHandler(file_handler)
    
    # Set specific logger levels
    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.INFO)
    logging.getLogger("fastapi").setLevel(logging.INFO)
    logging.getLogger("sqlalchemy.engine").setLevel(
        logging.INFO if settings.database.echo else logging.WARNING
    )
    
    # Suppress noisy loggers in production
    if settings.is_production:
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("boto3").setLevel(logging.WARNING)
        logging.getLogger("botocore").setLevel(logging.WARNING)


def get_logger(name: str, context: Optional[Dict[str, Any]] = None) -> logging.Logger:
    """
    Get a logger with optional context.
    
    Args:
        name: Logger name (usually __name__)
        context: Additional context to add to all log messages
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    if context:
        context_filter = ContextFilter(context)
        logger.addFilter(context_filter)
    
    return logger


class LoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that adds extra context to log messages."""
    
    def __init__(self, logger: logging.Logger, extra: Dict[str, Any]):
        super().__init__(logger, extra)
    
    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
        """Process log message and add extra context."""
        if "extra" not in kwargs:
            kwargs["extra"] = {}
        
        kwargs["extra"].update(self.extra)
        return msg, kwargs


def log_function_call(
    logger: logging.Logger,
    level: int = logging.DEBUG,
    include_args: bool = True,
    include_result: bool = False,
):
    """
    Decorator to log function calls.
    
    Args:
        logger: Logger instance to use
        level: Log level for the messages
        include_args: Whether to include function arguments
        include_result: Whether to include function result
    """
    def decorator(func):
        import functools
        import inspect
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func_name = func.__name__
            
            # Log function entry
            if include_args:
                # Get function signature
                sig = inspect.signature(func)
                bound_args = sig.bind(*args, **kwargs)
                bound_args.apply_defaults()
                
                logger.log(
                    level,
                    f"Calling {func_name} with args: {dict(bound_args.arguments)}"
                )
            else:
                logger.log(level, f"Calling {func_name}")
            
            try:
                # Execute function
                result = func(*args, **kwargs)
                
                # Log function exit
                if include_result:
                    logger.log(level, f"{func_name} returned: {result}")
                else:
                    logger.log(level, f"{func_name} completed successfully")
                
                return result
                
            except Exception as e:
                logger.error(f"{func_name} failed with error: {e}", exc_info=True)
                raise
        
        return wrapper
    return decorator


def log_execution_time(logger: logging.Logger, level: int = logging.INFO):
    """
    Decorator to log function execution time.
    
    Args:
        logger: Logger instance to use
        level: Log level for the messages
    """
    def decorator(func):
        import functools
        import time
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func_name = func.__name__
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                
                logger.log(
                    level,
                    f"{func_name} executed in {execution_time:.4f} seconds"
                )
                
                return result
                
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(
                    f"{func_name} failed after {execution_time:.4f} seconds: {e}",
                    exc_info=True
                )
                raise
        
        return wrapper
    return decorator


class StructuredLogger:
    """Wrapper for structured logging with predefined fields."""
    
    def __init__(self, name: str, **default_fields):
        self.logger = logging.getLogger(name)
        self.default_fields = default_fields
    
    def _log(self, level: int, message: str, **fields):
        """Log message with structured fields."""
        extra_fields = {**self.default_fields, **fields}
        extra = {"extra_fields": extra_fields}
        self.logger.log(level, message, extra=extra)
    
    def debug(self, message: str, **fields):
        """Log debug message."""
        self._log(logging.DEBUG, message, **fields)
    
    def info(self, message: str, **fields):
        """Log info message."""
        self._log(logging.INFO, message, **fields)
    
    def warning(self, message: str, **fields):
        """Log warning message."""
        self._log(logging.WARNING, message, **fields)
    
    def error(self, message: str, **fields):
        """Log error message."""
        self._log(logging.ERROR, message, **fields)
    
    def critical(self, message: str, **fields):
        """Log critical message."""
        self._log(logging.CRITICAL, message, **fields)


def audit_log(
    action: str,
    resource: str,
    user_id: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    success: bool = True,
):
    """
    Log audit events for security and compliance.
    
    Args:
        action: Action performed (e.g., 'CREATE', 'UPDATE', 'DELETE')
        resource: Resource affected (e.g., 'USER', 'ORDER')
        user_id: ID of the user performing the action
        details: Additional details about the action
        success: Whether the action was successful
    """
    audit_logger = StructuredLogger("audit")
    
    audit_logger.info(
        "Audit event",
        action=action,
        resource=resource,
        user_id=user_id,
        success=success,
        details=details or {},
        event_type="audit"
    )


# Initialize logging when module is imported
if not logging.getLogger().handlers:
    setup_logging()