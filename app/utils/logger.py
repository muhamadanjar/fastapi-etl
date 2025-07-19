"""
Logging utilities for the ETL system.
Provides centralized logging configuration and logger instances.
"""

import logging
import logging.handlers
import os
import sys
from datetime import datetime
from typing import Optional
from pathlib import Path
import json

from app.core.config import settings


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging."""
    
    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "thread": record.thread,
            "process": record.process
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields
        if hasattr(record, 'extra_data'):
            log_entry["extra"] = record.extra_data
        
        # Add correlation ID if present
        if hasattr(record, 'correlation_id'):
            log_entry["correlation_id"] = record.correlation_id
        
        # Add user context if present
        if hasattr(record, 'user_id'):
            log_entry["user_id"] = record.user_id
        
        return json.dumps(log_entry, default=str)


class ETLLoggerAdapter(logging.LoggerAdapter):
    """Custom logger adapter for ETL operations."""
    
    def __init__(self, logger, extra=None):
        super().__init__(logger, extra or {})
    
    def process(self, msg, kwargs):
        # Add extra context to log records
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        
        # Merge adapter extra with kwargs extra
        kwargs['extra'].update(self.extra)
        
        return msg, kwargs
    
    def log_operation(self, operation: str, details: Optional[dict] = None, level: int = logging.INFO):
        """Log an operation with structured data."""
        extra_data = {
            "operation": operation,
            "details": details or {}
        }
        self.log(level, f"Operation: {operation}", extra={"extra_data": extra_data})
    
    def log_error(self, operation: str, error: Exception, details: Optional[dict] = None):
        """Log an error with operation context."""
        extra_data = {
            "operation": operation,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "details": details or {}
        }
        self.error(f"Error in operation: {operation}", extra={"extra_data": extra_data}, exc_info=True)
    
    def log_performance(self, operation: str, duration: float, details: Optional[dict] = None):
        """Log performance metrics."""
        extra_data = {
            "operation": operation,
            "duration_seconds": duration,
            "performance_metric": True,
            "details": details or {}
        }
        self.info(f"Performance: {operation} completed in {duration:.2f}s", 
                 extra={"extra_data": extra_data})


def setup_logging(
    log_level: str = None,
    log_file: str = None,
    enable_json: bool = False,
    enable_console: bool = True,
    max_file_size: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5
) -> None:
    """
    Setup logging configuration for the ETL system.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file
        enable_json: Whether to use JSON formatting
        enable_console: Whether to log to console
        max_file_size: Maximum size of log file before rotation
        backup_count: Number of backup files to keep
    """
    
    # Get configuration from settings if not provided
    log_level = log_level or getattr(settings, 'LOG_LEVEL', 'INFO')
    log_file = log_file or getattr(settings, 'LOG_FILE', None)
    enable_json = enable_json or getattr(settings, 'LOG_JSON_FORMAT', False)
    
    # Convert log level string to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Create logs directory if it doesn't exist
    if log_file:
        log_dir = Path(log_file).parent
        log_dir.mkdir(parents=True, exist_ok=True)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Choose formatter
    if enable_json:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    # Console handler
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(numeric_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # File handler with rotation
    if log_file:
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_file,
            maxBytes=max_file_size,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Set specific logger levels
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy.pool').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    # Log the configuration
    root_logger.info(f"Logging configured - Level: {log_level}, File: {log_file}, JSON: {enable_json}")


def get_logger(name: str, extra_context: Optional[dict] = None) -> ETLLoggerAdapter:
    """
    Get a logger instance with ETL-specific functionality.
    
    Args:
        name: Logger name (usually __name__)
        extra_context: Additional context to include in all log messages
    
    Returns:
        ETLLoggerAdapter instance
    """
    base_logger = logging.getLogger(name)
    return ETLLoggerAdapter(base_logger, extra_context or {})


def get_correlation_logger(name: str, correlation_id: str, user_id: Optional[str] = None) -> ETLLoggerAdapter:
    """
    Get a logger with correlation ID for request tracing.
    
    Args:
        name: Logger name
        correlation_id: Unique identifier for the request/operation
        user_id: Optional user identifier
    
    Returns:
        ETLLoggerAdapter with correlation context
    """
    context = {"correlation_id": correlation_id}
    if user_id:
        context["user_id"] = user_id
    
    return get_logger(name, context)


def log_function_call(func):
    """
    Decorator to log function calls with parameters and execution time.
    
    Usage:
        @log_function_call
        def my_function(param1, param2):
            return result
    """
    import functools
    import time
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        
        # Log function entry
        logger.log_operation(
            f"Entering {func.__name__}",
            {
                "function": func.__name__,
                "module": func.__module__,
                "args_count": len(args),
                "kwargs_keys": list(kwargs.keys())
            }
        )
        
        start_time = time.time()
        try:
            # Execute function
            result = func(*args, **kwargs)
            
            # Log successful completion
            duration = time.time() - start_time
            logger.log_performance(
                f"Completed {func.__name__}",
                duration,
                {
                    "function": func.__name__,
                    "module": func.__module__,
                    "success": True
                }
            )
            
            return result
            
        except Exception as e:
            # Log error
            duration = time.time() - start_time
            logger.log_error(
                f"Failed {func.__name__}",
                e,
                {
                    "function": func.__name__,
                    "module": func.__module__,
                    "duration": duration
                }
            )
            raise
    
    return wrapper


class LogContext:
    """
    Context manager for adding temporary context to log messages.
    
    Usage:
        logger = get_logger(__name__)
        with LogContext(logger, operation="data_processing", batch_id="batch_123"):
            logger.info("Processing data")  # Will include operation and batch_id
    """
    
    def __init__(self, logger_adapter: ETLLoggerAdapter, **context):
        self.logger_adapter = logger_adapter
        self.context = context
        self.original_extra = None
    
    def __enter__(self):
        # Save original extra context
        self.original_extra = self.logger_adapter.extra.copy()
        
        # Add new context
        self.logger_adapter.extra.update(self.context)
        
        return self.logger_adapter
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore original context
        self.logger_adapter.extra = self.original_extra


def configure_uvicorn_logging():
    """Configure uvicorn logging to work with our logging setup."""
    
    # Get our formatter
    enable_json = getattr(settings, 'LOG_JSON_FORMAT', False)
    if enable_json:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    # Configure uvicorn loggers
    uvicorn_loggers = [
        "uvicorn",
        "uvicorn.error",
        "uvicorn.access"
    ]
    
    for logger_name in uvicorn_loggers:
        logger = logging.getLogger(logger_name)
        
        # Clear existing handlers
        logger.handlers.clear()
        
        # Add our handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False


# Initialize logging when module is imported
if not logging.getLogger().handlers:
    setup_logging()


# Create a default logger for this module
module_logger = get_logger(__name__)