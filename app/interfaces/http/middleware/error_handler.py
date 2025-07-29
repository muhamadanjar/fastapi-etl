import traceback
import sys
from datetime import datetime
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from pydantic import ValidationError
import json

from app.core.exceptions import (
    ETLException, 
    FileProcessingException,
    DataQualityException,
    AuthenticationException,
    AuthorizationException
)
from app.utils.logger import get_logger
from app.services.notification_service import NotificationService

logger = get_logger(__name__)

class ErrorHandlerMiddleware(BaseHTTPMiddleware):
    """
    Global error handling middleware for consistent error responses,
    logging, and monitoring
    """
    
    def __init__(self, app):
        super().__init__(app)
        self.notification_service = NotificationService()
    
    async def dispatch(self, request: Request, call_next):
        try:
            return await call_next(request)
        except Exception as e:
            return await self._handle_exception(request, e)
    
    async def _handle_exception(self, request: Request, exception: Exception) -> JSONResponse:
        """Handle different types of exceptions and return appropriate responses"""
        
        # Get request context
        request_id = getattr(request.state, 'request_id', 'unknown')
        user_id = getattr(request.state, 'user_id', None)
        path = request.url.path
        method = request.method
        
        # Prepare base error context
        error_context = {
            "request_id": request_id,
            "timestamp": datetime.utcnow().isoformat(),
            "path": path,
            "method": method,
            "user_id": user_id,
            "error_type": type(exception).__name__,
            "error_message": str(exception)
        }
        
        # Handle specific exception types
        if isinstance(exception, HTTPException):
            return await self._handle_http_exception(exception, error_context)
        
        elif isinstance(exception, ValidationError):
            return await self._handle_validation_error(exception, error_context)
        
        elif isinstance(exception, AuthenticationException):
            return await self._handle_auth_exception(exception, error_context, 401)
        
        elif isinstance(exception, AuthorizationException):
            return await self._handle_auth_exception(exception, error_context, 403)
        
        elif isinstance(exception, FileProcessingException):
            return await self._handle_file_processing_error(exception, error_context)
        
        elif isinstance(exception, DataQualityException):
            return await self._handle_data_quality_error(exception, error_context)
        
        elif isinstance(exception, ETLException):
            return await self._handle_etl_error(exception, error_context)
        
        elif isinstance(exception, SQLAlchemyError):
            return await self._handle_database_error(exception, error_context)
        
        else:
            return await self._handle_unexpected_error(exception, error_context)
    
    async def _handle_http_exception(self, exception: HTTPException, context: dict) -> JSONResponse:
        """Handle FastAPI HTTP exceptions"""
        context.update({
            "status_code": exception.status_code,
            "detail": exception.detail
        })
        
        # Log based on status code severity
        if exception.status_code >= 500:
            logger.error(f"HTTP Exception: {json.dumps(context)}")
        else:
            logger.warning(f"HTTP Exception: {json.dumps(context)}")
        
        return JSONResponse(
            status_code=exception.status_code,
            content={
                "error": {
                    "type": "http_exception",
                    "message": exception.detail,
                    "status_code": exception.status_code,
                    "request_id": context["request_id"]
                }
            }
        )
    
    async def _handle_validation_error(self, exception: ValidationError, context: dict) -> JSONResponse:
        """Handle Pydantic validation errors"""
        context.update({
            "validation_errors": exception.errors(),
            "status_code": 422
        })
        
        logger.warning(f"Validation Error: {json.dumps(context, default=str)}")
        
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "type": "validation_error",
                    "message": "Request validation failed",
                    "details": exception.errors(),
                    "request_id": context["request_id"]
                }
            }
        )
    
    async def _handle_auth_exception(self, exception: Exception, context: dict, status_code: int) -> JSONResponse:
        """Handle authentication and authorization errors"""
        context.update({"status_code": status_code})
        
        logger.warning(f"Auth Exception: {json.dumps(context)}")
        
        return JSONResponse(
            status_code=status_code,
            content={
                "error": {
                    "type": "authentication_error" if status_code == 401 else "authorization_error",
                    "message": str(exception),
                    "request_id": context["request_id"]
                }
            }
        )
    
    async def _handle_file_processing_error(self, exception: FileProcessingException, context: dict) -> JSONResponse:
        """Handle file processing specific errors"""
        context.update({
            "status_code": 400,
            "file_info": getattr(exception, 'file_info', None)
        })
        
        logger.error(f"File Processing Error: {json.dumps(context, default=str)}")
        
        return JSONResponse(
            status_code=400,
            content={
                "error": {
                    "type": "file_processing_error",
                    "message": str(exception),
                    "file_info": getattr(exception, 'file_info', None),
                    "request_id": context["request_id"]
                }
            }
        )
    
    async def _handle_data_quality_error(self, exception: DataQualityException, context: dict) -> JSONResponse:
        """Handle data quality specific errors"""
        context.update({
            "status_code": 400,
            "quality_issues": getattr(exception, 'quality_issues', None)
        })
        
        logger.error(f"Data Quality Error: {json.dumps(context, default=str)}")
        
        return JSONResponse(
            status_code=400,
            content={
                "error": {
                    "type": "data_quality_error",
                    "message": str(exception),
                    "quality_issues": getattr(exception, 'quality_issues', None),
                    "request_id": context["request_id"]
                }
            }
        )
    
    async def _handle_etl_error(self, exception: ETLException, context: dict) -> JSONResponse:
        """Handle ETL process specific errors"""
        context.update({
            "status_code": 500,
            "job_info": getattr(exception, 'job_info', None)
        })
        
        logger.error(f"ETL Error: {json.dumps(context, default=str)}")
        
        # Send critical alert for ETL failures
        await self._send_critical_alert(context)
        
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "type": "etl_error",
                    "message": str(exception),
                    "job_info": getattr(exception, 'job_info', None),
                    "request_id": context["request_id"]
                }
            }
        )
    
    async def _handle_database_error(self, exception: SQLAlchemyError, context: dict) -> JSONResponse:
        """Handle database related errors"""
        context.update({
            "status_code": 500,
            "db_error_code": getattr(exception, 'code', None)
        })
        
        logger.error(f"Database Error: {json.dumps(context, default=str)}")
        
        # Check for specific database errors
        if isinstance(exception, IntegrityError):
            return JSONResponse(
                status_code=409,
                content={
                    "error": {
                        "type": "integrity_error",
                        "message": "Data integrity constraint violation",
                        "request_id": context["request_id"]
                    }
                }
            )
        
        # Send critical alert for database issues
        await self._send_critical_alert(context)
        
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "type": "database_error", 
                    "message": "Database operation failed",
                    "request_id": context["request_id"]
                }
            }
        )
    
    async def _handle_unexpected_error(self, exception: Exception, context: dict) -> JSONResponse:
        """Handle unexpected/unhandled errors"""
        # Capture full traceback
        tb = traceback.format_exception(type(exception), exception, exception.__traceback__)
        context.update({
            "status_code": 500,
            "traceback": "".join(tb),
            "python_version": sys.version
        })
        
        logger.critical(f"Unexpected Error: {json.dumps(context, default=str)}")
        
        # Send critical alert for unexpected errors
        await self._send_critical_alert(context)
        
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "type": "internal_server_error",
                    "message": "An unexpected error occurred",
                    "request_id": context["request_id"]
                }
            }
        )
    
    async def _send_critical_alert(self, context: dict):
        """Send critical alerts for severe errors"""
        try:
            alert_data = {
                "severity": "critical",
                "source": "error_handler_middleware",
                "message": f"Critical error in {context['path']}",
                "context": context,
                "timestamp": context["timestamp"]
            }
            
            # Send to notification service (email, Slack, etc.)
            await self.notification_service.send_critical_alert(alert_data)
            
        except Exception as e:
            # Don't let notification errors break the error handler
            logger.error(f"Failed to send critical alert: {str(e)}")