import time
import json
import uuid
from datetime import datetime
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from typing import Callable, Dict, Any
import asyncio

from app.utils.logger import get_logger
from app.core.constants import LOG_EXCLUDE_PATHS

logger = get_logger(__name__)

class LoggingMiddleware(BaseHTTPMiddleware):
    """
    Comprehensive logging middleware for request/response tracking,
    performance monitoring, and audit trail
    """
    
    def __init__(self, app, exclude_paths: list = None):
        super().__init__(app)
        self.exclude_paths = exclude_paths or LOG_EXCLUDE_PATHS
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Skip logging for excluded paths
        if self._should_skip_logging(request.url.path):
            return await call_next(request)
        
        # Generate unique request ID
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        
        # Capture request start time
        start_time = time.time()
        
        # Log request details
        await self._log_request(request, request_id)
        
        try:
            # Process request
            response = await call_next(request)
            
            # Calculate processing time
            process_time = time.time() - start_time
            
            # Log response details
            await self._log_response(request, response, request_id, process_time)
            
            # Add custom headers
            response.headers["X-Request-ID"] = request_id
            response.headers["X-Process-Time"] = str(round(process_time, 4))
            
            return response
            
        except Exception as e:
            # Calculate processing time for error case
            process_time = time.time() - start_time
            
            # Log error details
            await self._log_error(request, e, request_id, process_time)
            
            # Re-raise the exception
            raise
    
    def _should_skip_logging(self, path: str) -> bool:
        """Check if path should skip detailed logging"""
        return any(excluded in path for excluded in self.exclude_paths)
    
    async def _log_request(self, request: Request, request_id: str):
        """Log incoming request details"""
        # Extract user info if available
        user_id = getattr(request.state, 'user_id', None)
        user_info = f"user:{user_id}" if user_id else "anonymous"
        
        # Extract client info
        client_ip = self._get_client_ip(request)
        user_agent = request.headers.get("User-Agent", "Unknown")
        
        # Prepare request data
        request_data = {
            "request_id": request_id,
            "timestamp": datetime.utcnow().isoformat(),
            "method": request.method,
            "url": str(request.url),
            "path": request.url.path,
            "query_params": dict(request.query_params),
            "headers": dict(request.headers),
            "client_ip": client_ip,
            "user_agent": user_agent,
            "user": user_info,
            "content_type": request.headers.get("Content-Type"),
            "content_length": request.headers.get("Content-Length")
        }
        
        # Log request body for POST/PUT requests (excluding sensitive data)
        if request.method in ["POST", "PUT", "PATCH"]:
            try:
                body = await self._get_request_body(request)
                if body:
                    request_data["body_size"] = len(body)
                    # Only log body for non-file uploads
                    if "multipart/form-data" not in request.headers.get("Content-Type", ""):
                        request_data["body_preview"] = body[:1000]  # First 1000 chars
            except Exception as e:
                logger.warning(f"Could not capture request body: {str(e)}")
        
        logger.info(f"REQUEST: {json.dumps(request_data, default=str)}")
    
    async def _log_response(self, request: Request, response: Response, request_id: str, process_time: float):
        """Log response details"""
        # Extract user info if available
        user_id = getattr(request.state, 'user_id', None)
        user_info = f"user:{user_id}" if user_id else "anonymous"
        
        # Prepare response data
        response_data = {
            "request_id": request_id,
            "timestamp": datetime.utcnow().isoformat(),
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "response_headers": dict(response.headers),
            "process_time": round(process_time, 4),
            "user": user_info
        }
        
        # Add performance classification
        if process_time > 10:
            response_data["performance"] = "slow"
        elif process_time > 1:
            response_data["performance"] = "moderate"
        else:
            response_data["performance"] = "fast"
        
        # Log level based on status code
        if response.status_code >= 500:
            logger.error(f"RESPONSE: {json.dumps(response_data, default=str)}")
        elif response.status_code >= 400:
            logger.warning(f"RESPONSE: {json.dumps(response_data, default=str)}")
        else:
            logger.info(f"RESPONSE: {json.dumps(response_data, default=str)}")
    
    async def _log_error(self, request: Request, exception: Exception, request_id: str, process_time: float):
        """Log error details"""
        user_id = getattr(request.state, 'user_id', None)
        user_info = f"user:{user_id}" if user_id else "anonymous"
        
        error_data = {
            "request_id": request_id,
            "timestamp": datetime.utcnow().isoformat(),
            "method": request.method,
            "path": request.url.path,
            "error_type": type(exception).__name__,
            "error_message": str(exception),
            "process_time": round(process_time, 4),
            "user": user_info
        }
        
        logger.error(f"ERROR: {json.dumps(error_data, default=str)}")
    
    async def _get_request_body(self, request: Request) -> str:
        """Safely extract request body"""
        try:
            body = await request.body()
            return body.decode("utf-8") if body else ""
        except Exception:
            return ""
    
    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP address considering proxies"""
        # Check for forwarded headers (load balancers, proxies)
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()
        
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip
        
        # Fallback to direct client IP
        return request.client.host if request.client else "unknown"
    