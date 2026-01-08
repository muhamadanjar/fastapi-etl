"""
Error service untuk managing error logs.
Handles error logging to database, error retrieval, dan error resolution tracking.
"""

import traceback
from typing import Dict, Any, List, Optional
from uuid import UUID
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, func
from app.application.services.base import BaseService
from app.infrastructure.db.models.etl_control.error_logs import (
    ErrorLog,
    ErrorType,
    ErrorSeverity,
    ErrorLogCreate,
    ErrorLogUpdate
)
from app.core.exceptions import ETLError


class ErrorService(BaseService):
    """Service untuk managing error logs."""
    
    def __init__(self, db_session: Session):
        super().__init__(db_session)
    
    def get_service_name(self) -> str:
        return "ErrorService"
    
    async def log_error(
        self,
        error_type: ErrorType,
        error_message: str,
        error_severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        job_execution_id: Optional[UUID] = None,
        error_details: Optional[Dict[str, Any]] = None,
        stack_trace: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Log an error to the database.
        
        Args:
            error_type: Type of error
            error_message: Error message
            error_severity: Severity level
            job_execution_id: Related job execution ID
            error_details: Additional error details
            stack_trace: Full stack trace
            context: Context information
            
        Returns:
            Created error log details
        """
        try:
            self.log_operation("log_error", {
                "error_type": error_type.value,
                "error_severity": error_severity.value,
                "job_execution_id": job_execution_id
            })
            
            error_log = ErrorLog(
                job_execution_id=job_execution_id,
                error_type=error_type,
                error_severity=error_severity,
                error_message=error_message,
                error_details=error_details,
                stack_trace=stack_trace,
                context=context,
                is_resolved=False
            )
            
            self.db_session.add(error_log)
            self.db_session.commit()
            self.db_session.refresh(error_log)
            
            return {
                "error_id": error_log.id,
                "error_type": error_type.value,
                "error_severity": error_severity.value,
                "error_message": error_message,
                "occurred_at": error_log.occurred_at,
                "status": "logged"
            }
            
        except Exception as e:
            self.db_session.rollback()
            self.handle_error(e, "log_error")
    
    async def log_exception(
        self,
        exception: Exception,
        error_type: ErrorType = ErrorType.UNKNOWN_ERROR,
        error_severity: ErrorSeverity = ErrorSeverity.HIGH,
        job_execution_id: Optional[UUID] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Log an exception to the database with full stack trace.
        
        Args:
            exception: The exception to log
            error_type: Type of error
            error_severity: Severity level
            job_execution_id: Related job execution ID
            context: Context information
            
        Returns:
            Created error log details
        """
        error_message = str(exception)
        stack_trace = traceback.format_exc()
        
        error_details = {
            "exception_type": type(exception).__name__,
            "exception_module": type(exception).__module__,
        }
        
        # Add exception attributes if available
        if hasattr(exception, '__dict__'):
            error_details["exception_attributes"] = {
                k: str(v) for k, v in exception.__dict__.items()
            }
        
        return await self.log_error(
            error_type=error_type,
            error_message=error_message,
            error_severity=error_severity,
            job_execution_id=job_execution_id,
            error_details=error_details,
            stack_trace=stack_trace,
            context=context
        )
    
    async def get_errors(
        self,
        job_execution_id: Optional[UUID] = None,
        error_type: Optional[ErrorType] = None,
        error_severity: Optional[ErrorSeverity] = None,
        is_resolved: Optional[bool] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Get error logs with optional filters.
        
        Args:
            job_execution_id: Filter by job execution
            error_type: Filter by error type
            error_severity: Filter by severity
            is_resolved: Filter by resolution status
            start_date: Filter errors after this date
            end_date: Filter errors before this date
            limit: Maximum number of results
            offset: Offset for pagination
            
        Returns:
            List of error logs and metadata
        """
        try:
            self.log_operation("get_errors", {
                "job_execution_id": job_execution_id,
                "error_type": error_type.value if error_type else None,
                "limit": limit,
                "offset": offset
            })
            
            stmt = select(ErrorLog)
            
            # Apply filters
            if job_execution_id:
                stmt = stmt.where(ErrorLog.job_execution_id == job_execution_id)
            if error_type:
                stmt = stmt.where(ErrorLog.error_type == error_type)
            if error_severity:
                stmt = stmt.where(ErrorLog.error_severity == error_severity)
            if is_resolved is not None:
                stmt = stmt.where(ErrorLog.is_resolved == is_resolved)
            if start_date:
                stmt = stmt.where(ErrorLog.occurred_at >= start_date)
            if end_date:
                stmt = stmt.where(ErrorLog.occurred_at <= end_date)
            
            # Get total count
            count_stmt = select(func.count()).select_from(stmt.subquery())
            total_count = self.db_session.execute(count_stmt).scalar()
            
            # Apply pagination and ordering
            stmt = stmt.order_by(ErrorLog.occurred_at.desc())
            stmt = stmt.limit(limit).offset(offset)
            
            errors = self.db_session.execute(stmt).scalars().all()
            
            return {
                "errors": [
                    {
                        "error_id": error.id,
                        "job_execution_id": error.job_execution_id,
                        "error_type": error.error_type.value,
                        "error_severity": error.error_severity.value,
                        "error_message": error.error_message,
                        "error_details": error.error_details,
                        "occurred_at": error.occurred_at,
                        "is_resolved": error.is_resolved,
                        "resolved_at": error.resolved_at,
                        "resolved_by": error.resolved_by
                    }
                    for error in errors
                ],
                "total_count": total_count,
                "limit": limit,
                "offset": offset,
                "has_more": (offset + limit) < total_count
            }
            
        except Exception as e:
            self.handle_error(e, "get_errors")
    
    async def resolve_error(
        self,
        error_id: UUID,
        resolved_by: Optional[UUID] = None
    ) -> Dict[str, Any]:
        """
        Mark an error as resolved.
        
        Args:
            error_id: Error ID to resolve
            resolved_by: User ID who resolved the error
            
        Returns:
            Updated error details
        """
        try:
            self.log_operation("resolve_error", {
                "error_id": error_id,
                "resolved_by": resolved_by
            })
            
            error = self.db_session.get(ErrorLog, error_id)
            if not error:
                raise ETLError("Error log not found")
            
            if error.is_resolved:
                raise ETLError("Error is already resolved")
            
            error.is_resolved = True
            error.resolved_at = datetime.utcnow()
            error.resolved_by = resolved_by
            
            self.db_session.commit()
            
            return {
                "error_id": error_id,
                "is_resolved": True,
                "resolved_at": error.resolved_at,
                "resolved_by": resolved_by,
                "status": "resolved"
            }
            
        except Exception as e:
            self.db_session.rollback()
            self.handle_error(e, "resolve_error")
    
    async def get_error_summary(
        self,
        job_execution_id: Optional[UUID] = None,
        days: int = 7
    ) -> Dict[str, Any]:
        """
        Get error summary statistics.
        
        Args:
            job_execution_id: Filter by job execution
            days: Number of days to look back
            
        Returns:
            Error summary statistics
        """
        try:
            self.log_operation("get_error_summary", {
                "job_execution_id": job_execution_id,
                "days": days
            })
            
            start_date = datetime.utcnow() - timedelta(days=days)
            
            stmt = select(ErrorLog).where(ErrorLog.occurred_at >= start_date)
            if job_execution_id:
                stmt = stmt.where(ErrorLog.job_execution_id == job_execution_id)
            
            errors = self.db_session.execute(stmt).scalars().all()
            
            # Calculate statistics
            total_errors = len(errors)
            resolved_errors = len([e for e in errors if e.is_resolved])
            unresolved_errors = total_errors - resolved_errors
            
            # Group by severity
            by_severity = {}
            for severity in ErrorSeverity:
                count = len([e for e in errors if e.error_severity == severity])
                by_severity[severity.value] = count
            
            # Group by type
            by_type = {}
            for error_type in ErrorType:
                count = len([e for e in errors if e.error_type == error_type])
                if count > 0:
                    by_type[error_type.value] = count
            
            # Get most recent errors
            recent_errors = sorted(errors, key=lambda e: e.occurred_at, reverse=True)[:5]
            
            return {
                "period_days": days,
                "total_errors": total_errors,
                "resolved_errors": resolved_errors,
                "unresolved_errors": unresolved_errors,
                "resolution_rate": (resolved_errors / total_errors * 100) if total_errors > 0 else 0,
                "by_severity": by_severity,
                "by_type": by_type,
                "recent_errors": [
                    {
                        "error_id": e.id,
                        "error_type": e.error_type.value,
                        "error_severity": e.error_severity.value,
                        "error_message": e.error_message[:100],  # Truncate
                        "occurred_at": e.occurred_at,
                        "is_resolved": e.is_resolved
                    }
                    for e in recent_errors
                ]
            }
            
        except Exception as e:
            self.handle_error(e, "get_error_summary")
