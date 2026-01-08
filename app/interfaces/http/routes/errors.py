"""
API routes untuk managing error logs.
"""

from typing import Optional
from uuid import UUID
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

from app.infrastructure.db import get_session_dependency
from app.interfaces.dependencies import get_current_user
from app.application.services.error_service import ErrorService
from app.infrastructure.db.models.etl_control.error_logs import (
    ErrorType,
    ErrorSeverity
)
from pydantic import BaseModel


router = APIRouter(prefix="/errors", )


class ErrorResponse(BaseModel):
    """Response model for error operations"""
    success: bool
    message: str
    data: dict | None = None


class ResolveErrorRequest(BaseModel):
    """Request model for resolving error"""
    resolved_by: UUID | None = None


@router.get("", response_model=ErrorResponse)
async def get_errors(
    job_execution_id: Optional[UUID] = Query(None),
    error_type: Optional[ErrorType] = Query(None),
    error_severity: Optional[ErrorSeverity] = Query(None),
    is_resolved: Optional[bool] = Query(None),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_session_dependency),
    current_user: dict = Depends(get_current_user)
):
    """
    Get error logs with optional filters.
    
    Supports filtering by:
    - job_execution_id: Filter by specific execution
    - error_type: Filter by error type
    - error_severity: Filter by severity level
    - is_resolved: Filter by resolution status
    - start_date/end_date: Filter by date range
    """
    try:
        service = ErrorService(db)
        errors = await service.get_errors(
            job_execution_id=job_execution_id,
            error_type=error_type,
            error_severity=error_severity,
            is_resolved=is_resolved,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            offset=offset
        )
        
        return ErrorResponse(
            success=True,
            message=f"Retrieved {len(errors['errors'])} errors",
            data=errors
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/jobs/{job_id}/errors", response_model=ErrorResponse)
async def get_job_errors(
    job_id: UUID,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_session_dependency),
    current_user: dict = Depends(get_current_user)
):
    """
    Get all errors for a specific job (across all executions).
    """
    try:
        # Get all executions for this job
        from app.infrastructure.db.models.etl_control.job_executions import JobExecution
        from sqlalchemy import select
        
        executions = db.execute(
            select(JobExecution.id).where(JobExecution.job_id == job_id)
        ).scalars().all()
        
        if not executions:
            return ErrorResponse(
                success=True,
                message="No executions found for this job",
                data={"errors": [], "total_count": 0}
            )
        
        # Get errors for all executions
        service = ErrorService(db)
        all_errors = []
        
        for exec_id in executions:
            errors = await service.get_errors(
                job_execution_id=exec_id,
                limit=limit,
                offset=offset
            )
            all_errors.extend(errors['errors'])
        
        return ErrorResponse(
            success=True,
            message=f"Retrieved {len(all_errors)} errors for job",
            data={
                "errors": all_errors[:limit],
                "total_count": len(all_errors)
            }
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/executions/{execution_id}/errors", response_model=ErrorResponse)
async def get_execution_errors(
    execution_id: UUID,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_session_dependency),
    current_user: dict = Depends(get_current_user)
):
    """
    Get all errors for a specific execution.
    """
    try:
        service = ErrorService(db)
        errors = await service.get_errors(
            job_execution_id=execution_id,
            limit=limit,
            offset=offset
        )
        
        return ErrorResponse(
            success=True,
            message=f"Retrieved {len(errors['errors'])} errors for execution",
            data=errors
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.patch("/{error_id}/resolve", response_model=ErrorResponse)
async def resolve_error(
    error_id: UUID,
    request: ResolveErrorRequest,
    db: Session = Depends(get_session_dependency),
    current_user: dict = Depends(get_current_user)
):
    """
    Mark an error as resolved.
    """
    try:
        service = ErrorService(db)
        result = await service.resolve_error(
            error_id=error_id,
            resolved_by=request.resolved_by or current_user.get("user_id")
        )
        
        return ErrorResponse(
            success=True,
            message="Error marked as resolved",
            data=result
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/summary", response_model=ErrorResponse)
async def get_error_summary(
    job_execution_id: Optional[UUID] = Query(None),
    days: int = Query(7, ge=1, le=90),
    db: Session = Depends(get_session_dependency),
    current_user: dict = Depends(get_current_user)
):
    """
    Get error summary statistics.
    
    Returns aggregated error statistics for the specified period.
    """
    try:
        service = ErrorService(db)
        summary = await service.get_error_summary(
            job_execution_id=job_execution_id,
            days=days
        )
        
        return ErrorResponse(
            success=True,
            message=f"Error summary for last {days} days",
            data=summary
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/trends", response_model=ErrorResponse)
async def get_error_trends(
    days: int = Query(30, ge=1, le=365),
    db: Session = Depends(get_session_dependency),
    current_user: dict = Depends(get_current_user)
):
    """
    Get error trends over time.
    
    Returns daily error counts for trend analysis.
    """
    try:
        from datetime import timedelta
        from sqlalchemy import select, func
        from app.infrastructure.db.models.etl_control.error_logs import ErrorLog
        
        start_date = datetime.utcnow() - timedelta(days=days)
        
        # Group errors by date
        stmt = select(
            func.date(ErrorLog.occurred_at).label('date'),
            func.count(ErrorLog.id).label('count'),
            ErrorLog.error_severity
        ).where(
            ErrorLog.occurred_at >= start_date
        ).group_by(
            func.date(ErrorLog.occurred_at),
            ErrorLog.error_severity
        ).order_by(
            func.date(ErrorLog.occurred_at)
        )
        
        results = db.execute(stmt).all()
        
        # Format results
        trends = {}
        for row in results:
            date_str = row.date.isoformat()
            if date_str not in trends:
                trends[date_str] = {"total": 0, "by_severity": {}}
            
            trends[date_str]["total"] += row.count
            trends[date_str]["by_severity"][row.error_severity.value] = row.count
        
        return ErrorResponse(
            success=True,
            message=f"Error trends for last {days} days",
            data={
                "period_days": days,
                "trends": trends
            }
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
