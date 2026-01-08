"""
API routes for performance metrics management.
"""

from typing import List, Optional
from uuid import UUID
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session

from app.infrastructure.db.connection import get_session
from app.application.services.metrics_service import MetricsService
from app.infrastructure.db.models.etl_control.performance_metrics import PerformanceMetricRead
from app.interfaces.dependencies import get_current_user
from app.core.response import APIResponse

router = APIRouter(prefix="/metrics", tags=["Performance Metrics"])


@router.get("", response_model=APIResponse[List[PerformanceMetricRead]])
async def list_metrics(
    execution_id: Optional[UUID] = Query(None, description="Filter by execution ID"),
    start_date: Optional[datetime] = Query(None, description="Filter by start date"),
    end_date: Optional[datetime] = Query(None, description="Filter by end date"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: Session = Depends(get_session),
    current_user = Depends(get_current_user)
):
    """
    List performance metrics with optional filters.
    """
    try:
        service = MetricsService(db)
        metrics = await service.get_metrics(
            execution_id=execution_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            offset=offset
        )
        
        return APIResponse.success(
            data=metrics,
            message=f"Retrieved {len(metrics)} performance metrics"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/executions/{execution_id}", response_model=APIResponse[List[PerformanceMetricRead]])
async def get_execution_metrics(
    execution_id: UUID,
    db: Session = Depends(get_session),
    current_user = Depends(get_current_user)
):
    """
    Get all metrics for a specific execution.
    """
    try:
        service = MetricsService(db)
        metrics = await service.get_execution_metrics(execution_id)
        
        return APIResponse.success(
            data=metrics,
            message=f"Retrieved {len(metrics)} metrics for execution {execution_id}"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary", response_model=APIResponse[dict])
async def get_metrics_summary(
    execution_id: Optional[UUID] = Query(None, description="Filter by execution ID"),
    days: int = Query(7, ge=1, le=365, description="Number of days to analyze"),
    db: Session = Depends(get_session),
    current_user = Depends(get_current_user)
):
    """
    Get aggregated metrics summary.
    """
    try:
        service = MetricsService(db)
        summary = await service.get_metric_summary(
            execution_id=execution_id,
            days=days
        )
        
        return APIResponse.success(
            data=summary,
            message="Metrics summary generated successfully"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trends", response_model=APIResponse[List[dict]])
async def get_metrics_trends(
    metric_name: str = Query("records_per_second", description="Metric to analyze"),
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
    interval: str = Query("day", regex="^(hour|day|week)$", description="Grouping interval"),
    db: Session = Depends(get_session),
    current_user = Depends(get_current_user)
):
    """
    Get metric trends over time.
    """
    try:
        service = MetricsService(db)
        trends = await service.get_metric_trends(
            metric_name=metric_name,
            days=days,
            interval=interval
        )
        
        return APIResponse.success(
            data=trends,
            message=f"Retrieved {len(trends)} trend data points"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/system", response_model=APIResponse[dict])
async def get_system_metrics(
    db: Session = Depends(get_session),
    current_user = Depends(get_current_user)
):
    """
    Get current system metrics (CPU, memory, disk).
    """
    try:
        service = MetricsService(db)
        metrics = await service.get_system_metrics()
        
        return APIResponse.success(
            data=metrics,
            message="System metrics retrieved successfully"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
