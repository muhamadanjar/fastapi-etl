from fastapi import APIRouter, Depends, Query
from sqlmodel import Session
from sqlmodel.ext.asyncio.session import AsyncSession
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

from app.interfaces.dependencies import get_db, get_current_user
from app.infrastructure.db.manager import (
    database_manager,
    get_session_dependency,
    get_async_session_dependency
)
from app.application.services.monitoring_service import MonitoringService
from app.schemas.remote_user import RemoteUserInfo as User
from app.core.response import APIResponse

router = APIRouter()

@router.get("/dashboard")
async def get_dashboard_data(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_async_session_dependency)
):
    """Get dashboard overview data"""
    monitoring_service = MonitoringService(db)
    data = await monitoring_service.get_dashboard_data()
    return APIResponse.success(data=data)

@router.get("/health")
async def health_check(
    db: Session = Depends(get_db)
):
    """System health check"""
    monitoring_service = MonitoringService(db)
    data = await monitoring_service.health_check()
    return APIResponse.success(data=data)

@router.get("/metrics")
async def get_system_metrics(
    period: str = Query("24h", regex="^(1h|6h|24h|7d|30d)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get system performance metrics"""
    monitoring_service = MonitoringService(db)
    data = await monitoring_service.get_system_metrics(period)
    return APIResponse.success(data=data)

@router.get("/job-performance")
async def get_job_performance(
    job_id: Optional[str] = Query(None, description="Filter by specific job"),
    period: str = Query("7d", regex="^(1d|7d|30d)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get job performance statistics"""
    monitoring_service = MonitoringService(db)
    data = await monitoring_service.get_job_performance(job_id, period)
    return APIResponse.success(data=data)

@router.get("/data-quality-trends")
async def get_data_quality_trends(
    period: str = Query("30d", regex="^(7d|30d|90d)$"),
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get data quality trends over time"""
    monitoring_service = MonitoringService(db)
    data = await monitoring_service.get_data_quality_trends(period, entity_type)
    return APIResponse.success(data=data)

@router.get("/active-jobs")
async def get_active_jobs(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get currently running jobs"""
    monitoring_service = MonitoringService(db)
    data = await monitoring_service.get_active_jobs()
    return APIResponse.success(data=data)

@router.get("/recent-errors")
async def get_recent_errors(
    limit: int = Query(50, ge=1, le=500),
    hours: int = Query(24, ge=1, le=168),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get recent system errors"""
    monitoring_service = MonitoringService(db)
    data = await monitoring_service.get_recent_errors(limit, hours)
    return APIResponse.success(data=data)

@router.get("/storage-usage")
async def get_storage_usage(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get storage usage statistics"""
    monitoring_service = MonitoringService(db)
    data = await monitoring_service.get_storage_usage()
    return APIResponse.success(data=data)

@router.get("/alerts")
async def get_system_alerts(
    severity: Optional[str] = Query(None, regex="^(low|medium|high|critical)$"),
    status: Optional[str] = Query(None, regex="^(active|resolved|dismissed)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get system alerts"""
    monitoring_service = MonitoringService(db)
    data = await monitoring_service.get_system_alerts(severity, status)
    return APIResponse.success(data=data)

@router.post("/alerts/{alert_id}/dismiss")
async def dismiss_alert(
    alert_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Dismiss a system alert"""
    monitoring_service = MonitoringService(db)
    data = await monitoring_service.dismiss_alert(alert_id, current_user.id)
    return APIResponse.success(data=data)