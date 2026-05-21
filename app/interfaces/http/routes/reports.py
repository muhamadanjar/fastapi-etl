from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from fastapi.responses import FileResponse
from sqlmodel import Session
from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime

from app.interfaces.dependencies import get_db, get_current_user
from app.schemas.response_schemas import ReportResponse, ReportRequest
from app.application.services.report_service import ReportService
from app.schemas.remote_user import RemoteUserInfo as User
from app.core.response import APIResponse
from app.core.exceptions import ServiceError

router = APIRouter()

@router.post("/generate")
async def generate_report(
    report_request: ReportRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Generate a new report"""
    try:
        report_service = ReportService(db)
        report = await report_service.generate_report(report_request, current_user.id, background_tasks)
        return APIResponse.success(data=ReportResponse.from_orm(report))
    except ServiceError:
        raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Not yet implemented")

@router.get("/templates")
async def get_report_templates(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get available report templates"""
    try:
        report_service = ReportService(db)
        result = await report_service.get_report_templates()
        return APIResponse.success(data=result)
    except ServiceError:
        raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Not yet implemented")

@router.get("/dashboard/summary")
async def get_dashboard_summary(
    period: str = Query("30d", regex="^(7d|30d|90d)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get dashboard summary data"""
    try:
        report_service = ReportService(db)
        result = await report_service.get_dashboard_summary(period)
        return APIResponse.success(data=result)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@router.get("/analytics/data-processing")
async def get_data_processing_analytics(
    period: str = Query("30d", regex="^(7d|30d|90d)$"),
    granularity: str = Query("daily", regex="^(hourly|daily|weekly)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get data processing analytics"""
    try:
        report_service = ReportService(db)
        result = await report_service.get_data_processing_analytics(period, granularity)
        return APIResponse.success(data=result)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@router.get("/analytics/data-quality")
async def get_data_quality_analytics(
    period: str = Query("30d", regex="^(7d|30d|90d)$"),
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get data quality analytics"""
    try:
        report_service = ReportService(db)
        result = await report_service.get_data_quality_analytics(period, entity_type)
        return APIResponse.success(data=result)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@router.get("/analytics/entity-growth")
async def get_entity_growth_analytics(
    period: str = Query("90d", regex="^(30d|90d|365d)$"),
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get entity growth analytics"""
    try:
        report_service = ReportService(db)
        result = await report_service.get_entity_growth_analytics(period, entity_type)
        return APIResponse.success(data=result)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@router.get("/analytics/performance")
async def get_performance_analytics(
    period: str = Query("30d", regex="^(7d|30d|90d)$"),
    job_type: Optional[str] = Query(None, description="Filter by job type"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get system performance analytics"""
    try:
        report_service = ReportService(db)
        result = await report_service.get_performance_analytics(period, job_type)
        return APIResponse.success(data=result)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@router.post("/schedule")
async def schedule_report(
    report_request: ReportRequest,
    schedule_expression: str = Query(..., description="Cron expression for scheduling"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Schedule automatic report generation"""
    try:
        report_service = ReportService(db)
        schedule_id = await report_service.schedule_report(report_request, schedule_expression, current_user.id)
        return APIResponse.success(data={"schedule_id": str(schedule_id)})
    except ServiceError:
        raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Not yet implemented")

@router.get("/export/entities")
async def export_entities(
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    format: str = Query("csv", regex="^(csv|excel|json)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Export entities data"""
    try:
        report_service = ReportService(db)
        file_path = await report_service.export_entities(entity_type, format, current_user.id)
        return FileResponse(
            path=file_path,
            media_type='application/octet-stream',
            filename=f"entities_export.{format}"
        )
    except ServiceError:
        raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Not yet implemented")

@router.get("/export/job-executions")
async def export_job_executions(
    job_id: Optional[UUID] = Query(None, description="Filter by job ID"),
    period: str = Query("30d", regex="^(7d|30d|90d)$"),
    format: str = Query("csv", regex="^(csv|excel)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Export job execution data"""
    try:
        report_service = ReportService(db)
        file_path = await report_service.export_job_executions(job_id, period, format, current_user.id)
        return FileResponse(
            path=file_path,
            media_type='application/octet-stream',
            filename=f"job_executions_export.{format}"
        )
    except ServiceError:
        raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Not yet implemented")

@router.get("/export/data-lineage")
async def export_data_lineage(
    entity_id: Optional[UUID] = Query(None, description="Filter by entity ID"),
    format: str = Query("csv", regex="^(csv|excel|json)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Export data lineage information"""
    try:
        report_service = ReportService(db)
        file_path = await report_service.export_data_lineage(entity_id, format, current_user.id)
        return FileResponse(
            path=file_path,
            media_type='application/octet-stream',
            filename=f"data_lineage_export.{format}"
        )
    except ServiceError:
        raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Not yet implemented")

@router.get("/")
async def list_reports(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    report_type: Optional[str] = Query(None, description="Filter by report type"),
    status: Optional[str] = Query(None, description="Filter by status"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List generated reports"""
    try:
        report_service = ReportService(db)
        result = await report_service.list_reports(skip, limit, report_type, status, current_user.id)
        return APIResponse.success(data=result)
    except ServiceError:
        raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Not yet implemented")

@router.get("/{report_id}")
async def get_report(
    report_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get specific report details"""
    try:
        report_service = ReportService(db)
        report = await report_service.get_report(report_id, current_user.id)
        if not report:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Report not found"
            )
        return APIResponse.success(data=ReportResponse.from_orm(report))
    except ServiceError:
        raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Not yet implemented")

@router.get("/{report_id}/download")
async def download_report(
    report_id: UUID,
    format: str = Query("pdf", regex="^(pdf|excel|csv)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Download generated report"""
    try:
        report_service = ReportService(db)
        file_path = await report_service.download_report(report_id, format, current_user.id)
        return FileResponse(
            path=file_path,
            media_type='application/octet-stream',
            filename=f"report_{report_id}.{format}"
        )
    except ServiceError:
        raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Not yet implemented")

@router.delete("/{report_id}")
async def delete_report(
    report_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Delete a report"""
    try:
        report_service = ReportService(db)
        await report_service.delete_report(report_id, current_user.id)
        return APIResponse.success(data={"message": "Report deleted successfully"})
    except ServiceError:
        raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Not yet implemented")