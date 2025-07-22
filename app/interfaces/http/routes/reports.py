from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from fastapi.responses import FileResponse
from sqlmodel import Session
from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime

from app.interfaces.dependencies import get_db, get_current_user
from app.schemas.response_schemas import ReportResponse, ReportRequest
from app.services.report_service import ReportService
from app.infrastructure.db.models import User

router = APIRouter()

@router.post("/generate", response_model=ReportResponse)
async def generate_report(
    report_request: ReportRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> ReportResponse:
    """Generate a new report"""
    report_service = ReportService(db)
    report = await report_service.generate_report(report_request, current_user.id, background_tasks)
    return ReportResponse.from_orm(report)

@router.get("/", response_model=List[ReportResponse])
async def list_reports(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    report_type: Optional[str] = Query(None, description="Filter by report type"),
    status: Optional[str] = Query(None, description="Filter by status"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> List[ReportResponse]:
    """List generated reports"""
    report_service = ReportService(db)
    return await report_service.list_reports(skip, limit, report_type, status, current_user.id)

@router.get("/{report_id}", response_model=ReportResponse)
async def get_report(
    report_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> ReportResponse:
    """Get specific report details"""
    report_service = ReportService(db)
    report = await report_service.get_report(report_id, current_user.id)
    if not report:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Report not found"
        )
    return ReportResponse.from_orm(report)

@router.get("/{report_id}/download")
async def download_report(
    report_id: UUID,
    format: str = Query("pdf", regex="^(pdf|excel|csv)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> FileResponse:
    """Download generated report"""
    report_service = ReportService(db)
    file_path = await report_service.download_report(report_id, format, current_user.id)
    return FileResponse(
        path=file_path,
        media_type='application/octet-stream',
        filename=f"report_{report_id}.{format}"
    )

@router.delete("/{report_id}")
async def delete_report(
    report_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Delete a report"""
    report_service = ReportService(db)
    await report_service.delete_report(report_id, current_user.id)
    return {"message": "Report deleted successfully"}

@router.get("/templates/")
async def get_report_templates(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> List[Dict[str, Any]]:
    """Get available report templates"""
    report_service = ReportService(db)
    return await report_service.get_report_templates()

@router.get("/dashboard/summary")
async def get_dashboard_summary(
    period: str = Query("30d", regex="^(7d|30d|90d)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Get dashboard summary data"""
    report_service = ReportService(db)
    return await report_service.get_dashboard_summary(period)

@router.get("/analytics/data-processing")
async def get_data_processing_analytics(
    period: str = Query("30d", regex="^(7d|30d|90d)$"),
    granularity: str = Query("daily", regex="^(hourly|daily|weekly)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Get data processing analytics"""
    report_service = ReportService(db)
    return await report_service.get_data_processing_analytics(period, granularity)

@router.get("/analytics/data-quality")
async def get_data_quality_analytics(
    period: str = Query("30d", regex="^(7d|30d|90d)$"),
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Get data quality analytics"""
    report_service = ReportService(db)
    return await report_service.get_data_quality_analytics(period, entity_type)

@router.get("/analytics/entity-growth")
async def get_entity_growth_analytics(
    period: str = Query("90d", regex="^(30d|90d|365d)$"),
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Get entity growth analytics"""
    report_service = ReportService(db)
    return await report_service.get_entity_growth_analytics(period, entity_type)

@router.get("/analytics/performance")
async def get_performance_analytics(
    period: str = Query("30d", regex="^(7d|30d|90d)$"),
    job_type: Optional[str] = Query(None, description="Filter by job type"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Get system performance analytics"""
    report_service = ReportService(db)
    return await report_service.get_performance_analytics(period, job_type)

@router.post("/schedule")
async def schedule_report(
    report_request: ReportRequest,
    schedule_expression: str = Query(..., description="Cron expression for scheduling"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Schedule automatic report generation"""
    report_service = ReportService(db)
    schedule_id = await report_service.schedule_report(report_request, schedule_expression, current_user.id)
    return {"message": "Report scheduled successfully", "schedule_id": str(schedule_id)}

@router.get("/export/entities")
async def export_entities(
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    format: str = Query("csv", regex="^(csv|excel|json)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> FileResponse:
    """Export entities data"""
    report_service = ReportService(db)
    file_path = await report_service.export_entities(entity_type, format, current_user.id)
    return FileResponse(
        path=file_path,
        media_type='application/octet-stream',
        filename=f"entities_export.{format}"
    )

@router.get("/export/job-executions")
async def export_job_executions(
    job_id: Optional[UUID] = Query(None, description="Filter by job ID"),
    period: str = Query("30d", regex="^(7d|30d|90d)$"),
    format: str = Query("csv", regex="^(csv|excel)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> FileResponse:
    """Export job execution data"""
    report_service = ReportService(db)
    file_path = await report_service.export_job_executions(job_id, period, format, current_user.id)
    return FileResponse(
        path=file_path,
        media_type='application/octet-stream',
        filename=f"job_executions_export.{format}"
    )

@router.get("/export/data-lineage")
async def export_data_lineage(
    entity_id: Optional[UUID] = Query(None, description="Filter by entity ID"),
    format: str = Query("csv", regex="^(csv|excel|json)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> FileResponse:
    """Export data lineage information"""
    report_service = ReportService(db)
    file_path = await report_service.export_data_lineage(entity_id, format, current_user.id)
    return FileResponse(
        path=file_path,
        media_type='application/octet-stream',
        filename=f"data_lineage_export.{format}"
    )