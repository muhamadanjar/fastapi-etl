from fastapi import APIRouter, Depends, HTTPException, status, Query, Body
from sqlmodel import Session
from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime

from app.dependencies import get_db, get_current_user
from app.schemas.job_schemas import (
    JobCreate, JobUpdate, JobResponse, JobExecutionResponse, 
    JobScheduleCreate, JobConfigUpdate
)
from app.services.etl_service import ETLService
from app.models.base import User

router = APIRouter()

@router.post("/", response_model=JobResponse)
async def create_job(
    job_data: JobCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> JobResponse:
    """Create a new ETL job"""
    etl_service = ETLService(db)
    job = await etl_service.create_job(job_data, current_user.id)
    return JobResponse.from_orm(job)

@router.get("/", response_model=List[JobResponse])
async def list_jobs(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    job_type: Optional[str] = Query(None, description="Filter by job type"),
    job_category: Optional[str] = Query(None, description="Filter by job category"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> List[JobResponse]:
    """List all ETL jobs with pagination and filters"""
    etl_service = ETLService(db)
    return await etl_service.list_jobs(skip, limit, job_type, job_category, is_active)

@router.get("/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> JobResponse:
    """Get specific job details"""
    etl_service = ETLService(db)
    job = await etl_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    return JobResponse.from_orm(job)

@router.put("/{job_id}", response_model=JobResponse)
async def update_job(
    job_id: UUID,
    job_data: JobUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> JobResponse:
    """Update job configuration"""
    etl_service = ETLService(db)
    job = await etl_service.update_job(job_id, job_data, current_user.id)
    return JobResponse.from_orm(job)

@router.delete("/{job_id}")
async def delete_job(
    job_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Delete a job"""
    etl_service = ETLService(db)
    await etl_service.delete_job(job_id, current_user.id)
    return {"message": "Job deleted successfully"}

@router.post("/{job_id}/execute")
async def execute_job(
    job_id: UUID,
    parameters: Optional[Dict[str, Any]] = Body(None),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Execute a job manually"""
    etl_service = ETLService(db)
    execution_id = await etl_service.execute_job(job_id, current_user.id, parameters)
    return {"message": "Job execution started", "execution_id": str(execution_id)}

@router.post("/{job_id}/schedule")
async def schedule_job(
    job_id: UUID,
    schedule_data: JobScheduleCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Schedule a job to run automatically"""
    etl_service = ETLService(db)
    await etl_service.schedule_job(job_id, schedule_data, current_user.id)
    return {"message": "Job scheduled successfully"}

@router.delete("/{job_id}/schedule")
async def unschedule_job(
    job_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Remove job schedule"""
    etl_service = ETLService(db)
    await etl_service.unschedule_job(job_id, current_user.id)
    return {"message": "Job unscheduled successfully"}

@router.get("/{job_id}/executions", response_model=List[JobExecutionResponse])
async def get_job_executions(
    job_id: UUID,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    status: Optional[str] = Query(None, description="Filter by execution status"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> List[JobExecutionResponse]:
    """Get job execution history"""
    etl_service = ETLService(db)
    return await etl_service.get_job_executions(job_id, skip, limit, status)

@router.post("/{job_id}/stop")
async def stop_job_execution(
    job_id: UUID,
    execution_id: Optional[UUID] = Query(None, description="Specific execution to stop"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Stop running job execution"""
    etl_service = ETLService(db)
    await etl_service.stop_job_execution(job_id, execution_id, current_user.id)
    return {"message": "Job execution stopped"}

@router.post("/{job_id}/restart")
async def restart_failed_job(
    job_id: UUID,
    execution_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Restart a failed job execution"""
    etl_service = ETLService(db)
    new_execution_id = await etl_service.restart_job_execution(job_id, execution_id, current_user.id)
    return {"message": "Job restarted", "execution_id": str(new_execution_id)}
