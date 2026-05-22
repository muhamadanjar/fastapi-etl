from fastapi import APIRouter, Depends, HTTPException, status, Query, Body
from sqlmodel import Session
from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime

from app.interfaces.dependencies import get_current_user
from app.schemas.job_schemas import (
    JobCreate, JobUpdate, JobResponse, JobExecutionResponse,
    JobScheduleCreate, JobConfigUpdate
)
from app.infrastructure.db.manager import get_session_dependency
from app.application.services.etl_service import ETLService
from app.application.services.transformation_service import TransformationService
from app.application.services.data_quality_service import DataQualityService
from app.schemas.transformation import TransformationRuleCreate, FieldMappingCreate
from app.schemas.data_quality_schema import QualityRuleCreate
from app.schemas.remote_user import RemoteUserInfo as User
from app.core.response import APIResponse
from app.infrastructure.db.models.etl_control.etl_jobs import EtlJob

router = APIRouter()

@router.post("/", response_model=JobResponse)
async def create_job(
    job_data: JobCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> JobResponse:
    """Create a new ETL job"""
    etl_service = ETLService(db)
    job_dict = await etl_service.create_etl_job(job_data.model_dump())
    # Get the full job object from database
    from app.infrastructure.db.models.etl_control.etl_jobs import EtlJob
    job = db.get(EtlJob, job_dict["job_id"])
    return JobResponse.model_validate(job)

@router.get("/", response_model=APIResponse[List[JobResponse]])
async def list_jobs(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    job_type: Optional[str] = Query(None, description="Filter by job type"),
    job_category: Optional[str] = Query(None, description="Filter by job category"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> APIResponse[List[JobResponse]]:
    """List all ETL jobs with pagination and filters"""
    etl_service = ETLService(db)
    jobs_data = await etl_service.get_jobs_list(job_type, is_active)
    jobs_result = []
    for job in jobs_data:
        job_data  = JobResponse.model_validate({
            "job_id": job.get('job_id'),
            "job_name": job.get('job_name'),
            "job_type": job.get('job_type'),
            "job_category": job.get('job_category'),
            "source_type": job.get('source_type'),
            "is_active": job.get('is_active'),
            "created_at": job.get('created_at'),
            "updated_at": job.get('updated_at'),
            "created_by": job.get('created_by'),
            "updated_by": job.get('updated_by'),
        })
        jobs_result.append(job_data) 
    return APIResponse(
        success=True,
        message="Jobs retrieved successfully", 
        data=jobs_result
    )

@router.get("/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> JobResponse:
    """Get specific job details"""
    from app.infrastructure.db.models.etl_control.etl_jobs import EtlJob
    job = db.get(EtlJob, job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    return JobResponse.model_validate(job)

@router.put("/{job_id}", response_model=JobResponse)
async def update_job(
    job_id: UUID,
    job_data: JobUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> JobResponse:
    """Update job configuration"""
    from app.infrastructure.db.models.etl_control.etl_jobs import EtlJob
    etl_service = ETLService(db)
    await etl_service.update_job(job_id, job_data.model_dump(exclude_unset=True))
    # Get updated job from database
    job = db.get(EtlJob, job_id)
    return JobResponse.model_validate(job)

@router.delete("/{job_id}")
async def delete_job(
    job_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
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
    db: Session = Depends(get_session_dependency)
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
    db: Session = Depends(get_session_dependency)
) -> Dict[str, str]:
    """Schedule a job to run automatically"""
    etl_service = ETLService(db)
    await etl_service.schedule_job(job_id, schedule_data, current_user.id)
    return {"message": "Job scheduled successfully"}

@router.delete("/{job_id}/schedule")
async def unschedule_job(
    job_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
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
    db: Session = Depends(get_session_dependency)
) -> List[JobExecutionResponse]:
    """Get job execution history"""
    etl_service = ETLService(db)
    return await etl_service.get_job_executions(job_id, skip, limit, status)

@router.post("/{job_id}/stop")
async def stop_job_execution(
    job_id: UUID,
    execution_id: Optional[UUID] = Query(None, description="Specific execution to stop"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
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
    db: Session = Depends(get_session_dependency)
) -> Dict[str, str]:
    """Restart a failed job execution"""
    etl_service = ETLService(db)
    new_execution_id = await etl_service.restart_job_execution(job_id, execution_id, current_user.id)
    return {"message": "Job restarted", "execution_id": str(new_execution_id)}

@router.post("/{job_id}/transformation-rules")
async def create_transformation_rule(
    job_id: UUID,
    rule_data: TransformationRuleCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> Dict[str, Any]:
    """Create a transformation rule for a job"""
    job = db.get(EtlJob, job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    transformation_service = TransformationService(db)
    rule_dict = rule_data.model_dump()
    rule_dict["job_id"] = job_id
    return await transformation_service.create_transformation_rule(rule_dict)

@router.get("/{job_id}/transformation-rules")
async def get_transformation_rules(
    job_id: UUID,
    source_format: Optional[str] = Query(None),
    target_format: Optional[str] = Query(None),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> List[Dict[str, Any]]:
    """Get transformation rules for a job"""
    job = db.get(EtlJob, job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    transformation_service = TransformationService(db)
    return await transformation_service.get_transformation_rules(source_format, target_format, job_id)

@router.post("/{job_id}/field-mappings")
async def create_field_mapping(
    job_id: UUID,
    mapping_data: FieldMappingCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> Dict[str, Any]:
    """Create a field mapping for a job"""
    job = db.get(EtlJob, job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    transformation_service = TransformationService(db)
    mapping_dict = mapping_data.model_dump()
    mapping_dict["job_id"] = job_id
    return await transformation_service.create_field_mapping(mapping_dict)

@router.get("/{job_id}/field-mappings")
async def get_field_mappings(
    job_id: UUID,
    source_entity: Optional[str] = Query(None),
    target_entity: Optional[str] = Query(None),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> List[Dict[str, Any]]:
    """Get field mappings for a job"""
    job = db.get(EtlJob, job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    transformation_service = TransformationService(db)
    return await transformation_service.get_field_mappings(source_entity, target_entity, job_id)

@router.post("/{job_id}/quality-rules")
async def create_quality_rule(
    job_id: UUID,
    rule_data: QualityRuleCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> Dict[str, Any]:
    """Create a quality rule for a job"""
    job = db.get(EtlJob, job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    data_quality_service = DataQualityService(db)
    rule_dict = rule_data.model_dump()
    rule_dict["job_id"] = job_id
    return await data_quality_service.create_quality_rule(rule_dict)

@router.get("/{job_id}/quality-rules")
async def get_quality_rules(
    job_id: UUID,
    entity_type: Optional[str] = Query(None),
    rule_type: Optional[str] = Query(None),
    is_active: Optional[bool] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> List[Dict[str, Any]]:
    """Get quality rules for a job"""
    job = db.get(EtlJob, job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    data_quality_service = DataQualityService(db)
    return await data_quality_service.get_quality_rules(entity_type, rule_type, is_active, skip, limit, job_id)

@router.get("/{job_id}/schedule")
async def get_job_schedule(
    job_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> Dict[str, Any]:
    """Get job schedule configuration"""
    job = db.get(EtlJob, job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    return {"schedule_expression": job.schedule_expression}
