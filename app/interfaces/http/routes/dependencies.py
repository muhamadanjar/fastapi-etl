"""
API routes untuk managing job dependencies.
"""

from typing import List
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.infrastructure.db.connection import get_session_dependency
from app.interfaces.dependencies import get_current_user
from app.application.services.dependency_service import DependencyService, DependencyError
from app.infrastructure.db.models.etl_control.job_dependencies import (
    DependencyType,
    JobDependencyCreate,
    JobDependencyRead
)
from pydantic import BaseModel


router = APIRouter(prefix="/jobs", )


class AddDependencyRequest(BaseModel):
    """Request model for adding dependency"""
    parent_job_id: UUID
    dependency_type: DependencyType = DependencyType.SUCCESS
    description: str | None = None


class DependencyResponse(BaseModel):
    """Response model for dependency operations"""
    success: bool
    message: str
    data: dict | None = None


@router.get("/{job_id}/dependencies", response_model=DependencyResponse)
async def get_job_dependencies(
    job_id: UUID,
    include_inactive: bool = False,
    db: Session = Depends(get_session_dependency),
    current_user: dict = Depends(get_current_user)
):
    """
    Get all dependencies for a job.
    
    Returns both parent dependencies (jobs this job depends on) and
    child dependencies (jobs that depend on this job).
    """
    try:
        service = DependencyService(db)
        dependencies = await service.get_job_dependencies(job_id, include_inactive)
        
        return DependencyResponse(
            success=True,
            message="Dependencies retrieved successfully",
            data=dependencies
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/{job_id}/dependencies", response_model=DependencyResponse, status_code=status.HTTP_201_CREATED)
async def add_job_dependency(
    job_id: UUID,
    request: AddDependencyRequest,
    db: Session = Depends(get_session_dependency),
    current_user: dict = Depends(get_current_user)
):
    """
    Add a dependency to a job.
    
    The specified job (job_id) will depend on the parent_job_id.
    This means the parent job must complete before this job can execute.
    """
    try:
        service = DependencyService(db)
        result = await service.add_dependency(
            parent_job_id=request.parent_job_id,
            child_job_id=job_id,
            dependency_type=request.dependency_type,
            description=request.description
        )
        
        return DependencyResponse(
            success=True,
            message="Dependency added successfully",
            data=result
        )
    except DependencyError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.delete("/{job_id}/dependencies/{dependency_id}", response_model=DependencyResponse)
async def remove_job_dependency(
    job_id: UUID,
    dependency_id: UUID,
    db: Session = Depends(get_session_dependency),
    current_user: dict = Depends(get_current_user)
):
    """
    Remove a dependency from a job.
    
    This will mark the dependency as inactive.
    """
    try:
        service = DependencyService(db)
        success = await service.remove_dependency(dependency_id)
        
        return DependencyResponse(
            success=success,
            message="Dependency removed successfully",
            data={"dependency_id": dependency_id}
        )
    except DependencyError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/{job_id}/dependencies/check", response_model=DependencyResponse)
async def check_dependencies(
    job_id: UUID,
    db: Session = Depends(get_session_dependency),
    current_user: dict = Depends(get_current_user)
):
    """
    Check if all dependencies for a job are met.
    
    Returns detailed information about which dependencies are met
    and which are not.
    """
    try:
        service = DependencyService(db)
        status_info = await service.check_dependencies_met(job_id)
        
        return DependencyResponse(
            success=True,
            message=status_info["message"],
            data=status_info
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/{job_id}/dependency-tree", response_model=DependencyResponse)
async def get_dependency_tree(
    job_id: UUID,
    max_depth: int = 10,
    db: Session = Depends(get_session_dependency),
    current_user: dict = Depends(get_current_user)
):
    """
    Get the full dependency tree for a job.
    
    Shows all parent dependencies recursively up to max_depth levels.
    """
    try:
        service = DependencyService(db)
        tree = await service.get_dependency_tree(job_id, max_depth)
        
        return DependencyResponse(
            success=True,
            message="Dependency tree retrieved successfully",
            data=tree
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/executable", response_model=DependencyResponse)
async def get_executable_jobs(
    db: Session = Depends(get_session_dependency),
    current_user: dict = Depends(get_current_user)
):
    """
    Get list of jobs that can be executed.
    
    Returns all active jobs where all dependencies are met.
    """
    try:
        service = DependencyService(db)
        jobs = await service.get_executable_jobs()
        
        return DependencyResponse(
            success=True,
            message=f"Found {len(jobs)} executable jobs",
            data={"executable_jobs": jobs, "total": len(jobs)}
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
