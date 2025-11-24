"""
API routes for rejected records management.
"""

from typing import List, Optional
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session

from app.infrastructure.db.connection import get_session
from app.services.rejected_records_service import RejectedRecordsService
from app.infrastructure.db.models.raw_data.rejected_records import (
    RejectedRecord,
    RejectedRecordRead,
    RejectedRecordSummary
)
from app.interfaces.dependencies import get_current_user
from app.core.response import APIResponse

router = APIRouter(prefix="/rejected-records", tags=["Rejected Records"])


@router.get("", response_model=APIResponse[List[RejectedRecordRead]])
async def list_rejected_records(
    source_file_id: Optional[UUID] = Query(None, description="Filter by source file ID"),
    batch_id: Optional[str] = Query(None, description="Filter by batch ID"),
    is_resolved: Optional[bool] = Query(None, description="Filter by resolution status"),
    can_retry: Optional[bool] = Query(None, description="Filter by retry capability"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: Session = Depends(get_session),
    current_user = Depends(get_current_user)
):
    """
    List rejected records with optional filters.
    """
    try:
        service = RejectedRecordsService(db)
        records = await service.get_rejected_records(
            source_file_id=source_file_id,
            batch_id=batch_id,
            is_resolved=is_resolved,
            can_retry=can_retry,
            limit=limit,
            offset=offset
        )
        
        return APIResponse.success(
            data=records,
            message=f"Retrieved {len(records)} rejected records"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/files/{file_id}", response_model=APIResponse[List[RejectedRecordRead]])
async def get_file_rejected_records(
    file_id: UUID,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_session),
    current_user = Depends(get_current_user)
):
    """
    Get all rejected records for a specific file.
    """
    try:
        service = RejectedRecordsService(db)
        records = await service.get_rejected_records(
            source_file_id=file_id,
            limit=limit,
            offset=offset
        )
        
        return APIResponse.success(
            data=records,
            message=f"Retrieved {len(records)} rejected records for file {file_id}"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary", response_model=APIResponse[dict])
async def get_rejection_summary(
    source_file_id: Optional[UUID] = Query(None),
    batch_id: Optional[str] = Query(None),
    db: Session = Depends(get_session),
    current_user = Depends(get_current_user)
):
    """
    Get summary statistics for rejected records.
    """
    try:
        service = RejectedRecordsService(db)
        summary = await service.get_rejection_summary(
            source_file_id=source_file_id,
            batch_id=batch_id
        )
        
        return APIResponse.success(
            data=summary,
            message="Rejection summary generated successfully"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/{rejection_id}/resolve", response_model=APIResponse[RejectedRecordRead])
async def mark_record_resolved(
    rejection_id: UUID,
    resolved: bool = Query(True, description="Mark as resolved or unresolved"),
    db: Session = Depends(get_session),
    current_user = Depends(get_current_user)
):
    """
    Mark a rejected record as resolved or unresolved.
    """
    try:
        service = RejectedRecordsService(db)
        record = await service.mark_as_resolved(rejection_id, resolved)
        
        return APIResponse.success(
            data=record,
            message=f"Record marked as {'resolved' if resolved else 'unresolved'}"
        )
        
    except Exception as e:
        raise HTTPException(status_code=404 if "not found" in str(e).lower() else 500, detail=str(e))


@router.post("/{rejection_id}/retry", response_model=APIResponse[RejectedRecordRead])
async def retry_rejected_record(
    rejection_id: UUID,
    db: Session = Depends(get_session),
    current_user = Depends(get_current_user)
):
    """
    Increment retry count for a rejected record.
    """
    try:
        service = RejectedRecordsService(db)
        record = await service.retry_rejected_record(rejection_id)
        
        return APIResponse.success(
            data=record,
            message=f"Retry count incremented to {record.retry_count}"
        )
        
    except Exception as e:
        raise HTTPException(status_code=404 if "not found" in str(e).lower() else 500, detail=str(e))


@router.delete("/{rejection_id}", response_model=APIResponse[bool])
async def delete_rejected_record(
    rejection_id: UUID,
    db: Session = Depends(get_session),
    current_user = Depends(get_current_user)
):
    """
    Delete a rejected record.
    """
    try:
        service = RejectedRecordsService(db)
        success = await service.delete_rejected_record(rejection_id)
        
        return APIResponse.success(
            data=success,
            message="Rejected record deleted successfully"
        )
        
    except Exception as e:
        raise HTTPException(status_code=404 if "not found" in str(e).lower() else 500, detail=str(e))


@router.post("/export", response_model=APIResponse[str])
async def export_rejected_records(
    source_file_id: Optional[UUID] = Query(None),
    batch_id: Optional[str] = Query(None),
    format: str = Query("csv", regex="^(csv|json)$"),
    db: Session = Depends(get_session),
    current_user = Depends(get_current_user)
):
    """
    Export rejected records to file.
    """
    try:
        service = RejectedRecordsService(db)
        filepath = await service.export_rejected_records(
            source_file_id=source_file_id,
            batch_id=batch_id,
            format=format
        )
        
        return APIResponse.success(
            data=filepath,
            message=f"Rejected records exported to {filepath}"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
