from uuid import UUID
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, status, Query
from sqlmodel import Session
from typing import List, Optional, Dict, Any

from app.interfaces.dependencies import get_current_user
from app.schemas.file_upload import FileUploadResponse, FileListResponse, FileDetailResponse
from app.services.file_service import FileService
from app.infrastructure.db.models.auth import User
from app.infrastructure.db.connection import get_session_dependency

router = APIRouter()

@router.post("/upload", response_model=FileUploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    source_system: str = Query(..., description="Source system name"),
    batch_id: Optional[str] = Query(None, description="Batch ID for grouping files"),
    metadata: Optional[str] = Query(None, description="Additional metadata as JSON string"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> FileUploadResponse:
    """Upload a file for ETL processing"""
    file_service = FileService(db)
    
    # Validate file type
    allowed_types = [
        'text/csv', 
        'application/vnd.ms-excel', 
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'application/json', 
        'application/xml', 
        'text/xml'
    ]
    
    if file.content_type not in allowed_types:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File type {file.content_type} not supported. Allowed types: {', '.join(allowed_types)}"
        )
    
    return await file_service.upload_file(
        file=file,
        source_system=source_system,
        batch_id=batch_id,
        metadata=metadata,
        user_id=current_user.id
    )

@router.get("/", response_model=FileListResponse)
async def list_files(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    file_type: Optional[str] = Query(None, description="Filter by file type (CSV, EXCEL, JSON, XML)"),
    source_system: Optional[str] = Query(None, description="Filter by source system"),
    status: Optional[str] = Query(None, description="Filter by processing status (PENDING, PROCESSING, COMPLETED, FAILED)"),
    batch_id: Optional[str] = Query(None, description="Filter by batch ID"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> FileListResponse:
    """List uploaded files with pagination and filters"""
    file_service = FileService(db)
    return await file_service.get_file_list(
        skip=skip,
        limit=limit,
        file_type=file_type,
        source_system=source_system,
        status=status,
        batch_id=batch_id
    )

@router.get("/{file_id}", response_model=FileDetailResponse)
async def get_file_detail(
    file_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> FileDetailResponse:
    """Get detailed information about a specific file"""
    file_service = FileService(db)
    file_detail = await file_service.get_file_detail(file_id)
    
    if not file_detail:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found"
        )
    
    return file_detail

@router.post("/{file_id}/process")
async def process_file(
    file_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> Dict[str, str]:
    """Start processing a specific file"""
    try:
        file_service = FileService(db)
        task_id = await file_service.start_file_processing(file_id, current_user.id)
        
        return {
            "message": "File processing started",
            "file_id": str(file_id),
            "task_id": task_id,
            "status": "processing"
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.delete("/{file_id}")
async def delete_file(
    file_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> Dict[str, str]:
    """Delete a file and its associated data"""
    try:
        file_service = FileService(db)
        success = await file_service.delete_file(file_id, current_user.id)
        
        if success:
            return {"message": "File deleted successfully", "file_id": str(file_id)}
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to delete file"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.get("/{file_id}/download")
async def download_file(
    file_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
):
    """Download original file"""
    try:
        file_service = FileService(db)
        return await file_service.download_file(file_id)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )

@router.get("/{file_id}/preview")
async def preview_file_data(
    file_id: UUID,
    rows: int = Query(10, ge=1, le=100, description="Number of rows to preview"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> Dict[str, Any]:
    """Preview file data (first N rows)"""
    try:
        file_service = FileService(db)
        return await file_service.preview_file_data(file_id, rows)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/batch-upload")
async def batch_upload(
    files: List[UploadFile] = File(...),
    source_system: str = Query(..., description="Source system name"),
    batch_id: Optional[str] = Query(None, description="Batch ID for grouping files"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> Dict[str, Any]:
    """Upload multiple files in a batch"""
    
    # Validate all files first
    allowed_types = [
        'text/csv', 
        'application/vnd.ms-excel', 
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'application/json', 
        'application/xml', 
        'text/xml'
    ]
    
    for file in files:
        if file.content_type not in allowed_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File {file.filename} has unsupported type {file.content_type}"
            )
    
    file_service = FileService(db)
    return await file_service.batch_upload(
        files=files,
        source_system=source_system,
        batch_id=batch_id,
        user_id=current_user.id
    )

@router.get("/{file_id}/processing-status")
async def get_processing_status(
    file_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> Dict[str, Any]:
    """Get current processing status of a file"""
    try:
        file_service = FileService(db)
        file_detail = await file_service.get_file_detail(file_id)
        
        if not file_detail:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="File not found"
            )
        
        return {
            "file_id": file_id,
            "file_name": file_detail.file.file_name,
            "processing_status": file_detail.file.processing_status,
            "records_count": file_detail.validation_result.total_records,
            "valid_records": file_detail.validation_result.valid_records,
            "invalid_records": file_detail.validation_result.invalid_records,
            "upload_date": file_detail.file.upload_date
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/{file_id}/reprocess")
async def reprocess_file(
    file_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> Dict[str, str]:
    """Reprocess a file (useful if previous processing failed)"""
    try:
        file_service = FileService(db)
        task_id = await file_service.start_file_processing(file_id, current_user.id)
        
        return {
            "message": "File reprocessing started",
            "file_id": str(file_id),
            "task_id": task_id,
            "status": "processing"
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )