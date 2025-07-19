from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, status, Query
from sqlmodel import Session
from typing import List, Optional, Dict, Any
from uuid import UUID

from app.interfaces.dependencies import get_db, get_current_user
from app.schemas.file_upload import FileUploadResponse, FileListResponse, FileDetailResponse
from app.services.file_service import FileService
from app.infrastructure.db.models.user import User

router = APIRouter()

@router.post("/upload", response_model=FileUploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    source_system: str = Query(..., description="Source system name"),
    batch_id: Optional[str] = Query(None, description="Batch ID for grouping files"),
    metadata: Optional[str] = Query(None, description="Additional metadata as JSON string"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> FileUploadResponse:
    """Upload a file for ETL processing"""
    file_service = FileService(db)
    
    # Validate file type
    allowed_types = ['text/csv', 'application/vnd.ms-excel', 
                    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    'application/json', 'application/xml', 'text/xml']
    
    if file.content_type not in allowed_types:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File type {file.content_type} not supported"
        )
    
    return await file_service.upload_file(file, source_system, batch_id, metadata, current_user.id)

@router.get("/", response_model=List[FileListResponse])
async def list_files(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    file_type: Optional[str] = Query(None, description="Filter by file type"),
    source_system: Optional[str] = Query(None, description="Filter by source system"),
    status: Optional[str] = Query(None, description="Filter by processing status"),
    batch_id: Optional[str] = Query(None, description="Filter by batch ID"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> List[FileListResponse]:
    """List uploaded files with pagination and filters"""
    file_service = FileService(db)
    return await file_service.list_files(skip, limit, file_type, source_system, status, batch_id)

@router.get("/{file_id}", response_model=FileDetailResponse)
async def get_file_detail(
    file_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
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
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Start processing a specific file"""
    file_service = FileService(db)
    job_id = await file_service.start_file_processing(file_id, current_user.id)
    return {"message": "File processing started", "job_id": str(job_id)}

@router.delete("/{file_id}")
async def delete_file(
    file_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Delete a file and its associated data"""
    file_service = FileService(db)
    await file_service.delete_file(file_id, current_user.id)
    return {"message": "File deleted successfully"}

@router.get("/{file_id}/download")
async def download_file(
    file_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Download original file"""
    file_service = FileService(db)
    return await file_service.download_file(file_id)

@router.get("/{file_id}/preview")
async def preview_file_data(
    file_id: UUID,
    rows: int = Query(10, ge=1, le=100, description="Number of rows to preview"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Preview file data (first N rows)"""
    file_service = FileService(db)
    return await file_service.preview_file_data(file_id, rows)

@router.post("/batch-upload")
async def batch_upload(
    files: List[UploadFile] = File(...),
    source_system: str = Query(..., description="Source system name"),
    batch_id: Optional[str] = Query(None, description="Batch ID for grouping files"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Upload multiple files in a batch"""
    file_service = FileService(db)
    return await file_service.batch_upload(files, source_system, batch_id, current_user.id)
