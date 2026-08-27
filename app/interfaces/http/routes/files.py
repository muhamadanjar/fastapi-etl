from uuid import UUID, uuid4
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, status, Query, Request, Header, Path
from sqlmodel import Session, select
from typing import List, Optional, Dict, Any

from app.interfaces.dependencies import get_current_user
from app.schemas.file_upload import (
    ArtifactFileRegistrationRequest,
    FileUploadResponse,
    FileListResponse,
    FileDetailResponse,
)
from app.schemas.upload_session import (
    InitUploadSessionRequest,
    InitUploadSessionResponse,
    ChunkUploadResponse,
    UploadSessionStatusResponse,
)
from app.application.services.file_service import FileService
from app.core.exceptions import BadRequestError, NotFoundError
from app.schemas.remote_user import RemoteUserInfo as User
from app.infrastructure.db.manager import get_session_dependency
from app.infrastructure.db.models.raw_data.file_registry import FileRegistry
from app.infrastructure.upload_artifact_client import UploadArtifactClient
from app.core.enums import FileTypeEnum, ProcessingStatus
from pathlib import Path as FilePath
from datetime import datetime

router = APIRouter()


_ARTIFACT_FILE_TYPES = {
    ".csv": FileTypeEnum.CSV,
    ".xls": FileTypeEnum.EXCEL,
    ".xlsx": FileTypeEnum.EXCEL,
    ".json": FileTypeEnum.JSON,
    ".xml": FileTypeEnum.XML,
}

# Exact path routes first (highest specificity)
@router.post("/upload/session", response_model=InitUploadSessionResponse)
async def init_upload_session(
    request: InitUploadSessionRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency),
) -> InitUploadSessionResponse:
    """Initiate a chunked upload session for large files"""
    service = FileService(db)
    return await service.init_upload_session(request, user_id=current_user.id)


@router.post("/artifacts", response_model=FileUploadResponse, status_code=status.HTTP_201_CREATED)
async def register_upload_artifact(
    body: ArtifactFileRegistrationRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency),
) -> FileUploadResponse:
    """Register an available upload_api artifact as an ETL source without copying it permanently."""
    existing = db.exec(
        select(FileRegistry).where(FileRegistry.artifact_id == body.artifact_id)
    ).first()
    if existing:
        if existing.created_by != current_user.id:
            raise NotFoundError(message="Upload artifact registration not found")
        return FileUploadResponse(
            file_id=existing.id,
            file_name=existing.file_name,
            file_type=existing.file_type,
            file_size=existing.file_size,
            batch_id=existing.batch_id,
            processing_status=existing.processing_status,
            upload_date=existing.upload_date,
        )

    client = UploadArtifactClient()
    # A stable future registry ID is needed as the idempotent lease reference.
    registry_id = uuid4()
    lease = await client.acquire_lease(body.artifact_id, body.grant_id, str(registry_id))
    try:
        artifact = await client.metadata(body.artifact_id)
        suffix = FilePath(artifact["filename"]).suffix.lower()
        file_type = _ARTIFACT_FILE_TYPES.get(suffix)
        if not file_type:
            raise BadRequestError(message=f"Artifact file extension {suffix or '(none)'} is not supported by ETL")
        record = FileRegistry(
            id=registry_id,
            file_name=artifact["filename"],
            file_path=f"artifact://{body.artifact_id}",
            artifact_id=body.artifact_id,
            artifact_lease_id=lease["lease_id"],
            file_type=file_type,
            file_size=artifact["size_bytes"],
            source_system=body.source_system,
            batch_id=body.batch_id,
            created_by=current_user.id,
            processing_status=ProcessingStatus.PENDING,
            file_metadata={**(body.metadata or {}), "upload_artifact": artifact},
        )
        db.add(record)
        db.commit()
        db.refresh(record)
    except Exception:
        await client.release_lease(body.artifact_id, lease["lease_id"])
        raise
    return FileUploadResponse(
        file_id=record.id,
        file_name=record.file_name,
        file_type=record.file_type,
        file_size=record.file_size,
        batch_id=record.batch_id,
        processing_status=record.processing_status,
        upload_date=record.upload_date,
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
        'application/wps-office.xls',
        'application/json',
        'application/xml',
        'text/xml'
    ]

    for file in files:
        if file.content_type not in allowed_types:
            raise BadRequestError(message=f"File {file.filename} has unsupported type {file.content_type}")

    file_service = FileService(db)
    return await file_service.batch_upload(
        files=files,
        source_system=source_system,
        batch_id=batch_id,
        user_id=current_user.id
    )


# Routes with path parameters (more specific before generic)
@router.get("/upload/session/{session_id}", response_model=UploadSessionStatusResponse)
async def get_upload_session_status(
    session_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency),
) -> UploadSessionStatusResponse:
    """Get current status of upload session (for resume capability)"""
    service = FileService(db)
    return await service.get_upload_session_status(session_id)


@router.post("/upload/{session_id}/{chunk_index}", response_model=ChunkUploadResponse)
async def chunk_upload(
    session_id: UUID = Path(..., description="Upload session ID"),
    chunk_index: int = Path(..., ge=0, description="Chunk index (0-based)"),
    request: Request = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency),
) -> ChunkUploadResponse:
    """Upload a single chunk by index. Backend calculates position from chunk_index."""
    chunk_data = await request.body()
    service = FileService(db)
    return await service.upload_chunk(session_id, chunk_index, chunk_data)


# Generic routes last (lowest specificity)
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
        'application/wps-office.xls',
        'application/json',
        'application/xml',
        'text/xml'
    ]

    if file.content_type not in allowed_types:
        raise BadRequestError(message=f"File type {file.content_type} not supported. Allowed types: {', '.join(allowed_types)}")

    return await file_service.upload_file(
        file=file,
        source_system=source_system,
        batch_id=batch_id,
        metadata=metadata,
        user_id=current_user.id
    )


@router.get("", response_model=FileListResponse)
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


# File ID routes: specific suffixes before generic
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
        raise NotFoundError(message=str(e))


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
        raise BadRequestError(message=str(e))


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
            raise NotFoundError(message="File not found")

        return {
            "file_id": file_id,
            "file_name": file_detail.file.file_name,
            "processing_status": file_detail.file.processing_status,
            "records_count": file_detail.validation_result.total_records,
            "valid_records": file_detail.validation_result.valid_records,
            "invalid_records": file_detail.validation_result.invalid_records,
            "upload_date": file_detail.file.upload_date
        }
    except (HTTPException, BadRequestError, NotFoundError):
        raise
    except Exception as e:
        raise BadRequestError(message=str(e))


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
        raise BadRequestError(message=str(e))


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
        raise BadRequestError(message=str(e))


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
        raise NotFoundError(message="File not found")

    return file_detail


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
            raise BadRequestError(message="Failed to delete file")
    except Exception as e:
        raise BadRequestError(message=str(e))
