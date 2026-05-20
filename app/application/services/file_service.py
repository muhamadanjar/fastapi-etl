"""
File service untuk mengelola upload, processing, dan management file.
"""

from math import ceil
import os
import json
import uuid
import shutil
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from pathlib import Path
from fastapi import UploadFile, HTTPException, status
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, or_
from uuid import UUID

from app.infrastructure.db.repositories.file_repository import FileRegistryRepository
from app.infrastructure.db.repositories.upload_session_repository import UploadSessionRepository
from app.schemas.base import PaginatedMetaDataResponse
from app.application.services.base import BaseService
from app.application.services.user_service import UserService
from app.infrastructure.db.models.raw_data.file_registry import FileRegistry
from app.infrastructure.db.models.raw_data.raw_records import RawRecords
from app.infrastructure.db.models.raw_data.column_structure import ColumnStructure
from app.infrastructure.db.models.raw_data.upload_session import UploadSession, UploadSessionStatus
from app.schemas.file_upload import FileMetadata, FilePreview, FileStructureAnalysis, FileUploadResponse, FileListResponse, FileDetailResponse, FileValidationResult
from app.schemas.upload_session import (
    InitUploadSessionRequest,
    InitUploadSessionResponse,
    ChunkUploadResponse,
    UploadSessionStatusResponse,
)
from app.core.exceptions import FileError, ServiceError, AppException
from app.core.enums import ProcessingStatus, FileTypeEnum
from app.core.config import settings
from app.utils.file_utils import get_file_type, calculate_file_hash, validate_file_size
from app.utils.date_utils import get_current_timestamp
# Processor imports are deferred inside _get_processor() to break the circular
# import cycle: processors.__init__ → base_processor → application.services →
# file_service → processors.{csv,excel,json,xml}_processor → base_processor


class FileService(BaseService):
    """Service untuk mengelola file operations."""
    
    def __init__(self, db: Session):
        super().__init__(db)
        self.upload_dir = Path("storage/uploads")
        self.processed_dir = Path("storage/processed")
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.repo = FileRegistryRepository(db)
        self.upload_session_repo = UploadSessionRepository(db)
    
    def get_service_name(self) -> str:
        return "FileService"
    
    async def upload_file(
        self,
        file: UploadFile,
        source_system: str,
        batch_id: Optional[str] = None,
        metadata: Optional[str] = None,
        user_id: UUID = None
    ) -> FileUploadResponse:
        """Upload file dan simpan metadata."""
        try:
            self.log_operation("upload_file", {"filename": file.filename, "source_system": source_system, "user_id": user_id})

            # Validate user exists from cache/remote (non-blocking)
            # User already validated in endpoint via get_current_user
            if user_id:
                try:
                    user_exists = await UserService.validate_user_exists_remote(user_id)
                    if not user_exists:
                        self.logger.warning(f"User {user_id} not found in usermanagement API")
                except Exception as e:
                    self.logger.debug(f"User validation skipped: {str(e)}")
                    # Continue anyway - user was validated at endpoint level

            # Validate file
            if not file.filename:
                raise FileError("Filename is required")
            
            file_type = get_file_type(file.content_type)
            if not file_type:
                raise FileError(f"File type {file.content_type} not supported")
            
            # Generate unique filename
            file_id = str(uuid.uuid4())
            file_extension = Path(file.filename).suffix
            unique_filename = f"{file_id}{file_extension}"
            file_path = self.upload_dir / unique_filename
            
            # Save file to storage
            file_size = await self._save_uploaded_file(file, file_path)
            
            # Validate file size
            validate_file_size(file_path)
            
            # Parse metadata
            parsed_metadata = {}
            if metadata:
                try:
                    parsed_metadata = json.loads(metadata)
                except json.JSONDecodeError:
                    raise FileError("Invalid metadata JSON format")
            
            # Generate batch_id if not provided
            if not batch_id:
                batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Create file registry record
            file_registry = FileRegistry(
                file_name=file.filename,
                file_path=str(file_path),
                file_type=file_type,
                file_size=file_size,
                source_system=source_system,
                processing_status=ProcessingStatus.PENDING.value,
                batch_id=batch_id,
                created_by=str(user_id) if user_id else None,
                metadata=parsed_metadata
            )
            
            self.db.add(file_registry)
            self.db.commit()
            self.db.refresh(file_registry)
            
            return FileUploadResponse(
                file_id=file_registry.id,
                file_name=file_registry.file_name,
                file_type=file_registry.file_type,
                file_size=file_registry.file_size,
                source_system=file_registry.source_system,
                batch_id=file_registry.batch_id,
                processing_status=file_registry.processing_status,
                upload_date=file_registry.upload_date,
                message="File uploaded successfully"
            )
            
        except Exception as e:
            self.db.rollback()
            # Clean up file if database operation fails
            if 'file_path' in locals() and file_path.exists():
                file_path.unlink()
            self.handle_error(e, "upload_file")
    
    async def get_file_list(
        self,
        skip: int = 0,
        limit: int = 100,
        file_type: Optional[str] = None,
        source_system: Optional[str] = None,
        status: Optional[str] = None,
        batch_id: Optional[str] = None
    ) -> FileListResponse:
        """Get list of files dengan filtering dan pagination."""
        try:
            self.log_operation("get_file_list", {
                "skip": skip, "limit": limit, "file_type": file_type,
                "source_system": source_system, "status": status, "batch_id": batch_id
            })
            conditions = []
            if file_type:
                conditions.append(['file_type', file_type])
            if source_system:
                conditions.append(['source_system', source_system])
            if status:
                conditions.append(['processing_status', status])   
            if batch_id:
                conditions.append(['batch_id', batch_id])
            sort_by = [{'field': 'upload_date', 'direction': 'desc'}]
            criteria = {'and': conditions} if conditions else None
            query = self.repo.build_query(
                criteria=criteria,
                sort_by=sort_by
            )
            page = skip // limit + 1
            query = query.offset(skip).limit(limit)
            
            total_data = await self.repo.count_filtered(criteria=criteria)
            files = self.db.execute(query).scalars().all()

            file_metadata = [
                FileMetadata(
                    file_id=file.id,
                    file_name=file.file_name,
                    file_type=file.file_type,
                    file_size=file.file_size,
                    file_path=file.file_path,
                    source_system=file.source_system,
                    processing_status=file.processing_status,
                    batch_id=file.batch_id,
                    upload_date=file.upload_date,
                    created_by=file.created_by,
                    metadata=file.file_metadata if file.file_metadata else {}
                ) for file in files
            ]

            metas = PaginatedMetaDataResponse(
                total_pages=ceil(total_data / limit),
                page=page,
                per_page=limit,
                total=total_data,
                has_next=(skip + limit < total_data),
                has_prev=(skip > 0)
            )

            return FileListResponse(
                data=file_metadata,
                metas=metas
            )
            
            
        except Exception as e:
            self.handle_error(e, "get_file_list")
    
    async def get_file_detail(self, file_id: UUID) -> Optional[FileDetailResponse]:
        """Get detailed information tentang file."""
        try:
            self.log_operation("get_file_detail", {"file_id": file_id})
            
            file_registry = self.db.get(FileRegistry, file_id)
            if not file_registry:
                return None
            
            # Get raw records count
            raw_records_stmt = select(RawRecords).where(RawRecords.file_id == file_id)
            raw_records = self.db.execute(raw_records_stmt).scalars().all()
            
            # Get column structure
            column_structure_stmt = select(ColumnStructure).where(ColumnStructure.file_id == file_id)
            columns = self.db.execute(column_structure_stmt).scalars().all()

            file_metadata = FileMetadata(
                file_id=file_registry.id,
                file_name=file_registry.file_name,
                file_path=file_registry.file_path,
                file_type=file_registry.file_type,
                file_size=file_registry.file_size,
                source_system=file_registry.source_system,
                processing_status=file_registry.processing_status,
                batch_id=file_registry.batch_id,
                upload_date=file_registry.upload_date,
                created_by=file_registry.created_by,
                metadata=file_registry.file_metadata if file_registry.file_metadata else {}
            )
            return FileDetailResponse(
                file=file_metadata,
                structure_analysis=FileStructureAnalysis(
                    file_id=file_registry.id,
                    total_rows=len(raw_records),
                    total_columns=len(columns),
                    columns=[{
                        "column_name": col.column_name,
                        "column_position": col.column_position,
                        "data_type": col.data_type,
                        "sample_values": col.sample_values,
                        "null_count": col.null_count,
                        "unique_count": col.unique_count
                    } for col in columns],
                    data_quality_score=1.0,
                    issues_found=[],
                    recommendations=[]
                ),
                preview=FilePreview(
                    file_id=file_registry.id,
                    headers=[col.column_name for col in columns],
                    sample_data=[{
                        col.column_name: getattr(record, col.column_name, None)
                        for col in columns
                    } for record in raw_records[:10]],  # Preview first 10 records  
                    total_rows=len(raw_records),
                    preview_rows=min(10, len(raw_records))  # Limit preview to 10 rows
                ),
                validation_result=FileValidationResult(
                    file_id=file_registry.id,
                    validation_status='VALID',  # Assuming validation is done
                    valid_records=len([r for r in raw_records if r.validation_status == 'VALID']),
                    invalid_records=len([r for r in raw_records if r.validation_status == 'INVALID']),
                    quality_score=1.0,  # Placeholder for quality score
                    warnings=0,
                    total_records=len(raw_records),
                )
                
                
                # records_count=len(raw_records),
                # valid_records=len([r for r in raw_records if r.validation_status == 'VALID']),
                # invalid_records=len([r for r in raw_records if r.validation_status == 'INVALID']),
                # columns=[{
                #     "name": col.column_name,
                #     "position": col.column_position,
                #     "data_type": col.data_type,
                #     "sample_values": col.sample_values,
                #     "null_count": col.null_count,
                #     "unique_count": col.unique_count
                # } for col in columns]
            )
            
        except Exception as e:
            self.handle_error(e, "get_file_detail")
    
    async def start_file_processing(self, file_id: str, user_id: str) -> str:
        """Start processing file secara asynchronous."""
        try:
            from app.tasks.etl_tasks import process_file_task

            self.log_operation("start_file_processing", {"file_id": file_id, "user_id": user_id})

            # Validate user exists from cache/remote (non-blocking, for audit/logging)
            if user_id:
                try:
                    user_exists = await UserService.validate_user_exists_remote(UUID(user_id))
                    if not user_exists:
                        self.logger.warning(f"User {user_id} not found in usermanagement API during file processing")
                except Exception as e:
                    self.logger.debug(f"User validation skipped: {str(e)}")

            file_registry = self.db.get(FileRegistry, file_id)
            if not file_registry:
                raise FileError("File not found")
            
            if file_registry.processing_status == ProcessingStatus.PROCESSING.value:
                raise FileError("File is already being processed")
            
            if file_registry.processing_status == ProcessingStatus.COMPLETED.value:
                raise FileError("File has already been processed")
            
            # Update status to processing
            file_registry.processing_status = ProcessingStatus.PROCESSING.value
            self.db.commit()
            
            # Start background task
            task_result = process_file_task.delay(file_id, user_id)
            
            return task_result.id
            
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "start_file_processing")
    
    async def delete_file(self, file_id: UUID, user_id: int) -> bool:
        """Delete file dan semua data yang terkait."""
        try:
            self.log_operation("delete_file", {"file_id": file_id, "user_id": user_id})
            
            file_registry = self.db.get(FileRegistry, file_id)
            if not file_registry:
                raise FileError("File not found")
            
            if file_registry.processing_status == ProcessingStatus.PROCESSING.value:
                raise FileError("Cannot delete file while processing")
            
            # Delete physical file
            file_path = Path(file_registry.file_path)
            if file_path.exists():
                file_path.unlink()
            
            # Delete related records (cascade delete should handle this)
            # But explicitly delete for safety
            await self._delete_related_data(file_id)
            
            # Delete file registry
            self.db.delete(file_registry)
            self.db.commit()
            
            return True
            
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "delete_file")
    
    async def download_file(self, file_id: UUID) -> FileResponse:
        """Download original file."""
        try:
            self.log_operation("download_file", {"file_id": file_id})
            
            file_registry = self.db.get(FileRegistry, file_id)
            if not file_registry:
                raise FileError("File not found")
            
            file_path = Path(file_registry.file_path)
            if not file_path.exists():
                raise FileError("Physical file not found")
            
            return FileResponse(
                path=str(file_path),
                filename=file_registry.file_name,
                media_type='application/octet-stream'
            )
            
        except Exception as e:
            self.handle_error(e, "download_file")
    
    async def preview_file_data(self, file_id: UUID, rows: int = 10) -> Dict[str, Any]:
        """Preview file data (first N rows)."""
        try:
            self.log_operation("preview_file_data", {"file_id": file_id, "rows": rows})
            
            file_registry = self.db.get(FileRegistry, file_id)
            if not file_registry:
                raise FileError("File not found")
            
            file_path = Path(file_registry.file_path)
            if not file_path.exists():
                raise FileError("Physical file not found")
            
            # Get appropriate processor
            processor = self._get_processor(file_registry.file_type)
            
            # Preview data
            preview_data = await processor.preview_data(str(file_path), rows)
            
            return {
                "file_id": file_id,
                "file_name": file_registry.file_name,
                "file_type": file_registry.file_type,
                "preview_rows": rows,
                "data": preview_data
            }
            
        except Exception as e:
            self.handle_error(e, "preview_file_data")
    
    async def batch_upload(
        self,
        files: List[UploadFile],
        source_system: str,
        batch_id: Optional[str] = None,
        user_id: UUID = None
    ) -> Dict[str, Any]:
        """Upload multiple files dalam satu batch."""
        try:
            self.log_operation("batch_upload", {
                "files_count": len(files),
                "source_system": source_system,
                "batch_id": batch_id,
                "user_id": user_id
            })

            # Validate user exists from cache/remote (non-blocking, for audit)
            if user_id:
                try:
                    user_exists = await UserService.validate_user_exists_remote(user_id)
                    if not user_exists:
                        self.logger.warning(f"User {user_id} not found in usermanagement API during batch upload")
                except Exception as e:
                    self.logger.debug(f"User validation skipped: {str(e)}")
            
            if not batch_id:
                batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
            
            results = []
            success_count = 0
            error_count = 0
            
            for file in files:
                try:
                    result = await self.upload_file(file, source_system, batch_id, None, user_id)
                    results.append({
                        "file_name": file.filename,
                        "status": "success",
                        "file_id": result.file_id,
                        "message": "Uploaded successfully"
                    })
                    success_count += 1
                except Exception as e:
                    results.append({
                        "file_name": file.filename,
                        "status": "error",
                        "file_id": None,
                        "message": str(e)
                    })
                    error_count += 1
            
            return {
                "batch_id": batch_id,
                "total_files": len(files),
                "success_count": success_count,
                "error_count": error_count,
                "results": results
            }
            
        except Exception as e:
            self.handle_error(e, "batch_upload")
    
    async def process_file_content(self, file_id: int) -> Dict[str, Any]:
        """Process file content dan extract data."""
        try:
            self.log_operation("process_file_content", {"file_id": file_id})
            
            file_registry = self.db.get(FileRegistry, file_id)
            if not file_registry:
                raise FileError("File not found")
            
            file_path = Path(file_registry.file_path)
            if not file_path.exists():
                raise FileError("Physical file not found")
            
            # Get appropriate processor
            processor = self._get_processor(file_registry.file_type)
            
            # Process file
            processing_result = await processor.process_file(str(file_path), file_id, file_registry.batch_id)
            
            # Update processing status
            file_registry.processing_status = ProcessingStatus.COMPLETED.value
            self.db.commit()
            
            return processing_result
            
        except Exception as e:
            self.db.rollback()
            # Update status to failed
            file_registry = self.db.get(FileRegistry, file_id)
            if file_registry:
                file_registry.processing_status = ProcessingStatus.FAILED.value
                self.db.commit()
            
            self.handle_error(e, "process_file_content")
    
    # Private helper methods
    async def _save_uploaded_file(self, file: UploadFile, file_path: Path) -> int:
        """Save uploaded file to storage."""
        try:
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            return file_path.stat().st_size
        except Exception as e:
            if file_path.exists():
                file_path.unlink()
            raise FileError(f"Failed to save file: {str(e)}")
    
    def _get_processor(self, file_type: str):
        """Get appropriate processor based on file type."""
        # Deferred imports: these processor subclasses inherit from BaseProcessor
        # which (at module-level) formerly imported RejectedRecordsService from
        # this same application.services package, forming a cycle.  Importing
        # here — inside the method body — ensures processors.__init__ is fully
        # initialised before these names are resolved.
        from app.processors.csv_processor import CSVProcessor
        from app.processors.excel_processor import ExcelProcessor
        from app.processors.json_processor import JSONProcessor
        from app.processors.xml_processor import XMLProcessor

        processors = {
            FileTypeEnum.CSV.value: CSVProcessor,
            FileTypeEnum.EXCEL.value: ExcelProcessor,
            FileTypeEnum.JSON.value: JSONProcessor,
            FileTypeEnum.XML.value: XMLProcessor
        }
        
        processor_class = processors.get(file_type)
        if not processor_class:
            raise FileError(f"No processor available for file type: {file_type}")
        
        return processor_class(self.db)
    
    async def _delete_related_data(self, file_id: int):
        """Delete all related data for a file."""
        # Delete raw records
        raw_records_stmt = select(RawRecords).where(RawRecords.file_id == file_id)
        raw_records = self.db.execute(raw_records_stmt).scalars().all()
        for record in raw_records:
            self.db.delete(record)

        # Delete column structure
        column_structure_stmt = select(ColumnStructure).where(ColumnStructure.file_id == file_id)
        columns = self.db.execute(column_structure_stmt).scalars().all()
        for column in columns:
            self.db.delete(column)

    # ============ CHUNKED UPLOAD METHODS ============

    async def init_upload_session(
        self, request: InitUploadSessionRequest, user_id: UUID
    ) -> InitUploadSessionResponse:
        """Initialize a new chunked upload session"""
        try:
            self.log_operation("init_upload_session", {"file_name": request.file_name, "file_size": request.file_size})

            # Calculate chunks
            chunk_size = settings.storage_settings.chunk_size
            total_chunks = ceil(request.file_size / chunk_size)
            expires_at = datetime.utcnow() + timedelta(
                hours=settings.storage_settings.upload_session_expire_hours
            )

            # Generate file path
            file_uuid = uuid.uuid4()
            ext = Path(request.file_name).suffix or ".bin"
            file_path = f"{file_uuid}{ext}"  # Store only relative path

            # Pre-create sparse temp file
            temp_dir = Path(settings.storage_settings.local_storage_path) / "chunks"
            temp_dir.mkdir(parents=True, exist_ok=True)
            temp_path = temp_dir / f"{file_uuid}.part"

            # Create sparse file
            with open(temp_path, "wb") as f:
                f.seek(request.file_size - 1)
                f.write(b"\0")

            # Parse metadata if provided
            file_metadata = {}
            if request.metadata:
                try:
                    file_metadata = json.loads(request.metadata)
                except Exception:
                    pass

            # Create session record
            session_data = {
                "file_name": request.file_name,
                "file_path": file_path,
                "file_size": request.file_size,
                "file_type": request.file_type,
                "chunk_size": chunk_size,
                "total_chunks": total_chunks,
                "status": UploadSessionStatus.PENDING,
                "source_system": request.source_system,
                "batch_id": request.batch_id,
                "file_metadata": file_metadata,
                "created_by": user_id,
                "expires_at": expires_at,
                "chunk_map": {},
            }

            session = await self.upload_session_repo.create(session_data)

            return InitUploadSessionResponse(
                session_id=session.id,
                chunk_size=chunk_size,
                total_chunks=total_chunks,
                expires_at=expires_at,
                status=session.status,
            )

        except Exception as e:
            self.log_error(e, "init_upload_session")
            raise AppException(status_code=400, message=f"Failed to initialize upload session: {str(e)}")

    async def upload_chunk(
        self, session_id: UUID, content_range: str, chunk_data: bytes
    ) -> ChunkUploadResponse:
        """Upload a single chunk with Content-Range header"""
        try:
            # Get session
            session = await self.upload_session_repo.get_by_id(session_id)
            if not session:
                raise AppException(status_code=404, message="Upload session not found")

            # Check expiry
            if datetime.utcnow() > session.expires_at:
                await self.upload_session_repo.mark_expired(session_id)
                raise AppException(status_code=410, message="Upload session expired")

            # Check status
            if session.status not in [
                UploadSessionStatus.PENDING,
                UploadSessionStatus.UPLOADING,
                UploadSessionStatus.PAUSED,
            ]:
                raise AppException(
                    status_code=400,
                    message=f"Cannot upload to session with status: {session.status}",
                )

            # Parse Content-Range header
            start, end, total = self._parse_content_range(content_range)

            # Validate range
            if total != session.file_size:
                raise AppException(
                    status_code=400,
                    message=f"File size mismatch: expected {session.file_size}, got {total}",
                )

            if end > session.file_size:
                raise AppException(
                    status_code=416,
                    message="Range exceeds file size",
                )

            # Calculate chunk index
            chunk_index = start // session.chunk_size

            # Write chunk to temp file
            file_uuid = session.file_path.split(".")[0]
            temp_path = self._get_chunk_path(file_uuid)

            with open(temp_path, "r+b") as f:
                f.seek(start)
                f.write(chunk_data)

            # Update session chunk tracking
            chunk_map = session.chunk_map or {}
            chunk_map[str(chunk_index)] = True

            # Calculate received bytes more accurately
            received_bytes = sum(
                min(session.chunk_size, session.file_size - int(k) * session.chunk_size)
                for k in chunk_map.keys()
            )

            uploaded_chunks = len(chunk_map)

            # Update session
            await self.upload_session_repo.update_chunk_map(
                session_id, chunk_index, received_bytes, uploaded_chunks
            )

            # Refresh session for current status
            session = await self.upload_session_repo.get_by_id(session_id)
            progress_percent = (received_bytes / session.file_size) * 100

            # Check if all chunks received
            if uploaded_chunks >= session.total_chunks:
                await self._assemble_and_finalize(session)
                progress_percent = 100.0

            return ChunkUploadResponse(
                session_id=session_id,
                status=session.status,
                received_bytes=received_bytes,
                uploaded_chunks=uploaded_chunks,
                total_chunks=session.total_chunks,
                progress_percent=progress_percent,
            )

        except AppException:
            raise
        except Exception as e:
            self.log_error(e, "upload_chunk")
            raise AppException(status_code=400, message=f"Failed to upload chunk: {str(e)}")

    async def get_upload_session_status(self, session_id: UUID) -> UploadSessionStatusResponse:
        """Get current session status for resume capability"""
        try:
            session = await self.upload_session_repo.get_by_id(session_id)
            if not session:
                raise AppException(status_code=404, message="Upload session not found")

            # Check expiry
            if datetime.utcnow() > session.expires_at:
                await self.upload_session_repo.mark_expired(session_id)
                session = await self.upload_session_repo.get_by_id(session_id)

            chunk_map = session.chunk_map or {}
            received_bytes = sum(
                min(session.chunk_size, session.file_size - int(k) * session.chunk_size)
                for k in chunk_map.keys()
            )
            progress_percent = (received_bytes / session.file_size) * 100

            return UploadSessionStatusResponse(
                session_id=session.id,
                status=session.status,
                file_name=session.file_name,
                file_size=session.file_size,
                received_bytes=received_bytes,
                uploaded_chunks=len(chunk_map),
                total_chunks=session.total_chunks,
                chunk_size=session.chunk_size,
                chunk_map=chunk_map,
                progress_percent=progress_percent,
                file_registry_id=session.file_registry_id,
                expires_at=session.expires_at,
            )

        except AppException:
            raise
        except Exception as e:
            self.log_error(e, "get_upload_session_status")
            raise AppException(status_code=400, message=f"Failed to get session status: {str(e)}")

    async def _assemble_and_finalize(self, session: UploadSession) -> None:
        """Assemble chunks and create FileRegistry after upload completes"""
        try:
            base_path = Path(settings.storage_settings.local_storage_path)

            # Move temp file to uploads directory
            file_uuid = session.file_path.split(".")[0]
            temp_path = self._get_chunk_path(file_uuid)
            final_dir = base_path / "uploads"
            final_dir.mkdir(parents=True, exist_ok=True)
            final_path = final_dir / session.file_path

            shutil.move(str(temp_path), str(final_path))

            # Create FileRegistry record
            file_registry = FileRegistry(
                file_name=session.file_name,
                file_path=str(final_path),  # Store relative path
                file_type=session.file_type,
                file_size=session.file_size,
                source_system=session.source_system,
                batch_id=session.batch_id,
                processing_status=ProcessingStatus.PENDING.value,
                created_by=session.created_by,
                file_metadata=session.file_metadata or {},
            )
            self.db.add(file_registry)
            self.db.flush()

            # Update session
            await self.upload_session_repo.mark_completed(session.id, file_registry.id)

        except Exception as e:
            self.log_error(e, "_assemble_and_finalize")
            raise AppException(status_code=500, message=f"Failed to finalize upload: {str(e)}")

    def _parse_content_range(self, content_range: str) -> Tuple[int, int, int]:
        """Parse Content-Range header
        Format: bytes {start}-{end}/{total}
        Example: bytes 0-1023/10240
        """
        try:
            range_part = content_range.split(" ")[1]
            start_end, total = range_part.split("/")
            start, end = start_end.split("-")
            return int(start), int(end), int(total)
        except Exception:
            raise AppException(
                status_code=400,
                message="Invalid Content-Range header format",
            )

    def _get_chunk_path(self, file_uuid: str) -> Path:
        """Get path to temp chunk file"""
        base = Path(settings.storage_settings.local_storage_path)
        return base / "chunks" / f"{file_uuid}.part"