"""
File service untuk mengelola upload, processing, dan management file.
"""

from math import ceil
import os
import json
import uuid
import shutil
from typing import Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path
from fastapi import UploadFile, HTTPException, status
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, or_
from uuid import UUID

from app.infrastructure.db.repositories.file_repository import FileRegistryRepository
from app.schemas.base import PaginatedMetaDataResponse
from app.services.base import BaseService
from app.infrastructure.db.models.raw_data.file_registry import FileRegistry
from app.infrastructure.db.models.raw_data.raw_records import RawRecords
from app.infrastructure.db.models.raw_data.column_structure import ColumnStructure
from app.schemas.file_upload import FileMetadata, FileStructureAnalysis, FileUploadResponse, FileListResponse, FileDetailResponse
from app.core.exceptions import FileError, ServiceError
from app.core.enums import ProcessingStatus, FileTypeEnum
from app.utils.file_utils import get_file_type, calculate_file_hash, validate_file_size
from app.utils.date_utils import get_current_timestamp
from app.processors.csv_processor import CSVProcessor
from app.processors.excel_processor import ExcelProcessor
from app.processors.json_processor import JSONProcessor
from app.processors.xml_processor import XMLProcessor


class FileService(BaseService):
    """Service untuk mengelola file operations."""
    
    def __init__(self, db: Session):
        super().__init__(db)
        self.upload_dir = Path("storage/uploads")
        self.processed_dir = Path("storage/processed")
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.repo = FileRegistryRepository(db)
    
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
            self.log_operation("upload_file", {"filename": file.filename, "source_system": source_system})
            
            # Validate file
            if not file.filename:
                raise FileError("Filename is required")
            
            file_type = get_file_type(file.content_type)
            print(f"Detected file type: {file_type}")
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
    
    async def delete_file(self, file_id: int, user_id: int) -> bool:
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
    
    async def download_file(self, file_id: int) -> FileResponse:
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
    
    async def preview_file_data(self, file_id: int, rows: int = 10) -> Dict[str, Any]:
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
        user_id: int = None
    ) -> Dict[str, Any]:
        """Upload multiple files dalam satu batch."""
        try:
            self.log_operation("batch_upload", {
                "files_count": len(files), 
                "source_system": source_system,
                "batch_id": batch_id
            })
            
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
                        "file_id": result.id,
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