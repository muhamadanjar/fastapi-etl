"""
File service for handling file upload, processing, and management.
"""

import os
import hashlib
import mimetypes
from typing import Dict, Any, List, Optional, BinaryIO
from datetime import datetime
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy import select, and_
from fastapi import UploadFile
from app.services.base import BaseService
from app.core.exceptions import FileError, ServiceError
from app.core.constants import UPLOAD_DIRECTORY, ALLOWED_FILE_TYPES, MAX_FILE_SIZE
from app.utils.file_utils import get_file_extension, sanitize_filename
from app.processors import get_processor


class FileService(BaseService):
    """Service for handling file operations."""
    
    def __init__(self, db_session: Session):
        super().__init__(db_session)
        self.upload_dir = Path(UPLOAD_DIRECTORY)
        self.upload_dir.mkdir(parents=True, exist_ok=True)
    
    def get_service_name(self) -> str:
        return "FileService"
    
    async def upload_file(self, file: UploadFile, user_id: int, batch_id: str = None) -> Dict[str, Any]:
        """Upload and register a new file."""
        try:
            self.log_operation("upload_file", {"filename": file.filename, "user_id": user_id})
            
            # Validate file
            await self._validate_file(file)
            
            # Generate unique filename
            safe_filename = sanitize_filename(file.filename)
            file_extension = get_file_extension(safe_filename)
            unique_filename = f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{safe_filename}"
            
            # Save file to disk
            file_path = self.upload_dir / unique_filename
            file_content = await file.read()
            
            with open(file_path, "wb") as f:
                f.write(file_content)
            
            # Calculate file hash
            file_hash = hashlib.sha256(file_content).hexdigest()
            
            # Get file metadata
            file_size = len(file_content)
            file_type = self._detect_file_type(file.filename, file.content_type)
            
            # Register file in database
            file_registry = await self._create_file_registry({
                "file_name": safe_filename,
                "file_path": str(file_path),
                "file_type": file_type,
                "file_size": file_size,
                "source_system": "WEB_UPLOAD",
                "upload_date": datetime.utcnow(),
                "processing_status": "PENDING",
                "batch_id": batch_id or self._generate_batch_id(),
                "created_by": str(user_id),
                "metadata": {
                    "original_filename": file.filename,
                    "content_type": file.content_type,
                    "file_hash": file_hash
                }
            })
            
            return {
                "file_id": file_registry.file_id,
                "file_name": file_registry.file_name,
                "file_type": file_registry.file_type,
                "file_size": file_registry.file_size,
                "batch_id": file_registry.batch_id,
                "status": "uploaded"
            }
            
        except Exception as e:
            self.handle_error(e, "upload_file")
    
    async def process_file(self, file_id: int) -> Dict[str, Any]:
        """Process uploaded file and extract data."""
        try:
            self.log_operation("process_file", {"file_id": file_id})
            
            # Get file registry
            file_registry = await self._get_file_registry(file_id)
            if not file_registry:
                raise FileError("File not found")
            
            # Update status to processing
            await self._update_file_status(file_id, "PROCESSING")
            
            # Get appropriate processor
            processor = get_processor(file_registry.file_type)
            if not processor:
                raise FileError(f"No processor available for file type: {file_registry.file_type}")
            
            # Process file
            processing_result = await processor.process_file(file_registry.file_path)
            
            # Save raw records
            records_saved = await self._save_raw_records(file_id, processing_result)
            
            # Update file status
            await self._update_file_status(file_id, "COMPLETED")
            
            return {
                "file_id": file_id,
                "records_processed": len(processing_result.get("records", [])),
                "records_saved": records_saved,
                "columns_detected": processing_result.get("columns", []),
                "status": "completed"
            }
            
        except Exception as e:
            await self._update_file_status(file_id, "FAILED", str(e))
            self.handle_error(e, "process_file")
    
    async def get_file_info(self, file_id: int) -> Dict[str, Any]:
        """Get file information and processing status."""
        try:
            file_registry = await self._get_file_registry(file_id)
            if not file_registry:
                raise FileError("File not found")
            
            # Get record count
            record_count = await self._get_file_record_count(file_id)
            
            return {
                "file_id": file_registry.file_id,
                "file_name": file_registry.file_name,
                "file_type": file_registry.file_type,
                "file_size": file_registry.file_size,
                "source_system": file_registry.source_system,
                "upload_date": file_registry.upload_date,
                "processing_status": file_registry.processing_status,
                "batch_id": file_registry.batch_id,
                "record_count": record_count,
                "metadata": file_registry.metadata
            }
            
        except Exception as e:
            self.handle_error(e, "get_file_info")
    
    async def get_file_list(self, batch_id: str = None, status: str = None) -> List[Dict[str, Any]]:
        """Get list of files with optional filtering."""
        try:
            self.log_operation("get_file_list", {"batch_id": batch_id, "status": status})
            
            files = await self._get_file_list(batch_id, status)
            
            result = []
            for file_registry in files:
                record_count = await self._get_file_record_count(file_registry.file_id)
                result.append({
                    "file_id": file_registry.file_id,
                    "file_name": file_registry.file_name,
                    "file_type": file_registry.file_type,
                    "file_size": file_registry.file_size,
                    "upload_date": file_registry.upload_date,
                    "processing_status": file_registry.processing_status,
                    "batch_id": file_registry.batch_id,
                    "record_count": record_count
                })
            
            return result
            
        except Exception as e:
            self.handle_error(e, "get_file_list")
    
    async def delete_file(self, file_id: int) -> bool:
        """Delete file and its records."""
        try:
            self.log_operation("delete_file", {"file_id": file_id})
            
            # Get file registry
            file_registry = await self._get_file_registry(file_id)
            if not file_registry:
                raise FileError("File not found")
            
            # Delete physical file
            if os.path.exists(file_registry.file_path):
                os.remove(file_registry.file_path)
            
            # Delete records
            await self._delete_file_records(file_id)
            
            # Delete file registry
            await self._delete_file_registry(file_id)
            
            return True
            
        except Exception as e:
            self.handle_error(e, "delete_file")
    
    async def get_file_preview(self, file_id: int, limit: int = 100) -> Dict[str, Any]:
        """Get preview of file data."""
        try:
            self.log_operation("get_file_preview", {"file_id": file_id, "limit": limit})
            
            # Get file registry
            file_registry = await self._get_file_registry(file_id)
            if not file_registry:
                raise FileError("File not found")
            
            # Get sample records
            records = await self._get_file_records(file_id, limit)
            
            # Get column structure
            columns = await self._get_file_columns(file_id)
            
            return {
                "file_id": file_id,
                "file_name": file_registry.file_name,
                "columns": columns,
                "records": records,
                "total_records": await self._get_file_record_count(file_id)
            }
            
        except Exception as e:
            self.handle_error(e, "get_file_preview")
    
    # Private helper methods
    async def _validate_file(self, file: UploadFile):
        """Validate uploaded file."""
        if not file.filename:
            raise FileError("No filename provided")
        
        # Check file extension
        file_extension = get_file_extension(file.filename).lower()
        if file_extension not in ALLOWED_FILE_TYPES:
            raise FileError(f"File type not allowed: {file_extension}")
        
        # Check file size
        file_content = await file.read()
        if len(file_content) > MAX_FILE_SIZE:
            raise FileError(f"File too large: {len(file_content)} bytes")
        
        # Reset file pointer
        await file.seek(0)
    
    def _detect_file_type(self, filename: str, content_type: str) -> str:
        """Detect file type from filename and content type."""
        extension = get_file_extension(filename).lower()
        
        type_mapping = {
            '.csv': 'CSV',
            '.xlsx': 'EXCEL',
            '.xls': 'EXCEL',
            '.json': 'JSON',
            '.xml': 'XML',
            '.txt': 'TXT'
        }
        
        return type_mapping.get(extension, 'UNKNOWN')
    
    def _generate_batch_id(self) -> str:
        """Generate unique batch ID."""
        return f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{os.urandom(4).hex()}"
    
    # Database helper methods (implement based on your models)
    async def _create_file_registry(self, file_data: Dict[str, Any]):
        """Create file registry record."""
        # Implement database insert
        pass
    
    async def _get_file_registry(self, file_id: int):
        """Get file registry by ID."""
        # Implement database query
        pass
    
    async def _update_file_status(self, file_id: int, status: str, error_message: str = None):
        """Update file processing status."""
        # Implement database update
        pass
    
    async def _save_raw_records(self, file_id: int, processing_result: Dict[str, Any]) -> int:
        """Save raw records to database."""
        # Implement database insert
        pass
    
    async def _get_file_record_count(self, file_id: int) -> int:
        """Get record count for file."""
        # Implement database query
        pass
    
    async def _get_file_list(self, batch_id: str = None, status: str = None):
        """Get file list with filters."""
        # Implement database query
        pass
    
    async def _delete_file_records(self, file_id: int):
        """Delete all records for file."""
        # Implement database delete
        pass
    
    async def _delete_file_registry(self, file_id: int):
        """Delete file registry record."""
        # Implement database delete
        pass
    
    async def _get_file_records(self, file_id: int, limit: int):
        """Get file records with limit."""
        # Implement database query
        pass
    
    async def _get_file_columns(self, file_id: int):
        """Get column structure for file."""
        # Implement database query
        pass