"""
High-level storage service for application use.

This module provides a high-level service layer that sits on top
of the storage backends and provides business logic for file operations.
"""

import logging
import mimetypes
from typing import Optional, Dict, Any, List, BinaryIO, Union, Tuple
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, asdict
from enum import Enum

from ...core.config import get_settings
from .base import StorageBackend, StorageFileInfo, StorageException
from .factory import get_default_storage, get_storage, StorageType

logger = logging.getLogger(__name__)
settings = get_settings()


class FileCategory(str, Enum):
    """File categories for organizing storage."""
    IMAGES = "images"
    DOCUMENTS = "documents"
    VIDEOS = "videos"
    AUDIO = "audio"
    ARCHIVES = "archives"
    TEMP = "temp"
    USER_UPLOADS = "user_uploads"
    SYSTEM = "system"


@dataclass
class UploadRequest:
    """File upload request data."""
    file_data: Union[bytes, BinaryIO]
    filename: str
    content_type: Optional[str] = None
    category: Optional[FileCategory] = None
    subfolder: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    user_id: Optional[str] = None
    tags: Optional[List[str]] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        if self.tags is None:
            self.tags = []


@dataclass
class UploadResult:
    """File upload result data."""
    file_info: StorageFileInfo
    file_url: str
    category: Optional[FileCategory] = None
    upload_time: datetime = None
    
    def __post_init__(self):
        if self.upload_time is None:
            self.upload_time = datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = {
            "file_info": self.file_info.to_dict(),
            "file_url": self.file_url,
            "upload_time": self.upload_time.isoformat(),
        }
        if self.category:
            result["category"] = self.category.value
        return result


class FileValidationError(StorageException):
    """File validation error."""
    pass


class FileValidator:
    """File validation utility."""
    
    # File type mappings
    IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg'}
    DOCUMENT_EXTENSIONS = {'.pdf', '.doc', '.docx', '.txt', '.rtf', '.odt'}
    VIDEO_EXTENSIONS = {'.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.mkv'}
    AUDIO_EXTENSIONS = {'.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma'}
    ARCHIVE_EXTENSIONS = {'.zip', '.tar', '.gz', '.rar', '.7z', '.bz2'}
    
    IMAGE_MIME_TYPES = {
        'image/jpeg', 'image/png', 'image/gif', 'image/bmp', 
        'image/webp', 'image/svg+xml'
    }
    
    @classmethod
    def get_file_category(cls, filename: str, content_type: str = None) -> FileCategory:
        """
        Determine file category based on extension and content type.
        
        Args:
            filename: Filename to check
            content_type: Optional content type
            
        Returns:
            FileCategory enum value
        """
        ext = Path(filename).suffix.lower()
        
        if ext in cls.IMAGE_EXTENSIONS:
            return FileCategory.IMAGES
        elif ext in cls.DOCUMENT_EXTENSIONS:
            return FileCategory.DOCUMENTS
        elif ext in cls.VIDEO_EXTENSIONS:
            return FileCategory.VIDEOS
        elif ext in cls.AUDIO_EXTENSIONS:
            return FileCategory.AUDIO
        elif ext in cls.ARCHIVE_EXTENSIONS:
            return FileCategory.ARCHIVES
        else:
            # Try to determine from content type
            if content_type:
                if content_type.startswith('image/'):
                    return FileCategory.IMAGES
                elif content_type.startswith('video/'):
                    return FileCategory.VIDEOS
                elif content_type.startswith('audio/'):
                    return FileCategory.AUDIO
                elif content_type in ['application/pdf', 'application/msword']:
                    return FileCategory.DOCUMENTS
        
        return FileCategory.USER_UPLOADS
    
    @classmethod
    def validate_file_size(cls, file_size: int, max_size: int = None) -> bool:
        """
        Validate file size.
        
        Args:
            file_size: File size in bytes
            max_size: Maximum allowed size in bytes
            
        Returns:
            True if valid
            
        Raises:
            FileValidationError: If file is too large
        """
        if max_size is None:
            max_size = getattr(settings.storage, 'max_file_size', 50 * 1024 * 1024)  # 50MB default
        
        if file_size > max_size:
            raise FileValidationError(
                f"File size {file_size} bytes exceeds maximum {max_size} bytes"
            )
        
        return True
    
    @classmethod
    def validate_file_extension(cls, filename: str, allowed_extensions: List[str] = None) -> bool:
        """
        Validate file extension.
        
        Args:
            filename: Filename to check
            allowed_extensions: List of allowed extensions
            
        Returns:
            True if valid
            
        Raises:
            FileValidationError: If extension not allowed
        """
        if allowed_extensions is None:
            allowed_extensions = getattr(settings.storage, 'allowed_file_extensions', None)
        
        if allowed_extensions:
            ext = Path(filename).suffix.lower()
            if ext not in allowed_extensions:
                raise FileValidationError(
                    f"File extension {ext} not allowed. Allowed: {', '.join(allowed_extensions)}"
                )
        
        return True
    
    @classmethod
    def validate_content_type(cls, content_type: str, filename: str) -> str:
        """
        Validate and normalize content type.
        
        Args:
            content_type: Content type to validate
            filename: Filename for fallback detection
            
        Returns:
            Validated content type
        """
        if not content_type:
            # Try to guess from filename
            content_type, _ = mimetypes.guess_type(filename)
            if not content_type:
                content_type = 'application/octet-stream'
        
        return content_type
    
    @classmethod
    def is_image_file(cls, filename: str, content_type: str = None) -> bool:
        """Check if file is an image."""
        ext = Path(filename).suffix.lower()
        return (ext in cls.IMAGE_EXTENSIONS or 
                (content_type and content_type in cls.IMAGE_MIME_TYPES))


class StorageService:
    """
    High-level storage service.
    
    Provides business logic layer on top of storage backends
    with file validation, categorization, and metadata management.
    """
    
    def __init__(
        self,
        storage_backend: Optional[StorageBackend] = None,
        temp_storage_backend: Optional[StorageBackend] = None,
    ):
        """
        Initialize storage service.
        
        Args:
            storage_backend: Main storage backend (uses default if None)
            temp_storage_backend: Temporary storage backend (uses main if None)
        """
        self.storage = storage_backend or get_default_storage()
        self.temp_storage = temp_storage_backend or self.storage
        self.validator = FileValidator()
        
        logger.info(f"Storage service initialized with {self.storage.__class__.__name__}")
    
    def upload_file(self, request: UploadRequest) -> UploadResult:
        """
        Upload file with validation and categorization.
        
        Args:
            request: Upload request data
            
        Returns:
            Upload result
            
        Raises:
            FileValidationError: If file validation fails
            StorageException: If upload fails
        """
        try:
            # Validate file
            self._validate_upload_request(request)
            
            # Determine category if not provided
            if not request.category:
                request.category = self.validator.get_file_category(
                    request.filename, 
                    request.content_type
                )
            
            # Prepare metadata
            metadata = self._prepare_metadata(request)
            
            # Determine subfolder based on category
            subfolder = self._get_subfolder_for_category(
                request.category, 
                request.subfolder,
                request.user_id
            )
            
            # Upload file
            file_info = self.storage.save_file(
                file_data=request.file_data,
                filename=request.filename,
                subfolder=subfolder,
                metadata=metadata,
                content_type=request.content_type,
            )
            
            # Generate file URL
            file_url = self.storage.get_file_url(file_info.identifier)
            
            result = UploadResult(
                file_info=file_info,
                file_url=file_url,
                category=request.category,
            )
            
            logger.info(f"File uploaded successfully: {request.filename} -> {file_info.identifier}")
            return result
            
        except FileValidationError:
            raise
        except Exception as e:
            logger.error(f"Failed to upload file {request.filename}: {e}")
            raise StorageException(f"Upload failed: {e}")
    
    def get_file(self, identifier: str) -> bytes:
        """
        Get file content.
        
        Args:
            identifier: File identifier
            
        Returns:
            File content as bytes
        """
        return self.storage.get_file(identifier)
    
    def get_file_info(self, identifier: str) -> StorageFileInfo:
        """
        Get file information.
        
        Args:
            identifier: File identifier
            
        Returns:
            File information
        """
        return self.storage.get_file_info(identifier)
    
    def get_file_url(
        self, 
        identifier: str, 
        expires_in: Optional[int] = None,
        download: bool = False
    ) -> str:
        """
        Get file URL.
        
        Args:
            identifier: File identifier
            expires_in: URL expiration time in seconds
            download: Whether URL should force download
            
        Returns:
            File URL
        """
        url = self.storage.get_file_url(identifier, expires_in)
        
        # Add download parameter if needed
        if download and expires_in:
            # For presigned URLs, add response-content-disposition
            separator = '&' if '?' in url else '?'
            url += f"{separator}response-content-disposition=attachment"
        
        return url
    
    def delete_file(self, identifier: str) -> bool:
        """
        Delete file.
        
        Args:
            identifier: File identifier
            
        Returns:
            True if deleted, False if not found
        """
        result = self.storage.delete_file(identifier)
        if result:
            logger.info(f"File deleted: {identifier}")
        return result
    
    def list_files(
        self,
        category: Optional[FileCategory] = None,
        user_id: Optional[str] = None,
        tags: Optional[List[str]] = None,
        limit: Optional[int] = None,
        pattern: str = "*",
    ) -> List[StorageFileInfo]:
        """
        List files with filtering.
        
        Args:
            category: Filter by file category
            user_id: Filter by user ID
            tags: Filter by tags
            limit: Maximum number of files to return
            pattern: File pattern to match
            
        Returns:
            List of file information
        """
        # Determine subfolder based on filters
        subfolder = None
        if category:
            subfolder = self._get_subfolder_for_category(category, None, user_id)
        elif user_id:
            subfolder = f"users/{user_id}"
        
        files = self.storage.list_files(
            subfolder=subfolder,
            pattern=pattern,
            limit=limit,
        )
        
        # Apply additional filtering
        if tags:
            files = self._filter_by_tags(files, tags)
        
        return files
    
    def copy_file(
        self, 
        source_identifier: str, 
        dest_filename: str,
        dest_category: Optional[FileCategory] = None,
        dest_user_id: Optional[str] = None,
    ) -> StorageFileInfo:
        """
        Copy file to new location.
        
        Args:
            source_identifier: Source file identifier
            dest_filename: Destination filename
            dest_category: Destination category
            dest_user_id: Destination user ID
            
        Returns:
            New file information
        """
        # Get source file info
        source_info = self.storage.get_file_info(source_identifier)
        
        # Determine destination
        if not dest_category:
            dest_category = self.validator.get_file_category(
                dest_filename, 
                source_info.content_type
            )
        
        dest_subfolder = self._get_subfolder_for_category(
            dest_category, 
            None, 
            dest_user_id
        )
        
        # Generate destination identifier
        dest_identifier = self._generate_file_path(dest_filename, dest_subfolder)
        
        return self.storage.copy_file(source_identifier, dest_identifier)
    
    def move_file(
        self, 
        source_identifier: str, 
        dest_filename: str,
        dest_category: Optional[FileCategory] = None,
        dest_user_id: Optional[str] = None,
    ) -> StorageFileInfo:
        """
        Move file to new location.
        
        Args:
            source_identifier: Source file identifier
            dest_filename: Destination filename
            dest_category: Destination category
            dest_user_id: Destination user ID
            
        Returns:
            New file information
        """
        # Get source file info
        source_info = self.storage.get_file_info(source_identifier)
        
        # Determine destination
        if not dest_category:
            dest_category = self.validator.get_file_category(
                dest_filename, 
                source_info.content_type
            )
        
        dest_subfolder = self._get_subfolder_for_category(
            dest_category, 
            None, 
            dest_user_id
        )
        
        # Generate destination identifier
        dest_identifier = self._generate_file_path(dest_filename, dest_subfolder)
        
        return self.storage.move_file(source_identifier, dest_identifier)
    
    def create_temporary_file(
        self, 
        file_data: Union[bytes, BinaryIO], 
        filename: str,
        expires_in: int = 3600,
    ) -> Tuple[str, str]:
        """
        Create temporary file.
        
        Args:
            file_data: File content
            filename: Original filename
            expires_in: Expiration time in seconds
            
        Returns:
            Tuple of (file_identifier, file_url)
        """
        # Add expiration metadata
        metadata = {
            "temporary": True,
            "expires_at": (datetime.utcnow() + timedelta(seconds=expires_in)).isoformat(),
            "created_by": "storage_service",
        }
        
        file_info = self.temp_storage.save_file(
            file_data=file_data,
            filename=filename,
            subfolder="temp",
            metadata=metadata,
        )
        
        file_url = self.temp_storage.get_file_url(file_info.identifier, expires_in)
        
        logger.info(f"Temporary file created: {filename} (expires in {expires_in}s)")
        
        return file_info.identifier, file_url
    
    def cleanup_temporary_files(self, older_than: Optional[datetime] = None) -> int:
        """
        Clean up expired temporary files.
        
        Args:
            older_than: Remove files older than this datetime (uses current time if None)
            
        Returns:
            Number of files cleaned up
        """
        if older_than is None:
            older_than = datetime.utcnow()
        
        try:
            temp_files = self.temp_storage.list_files(subfolder="temp")
            cleaned_count = 0
            
            for file_info in temp_files:
                # Check if file has expiration metadata
                metadata = file_info.metadata or {}
                expires_at_str = metadata.get("expires_at")
                
                if expires_at_str:
                    try:
                        expires_at = datetime.fromisoformat(expires_at_str.replace('Z', '+00:00'))
                        if expires_at <= older_than:
                            self.temp_storage.delete_file(file_info.identifier)
                            cleaned_count += 1
                            logger.debug(f"Cleaned up expired temp file: {file_info.identifier}")
                    except ValueError:
                        logger.warning(f"Invalid expiration date in metadata: {expires_at_str}")
                
                # Also check file modification time as fallback
                elif file_info.modified_at <= older_than - timedelta(hours=24):
                    # Remove files older than 24 hours if no expiration metadata
                    self.temp_storage.delete_file(file_info.identifier)
                    cleaned_count += 1
                    logger.debug(f"Cleaned up old temp file: {file_info.identifier}")
            
            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} temporary files")
            
            return cleaned_count
            
        except Exception as e:
            logger.error(f"Failed to cleanup temporary files: {e}")
            return 0
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """
        Get storage statistics.
        
        Returns:
            Storage statistics
        """
        stats = self.storage.get_storage_stats()
        
        # Add service-level statistics
        stats.update({
            "service_version": "1.0.0",
            "supported_categories": [category.value for category in FileCategory],
            "validator_extensions": {
                "images": list(self.validator.IMAGE_EXTENSIONS),
                "documents": list(self.validator.DOCUMENT_EXTENSIONS),
                "videos": list(self.validator.VIDEO_EXTENSIONS),
                "audio": list(self.validator.AUDIO_EXTENSIONS),
                "archives": list(self.validator.ARCHIVE_EXTENSIONS),
            }
        })
        
        return stats
    
    def _validate_upload_request(self, request: UploadRequest) -> None:
        """
        Validate upload request.
        
        Args:
            request: Upload request to validate
            
        Raises:
            FileValidationError: If validation fails
        """
        # Validate filename
        if not request.filename or not request.filename.strip():
            raise FileValidationError("Filename is required")
        
        # Validate file extension
        self.validator.validate_file_extension(request.filename)
        
        # Validate content type
        request.content_type = self.validator.validate_content_type(
            request.content_type, 
            request.filename
        )
        
        # Validate file size
        if isinstance(request.file_data, bytes):
            file_size = len(request.file_data)
        else:
            # For file-like objects, try to get size
            current_pos = request.file_data.tell()
            request.file_data.seek(0, 2)  # Seek to end
            file_size = request.file_data.tell()
            request.file_data.seek(current_pos)  # Restore position
        
        self.validator.validate_file_size(file_size)
    
    def _prepare_metadata(self, request: UploadRequest) -> Dict[str, Any]:
        """
        Prepare metadata for file upload.
        
        Args:
            request: Upload request
            
        Returns:
            Metadata dictionary
        """
        metadata = request.metadata.copy() if request.metadata else {}
        
        # Add standard metadata
        metadata.update({
            "original_filename": request.filename,
            "upload_time": datetime.utcnow().isoformat(),
            "content_type": request.content_type,
        })
        
        if request.category:
            metadata["category"] = request.category.value
        
        if request.user_id:
            metadata["user_id"] = request.user_id
        
        if request.tags:
            metadata["tags"] = ",".join(request.tags)
        
        # Add file type information
        metadata["is_image"] = self.validator.is_image_file(
            request.filename, 
            request.content_type
        )
        
        return metadata
    
    def _get_subfolder_for_category(
        self, 
        category: FileCategory, 
        custom_subfolder: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> str:
        """
        Generate subfolder path based on category and user.
        
        Args:
            category: File category
            custom_subfolder: Custom subfolder override
            user_id: User ID for user-specific folders
            
        Returns:
            Subfolder path
        """
        if custom_subfolder:
            return custom_subfolder
        
        parts = []
        
        if user_id:
            parts.append("users")
            parts.append(user_id)
        
        if category:
            parts.append(category.value)
        
        # Add date-based organization for better scalability
        now = datetime.utcnow()
        parts.extend([str(now.year), f"{now.month:02d}"])
        
        return "/".join(parts)
    
    def _generate_file_path(self, filename: str, subfolder: str) -> str:
        """
        Generate full file path.
        
        Args:
            filename: Filename
            subfolder: Subfolder path
            
        Returns:
            Full file path
        """
        if subfolder:
            return f"{subfolder}/{filename}"
        return filename
    
    def _filter_by_tags(
        self, 
        files: List[StorageFileInfo], 
        tags: List[str]
    ) -> List[StorageFileInfo]:
        """
        Filter files by tags.
        
        Args:
            files: List of files to filter
            tags: Tags to filter by
            
        Returns:
            Filtered list of files
        """
        filtered_files = []
        
        for file_info in files:
            file_tags_str = file_info.metadata.get("tags", "")
            if file_tags_str:
                file_tags = set(file_tags_str.split(","))
                if any(tag in file_tags for tag in tags):
                    filtered_files.append(file_info)
        
        return filtered_files


# Convenience functions
def create_storage_service(
    storage_type: Optional[StorageType] = None,
    **config
) -> StorageService:
    """
    Create storage service with specified backend.
    
    Args:
        storage_type: Type of storage backend
        **config: Storage configuration
        
    Returns:
        StorageService instance
    """
    if storage_type:
        from .factory import create_storage
        storage_backend = create_storage(storage_type, **config)
        return StorageService(storage_backend)
    else:
        return StorageService()


def get_storage_service() -> StorageService:
    """
    Get default storage service instance.
    
    Returns:
        StorageService instance
    """
    return StorageService()


# File upload helper functions
def upload_file_from_path(
    file_path: str,
    filename: Optional[str] = None,
    category: Optional[FileCategory] = None,
    user_id: Optional[str] = None,
    **metadata
) -> UploadResult:
    """
    Upload file from filesystem path.
    
    Args:
        file_path: Path to file
        filename: Override filename (uses original if None)
        category: File category
        user_id: User ID
        **metadata: Additional metadata
        
    Returns:
        Upload result
    """
    path = Path(file_path)
    
    if not path.exists():
        raise FileValidationError(f"File not found: {file_path}")
    
    with open(path, 'rb') as f:
        request = UploadRequest(
            file_data=f,
            filename=filename or path.name,
            category=category,
            user_id=user_id,
            metadata=metadata,
        )
        
        service = get_storage_service()
        return service.upload_file(request)


def upload_file_from_bytes(
    file_data: bytes,
    filename: str,
    content_type: Optional[str] = None,
    category: Optional[FileCategory] = None,
    user_id: Optional[str] = None,
    **metadata
) -> UploadResult:
    """
    Upload file from bytes data.
    
    Args:
        file_data: File content as bytes
        filename: Filename
        content_type: Content type
        category: File category
        user_id: User ID
        **metadata: Additional metadata
        
    Returns:
        Upload result
    """
    request = UploadRequest(
        file_data=file_data,
        filename=filename,
        content_type=content_type,
        category=category,
        user_id=user_id,
        metadata=metadata,
    )
    
    service = get_storage_service()
    return service.upload_file(request)
