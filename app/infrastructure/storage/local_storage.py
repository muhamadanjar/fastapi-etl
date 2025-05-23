"""
Local file storage implementation.

This module provides local filesystem storage functionality
with proper path handling, security, and metadata management.
"""

import hashlib
import logging
import mimetypes
import os
import shutil
from pathlib import Path
from typing import Optional, Dict, Any, List, BinaryIO, Union
from datetime import datetime
from dataclasses import dataclass

from ...core.config import get_settings
from ...core.exceptions import FileStorageError

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class FileInfo:
    """File information metadata."""
    
    filename: str
    file_path: str
    size: int
    content_type: str
    created_at: datetime
    modified_at: datetime
    md5_hash: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "filename": self.filename,
            "file_path": self.file_path,
            "size": self.size,
            "content_type": self.content_type,
            "created_at": self.created_at.isoformat(),
            "modified_at": self.modified_at.isoformat(),
            "md5_hash": self.md5_hash,
        }


class LocalFileStorage:
    """
    Local filesystem storage with security and metadata features.
    
    Provides secure file storage on local filesystem with proper
    path validation, content type detection, and metadata tracking.
    """
    
    def __init__(
        self,
        base_path: Optional[str] = None,
        create_dirs: bool = True,
        allowed_extensions: Optional[List[str]] = None,
        max_file_size: Optional[int] = None,  # bytes
        preserve_filename: bool = True,
    ):
        """
        Initialize local file storage.
        
        Args:
            base_path: Base directory for file storage
            create_dirs: Whether to create directories if they don't exist
            allowed_extensions: List of allowed file extensions
            max_file_size: Maximum file size in bytes
            preserve_filename: Whether to preserve original filenames
        """
        self.base_path = Path(base_path or settings.storage.local_storage_path)
        self.create_dirs = create_dirs
        self.allowed_extensions = allowed_extensions
        self.max_file_size = max_file_size
        self.preserve_filename = preserve_filename
        
        # Ensure base directory exists
        if self.create_dirs:
            self.base_path.mkdir(parents=True, exist_ok=True)
        
        if not self.base_path.exists():
            raise FileStorageError(f"Storage directory does not exist: {self.base_path}")
        
        if not self.base_path.is_dir():
            raise FileStorageError(f"Storage path is not a directory: {self.base_path}")
        
        logger.info(f"Local file storage initialized at {self.base_path}")
    
    def save_file(
        self,
        file_data: Union[bytes, BinaryIO],
        filename: str,
        subfolder: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> FileInfo:
        """
        Save file to storage.
        
        Args:
            file_data: File content as bytes or file-like object
            filename: Original filename
            subfolder: Optional subfolder within base path
            metadata: Additional metadata to store
            
        Returns:
            FileInfo object with file details
            
        Raises:
            FileStorageError: If file saving fails
        """
        try:
            # Validate filename
            safe_filename = self._sanitize_filename(filename)
            self._validate_file(safe_filename, file_data)
            
            # Determine storage path
            storage_dir = self.base_path
            if subfolder:
                storage_dir = storage_dir / self._sanitize_path(subfolder)
                storage_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate unique filename if needed
            if self.preserve_filename:
                final_filename = self._get_unique_filename(storage_dir, safe_filename)
            else:
                final_filename = self._generate_filename(safe_filename)
            
            file_path = storage_dir / final_filename
            
            # Get file content
            if isinstance(file_data, bytes):
                content = file_data
            else:
                content = file_data.read()
            
            # Calculate MD5 hash
            md5_hash = hashlib.md5(content).hexdigest()
            
            # Write file
            with open(file_path, 'wb') as f:
                f.write(content)
            
            # Get file stats
            stat = file_path.stat()
            content_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
            
            # Create file info
            file_info = FileInfo(
                filename=safe_filename,
                file_path=str(file_path.relative_to(self.base_path)),
                size=stat.st_size,
                content_type=content_type,
                created_at=datetime.fromtimestamp(stat.st_ctime),
                modified_at=datetime.fromtimestamp(stat.st_mtime),
                md5_hash=md5_hash,
            )
            
            # Save metadata if provided
            if metadata:
                self._save_metadata(file_path, metadata)
            
            logger.info(f"File saved: {final_filename} ({file_info.size} bytes)")
            return file_info
            
        except Exception as e:
            logger.error(f"Failed to save file {filename}: {e}")
            raise FileStorageError(f"File save failed: {e}")
    
    def get_file(self, file_path: str) -> bytes:
        """
        Get file content.
        
        Args:
            file_path: Relative path to file
            
        Returns:
            File content as bytes
            
        Raises:
            FileStorageError: If file not found or read fails
        """
        try:
            full_path = self.base_path / file_path
            self._validate_path(full_path)
            
            if not full_path.exists():
                raise FileStorageError(f"File not found: {file_path}")
            
            with open(full_path, 'rb') as f:
                content = f.read()
            
            logger.debug(f"File retrieved: {file_path}")
            return content
            
        except FileStorageError:
            raise
        except Exception as e:
            logger.error(f"Failed to get file {file_path}: {e}")
            raise FileStorageError(f"File retrieval failed: {e}")
    
    def get_file_info(self, file_path: str) -> FileInfo:
        """
        Get file information.
        
        Args:
            file_path: Relative path to file
            
        Returns:
            FileInfo object
            
        Raises:
            FileStorageError: If file not found
        """
        try:
            full_path = self.base_path / file_path
            self._validate_path(full_path)
            
            if not full_path.exists():
                raise FileStorageError(f"File not found: {file_path}")
            
            stat = full_path.stat()
            content_type = mimetypes.guess_type(str(full_path))[0] or "application/octet-stream"
            
            # Calculate MD5 hash
            with open(full_path, 'rb') as f:
                md5_hash = hashlib.md5(f.read()).hexdigest()
            
            return FileInfo(
                filename=full_path.name,
                file_path=file_path,
                size=stat.st_size,
                content_type=content_type,
                created_at=datetime.fromtimestamp(stat.st_ctime),
                modified_at=datetime.fromtimestamp(stat.st_mtime),
                md5_hash=md5_hash,
            )
            
        except FileStorageError:
            raise
        except Exception as e:
            logger.error(f"Failed to get file info for {file_path}: {e}")
            raise FileStorageError(f"File info retrieval failed: {e}")
    
    def delete_file(self, file_path: str) -> bool:
        """
        Delete file from storage.
        
        Args:
            file_path: Relative path to file
            
        Returns:
            True if file was deleted, False if not found
            
        Raises:
            FileStorageError: If deletion fails
        """
        try:
            full_path = self.base_path / file_path
            self._validate_path(full_path)
            
            if not full_path.exists():
                logger.debug(f"File not found for deletion: {file_path}")
                return False
            
            full_path.unlink()
            
            # Delete metadata file if exists
            metadata_path = self._get_metadata_path(full_path)
            if metadata_path.exists():
                metadata_path.unlink()
            
            logger.info(f"File deleted: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete file {file_path}: {e}")
            raise FileStorageError(f"File deletion failed: {e}")
    
    def file_exists(self, file_path: str) -> bool:
        """
        Check if file exists.
        
        Args:
            file_path: Relative path to file
            
        Returns:
            True if file exists
        """
        try:
            full_path = self.base_path / file_path
            return full_path.exists() and full_path.is_file()
        except Exception:
            return False
    
    def list_files(
        self,
        subfolder: Optional[str] = None,
        pattern: str = "*",
        recursive: bool = False,
    ) -> List[FileInfo]:
        """
        List files in storage.
        
        Args:
            subfolder: Subfolder to list (None for root)
            pattern: File pattern to match
            recursive: Whether to list recursively
            
        Returns:
            List of FileInfo objects
        """
        try:
            search_dir = self.base_path
            if subfolder:
                search_dir = search_dir / self._sanitize_path(subfolder)
            
            if not search_dir.exists():
                return []
            
            files = []
            
            if recursive:
                file_paths = search_dir.rglob(pattern)
            else:
                file_paths = search_dir.glob(pattern)
            
            for file_path in file_paths:
                if file_path.is_file() and not file_path.name.endswith('.metadata'):
                    try:
                        rel_path = str(file_path.relative_to(self.base_path))
                        file_info = self.get_file_info(rel_path)
                        files.append(file_info)
                    except Exception as e:
                        logger.warning(f"Failed to get info for file {file_path}: {e}")
            
            logger.debug(f"Listed {len(files)} files in {search_dir}")
            return files
            
        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            raise FileStorageError(f"File listing failed: {e}")
    
    def get_file_url(self, file_path: str, expires_in: Optional[int] = None) -> str:
        """
        Get URL for file (for local storage, returns file path).
        
        Args:
            file_path: Relative path to file
            expires_in: Expiration time (not used for local storage)
            
        Returns:
            File URL/path
        """
        return f"/files/{file_path}"
    
    def copy_file(self, source_path: str, dest_path: str) -> FileInfo:
        """
        Copy file within storage.
        
        Args:
            source_path: Source file path
            dest_path: Destination file path
            
        Returns:
            FileInfo for copied file
        """
        try:
            source_full = self.base_path / source_path
            dest_full = self.base_path / dest_path
            
            self._validate_path(source_full)
            self._validate_path(dest_full)
            
            if not source_full.exists():
                raise FileStorageError(f"Source file not found: {source_path}")
            
            # Create destination directory if needed
            dest_full.parent.mkdir(parents=True, exist_ok=True)
            
            # Copy file
            shutil.copy2(source_full, dest_full)
            
            # Copy metadata if exists
            source_metadata = self._get_metadata_path(source_full)
            if source_metadata.exists():
                dest_metadata = self._get_metadata_path(dest_full)
                shutil.copy2(source_metadata, dest_metadata)
            
            logger.info(f"File copied: {source_path} -> {dest_path}")
            return self.get_file_info(dest_path)
            
        except FileStorageError:
            raise
        except Exception as e:
            logger.error(f"Failed to copy file: {e}")
            raise FileStorageError(f"File copy failed: {e}")
    
    def move_file(self, source_path: str, dest_path: str) -> FileInfo:
        """
        Move file within storage.
        
        Args:
            source_path: Source file path
            dest_path: Destination file path
            
        Returns:
            FileInfo for moved file
        """
        try:
            source_full = self.base_path / source_path
            dest_full = self.base_path / dest_path
            
            self._validate_path(source_full)
            self._validate_path(dest_full)
            
            if not source_full.exists():
                raise FileStorageError(f"Source file not found: {source_path}")
            
            # Create destination directory if needed
            dest_full.parent.mkdir(parents=True, exist_ok=True)
            
            # Move file
            shutil.move(str(source_full), str(dest_full))
            
            # Move metadata if exists
            source_metadata = self._get_metadata_path(source_full)
            dest_metadata = self._get_metadata_path(dest_full)
            if source_metadata.exists():
                shutil.move(str(source_metadata), str(dest_metadata))
            
            logger.info(f"File moved: {source_path} -> {dest_path}")
            return self.get_file_info(dest_path)
            
        except FileStorageError:
            raise
        except Exception as e:
            logger.error(f"Failed to move file: {e}")
            raise FileStorageError(f"File move failed: {e}")
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """
        Get storage statistics.
        
        Returns:
            Dictionary with storage stats
        """
        try:
            total_size = 0
            file_count = 0
            
            for file_path in self.base_path.rglob("*"):
                if file_path.is_file() and not file_path.name.endswith('.metadata'):
                    total_size += file_path.stat().st_size
                    file_count += 1
            
            # Get available space
            usage = shutil.disk_usage(self.base_path)
            
            return {
                "base_path": str(self.base_path),
                "total_files": file_count,
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "disk_total_bytes": usage.total,
                "disk_used_bytes": usage.used,
                "disk_free_bytes": usage.free,
                "disk_usage_percent": round((usage.used / usage.total) * 100, 2),
            }
            
        except Exception as e:
            logger.error(f"Failed to get storage stats: {e}")
            raise FileStorageError(f"Storage stats retrieval failed: {e}")
    
    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename to prevent security issues."""
        # Remove directory components
        safe_name = os.path.basename(filename)
        
        # Remove or replace dangerous characters
        dangerous_chars = ['<', '>', ':', '"', '|', '?', '*', '..', '/', '\\']
        for char in dangerous_chars:
            safe_name = safe_name.replace(char, '_')
        
        # Remove leading/trailing whitespace and dots
        safe_name = safe_name.strip(' .')
        
        # Ensure filename is not empty
        if not safe_name:
            safe_name = "unnamed_file"
        
        return safe_name
    
    def _sanitize_path(self, path: str) -> str:
        """Sanitize path components."""
        # Remove dangerous path components
        parts = []
        for part in Path(path).parts:
            if part not in ['.', '..', '']:
                parts.append(self._sanitize_filename(part))
        
        return str(Path(*parts)) if parts else ""
    
    def _validate_path(self, full_path: Path) -> None:
        """Validate that path is within base directory."""
        try:
            full_path.resolve().relative_to(self.base_path.resolve())
        except ValueError:
            raise FileStorageError("Path traversal attempt detected")
    
    def _validate_file(self, filename: str, file_data: Union[bytes, BinaryIO]) -> None:
        """Validate file before saving."""
        # Check file extension
        if self.allowed_extensions:
            file_ext = Path(filename).suffix.lower()
            if file_ext not in self.allowed_extensions:
                raise FileStorageError(
                    f"File extension {file_ext} not allowed. "
                    f"Allowed: {', '.join(self.allowed_extensions)}"
                )
        
        # Check file size
        if self.max_file_size:
            if isinstance(file_data, bytes):
                size = len(file_data)
            else:
                # For file-like objects, try to get size
                current_pos = file_data.tell()
                file_data.seek(0, 2)  # Seek to end
                size = file_data.tell()
                file_data.seek(current_pos)  # Restore position
            
            if size > self.max_file_size:
                raise FileStorageError(
                    f"File size {size} bytes exceeds maximum {self.max_file_size} bytes"
                )
    
    def _get_unique_filename(self, directory: Path, filename: str) -> str:
        """Get unique filename by adding counter if file exists."""
        base_path = directory / filename
        
        if not base_path.exists():
            return filename
        
        # Split filename and extension
        stem = Path(filename).stem
        suffix = Path(filename).suffix
        
        counter = 1
        while True:
            new_filename = f"{stem}_{counter}{suffix}"
            if not (directory / new_filename).exists():
                return new_filename
            counter += 1
    
    def _generate_filename(self, original_filename: str) -> str:
        """Generate unique filename using timestamp and hash."""
        import uuid
        
        # Get file extension
        suffix = Path(original_filename).suffix
        
        # Generate unique name
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        
        return f"{timestamp}_{unique_id}{suffix}"
    
    def _save_metadata(self, file_path: Path, metadata: Dict[str, Any]) -> None:
        """Save metadata for file."""
        import json
        
        metadata_path = self._get_metadata_path(file_path)
        
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, default=str, indent=2)
    
    def _get_metadata_path(self, file_path: Path) -> Path:
        """Get metadata file path for given file."""
        return file_path.with_suffix(file_path.suffix + '.metadata')
    
    def get_metadata(self, file_path: str) -> Dict[str, Any]:
        """
        Get metadata for file.
        
        Args:
            file_path: Relative path to file
            
        Returns:
            Metadata dictionary
        """
        try:
            full_path = self.base_path / file_path
            metadata_path = self._get_metadata_path(full_path)
            
            if not metadata_path.exists():
                return {}
            
            import json
            with open(metadata_path, 'r') as f:
                return json.load(f)
                
        except Exception as e:
            logger.warning(f"Failed to get metadata for {file_path}: {e}")
            return {}
    
    def set_metadata(self, file_path: str, metadata: Dict[str, Any]) -> None:
        """
        Set metadata for file.
        
        Args:
            file_path: Relative path to file
            metadata: Metadata to save
        """
        try:
            full_path = self.base_path / file_path
            self._validate_path(full_path)
            
            if not full_path.exists():
                raise FileStorageError(f"File not found: {file_path}")
            
            self._save_metadata(full_path, metadata)
            logger.debug(f"Metadata saved for {file_path}")
            
        except FileStorageError:
            raise
        except Exception as e:
            logger.error(f"Failed to set metadata for {file_path}: {e}")
            raise FileStorageError(f"Metadata save failed: {e}")


# Utility functions for file operations
def ensure_directory(directory: Union[str, Path]) -> Path:
    """
    Ensure directory exists, create if necessary.
    
    Args:
        directory: Directory path
        
    Returns:
        Path object for directory
    """
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_file_hash(file_path: Union[str, Path], algorithm: str = "md5") -> str:
    """
    Calculate hash of file.
    
    Args:
        file_path: Path to file
        algorithm: Hash algorithm (md5, sha1, sha256)
        
    Returns:
        Hex digest of file hash
    """
    hash_obj = hashlib.new(algorithm)
    
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_obj.update(chunk)
    
    return hash_obj.hexdigest()


def get_file_mime_type(filename: str) -> str:
    """
    Get MIME type for file.
    
    Args:
        filename: Filename or path
        
    Returns:
        MIME type string
    """
    mime_type, _ = mimetypes.guess_type(filename)
    return mime_type or "application/octet-stream"


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human readable format.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Formatted size string
    """
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.1f} {size_names[i]}"


class FileValidator:
    """File validation utility."""
    
    ALLOWED_IMAGE_TYPES = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']
    ALLOWED_DOCUMENT_TYPES = ['.pdf', '.doc', '.docx', '.txt', '.rtf']
    ALLOWED_ARCHIVE_TYPES = ['.zip', '.tar', '.gz', '.rar', '.7z']
    
    @staticmethod
    def is_image(filename: str) -> bool:
        """Check if file is an image."""
        return Path(filename).suffix.lower() in FileValidator.ALLOWED_IMAGE_TYPES
    
    @staticmethod
    def is_document(filename: str) -> bool:
        """Check if file is a document."""
        return Path(filename).suffix.lower() in FileValidator.ALLOWED_DOCUMENT_TYPES
    
    @staticmethod
    def is_archive(filename: str) -> bool:
        """Check if file is an archive."""
        return Path(filename).suffix.lower() in FileValidator.ALLOWED_ARCHIVE_TYPES
    
    @staticmethod
    def validate_image_content(file_content: bytes) -> bool:
        """
        Validate that file content is actually an image.
        
        Args:
            file_content: File content as bytes
            
        Returns:
            True if content is valid image
        """
        # Check for common image file signatures
        image_signatures = {
            b'\xff\xd8\xff': 'jpeg',
            b'\x89PNG\r\n\x1a\n': 'png',
            b'GIF87a': 'gif',
            b'GIF89a': 'gif',
            b'BM': 'bmp',
            b'RIFF': 'webp',  # WebP files start with RIFF
        }
        
        for signature in image_signatures:
            if file_content.startswith(signature):
                return True
        
        return False


class TemporaryFileManager:
    """Manager for temporary files."""
    
    def __init__(self, temp_dir: Optional[str] = None):
        """
        Initialize temporary file manager.
        
        Args:
            temp_dir: Temporary directory path
        """
        import tempfile
        
        self.temp_dir = Path(temp_dir) if temp_dir else Path(tempfile.gettempdir())
        self.temp_files: List[Path] = []
    
    def create_temp_file(
        self,
        content: bytes,
        suffix: str = "",
        prefix: str = "temp_",
    ) -> Path:
        """
        Create temporary file with content.
        
        Args:
            content: File content
            suffix: File suffix/extension
            prefix: File prefix
            
        Returns:
            Path to temporary file
        """
        import tempfile
        
        fd, temp_path = tempfile.mkstemp(
            suffix=suffix,
            prefix=prefix,
            dir=self.temp_dir
        )
        
        try:
            with os.fdopen(fd, 'wb') as f:
                f.write(content)
            
            temp_file = Path(temp_path)
            self.temp_files.append(temp_file)
            return temp_file
            
        except Exception:
            os.close(fd)
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise
    
    def cleanup(self) -> None:
        """Clean up all temporary files."""
        for temp_file in self.temp_files:
            try:
                if temp_file.exists():
                    temp_file.unlink()
            except Exception as e:
                logger.warning(f"Failed to cleanup temp file {temp_file}: {e}")
        
        self.temp_files.clear()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()