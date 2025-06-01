"""
Base storage interface and abstract classes.

This module defines the abstract base classes and interfaces
for file storage implementations to ensure consistent API.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List, BinaryIO, Union
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path


@dataclass
class StorageFileInfo:
    """Generic file information metadata."""
    
    identifier: str  # File path/key/ID
    filename: str
    size: int
    content_type: str
    created_at: datetime
    modified_at: datetime
    checksum: str
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "identifier": self.identifier,
            "filename": self.filename,
            "size": self.size,
            "content_type": self.content_type,
            "created_at": self.created_at.isoformat(),
            "modified_at": self.modified_at.isoformat(),
            "checksum": self.checksum,
            "metadata": self.metadata,
        }


class StorageBackend(ABC):
    """
    Abstract base class for storage backends.
    
    This defines the interface that all storage implementations
    must follow to ensure consistency across different providers.
    """
    
    @abstractmethod
    def save_file(
        self,
        file_data: Union[bytes, BinaryIO],
        filename: str,
        subfolder: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> StorageFileInfo:
        """
        Save file to storage.
        
        Args:
            file_data: File content as bytes or file-like object
            filename: Original filename
            subfolder: Optional subfolder within storage
            metadata: Additional metadata to store
            **kwargs: Provider-specific arguments
            
        Returns:
            StorageFileInfo object with file details
        """
        pass
    
    @abstractmethod
    def get_file(self, identifier: str) -> bytes:
        """
        Get file content.
        
        Args:
            identifier: File identifier (path/key/ID)
            
        Returns:
            File content as bytes
        """
        pass
    
    @abstractmethod
    def get_file_info(self, identifier: str) -> StorageFileInfo:
        """
        Get file information.
        
        Args:
            identifier: File identifier (path/key/ID)
            
        Returns:
            StorageFileInfo object
        """
        pass
    
    @abstractmethod
    def delete_file(self, identifier: str) -> bool:
        """
        Delete file from storage.
        
        Args:
            identifier: File identifier (path/key/ID)
            
        Returns:
            True if file was deleted, False if not found
        """
        pass
    
    @abstractmethod
    def file_exists(self, identifier: str) -> bool:
        """
        Check if file exists.
        
        Args:
            identifier: File identifier (path/key/ID)
            
        Returns:
            True if file exists
        """
        pass
    
    @abstractmethod
    def list_files(
        self,
        subfolder: Optional[str] = None,
        pattern: str = "*",
        limit: Optional[int] = None,
        **kwargs
    ) -> List[StorageFileInfo]:
        """
        List files in storage.
        
        Args:
            subfolder: Subfolder to list (None for root)
            pattern: File pattern to match
            limit: Maximum number of files to return
            **kwargs: Provider-specific arguments
            
        Returns:
            List of StorageFileInfo objects
        """
        pass
    
    @abstractmethod
    def get_file_url(
        self,
        identifier: str,
        expires_in: Optional[int] = None,
        **kwargs
    ) -> str:
        """
        Get URL for file access.
        
        Args:
            identifier: File identifier (path/key/ID)
            expires_in: URL expiration time in seconds
            **kwargs: Provider-specific arguments
            
        Returns:
            File URL
        """
        pass
    
    def copy_file(
        self,
        source_identifier: str,
        dest_identifier: str,
        **kwargs
    ) -> StorageFileInfo:
        """
        Copy file within storage.
        
        Args:
            source_identifier: Source file identifier
            dest_identifier: Destination file identifier
            **kwargs: Provider-specific arguments
            
        Returns:
            StorageFileInfo for copied file
        """
        # Default implementation using get and save
        content = self.get_file(source_identifier)
        source_info = self.get_file_info(source_identifier)
        
        return self.save_file(
            content,
            source_info.filename,
            metadata=source_info.metadata,
            **kwargs
        )
    
    def move_file(
        self,
        source_identifier: str,
        dest_identifier: str,
        **kwargs
    ) -> StorageFileInfo:
        """
        Move file within storage.
        
        Args:
            source_identifier: Source file identifier
            dest_identifier: Destination file identifier
            **kwargs: Provider-specific arguments
            
        Returns:
            StorageFileInfo for moved file
        """
        # Default implementation using copy and delete
        file_info = self.copy_file(source_identifier, dest_identifier, **kwargs)
        self.delete_file(source_identifier)
        return file_info
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """
        Get storage statistics.
        
        Returns:
            Dictionary with storage stats
        """
        # Default implementation - override in specific backends
        return {
            "backend_type": self.__class__.__name__,
            "features_supported": [
                "save_file",
                "get_file",
                "delete_file",
                "list_files",
                "file_exists",
            ]
        }


class StorageException(Exception):
    """Base exception for storage operations."""
    
    def __init__(self, message: str, error_code: str = None, details: Dict[str, Any] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}


class FileNotFoundError(StorageException):
    """File not found in storage."""
    
    def __init__(self, identifier: str):
        super().__init__(
            f"File not found: {identifier}",
            error_code="FILE_NOT_FOUND",
            details={"identifier": identifier}
        )


class FileExistsError(StorageException):
    """File already exists in storage."""
    
    def __init__(self, identifier: str):
        super().__init__(
            f"File already exists: {identifier}",
            error_code="FILE_EXISTS",
            details={"identifier": identifier}
        )


class StorageQuotaExceededError(StorageException):
    """Storage quota exceeded."""
    
    def __init__(self, current_size: int, max_size: int):
        super().__init__(
            f"Storage quota exceeded: {current_size} > {max_size}",
            error_code="QUOTA_EXCEEDED",
            details={"current_size": current_size, "max_size": max_size}
        )


class InvalidFileError(StorageException):
    """Invalid file data or format."""
    
    def __init__(self, reason: str):
        super().__init__(
            f"Invalid file: {reason}",
            error_code="INVALID_FILE",
            details={"reason": reason}
        )


class StorageConfigurationError(StorageException):
    """Storage configuration error."""
    
    def __init__(self, message: str):
        super().__init__(
            f"Storage configuration error: {message}",
            error_code="CONFIG_ERROR",
            details={"message": message}
        )


class StoragePermissionError(StorageException):
    """Storage permission error."""
    
    def __init__(self, operation: str, identifier: str = None):
        message = f"Permission denied for {operation}"
        if identifier:
            message += f" on {identifier}"
        
        super().__init__(
            message,
            error_code="PERMISSION_DENIED",
            details={"operation": operation, "identifier": identifier}
        )
        