"""
Local storage adapter implementing the base storage interface.

This module adapts the existing LocalFileStorage implementation
to conform to the base StorageBackend interface.
"""

import logging
from typing import Optional, Dict, Any, List, BinaryIO, Union
from datetime import datetime

from .base import StorageBackend, StorageFileInfo, FileNotFoundError
from .local_storage import LocalFileStorage, FileInfo

logger = logging.getLogger(__name__)


class LocalStorageAdapter(StorageBackend):
    """
    Adapter for LocalFileStorage to implement StorageBackend interface.
    
    This adapter wraps the existing LocalFileStorage implementation
    and provides the standardized StorageBackend interface.
    """
    
    def __init__(
        self,
        base_path: Optional[str] = None,
        create_dirs: bool = True,
        allowed_extensions: Optional[List[str]] = None,
        max_file_size: Optional[int] = None,
        preserve_filename: bool = True,
        **kwargs
    ):
        """
        Initialize local storage adapter.
        
        Args:
            base_path: Base directory for file storage
            create_dirs: Whether to create directories if they don't exist
            allowed_extensions: List of allowed file extensions
            max_file_size: Maximum file size in bytes
            preserve_filename: Whether to preserve original filenames
            **kwargs: Additional configuration (ignored)
        """
        self._storage = LocalFileStorage(
            base_path=base_path,
            create_dirs=create_dirs,
            allowed_extensions=allowed_extensions,
            max_file_size=max_file_size,
            preserve_filename=preserve_filename,
        )
        
        logger.info(f"Local storage adapter initialized at {self._storage.base_path}")
    
    def save_file(
        self,
        file_data: Union[bytes, BinaryIO],
        filename: str,
        subfolder: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> StorageFileInfo:
        """Save file to local storage."""
        try:
            file_info = self._storage.save_file(
                file_data=file_data,
                filename=filename,
                subfolder=subfolder,
                metadata=metadata,
            )
            
            return self._convert_file_info(file_info)
            
        except Exception as e:
            logger.error(f"Failed to save file {filename}: {e}")
            raise
    
    def get_file(self, identifier: str) -> bytes:
        """Get file content from local storage."""
        try:
            return self._storage.get_file(identifier)
        except Exception as e:
            if "not found" in str(e).lower():
                raise FileNotFoundError(identifier)
            raise
    
    def get_file_info(self, identifier: str) -> StorageFileInfo:
        """Get file information from local storage."""
        try:
            file_info = self._storage.get_file_info(identifier)
            return self._convert_file_info(file_info)
        except Exception as e:
            if "not found" in str(e).lower():
                raise FileNotFoundError(identifier)
            raise
    
    def delete_file(self, identifier: str) -> bool:
        """Delete file from local storage."""
        return self._storage.delete_file(identifier)
    
    def file_exists(self, identifier: str) -> bool:
        """Check if file exists in local storage."""
        return self._storage.file_exists(identifier)
    
    def list_files(
        self,
        subfolder: Optional[str] = None,
        pattern: str = "*",
        limit: Optional[int] = None,
        **kwargs
    ) -> List[StorageFileInfo]:
        """List files in local storage."""
        recursive = kwargs.get("recursive", False)
        
        files = self._storage.list_files(
            subfolder=subfolder,
            pattern=pattern,
            recursive=recursive,
        )
        
        # Convert to StorageFileInfo and apply limit
        result = [self._convert_file_info(file_info) for file_info in files]
        
        if limit:
            result = result[:limit]
        
        return result
    
    def get_file_url(
        self,
        identifier: str,
        expires_in: Optional[int] = None,
        **kwargs
    ) -> str:
        """Get URL for file access."""
        return self._storage.get_file_url(identifier, expires_in)
    
    def copy_file(
        self,
        source_identifier: str,
        dest_identifier: str,
        **kwargs
    ) -> StorageFileInfo:
        """Copy file within local storage."""
        file_info = self._storage.copy_file(source_identifier, dest_identifier)
        return self._convert_file_info(file_info)
    
    def move_file(
        self,
        source_identifier: str,
        dest_identifier: str,
        **kwargs
    ) -> StorageFileInfo:
        """Move file within local storage."""
        file_info = self._storage.move_file(source_identifier, dest_identifier)
        return self._convert_file_info(file_info)
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get local storage statistics."""
        stats = self._storage.get_storage_stats()
        
        # Add adapter-specific information
        stats.update({
            "backend_type": "LocalStorageAdapter",
            "adapter_version": "1.0.0",
            "features_supported": [
                "save_file",
                "get_file",
                "delete_file",
                "list_files",
                "file_exists",
                "copy_file",
                "move_file",
                "metadata_support",
                "recursive_listing",
                "file_validation",
            ]
        })
        
        return stats
    
    def get_metadata(self, identifier: str) -> Dict[str, Any]:
        """
        Get metadata for file.
        
        Args:
            identifier: File identifier
            
        Returns:
            Metadata dictionary
        """
        return self._storage.get_metadata(identifier)
    
    def set_metadata(self, identifier: str, metadata: Dict[str, Any]) -> None:
        """
        Set metadata for file.
        
        Args:
            identifier: File identifier
            metadata: Metadata to save
        """
        self._storage.set_metadata(identifier, metadata)
    
    def _convert_file_info(self, file_info: FileInfo) -> StorageFileInfo:
        """
        Convert LocalFileStorage FileInfo to StorageFileInfo.
        
        Args:
            file_info: LocalFileStorage FileInfo object
            
        Returns:
            StorageFileInfo object
        """
        # Get additional metadata if available
        metadata = {}
        try:
            metadata = self._storage.get_metadata(file_info.file_path)
        except Exception:
            pass  # Metadata is optional
        
        return StorageFileInfo(
            identifier=file_info.file_path,
            filename=file_info.filename,
            size=file_info.size,
            content_type=file_info.content_type,
            created_at=file_info.created_at,
            modified_at=file_info.modified_at,
            checksum=file_info.md5_hash,
            metadata=metadata,
        )
    
    @property
    def base_path(self):
        """Get base storage path."""
        return self._storage.base_path
    
    @property
    def storage_instance(self):
        """Get underlying storage instance."""
        return self._storage


# Alias for backward compatibility
LocalFileStorage = LocalStorageAdapter