"""
S3 storage adapter implementing the base storage interface.

This module adapts the existing S3FileStorage implementation
to conform to the base StorageBackend interface.
Supports both AWS S3 and MinIO (S3-compatible).
"""

import logging
from typing import Optional, Dict, Any, List, BinaryIO, Union
from datetime import datetime

from .base import StorageBackend, StorageFileInfo, FileNotFoundError
from .s3_storage import S3FileStorage, S3FileInfo

logger = logging.getLogger(__name__)


class S3StorageAdapter(StorageBackend):
    """
    Adapter for S3FileStorage to implement StorageBackend interface.
    
    This adapter wraps the existing S3FileStorage implementation
    and provides the standardized StorageBackend interface.
    Works with both AWS S3 and MinIO.
    """
    
    def __init__(
        self,
        bucket_name: Optional[str] = None,
        region: Optional[str] = None,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        prefix: str = "",
        storage_class: str = "STANDARD",
        server_side_encryption: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize S3 storage adapter.
        
        Args:
            bucket_name: S3 bucket name
            region: AWS region
            access_key_id: AWS access key ID
            secret_access_key: AWS secret access key
            endpoint_url: Custom endpoint URL for S3-compatible services (MinIO)
            prefix: Key prefix for all files
            storage_class: S3 storage class
            server_side_encryption: Server-side encryption method
            **kwargs: Additional configuration (ignored)
        """
        self._storage = S3FileStorage(
            bucket_name=bucket_name,
            region=region,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            endpoint_url=endpoint_url,
            prefix=prefix,
            storage_class=storage_class,
            server_side_encryption=server_side_encryption,
        )
        
        logger.info(f"S3 storage adapter initialized for bucket {self._storage.bucket_name}")
    
    def save_file(
        self,
        file_data: Union[bytes, BinaryIO],
        filename: str,
        subfolder: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> StorageFileInfo:
        """Save file to S3 storage."""
        try:
            # Convert metadata to string values for S3
            s3_metadata = {}
            if metadata:
                s3_metadata = {k: str(v) for k, v in metadata.items()}
            
            # Extract S3-specific parameters
            content_type = kwargs.get("content_type")
            cache_control = kwargs.get("cache_control")
            expires = kwargs.get("expires")
            
            file_info = self._storage.save_file(
                file_data=file_data,
                filename=filename,
                subfolder=subfolder,
                metadata=s3_metadata,
                content_type=content_type,
                cache_control=cache_control,
                expires=expires,
            )
            
            return self._convert_file_info(file_info)
            
        except Exception as e:
            logger.error(f"Failed to save file {filename}: {e}")
            raise
    
    def get_file(self, identifier: str) -> bytes:
        """Get file content from S3 storage."""
        try:
            return self._storage.get_file(identifier)
        except Exception as e:
            if "not found" in str(e).lower() or "NoSuchKey" in str(e):
                raise FileNotFoundError(identifier)
            raise
    
    def get_file_info(self, identifier: str) -> StorageFileInfo:
        """Get file information from S3 storage."""
        try:
            file_info = self._storage.get_file_info(identifier)
            return self._convert_file_info(file_info)
        except Exception as e:
            if "not found" in str(e).lower() or "404" in str(e):
                raise FileNotFoundError(identifier)
            raise
    
    def delete_file(self, identifier: str) -> bool:
        """Delete file from S3 storage."""
        return self._storage.delete_file(identifier)
    
    def file_exists(self, identifier: str) -> bool:
        """Check if file exists in S3 storage."""
        return self._storage.file_exists(identifier)
    
    def list_files(
        self,
        subfolder: Optional[str] = None,
        pattern: str = "*",
        limit: Optional[int] = None,
        **kwargs
    ) -> List[StorageFileInfo]:
        """List files in S3 storage."""
        # S3 doesn't support glob patterns directly, so we use prefix
        prefix = subfolder
        max_keys = limit or 1000
        continuation_token = kwargs.get("continuation_token")
        
        result = self._storage.list_files(
            prefix=prefix,
            max_keys=max_keys,
            continuation_token=continuation_token,
        )
        
        # Convert S3FileInfo to StorageFileInfo
        files = [self._convert_file_info(file_info) for file_info in result['files']]
        
        # Apply pattern filtering if not "*"
        if pattern != "*" and pattern != "**":
            import fnmatch
            files = [
                f for f in files 
                if fnmatch.fnmatch(f.filename, pattern)
            ]
        
        return files
    
    def get_file_url(
        self,
        identifier: str,
        expires_in: Optional[int] = None,
        **kwargs
    ) -> str:
        """Get URL for file access."""
        return self._storage.get_file_url(identifier, expires_in)
    
    def get_presigned_url(
        self,
        identifier: str,
        expires_in: int = 3600,
        http_method: str = 'GET',
    ) -> str:
        """
        Generate presigned URL for file access.
        
        Args:
            identifier: S3 object key
            expires_in: URL expiration time in seconds
            http_method: HTTP method (GET, PUT, DELETE)
            
        Returns:
            Presigned URL
        """
        return self._storage.get_presigned_url(
            key=identifier,
            expires_in=expires_in,
            http_method=http_method,
        )
    
    def copy_file(
        self,
        source_identifier: str,
        dest_identifier: str,
        **kwargs
    ) -> StorageFileInfo:
        """Copy file within S3 storage."""
        metadata = kwargs.get("metadata")
        if metadata:
            # Convert to string values for S3
            metadata = {k: str(v) for k, v in metadata.items()}
        
        file_info = self._storage.copy_file(
            source_key=source_identifier,
            dest_key=dest_identifier,
            metadata=metadata,
        )
        return self._convert_file_info(file_info)
    
    def move_file(
        self,
        source_identifier: str,
        dest_identifier: str,
        **kwargs
    ) -> StorageFileInfo:
        """Move file within S3 storage."""
        file_info = self._storage.move_file(
            source_key=source_identifier,
            dest_key=dest_identifier,
        )
        return self._convert_file_info(file_info)
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get S3 storage statistics."""
        try:
            bucket_info = self._storage.get_bucket_info()
            
            # Add adapter-specific information
            bucket_info.update({
                "backend_type": "S3StorageAdapter",
                "adapter_version": "1.0.0",
                "features_supported": [
                    "save_file",
                    "get_file",
                    "delete_file",
                    "list_files",
                    "file_exists",
                    "copy_file",
                    "move_file",
                    "presigned_urls",
                    "server_side_encryption",
                    "storage_classes",
                    "metadata_support",
                ]
            })
            
            return bucket_info
            
        except Exception as e:
            logger.error(f"Failed to get storage stats: {e}")
            return {
                "backend_type": "S3StorageAdapter",
                "bucket_name": self._storage.bucket_name,
                "error": str(e)
            }
    
    def create_multipart_upload(self, key: str, **kwargs):
        """
        Create multipart upload for large files.
        
        Args:
            key: S3 object key
            **kwargs: Upload parameters
            
        Returns:
            Multipart upload manager
        """
        from .s3_storage import create_multipart_upload_manager
        return create_multipart_upload_manager(self._storage, key, **kwargs)
    
    def upload_large_file(
        self,
        file_data: BinaryIO,
        key: str,
        chunk_size: int = 10 * 1024 * 1024,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Upload large file using multipart upload.
        
        Args:
            file_data: File-like object
            key: S3 object key
            chunk_size: Size of each chunk in bytes
            **kwargs: Upload parameters
            
        Returns:
            Upload response
        """
        from .s3_storage import upload_large_file
        return upload_large_file(self._storage, file_data, key, chunk_size, **kwargs)
    
    def _convert_file_info(self, file_info: S3FileInfo) -> StorageFileInfo:
        """
        Convert S3FileInfo to StorageFileInfo.
        
        Args:
            file_info: S3FileInfo object
            
        Returns:
            StorageFileInfo object
        """
        # Convert S3 metadata (string values) back to appropriate types
        metadata = {}
        for key, value in file_info.metadata.items():
            # Try to convert common types
            if value.lower() in ('true', 'false'):
                metadata[key] = value.lower() == 'true'
            elif value.isdigit():
                metadata[key] = int(value)
            else:
                try:
                    metadata[key] = float(value)
                except ValueError:
                    metadata[key] = value
        
        return StorageFileInfo(
            identifier=file_info.key,
            filename=file_info.key.split('/')[-1],  # Extract filename from key
            size=file_info.size,
            content_type=file_info.content_type,
            created_at=file_info.last_modified,  # S3 doesn't have separate created_at
            modified_at=file_info.last_modified,
            checksum=file_info.etag,
            metadata=metadata,
        )
    
    @property
    def bucket_name(self):
        """Get S3 bucket name."""
        return self._storage.bucket_name
    
    @property
    def region(self):
        """Get AWS region."""
        return self._storage.region
    
    @property
    def storage_instance(self):
        """Get underlying storage instance."""
        return self._storage


# Configuration and utility classes
class S3ConfigurationManager:
    """
    Configuration manager for S3 storage.
    
    Provides methods to configure S3 bucket settings like
    versioning, lifecycle policies, and CORS.
    """
    
    def __init__(self, s3_adapter: S3StorageAdapter):
        """
        Initialize configuration manager.
        
        Args:
            s3_adapter: S3StorageAdapter instance
        """
        self.adapter = s3_adapter
        self.storage = s3_adapter.storage_instance
    
    def enable_versioning(self) -> None:
        """Enable bucket versioning."""
        from .s3_storage import S3ConfigurationManager
        config_manager = S3ConfigurationManager(self.storage)
        config_manager.enable_versioning()
    
    def set_lifecycle_policy(self, policy: Dict[str, Any]) -> None:
        """Set bucket lifecycle policy."""
        from .s3_storage import S3ConfigurationManager
        config_manager = S3ConfigurationManager(self.storage)
        config_manager.set_lifecycle_policy(policy)
    
    def set_cors_configuration(self, cors_rules: List[Dict[str, Any]]) -> None:
        """Set CORS configuration for bucket."""
        from .s3_storage import S3ConfigurationManager
        config_manager = S3ConfigurationManager(self.storage)
        config_manager.set_cors_configuration(cors_rules)
    
    def enable_encryption(self, kms_key_id: Optional[str] = None) -> None:
        """Enable bucket encryption."""
        from .s3_storage import S3ConfigurationManager
        config_manager = S3ConfigurationManager(self.storage)
        config_manager.enable_encryption(kms_key_id)


class S3PresignedPostManager:
    """
    Manager for S3 presigned POST uploads.
    
    Provides functionality for generating presigned POST URLs
    for direct browser uploads to S3.
    """
    
    def __init__(self, s3_adapter: S3StorageAdapter):
        """
        Initialize presigned POST manager.
        
        Args:
            s3_adapter: S3StorageAdapter instance
        """
        self.adapter = s3_adapter
        self.storage = s3_adapter.storage_instance
    
    def generate_presigned_post(
        self,
        key: str,
        expires_in: int = 3600,
        content_length_range: Optional[tuple] = None,
        allowed_content_types: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Generate presigned POST for direct browser uploads."""
        from .s3_storage import S3PresignedPostManager
        post_manager = S3PresignedPostManager(self.storage)
        return post_manager.generate_presigned_post(
            key=key,
            expires_in=expires_in,
            content_length_range=content_length_range,
            allowed_content_types=allowed_content_types,
        )


# Utility functions
def create_default_lifecycle_policy(
    transition_to_ia_days: int = 30,
    transition_to_glacier_days: int = 90,
    expire_days: int = 365,
) -> Dict[str, Any]:
    """Create default lifecycle policy for S3 bucket."""
    from .s3_storage import create_default_lifecycle_policy
    return create_default_lifecycle_policy(
        transition_to_ia_days,
        transition_to_glacier_days,
        expire_days,
    )


def create_default_cors_rules() -> List[Dict[str, Any]]:
    """Create default CORS rules for web applications."""
    from .s3_storage import create_default_cors_rules
    return create_default_cors_rules()


# Alias for backward compatibility
S3FileStorage = S3StorageAdapter