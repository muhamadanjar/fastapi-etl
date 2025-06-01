"""
Storage infrastructure module.

This module provides a complete file storage abstraction layer
with support for multiple backends including local filesystem,
Amazon S3, and MinIO.
"""

from .base import (
    StorageBackend,
    StorageFileInfo,
    StorageException,
    FileNotFoundError,
    FileExistsError,
    StorageQuotaExceededError,
    InvalidFileError,
    StorageConfigurationError,
    StoragePermissionError,
)

from .factory import (
    StorageFactory,
    StorageType,
    StorageConfig,
    MultiStorageManager,
    get_default_storage,
    get_storage,
    create_storage,
    storage_manager,
)

from .local_storage_adapter import LocalStorageAdapter
from .s3_storage_adapter import S3StorageAdapter

from .service import (
    StorageService,
    UploadRequest,
    UploadResult,
    FileCategory,
    FileValidator,
    FileValidationError,
    create_storage_service,
    get_storage_service,
    upload_file_from_path,
    upload_file_from_bytes,
)

from .minio_config import (
    MinIOConfig,
    MinIOBucketManager,
    MinIOExamples,
    create_minio_storage,
    create_minio_storage_from_env,
    create_storage_service_with_minio,
    check_minio_health,
)

from .config_settings import (
    StorageSettings,
    EnvironmentConfigurations,
    SecuritySettings,
    PerformanceSettings,
    MonitoringSettings,
    UseCaseConfigurations,
    validate_storage_configuration,
    test_storage_configuration,
)

# Convenience aliases
LocalFileStorage = LocalStorageAdapter
S3FileStorage = S3StorageAdapter

__all__ = [
    # Base classes and exceptions
    "StorageBackend",
    "StorageFileInfo", 
    "StorageException",
    "FileNotFoundError",
    "FileExistsError",
    "StorageQuotaExceededError",
    "InvalidFileError",
    "StorageConfigurationError",
    "StoragePermissionError",
    
    # Factory and configuration
    "StorageFactory",
    "StorageType",
    "StorageConfig",
    "MultiStorageManager",
    "get_default_storage",
    "get_storage",
    "create_storage",
    "storage_manager",
    
    # Storage adapters
    "LocalStorageAdapter",
    "LocalFileStorage",  # Alias
    "S3StorageAdapter", 
    "S3FileStorage",  # Alias
    
    # High-level service
    "StorageService",
    "UploadRequest",
    "UploadResult",
    "FileCategory",
    "FileValidator",
    "FileValidationError",
    "create_storage_service",
    "get_storage_service",
    "upload_file_from_path",
    "upload_file_from_bytes",
    
    # MinIO support
    "MinIOConfig",
    "MinIOBucketManager",
    "MinIOExamples",
    "create_minio_storage",
    "create_minio_storage_from_env",
    "create_storage_service_with_minio",
    "check_minio_health",
    
    # Configuration
    "StorageSettings",
    "EnvironmentConfigurations",
    "SecuritySettings",
    "PerformanceSettings",
    "MonitoringSettings",
    "UseCaseConfigurations",
    "validate_storage_configuration",
    "test_storage_configuration",
]

# Version info
__version__ = "1.0.0"
__author__ = "FastAPI Clean Architecture"
__description__ = "Multi-backend file storage infrastructure"

# Default configuration
def configure_default_storage():
    """Configure default storage based on settings."""
    try:
        from ...core.config import get_settings
        settings = get_settings()
        
        # Auto-configure based on available settings
        default_backend = getattr(settings.storage, 'default_backend', 'local')
        
        if default_backend == 'local':
            # Ensure local storage is configured
            storage_manager.add_storage(
                name="default",
                storage_type=StorageType.LOCAL,
                set_as_default=True,
            )
        elif default_backend == 's3':
            # Configure S3 storage
            storage_manager.add_storage(
                name="default", 
                storage_type=StorageType.S3,
                set_as_default=True,
            )
        elif default_backend == 'minio':
            # Configure MinIO storage using S3 adapter
            try:
                minio_storage = create_minio_storage_from_env("default")
                storage_manager.storages["default"] = minio_storage
                storage_manager.default_storage_name = "default"
            except ValueError:
                # Fall back to local storage if MinIO env vars not set
                storage_manager.add_storage(
                    name="default",
                    storage_type=StorageType.LOCAL,
                    set_as_default=True,
                )
        
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Failed to configure default storage: {e}")
        
        # Fall back to local storage
        try:
            storage_manager.add_storage(
                name="default",
                storage_type=StorageType.LOCAL,
                set_as_default=True,
            )
        except Exception:
            pass  # Will be configured on first use


# Auto-configure on import
configure_default_storage()