"""
Storage factory for creating storage backend instances.

This module provides a factory pattern for creating different
storage backends based on configuration.
"""

import logging
from typing import Optional, Dict, Any, Type
from enum import Enum

from ...core.config import get_settings
from .base import StorageBackend, StorageConfigurationError

logger = logging.getLogger(__name__)
settings = get_settings()


class StorageType(str, Enum):
    """Supported storage types."""
    LOCAL = "local"
    S3 = "s3"
    GCS = "gcs"  # For future implementation
    AZURE = "azure"  # For future implementation


class StorageFactory:
    """
    Factory for creating storage backend instances.
    
    Supports multiple storage backends and provides a unified
    interface for creating and configuring storage instances.
    """
    
    _backends: Dict[StorageType, Type[StorageBackend]] = {}
    _instances: Dict[str, StorageBackend] = {}
    
    @classmethod
    def register_backend(cls, storage_type: StorageType, backend_class: Type[StorageBackend]):
        """
        Register a storage backend class.
        
        Args:
            storage_type: Storage type identifier
            backend_class: Backend class to register
        """
        cls._backends[storage_type] = backend_class
        logger.info(f"Registered storage backend: {storage_type} -> {backend_class.__name__}")
    
    @classmethod
    def create_storage(
        cls,
        storage_type: Optional[StorageType] = None,
        instance_name: str = "default",
        **kwargs
    ) -> StorageBackend:
        """
        Create storage backend instance.
        
        Args:
            storage_type: Type of storage backend to create
            instance_name: Name for this storage instance
            **kwargs: Backend-specific configuration
            
        Returns:
            Storage backend instance
            
        Raises:
            StorageConfigurationError: If backend is not supported or configuration is invalid
        """
        # Use default storage type from settings if not provided
        if storage_type is None:
            storage_type = StorageType(settings.storage.default_backend)
        
        # Check if instance already exists
        cache_key = f"{storage_type}:{instance_name}"
        if cache_key in cls._instances:
            return cls._instances[cache_key]
        
        # Check if backend is registered
        if storage_type not in cls._backends:
            raise StorageConfigurationError(f"Storage backend not supported: {storage_type}")
        
        backend_class = cls._backends[storage_type]
        
        try:
            # Create instance with configuration
            config = cls._get_backend_config(storage_type, **kwargs)
            instance = backend_class(**config)
            
            # Cache instance
            cls._instances[cache_key] = instance
            
            logger.info(f"Created storage instance: {storage_type}:{instance_name}")
            return instance
            
        except Exception as e:
            logger.error(f"Failed to create storage backend {storage_type}: {e}")
            raise StorageConfigurationError(f"Failed to create {storage_type} backend: {e}")
    
    @classmethod
    def get_storage(cls, instance_name: str = "default") -> StorageBackend:
        """
        Get existing storage instance.
        
        Args:
            instance_name: Name of storage instance
            
        Returns:
            Storage backend instance
            
        Raises:
            StorageConfigurationError: If instance not found
        """
        # Try to find instance with default backend type
        default_type = StorageType(settings.storage.default_backend)
        cache_key = f"{default_type}:{instance_name}"
        
        if cache_key in cls._instances:
            return cls._instances[cache_key]
        
        # Try to find any instance with this name
        for key, instance in cls._instances.items():
            if key.endswith(f":{instance_name}"):
                return instance
        
        # Create new instance if not found
        return cls.create_storage(instance_name=instance_name)
    
    @classmethod
    def _get_backend_config(cls, storage_type: StorageType, **kwargs) -> Dict[str, Any]:
        """
        Get configuration for specific backend type.
        
        Args:
            storage_type: Storage backend type
            **kwargs: Override configuration
            
        Returns:
            Configuration dictionary
        """
        config = {}
        
        if storage_type == StorageType.LOCAL:
            config.update({
                "base_path": settings.storage.local_storage_path,
                "create_dirs": True,
                "allowed_extensions": getattr(settings.storage, "allowed_file_extensions", None),
                "max_file_size": getattr(settings.storage, "max_file_size", None),
                "preserve_filename": getattr(settings.storage, "preserve_filename", True),
            })
        
        elif storage_type == StorageType.S3:
            config.update({
                "bucket_name": settings.storage.aws_s3_bucket,
                "region": settings.storage.aws_s3_region,
                "access_key_id": getattr(settings.storage, "aws_s3_access_key_id", None),
                "secret_access_key": getattr(settings.storage, "aws_s3_secret_access_key", None),
                "endpoint_url": getattr(settings.storage, "aws_s3_endpoint_url", None),
                "prefix": getattr(settings.storage, "aws_s3_prefix", ""),
                "storage_class": getattr(settings.storage, "aws_s3_storage_class", "STANDARD"),
                "server_side_encryption": getattr(settings.storage, "aws_s3_encryption", None),
            })
        
        # Override with provided kwargs
        config.update(kwargs)
        
        return config
    
    @classmethod
    def list_registered_backends(cls) -> Dict[str, str]:
        """
        List all registered storage backends.
        
        Returns:
            Dictionary mapping storage types to backend class names
        """
        return {
            storage_type.value: backend_class.__name__
            for storage_type, backend_class in cls._backends.items()
        }
    
    @classmethod
    def clear_instances(cls):
        """Clear all cached storage instances."""
        cls._instances.clear()
        logger.info("Cleared all storage instances")


# Auto-register available backends
def _register_available_backends():
    """Register all available storage backends."""
    try:
        from .local_storage_adapter import LocalStorageAdapter
        StorageFactory.register_backend(StorageType.LOCAL, LocalStorageAdapter)
    except ImportError as e:
        logger.warning(f"Local storage backend not available: {e}")
    
    try:
        from .s3_storage_adapter import S3StorageAdapter
        StorageFactory.register_backend(StorageType.S3, S3StorageAdapter)
    except ImportError as e:
        logger.warning(f"S3 storage backend not available: {e}")
    
    # Register other backends when available
    # try:
    #     from .gcs_storage import GCSFileStorage
    #     StorageFactory.register_backend(StorageType.GCS, GCSFileStorage)
    # except ImportError:
    #     pass
    
    # try:
    #     from .azure_storage import AzureFileStorage
    #     StorageFactory.register_backend(StorageType.AZURE, AzureFileStorage)
    # except ImportError:
    #     pass


# Initialize backends on import
_register_available_backends()


# Convenience functions
def get_default_storage() -> StorageBackend:
    """
    Get default storage backend instance.
    
    Returns:
        Default storage backend
    """
    return StorageFactory.get_storage("default")


def get_storage(instance_name: str = "default") -> StorageBackend:
    """
    Get storage backend instance by name.
    
    Args:
        instance_name: Name of storage instance
        
    Returns:
        Storage backend instance
    """
    return StorageFactory.get_storage(instance_name)


def create_storage(
    storage_type: StorageType,
    instance_name: str = "default",
    **kwargs
) -> StorageBackend:
    """
    Create new storage backend instance.
    
    Args:
        storage_type: Type of storage backend
        instance_name: Name for this instance
        **kwargs: Backend-specific configuration
        
    Returns:
        Storage backend instance
    """
    return StorageFactory.create_storage(storage_type, instance_name, **kwargs)


# Storage configuration helper
class StorageConfig:
    """Helper class for storage configuration."""
    
    @staticmethod
    def for_local_storage(
        base_path: str,
        allowed_extensions: Optional[list] = None,
        max_file_size: Optional[int] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Create configuration for local storage.
        
        Args:
            base_path: Base directory path
            allowed_extensions: List of allowed file extensions
            max_file_size: Maximum file size in bytes
            **kwargs: Additional configuration
            
        Returns:
            Configuration dictionary
        """
        config = {
            "base_path": base_path,
            "allowed_extensions": allowed_extensions,
            "max_file_size": max_file_size,
        }
        config.update(kwargs)
        return {k: v for k, v in config.items() if v is not None}
    
    @staticmethod
    def for_s3_storage(
        bucket_name: str,
        region: str = "us-east-1",
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Create configuration for S3 storage.
        
        Args:
            bucket_name: S3 bucket name
            region: AWS region
            access_key_id: AWS access key ID
            secret_access_key: AWS secret access key
            **kwargs: Additional configuration
            
        Returns:
            Configuration dictionary
        """
        config = {
            "bucket_name": bucket_name,
            "region": region,
            "access_key_id": access_key_id,
            "secret_access_key": secret_access_key,
        }
        config.update(kwargs)
        return {k: v for k, v in config.items() if v is not None}


# Multi-backend storage manager
class MultiStorageManager:
    """
    Manager for multiple storage backends.
    
    Allows using different storage backends for different purposes
    (e.g., local for temp files, S3 for permanent storage).
    """
    
    def __init__(self):
        self.storages: Dict[str, StorageBackend] = {}
        self.default_storage_name: Optional[str] = None
    
    def add_storage(
        self,
        name: str,
        storage_type: StorageType,
        set_as_default: bool = False,
        **config
    ) -> StorageBackend:
        """
        Add storage backend.
        
        Args:
            name: Storage name/identifier
            storage_type: Type of storage backend
            set_as_default: Whether to set as default storage
            **config: Storage configuration
            
        Returns:
            Storage backend instance
        """
        storage = StorageFactory.create_storage(
            storage_type,
            instance_name=name,
            **config
        )
        
        self.storages[name] = storage
        
        if set_as_default or self.default_storage_name is None:
            self.default_storage_name = name
        
        logger.info(f"Added storage backend: {name} ({storage_type})")
        return storage
    
    def get_storage(self, name: Optional[str] = None) -> StorageBackend:
        """
        Get storage backend by name.
        
        Args:
            name: Storage name (uses default if None)
            
        Returns:
            Storage backend instance
        """
        storage_name = name or self.default_storage_name
        
        if storage_name is None:
            raise StorageConfigurationError("No default storage configured")
        
        if storage_name not in self.storages:
            raise StorageConfigurationError(f"Storage not found: {storage_name}")
        
        return self.storages[storage_name]
    
    def list_storages(self) -> Dict[str, str]:
        """
        List all configured storages.
        
        Returns:
            Dictionary mapping storage names to backend types
        """
        return {
            name: storage.__class__.__name__
            for name, storage in self.storages.items()
        }
    
    def remove_storage(self, name: str):
        """
        Remove storage backend.
        
        Args:
            name: Storage name to remove
        """
        if name in self.storages:
            del self.storages[name]
            
            if self.default_storage_name == name:
                # Set new default if available
                if self.storages:
                    self.default_storage_name = next(iter(self.storages))
                else:
                    self.default_storage_name = None
            
            logger.info(f"Removed storage backend: {name}")


# Global multi-storage manager instance
storage_manager = MultiStorageManager()