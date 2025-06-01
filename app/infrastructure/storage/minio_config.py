"""
MinIO configuration and examples for S3-compatible storage.

This module provides configuration helpers and examples for using MinIO
with the S3 storage adapter, since MinIO is S3-compatible.
"""

import logging
import time
from typing import Optional, Dict, Any
from urllib.parse import urlparse

from ...core.config import get_settings
from .factory import StorageFactory, StorageType, StorageConfig
from .s3_storage_adapter import S3StorageAdapter

logger = logging.getLogger(__name__)
settings = get_settings()


class MinIOConfig:
    """
    Configuration helper for MinIO storage.
    
    MinIO is S3-compatible, so we use the S3 adapter with MinIO-specific settings.
    """
    
    @staticmethod
    def create_config(
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        region: str = "us-east-1",
        secure: bool = True,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Create MinIO configuration for S3 adapter.
        
        Args:
            endpoint_url: MinIO server endpoint (e.g., "http://localhost:9000")
            access_key: MinIO access key
            secret_key: MinIO secret key
            bucket_name: MinIO bucket name
            region: Region (can be arbitrary for MinIO)
            secure: Whether to use HTTPS
            **kwargs: Additional S3 configuration
            
        Returns:
            Configuration dictionary for S3 adapter
        """
        # Ensure endpoint URL has proper scheme
        parsed_url = urlparse(endpoint_url)
        if not parsed_url.scheme:
            scheme = "https" if secure else "http"
            endpoint_url = f"{scheme}://{endpoint_url}"
        
        config = {
            "bucket_name": bucket_name,
            "region": region,
            "access_key_id": access_key,
            "secret_access_key": secret_key,
            "endpoint_url": endpoint_url,
        }
        
        # Add additional configuration
        config.update(kwargs)
        
        return config
    
    @staticmethod
    def from_env() -> Dict[str, Any]:
        """
        Create MinIO configuration from environment variables.
        
        Expected environment variables:
        - MINIO_ENDPOINT: MinIO server endpoint
        - MINIO_ACCESS_KEY: MinIO access key
        - MINIO_SECRET_KEY: MinIO secret key
        - MINIO_BUCKET: MinIO bucket name
        - MINIO_REGION: MinIO region (optional, defaults to "us-east-1")
        - MINIO_SECURE: Whether to use HTTPS (optional, defaults to "true")
        
        Returns:
            Configuration dictionary
        """
        import os
        
        endpoint_url = os.getenv("MINIO_ENDPOINT")
        access_key = os.getenv("MINIO_ACCESS_KEY")
        secret_key = os.getenv("MINIO_SECRET_KEY")
        bucket_name = os.getenv("MINIO_BUCKET")
        region = os.getenv("MINIO_REGION", "us-east-1")
        secure = os.getenv("MINIO_SECURE", "true").lower() == "true"
        
        if not all([endpoint_url, access_key, secret_key, bucket_name]):
            raise ValueError("Missing required MinIO environment variables")
        
        return MinIOConfig.create_config(
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            bucket_name=bucket_name,
            region=region,
            secure=secure,
        )


def create_minio_storage(
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    bucket_name: str,
    region: str = "us-east-1",
    secure: bool = True,
    instance_name: str = "minio",
    **kwargs
) -> S3StorageAdapter:
    """
    Create MinIO storage adapter.
    
    Args:
        endpoint_url: MinIO server endpoint
        access_key: MinIO access key
        secret_key: MinIO secret key
        bucket_name: MinIO bucket name
        region: Region (can be arbitrary for MinIO)
        secure: Whether to use HTTPS
        instance_name: Instance name for caching
        **kwargs: Additional configuration
        
    Returns:
        S3StorageAdapter configured for MinIO
    """
    config = MinIOConfig.create_config(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name,
        region=region,
        secure=secure,
        **kwargs
    )
    
    return StorageFactory.create_storage(
        StorageType.S3,
        instance_name=instance_name,
        **config
    )


def create_minio_storage_from_env(instance_name: str = "minio") -> S3StorageAdapter:
    """
    Create MinIO storage adapter from environment variables.
    
    Args:
        instance_name: Instance name for caching
        
    Returns:
        S3StorageAdapter configured for MinIO
    """
    config = MinIOConfig.from_env()
    
    return StorageFactory.create_storage(
        StorageType.S3,
        instance_name=instance_name,
        **config
    )


# Example configurations
class MinIOExamples:
    """Example MinIO configurations."""
    
    @staticmethod
    def local_development() -> Dict[str, Any]:
        """
        Configuration for local MinIO development server.
        
        Returns:
            MinIO configuration for local development
        """
        return MinIOConfig.create_config(
            endpoint_url="http://localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            bucket_name="development",
            region="us-east-1",
            secure=False,
        )
    
    @staticmethod
    def docker_compose() -> Dict[str, Any]:
        """
        Configuration for MinIO in Docker Compose.
        
        Returns:
            MinIO configuration for Docker Compose
        """
        return MinIOConfig.create_config(
            endpoint_url="http://minio:9000",
            access_key="minio_access_key",
            secret_key="minio_secret_key",
            bucket_name="app-storage",
            region="us-east-1",
            secure=False,
        )
    
    @staticmethod
    def production_cluster() -> Dict[str, Any]:
        """
        Configuration for production MinIO cluster.
        
        Returns:
            MinIO configuration for production
        """
        return MinIOConfig.create_config(
            endpoint_url="https://minio.example.com",
            access_key="production_access_key",
            secret_key="production_secret_key",
            bucket_name="production-storage",
            region="us-east-1",
            secure=True,
            # Production-specific settings
            storage_class="STANDARD",
            server_side_encryption="AES256",
        )


# MinIO-specific utilities
class MinIOBucketManager:
    """
    MinIO bucket management utilities.
    
    Provides helpers for creating and managing MinIO buckets.
    """
    
    def __init__(self, storage_adapter: S3StorageAdapter):
        """
        Initialize bucket manager.
        
        Args:
            storage_adapter: S3StorageAdapter configured for MinIO
        """
        self.storage = storage_adapter
        self.s3_client = storage_adapter.storage_instance.s3_client
    
    def create_bucket(self, bucket_name: str) -> bool:
        """
        Create MinIO bucket.
        
        Args:
            bucket_name: Name of bucket to create
            
        Returns:
            True if bucket was created, False if already exists
        """
        try:
            self.s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"Created MinIO bucket: {bucket_name}")
            return True
        except Exception as e:
            if "BucketAlreadyExists" in str(e) or "BucketAlreadyOwnedByYou" in str(e):
                logger.debug(f"MinIO bucket already exists: {bucket_name}")
                return False
            else:
                logger.error(f"Failed to create MinIO bucket {bucket_name}: {e}")
                raise
    
    def list_buckets(self) -> list:
        """
        List all MinIO buckets.
        
        Returns:
            List of bucket names
        """
        try:
            response = self.s3_client.list_buckets()
            return [bucket['Name'] for bucket in response.get('Buckets', [])]
        except Exception as e:
            logger.error(f"Failed to list MinIO buckets: {e}")
            raise
    
    def delete_bucket(self, bucket_name: str, force: bool = False) -> bool:
        """
        Delete MinIO bucket.
        
        Args:
            bucket_name: Name of bucket to delete
            force: Whether to delete bucket even if not empty
            
        Returns:
            True if bucket was deleted
        """
        try:
            if force:
                # Delete all objects first
                self._empty_bucket(bucket_name)
            
            self.s3_client.delete_bucket(Bucket=bucket_name)
            logger.info(f"Deleted MinIO bucket: {bucket_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete MinIO bucket {bucket_name}: {e}")
            raise
    
    def _empty_bucket(self, bucket_name: str):
        """Empty all objects from bucket."""
        try:
            # List all objects
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name)
            
            for page in pages:
                if 'Contents' in page:
                    # Delete objects in batches
                    objects = [{'Key': obj['Key']} for obj in page['Contents']]
                    self.s3_client.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': objects}
                    )
            
            logger.info(f"Emptied MinIO bucket: {bucket_name}")
        except Exception as e:
            logger.error(f"Failed to empty MinIO bucket {bucket_name}: {e}")
            raise


# MinIO health check
def check_minio_health(storage_adapter: S3StorageAdapter) -> Dict[str, Any]:
    """
    Check MinIO server health.
    
    Args:
        storage_adapter: S3StorageAdapter configured for MinIO
        
    Returns:
        Health check results
    """
    try:
        s3_client = storage_adapter.storage_instance.s3_client
        
        # Try to list buckets (minimal operation)
        start_time = time.time()
        response = s3_client.list_buckets()
        response_time = time.time() - start_time
        
        return {
            "status": "healthy",
            "response_time_ms": round(response_time * 1000, 2),
            "bucket_count": len(response.get('Buckets', [])),
            "endpoint": storage_adapter.storage_instance.s3_client._endpoint.host,
            "region": storage_adapter.region,
        }
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "endpoint": getattr(storage_adapter.storage_instance.s3_client, '_endpoint', {}).get('host', 'unknown'),
        }


# Integration with storage service
def create_storage_service_with_minio(
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    bucket_name: str,
    **kwargs
):
    """
    Create storage service with MinIO backend.
    
    Args:
        endpoint_url: MinIO server endpoint
        access_key: MinIO access key
        secret_key: MinIO secret key
        bucket_name: MinIO bucket name
        **kwargs: Additional configuration
        
    Returns:
        StorageService instance with MinIO backend
    """
    from .service import StorageService
    
    minio_storage = create_minio_storage(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name,
        **kwargs
    )
    
    return StorageService(storage_backend=minio_storage)


# Docker Compose configuration examples
DOCKER_COMPOSE_MINIO_SERVICE = """
version: '3.8'

services:
  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minio_access_key
      MINIO_ROOT_PASSWORD: minio_secret_key
    command: server /data --console-address ":9001"
    volumes:
      - minio_data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
      timeout: 20s
      retries: 3

  app:
    build: .
    environment:
      STORAGE_DEFAULT_BACKEND: minio
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ACCESS_KEY: minio_access_key
      MINIO_SECRET_KEY: minio_secret_key
      MINIO_BUCKET: app-storage
      MINIO_SECURE: false
    depends_on:
      - minio

volumes:
  minio_data:
"""

# Kubernetes deployment example
KUBERNETES_MINIO_DEPLOYMENT = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
spec:
  replicas: 1
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
    spec:
      containers:
      - name: minio
        image: minio/minio:latest
        ports:
        - containerPort: 9000
        - containerPort: 9001
        env:
        - name: MINIO_ROOT_USER
          value: "minio_access_key"
        - name: MINIO_ROOT_PASSWORD
          value: "minio_secret_key"
        command:
        - /bin/bash
        - -c
        args:
        - minio server /data --console-address :9001
        volumeMounts:
        - name: data
          mountPath: /data
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: minio-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: minio-service
spec:
  selector:
    app: minio
  ports:
  - name: api
    port: 9000
    targetPort: 9000
  - name: console
    port: 9001
    targetPort: 9001
"""

# Example usage and testing
if __name__ == "__main__":
    # Example: Create MinIO storage for development
    try:
        # Using local MinIO instance
        minio_storage = create_minio_storage(
            endpoint_url="http://localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            bucket_name="test-bucket",
            secure=False,
        )
        
        # Test the connection
        health = check_minio_health(minio_storage)
        print(f"MinIO Health: {health}")
        
        # Create bucket if needed
        bucket_manager = MinIOBucketManager(minio_storage)
        bucket_manager.create_bucket("test-bucket")
        
        # Upload a test file
        test_data = b"Hello, MinIO!"
        file_info = minio_storage.save_file(
            file_data=test_data,
            filename="test.txt",
            subfolder="tests",
        )
        
        print(f"Uploaded file: {file_info.to_dict()}")
        
        # Get file URL
        file_url = minio_storage.get_file_url(file_info.identifier, expires_in=3600)
        print(f"File URL: {file_url}")
        
        # Clean up
        minio_storage.delete_file(file_info.identifier)
        print("Test file deleted")
        
    except Exception as e:
        print(f"MinIO test failed: {e}")
        print("Make sure MinIO is running on localhost:9000")