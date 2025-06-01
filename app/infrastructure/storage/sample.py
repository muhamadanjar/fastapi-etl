"""
Examples of how to use the storage infrastructure.

This file shows various usage patterns for the storage system
including local storage, S3, and MinIO configurations.
"""

from pathlib import Path
from datetime import datetime
import asyncio
import concurrent.futures

# Import the storage components
from app.infrastructure.storage import (
    # Core components
    StorageService,
    StorageType,
    FileCategory,
    UploadRequest,
    
    # Factory functions
    create_storage,
    get_storage_service,
    storage_manager,
    
    # Convenience functions
    upload_file_from_path,
    upload_file_from_bytes,
    
    # MinIO specific
    create_minio_storage,
    MinIOConfig,
    create_storage_service_with_minio,
    check_minio_health,
    MinIOBucketManager,
    
    # Configuration
    StorageSettings,
    EnvironmentConfigurations,
    test_storage_configuration,
)


# Example 1: Basic file upload using default storage
def example_basic_upload():
    """Basic file upload example."""
    print("=== Basic File Upload ===")
    
    # Get the default storage service
    storage_service = get_storage_service()
    
    # Upload a file from bytes
    file_content = b"Hello, this is a test file!"
    request = UploadRequest(
        file_data=file_content,
        filename="test.txt",
        category=FileCategory.DOCUMENTS,
        user_id="user_123",
        metadata={"description": "Test file upload"},
        tags=["test", "example"]
    )
    
    try:
        result = storage_service.upload_file(request)
        print(f"File uploaded successfully:")
        print(f"  - Identifier: {result.file_info.identifier}")
        print(f"  - URL: {result.file_url}")
        print(f"  - Size: {result.file_info.size} bytes")
        print(f"  - Category: {result.category}")
        
        return result.file_info.identifier
        
    except Exception as e:
        print(f"Upload failed: {e}")
        return None


# Example 2: Upload from file path
def example_upload_from_file():
    """Upload file from filesystem path."""
    print("\n=== Upload from File Path ===")
    
    # Create a temporary file for testing
    temp_file = Path("/tmp/test_image.txt")
    temp_file.write_text("This is a test image file content")
    
    try:
        result = upload_file_from_path(
            file_path=str(temp_file),
            filename="my_image.txt",
            category=FileCategory.IMAGES,
            user_id="user_456",
            description="Uploaded from file system",
            source="filesystem"
        )
        
        print(f"File uploaded from path:")
        print(f"  - Identifier: {result.file_info.identifier}")
        print(f"  - Original filename: {result.file_info.filename}")
        print(f"  - Content type: {result.file_info.content_type}")
        
        return result.file_info.identifier
        
    except Exception as e:
        print(f"Upload from file failed: {e}")
        return None
    finally:
        # Clean up temp file
        if temp_file.exists():
            temp_file.unlink()


# Example 3: Multiple storage backends
def example_multiple_backends():
    """Example using multiple storage backends."""
    print("\n=== Multiple Storage Backends ===")
    
    # Add different storage backends
    try:
        # Add local storage for temporary files
        storage_manager.add_storage(
            name="temp",
            storage_type=StorageType.LOCAL,
            base_path="/tmp/app_temp_storage"
        )
        
        # Add S3 storage for permanent files (if configured)
        try:
            storage_manager.add_storage(
                name="permanent",
                storage_type=StorageType.S3,
                # S3 config will be read from settings
            )
            print("S3 storage configured for permanent files")
        except Exception as e:
            print(f"S3 storage not available: {e}")
        
        # List configured storages
        storages = storage_manager.list_storages()
        print(f"Configured storages: {storages}")
        
        # Use specific storage
        temp_storage = storage_manager.get_storage("temp")
        temp_service = StorageService(temp_storage)
        
        # Upload to temporary storage
        result = temp_service.upload_file(UploadRequest(
            file_data=b"Temporary file content",
            filename="temp_file.txt",
            category=FileCategory.TEMP
        ))
        
        print(f"Uploaded to temp storage: {result.file_info.identifier}")
        
        return result.file_info.identifier
        
    except Exception as e:
        print(f"Multiple backends example failed: {e}")
        return None


# Example 4: MinIO configuration
def example_minio_setup():
    """Example MinIO storage setup."""
    print("\n=== MinIO Storage Setup ===")
    
    try:
        # Method 1: Direct configuration
        minio_storage = create_minio_storage(
            endpoint_url="http://localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            bucket_name="app-storage",
            secure=False
        )
        
        print("MinIO storage created successfully")
        
        # Test MinIO health
        health = check_minio_health(minio_storage)
        print(f"MinIO health check: {health}")
        
        # Method 2: Using storage service with MinIO
        minio_service = create_storage_service_with_minio(
            endpoint_url="http://localhost:9000",
            access_key="minioadmin", 
            secret_key="minioadmin",
            bucket_name="app-storage",
            secure=False
        )
        
        # Create bucket if needed
        bucket_manager = MinIOBucketManager(minio_storage)
        bucket_created = bucket_manager.create_bucket("app-storage")
        print(f"Bucket creation result: {bucket_created}")
        
        # Upload a file to MinIO
        result = minio_service.upload_file(UploadRequest(
            file_data=b"Hello MinIO!",
            filename="minio_test.txt",
            category=FileCategory.DOCUMENTS,
            metadata={"storage": "minio", "test": True}
        ))
        
        print(f"Uploaded to MinIO:")
        print(f"  - Identifier: {result.file_info.identifier}")
        print(f"  - URL: {result.file_url}")
        
        return result.file_info.identifier
        
    except Exception as e:
        print(f"MinIO setup failed: {e}")
        print("Make sure MinIO is running on localhost:9000")
        return None


# Example 5: File operations
def example_file_operations(file_identifier: str):
    """Example file operations like copy, move, delete."""
    print("\n=== File Operations ===")
    
    if not file_identifier:
        print("No file identifier provided, skipping operations")
        return
    
    storage_service = get_storage_service()
    
    try:
        # Get file info
        file_info = storage_service.get_file_info(file_identifier)
        print(f"File info: {file_info.filename} ({file_info.size} bytes)")
        
        # Get file content
        content = storage_service.get_file(file_identifier)
        print(f"File content preview: {content[:50]}...")
        
        # Copy file
        copied_file = storage_service.copy_file(
            source_identifier=file_identifier,
            dest_filename=f"copy_{file_info.filename}",
            dest_category=FileCategory.DOCUMENTS
        )
        print(f"File copied to: {copied_file.identifier}")
        
        # Generate temporary download URL (if supported)
        try:
            download_url = storage_service.get_file_url(
                file_identifier, 
                expires_in=3600,
                download=True
            )
            print(f"Download URL: {download_url}")
        except Exception as e:
            print(f"Download URL not supported: {e}")
        
        # Delete the copied file
        deleted = storage_service.delete_file(copied_file.identifier)
        print(f"Copied file deleted: {deleted}")
        
    except Exception as e:
        print(f"File operations failed: {e}")


# Example 6: File listing and filtering
def example_file_listing():
    """Example file listing with filtering."""
    print("\n=== File Listing and Filtering ===")
    
    storage_service = get_storage_service()
    
    try:
        # List all files
        all_files = storage_service.list_files(limit=10)
        print(f"Total files (max 10): {len(all_files)}")
        
        # List files by category
        document_files = storage_service.list_files(
            category=FileCategory.DOCUMENTS,
            limit=5
        )
        print(f"Document files: {len(document_files)}")
        
        # List files by user
        user_files = storage_service.list_files(
            user_id="user_123",
            limit=5
        )
        print(f"Files for user_123: {len(user_files)}")
        
        # List files by tags
        tagged_files = storage_service.list_files(
            tags=["test"],
            limit=5
        )
        print(f"Files with 'test' tag: {len(tagged_files)}")
        
        # Display file details
        for file_info in all_files[:3]:  # Show first 3 files
            print(f"  - {file_info.filename} ({file_info.size} bytes)")
            print(f"    Category: {file_info.metadata.get('category', 'unknown')}")
            print(f"    Upload time: {file_info.metadata.get('upload_time', 'unknown')}")
        
    except Exception as e:
        print(f"File listing failed: {e}")


# Example 7: Temporary files and cleanup
def example_temporary_files():
    """Example temporary file handling."""
    print("\n=== Temporary Files ===")
    
    storage_service = get_storage_service()
    
    try:
        # Create temporary files
        temp_files = []
        
        for i in range(3):
            file_id, file_url = storage_service.create_temporary_file(
                file_data=f"Temporary file {i} content".encode(),
                filename=f"temp_{i}.txt",
                expires_in=3600  # 1 hour
            )
            temp_files.append(file_id)
            print(f"Created temp file {i}: {file_id}")
        
        print(f"Created {len(temp_files)} temporary files")
        
        # Clean up expired files
        cleaned_count = storage_service.cleanup_temporary_files()
        print(f"Cleaned up {cleaned_count} expired temporary files")
        
        # Manual cleanup of our test files
        for file_id in temp_files:
            deleted = storage_service.delete_file(file_id)
            print(f"Deleted temp file {file_id}: {deleted}")
        
    except Exception as e:
        print(f"Temporary files example failed: {e}")


# Example 8: Storage statistics
def example_storage_stats():
    """Example storage statistics."""
    print("\n=== Storage Statistics ===")
    
    storage_service = get_storage_service()
    
    try:
        stats = storage_service.get_storage_stats()
        
        print("Storage Statistics:")
        print(f"  - Backend type: {stats.get('backend_type')}")
        print(f"  - Total files: {stats.get('total_files', 'unknown')}")
        print(f"  - Total size: {stats.get('total_size_mb', 'unknown')} MB")
        
        if 'base_path' in stats:
            print(f"  - Base path: {stats['base_path']}")
        
        if 'bucket_name' in stats:
            print(f"  - Bucket name: {stats['bucket_name']}")
            print(f"  - Region: {stats.get('region')}")
        
        features = stats.get('features_supported', [])
        print(f"  - Supported features: {', '.join(features)}")
        
    except Exception as e:
        print(f"Storage stats failed: {e}")


# Example 9: Error handling
def example_error_handling():
    """Example error handling patterns."""
    print("\n=== Error Handling ===")
    
    storage_service = get_storage_service()
    
    # Test file not found
    try:
        storage_service.get_file("non_existent_file.txt")
    except Exception as e:
        print(f"Expected error for non-existent file: {type(e).__name__}: {e}")
    
    # Test invalid file upload
    try:
        invalid_request = UploadRequest(
            file_data=b"x" * (100 * 1024 * 1024),  # 100MB file
            filename="large_file.txt"
        )
        storage_service.upload_file(invalid_request)
    except Exception as e:
        print(f"Expected error for large file: {type(e).__name__}: {e}")
    
    # Test invalid filename
    try:
        invalid_request = UploadRequest(
            file_data=b"content",
            filename="../../../etc/passwd"  # Path traversal attempt
        )
        storage_service.upload_file(invalid_request)
    except Exception as e:
        print(f"Expected error for invalid filename: {type(e).__name__}: {e}")


# Example 10: Configuration examples
def example_configurations():
    """Example storage configurations."""
    print("\n=== Configuration Examples ===")
    
    # Development configuration
    dev_config = EnvironmentConfigurations.development()
    print("Development Configuration:")
    print(f"  - Backend: {dev_config.default_backend}")
    print(f"  - Path: {dev_config.local_storage_path}")
    print(f"  - Max size: {dev_config.max_file_size / (1024*1024):.1f} MB")
    
    # Test configuration
    test_result = test_storage_configuration(dev_config)
    print(f"  - Valid: {test_result['valid']}")
    print(f"  - Backend available: {test_result['backend_available']}")
    if test_result['errors']:
        print(f"  - Errors: {test_result['errors']}")
    
    # Production configuration
    prod_config = EnvironmentConfigurations.production()
    print("\nProduction Configuration:")
    print(f"  - Backend: {prod_config.default_backend}")
    print(f"  - S3 configured: {prod_config.is_s3_configured}")
    print(f"  - Encryption: {prod_config.aws_s3_encryption}")


# Example 11: Async usage pattern
async def example_async_usage():
    """Example async usage pattern."""
    print("\n=== Async Usage Pattern ===")
    
    def upload_multiple_files():
        """Upload multiple files concurrently."""
        storage_service = get_storage_service()
        results = []
        
        for i in range(3):
            request = UploadRequest(
                file_data=f"Async file {i} content".encode(),
                filename=f"async_{i}.txt",
                category=FileCategory.DOCUMENTS,
                user_id="async_user"
            )
            
            try:
                result = storage_service.upload_file(request)
                results.append(result)
                print(f"Uploaded async file {i}: {result.file_info.identifier}")
            except Exception as e:
                print(f"Failed to upload async file {i}: {e}")
        
        return results
    
    # Run in executor to avoid blocking
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(upload_multiple_files)
        results = future.result()
    
    print(f"Uploaded {len(results)} files asynchronously")
    
    # Clean up uploaded files
    storage_service = get_storage_service()
    for result in results:
        storage_service.delete_file(result.file_info.identifier)


# Example 12: Advanced MinIO operations
def example_advanced_minio():
    """Advanced MinIO operations example."""
    print("\n=== Advanced MinIO Operations ===")
    
    try:
        # Create MinIO storage
        minio_storage = create_minio_storage(
            endpoint_url="http://localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            bucket_name="advanced-test",
            secure=False
        )
        
        # Initialize bucket manager
        bucket_manager = MinIOBucketManager(minio_storage)
        
        # Create bucket
        bucket_manager.create_bucket("advanced-test")
        
        # List all buckets
        buckets = bucket_manager.list_buckets()
        print(f"Available buckets: {buckets}")
        
        # Upload file with presigned URL
        test_data = b"Advanced MinIO test content"
        file_info = minio_storage.save_file(
            file_data=test_data,
            filename="advanced_test.txt",
            metadata={"test_type": "advanced", "created_by": "example"}
        )
        
        # Generate presigned URL for download
        download_url = minio_storage.get_presigned_url(
            identifier=file_info.identifier,
            expires_in=3600,
            http_method='GET'
        )
        print(f"Presigned download URL: {download_url}")
        
        # Generate presigned URL for upload
        upload_url = minio_storage.get_presigned_url(
            identifier="uploads/new_file.txt",
            expires_in=3600,
            http_method='PUT'
        )
        print(f"Presigned upload URL: {upload_url}")
        
        # Clean up
        minio_storage.delete_file(file_info.identifier)
        print("Advanced test file deleted")
        
    except Exception as e:
        print(f"Advanced MinIO operations failed: {e}")


# Example 13: Performance testing
def example_performance_testing():
    """Performance testing example."""
    print("\n=== Performance Testing ===")
    
    import time
    
    storage_service = get_storage_service()
    
    # Test upload performance
    file_sizes = [1024, 10*1024, 100*1024]  # 1KB, 10KB, 100KB
    
    for size in file_sizes:
        test_data = b"x" * size
        
        start_time = time.time()
        
        try:
            result = storage_service.upload_file(UploadRequest(
                file_data=test_data,
                filename=f"perf_test_{size}.txt",
                category=FileCategory.TEMP
            ))
            
            upload_time = time.time() - start_time
            
            # Test download performance
            start_time = time.time()
            downloaded_data = storage_service.get_file(result.file_info.identifier)
            download_time = time.time() - start_time
            
            print(f"File size: {size} bytes")
            print(f"  Upload time: {upload_time:.3f}s ({size/upload_time/1024:.1f} KB/s)")
            print(f"  Download time: {download_time:.3f}s ({size/download_time/1024:.1f} KB/s)")
            print(f"  Data integrity: {'OK' if len(downloaded_data) == size else 'FAILED'}")
            
            # Clean up
            storage_service.delete_file(result.file_info.identifier)
            
        except Exception as e:
            print(f"Performance test failed for {size} bytes: {e}")


# Main execution
def main():
    """Run all examples."""
    print("FastAPI Clean Architecture - Storage Examples")
    print("=" * 50)
    
    # Run examples
    file_id1 = example_basic_upload()
    file_id2 = example_upload_from_file()
    file_id3 = example_multiple_backends()
    file_id4 = example_minio_setup()
    
    # Use the first successfully uploaded file for operations
    test_file_id = file_id1 or file_id2 or file_id3 or file_id4
    
    example_file_operations(test_file_id)
    example_file_listing()
    example_temporary_files()
    example_storage_stats()
    example_error_handling()
    example_configurations()
    example_advanced_minio()
    example_performance_testing()
    
    # Run async example
    try:
        asyncio.run(example_async_usage())
    except Exception as e:
        print(f"Async example failed: {e}")
    
    # Clean up test files
    if test_file_id:
        storage_service = get_storage_service()
        storage_service.delete_file(test_file_id)
        print(f"\nCleaned up test file: {test_file_id}")
    
    print("\n" + "=" * 50)
    print("Examples completed!")


# Configuration examples for different environments
class ConfigurationExamples:
    """Configuration examples for different deployment scenarios."""
    
    @staticmethod
    def development_config():
        """Development environment configuration."""
        return {
            "storage": {
                "default_backend": "local",
                "local_storage_path": "./storage/dev",
                "max_file_size": 50 * 1024 * 1024,  # 50MB
                "allowed_file_extensions": [".jpg", ".png", ".pdf", ".txt", ".docx"],
                "preserve_filename": True,
            }
        }
    
    @staticmethod
    def production_config():
        """Production environment configuration."""
        return {
            "storage": {
                "default_backend": "s3",
                "aws_s3_bucket": "production-app-storage",
                "aws_s3_region": "us-east-1",
                "aws_s3_storage_class": "STANDARD",
                "aws_s3_encryption": "AES256",
                "max_file_size": 100 * 1024 * 1024,  # 100MB
                "allowed_file_extensions": [".jpg", ".png", ".pdf", ".txt", ".docx"],
            }
        }
    
    @staticmethod
    def minio_config():
        """MinIO environment configuration."""
        return {
            "storage": {
                "default_backend": "minio",
                "max_file_size": 100 * 1024 * 1024,  # 100MB
                "allowed_file_extensions": [".jpg", ".png", ".pdf", ".txt", ".docx"],
            },
            # MinIO configuration via environment variables:
            # MINIO_ENDPOINT=http://localhost:9000
            # MINIO_ACCESS_KEY=minioadmin
            # MINIO_SECRET_KEY=minioadmin
            # MINIO_BUCKET=app-storage
            # MINIO_REGION=us-east-1
            # MINIO_SECURE=false
        }
    
    @staticmethod
    def docker_compose_config():
        """Docker Compose environment configuration."""
        return {
            "storage": {
                "default_backend": "minio",
                "max_file_size": 50 * 1024 * 1024,  # 50MB
            },
            # Docker Compose MinIO service configuration:
            # MINIO_ENDPOINT=http://minio:9000
            # MINIO_ACCESS_KEY=minio_access_key
            # MINIO_SECRET_KEY=minio_secret_key
            # MINIO_BUCKET=app-storage
            # MINIO_SECURE=false
        }


# Testing utilities
class StorageTestUtils:
    """Utilities for testing storage functionality."""
    
    @staticmethod
    def create_test_file(size_bytes: int = 1024) -> bytes:
        """Create test file content of specified size."""
        content = f"Test file content - {datetime.now()}\n"
        content += "x" * (size_bytes - len(content.encode()))
        return content.encode()[:size_bytes]
    
    @staticmethod
    def test_storage_backend(storage_backend):
        """Test basic storage backend functionality."""
        test_results = {
            "save_file": False,
            "get_file": False,
            "file_exists": False,
            "get_file_info": False,
            "delete_file": False,
        }
        
        test_data = StorageTestUtils.create_test_file(100)
        test_filename = f"test_{datetime.now().timestamp()}.txt"
        file_identifier = None
        
        try:
            # Test save
            from app.infrastructure.storage.base import StorageFileInfo
            file_info = storage_backend.save_file(test_data, test_filename)
            if isinstance(file_info, StorageFileInfo):
                test_results["save_file"] = True
                file_identifier = file_info.identifier
            
            if file_identifier:
                # Test exists
                if storage_backend.file_exists(file_identifier):
                    test_results["file_exists"] = True
                
                # Test get file info
                file_info = storage_backend.get_file_info(file_identifier)
                if file_info.size == len(test_data):
                    test_results["get_file_info"] = True
                
                # Test get file
                retrieved_data = storage_backend.get_file(file_identifier)
                if retrieved_data == test_data:
                    test_results["get_file"] = True
                
                # Test delete
                if storage_backend.delete_file(file_identifier):
                    test_results["delete_file"] = True
        
        except Exception as e:
            print(f"Storage test error: {e}")
        
        return test_results


# Integration examples
class IntegrationExamples:
    """Examples of integrating storage with FastAPI endpoints."""
    
    @staticmethod
    def fastapi_upload_endpoint():
        """Example FastAPI endpoint for file uploads."""
        return '''
from fastapi import FastAPI, UploadFile, File, HTTPException
from app.infrastructure.storage import get_storage_service, UploadRequest, FileCategory

app = FastAPI()

@app.post("/upload/")
async def upload_file(
    file: UploadFile = File(...),
    user_id: str = None,
    category: str = "user_uploads"
):
    """Upload file endpoint."""
    try:
        # Read file content
        content = await file.read()
        
        # Create upload request
        request = UploadRequest(
            file_data=content,
            filename=file.filename,
            content_type=file.content_type,
            category=FileCategory(category),
            user_id=user_id,
            metadata={
                "original_size": len(content),
                "client_filename": file.filename,
            }
        )
        
        # Upload using storage service
        storage_service = get_storage_service()
        result = storage_service.upload_file(request)
        
        return {
            "success": True,
            "file_id": result.file_info.identifier,
            "file_url": result.file_url,
            "size": result.file_info.size,
            "category": result.category
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/files/{file_id}")
async def get_file(file_id: str):
    """Get file download URL."""
    try:
        storage_service = get_storage_service()
        file_info = storage_service.get_file_info(file_id)
        download_url = storage_service.get_file_url(file_id, expires_in=3600)
        
        return {
            "file_info": file_info.to_dict(),
            "download_url": download_url
        }
        
    except Exception as e:
        raise HTTPException(status_code=404, detail="File not found")
'''


if __name__ == "__main__":
    main()