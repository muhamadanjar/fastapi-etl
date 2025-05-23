"""
Amazon S3 file storage implementation.

This module provides S3-compatible storage functionality
with proper AWS SDK integration, presigned URLs, and metadata handling.
"""

import hashlib
import logging
import mimetypes
from typing import Optional, Dict, Any, List, BinaryIO, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.config import Config

from ...core.config import get_settings
from ...core.exceptions import FileStorageError

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class S3FileInfo:
    """S3 file information metadata."""
    
    key: str
    bucket: str
    size: int
    content_type: str
    last_modified: datetime
    etag: str
    metadata: Dict[str, str]
    storage_class: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "key": self.key,
            "bucket": self.bucket,
            "size": self.size,
            "content_type": self.content_type,
            "last_modified": self.last_modified.isoformat(),
            "etag": self.etag,
            "metadata": self.metadata,
            "storage_class": self.storage_class,
        }


class S3FileStorage:
    """
    Amazon S3 file storage with advanced features.
    
    Provides S3-compatible storage with presigned URLs, metadata handling,
    lifecycle management, and multipart upload support.
    """
    
    def __init__(
        self,
        bucket_name: Optional[str] = None,
        region: Optional[str] = None,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        endpoint_url: Optional[str] = None,  # For S3-compatible services
        prefix: str = "",
        storage_class: str = "STANDARD",
        server_side_encryption: Optional[str] = None,
    ):
        """
        Initialize S3 file storage.
        
        Args:
            bucket_name: S3 bucket name
            region: AWS region
            access_key_id: AWS access key ID
            secret_access_key: AWS secret access key
            endpoint_url: Custom endpoint URL for S3-compatible services
            prefix: Key prefix for all files
            storage_class: S3 storage class
            server_side_encryption: Server-side encryption method
        """
        self.bucket_name = bucket_name or settings.storage.aws_s3_bucket
        self.region = region or settings.storage.aws_s3_region
        self.prefix = prefix
        self.storage_class = storage_class
        self.server_side_encryption = server_side_encryption
        
        if not self.bucket_name:
            raise FileStorageError("S3 bucket name is required")
        
        # Configure boto3 client
        config = Config(
            region_name=self.region,
            retries={'max_attempts': 3, 'mode': 'adaptive'},
            max_pool_connections=50,
        )
        
        session_kwargs = {}
        if access_key_id and secret_access_key:
            session_kwargs.update({
                'aws_access_key_id': access_key_id,
                'aws_secret_access_key': secret_access_key,
            })
        elif settings.storage.aws_s3_access_key_id and settings.storage.aws_s3_secret_access_key:
            session_kwargs.update({
                'aws_access_key_id': settings.storage.aws_s3_access_key_id,
                'aws_secret_access_key': settings.storage.aws_s3_secret_access_key,
            })
        
        try:
            self.s3_client = boto3.client(
                's3',
                config=config,
                endpoint_url=endpoint_url,
                **session_kwargs
            )
            
            # Test connection
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            
            logger.info(f"S3 storage initialized for bucket {self.bucket_name}")
            
        except NoCredentialsError:
            raise FileStorageError("AWS credentials not found")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise FileStorageError(f"S3 bucket not found: {self.bucket_name}")
            elif e.response['Error']['Code'] == '403':
                raise FileStorageError(f"Access denied to S3 bucket: {self.bucket_name}")
            else:
                raise FileStorageError(f"S3 connection failed: {e}")
    
    def save_file(
        self,
        file_data: Union[bytes, BinaryIO],
        filename: str,
        subfolder: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        content_type: Optional[str] = None,
        cache_control: Optional[str] = None,
        expires: Optional[datetime] = None,
    ) -> S3FileInfo:
        """
        Save file to S3.
        
        Args:
            file_data: File content as bytes or file-like object
            filename: Original filename
            subfolder: Optional subfolder within bucket
            metadata: Additional metadata to store
            content_type: Content type (auto-detected if not provided)
            cache_control: Cache control header
            expires: Expiration date
            
        Returns:
            S3FileInfo object with file details
            
        Raises:
            FileStorageError: If file saving fails
        """
        try:
            # Generate S3 key
            key = self._generate_key(filename, subfolder)
            
            # Auto-detect content type if not provided
            if not content_type:
                content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
            
            # Prepare upload arguments
            upload_args = {
                'Bucket': self.bucket_name,
                'Key': key,
                'ContentType': content_type,
                'StorageClass': self.storage_class,
            }
            
            # Add metadata
            if metadata:
                upload_args['Metadata'] = metadata
            
            # Add cache control
            if cache_control:
                upload_args['CacheControl'] = cache_control
            
            # Add expiration
            if expires:
                upload_args['Expires'] = expires
            
            # Add server-side encryption
            if self.server_side_encryption:
                upload_args['ServerSideEncryption'] = self.server_side_encryption
            
            # Get file content
            if isinstance(file_data, bytes):
                body = file_data
            else:
                body = file_data.read()
            
            upload_args['Body'] = body
            
            # Calculate content MD5 for integrity check
            content_md5 = hashlib.md5(body).digest()
            import base64
            upload_args['ContentMD5'] = base64.b64encode(content_md5).decode()
            
            # Upload file
            response = self.s3_client.put_object(**upload_args)
            
            # Get file info
            file_info = self.get_file_info(key)
            
            logger.info(f"File uploaded to S3: {key} ({len(body)} bytes)")
            return file_info
            
        except ClientError as e:
            logger.error(f"Failed to upload file to S3: {e}")
            raise FileStorageError(f"S3 upload failed: {e}")
        except Exception as e:
            logger.error(f"Failed to save file {filename}: {e}")
            raise FileStorageError(f"File save failed: {e}")
    
    def get_file(self, key: str) -> bytes:
        """
        Get file content from S3.
        
        Args:
            key: S3 object key
            
        Returns:
            File content as bytes
            
        Raises:
            FileStorageError: If file not found or download fails
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            content = response['Body'].read()
            logger.debug(f"File downloaded from S3: {key}")
            return content
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise FileStorageError(f"File not found: {key}")
            else:
                logger.error(f"Failed to download file from S3: {e}")
                raise FileStorageError(f"S3 download failed: {e}")
        except Exception as e:
            logger.error(f"Failed to get file {key}: {e}")
            raise FileStorageError(f"File retrieval failed: {e}")
    
    def get_file_info(self, key: str) -> S3FileInfo:
        """
        Get file information from S3.
        
        Args:
            key: S3 object key
            
        Returns:
            S3FileInfo object
            
        Raises:
            FileStorageError: If file not found
        """
        try:
            response = self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            return S3FileInfo(
                key=key,
                bucket=self.bucket_name,
                size=response['ContentLength'],
                content_type=response.get('ContentType', 'application/octet-stream'),
                last_modified=response['LastModified'],
                etag=response['ETag'].strip('"'),
                metadata=response.get('Metadata', {}),
                storage_class=response.get('StorageClass', 'STANDARD'),
            )
            
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise FileStorageError(f"File not found: {key}")
            else:
                logger.error(f"Failed to get file info from S3: {e}")
                raise FileStorageError(f"S3 file info retrieval failed: {e}")
        except Exception as e:
            logger.error(f"Failed to get file info for {key}: {e}")
            raise FileStorageError(f"File info retrieval failed: {e}")
    
    def delete_file(self, key: str) -> bool:
        """
        Delete file from S3.
        
        Args:
            key: S3 object key
            
        Returns:
            True if file was deleted, False if not found
            
        Raises:
            FileStorageError: If deletion fails
        """
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            logger.info(f"File deleted from S3: {key}")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.debug(f"File not found for deletion: {key}")
                return False
            else:
                logger.error(f"Failed to delete file from S3: {e}")
                raise FileStorageError(f"S3 deletion failed: {e}")
        except Exception as e:
            logger.error(f"Failed to delete file {key}: {e}")
            raise FileStorageError(f"File deletion failed: {e}")
    
    def file_exists(self, key: str) -> bool:
        """
        Check if file exists in S3.
        
        Args:
            key: S3 object key
            
        Returns:
            True if file exists
        """
        try:
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=key
            )
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(f"Error checking file existence: {e}")
                return False
        except Exception:
            return False
    
    def list_files(
        self,
        prefix: Optional[str] = None,
        max_keys: int = 1000,
        continuation_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        List files in S3 bucket.
        
        Args:
            prefix: Key prefix to filter by
            max_keys: Maximum number of keys to return
            continuation_token: Token for pagination
            
        Returns:
            Dictionary with file list and pagination info
        """
        try:
            list_args = {
                'Bucket': self.bucket_name,
                'MaxKeys': max_keys,
            }
            
            # Add prefix
            search_prefix = prefix or self.prefix
            if search_prefix:
                list_args['Prefix'] = search_prefix
            
            # Add continuation token for pagination
            if continuation_token:
                list_args['ContinuationToken'] = continuation_token
            
            response = self.s3_client.list_objects_v2(**list_args)
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append(S3FileInfo(
                        key=obj['Key'],
                        bucket=self.bucket_name,
                        size=obj['Size'],
                        content_type='',  # Not available in list response
                        last_modified=obj['LastModified'],
                        etag=obj['ETag'].strip('"'),
                        metadata={},  # Not available in list response
                        storage_class=obj.get('StorageClass', 'STANDARD'),
                    ))
            
            result = {
                'files': files,
                'is_truncated': response.get('IsTruncated', False),
                'key_count': response.get('KeyCount', 0),
            }
            
            if 'NextContinuationToken' in response:
                result['next_continuation_token'] = response['NextContinuationToken']
            
            logger.debug(f"Listed {len(files)} files from S3")
            return result
            
        except ClientError as e:
            logger.error(f"Failed to list files from S3: {e}")
            raise FileStorageError(f"S3 file listing failed: {e}")
        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            raise FileStorageError(f"File listing failed: {e}")
    
    def get_presigned_url(
        self,
        key: str,
        expires_in: int = 3600,
        http_method: str = 'GET',
    ) -> str:
        """
        Generate presigned URL for file access.
        
        Args:
            key: S3 object key
            expires_in: URL expiration time in seconds
            http_method: HTTP method (GET, PUT, DELETE)
            
        Returns:
            Presigned URL
        """
        try:
            method_map = {
                'GET': 'get_object',
                'PUT': 'put_object',
                'DELETE': 'delete_object',
            }
            
            operation = method_map.get(http_method.upper())
            if not operation:
                raise FileStorageError(f"Unsupported HTTP method: {http_method}")
            
            url = self.s3_client.generate_presigned_url(
                operation,
                Params={
                    'Bucket': self.bucket_name,
                    'Key': key,
                },
                ExpiresIn=expires_in,
            )
            
            logger.debug(f"Generated presigned URL for {key} (expires in {expires_in}s)")
            return url
            
        except ClientError as e:
            logger.error(f"Failed to generate presigned URL: {e}")
            raise FileStorageError(f"Presigned URL generation failed: {e}")
        except Exception as e:
            logger.error(f"Failed to generate presigned URL for {key}: {e}")
            raise FileStorageError(f"Presigned URL generation failed: {e}")
    
    def get_file_url(self, key: str, expires_in: Optional[int] = None) -> str:
        """
        Get URL for file access.
        
        Args:
            key: S3 object key
            expires_in: URL expiration time in seconds (None for public URL)
            
        Returns:
            File URL
        """
        if expires_in:
            return self.get_presigned_url(key, expires_in)
        else:
            # Return public URL (assumes bucket is publicly readable)
            return f"https://{self.bucket_name}.s3.{self.region}.amazonaws.com/{key}"
    
    def copy_file(self, source_key: str, dest_key: str, metadata: Optional[Dict[str, str]] = None) -> S3FileInfo:
        """
        Copy file within S3 bucket.
        
        Args:
            source_key: Source object key
            dest_key: Destination object key
            metadata: Optional metadata for copied file
            
        Returns:
            S3FileInfo for copied file
        """
        try:
            copy_source = {
                'Bucket': self.bucket_name,
                'Key': source_key,
            }
            
            copy_args = {
                'CopySource': copy_source,
                'Bucket': self.bucket_name,
                'Key': dest_key,
                'StorageClass': self.storage_class,
            }
            
            if metadata:
                copy_args['Metadata'] = metadata
                copy_args['MetadataDirective'] = 'REPLACE'
            else:
                copy_args['MetadataDirective'] = 'COPY'
            
            if self.server_side_encryption:
                copy_args['ServerSideEncryption'] = self.server_side_encryption
            
            self.s3_client.copy_object(**copy_args)
            
            logger.info(f"File copied in S3: {source_key} -> {dest_key}")
            return self.get_file_info(dest_key)
            
        except ClientError as e:
            logger.error(f"Failed to copy file in S3: {e}")
            raise FileStorageError(f"S3 file copy failed: {e}")
        except Exception as e:
            logger.error(f"Failed to copy file: {e}")
            raise FileStorageError(f"File copy failed: {e}")
    
    def move_file(self, source_key: str, dest_key: str) -> S3FileInfo:
        """
        Move file within S3 bucket.
        
        Args:
            source_key: Source object key
            dest_key: Destination object key
            
        Returns:
            S3FileInfo for moved file
        """
        # Copy file to new location
        file_info = self.copy_file(source_key, dest_key)
        
        # Delete original file
        self.delete_file(source_key)
        
        logger.info(f"File moved in S3: {source_key} -> {dest_key}")
        return file_info
    
    def _generate_key(self, filename: str, subfolder: Optional[str] = None) -> str:
        """
        Generate S3 object key.
        
        Args:
            filename: Original filename
            subfolder: Optional subfolder
            
        Returns:
            S3 object key
        """
        from pathlib import Path
        import uuid
        
        parts = []
        
        # Add base prefix
        if self.prefix:
            parts.append(self.prefix.strip('/'))
        
        # Add subfolder
        if subfolder:
            parts.append(subfolder.strip('/'))
        
        # Add timestamp and UUID for uniqueness
        timestamp = datetime.utcnow().strftime("%Y/%m/%d")
        unique_id = str(uuid.uuid4())[:8]
        
        # Sanitize filename
        safe_filename = Path(filename).name
        parts.extend([timestamp, f"{unique_id}_{safe_filename}"])
        
        return '/'.join(parts)
    
    def get_bucket_info(self) -> Dict[str, Any]:
        """
        Get S3 bucket information.
        
        Returns:
            Dictionary with bucket details
        """
        try:
            # Get bucket location
            location_response = self.s3_client.get_bucket_location(
                Bucket=self.bucket_name
            )
            region = location_response.get('LocationConstraint') or 'us-east-1'
            
            # Get bucket versioning
            try:
                versioning_response = self.s3_client.get_bucket_versioning(
                    Bucket=self.bucket_name
                )
                versioning_status = versioning_response.get('Status', 'Disabled')
            except ClientError:
                versioning_status = 'Unknown'
            
            # Get bucket encryption
            try:
                encryption_response = self.s3_client.get_bucket_encryption(
                    Bucket=self.bucket_name
                )
                encryption_enabled = True
            except ClientError:
                encryption_enabled = False
            
            return {
                'bucket_name': self.bucket_name,
                'region': region,
                'versioning_status': versioning_status,
                'encryption_enabled': encryption_enabled,
                'storage_class': self.storage_class,
                'prefix': self.prefix,
            }
            
        except ClientError as e:
            logger.error(f"Failed to get bucket info: {e}")
            raise FileStorageError(f"Bucket info retrieval failed: {e}")
        except Exception as e:
            logger.error(f"Failed to get bucket info: {e}")
            raise FileStorageError(f"Bucket info retrieval failed: {e}")


# Utility functions for S3 operations
def create_multipart_upload_manager(s3_storage: S3FileStorage, key: str, **kwargs):
    """
    Create multipart upload manager for large files.
    
    Args:
        s3_storage: S3FileStorage instance
        key: S3 object key
        **kwargs: Additional upload parameters
        
    Returns:
        Multipart upload manager
    """
    class MultipartUploadManager:
        def __init__(self, storage, object_key, **upload_kwargs):
            self.storage = storage
            self.key = object_key
            self.upload_kwargs = upload_kwargs
            self.upload_id = None
            self.parts = []
            self.part_number = 1
        
        def __enter__(self):
            """Start multipart upload."""
            try:
                response = self.storage.s3_client.create_multipart_upload(
                    Bucket=self.storage.bucket_name,
                    Key=self.key,
                    **self.upload_kwargs
                )
                self.upload_id = response['UploadId']
                logger.info(f"Started multipart upload for {self.key}")
                return self
            except Exception as e:
                logger.error(f"Failed to start multipart upload: {e}")
                raise FileStorageError(f"Multipart upload initiation failed: {e}")
        
        def upload_part(self, data: bytes) -> Dict[str, Any]:
            """Upload a part of the file."""
            try:
                response = self.storage.s3_client.upload_part(
                    Bucket=self.storage.bucket_name,
                    Key=self.key,
                    PartNumber=self.part_number,
                    UploadId=self.upload_id,
                    Body=data
                )
                
                part_info = {
                    'ETag': response['ETag'],
                    'PartNumber': self.part_number,
                }
                
                self.parts.append(part_info)
                self.part_number += 1
                
                logger.debug(f"Uploaded part {self.part_number - 1} for {self.key}")
                return part_info
                
            except Exception as e:
                logger.error(f"Failed to upload part: {e}")
                raise FileStorageError(f"Part upload failed: {e}")
        
        def complete_upload(self) -> Dict[str, Any]:
            """Complete multipart upload."""
            try:
                response = self.storage.s3_client.complete_multipart_upload(
                    Bucket=self.storage.bucket_name,
                    Key=self.key,
                    UploadId=self.upload_id,
                    MultipartUpload={'Parts': self.parts}
                )
                
                logger.info(f"Completed multipart upload for {self.key}")
                return response
                
            except Exception as e:
                logger.error(f"Failed to complete multipart upload: {e}")
                raise FileStorageError(f"Multipart upload completion failed: {e}")
        
        def abort_upload(self):
            """Abort multipart upload."""
            try:
                if self.upload_id:
                    self.storage.s3_client.abort_multipart_upload(
                        Bucket=self.storage.bucket_name,
                        Key=self.key,
                        UploadId=self.upload_id
                    )
                    logger.info(f"Aborted multipart upload for {self.key}")
            except Exception as e:
                logger.error(f"Failed to abort multipart upload: {e}")
        
        def __exit__(self, exc_type, exc_val, exc_tb):
            """Complete or abort upload based on context exit."""
            if exc_type is None:
                # No exception, complete upload
                try:
                    self.complete_upload()
                except Exception:
                    self.abort_upload()
                    raise
            else:
                # Exception occurred, abort upload
                self.abort_upload()
    
    return MultipartUploadManager(s3_storage, key, **kwargs)


def upload_large_file(
    s3_storage: S3FileStorage,
    file_data: BinaryIO,
    key: str,
    chunk_size: int = 10 * 1024 * 1024,  # 10MB chunks
    **kwargs
) -> Dict[str, Any]:
    """
    Upload large file using multipart upload.
    
    Args:
        s3_storage: S3FileStorage instance
        file_data: File-like object
        key: S3 object key
        chunk_size: Size of each chunk in bytes
        **kwargs: Additional upload parameters
        
    Returns:
        Upload response
    """
    with create_multipart_upload_manager(s3_storage, key, **kwargs) as upload:
        while True:
            chunk = file_data.read(chunk_size)
            if not chunk:
                break
            upload.upload_part(chunk)
        
        return upload.complete_upload()


class S3ConfigurationManager:
    """Manager for S3 bucket configuration."""
    
    def __init__(self, s3_storage: S3FileStorage):
        """
        Initialize configuration manager.
        
        Args:
            s3_storage: S3FileStorage instance
        """
        self.storage = s3_storage
        self.s3_client = s3_storage.s3_client
        self.bucket_name = s3_storage.bucket_name
    
    def enable_versioning(self) -> None:
        """Enable bucket versioning."""
        try:
            self.s3_client.put_bucket_versioning(
                Bucket=self.bucket_name,
                VersioningConfiguration={'Status': 'Enabled'}
            )
            logger.info(f"Enabled versioning for bucket {self.bucket_name}")
        except ClientError as e:
            logger.error(f"Failed to enable versioning: {e}")
            raise FileStorageError(f"Versioning configuration failed: {e}")
    
    def set_lifecycle_policy(self, policy: Dict[str, Any]) -> None:
        """
        Set bucket lifecycle policy.
        
        Args:
            policy: Lifecycle policy configuration
        """
        try:
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=self.bucket_name,
                LifecycleConfiguration=policy
            )
            logger.info(f"Set lifecycle policy for bucket {self.bucket_name}")
        except ClientError as e:
            logger.error(f"Failed to set lifecycle policy: {e}")
            raise FileStorageError(f"Lifecycle policy configuration failed: {e}")
    
    def set_cors_configuration(self, cors_rules: List[Dict[str, Any]]) -> None:
        """
        Set CORS configuration for bucket.
        
        Args:
            cors_rules: List of CORS rules
        """
        try:
            self.s3_client.put_bucket_cors(
                Bucket=self.bucket_name,
                CORSConfiguration={'CORSRules': cors_rules}
            )
            logger.info(f"Set CORS configuration for bucket {self.bucket_name}")
        except ClientError as e:
            logger.error(f"Failed to set CORS configuration: {e}")
            raise FileStorageError(f"CORS configuration failed: {e}")
    
    def enable_encryption(self, kms_key_id: Optional[str] = None) -> None:
        """
        Enable bucket encryption.
        
        Args:
            kms_key_id: KMS key ID (uses AES256 if not provided)
        """
        try:
            if kms_key_id:
                encryption_config = {
                    'Rules': [{
                        'ApplyServerSideEncryptionByDefault': {
                            'SSEAlgorithm': 'aws:kms',
                            'KMSMasterKeyID': kms_key_id
                        }
                    }]
                }
            else:
                encryption_config = {
                    'Rules': [{
                        'ApplyServerSideEncryptionByDefault': {
                            'SSEAlgorithm': 'AES256'
                        }
                    }]
                }
            
            self.s3_client.put_bucket_encryption(
                Bucket=self.bucket_name,
                ServerSideEncryptionConfiguration=encryption_config
            )
            logger.info(f"Enabled encryption for bucket {self.bucket_name}")
            
        except ClientError as e:
            logger.error(f"Failed to enable encryption: {e}")
            raise FileStorageError(f"Encryption configuration failed: {e}")


def create_default_lifecycle_policy(
    transition_to_ia_days: int = 30,
    transition_to_glacier_days: int = 90,
    expire_days: int = 365,
) -> Dict[str, Any]:
    """
    Create default lifecycle policy for S3 bucket.
    
    Args:
        transition_to_ia_days: Days after which to transition to IA storage
        transition_to_glacier_days: Days after which to transition to Glacier
        expire_days: Days after which to expire objects
        
    Returns:
        Lifecycle policy configuration
    """
    return {
        'Rules': [
            {
                'ID': 'DefaultLifecycleRule',
                'Status': 'Enabled',
                'Filter': {'Prefix': ''},
                'Transitions': [
                    {
                        'Days': transition_to_ia_days,
                        'StorageClass': 'STANDARD_IA'
                    },
                    {
                        'Days': transition_to_glacier_days,
                        'StorageClass': 'GLACIER'
                    }
                ],
                'Expiration': {
                    'Days': expire_days
                },
                'AbortIncompleteMultipartUpload': {
                    'DaysAfterInitiation': 7
                }
            }
        ]
    }


def create_default_cors_rules() -> List[Dict[str, Any]]:
    """
    Create default CORS rules for web applications.
    
    Returns:
        List of CORS rules
    """
    return [
        {
            'AllowedHeaders': ['*'],
            'AllowedMethods': ['GET', 'PUT', 'POST', 'DELETE'],
            'AllowedOrigins': ['*'],
            'ExposeHeaders': ['ETag'],
            'MaxAgeSeconds': 3600
        }
    ]


class S3PresignedPostManager:
    """Manager for S3 presigned POST uploads."""
    
    def __init__(self, s3_storage: S3FileStorage):
        """
        Initialize presigned POST manager.
        
        Args:
            s3_storage: S3FileStorage instance
        """
        self.storage = s3_storage
        self.s3_client = s3_storage.s3_client
        self.bucket_name = s3_storage.bucket_name
    
    def generate_presigned_post(
        self,
        key: str,
        expires_in: int = 3600,
        content_length_range: Optional[tuple] = None,
        allowed_content_types: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Generate presigned POST for direct browser uploads.
        
        Args:
            key: S3 object key
            expires_in: Expiration time in seconds
            content_length_range: Min and max content length (bytes)
            allowed_content_types: List of allowed content types
            
        Returns:
            Presigned POST data
        """
        try:
            conditions = []
            
            if content_length_range:
                conditions.append(['content-length-range', content_length_range[0], content_length_range[1]])
            
            if allowed_content_types:
                conditions.append(['starts-with', '$Content-Type', ''])
                for content_type in allowed_content_types:
                    conditions.append(['eq', '$Content-Type', content_type])
            
            response = self.s3_client.generate_presigned_post(
                Bucket=self.bucket_name,
                Key=key,
                ExpiresIn=expires_in,
                Conditions=conditions if conditions else None
            )
            
            logger.debug(f"Generated presigned POST for {key}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to generate presigned POST: {e}")
            raise FileStorageError(f"Presigned POST generation failed: {e}")
        except Exception as e:
            logger.error(f"Failed to generate presigned POST for {key}: {e}")
            raise FileStorageError(f"Presigned POST generation failed: {e}")