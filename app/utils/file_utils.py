"""
File utilities for the ETL system.
Provides functions for file operations, validation, and management.
"""

import os
import shutil
import hashlib
import mimetypes
import tempfile
from pathlib import Path
from typing import Optional, List, Dict, Any, Union, Tuple
import magic
import zipfile
import tarfile
from datetime import datetime
import re

from app.utils.logger import get_logger
from app.core.exceptions import FileError

logger = get_logger(__name__)

# Supported file types and their extensions
SUPPORTED_FILE_TYPES = {
    'CSV': ['.csv', '.tsv'],
    'EXCEL': ['.xlsx', '.xls', '.xlsm'],
    'JSON': ['.json', '.jsonl'],
    'XML': ['.xml'],
    'TEXT': ['.txt', '.log'],
    'PARQUET': ['.parquet'],
    'AVRO': ['.avro'],
    'ARCHIVE': ['.zip', '.tar', '.tar.gz', '.tar.bz2', '.rar']
}

SUPPORTED_MIME_TYPES = {
    "image": [
        "image/jpeg", "image/png", "image/gif", "image/webp", "image/bmp"
    ],
    "video": [
        "video/mp4", "video/mpeg", "video/quicktime", "video/x-msvideo"
    ],
    "audio": [
        "audio/mpeg", "audio/wav", "audio/ogg"
    ],
    "document": [
        "application/pdf", "application/msword",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    ],
    "spreadsheet": [
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "text/csv"
    ],
    "archive": [
        "application/zip", "application/x-tar",
        "application/x-rar-compressed", "application/gzip"
    ],
    "text": [
        "text/plain", "text/html", "text/css", "text/markdown"
    ],
}

# Maximum file sizes (in bytes)
MAX_FILE_SIZES = {
    'CSV': 500 * 1024 * 1024,      # 500MB
    'EXCEL': 100 * 1024 * 1024,    # 100MB
    'JSON': 200 * 1024 * 1024,     # 200MB
    'XML': 200 * 1024 * 1024,      # 200MB
    'TEXT': 50 * 1024 * 1024,      # 50MB
    'PARQUET': 1024 * 1024 * 1024, # 1GB
    'AVRO': 1024 * 1024 * 1024,    # 1GB
    'ARCHIVE': 2 * 1024 * 1024 * 1024  # 2GB
}


def get_file_extension(filename: str) -> str:
    """
    Get file extension from filename.
    
    Args:
        filename: Name of the file
        
    Returns:
        File extension (including the dot)
    """
    try:
        return Path(filename).suffix.lower()
    except Exception as e:
        logger.log_error("get_file_extension", e, {"filename": filename})
        return ""


def get_file_stem(filename: str) -> str:
    """
    Get file stem (filename without extension).
    
    Args:
        filename: Name of the file
        
    Returns:
        File stem
    """
    try:
        return Path(filename).stem
    except Exception as e:
        logger.log_error("get_file_stem", e, {"filename": filename})
        return filename


def sanitize_filename(filename: str, max_length: int = 255) -> str:
    """
    Sanitize filename to make it safe for filesystem.
    
    Args:
        filename: Original filename
        max_length: Maximum allowed length
        
    Returns:
        Sanitized filename
    """
    try:
        # Get the original extension
        original_ext = get_file_extension(filename)
        name_part = get_file_stem(filename)
        
        # Remove or replace unsafe characters
        unsafe_chars = r'[<>:"/\\|?*\x00-\x1f]'
        name_part = re.sub(unsafe_chars, '_', name_part)
        
        # Remove leading/trailing dots and spaces
        name_part = name_part.strip('. ')
        
        # Replace multiple underscores with single
        name_part = re.sub(r'_+', '_', name_part)
        
        # Ensure name is not empty
        if not name_part:
            name_part = "file"
        
        # Reconstruct filename
        sanitized = name_part + original_ext
        
        # Truncate if too long
        if len(sanitized) > max_length:
            # Calculate how much space we have for the name part
            name_max_length = max_length - len(original_ext)
            if name_max_length > 0:
                name_part = name_part[:name_max_length]
                sanitized = name_part + original_ext
            else:
                # Extension is too long, truncate everything
                sanitized = sanitized[:max_length]
        
        logger.log_operation("sanitize_filename", {
            "original": filename,
            "sanitized": sanitized
        })
        
        return sanitized
        
    except Exception as e:
        logger.log_error("sanitize_filename", e, {"filename": filename})
        return "sanitized_file.txt"


def get_file_size(file_path: Union[str, Path]) -> int:
    """
    Get file size in bytes.
    
    Args:
        file_path: Path to the file
        
    Returns:
        File size in bytes
    """
    try:
        return Path(file_path).stat().st_size
    except Exception as e:
        logger.log_error("get_file_size", e, {"file_path": str(file_path)})
        return 0


def get_file_info(file_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Get comprehensive file information.
    
    Args:
        file_path: Path to the file
        
    Returns:
        Dictionary with file information
    """
    try:
        path = Path(file_path)
        stat = path.stat()
        
        info = {
            "name": path.name,
            "stem": path.stem,
            "suffix": path.suffix,
            "size_bytes": stat.st_size,
            "size_human": format_file_size(stat.st_size),
            "created": datetime.fromtimestamp(stat.st_ctime),
            "modified": datetime.fromtimestamp(stat.st_mtime),
            "accessed": datetime.fromtimestamp(stat.st_atime),
            "is_file": path.is_file(),
            "is_directory": path.is_dir(),
            "exists": path.exists(),
            "absolute_path": str(path.absolute()),
            "mime_type": get_mime_type(file_path)
        }
        
        # Add file type based on extension
        info["file_type"] = detect_file_type(path.name)
        
        # Add hash if it's a file
        if path.is_file():
            info["md5_hash"] = calculate_file_hash(file_path, "md5")
        
        return info
        
    except Exception as e:
        logger.log_error("get_file_info", e, {"file_path": str(file_path)})
        return {}


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Formatted size string
    """
    try:
        if size_bytes == 0:
            return "0 B"
        
        size_names = ["B", "KB", "MB", "GB", "TB", "PB"]
        import math
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        
        return f"{s} {size_names[i]}"
        
    except Exception as e:
        logger.log_error("format_file_size", e, {"size_bytes": size_bytes})
        return f"{size_bytes} B"


def calculate_file_hash(file_path: Union[str, Path], algorithm: str = "sha256") -> str:
    """
    Calculate hash of a file.
    
    Args:
        file_path: Path to the file
        algorithm: Hash algorithm (md5, sha1, sha256, sha512)
        
    Returns:
        Hex string of the hash
    """
    try:
        hash_obj = hashlib.new(algorithm)
        
        with open(file_path, 'rb') as f:
            # Read file in chunks to handle large files
            for chunk in iter(lambda: f.read(4096), b""):
                hash_obj.update(chunk)
        
        file_hash = hash_obj.hexdigest()
        
        logger.log_operation("calculate_file_hash", {
            "file_path": str(file_path),
            "algorithm": algorithm,
            "hash": file_hash[:16] + "..."  # Log only first 16 chars for security
        })
        
        return file_hash
        
    except Exception as e:
        logger.log_error("calculate_file_hash", e, {
            "file_path": str(file_path),
            "algorithm": algorithm
        })
        raise


def get_mime_type(file_path: Union[str, Path]) -> str:
    """
    Get MIME type of a file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        MIME type string
    """
    try:
        # Try using python-magic first (more accurate)
        try:
            mime_type = magic.from_file(str(file_path), mime=True)
            if mime_type:
                return mime_type
        except:
            pass
        
        # Fallback to mimetypes module
        mime_type, _ = mimetypes.guess_type(str(file_path))
        return mime_type or "application/octet-stream"
        
    except Exception as e:
        logger.log_error("get_mime_type", e, {"file_path": str(file_path)})
        return "application/octet-stream"


def get_file_type(content_type: str) -> str:
    """
    Determine the file type based on MIME content type.

    Args:
        content_type (str): MIME type (e.g. 'image/jpeg', 'application/pdf')

    Returns:
        str: File type (e.g. 'image', 'video', 'document'), or 'UNKNOWN' if not recognized
    """
    try:
        if not content_type:
            return "UNKNOWN"

        content_type = content_type.lower()

        for file_type, mime_types in SUPPORTED_MIME_TYPES.items():
            if content_type in mime_types or content_type.startswith(file_type + "/"):
                return file_type

        return "UNKNOWN"

    except Exception as e:
        logger.log_error("get_file_type", e, {"content_type": content_type})
        return "UNKNOWN"

def detect_file_type(filename: str) -> str:
    """
    Detect file type based on extension.
    
    Args:
        filename: Name of the file
        
    Returns:
        File type string
    """
    try:
        extension = get_file_extension(filename)
        
        for file_type, extensions in SUPPORTED_FILE_TYPES.items():
            if extension in extensions:
                return file_type
        
        return "UNKNOWN"
        
    except Exception as e:
        logger.log_error("detect_file_type", e, {"filename": filename})
        return "UNKNOWN"


def validate_file_type(filename: str, allowed_types: Optional[List[str]] = None) -> bool:
    """
    Validate if file type is allowed.
    
    Args:
        filename: Name of the file
        allowed_types: List of allowed file types (if None, use all supported types)
        
    Returns:
        True if file type is allowed, False otherwise
    """
    try:
        file_type = detect_file_type(filename)
        
        if allowed_types is None:
            allowed_types = list(SUPPORTED_FILE_TYPES.keys())
        
        return file_type in allowed_types
        
    except Exception as e:
        logger.log_error("validate_file_type", e, {
            "filename": filename,
            "allowed_types": allowed_types
        })
        return False


def validate_file_size(file_path: Union[str, Path], max_size: Optional[int] = None) -> bool:
    """
    Validate if file size is within limits.
    
    Args:
        file_path: Path to the file
        max_size: Maximum allowed size in bytes (if None, use type-based limit)
        
    Returns:
        True if file size is valid, False otherwise
    """
    try:
        file_size = get_file_size(file_path)
        filename = Path(file_path).name
        
        if max_size is None:
            file_type = detect_file_type(filename)
            max_size = MAX_FILE_SIZES.get(file_type, 100 * 1024 * 1024)  # 100MB default
        
        is_valid = file_size <= max_size
        
        logger.log_operation("validate_file_size", {
            "file_path": str(file_path),
            "file_size": file_size,
            "max_size": max_size,
            "is_valid": is_valid
        })
        
        return is_valid
        
    except Exception as e:
        logger.log_error("validate_file_size", e, {
            "file_path": str(file_path),
            "max_size": max_size
        })
        return False


def create_directory(directory_path: Union[str, Path], exist_ok: bool = True) -> bool:
    """
    Create directory and all parent directories.
    
    Args:
        directory_path: Path to the directory
        exist_ok: If True, don't raise error if directory exists
        
    Returns:
        True if successful, False otherwise
    """
    try:
        Path(directory_path).mkdir(parents=True, exist_ok=exist_ok)
        
        logger.log_operation("create_directory", {
            "directory_path": str(directory_path),
            "exist_ok": exist_ok
        })
        
        return True
        
    except Exception as e:
        logger.log_error("create_directory", e, {"directory_path": str(directory_path)})
        return False


def delete_file_safely(file_path: Union[str, Path], backup: bool = False) -> bool:
    """
    Safely delete a file with optional backup.
    
    Args:
        file_path: Path to the file
        backup: If True, create backup before deletion
        
    Returns:
        True if successful, False otherwise
    """
    try:
        path = Path(file_path)
        
        if not path.exists():
            logger.log_operation("delete_file_safely", {
                "file_path": str(file_path),
                "status": "file_not_exists"
            })
            return True
        
        # Create backup if requested
        if backup:
            backup_path = path.with_suffix(path.suffix + ".backup")
            shutil.copy2(path, backup_path)
            logger.log_operation("file_backup_created", {
                "original": str(path),
                "backup": str(backup_path)
            })
        
        # Delete the file
        path.unlink()
        
        logger.log_operation("delete_file_safely", {
            "file_path": str(file_path),
            "backup_created": backup,
            "status": "deleted"
        })
        
        return True
        
    except Exception as e:
        logger.log_error("delete_file_safely", e, {"file_path": str(file_path)})
        return False


def move_file_safely(source: Union[str, Path], destination: Union[str, Path], 
                    overwrite: bool = False) -> bool:
    """
    Safely move a file to another location.
    
    Args:
        source: Source file path
        destination: Destination file path
        overwrite: If True, overwrite existing file
        
    Returns:
        True if successful, False otherwise
    """
    try:
        src_path = Path(source)
        dst_path = Path(destination)
        
        if not src_path.exists():
            raise FileError(f"Source file does not exist: {source}")
        
        if dst_path.exists() and not overwrite:
            raise FileError(f"Destination file exists: {destination}")
        
        # Create destination directory if it doesn't exist
        create_directory(dst_path.parent)
        
        # Move the file
        shutil.move(str(src_path), str(dst_path))
        
        logger.log_operation("move_file_safely", {
            "source": str(source),
            "destination": str(destination),
            "overwrite": overwrite
        })
        
        return True
        
    except Exception as e:
        logger.log_error("move_file_safely", e, {
            "source": str(source),
            "destination": str(destination)
        })
        return False


def copy_file_safely(source: Union[str, Path], destination: Union[str, Path], 
                    overwrite: bool = False) -> bool:
    """
    Safely copy a file to another location.
    
    Args:
        source: Source file path
        destination: Destination file path
        overwrite: If True, overwrite existing file
        
    Returns:
        True if successful, False otherwise
    """
    try:
        src_path = Path(source)
        dst_path = Path(destination)
        
        if not src_path.exists():
            raise FileError(f"Source file does not exist: {source}")
        
        if dst_path.exists() and not overwrite:
            raise FileError(f"Destination file exists: {destination}")
        
        # Create destination directory if it doesn't exist
        create_directory(dst_path.parent)
        
        # Copy the file
        shutil.copy2(str(src_path), str(dst_path))
        
        logger.log_operation("copy_file_safely", {
            "source": str(source),
            "destination": str(destination),
            "overwrite": overwrite
        })
        
        return True
        
    except Exception as e:
        logger.log_error("copy_file_safely", e, {
            "source": str(source),
            "destination": str(destination)
        })
        return False


def create_temp_file(suffix: str = "", prefix: str = "etl_", directory: Optional[str] = None) -> str:
    """
    Create a temporary file and return its path.
    
    Args:
        suffix: File suffix/extension
        prefix: File prefix
        directory: Directory to create temp file in
        
    Returns:
        Path to the temporary file
    """
    try:
        fd, temp_path = tempfile.mkstemp(
            suffix=suffix,
            prefix=prefix,
            dir=directory
        )
        
        # Close the file descriptor
        os.close(fd)
        
        logger.log_operation("create_temp_file", {
            "temp_path": temp_path,
            "suffix": suffix,
            "prefix": prefix
        })
        
        return temp_path
        
    except Exception as e:
        logger.log_error("create_temp_file", e)
        raise


def create_temp_directory(prefix: str = "etl_", directory: Optional[str] = None) -> str:
    """
    Create a temporary directory and return its path.
    
    Args:
        prefix: Directory prefix
        directory: Parent directory to create temp directory in
        
    Returns:
        Path to the temporary directory
    """
    try:
        temp_dir = tempfile.mkdtemp(
            prefix=prefix,
            dir=directory
        )
        
        logger.log_operation("create_temp_directory", {
            "temp_dir": temp_dir,
            "prefix": prefix
        })
        
        return temp_dir
        
    except Exception as e:
        logger.log_error("create_temp_directory", e)
        raise


def extract_archive(archive_path: Union[str, Path], extract_to: Union[str, Path]) -> List[str]:
    """
    Extract archive file and return list of extracted files.
    
    Args:
        archive_path: Path to the archive file
        extract_to: Directory to extract files to
        
    Returns:
        List of extracted file paths
    """
    try:
        archive_path = Path(archive_path)
        extract_to = Path(extract_to)
        
        # Create extraction directory
        create_directory(extract_to)
        
        extracted_files = []
        
        if archive_path.suffix.lower() == '.zip':
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
                extracted_files = [str(extract_to / name) for name in zip_ref.namelist()]
                
        elif archive_path.suffix.lower() in ['.tar', '.tar.gz', '.tar.bz2']:
            with tarfile.open(archive_path, 'r') as tar_ref:
                tar_ref.extractall(extract_to)
                extracted_files = [str(extract_to / name) for name in tar_ref.getnames()]
        
        else:
            raise FileError(f"Unsupported archive format: {archive_path.suffix}")
        
        logger.log_operation("extract_archive", {
            "archive_path": str(archive_path),
            "extract_to": str(extract_to),
            "extracted_count": len(extracted_files)
        })
        
        return extracted_files
        
    except Exception as e:
        logger.log_error("extract_archive", e, {
            "archive_path": str(archive_path),
            "extract_to": str(extract_to)
        })
        raise


def list_directory_files(directory_path: Union[str, Path], 
                        pattern: str = "*", 
                        recursive: bool = False) -> List[Dict[str, Any]]:
    """
    List files in directory with their information.
    
    Args:
        directory_path: Path to the directory
        pattern: File pattern to match
        recursive: If True, search recursively
        
    Returns:
        List of file information dictionaries
    """
    try:
        directory_path = Path(directory_path)
        
        if not directory_path.exists():
            raise FileError(f"Directory does not exist: {directory_path}")
        
        if recursive:
            files = directory_path.rglob(pattern)
        else:
            files = directory_path.glob(pattern)
        
        file_list = []
        for file_path in files:
            if file_path.is_file():
                file_info = get_file_info(file_path)
                file_list.append(file_info)
        
        logger.log_operation("list_directory_files", {
            "directory_path": str(directory_path),
            "pattern": pattern,
            "recursive": recursive,
            "file_count": len(file_list)
        })
        
        return file_list
        
    except Exception as e:
        logger.log_error("list_directory_files", e, {
            "directory_path": str(directory_path),
            "pattern": pattern
        })
        return []


def cleanup_old_files(directory_path: Union[str, Path], 
                     days_old: int = 7, 
                     pattern: str = "*",
                     dry_run: bool = False) -> Dict[str, Any]:
    """
    Clean up old files in directory.
    
    Args:
        directory_path: Path to the directory
        days_old: Files older than this many days will be deleted
        pattern: File pattern to match
        dry_run: If True, only list files that would be deleted
        
    Returns:
        Dictionary with cleanup results
    """
    try:
        directory_path = Path(directory_path)
        cutoff_time = datetime.now().timestamp() - (days_old * 24 * 3600)
        
        files = directory_path.glob(pattern)
        old_files = []
        deleted_files = []
        total_size_deleted = 0
        
        for file_path in files:
            if file_path.is_file():
                file_mtime = file_path.stat().st_mtime
                if file_mtime < cutoff_time:
                    file_size = file_path.stat().st_size
                    old_files.append({
                        "path": str(file_path),
                        "size": file_size,
                        "modified": datetime.fromtimestamp(file_mtime)
                    })
                    
                    if not dry_run:
                        try:
                            file_path.unlink()
                            deleted_files.append(str(file_path))
                            total_size_deleted += file_size
                        except Exception as delete_error:
                            logger.log_error("cleanup_file_deletion", delete_error, {
                                "file_path": str(file_path)
                            })
        
        result = {
            "directory": str(directory_path),
            "days_old": days_old,
            "pattern": pattern,
            "dry_run": dry_run,
            "old_files_found": len(old_files),
            "files_deleted": len(deleted_files),
            "total_size_deleted": total_size_deleted,
            "size_deleted_human": format_file_size(total_size_deleted)
        }
        
        if dry_run:
            result["files_to_delete"] = old_files
        else:
            result["deleted_files"] = deleted_files
        
        logger.log_operation("cleanup_old_files", result)
        
        return result
        
    except Exception as e:
        logger.log_error("cleanup_old_files", e, {
            "directory_path": str(directory_path),
            "days_old": days_old
        })
        return {"error": str(e)}


def ensure_file_permissions(file_path: Union[str, Path], mode: int = 0o644) -> bool:
    """
    Ensure file has correct permissions.
    
    Args:
        file_path: Path to the file
        mode: Permission mode (octal)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        Path(file_path).chmod(mode)
        
        logger.log_operation("ensure_file_permissions", {
            "file_path": str(file_path),
            "mode": oct(mode)
        })
        
        return True
        
    except Exception as e:
        logger.log_error("ensure_file_permissions", e, {
            "file_path": str(file_path),
            "mode": oct(mode)
        })
        return False