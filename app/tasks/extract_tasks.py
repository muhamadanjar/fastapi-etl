# ==============================================
# app/tasks/etl_tasks.py
# ==============================================
import asyncio
import os
import json
import shutil
import traceback
import hashlib
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path
from sqlmodel import Session, select
import pandas as pd

from app.core.enums import ProcessingStatus

from .celery_app import celery_app
from app.infrastructure.db.models.raw_data.file_registry import FileRegistry
from app.infrastructure.db.models.raw_data.raw_records import RawRecords
from app.infrastructure.db.models.staging.standardized_data import StandardizedData, StandardizedDataCreate
from app.infrastructure.db.models.raw_data.rejected_records import RejectedRecord
from app.infrastructure.db.models.processed.entities import Entity
from app.infrastructure.db.models.processed.entity_relationships import EntityRelationship
from app.infrastructure.db.models.etl_control.etl_jobs import EtlJob
from app.infrastructure.db.models.etl_control.job_executions import JobExecution
from app.infrastructure.db.models.etl_control.error_logs import ErrorType, ErrorLog, ErrorSeverity
from app.infrastructure.db.models.etl_control.quality_check_results import QualityCheckResult, QualityCheckResultCreate
from app.infrastructure.db.models.etl_control.quality_rules import QualityRule
from app.infrastructure.db.models.audit.data_lineage import DataLineage, DataLineageCreate
from app.infrastructure.db.models.audit.change_log import ChangeLog, ChangeLogCreate
from app.infrastructure.db.manager import get_session
from celery import group
from app.processors import get_processor
from app.transformers import create_transformation_pipeline
from app.application.services.etl_service import ETLService
from app.application.services.file_service import FileService
from app.application.services.data_quality_service import DataQualityService
from app.utils.logger import get_logger
from app.core.exceptions import ETLException, FileProcessingException
from app.utils.event_publisher import get_event_publisher
from app.tasks.task_helpers import log_task_error, get_error_type_from_exception, get_error_severity_from_exception

logger = get_logger(__name__)

def process_file_task(self, file_id: str, user_id:str, processing_config: Dict[str, Any] = None):
    """
    PATH A — File Upload pipeline (staging-only).
    Processes an uploaded file: extract -> standardize -> store in `staging`
    (StandardizedData). It does NOT run the full Transform/Load into
    `processed` entities. Use the job-execution path (execute_etl_job)
    with job_type='full_etl' for the complete Extract->Transform->Load flow.

    Args:
        file_id: ID of the file to process
        user_id: ID of the uploading user
        processing_config: Optional processing configuration

    Returns:
        Processing results and statistics (staging scope only)
    """
    task_id = self.request.id
    logger.info(f"Starting file processing task {task_id} for file {file_id}")
    logger.info(f"User ID: {user_id}", extra=processing_config)
    print(f"Starting file processing task {task_id} for file {file_id}")
    
    try:
        with get_session() as db:
            # Get file record
            file_record = db.exec(select(FileRegistry).where(FileRegistry.id == file_id)).first()
            if not file_record:
                raise FileProcessingException(f"File record not found: {file_id}")
            
            # Update file status
            file_record.processing_status = ProcessingStatus.PROCESSING.value
            db.add(file_record)
            db.commit()
            
            # Determine processor type
            file_type = file_record.file_type.lower()
            processor = get_processor(file_type, db_session=db, batch_id=task_id)
            
            # Process the file
            file_path = file_record.file_path
            if not os.path.exists(file_path):
                raise FileProcessingException(f"File not found: {file_path}")
            
            # Validate file format
            # is_valid, error_message = await processor.validate_file_format(file_path)
            is_valid, error_message = asyncio.run(
                processor.validate_file_format(file_path)
            )
            if not is_valid:
                raise FileProcessingException(f"Invalid file format: {error_message}")
            
            # Process the file
            # processing_results = await processor.process_file(file_path, file_record)
            processing_results = asyncio.run(
                processor.process_file(file_path, file_record)
            )
            
            # Update file status based on results
            if processing_results.get('successful_records', 0) > 0:
                file_record.processing_status = ProcessingStatus.COMPLETED.value
            else:
                file_record.processing_status = ProcessingStatus.FAILED.value
            
            # Update metadata with processing results
            metadata = file_record.file_metadata or {}
            metadata.update({
                'processing_results': processing_results,
                'processed_at': datetime.utcnow().isoformat(),
                'task_id': task_id
            })
            file_record.file_metadata = metadata
            
            db.add(file_record)
            db.commit()
            
            logger.info(f"File processing completed for {file_id}: {processing_results}")
            
            return {
                'file_id': file_id,
                'status': 'completed',
                'results': processing_results
            }
            
    except FileProcessingException as e:
        logger.error(f"File processing failed for {file_id}: {str(e)}")
        
        # Log error to database
        try:
            with get_session() as db:
                asyncio.run(log_task_error(
                    db=db,
                    exception=e,
                    error_type=ErrorType.PROCESSING_ERROR,
                    error_severity=get_error_severity_from_exception(e),
                    context={
                        "file_id": file_id,
                        "task_id": task_id,
                        "file_type": file_type if 'file_type' in locals() else None
                    }
                ))
        except Exception as log_error:
            logger.error(f"Failed to log error to database: {log_error}")
        
        # Update file status
        try:
            with get_session() as db:
                if file_record is None:
                    file_record = db.exec(select(FileRegistry).where(FileRegistry.id == file_id)).first()
                
                if file_record:
                    file_record.processing_status = ProcessingStatus.FAILED.value
                    metadata = file_record.file_metadata or {}
                    metadata.update({
                        'error': str(e),
                        'failed_at': datetime.utcnow().isoformat(),
                        'task_id': task_id
                    })
                    file_record.file_metadata = metadata
                    db.add(file_record)
                    db.commit()
        except Exception as commit_error:
            logger.error(f"Failed to update file status after FileProcessingException: {commit_error}")
        
        # Retry the task if retries are available
        if self.request.retries < self.max_retries:
            logger.info(f"Retrying file processing for {file_id} (attempt {self.request.retries + 1})")
            raise self.retry(exc=e, countdown=self.default_retry_delay)
        
        raise ETLException(f"File processing failed after {self.max_retries} retries: {str(e)}")
        
    except Exception as e:
        logger.error(f"Unexpected error in file processing for {file_id}: {str(e)}")
        logger.error(traceback.format_exc())

        # Log error to database
        try:
            with get_session() as db:
                asyncio.run(log_task_error(
                    db=db,
                    exception=e,
                    error_type=ErrorType.SYSTEM_ERROR,
                    error_severity=get_error_severity_from_exception(e),
                    context={
                        "file_id": file_id,
                        "task_id": task_id
                    }
                ))
        except Exception as log_error:
            logger.error(f"Failed to log error to database: {log_error}")
        
        # Update file status to failed
        try:
            with get_session() as db:
                file_record = db.exec(select(FileRegistry).where(FileRegistry.id == file_id)).first()
                if file_record:
                    file_record.processing_status = ProcessingStatus.FAILED.value
                    metadata = file_record.file_metadata or {}
                    metadata.update({
                        'error': str(e),
                        'failed_at': datetime.utcnow().isoformat(),
                        'task_id': task_id
                    })
                    file_record.file_metadata = metadata
                    db.add(file_record)
                    db.commit()
        except Exception as commit_error:
            logger.error(f"Failed to update file status: {commit_error}")
        
        # Retry the task if retries are available
        if self.request.retries < self.max_retries:
            logger.info(f"Retrying file processing for {file_id} (attempt {self.request.retries + 1})")
            raise self.retry(exc=e, countdown=self.default_retry_delay)
        
        raise ETLException(f"File processing failed after {self.max_retries} retries: {str(e)}")
    
    

async def _execute_extract_job(db: Session, execution_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Execute extract job"""
    logger.info(f"Executing extract job for execution {execution_id}")
    
    # Get source configuration
    source_type = config.get('source_type', 'file')
    
    if source_type == 'file':
        # File-based extraction
        file_ids = config.get('file_ids', [])
        results = []

        for file_id in file_ids:
            # Process each file.
            # NOTE: do NOT call process_file_task.apply_async(...).get() from
            # inside another Celery task — that blocks the worker and can
            # deadlock. Invoke the task body directly via .run().
            file_result = process_file_task.run(file_id, None, config)
            results.append(file_result)
        
        return {
            'records_processed': sum(r.get('processing_results', {}).get('total_records', 0) for r in results),
            'records_successful': sum(r.get('processing_results', {}).get('successful_records', 0) for r in results),
            'files_processed': len(results),
            'logs': [f"Processed file: {r.get('file_id')}" for r in results]
        }
    
    elif source_type == 'api':
        # API-based extraction
        from app.processors.api_processor import APIProcessor

        api_processor = APIProcessor(db, execution_id, **config)
        # API extraction must be implemented per data source. Raising
        # explicitly prevents silent no-op (returning 0 records).
        raise NotImplementedError(
            f"API extraction not implemented for source config: {config}"
        )
    
    else:
        raise ETLException(f"Unknown source type: {source_type}")


def validate_file_format_task(self, file_id: str):
    """
    Validate file format before processing

    Args:
        file_id: ID of the file to validate

    Returns:
        Validation results
    """
    task_id = self.request.id
    logger.info(f"Starting file format validation task {task_id} for file {file_id}")

    with get_session() as db:
        try:
            # Get file record
            file_record = db.exec(select(FileRegistry).where(FileRegistry.id == file_id)).first()
            if not file_record:
                raise FileProcessingException(f"File record not found: {file_id}")

            # Get appropriate processor
            file_type = file_record.file_type.lower()
            processor = get_processor(file_type, db_session=db)

            # Validate file format
            is_valid, error_message = asyncio.run(processor.validate_file_format(file_record.file_path))

            # Update file metadata
            metadata = file_record.file_metadata or {}
            metadata.update({
                'format_validated': True,
                'format_valid': is_valid,
                'validation_error': error_message if not is_valid else None,
                'validated_at': datetime.utcnow().isoformat(),
                'validation_task_id': task_id
            })
            file_record.file_metadata = metadata
            db.add(file_record)
            db.commit()

            return {
                'status': 'success',
                'file_id': file_id,
                'is_valid': is_valid,
                'error_message': error_message,
                'task_id': task_id,
                'validated_at': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"File format validation failed for {file_id}: {str(e)}")
            raise FileProcessingException(f"File format validation failed: {str(e)}")


def preview_file_data_task(self, file_id: str, rows: int = 10):
    """
    Generate preview of file data

    Args:
        file_id: ID of the file to preview
        rows: Number of rows to preview

    Returns:
        File preview data
    """
    task_id = self.request.id
    logger.info(f"Starting file preview task {task_id} for file {file_id}")

    with get_session() as db:
        try:
            # Get file record
            file_record = db.exec(select(FileRegistry).where(FileRegistry.id == file_id)).first()
            if not file_record:
                raise FileProcessingException(f"File record not found: {file_id}")

            # Get appropriate processor
            file_type = file_record.file_type.lower()
            processor = get_processor(file_type, db_session=db)

            # Generate preview
            preview_data = asyncio.run(processor.preview_data(file_record.file_path, rows))

            return {
                'status': 'success',
                'file_id': file_id,
                'preview_data': preview_data,
                'task_id': task_id,
                'generated_at': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"File preview failed for {file_id}: {str(e)}")
            raise FileProcessingException(f"File preview failed: {str(e)}")


def batch_process_files_task(self, file_ids: List[str], processing_config: Dict[str, Any] = None):
    """
    Process multiple files in batch
    
    Args:
        file_ids: List of file IDs to process
        processing_config: Processing configuration
        
    Returns:
        Batch processing results
    """
    task_id = self.request.id
    logger.info(f"Starting batch file processing task {task_id} for {len(file_ids)} files")
    
    batch_results = {
        'total_files': len(file_ids),
        'successful_files': 0,
        'failed_files': 0,
        'file_results': [],
        'errors': []
    }
    
    try:
        # Process files in parallel using Celery group
        # Create group of file processing tasks
        file_tasks = group(
            process_file_task.s(file_id, processing_config)
            for file_id in file_ids
        )
        
        # Execute group and wait for results
        result = file_tasks.apply_async()
        
        # Collect results
        for i, file_result in enumerate(result.get()):
            batch_results['file_results'].append(file_result)
            
            if file_result.get('status') == 'success':
                batch_results['successful_files'] += 1
            else:
                batch_results['failed_files'] += 1
                batch_results['errors'].append({
                    'file_id': file_ids[i],
                    'error': file_result.get('error', 'Unknown error')
                })
        
        logger.info(f"Batch processing completed: {batch_results['successful_files']}/{batch_results['total_files']} successful")
        
        return {
            'status': 'success',
            'task_id': task_id,
            'batch_results': batch_results,
            'completed_at': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Batch file processing failed: {str(e)}")
        raise ETLException(f"Batch file processing failed: {str(e)}")

