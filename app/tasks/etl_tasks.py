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

@celery_app.task(
    bind=True,
    name='app.tasks.etl_tasks.process_file_task',
    max_retries=3,
    default_retry_delay=300,  # 5 minutes
    time_limit=1800,  # 30 minutes
    soft_time_limit=1500  # 25 minutes
)
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
    
    
@celery_app.task(
    bind=True,
    name='app.tasks.etl_tasks.transformation_pipeline',
    max_retries=2,
    default_retry_delay=600,  # 10 minutes
    time_limit=3600,  # 1 hour
    soft_time_limit=3300  # 55 minutes
)
def run_transformation_pipeline(self, job_execution_id: str, transformation_config: Dict[str, Any]):
    """
    Run data transformation pipeline

    Args:
        job_execution_id: ID of the job execution
        transformation_config: Transformation configuration

    Returns:
        Transformation results
    """
    task_id = self.request.id
    logger.info(f"Starting transformation pipeline task {task_id} for execution {job_execution_id}")

    with get_session() as db:
        try:
            # Get job execution record
            execution = db.exec(select(JobExecution).where(JobExecution.id == job_execution_id)).first()
            if not execution:
                raise ETLException(f"Job execution not found: {job_execution_id}")

            # Update execution status
            execution.status = 'RUNNING'
            execution.start_time = datetime.utcnow()
            db.add(execution)
            db.commit()

            # Create transformation pipeline
            stages = transformation_config.get('stages', ['clean', 'validate', 'normalize'])
            pipeline = create_transformation_pipeline(stages, **transformation_config)

            # Get source data
            source_query = transformation_config.get('source_query')
            if source_query:
                source_data = pd.read_sql(source_query, db.connection())
                input_records = source_data.to_dict('records')
            else:
                # Default: get recent raw records
                recent_records = db.exec(
                    select(RawRecords)
                    .where(RawRecords.validation_status == 'VALID')
                    .limit(transformation_config.get('limit', 10000))
                ).all()
                input_records = [record.raw_data for record in recent_records]

            # Execute transformation pipeline
            total_results = {}
            for stage_idx, transformer in enumerate(pipeline):
                stage_name = stages[stage_idx] if stage_idx < len(stages) else f"stage_{stage_idx}"
                logger.info(f"Executing transformation stage: {stage_name}")

                # Transform the data
                if stage_idx == 0:
                    # First stage uses input records
                    stage_results = asyncio.run(transformer.transform_dataset(
                        input_records,
                        output_entity_type=transformation_config.get('output_entity_type')
                    ))
                else:
                    # Subsequent stages use results from previous stage
                    previous_results = total_results[stages[stage_idx - 1]]['results']
                    stage_data = [result.data for result in previous_results if result.is_success()]
                    stage_results = asyncio.run(transformer.transform_dataset(
                        stage_data,
                        output_entity_type=transformation_config.get('output_entity_type')
                    ))

                total_results[stage_name] = stage_results

                # Check if stage failed critically
                if not stage_results['success']:
                    raise ETLException(f"Transformation stage '{stage_name}' failed critically")

            # Update execution with results
            execution.status = 'SUCCESS'
            execution.end_time = datetime.utcnow()
            execution.records_processed = sum(r['statistics']['records_processed'] for r in total_results.values())
            execution.records_successful = sum(r['statistics']['records_transformed'] for r in total_results.values())
            execution.records_failed = sum(r['statistics']['records_failed'] for r in total_results.values())

            # Store performance metrics
            performance_metrics = {
                'total_processing_time': (execution.end_time - execution.start_time).total_seconds(),
                'stages_executed': len(stages),
                'stage_results': {stage: result['statistics'] for stage, result in total_results.items()},
                'task_id': task_id
            }
            execution.performance_metrics = performance_metrics

            db.add(execution)
            db.commit()

            logger.info(f"Transformation pipeline completed for execution {job_execution_id}")

            return {
                'status': 'success',
                'execution_id': job_execution_id,
                'task_id': task_id,
                'results': total_results,
                'performance_metrics': performance_metrics,
                'completed_at': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Transformation pipeline failed for execution {job_execution_id}: {str(e)}")

            # Update execution status
            try:
                if 'execution' in locals():
                    execution.status = 'FAILED'
                    execution.end_time = datetime.utcnow()
                    execution.execution_log = str(e)
                    db.add(execution)
                    db.commit()
            except Exception as commit_error:
                logger.error(f"Failed to update execution status: {commit_error}")

            # Retry if retries available
            if self.request.retries < self.max_retries:
                logger.info(f"Retrying transformation pipeline for {job_execution_id}")
                raise self.retry(exc=e, countdown=self.default_retry_delay)

            raise ETLException(f"Transformation pipeline failed: {str(e)}")

@celery_app.task(
    bind=True,
    name='app.tasks.etl_tasks.execute_job',
    max_retries=2,
    default_retry_delay=900,  # 15 minutes
    time_limit=7200,  # 2 hours
    soft_time_limit=6900  # 1 hour 55 minutes
)
def execute_etl_job(self, job_id: str, execution_id: str = None, batch_id: str = None, parameters: Dict = None):
    """
    PATH B — Job Execution pipeline (full or partial).
    Orchestrates the complete ETL flow based on `job.job_type`:
      - 'extract'       -> _execute_extract_job    (raw -> staging)
      - 'transform'     -> _execute_transform_job (staging -> transformation)
      - 'load'          -> _execute_load_job       (transformation -> processed)
      - 'full_etl'      -> _execute_full_etl_job   (extract -> transform -> load)

    After the core phases, runs Phase 7 post-processing (triggers
    dependent jobs). This is the COMPLETE pipeline — distinct from
    PATH A (process_file_task) which only stages an uploaded file.

    Args:
        job_id: ID of the ETL job to execute
        execution_id: Execution ID
        batch_id: Batch ID
        parameters: Optional execution parameters

    Returns:
        Job execution results
    """
    task_id = self.request.id
    logger.info(f"Starting ETL job execution task {task_id} for job {job_id}")

    with get_session() as db:
        try:
            # Get ETL job
            job = db.exec(select(EtlJob).where(EtlJob.id == job_id)).first()
            if not job:
                raise ETLException(f"ETL job not found: {job_id}")

            if not job.is_active:
                raise ETLException(f"ETL job is not active: {job_id}")

            # Create job execution record
            execution = JobExecution(
                job_id=job_id,
                status='RUNNING',
                start_time=datetime.utcnow(),
                execution_log=f"Started by task {task_id}",
                performance_metrics={'task_id': task_id}
            )
            db.add(execution)
            db.commit()
            db.refresh(execution)

            if not execution_id:
                execution_id = str(execution.id)

            # Get job configuration
            job_config = job.job_config or {}
            if parameters:
                job_config.update(parameters)

            # Execute ETL steps based on job type
            job_type = job.job_type.lower()

            if job_type == 'extract':
                result = asyncio.run(_execute_extract_job(db, execution_id, job_config))
            elif job_type == 'transform':
                result = asyncio.run(_execute_transform_job(db, execution_id, job_config))
            elif job_type == 'load':
                result = asyncio.run(_execute_load_job(db, execution_id, job_config))
            elif job_type == 'full_etl':
                result = asyncio.run(_execute_full_etl_job(db, execution_id, job_config))
            else:
                raise ETLException(f"Unknown job type: {job_type}")

            # Update execution with results
            execution.status = 'SUCCESS'
            execution.end_time = datetime.utcnow()
            execution.records_processed = result.get('records_processed', 0)
            execution.records_successful = result.get('records_successful', 0)
            execution.records_failed = result.get('records_failed', 0)
            execution.execution_log = json.dumps(result.get('logs', []))

            # Mark execution as successful
            execution.status = 'SUCCESS'
            execution.end_time = datetime.utcnow()

            # Update performance metrics
            performance_metrics = execution.performance_metrics or {}
            performance_metrics.update(result.get('performance_metrics', {}))
            execution.performance_metrics = performance_metrics

            db.add(execution)
            db.commit()

            logger.info(f"ETL job {job_id} completed successfully (Phase 6 complete)")

            # ===== PHASE 7: POST-PROCESSING =====
            logger.info(f"Starting Phase 7 post-processing for execution {execution_id}")

            post_process_result = {'status': 'failed', 'total_triggered': 0}

            try:
                # Execute post-processing phase
                post_process_result = asyncio.run(post_process_job(
                    db=db,
                    execution_id=execution_id,
                    job_id=job_id,
                    job_name=job.job_name
                ))

                logger.info(
                    f"Phase 7 post-processing completed: "
                    f"dependent_jobs_triggered={post_process_result.get('dependent_jobs_triggered', 0)}"
                )

            except Exception as post_process_error:
                # Post-processing failures should not block job completion
                logger.error(
                    f"Phase 7 post-processing failed (non-blocking): {str(post_process_error)}",
                    exc_info=True
                )

                # Log the error but continue
                post_process_result = {
                    'status': 'failed',
                    'error': str(post_process_error),
                    'total_triggered': 0
                }

            return {
                'status': 'success',
                'job_id': job_id,
                'execution_id': execution_id,
                'task_id': task_id,
                'result': result,
                'post_processing': post_process_result,
                'completed_at': datetime.utcnow().isoformat(),
                'dependent_jobs_triggered': post_process_result.get('total_triggered', 0)
            }

        except Exception as e:
            logger.error(f"ETL job execution failed for job {job_id}: {str(e)}")

            # Update execution status
            try:
                if 'execution' in locals():
                    execution.status = 'FAILED'
                    execution.end_time = datetime.utcnow()
                    execution.execution_log = str(e)
                    error_details = {
                        'error_type': type(e).__name__,
                        'error_message': str(e),
                        'task_id': task_id
                    }
                    execution.error_details = error_details
                    db.add(execution)
                    db.commit()

                    # Publish job failed event
                    try:
                        publisher = asyncio.run(get_event_publisher())
                        asyncio.run(publisher.publish_job_failed(
                            job_id=job.job_id if 'job' in locals() else job_id,
                            execution_id=str(execution.id) if 'execution' in locals() else None,
                            job_name=job.job_name if 'job' in locals() else "Unknown",
                            error=str(e)
                        ))
                    except Exception as pub_error:
                        logger.warning(f"Failed to publish job failed event: {str(pub_error)}")

            except Exception as commit_error:
                logger.error(f"Failed to update execution status: {commit_error}")

            # Log error to database
            try:
                asyncio.run(log_task_error(
                    db=db,
                    exception=e,
                    error_type=get_error_type_from_exception(e),
                    error_severity=get_error_severity_from_exception(e),
                    context={
                        "job_id": job_id,
                        "execution_id": execution_id if 'execution_id' in locals() else None,
                        "task_id": task_id
                    }
                ))
            except Exception as log_error:
                logger.error(f"Failed to log error to database: {log_error}")

            # Retry if retries available
            if self.request.retries < self.max_retries:
                logger.info(f"Retrying ETL job execution for {job_id}")
                raise self.retry(exc=e, countdown=self.default_retry_delay)

            raise ETLException(f"ETL job execution failed: {str(e)}")

@celery_app.task(
    bind=True,
    name='etl.validate_data_quality',
    max_retries=1,
    default_retry_delay=300,
    time_limit=1800
)
def validate_data_quality(self, entity_type: str = None, validation_config: Dict[str, Any] = None):
    """
    Run data quality validation

    Args:
        entity_type: Type of entity to validate
        validation_config: Validation configuration

    Returns:
        Data quality results
    """
    task_id = self.request.id
    logger.info(f"Starting data quality validation task {task_id}")

    with get_session() as db:
        try:
            quality_service = DataQualityService(db)

            # Run quality checks
            if entity_type:
                results = asyncio.run(quality_service.run_quality_check(entity_type=entity_type, check_config=validation_config))
            else:
                # Run all active quality rules
                results = asyncio.run(quality_service.run_all_quality_checks())

            # Generate quality report
            report = asyncio.run(quality_service.generate_quality_report(entity_type))

            logger.info(f"Data quality validation completed with {len(results)} checks")

            return {
                'status': 'success',
                'task_id': task_id,
                'validation_results': results,
                'quality_report': report,
                'completed_at': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Data quality validation failed: {str(e)}")

            if self.request.retries < self.max_retries:
                raise self.retry(exc=e, countdown=self.default_retry_delay)

            raise ETLException(f"Data quality validation failed: {str(e)}")

@celery_app.task(
    bind=True,
    name='app.tasks.etl_tasks.generate_lineage',
    max_retries=1,
    time_limit=900
)
def generate_data_lineage(self, source_entity: str = None, target_entity: str = None):
    """
    Generate data lineage information

    Args:
        source_entity: Source entity name
        target_entity: Target entity name

    Returns:
        Data lineage information
    """
    task_id = self.request.id
    logger.info(f"Starting data lineage generation task {task_id}")

    with get_session() as db:
        try:
            # Build lineage query
            lineage_query = select(DataLineage)

            if source_entity:
                lineage_query = lineage_query.where(DataLineage.source_entity == source_entity)
            if target_entity:
                lineage_query = lineage_query.where(DataLineage.target_entity == target_entity)

            # Get lineage records
            lineage_records = db.exec(lineage_query).all()

            # Build lineage graph
            lineage_graph = {
                'nodes': set(),
                'edges': [],
                'transformations': {}
            }

            for record in lineage_records:
                # Add nodes
                lineage_graph['nodes'].add(f"{record.source_entity}.{record.source_field}")
                lineage_graph['nodes'].add(f"{record.target_entity}.{record.target_field}")

                # Add edge
                edge = {
                    'source': f"{record.source_entity}.{record.source_field}",
                    'target': f"{record.target_entity}.{record.target_field}",
                    'transformation': record.transformation_applied,
                    'execution_id': record.execution_id
                }
                lineage_graph['edges'].append(edge)

                # Store transformation details
                key = f"{edge['source']} -> {edge['target']}"
                lineage_graph['transformations'][key] = record.transformation_applied

            # Convert set to list for JSON serialization
            lineage_graph['nodes'] = list(lineage_graph['nodes'])

            logger.info(f"Data lineage generation completed with {len(lineage_graph['edges'])} relationships")

            return {
                'status': 'success',
                'task_id': task_id,
                'lineage_graph': lineage_graph,
                'total_relationships': len(lineage_graph['edges']),
                'completed_at': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Data lineage generation failed: {str(e)}")

            if self.request.retries < self.max_retries:
                raise self.retry(exc=e, countdown=self.default_retry_delay)

            raise ETLException(f"Data lineage generation failed: {str(e)}")

@celery_app.task(
    bind=True,
    name='app.tasks.etl_tasks.cleanup_files',
    time_limit=1800
)
def cleanup_old_files(self, days_old: int = 30, file_types: List[str] = None):
    """
    Clean up old processed files

    Args:
        days_old: Number of days old files to clean up
        file_types: List of file types to clean up

    Returns:
        Cleanup results
    """
    task_id = self.request.id
    logger.info(f"Starting file cleanup task {task_id}")

    with get_session() as db:
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days_old)

            # Build query for old files
            query = select(FileRegistry).where(
                FileRegistry.upload_date < cutoff_date,
                FileRegistry.processing_status == 'COMPLETED'
            )

            if file_types:
                query = query.where(FileRegistry.file_type.in_(file_types))

            old_files = db.exec(query).all()

            cleanup_results = {
                'files_processed': 0,
                'files_deleted': 0,
                'space_freed': 0,
                'errors': []
            }

            for file_record in old_files:
                try:
                    cleanup_results['files_processed'] += 1

                    # Get file size before deletion
                    if os.path.exists(file_record.file_path):
                        file_size = os.path.getsize(file_record.file_path)

                        # Delete physical file
                        os.remove(file_record.file_path)
                        cleanup_results['files_deleted'] += 1
                        cleanup_results['space_freed'] += file_size

                        logger.debug(f"Deleted file: {file_record.file_path}")

                    # Update database record
                    file_record.processing_status = 'ARCHIVED'
                    file_record.file_path = None  # Clear file path since file is deleted
                    db.add(file_record)

                except Exception as e:
                    error_msg = f"Failed to delete file {file_record.id}: {str(e)}"
                    cleanup_results['errors'].append(error_msg)
                    logger.warning(error_msg)

            db.commit()

            # Convert bytes to MB for reporting
            space_freed_mb = cleanup_results['space_freed'] / (1024 * 1024)

            logger.info(f"File cleanup completed: {cleanup_results['files_deleted']} files deleted, {space_freed_mb:.2f} MB freed")

            return {
                'status': 'success',
                'task_id': task_id,
                'cleanup_results': cleanup_results,
                'space_freed_mb': space_freed_mb,
                'completed_at': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"File cleanup failed: {str(e)}")
            raise ETLException(f"File cleanup failed: {str(e)}")

@celery_app.task(
    bind=True,
    name='app.tasks.etl_tasks.backup_data',
    time_limit=3600
)
def backup_processed_data(self, backup_config: Dict[str, Any] = None):
    """
    Backup processed data

    Args:
        backup_config: Backup configuration

    Returns:
        Backup results
    """
    task_id = self.request.id
    logger.info(f"Starting data backup task {task_id}")

    with get_session() as db:
        try:
            # Default backup configuration
            config = {
                'backup_path': '/app/storage/backups',
                'include_tables': ['processed.entities', 'processed.aggregated_data'],
                'format': 'json',  # json, csv, sql
                'compress': True
            }

            if backup_config:
                config.update(backup_config)

            backup_timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            backup_dir = Path(config['backup_path']) / f"backup_{backup_timestamp}"
            backup_dir.mkdir(parents=True, exist_ok=True)

            backup_results = {
                'tables_backed_up': 0,
                'total_records': 0,
                'backup_size': 0,
                'backup_path': str(backup_dir),
                'files_created': []
            }

            for table_name in config['include_tables']:
                try:
                    # Query data from table
                    query = f"SELECT * FROM {table_name}"
                    df = pd.read_sql(query, db.connection())

                    if df.empty:
                        logger.warning(f"No data found in table {table_name}")
                        continue

                    # Generate backup file
                    safe_table_name = table_name.replace('.', '_')

                    if config['format'] == 'json':
                        backup_file = backup_dir / f"{safe_table_name}.json"
                        df.to_json(backup_file, orient='records', date_format='iso')
                    elif config['format'] == 'csv':
                        backup_file = backup_dir / f"{safe_table_name}.csv"
                        df.to_csv(backup_file, index=False)
                    elif config['format'] == 'sql':
                        backup_file = backup_dir / f"{safe_table_name}.sql"
                        # This would need more sophisticated SQL generation
                        df.to_sql(safe_table_name, db.connection(), if_exists='replace', index=False)

                    backup_results['tables_backed_up'] += 1
                    backup_results['total_records'] += len(df)
                    backup_results['files_created'].append(str(backup_file))

                    logger.debug(f"Backed up table {table_name}: {len(df)} records")

                except Exception as e:
                    logger.error(f"Failed to backup table {table_name}: {str(e)}")
                    continue

            # Compress backup if requested
            if config['compress']:
                import tarfile

                archive_path = backup_dir.parent / f"backup_{backup_timestamp}.tar.gz"
                with tarfile.open(archive_path, 'w:gz') as tar:
                    tar.add(backup_dir, arcname=f"backup_{backup_timestamp}")

                # Remove uncompressed directory
                shutil.rmtree(backup_dir)

                backup_results['backup_path'] = str(archive_path)
                backup_results['compressed'] = True

            # Calculate backup size
            if Path(backup_results['backup_path']).exists():
                backup_results['backup_size'] = os.path.getsize(backup_results['backup_path'])

            backup_size_mb = backup_results['backup_size'] / (1024 * 1024)

            logger.info(f"Data backup completed: {backup_results['tables_backed_up']} tables, {backup_size_mb:.2f} MB")

            return {
                'status': 'success',
                'task_id': task_id,
                'backup_results': backup_results,
                'backup_size_mb': backup_size_mb,
                'completed_at': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Data backup failed: {str(e)}")
            raise ETLException(f"Data backup failed: {str(e)}")

# ==============================================
# PHASE 5: Transform Records
# ==============================================
# Purpose: Transform raw records through data cleansing, field mapping, and quality validation
# Input: raw_records WHERE is_processed=false
# Output: standardized_data (success) or rejected_records (failure)

async def transform_records(
    db: Session,
    execution_id: str,
    transform_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Transform raw records through the complete transform pipeline.

    Flow:
    1. Fetch unprocessed raw records from database
    2. For each record:
        a. Data Cleansing: remove whitespace, normalize case, handle nulls
        b. Field Mapping: apply direct/calculated/lookup/constant transformations
        c. Data Validation: check completeness, uniqueness, validity, range, consistency
        d. Result Handling:
           - SUCCESS: Insert to standardized_data, mark is_processed=true
           - FAILURE: Insert to rejected_records, increment records_failed counter

    Args:
        db: Database session
        execution_id: Job execution ID
        transform_config: Transformation configuration including:
            - entity_type: Type of entity being transformed
            - batch_size: Number of records to process at once
            - cleaner_config: Data cleaner configuration
            - field_mappings: List of field mappings
            - quality_rules: Quality validation rules

    Returns:
        Dictionary with transformation statistics
    """
    from app.infrastructure.db.models.raw_data.raw_records import RawRecords
    from app.infrastructure.db.models.staging.standardized_data import StandardizedData, StandardizedDataCreate
    from app.infrastructure.db.models.raw_data.rejected_records import RejectedRecord
    from app.infrastructure.db.models.etl_control.job_executions import JobExecution
    from app.infrastructure.db.models.transformation.field_mappings import FieldMapping
    from app.infrastructure.db.models.etl_control.quality_rules import QualityRule
    from app.infrastructure.db.models.etl_control.quality_check_results import (
        QualityCheckResult,
        QualityCheckResultCreate
    )
    from app.transformers.data_cleaner import DataCleaner
    from app.transformers.data_validator import DataValidator
    from app.transformers.data_normalizer import DataNormalizer
    from app.application.services.field_mapping_service import FieldMappingService
    from app.core.enums import QualityCheckResult as QualityCheckResultEnum

    logger.info(f"[PHASE 5] Starting transform for execution {execution_id}")

    records_processed = 0
    records_successful = 0
    records_failed = 0
    errors = []
    logs = []

    try:
        # Get execution record to retrieve job_id
        execution = db.exec(select(JobExecution).where(JobExecution.id == execution_id)).first()
        if not execution:
            raise ETLException(f"Job execution not found: {execution_id}")

        job_id = execution.job_id
        entity_type = transform_config.get("entity_type", "UNKNOWN")
        batch_size = transform_config.get("batch_size", 1000)

        logger.info(f"[PHASE 5] Job ID: {job_id}, Entity Type: {entity_type}")

        # Step 1: Query unprocessed raw records (validation_status = UNVALIDATED or VALID)
        logger.debug(f"[PHASE 5] Querying unprocessed raw records")
        from app.core.enums import ValidationStatus
        raw_records_query = select(RawRecords).where(
            RawRecords.validation_status.in_([ValidationStatus.UNVALIDATED, ValidationStatus.VALID])
        ).limit(batch_size)
        raw_records = db.exec(raw_records_query).all()

        if not raw_records:
            logger.info(f"[PHASE 5] No unprocessed records found")
            logs.append("No unprocessed records to transform")
            return {
                "records_processed": 0,
                "records_successful": 0,
                "records_failed": 0,
                "logs": logs,
                "performance_metrics": {}
            }

        logger.info(f"[PHASE 5] Found {len(raw_records)} unprocessed records")
        logs.append(f"Found {len(raw_records)} records to transform")

        # Initialize transformers
        cleaner_config = transform_config.get("cleaner_config", {})
        cleaner = DataCleaner(db, execution_id, **cleaner_config)

        validator_config = transform_config.get("validator_config", {})
        validator = DataValidator(db, execution_id, **validator_config)

        normalizer_config = transform_config.get("normalizer_config", {})
        normalizer = DataNormalizer(db, execution_id, **normalizer_config)

        field_mapping_service = FieldMappingService(db, job_id)

        # Fetch field mappings for this job
        field_mappings_query = select(FieldMapping).where(FieldMapping.job_id == job_id)
        field_mappings = db.exec(field_mappings_query).all()
        logger.debug(f"[PHASE 5] Loaded {len(field_mappings)} field mappings")

        # Fetch quality rules for this entity type
        quality_rules_query = select(QualityRule).where(
            QualityRule.is_active == True,
            QualityRule.entity_type == entity_type
        )
        quality_rules = db.exec(quality_rules_query).all()
        logger.debug(f"[PHASE 5] Loaded {len(quality_rules)} quality rules for {entity_type}")

        # Step 2: Process each raw record
        for raw_record in raw_records:
            records_processed += 1

            try:
                logger.debug(
                    f"[PHASE 5] Processing record {records_processed}/{len(raw_records)} "
                    f"(raw_record_id: {raw_record.id})"
                )

                # Step 2a: Data Cleansing
                logger.debug(f"[PHASE 5] Cleansing record {raw_record.id}")
                clean_result = await cleaner.transform_record(raw_record.raw_data)

                if not clean_result.is_success():
                    logger.warning(f"[PHASE 5] Data cleansing failed for record {raw_record.id}")
                    # Still continue with original data
                    cleaned_data = raw_record.raw_data
                else:
                    cleaned_data = clean_result.data

                # Step 2b: Field Mapping
                logger.debug(f"[PHASE 5] Applying field mappings to record {raw_record.id}")
                mapped_record, mapping_errors = await field_mapping_service.execute_mappings(
                    cleaned_data, field_mappings, execution_id
                )

                if mapping_errors:
                    logger.warning(
                        f"[PHASE 5] Field mapping errors for record {raw_record.id}: {mapping_errors}"
                    )
                    # If critical mapping errors, reject the record
                    if any(error for error in mapping_errors):
                        raise ETLException(f"Field mapping failed: {'; '.join(mapping_errors)}")

                # Step 2c: Data Validation
                logger.debug(f"[PHASE 5] Validating record {raw_record.id}")

                # Setup validator with quality rules
                validation_rules = {}
                for rule in quality_rules:
                    field = rule.field_name or "general"
                    if field not in validation_rules:
                        validation_rules[field] = []

                    validation_rules[field].append({
                        "type": rule.rule_type.value.lower(),
                        "severity": "error",  # Adjust based on rule configuration
                        "parameters": {"rule_id": str(rule.rule_id), "expression": rule.rule_expression},
                        "error_message": f"Quality rule '{rule.rule_name}' failed"
                    })

                # Validate the mapped record
                if validation_rules:
                    validator_with_rules = DataValidator(
                        db, execution_id,
                        validation_rules=validation_rules,
                        stop_on_first_error=False,
                        collect_all_errors=True
                    )
                    validation_result = await validator_with_rules.transform_record(mapped_record)
                else:
                    validation_result = await validator.transform_record(mapped_record)

                # Step 2d: Result Handling
                if validation_result.is_success() or validation_result.status.value == "warning":
                    # SUCCESS: Insert to standardized_data
                    logger.debug(f"[PHASE 5] Record {raw_record.id} validation PASSED")

                    standardized_record = StandardizedDataCreate(
                        source_file_id=raw_record.file_id,
                        source_record_id=raw_record.id,
                        entity_type=entity_type,
                        standardized_data=mapped_record,
                        quality_score=float(validation_result.metadata.get("quality_score", 0.95)),
                        transformation_rules_applied=[
                            f"{m.mapping_type}:{m.target_field}" for m in field_mappings[:5]  # Sample
                        ],
                        batch_id=execution_id,
                        validation_status='passed'
                    )

                    # Create StandardizedData model instance
                    standardized_data = StandardizedData.from_orm(standardized_record)
                    db.add(standardized_data)
                    db.flush()  # Flush to get the ID

                    # Insert quality check results for passed validation
                    for rule in quality_rules:
                        quality_check = QualityCheckResult(
                            execution_id=execution.id,
                            rule_id=rule.rule_id,
                            check_result=QualityCheckResultEnum.PASSED,
                            records_checked=1,
                            records_passed=1,
                            records_failed=0,
                            failure_details=None
                        )
                        db.add(quality_check)

                    # Mark raw record as processed
                    raw_record.validation_status = ValidationStatus.VALID
                    db.add(raw_record)

                    records_successful += 1
                    logger.debug(f"[PHASE 5] Record {raw_record.id} inserted to standardized_data")

                else:
                    # FAILURE: Insert to rejected_records
                    logger.warning(
                        f"[PHASE 5] Record {raw_record.id} validation FAILED: {validation_result.errors}"
                    )

                    rejected_record = RejectedRecord(
                        source_record_id=raw_record.id,
                        source_file_id=raw_record.file_id,
                        row_number=raw_record.row_number,
                        raw_data=raw_record.raw_data,
                        rejection_reason="; ".join(validation_result.errors),
                        validation_errors=[
                            {"error": error} for error in validation_result.errors
                        ],
                        batch_id=execution_id
                    )
                    db.add(rejected_record)

                    # Insert quality check results for failed validation
                    for rule in quality_rules:
                        quality_check = QualityCheckResult(
                            execution_id=execution.id,
                            rule_id=rule.rule_id,
                            check_result=QualityCheckResultEnum.FAILED,
                            records_checked=1,
                            records_passed=0,
                            records_failed=1,
                            failure_details={"errors": validation_result.errors}
                        )
                        db.add(quality_check)

                    records_failed += 1
                    logger.debug(f"[PHASE 5] Record {raw_record.id} inserted to rejected_records")

                db.commit()

            except Exception as e:
                db.rollback()
                error_msg = f"Error processing record {raw_record.id}: {str(e)}"
                logger.error(f"[PHASE 5] {error_msg}")
                errors.append(error_msg)
                records_failed += 1

                # Log the error for debugging
                continue

        # Step 3: Update job execution counters
        logger.info(
            f"[PHASE 5] Transform complete: {records_successful} successful, "
            f"{records_failed} failed out of {records_processed}"
        )

        execution.records_transformed = records_successful
        execution.records_failed = records_failed
        db.add(execution)
        db.commit()

        logs.append(f"Transformed {records_successful} records successfully")
        logs.append(f"Rejected {records_failed} records")

        return {
            "records_processed": records_processed,
            "records_successful": records_successful,
            "records_failed": records_failed,
            "logs": logs,
            "performance_metrics": {
                "success_rate": (records_successful / records_processed * 100) if records_processed > 0 else 0,
                "failure_rate": (records_failed / records_processed * 100) if records_processed > 0 else 0
            }
        }

    except Exception as e:
        logger.error(f"[PHASE 5] Transform failed: {str(e)}", exc_info=True)
        errors.append(f"Transform phase failed: {str(e)}")
        logs.append(f"Transform failed: {str(e)}")

        # Update execution status
        try:
            execution = db.exec(select(JobExecution).where(JobExecution.id == execution_id)).first()
            if execution:
                execution.records_failed += records_failed
                db.add(execution)
                db.commit()
        except Exception as commit_error:
            logger.error(f"Failed to update execution counters: {commit_error}")

        raise ETLException(f"Data transformation failed: {str(e)}") from e


# Helper functions for job execution

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

async def _execute_transform_job(db: Session, execution_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Execute transform job - Phase 5 of ETL pipeline"""
    logger.info(f"Executing transform job for execution {execution_id}")

    # Call the transform_records function
    transform_result = await transform_records(
        db=db,
        execution_id=execution_id,
        transform_config=config
    )

    return {
        'records_processed': transform_result.get('records_processed', 0),
        'records_successful': transform_result.get('records_successful', 0),
        'records_failed': transform_result.get('records_failed', 0),
        'logs': transform_result.get('logs', [])
    }

async def _execute_load_job(db: Session, execution_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Execute load job - Phase 6 of ETL pipeline (Entity Loading)"""
    logger.info(f"Executing load job for execution {execution_id}")

    # Phase 6: Load standardized records into processed entities
    load_result = await load_records(
        db=db,
        execution_id=execution_id,
        load_config=config
    )

    return {
        'records_processed': load_result.get('records_processed', 0),
        'records_successful': load_result.get('records_loaded', 0),
        'records_failed': 0,
        'logs': load_result.get('logs', []),
        'performance_metrics': load_result.get('performance_metrics', {})
    }

# ==============================================
# PHASE 6: Load Records
# ==============================================
# Purpose: Load standardized records into processed entities with entity matching,
# deduplication, conflict resolution, and complete lineage tracking.
# Input: standardized_data WHERE validation_status='passed'
# Output: entities, entity_relationships, data_lineage records with transaction rollback on failure

async def load_records(
    db: Session,
    execution_id: str,
    load_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Load standardized records into processed entities through entity matching,
    deduplication, and conflict resolution.

    Flow:
    1. Query standardized_data WHERE validation_status='passed'
    2. BEGIN TRANSACTION (explicit with db.begin())
    3. For each standardized record:
        a. EntityMatcher.match_entity():
           - Calculate entity_hash = MD5(key_fields)
           - SELECT entities WHERE data_hash=?
           - If exact match: confidence_score = 1.0, is_new=false, is_duplicate=false
           - Else: SELECT entities WHERE entity_type=?, fuzzy similarity
           - If similarity > threshold: is_new=false, is_duplicate=true
           - Else: is_new=true
        b. If NEW entity:
           - INSERT entities
           - INSERT data_lineage (standardized_record_id → entity_id)
           - UPDATE job_executions records_loaded += 1
        c. If DUPLICATE:
           - UPDATE entities duplicate_count += 1, master_entity_id = primary_entity_id
           - INSERT entity_relationships (type='duplicate_of')
        d. If UPDATE existing:
           - SELECT existing entity
           - MERGE DATA with CONFLICT RESOLUTION (newer value wins + score-based)
           - UPDATE entities
           - INSERT change_log
           - UPDATE job_executions records_loaded += 1
        e. All cases:
           - INSERT entity_relationships (general)
           - INSERT data_lineage (complete chain)
    4. COMMIT TRANSACTION
    5. On Failure:
       - ROLLBACK TRANSACTION
       - INSERT error_logs
       - UPDATE job_executions status='failed'

    Args:
        db: Database session
        execution_id: Job execution ID
        load_config: Load configuration including:
            - entity_type: Type of entity being loaded
            - key_fields: Fields to calculate entity_hash
            - batch_size: Number of records to process at once
            - similarity_threshold: Threshold for fuzzy matching (default 0.85)
            - conflict_resolution_strategy: 'newer_wins' | 'score_based' | 'manual_review'

    Returns:
        Dictionary with load statistics
    """
    from app.transformers.entity_matcher import EntityMatcher
    from app.application.services.entity_service import EntityService

    logger.info(f"[PHASE 6] Starting load for execution {execution_id}")

    records_processed = 0
    records_loaded = 0
    records_duplicated = 0
    records_merged = 0
    errors = []
    logs = []

    execution = None
    transaction = None

    try:
        # Get execution record
        execution = db.exec(select(JobExecution).where(JobExecution.id == execution_id)).first()
        if not execution:
            raise ETLException(f"Job execution not found: {execution_id}")

        job_id = execution.job_id
        entity_type = load_config.get("entity_type", "UNKNOWN")
        batch_size = load_config.get("batch_size", 1000)
        key_fields = load_config.get("key_fields", ["id", "name"])
        similarity_threshold = load_config.get("similarity_threshold", 0.85)
        conflict_resolution = load_config.get("conflict_resolution_strategy", "newer_wins")

        logger.info(f"[PHASE 6] Job ID: {job_id}, Entity Type: {entity_type}, Threshold: {similarity_threshold}")
        logs.append(f"Load phase initiated for entity_type: {entity_type}")

        # Step 1: Query validated standardized records (validation_status='passed')
        logger.debug(f"[PHASE 6] Querying validated standardized records")
        standardized_query = select(StandardizedData).where(
            StandardizedData.validation_status == 'passed'
        ).limit(batch_size)
        standardized_records = db.exec(standardized_query).all()

        if not standardized_records:
            logger.info(f"[PHASE 6] No validated records found to load")
            logs.append("No validated records to load")
            return {
                "records_processed": 0,
                "records_loaded": 0,
                "records_duplicated": 0,
                "records_merged": 0,
                "logs": logs,
                "performance_metrics": {}
            }

        logger.info(f"[PHASE 6] Found {len(standardized_records)} validated records to load")
        logs.append(f"Found {len(standardized_records)} records to load")

        # Initialize services
        entity_service = EntityService(db)
        entity_matcher = EntityMatcher(db, execution_id, **load_config)

        # Step 2: Process records with individual commits for atomic operations
        logger.debug(f"[PHASE 6] Starting record processing")

        # Step 3: Process each standardized record
        for std_record in standardized_records:
            records_processed += 1

            try:
                logger.debug(
                    f"[PHASE 6] Processing record {records_processed}/{len(standardized_records)} "
                    f"(std_record_id: {std_record.id})"
                )

                # Step 3a: Entity Matching
                logger.debug(f"[PHASE 6] Matching entity for record {std_record.id}")

                # Calculate entity_hash from key_fields
                hash_input = "_".join(
                    str(std_record.standardized_data.get(field, "")) for field in key_fields
                )
                entity_hash = hashlib.md5(hash_input.encode()).hexdigest()

                # Match entity
                match_result = await entity_matcher.match_entity(
                    std_record.standardized_data,
                    entity_type,
                    entity_hash,
                    similarity_threshold
                )

                # Unpack match result
                is_new = match_result.get("is_new", True)
                is_duplicate = match_result.get("is_duplicate", False)
                matched_entity = match_result.get("matched_entity")
                confidence_score = match_result.get("confidence_score", 1.0)
                match_score = match_result.get("match_score", 0.0)

                # Step 3b: NEW ENTITY
                if is_new:
                    logger.debug(f"[PHASE 6] Record {std_record.id} identified as NEW entity")

                    # INSERT entities
                    new_entity = Entity(
                        entity_type=entity_type,
                        entity_key=std_record.standardized_data.get(key_fields[0], f"entity_{std_record.id}"),
                        entity_data=std_record.standardized_data,
                        confidence_score=float(confidence_score),
                        source_files=[std_record.source_file_id] if std_record.source_file_id else [],
                        version=1,
                        is_active=True
                    )
                    db.add(new_entity)
                    db.flush()  # Flush to get the entity_id

                    # INSERT data_lineage (standardized → entity)
                    lineage = DataLineage(
                        source_entity_id=std_record.id,
                        source_entity_type="StandardizedData",
                        target_entity_id=new_entity.entity_id,
                        target_entity_type=entity_type,
                        transformation_rule_id=None,
                        job_execution_id=execution.id,
                        lineage_metadata={
                            "hash": entity_hash,
                            "confidence": confidence_score,
                            "match_type": "new"
                        }
                    )
                    db.add(lineage)

                    records_loaded += 1
                    logger.debug(f"[PHASE 6] NEW entity {new_entity.entity_id} created for record {std_record.id}")

                # Step 3c: DUPLICATE ENTITY
                elif is_duplicate and matched_entity:
                    logger.debug(f"[PHASE 6] Record {std_record.id} identified as DUPLICATE of entity {matched_entity.entity_id}")

                    # UPDATE entities: increment duplicate_count, set master_entity_id
                    matched_entity.duplicate_count = (matched_entity.duplicate_count or 0) + 1
                    matched_entity.master_entity_id = matched_entity.entity_id  # Self-reference for primary
                    db.add(matched_entity)

                    # INSERT entity_relationships (duplicate_of)
                    relationship = EntityRelationship(
                        entity_from=std_record.id,
                        entity_to=matched_entity.entity_id,
                        relationship_type="duplicate_of",
                        relationship_strength=float(match_score),
                        metadata={
                            "confidence": confidence_score,
                            "hash_match": entity_hash == getattr(matched_entity, 'entity_hash', None),
                            "fuzzy_score": match_score
                        }
                    )
                    db.add(relationship)

                    # INSERT data_lineage (duplicate link)
                    lineage = DataLineage(
                        source_entity_id=std_record.id,
                        source_entity_type="StandardizedData",
                        target_entity_id=matched_entity.entity_id,
                        target_entity_type=entity_type,
                        transformation_rule_id=None,
                        job_execution_id=execution.id,
                        lineage_metadata={
                            "hash": entity_hash,
                            "confidence": confidence_score,
                            "match_type": "duplicate",
                            "match_score": match_score
                        }
                    )
                    db.add(lineage)

                    records_duplicated += 1
                    logger.debug(f"[PHASE 6] Record {std_record.id} marked as duplicate of {matched_entity.entity_id}")

                # Step 3d: UPDATE EXISTING ENTITY
                elif matched_entity:
                    logger.debug(f"[PHASE 6] Record {std_record.id} identified for MERGE with entity {matched_entity.entity_id}")

                    # SELECT existing entity
                    existing_entity = matched_entity

                    # MERGE DATA with CONFLICT RESOLUTION
                    merged_data = await _merge_entity_data(
                        existing_data=existing_entity.entity_data or {},
                        new_data=std_record.standardized_data,
                        confidence_score=float(confidence_score),
                        strategy=conflict_resolution
                    )

                    # CREATE change_log entry
                    change_log = ChangeLog(
                        entity_id=existing_entity.entity_id,
                        change_type="UPDATE",
                        old_value=existing_entity.entity_data,
                        new_value=merged_data,
                        change_details={
                            "merge_strategy": conflict_resolution,
                            "new_confidence": confidence_score,
                            "old_confidence": existing_entity.confidence_score,
                            "match_score": match_score
                        }
                    )
                    db.add(change_log)

                    # UPDATE entities
                    existing_entity.entity_data = merged_data
                    existing_entity.confidence_score = float(max(
                        existing_entity.confidence_score or 0,
                        confidence_score
                    ))
                    existing_entity.version += 1
                    existing_entity.last_updated = datetime.utcnow()
                    db.add(existing_entity)

                    # INSERT data_lineage (merge)
                    lineage = DataLineage(
                        source_entity_id=std_record.id,
                        source_entity_type="StandardizedData",
                        target_entity_id=existing_entity.entity_id,
                        target_entity_type=entity_type,
                        transformation_rule_id=None,
                        job_execution_id=execution.id,
                        lineage_metadata={
                            "hash": entity_hash,
                            "confidence": confidence_score,
                            "match_type": "update",
                            "match_score": match_score
                        }
                    )
                    db.add(lineage)

                    records_merged += 1
                    records_loaded += 1
                    logger.debug(f"[PHASE 6] Record {std_record.id} merged into entity {existing_entity.entity_id}")

                db.commit()

            except Exception as e:
                db.rollback()
                error_msg = f"Error processing record {std_record.id}: {str(e)}"
                logger.error(f"[PHASE 6] {error_msg}", exc_info=True)
                errors.append(error_msg)
                continue

        # Step 4: Finalize load
        logger.info(
            f"[PHASE 6] Load complete: {records_loaded} loaded, "
            f"{records_duplicated} duplicated, {records_merged} merged out of {records_processed}"
        )

        # Step 5: Update job execution counters
        execution.records_loaded = records_loaded
        db.add(execution)
        db.commit()

        logs.append(f"Successfully loaded {records_loaded} records")
        logs.append(f"Identified {records_duplicated} duplicates")
        logs.append(f"Merged {records_merged} existing entities")

        return {
            "records_processed": records_processed,
            "records_loaded": records_loaded,
            "records_duplicated": records_duplicated,
            "records_merged": records_merged,
            "logs": logs,
            "performance_metrics": {
                "load_rate": (records_loaded / records_processed * 100) if records_processed > 0 else 0,
                "dedup_rate": (records_duplicated / records_processed * 100) if records_processed > 0 else 0,
                "merge_rate": (records_merged / records_processed * 100) if records_processed > 0 else 0
            }
        }

    except Exception as e:
        logger.error(f"[PHASE 6] Load failed: {str(e)}", exc_info=True)
        errors.append(f"Load phase failed: {str(e)}")
        logs.append(f"Load failed: {str(e)}")

        # ROLLBACK TRANSACTION
        if transaction:
            try:
                transaction.rollback()
                logger.info("[PHASE 6] Transaction rolled back due to error")
            except Exception as rollback_error:
                logger.error(f"[PHASE 6] Failed to rollback transaction: {rollback_error}")

        # UPDATE job_executions status='failed'
        try:
            if execution:
                execution.status = 'FAILED'
                execution.execution_log = f"Load phase failed: {str(e)}"
                db.add(execution)
                db.commit()
                logger.info(f"[PHASE 6] Updated execution {execution_id} status to FAILED")
        except Exception as update_error:
            logger.error(f"[PHASE 6] Failed to update execution status: {update_error}")

        # INSERT error_logs
        try:
            error_log = ErrorLog(
                job_execution_id=execution.id if execution else None,
                error_type=ErrorType.SYSTEM_ERROR,
                error_severity=ErrorSeverity.CRITICAL,
                error_message=str(e),
                error_details={
                    "phase": "LOAD",
                    "records_processed": records_processed,
                    "records_loaded": records_loaded,
                    "traceback": traceback.format_exc()
                },
                context={"execution_id": execution_id}
            )
            db.add(error_log)
            db.commit()
            logger.info("[PHASE 6] Error logged to database")
        except Exception as log_error:
            logger.error(f"[PHASE 6] Failed to log error: {log_error}")

        raise ETLException(f"Data load failed: {str(e)}") from e


async def _merge_entity_data(
    existing_data: Dict[str, Any],
    new_data: Dict[str, Any],
    confidence_score: float,
    strategy: str = "newer_wins"
) -> Dict[str, Any]:
    """
    Merge entity data with conflict resolution strategy.

    Strategies:
    - newer_wins: New values always win
    - score_based: Higher confidence score wins per field
    - conservative: Existing values always win
    - merge: Intelligently merge arrays/objects, prefer new scalars

    Args:
        existing_data: Current entity data
        new_data: New standardized data
        confidence_score: Confidence score of new data (0.0-1.0)
        strategy: Conflict resolution strategy

    Returns:
        Merged data dictionary
    """
    merged = existing_data.copy()

    if strategy == "newer_wins":
        # New values always win
        merged.update(new_data)

    elif strategy == "score_based":
        # Higher confidence score wins (assume existing has implicit score < new)
        # For simplicity: if new_confidence >= 0.9, use new value
        if confidence_score >= 0.9:
            merged.update(new_data)
        else:
            # Selective merge: only update fields that are significantly different
            for key, new_value in new_data.items():
                if key not in merged:
                    # New field, always add
                    merged[key] = new_value
                elif isinstance(new_value, (int, float)) and isinstance(merged.get(key), (int, float)):
                    # For numeric fields, take the higher value
                    merged[key] = max(merged[key], new_value)
                elif isinstance(new_value, str) and isinstance(merged.get(key), str):
                    # For strings, only update if new is significantly different (fuzzy match > threshold)
                    # For now, keep existing if confidence not high
                    pass
                elif isinstance(new_value, list):
                    # For lists, merge uniquely
                    merged[key] = list(set(merged.get(key, []) + new_value))

    elif strategy == "conservative":
        # Existing values always win (no update)
        pass

    elif strategy == "merge":
        # Intelligent merge
        for key, new_value in new_data.items():
            if key not in merged:
                merged[key] = new_value
            elif isinstance(new_value, dict) and isinstance(merged[key], dict):
                # Recursively merge dicts
                merged[key] = {**merged[key], **new_value}
            elif isinstance(new_value, list):
                # Merge lists (unique elements)
                merged[key] = list(set(merged.get(key, []) + new_value))
            else:
                # For scalars, prefer existing
                pass

    return merged


async def _execute_full_etl_job(db: Session, execution_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Execute full ETL job (Extract + Transform + Load)"""
    logger.info(f"Executing full ETL job for execution {execution_id}")
    
    total_results = {
        'extract': {},
        'transform': {},
        'load': {},
        'total_records_processed': 0,
        'total_records_successful': 0,
        'total_records_failed': 0,
        'logs': []
    }
    
    try:
        # Step 1: Extract
        extract_config = config.get('extract', {})
        if extract_config:
            logger.info("Starting extract phase")
            extract_result = await _execute_extract_job(db, execution_id, extract_config)
            total_results['extract'] = extract_result
            total_results['logs'].extend(extract_result.get('logs', []))
        
        # Step 2: Transform
        transform_config = config.get('transform', {})
        if transform_config:
            logger.info("Starting transform phase")
            transform_result = await _execute_transform_job(db, execution_id, transform_config)
            total_results['transform'] = transform_result
            total_results['logs'].extend(transform_result.get('logs', []))
        
        # Step 3: Load
        load_config = config.get('load', {})
        if load_config:
            logger.info("Starting load phase")
            load_result = await _execute_load_job(db, execution_id, load_config)
            total_results['load'] = load_result
            total_results['logs'].extend(load_result.get('logs', []))
        
        # Calculate totals
        for phase in ['extract', 'transform', 'load']:
            phase_result = total_results.get(phase, {})
            total_results['total_records_processed'] += phase_result.get('records_processed', 0)
            total_results['total_records_successful'] += phase_result.get('records_successful', 0)
            total_results['total_records_failed'] += phase_result.get('records_failed', 0)
        
        total_results['logs'].append("Full ETL job completed successfully")
        
        return total_results
        
    except Exception as e:
        total_results['logs'].append(f"Full ETL job failed: {str(e)}")
        raise ETLException(f"Full ETL job failed during execution: {str(e)}")

# Utility tasks for ETL operations

@celery_app.task(
    bind=True,
    name='etl.validate_file_format',
    time_limit=300
)
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

@celery_app.task(
    bind=True,
    name='etl.preview_file_data',
    time_limit=600
)
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

@celery_app.task(
    bind=True,
    name='etl.schedule_job_execution',
    time_limit=60
)
def schedule_job_execution_task(self, job_id: str, scheduled_time: str = None):
    """
    Schedule ETL job for execution
    
    Args:
        job_id: ID of the job to schedule
        scheduled_time: When to execute (ISO format, defaults to now)
        
    Returns:
        Scheduling results
    """
    task_id = self.request.id
    logger.info(f"Scheduling job execution task {task_id} for job {job_id}")
    
    try:
        if scheduled_time:
            # Parse scheduled time
            schedule_dt = datetime.fromisoformat(scheduled_time.replace('Z', '+00:00'))
            eta = schedule_dt
        else:
            # Execute immediately
            eta = None
        
        # Schedule the ETL job execution
        scheduled_task = execute_etl_job.apply_async(
            args=[job_id],
            eta=eta
        )
        
        return {
            'status': 'success',
            'job_id': job_id,
            'scheduled_task_id': scheduled_task.id,
            'scheduled_for': scheduled_time or 'immediate',
            'scheduler_task_id': task_id,
            'scheduled_at': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Job scheduling failed for {job_id}: {str(e)}")
        raise ETLException(f"Job scheduling failed: {str(e)}")

# Batch processing tasks

@celery_app.task(
    bind=True,
    name='etl.batch_process_files',
    time_limit=7200  # 2 hours
)
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

@celery_app.task(
    bind=True,
    name='etl.chain_job_execution',
    time_limit=14400  # 4 hours
)
def chain_job_execution_task(self, job_ids: List[str], execution_config: Dict[str, Any] = None):
    """
    Execute multiple ETL jobs in sequence (chain)
    
    Args:
        job_ids: List of job IDs to execute in order
        execution_config: Execution configuration
        
    Returns:
        Chain execution results
    """
    task_id = self.request.id
    logger.info(f"Starting job chain execution task {task_id} for {len(job_ids)} jobs")
    
    chain_results = {
        'total_jobs': len(job_ids),
        'successful_jobs': 0,
        'failed_jobs': 0,
        'job_results': [],
        'chain_stopped': False
    }
    
    try:
        # Execute jobs sequentially
        for i, job_id in enumerate(job_ids):
            logger.info(f"Executing job {i+1}/{len(job_ids)}: {job_id}")
            
            try:
                # Execute job (fire-and-forget to avoid blocking the worker)
                job_result = execute_etl_job.apply_async(args=[job_id, execution_config])
                chain_results['job_results'].append({
                    'job_id': str(job_id),
                    'task_id': job_result.id,
                })
                # NOTE: do not call .get() here — it blocks the Celery worker
                # and causes a deadlock. Chain continues asynchronously.
                chain_results['successful_jobs'] += 1
            
            except Exception as e:
                chain_results['failed_jobs'] += 1
                chain_results['job_results'].append({
                    'status': 'failed',
                    'job_id': job_id,
                    'error': str(e)
                })
                
                if execution_config and execution_config.get('stop_on_failure', True):
                    chain_results['chain_stopped'] = True
                    logger.warning(f"Chain execution stopped due to job error: {job_id}")
                    break
        
        logger.info(f"Job chain execution completed: {chain_results['successful_jobs']}/{chain_results['total_jobs']} successful")
        
        return {
            'status': 'success',
            'task_id': task_id,
            'chain_results': chain_results,
            'completed_at': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Job chain execution failed: {str(e)}")
        raise ETLException(f"Job chain execution failed: {str(e)}")
    
# ==============================================
# PHASE 7: Post-Processing
# ==============================================
# Purpose: Calculate metrics, generate quality reports, trigger dependent jobs,
# publish events, send notifications, and update cache
# Input: Completed job_executions from phases 4-6
# Output: performance_metrics, quality reports, triggered child jobs, notifications

async def post_process_job(
    db: Session,
    execution_id: str,
    job_id: str,
    job_name: str
) -> Dict[str, Any]:
    """
    Execute post-processing phase after job completion.

    Flow:
    1. Calculate performance metrics (duration, records/sec, memory)
    2. Insert PerformanceMetrics record
    3. Generate quality report and check thresholds
    4. Publish DataQualityAlert if needed
    5. Discover and trigger dependent jobs
    6. Publish JobCompletedEvent
    7. Send notifications (email/slack)
    8. Update cache (delete job:{job_id}, set execution:{execution_id}:summary)
    9. Mark job_executions as completed

    Args:
        db: Database session
        execution_id: Job execution ID
        job_id: Job ID
        job_name: Job name for notifications

    Returns:
        Dictionary with post-processing results
    """
    from app.infrastructure.db.models.etl_control.job_executions import JobExecution
    from app.infrastructure.db.models.etl_control.performance_metrics import PerformanceMetric
    from app.infrastructure.db.models.etl_control.quality_check_results import QualityCheckResult
    from app.application.services.data_quality_service import DataQualityService
    from app.application.services.notification_service import NotificationService
    from app.application.services.job_orchestration_service import JobOrchestrationService
    from app.utils.event_publisher import get_event_publisher
    import psutil
    import os

    logger.info(f"[PHASE 7] Starting post-processing for execution {execution_id}")

    logs = []
    warnings = []
    performance_data = {}

    try:
        # Step 1: Get execution record
        execution = db.exec(
            select(JobExecution).where(JobExecution.id == execution_id)
        ).first()

        if not execution:
            raise ETLException(f"Job execution not found: {execution_id}")

        logger.info(f"[PHASE 7] Execution found: status={execution.status}")

        # Step 2: Calculate performance metrics
        logger.debug(f"[PHASE 7] Calculating performance metrics")

        duration_seconds = 0
        records_per_second = 0.0
        memory_usage_mb = 0.0

        if execution.started_at and execution.completed_at:
            duration = execution.completed_at - execution.started_at
            duration_seconds = int(duration.total_seconds())

            if duration_seconds > 0:
                total_records = execution.records_loaded or 0
                records_per_second = total_records / duration_seconds
        else:
            # If timestamps not set, use current time
            now = datetime.utcnow()
            if execution.started_at:
                duration = now - execution.started_at
                duration_seconds = int(duration.total_seconds())
            execution.completed_at = now

        # Get memory usage
        try:
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            memory_usage_mb = memory_info.rss / (1024 * 1024)  # Convert bytes to MB
            logger.debug(f"[PHASE 7] Memory usage: {memory_usage_mb:.2f} MB")
        except Exception as mem_error:
            logger.warning(f"[PHASE 7] Could not get memory usage: {str(mem_error)}")
            memory_usage_mb = 0.0

        performance_data = {
            "duration_seconds": duration_seconds,
            "records_per_second": round(records_per_second, 2),
            "memory_usage_mb": round(memory_usage_mb, 2),
            "records_loaded": execution.records_loaded or 0,
            "records_transformed": execution.records_transformed or 0,
            "records_extracted": execution.records_extracted or 0,
            "records_failed": execution.records_failed or 0
        }

        logger.info(
            f"[PHASE 7] Performance metrics: "
            f"duration={duration_seconds}s, "
            f"throughput={records_per_second:.2f} rec/s, "
            f"memory={memory_usage_mb:.2f}MB"
        )

        logs.append(f"Performance metrics calculated: {performance_data}")

        # Step 3: Insert PerformanceMetrics record
        logger.debug(f"[PHASE 7] Inserting performance metrics record")

        try:
            performance_metric = PerformanceMetric(
                execution_id=execution.id,
                records_per_second=records_per_second,
                memory_usage_mb=memory_usage_mb,
                cpu_usage_percent=0.0,  # Future: get from psutil
                disk_io_mb=0.0,  # Future: track disk I/O
                network_io_mb=0.0,  # Future: track network I/O
                duration_seconds=duration_seconds,
                peak_memory_mb=memory_usage_mb,  # Simplified: use current as peak
                avg_cpu_percent=0.0,  # Future: average over duration
                cache_hit_rate=0.0,  # Future: track cache hits
                error_rate=0.0 if (execution.records_loaded or 0) == 0 else
                (execution.records_failed or 0) / (execution.records_loaded + execution.records_failed or 1) * 100
            )

            db.add(performance_metric)
            db.commit()

            logger.debug(
                f"[PHASE 7] Performance metric inserted: "
                f"duration={duration_seconds}s, throughput={records_per_second:.2f} rec/s"
            )
            logs.append("Performance metrics inserted to database")

        except Exception as metric_error:
            logger.warning(f"[PHASE 7] Failed to insert performance metrics: {str(metric_error)}")
            warnings.append(f"Performance metrics insertion failed: {str(metric_error)}")

        # Step 4: Generate quality report
        logger.debug(f"[PHASE 7] Generating quality report")

        quality_report = None
        quality_data = {
            "pass_rate": 0.0,
            "fail_rate": 0.0,
            "total_checks": 0,
            "passed_checks": 0,
            "failed_checks": 0
        }

        try:
            quality_service = DataQualityService(db)

            # Query quality check results for this execution
            quality_checks = db.exec(
                select(QualityCheckResult).where(
                    QualityCheckResult.execution_id == execution.id
                )
            ).all()

            if quality_checks:
                total_checks = len(quality_checks)
                passed_checks = sum(1 for qc in quality_checks if qc.check_result == "PASSED")
                failed_checks = total_checks - passed_checks

                quality_data = {
                    "pass_rate": (passed_checks / total_checks * 100) if total_checks > 0 else 100.0,
                    "fail_rate": (failed_checks / total_checks * 100) if total_checks > 0 else 0.0,
                    "total_checks": total_checks,
                    "passed_checks": passed_checks,
                    "failed_checks": failed_checks
                }

                logger.info(
                    f"[PHASE 7] Quality report: "
                    f"pass_rate={quality_data['pass_rate']:.2f}%, "
                    f"total_checks={total_checks}"
                )

                logs.append(f"Quality report generated: {quality_data}")

                # Step 5: Check quality threshold and publish alert if needed
                quality_threshold = 80.0  # Configurable
                if quality_data["pass_rate"] < quality_threshold:
                    logger.warning(
                        f"[PHASE 7] Quality threshold breached: "
                        f"{quality_data['pass_rate']:.2f}% < {quality_threshold}%"
                    )

                    # Publish DataQualityAlert event
                    try:
                        publisher = await get_event_publisher()
                        await publisher.publish(
                            event_type="DataQualityAlert",
                            event_data={
                                "execution_id": str(execution_id),
                                "job_id": str(job_id),
                                "job_name": job_name,
                                "pass_rate": quality_data["pass_rate"],
                                "threshold": quality_threshold,
                                "alert_level": "WARNING",
                                "timestamp": datetime.utcnow().isoformat()
                            }
                        )
                        logger.info(f"[PHASE 7] DataQualityAlert published")
                        logs.append("DataQualityAlert published")
                    except Exception as event_error:
                        logger.warning(f"[PHASE 7] Failed to publish quality alert: {str(event_error)}")
                        warnings.append(f"Quality alert publication failed: {str(event_error)}")

        except Exception as quality_error:
            logger.warning(f"[PHASE 7] Failed to generate quality report: {str(quality_error)}")
            warnings.append(f"Quality report generation failed: {str(quality_error)}")

        # Step 6: Trigger dependent jobs
        logger.debug(f"[PHASE 7] Triggering dependent jobs")

        dependent_jobs_result = {
            "total_triggered": 0,
            "triggered_jobs": [],
            "skipped_jobs": [],
            "errors": []
        }

        try:
            orchestration_service = JobOrchestrationService(db)

            dependent_jobs_result = await orchestration_service.trigger_dependent_jobs(
                parent_job_id=job_id,
                parent_execution_id=execution.id,
                parent_status="SUCCESS" if execution.status == "SUCCESS" else "FAILURE"
            )

            logger.info(
                f"[PHASE 7] Dependent jobs trigger result: "
                f"{dependent_jobs_result['total_triggered']} triggered, "
                f"{len(dependent_jobs_result['skipped_jobs'])} skipped"
            )

            if dependent_jobs_result["total_triggered"] > 0:
                logs.append(
                    f"Triggered {dependent_jobs_result['total_triggered']} dependent jobs"
                )

            if dependent_jobs_result["errors"]:
                logger.warning(
                    f"[PHASE 7] Errors during dependent job trigger: "
                    f"{dependent_jobs_result['errors']}"
                )
                warnings.extend(dependent_jobs_result["errors"])

        except Exception as orchestration_error:
            logger.warning(
                f"[PHASE 7] Failed to trigger dependent jobs: {str(orchestration_error)}"
            )
            warnings.append(f"Dependent job orchestration failed: {str(orchestration_error)}")

        # Step 7: Publish JobCompletedEvent
        logger.debug(f"[PHASE 7] Publishing JobCompletedEvent")

        try:
            publisher = await get_event_publisher()

            event_data = {
                "job_id": str(job_id),
                "execution_id": str(execution_id),
                "job_name": job_name,
                "status": execution.status or "SUCCESS",
                "duration_seconds": duration_seconds,
                "records_loaded": execution.records_loaded or 0,
                "records_transformed": execution.records_transformed or 0,
                "records_extracted": execution.records_extracted or 0,
                "records_failed": execution.records_failed or 0,
                "quality_pass_rate": quality_data.get("pass_rate", 0.0),
                "dependent_jobs_triggered": dependent_jobs_result["total_triggered"],
                "timestamp": datetime.utcnow().isoformat()
            }

            await publisher.publish(
                event_type="JobCompleted",
                event_data=event_data
            )

            logger.info(f"[PHASE 7] JobCompletedEvent published")
            logs.append("JobCompletedEvent published")

        except Exception as event_error:
            logger.warning(f"[PHASE 7] Failed to publish JobCompletedEvent: {str(event_error)}")
            warnings.append(f"JobCompletedEvent publication failed: {str(event_error)}")

        # Step 8: Send notifications
        logger.debug(f"[PHASE 7] Sending notifications")

        try:
            notification_service = NotificationService(db)

            notification_result = await notification_service.send_job_completion_notification(
                execution_id=execution.id,
                recipients=["admin@example.com"]  # TODO: Get from config or user preferences
            )

            logger.info(f"[PHASE 7] Notification result: {notification_result}")
            logs.append(f"Notifications sent: {notification_result.get('successful_sends', 0)} sent")

        except Exception as notification_error:
            logger.warning(f"[PHASE 7] Failed to send notifications: {str(notification_error)}")
            warnings.append(f"Notification sending failed: {str(notification_error)}")

        # Step 9: Update cache
        logger.debug(f"[PHASE 7] Updating cache")

        try:
            # Get shared cache manager
            cache = await cache_manager.get_cache()
            if not cache:
                logger.warning("[PHASE 7] No cache available, skipping cache update")
            else:
                # Delete job cache
                cache_key_job = f"job:{job_id}"
                await cache.delete(cache_key_job)
                logger.debug(f"[PHASE 7] Cache deleted: {cache_key_job}")

                # Set execution summary cache with 1-hour TTL
                cache_key_exec = f"execution:{execution_id}:summary"
                execution_summary = {
                    "job_id": str(job_id),
                    "job_name": job_name,
                    "status": execution.status,
                    "duration_seconds": duration_seconds,
                    "performance": performance_data,
                    "quality": quality_data,
                    "completed_at": execution.completed_at.isoformat() if execution.completed_at else None,
                    "dependent_jobs_triggered": dependent_jobs_result["total_triggered"]
                }

                await cache.set(
                    cache_key_exec,
                    execution_summary,
                    ttl=3600  # 1 hour
                )

                logger.debug(f"[PHASE 7] Cache set: {cache_key_exec} (TTL=3600s)")
                logs.append(f"Cache updated: execution summary cached for 1 hour")

        except Exception as cache_error:
            logger.warning(f"[PHASE 7] Failed to update cache: {str(cache_error)}")
            warnings.append(f"Cache update failed: {str(cache_error)}")

        # Step 10: Mark execution as completed
        logger.debug(f"[PHASE 7] Marking execution as completed")

        try:
            execution.status = "SUCCESS"
            execution.completed_at = datetime.utcnow()
            execution.execution_log = json.dumps({
                "performance": performance_data,
                "quality": quality_data,
                "dependent_jobs": dependent_jobs_result,
                "logs": logs,
                "warnings": warnings
            })

            db.add(execution)
            db.commit()

            logger.info(
                f"[PHASE 7] Execution marked as completed: {execution_id}"
            )
            logs.append("Execution marked as completed")

        except Exception as completion_error:
            logger.error(f"[PHASE 7] Failed to mark execution as completed: {str(completion_error)}")
            raise ETLException(f"Failed to complete execution: {str(completion_error)}")

        # Return results
        result = {
            "status": "success",
            "execution_id": str(execution_id),
            "job_id": str(job_id),
            "job_name": job_name,
            "performance_metrics": performance_data,
            "quality_report": quality_data,
            "dependent_jobs_triggered": dependent_jobs_result["total_triggered"],
            "logs": logs,
            "warnings": warnings,
            "completed_at": datetime.utcnow().isoformat()
        }

        logger.info(
            f"[PHASE 7] Post-processing complete: "
            f"status={result['status']}, "
            f"dependent_jobs={result['dependent_jobs_triggered']}, "
            f"quality_pass_rate={quality_data['pass_rate']:.2f}%"
        )

        return result

    except Exception as e:
        logger.error(f"[PHASE 7] Post-processing failed: {str(e)}", exc_info=True)

        # Try to mark execution as failed
        try:
            execution = db.exec(
                select(JobExecution).where(JobExecution.id == execution_id)
            ).first()

            if execution:
                execution.status = "FAILED"
                execution.execution_log = f"Post-processing failed: {str(e)}"
                db.add(execution)
                db.commit()
        except Exception as fail_error:
            logger.error(f"[PHASE 7] Failed to mark execution as failed: {str(fail_error)}")

        raise ETLException(f"Post-processing failed: {str(e)}") from e

@celery_app.task(bind=True, name='simple_test')
def simple_test(self, message="hello"):
    print(f"Simple test executed: {message}")
    return f"Success: {message}"