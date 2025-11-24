# ==============================================
# app/tasks/etl_tasks.py
# ==============================================
import asyncio
import os
import json
import shutil
import traceback
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path
from sqlmodel import Session, select
import pandas as pd

from app.core.enums import ProcessingStatus

from .celery_app import celery_app
from app.interfaces.dependencies import get_db
from app.infrastructure.db.models.raw_data.file_registry import FileRegistry
from app.infrastructure.db.models.etl_control.etl_jobs import EtlJob
from app.infrastructure.db.models.etl_control.job_executions import JobExecution
from app.infrastructure.db.models.audit.data_lineage import DataLineage
from app.infrastructure.db.connection import get_session
from app.processors import get_processor
from app.transformers import create_transformation_pipeline
from app.services.etl_service import ETLService
from app.services.file_service import FileService
from app.services.data_quality_service import DataQualityService
from app.utils.logger import get_logger
from app.core.exceptions import ETLException, FileProcessingException
from app.utils.event_publisher import get_event_publisher

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
    Process a single file through the ETL pipeline
    
    Args:
        file_id: ID of the file to process
        processing_config: Optional processing configuration
        
    Returns:
        Processing results and statistics
    """
    task_id = self.request.id
    logger.info(f"Starting file processing task {task_id} for file {file_id}")
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
                # Re-fetch file_record if it wasn't set or was from a closed session
                if file_record is None or not db.is_active:
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
            from app.tasks.task_helpers import log_task_error, get_error_type_from_exception, get_error_severity_from_exception
            from app.infrastructure.db.models.etl_control.error_logs import ErrorType
            
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
    name='etl.transformation_pipeline',
    max_retries=2,
    default_retry_delay=600,  # 10 minutes
    time_limit=3600,  # 1 hour
    soft_time_limit=3300  # 55 minutes
)
async def run_transformation_pipeline(self, job_execution_id: str, transformation_config: Dict[str, Any]):
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
    
    db = next(get_db())
    try:
        # Get job execution record
        execution = db.exec(select(JobExecution).where(JobExecution.execution_id == job_execution_id)).first()
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
            from app.infrastructure.db.models.raw_data.raw_records import RawRecords
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
                stage_results = await transformer.transform_dataset(
                    input_records, 
                    output_entity_type=transformation_config.get('output_entity_type')
                )
            else:
                # Subsequent stages use results from previous stage
                previous_results = total_results[stages[stage_idx - 1]]['results']
                stage_data = [result.data for result in previous_results if result.is_success()]
                stage_results = await transformer.transform_dataset(
                    stage_data,
                    output_entity_type=transformation_config.get('output_entity_type')
                )
            
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
    
    finally:
        db.close()

@celery_app.task(
    bind=True,
    name='etl.execute_job',
    max_retries=2,
    default_retry_delay=900,  # 15 minutes
    time_limit=7200,  # 2 hours
    soft_time_limit=6900  # 1 hour 55 minutes
)
async def execute_etl_job(self, job_id: str, execution_parameters: Dict[str, Any] = None):
    """
    Execute complete ETL job
    
    Args:
        job_id: ID of the ETL job to execute
        execution_parameters: Optional execution parameters
        
    Returns:
        Job execution results
    """
    task_id = self.request.id
    logger.info(f"Starting ETL job execution task {task_id} for job {job_id}")
    
    db = next(get_db())
    try:
        # Get ETL job
        job = db.exec(select(EtlJob).where(EtlJob.job_id == job_id)).first()
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
        
        execution_id = str(execution.execution_id)
        
        # Get job configuration
        job_config = job.job_config or {}
        if execution_parameters:
            job_config.update(execution_parameters)
        
        # Execute ETL steps based on job type
        job_type = job.job_type.lower()
        
        if job_type == 'extract':
            result = await _execute_extract_job(db, execution_id, job_config)
        elif job_type == 'transform':
            result = await _execute_transform_job(db, execution_id, job_config)
        elif job_type == 'load':
            result = await _execute_load_job(db, execution_id, job_config)
        elif job_type == 'full_etl':
            result = await _execute_full_etl_job(db, execution_id, job_config)
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
        # The following lines are redundant if result.get(...) is used above,
        # and `total_processed` etc. are not defined.
        # Assuming the intent was to use the values from `result`
        # and the user provided a partial snippet for the dependent job logic.
        # Keeping the original `result.get` assignments and adding the new logic.
        # If `total_processed` etc. were meant to be new variables, they need definition.
        # For now, I will assume the user wants to keep the existing `result.get` assignments
        # and add the dependent job triggering.
        
        # Update performance metrics
        performance_metrics = execution.performance_metrics or {}
        performance_metrics.update(result.get('performance_metrics', {}))
        execution.performance_metrics = performance_metrics
        
        db.add(execution)
        db.commit()
        
        logger.info(f"ETL job {job_id} completed successfully")
        
        # Publish job completed event
        try:
            publisher = asyncio.run(get_event_publisher())
            asyncio.run(publisher.publish_job_completed(
                job_id=job.job_id,
                execution_id=execution.execution_id,
                job_name=job.job_name,
                stats={
                    'records_processed': execution.records_processed,
                    'records_successful': execution.records_successful,
                    'records_failed': execution.records_failed,
                    'duration_seconds': (execution.end_time - execution.start_time).total_seconds()
                }
            ))
        except Exception as pub_error:
            logger.warning(f"Failed to publish job completed event: {str(pub_error)}")
        
        # Trigger dependent jobs if any
        triggered_result = {'total_triggered': 0} # Initialize to avoid NameError if trigger fails
        try:
            from app.tasks.task_helpers import trigger_dependent_jobs
            import asyncio
            import traceback
            
            logger.info(f"Checking for dependent jobs of {job_id}")
            triggered_result = asyncio.run(trigger_dependent_jobs(
                db=db,
                parent_job_id=job_id,
                parent_execution_status='SUCCESS'
            ))
            
            if triggered_result.get('total_triggered', 0) > 0:
                logger.info(
                    f"Triggered {triggered_result['total_triggered']} dependent jobs: "
                    f"{[j['child_job_id'] for j in triggered_result.get('triggered_jobs', [])]}"
                )
            else:
                logger.info(f"No dependent jobs to trigger for job {job_id}")
                
        except Exception as trigger_error:
            # Don't fail the main job if dependent job triggering fails
            logger.error(f"Failed to trigger dependent jobs: {trigger_error}")
            logger.error(traceback.format_exc())
        
        return {
            'status': 'success',
            'job_id': job_id,
            'execution_id': execution_id,
            'task_id': task_id,
            'result': result, # Keeping original 'result' key
            'completed_at': datetime.utcnow().isoformat(),
            'dependent_jobs_triggered': triggered_result.get('total_triggered', 0) if 'triggered_result' in locals() else 0
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
                        execution_id=execution.execution_id,
                        job_name=job.job_name if 'job' in locals() else "Unknown",
                        error=str(e)
                    ))
                except Exception as pub_error:
                    logger.warning(f"Failed to publish job failed event: {str(pub_error)}")
                    
        except Exception as commit_error:
            logger.error(f"Failed to update execution status: {commit_error}")
        
        # Log error to database
        try:
            from app.tasks.task_helpers import log_task_error, get_error_type_from_exception, get_error_severity_from_exception
            from app.infrastructure.db.models.etl_control.error_logs import ErrorType
            
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
    
    finally:
        db.close()

@celery_app.task(
    bind=True,
    name='etl.validate_data_quality',
    max_retries=1,
    default_retry_delay=300,
    time_limit=1800
)
async def validate_data_quality(self, entity_type: str = None, validation_config: Dict[str, Any] = None):
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
    
    db = next(get_db())
    try:
        quality_service = DataQualityService(db)
        
        # Run quality checks
        if entity_type:
            results = await quality_service.run_quality_check(entity_type=entity_type, check_config=validation_config)
        else:
            # Run all active quality rules
            results = await quality_service.run_all_quality_checks()
        
        # Generate quality report
        report = await quality_service.generate_quality_report(entity_type)
        
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
    
    finally:
        db.close()

@celery_app.task(
    bind=True,
    name='etl.generate_lineage',
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
    
    db = next(get_db())
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
    
    finally:
        db.close()

@celery_app.task(
    bind=True,
    name='etl.cleanup_files',
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
    
    db = next(get_db())
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
    
    finally:
        db.close()

@celery_app.task(
    bind=True,
    name='etl.backup_data',
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
    
    db = next(get_db())
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
    
    finally:
        db.close()

# Helper functions for job execution

async def _execute_extract_job(db: Session, execution_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Execute extract job"""
    logger.info(f"Executing extract job for execution {execution_id}")
    
    # Get source configuration
    source_type = config.get('source_type', 'file')
    
    if source_type == 'file':
        # File-based extraction
        file_paths = config.get('file_paths', [])
        results = []
        
        for file_path in file_paths:
            # Process each file
            file_result = await process_file_task.apply_async(
                args=[file_path, config]
            ).get()
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
        # API extraction logic would go here
        
        return {
            'records_processed': 0,
            'records_successful': 0,
            'logs': ['API extraction completed']
        }
    
    else:
        raise ETLException(f"Unknown source type: {source_type}")

async def _execute_transform_job(db: Session, execution_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Execute transform job"""
    logger.info(f"Executing transform job for execution {execution_id}")
    
    # Run transformation pipeline
    transform_result = await run_transformation_pipeline.apply_async(
        args=[execution_id, config]
    ).get()
    
    return {
        'records_processed': transform_result.get('performance_metrics', {}).get('total_records', 0),
        'records_successful': transform_result.get('performance_metrics', {}).get('successful_records', 0),
        'records_failed': transform_result.get('performance_metrics', {}).get('failed_records', 0),
        'logs': [f"Transformation completed: {transform_result.get('status')}"]
    }

async def _execute_load_job(db: Session, execution_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Execute load job"""
    logger.info(f"Executing load job for execution {execution_id}")
    
    # Load processed data to target system
    target_type = config.get('target_type', 'database')
    
    if target_type == 'database':
        # Database loading logic
        target_table = config.get('target_table')
        source_query = config.get('source_query')
        
        if not target_table or not source_query:
            raise ETLException("target_table and source_query required for database loading")
        
        # Execute data loading
        source_df = pd.read_sql(source_query, db.connection())
        records_processed = len(source_df)
        
        # Load to target table
        source_df.to_sql(target_table, db.connection(), if_exists='append', index=False)
        
        return {
            'records_processed': records_processed,
            'records_successful': records_processed,
            'records_failed': 0,
            'logs': [f"Loaded {records_processed} records to {target_table}"]
        }
    
    elif target_type == 'file':
        # File export logic
        export_format = config.get('export_format', 'csv')
        export_path = config.get('export_path')
        source_query = config.get('source_query')
        
        if not export_path or not source_query:
            raise ETLException("export_path and source_query required for file loading")
        
        # Export data
        source_df = pd.read_sql(source_query, db.connection())
        records_processed = len(source_df)
        
        if export_format == 'csv':
            source_df.to_csv(export_path, index=False)
        elif export_format == 'json':
            source_df.to_json(export_path, orient='records')
        elif export_format == 'excel':
            source_df.to_excel(export_path, index=False)
        else:
            raise ETLException(f"Unsupported export format: {export_format}")
        
        return {
            'records_processed': records_processed,
            'records_successful': records_processed,
            'records_failed': 0,
            'logs': [f"Exported {records_processed} records to {export_path}"]
        }
    
    else:
        raise ETLException(f"Unknown target type: {target_type}")

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
async def validate_file_format_task(self, file_id: str):
    """
    Validate file format before processing
    
    Args:
        file_id: ID of the file to validate
        
    Returns:
        Validation results
    """
    task_id = self.request.id
    logger.info(f"Starting file format validation task {task_id} for file {file_id}")
    
    db = next(get_db())
    try:
        # Get file record
        file_record = db.exec(select(FileRegistry).where(FileRegistry.id == file_id)).first()
        if not file_record:
            raise FileProcessingException(f"File record not found: {file_id}")
        
        # Get appropriate processor
        file_type = file_record.file_type.lower()
        processor = get_processor(file_type, db_session=db)
        
        # Validate file format
        is_valid, error_message = await processor.validate_file_format(file_record.file_path)
        
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
    
    finally:
        db.close()

@celery_app.task(
    bind=True,
    name='etl.preview_file_data',
    time_limit=600
)
async def preview_file_data_task(self, file_id: str, rows: int = 10):
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
    
    db = next(get_db())
    try:
        # Get file record
        file_record = db.exec(select(FileRegistry).where(FileRegistry.id == file_id)).first()
        if not file_record:
            raise FileProcessingException(f"File record not found: {file_id}")
        
        # Get appropriate processor
        file_type = file_record.file_type.lower()
        processor = get_processor(file_type, db_session=db)
        
        # Generate preview
        preview_data = await processor.preview_data(file_record.file_path, rows)
        
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
    
    finally:
        db.close()

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
        from celery import group
        
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
                # Execute job
                job_result = execute_etl_job.apply_async(args=[job_id, execution_config]).get()
                
                chain_results['job_results'].append(job_result)
                
                if job_result.get('status') == 'success':
                    chain_results['successful_jobs'] += 1
                else:
                    chain_results['failed_jobs'] += 1
                    
                    # Stop chain execution on failure if configured
                    if execution_config and execution_config.get('stop_on_failure', True):
                        chain_results['chain_stopped'] = True
                        logger.warning(f"Chain execution stopped due to job failure: {job_id}")
                        break
                
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
    
@celery_app.task(bind=True, name='simple_test')
def simple_test(self, message="hello"):
    print(f"Simple test executed: {message}")
    return f"Success: {message}"