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
            select(JobExecution).where(JobExecution.execution_id == execution_id)
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
                select(JobExecution).where(JobExecution.execution_id == execution_id)
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
