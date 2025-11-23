"""
Cleanup tasks untuk maintenance dan optimasi sistem ETL.
"""

import os
import shutil
import asyncio
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
from sqlalchemy import text, select, delete, and_
from celery import Task
from celery.utils.log import get_task_logger

from app.tasks.celery_app import celery_app
from app.infrastructure.db.connection import get_session
from app.infrastructure.db.models.raw_data.file_registry import FileRegistry
from app.infrastructure.db.models.raw_data.raw_records import RawRecords
from app.infrastructure.db.models.etl_control.job_executions import JobExecution
from app.infrastructure.db.models.processed.entities import Entity
from app.infrastructure.db.models.audit.change_log import ChangeLog
from app.core.enums import ProcessingStatus, JobStatus
from app.utils.date_utils import get_current_timestamp
from app.core.config import get_settings

# Import error logging helpers
from app.tasks.task_helpers import log_task_error, get_error_type_from_exception, get_error_severity_from_exception
from app.infrastructure.db.models.etl_control.error_logs import ErrorType, ErrorSeverity

logger = get_task_logger(__name__)
settings = get_settings()


class CleanupTask(Task):
    """Base class untuk cleanup tasks dengan error handling."""
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error(f"Cleanup task {self.name} failed: {exc}")
    
    def on_success(self, retval, task_id, args, kwargs):
        logger.info(f"Cleanup task {self.name} completed successfully")


@celery_app.task(bind=True, base=CleanupTask, name="cleanup_temporary_files")
def cleanup_temporary_files(self, older_than_hours: int = 24) -> Dict[str, Any]:
    """
    Cleanup temporary files yang sudah tidak digunakan.
    
    Args:
        older_than_hours: Hapus file yang lebih lama dari X jam
    
    Returns:
        Dict dengan hasil cleanup
    """
    try:
        logger.info(f"Starting cleanup of temporary files older than {older_than_hours} hours")
        
        cutoff_time = datetime.now() - timedelta(hours=older_than_hours)
        temp_dirs = [
            Path(settings.TEMP_DIR),
            Path("storage/temp"),
            Path("/tmp/etl_temp"),
            Path("storage/uploads/temp")
        ]
        
        total_files_deleted = 0
        total_size_freed = 0
        errors = []
        
        for temp_dir in temp_dirs:
            if not temp_dir.exists():
                continue
                
            try:
                for file_path in temp_dir.rglob("*"):
                    if file_path.is_file():
                        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                        
                        if file_mtime < cutoff_time:
                            try:
                                file_size = file_path.stat().st_size
                                file_path.unlink()
                                total_files_deleted += 1
                                total_size_freed += file_size
                                logger.debug(f"Deleted temp file: {file_path}")
                            except Exception as e:
                                errors.append(f"Failed to delete {file_path}: {str(e)}")
                
                # Cleanup empty directories
                for dir_path in temp_dir.rglob("*"):
                    if dir_path.is_dir() and not any(dir_path.iterdir()):
                        try:
                            dir_path.rmdir()
                            logger.debug(f"Deleted empty directory: {dir_path}")
                        except Exception as e:
                            errors.append(f"Failed to delete directory {dir_path}: {str(e)}")
                            
            except Exception as e:
                errors.append(f"Error processing directory {temp_dir}: {str(e)}")
        
        result = {
            "task": "cleanup_temporary_files",
            "status": "completed",
            "files_deleted": total_files_deleted,
            "size_freed_bytes": total_size_freed,
            "size_freed_mb": round(total_size_freed / (1024 * 1024), 2),
            "cutoff_time": cutoff_time.isoformat(),
            "errors": errors,
            "completed_at": get_current_timestamp().isoformat()
        }
        
        logger.info(f"Cleanup completed: {total_files_deleted} files deleted, {result['size_freed_mb']} MB freed")
        return result
        
    except Exception as e:
        logger.error(f"Error in cleanup_temporary_files: {str(e)}")
        
        # Log error to database
        try:
            with get_session() as db:
                asyncio.run(log_task_error(
                    db=db,
                    exception=e,
                    error_type=ErrorType.SYSTEM_ERROR,
                    error_severity=ErrorSeverity.MEDIUM,
                    context={
                        "task_name": "cleanup_temporary_files",
                        "older_than_hours": older_than_hours
                    }
                ))
        except Exception as log_error:
            logger.error(f"Failed to log error to database: {log_error}")
        
        raise


@celery_app.task(bind=True, base=CleanupTask, name="archive_old_data")
def archive_old_data(self, archive_after_days: int = 90) -> Dict[str, Any]:
    """
    Archive data lama ke storage yang lebih murah.
    
    Args:
        archive_after_days: Archive data yang lebih lama dari X hari
    
    Returns:
        Dict dengan hasil archiving
    """
    try:
        logger.info(f"Starting archiving of data older than {archive_after_days} days")
        
        cutoff_date = datetime.now() - timedelta(days=archive_after_days)
        archived_count = 0
        archived_size = 0
        errors = []
        
        with get_session() as db:
            # Archive old file records
            old_files_stmt = select(FileRegistry).where(
                and_(
                    FileRegistry.upload_date < cutoff_date,
                    FileRegistry.processing_status == ProcessingStatus.COMPLETED.value
                )
            )
            old_files = db.execute(old_files_stmt).scalars().all()
            
            archive_dir = Path("storage/archive") / str(cutoff_date.year)
            archive_dir.mkdir(parents=True, exist_ok=True)
            
            for file_record in old_files:
                try:
                    original_path = Path(file_record.file_path)
                    if original_path.exists():
                        # Create archive path
                        archive_path = archive_dir / f"{file_record.batch_id}" / original_path.name
                        archive_path.parent.mkdir(parents=True, exist_ok=True)
                        
                        # Move file to archive
                        shutil.move(str(original_path), str(archive_path))
                        
                        # Update file registry with new path
                        file_record.file_path = str(archive_path)
                        file_record.file_metadata = file_record.file_metadata or {}
                        file_record.file_metadata['archived_at'] = get_current_timestamp().isoformat()
                        file_record.file_metadata['archived_from'] = str(original_path)
                        
                        archived_count += 1
                        archived_size += original_path.stat().st_size if original_path.exists() else 0
                        
                        logger.debug(f"Archived file: {original_path} -> {archive_path}")
                        
                except Exception as e:
                    errors.append(f"Failed to archive file {file_record.file_id}: {str(e)}")
            
            db.commit()
            
            # Archive old raw records (compress to JSON files)
            old_records_stmt = select(RawRecords).where(
                RawRecords.created_at < cutoff_date
            ).limit(10000)  # Process in batches
            
            old_records = db.execute(old_records_stmt).scalars().all()
            
            if old_records:
                import json
                archive_file = archive_dir / f"raw_records_{cutoff_date.strftime('%Y%m%d')}.json"
                
                with open(archive_file, 'w') as f:
                    records_data = []
                    for record in old_records:
                        records_data.append({
                            'record_id': record.record_id,
                            'file_id': record.file_id,
                            'raw_data': record.raw_data,
                            'created_at': record.created_at.isoformat() if record.created_at else None
                        })
                    json.dump(records_data, f, indent=2)
                
                # Delete archived records from database
                for record in old_records:
                    db.delete(record)
                
                db.commit()
                logger.info(f"Archived {len(old_records)} raw records to {archive_file}")
        
        result = {
            "task": "archive_old_data",
            "status": "completed",
            "files_archived": archived_count,
            "size_archived_bytes": archived_size,
            "size_archived_mb": round(archived_size / (1024 * 1024), 2),
            "cutoff_date": cutoff_date.isoformat(),
            "archive_directory": str(archive_dir),
            "errors": errors,
            "completed_at": get_current_timestamp().isoformat()
        }
        
        logger.info(f"Archiving completed: {archived_count} files archived, {result['size_archived_mb']} MB")
        return result
        
    except Exception as e:
        logger.error(f"Error in archive_old_data: {str(e)}")
        
        # Log error to database
        try:
            with get_session() as db:
                asyncio.run(log_task_error(
                    db=db,
                    exception=e,
                    error_type=ErrorType.SYSTEM_ERROR,
                    error_severity=ErrorSeverity.HIGH,
                    context={
                        "task_name": "archive_old_data",
                        "archive_after_days": archive_after_days
                    }
                ))
        except Exception as log_error:
            logger.error(f"Failed to log error to database: {log_error}")
        
        raise


@celery_app.task(bind=True, base=CleanupTask, name="purge_expired_records")
def purge_expired_records(self, retention_days: int = 365) -> Dict[str, Any]:
    """
    Hapus records yang sudah melewati retention period.
    
    Args:
        retention_days: Hapus data yang lebih lama dari X hari
    
    Returns:
        Dict dengan hasil purging
    """
    try:
        logger.info(f"Starting purging of records older than {retention_days} days")
        
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        purged_counts = {}
        errors = []
        
        with get_session() as db:
            # Purge old job executions
            try:
                old_executions_stmt = delete(JobExecution).where(
                    and_(
                        JobExecution.created_at < cutoff_date,
                        JobExecution.status.in_([JobStatus.SUCCESS.value, JobStatus.FAILED.value])
                    )
                )
                result = db.execute(old_executions_stmt)
                purged_counts['job_executions'] = result.rowcount
                logger.info(f"Purged {result.rowcount} old job executions")
            except Exception as e:
                errors.append(f"Failed to purge job executions: {str(e)}")
            
            # Purge old change logs
            try:
                old_changes_stmt = delete(ChangeLog).where(
                    ChangeLog.changed_at < cutoff_date
                )
                result = db.execute(old_changes_stmt)
                purged_counts['change_logs'] = result.rowcount
                logger.info(f"Purged {result.rowcount} old change logs")
            except Exception as e:
                errors.append(f"Failed to purge change logs: {str(e)}")
            
            # Purge old inactive entities
            try:
                old_entities_stmt = delete(Entity).where(
                    and_(
                        Entity.last_updated < cutoff_date,
                        Entity.is_active == False
                    )
                )
                result = db.execute(old_entities_stmt)
                purged_counts['inactive_entities'] = result.rowcount
                logger.info(f"Purged {result.rowcount} old inactive entities")
            except Exception as e:
                errors.append(f"Failed to purge inactive entities: {str(e)}")
            
            # Purge old failed file records
            try:
                old_failed_files_stmt = delete(FileRegistry).where(
                    and_(
                        FileRegistry.upload_date < cutoff_date,
                        FileRegistry.processing_status == ProcessingStatus.FAILED.value
                    )
                )
                result = db.execute(old_failed_files_stmt)
                purged_counts['failed_files'] = result.rowcount
                logger.info(f"Purged {result.rowcount} old failed file records")
            except Exception as e:
                errors.append(f"Failed to purge failed files: {str(e)}")
            
            db.commit()
        
        total_purged = sum(purged_counts.values())
        
        result = {
            "task": "purge_expired_records",
            "status": "completed",
            "total_records_purged": total_purged,
            "purged_by_table": purged_counts,
            "retention_days": retention_days,
            "cutoff_date": cutoff_date.isoformat(),
            "errors": errors,
            "completed_at": get_current_timestamp().isoformat()
        }
        
        logger.info(f"Purging completed: {total_purged} total records purged")
        return result
        
    except Exception as e:
        logger.error(f"Error in purge_expired_records: {str(e)}")
        
        # Log error to database
        try:
            with get_session() as db:
                asyncio.run(log_task_error(
                    db=db,
                    exception=e,
                    error_type=ErrorType.DATABASE_ERROR,
                    error_severity=ErrorSeverity.HIGH,
                    context={
                        "task_name": "purge_expired_records",
                        "retention_days": retention_days
                    }
                ))
        except Exception as log_error:
            logger.error(f"Failed to log error to database: {log_error}")
        
        raise


@celery_app.task(bind=True, base=CleanupTask, name="optimize_database")
def optimize_database(self) -> Dict[str, Any]:
    """
    Optimasi database performance dengan VACUUM, ANALYZE, dan REINDEX.
    
    Returns:
        Dict dengan hasil optimasi
    """
    try:
        logger.info("Starting database optimization")
        
        optimization_results = {}
        errors = []
        
        with get_session() as db:
            try:
                # Get database size before optimization
                size_query = text("SELECT pg_size_pretty(pg_database_size(current_database())) as size")
                before_size = db.execute(size_query).scalar()
                optimization_results['database_size_before'] = before_size
                
                # Get table statistics
                stats_query = text("""
                    SELECT schemaname, tablename, n_tup_ins, n_tup_upd, n_tup_del, n_dead_tup
                    FROM pg_stat_user_tables
                    ORDER BY n_dead_tup DESC;
                """)
                table_stats = db.execute(stats_query).fetchall()
                optimization_results['tables_before_optimization'] = len(table_stats)
                
                # VACUUM ANALYZE all tables
                vacuum_commands = [
                    "VACUUM ANALYZE raw_data.file_registry;",
                    "VACUUM ANALYZE raw_data.raw_records;",
                    "VACUUM ANALYZE staging.standardized_data;",
                    "VACUUM ANALYZE processed.entities;",
                    "VACUUM ANALYZE etl_control.job_executions;",
                    "VACUUM ANALYZE audit.change_log;"
                ]
                
                for command in vacuum_commands:
                    try:
                        db.execute(text(command))
                        logger.debug(f"Executed: {command}")
                    except Exception as e:
                        errors.append(f"Failed to execute {command}: {str(e)}")
                
                # Update table statistics
                db.execute(text("ANALYZE;"))
                logger.info("Updated table statistics")
                
                # Reindex important tables
                reindex_commands = [
                    "REINDEX TABLE raw_data.file_registry;",
                    "REINDEX TABLE processed.entities;",
                    "REINDEX TABLE etl_control.job_executions;"
                ]
                
                for command in reindex_commands:
                    try:
                        db.execute(text(command))
                        logger.debug(f"Executed: {command}")
                    except Exception as e:
                        errors.append(f"Failed to execute {command}: {str(e)}")
                
                # Get database size after optimization
                after_size = db.execute(size_query).scalar()
                optimization_results['database_size_after'] = after_size
                
                # Get updated table statistics
                updated_stats = db.execute(stats_query).fetchall()
                optimization_results['dead_tuples_cleaned'] = sum(row.n_dead_tup for row in table_stats) - sum(row.n_dead_tup for row in updated_stats)
                
                db.commit()
                
            except Exception as e:
                errors.append(f"Database optimization error: {str(e)}")
                db.rollback()
        
        result = {
            "task": "optimize_database",
            "status": "completed" if not errors else "completed_with_errors",
            "optimization_results": optimization_results,
            "errors": errors,
            "completed_at": get_current_timestamp().isoformat()
        }
        
        logger.info("Database optimization completed")
        return result
        
    except Exception as e:
        logger.error(f"Error in optimize_database: {str(e)}")
        
        # Log error to database
        try:
            with get_session() as db:
                asyncio.run(log_task_error(
                    db=db,
                    exception=e,
                    error_type=ErrorType.DATABASE_ERROR,
                    error_severity=ErrorSeverity.CRITICAL,
                    context={
                        "task_name": "optimize_database"
                    }
                ))
        except Exception as log_error:
            logger.error(f"Failed to log error to database: {log_error}")
        
        raise


@celery_app.task(bind=True, base=CleanupTask, name="cleanup_failed_jobs")
def cleanup_failed_jobs(self, older_than_days: int = 7) -> Dict[str, Any]:
    """
    Cleanup job executions yang gagal dan sudah lama.
    
    Args:
        older_than_days: Cleanup job yang gagal lebih dari X hari
    
    Returns:
        Dict dengan hasil cleanup
    """
    try:
        logger.info(f"Starting cleanup of failed jobs older than {older_than_days} days")
        
        cutoff_date = datetime.now() - timedelta(days=older_than_days)
        cleaned_count = 0
        errors = []
        
        with get_session() as db:
            # Get failed job executions
            failed_jobs_stmt = select(JobExecution).where(
                and_(
                    JobExecution.status == JobStatus.FAILED.value,
                    JobExecution.created_at < cutoff_date
                )
            )
            failed_jobs = db.execute(failed_jobs_stmt).scalars().all()
            
            for job in failed_jobs:
                try:
                    # Archive error details before deletion
                    error_log_dir = Path("storage/logs/failed_jobs")
                    error_log_dir.mkdir(parents=True, exist_ok=True)
                    
                    error_file = error_log_dir / f"job_{job.execution_id}_{job.created_at.strftime('%Y%m%d_%H%M%S')}.json"
                    
                    import json
                    error_data = {
                        "execution_id": job.execution_id,
                        "job_id": job.job_id,
                        "batch_id": job.batch_id,
                        "start_time": job.start_time.isoformat() if job.start_time else None,
                        "end_time": job.end_time.isoformat() if job.end_time else None,
                        "execution_log": job.execution_log,
                        "error_details": job.error_details,
                        "archived_at": get_current_timestamp().isoformat()
                    }
                    
                    with open(error_file, 'w') as f:
                        json.dump(error_data, f, indent=2)
                    
                    # Delete the job execution
                    db.delete(job)
                    cleaned_count += 1
                    
                    logger.debug(f"Cleaned failed job {job.execution_id} and archived to {error_file}")
                    
                except Exception as e:
                    errors.append(f"Failed to cleanup job {job.execution_id}: {str(e)}")
            
            # Also cleanup related file records if they failed
            failed_files_stmt = select(FileRegistry).where(
                and_(
                    FileRegistry.processing_status == ProcessingStatus.FAILED.value,
                    FileRegistry.upload_date < cutoff_date
                )
            )
            failed_files = db.execute(failed_files_stmt).scalars().all()
            
            file_cleanup_count = 0
            for file_record in failed_files:
                try:
                    # Remove physical file if exists
                    file_path = Path(file_record.file_path)
                    if file_path.exists():
                        file_path.unlink()
                    
                    # Delete file record
                    db.delete(file_record)
                    file_cleanup_count += 1
                    
                except Exception as e:
                    errors.append(f"Failed to cleanup file {file_record.file_id}: {str(e)}")
            
            db.commit()
        
        result = {
            "task": "cleanup_failed_jobs",
            "status": "completed",
            "failed_jobs_cleaned": cleaned_count,
            "failed_files_cleaned": file_cleanup_count,
            "cutoff_date": cutoff_date.isoformat(),
            "errors": errors,
            "completed_at": get_current_timestamp().isoformat()
        }
        
        logger.info(f"Failed jobs cleanup completed: {cleaned_count} jobs, {file_cleanup_count} files cleaned")
        return result
        
    except Exception as e:
        logger.error(f"Error in cleanup_failed_jobs: {str(e)}")
        
        # Log error to database
        try:
            with get_session() as db:
                asyncio.run(log_task_error(
                    db=db,
                    exception=e,
                    error_type=ErrorType.SYSTEM_ERROR,
                    error_severity=ErrorSeverity.MEDIUM,
                    context={
                        "task_name": "cleanup_failed_jobs",
                        "older_than_days": older_than_days
                    }
                ))
        except Exception as log_error:
            logger.error(f"Failed to log error to database: {log_error}")
        
        raise


@celery_app.task(bind=True, base=CleanupTask, name="vacuum_database")
def vacuum_database(self, full_vacuum: bool = False) -> Dict[str, Any]:
    """
    Jalankan VACUUM pada database untuk reclaim space.
    
    Args:
        full_vacuum: Jalankan VACUUM FULL (lebih lama tapi lebih thorough)
    
    Returns:
        Dict dengan hasil vacuum
    """
    try:
        logger.info(f"Starting database vacuum (full={full_vacuum})")
        
        vacuum_results = {}
        errors = []
        
        with get_session() as db:
            try:
                # Get database size before vacuum
                size_query = text("SELECT pg_size_pretty(pg_database_size(current_database())) as size")
                before_size = db.execute(size_query).scalar()
                vacuum_results['database_size_before'] = before_size
                
                # Get table sizes before vacuum
                table_size_query = text("""
                    SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
                    FROM pg_tables 
                    WHERE schemaname IN ('raw_data', 'staging', 'processed', 'etl_control', 'audit')
                    ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
                """)
                table_sizes_before = db.execute(table_size_query).fetchall()
                vacuum_results['largest_tables_before'] = [
                    {"schema": row.schemaname, "table": row.tablename, "size": row.size} 
                    for row in table_sizes_before[:10]
                ]
                
                # Perform vacuum
                vacuum_command = "VACUUM FULL;" if full_vacuum else "VACUUM;"
                db.execute(text(vacuum_command))
                logger.info(f"Executed: {vacuum_command}")
                
                # Get database size after vacuum
                after_size = db.execute(size_query).scalar()
                vacuum_results['database_size_after'] = after_size
                
                # Calculate space reclaimed
                before_bytes_query = text("SELECT pg_database_size(current_database()) as size")
                after_bytes = db.execute(before_bytes_query).scalar()
                
                # Get updated table sizes
                table_sizes_after = db.execute(table_size_query).fetchall()
                vacuum_results['largest_tables_after'] = [
                    {"schema": row.schemaname, "table": row.tablename, "size": row.size} 
                    for row in table_sizes_after[:10]
                ]
                
                vacuum_results['vacuum_type'] = 'FULL' if full_vacuum else 'STANDARD'
                
                db.commit()
                
            except Exception as e:
                errors.append(f"Vacuum operation error: {str(e)}")
                db.rollback()
        
        result = {
            "task": "vacuum_database",
            "status": "completed" if not errors else "completed_with_errors",
            "vacuum_results": vacuum_results,
            "errors": errors,
            "completed_at": get_current_timestamp().isoformat()
        }
        
        logger.info(f"Database vacuum completed ({vacuum_results.get('vacuum_type', 'UNKNOWN')})")
        return result
        
    except Exception as e:
        logger.error(f"Error in vacuum_database: {str(e)}")
        
        # Log error to database
        try:
            with get_session() as db:
                asyncio.run(log_task_error(
                    db=db,
                    exception=e,
                    error_type=ErrorType.DATABASE_ERROR,
                    error_severity=ErrorSeverity.HIGH,
                    context={
                        "task_name": "vacuum_database",
                        "full_vacuum": full_vacuum
                    }
                ))
        except Exception as log_error:
            logger.error(f"Failed to log error to database: {log_error}")
        
        raise


# Additional cleanup tasks
@celery_app.task(bind=True, base=CleanupTask, name="cleanup_orphaned_files")
def cleanup_orphaned_files(self) -> Dict[str, Any]:
    """
    Cleanup file fisik yang sudah tidak ada record-nya di database.
    
    Returns:
        Dict dengan hasil cleanup
    """
    try:
        logger.info("Starting cleanup of orphaned files")
        
        orphaned_count = 0
        orphaned_size = 0
        errors = []
        
        storage_dirs = [
            Path("storage/uploads"),
            Path("storage/processed"),
            Path("storage/exports")
        ]
        
        with get_session() as db:
            # Get all file paths from database
            file_paths_stmt = select(FileRegistry.file_path)
            db_file_paths = set(db.execute(file_paths_stmt).scalars().all())
        
        for storage_dir in storage_dirs:
            if not storage_dir.exists():
                continue
                
            for file_path in storage_dir.rglob("*"):
                if file_path.is_file():
                    file_path_str = str(file_path)
                    
                    if file_path_str not in db_file_paths:
                        try:
                            file_size = file_path.stat().st_size
                            file_path.unlink()
                            orphaned_count += 1
                            orphaned_size += file_size
                            logger.debug(f"Deleted orphaned file: {file_path}")
                        except Exception as e:
                            errors.append(f"Failed to delete {file_path}: {str(e)}")
        
        result = {
            "task": "cleanup_orphaned_files",
            "status": "completed",
            "orphaned_files_deleted": orphaned_count,
            "size_freed_bytes": orphaned_size,
            "size_freed_mb": round(orphaned_size / (1024 * 1024), 2),
            "errors": errors,
            "completed_at": get_current_timestamp().isoformat()
        }
        
        logger.info(f"Orphaned files cleanup completed: {orphaned_count} files deleted")
        return result
        
    except Exception as e:
        logger.error(f"Error in cleanup_orphaned_files: {str(e)}")
        
        # Log error to database
        try:
            with get_session() as db:
                asyncio.run(log_task_error(
                    db=db,
                    exception=e,
                    error_type=ErrorType.SYSTEM_ERROR,
                    error_severity=ErrorSeverity.MEDIUM,
                    context={
                        "task_name": "cleanup_orphaned_files"
                    }
                ))
        except Exception as log_error:
            logger.error(f"Failed to log error to database: {log_error}")
        
        raise


@celery_app.task(bind=True, base=CleanupTask, name="reset_stuck_jobs")
def reset_stuck_jobs(self, stuck_hours: int = 24) -> Dict[str, Any]:
    """
    Reset job yang stuck dalam status RUNNING lebih dari X jam.
    
    Args:
        stuck_hours: Reset job yang stuck lebih dari X jam
    
    Returns:
        Dict dengan hasil reset
    """
    try:
        logger.info(f"Starting reset of jobs stuck for more than {stuck_hours} hours")
        
        cutoff_time = datetime.now() - timedelta(hours=stuck_hours)
        reset_count = 0
        errors = []
        
        with get_session() as db:
            # Find stuck jobs
            stuck_jobs_stmt = select(JobExecution).where(
                and_(
                    JobExecution.status == JobStatus.RUNNING.value,
                    JobExecution.start_time < cutoff_time
                )
            )
            stuck_jobs = db.execute(stuck_jobs_stmt).scalars().all()
            
            for job in stuck_jobs:
                try:
                    job.status = JobStatus.FAILED.value
                    job.end_time = get_current_timestamp()
                    job.execution_log = f"Job reset due to being stuck for more than {stuck_hours} hours"
                    job.error_details = {
                        "error": "Job timeout",
                        "reason": f"Stuck in RUNNING status for more than {stuck_hours} hours",
                        "reset_at": get_current_timestamp().isoformat()
                    }
                    
                    reset_count += 1
                    logger.debug(f"Reset stuck job {job.execution_id}")
                    
                except Exception as e:
                    errors.append(f"Failed to reset job {job.execution_id}: {str(e)}")
            
            # Also reset file processing status if stuck
            stuck_files_stmt = select(FileRegistry).where(
                and_(
                    FileRegistry.processing_status == ProcessingStatus.PROCESSING.value,
                    FileRegistry.upload_date < cutoff_time
                )
            )
            stuck_files = db.execute(stuck_files_stmt).scalars().all()
            
            file_reset_count = 0
            for file_record in stuck_files:
                try:
                    file_record.processing_status = ProcessingStatus.FAILED.value
                    file_record.file_metadata = file_record.file_metadata or {}
                    file_record.file_metadata['reset_reason'] = f"Stuck in processing for more than {stuck_hours} hours"
                    file_record.file_metadata['reset_at'] = get_current_timestamp().isoformat()
                    
                    file_reset_count += 1
                    
                except Exception as e:
                    errors.append(f"Failed to reset file {file_record.file_id}: {str(e)}")
            
            db.commit()
        
        result = {
            "task": "reset_stuck_jobs",
            "status": "completed",
            "stuck_jobs_reset": reset_count,
            "stuck_files_reset": file_reset_count,
            "stuck_threshold_hours": stuck_hours,
            "cutoff_time": cutoff_time.isoformat(),
            "errors": errors,
            "completed_at": get_current_timestamp().isoformat()
        }
        
        logger.info(f"Stuck jobs reset completed: {reset_count} jobs, {file_reset_count} files reset")
        return result
        
    except Exception as e:
        logger.error(f"Error in reset_stuck_jobs: {str(e)}")
        
        # Log error to database
        try:
            with get_session() as db:
                asyncio.run(log_task_error(
                    db=db,
                    exception=e,
                    error_type=ErrorType.SYSTEM_ERROR,
                    error_severity=ErrorSeverity.HIGH,
                    context={
                        "task_name": "reset_stuck_jobs",
                        "stuck_hours": stuck_hours
                    }
                ))
        except Exception as log_error:
            logger.error(f"Failed to log error to database: {log_error}")
        
        raise
