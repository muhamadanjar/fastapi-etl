"""
Helper utilities untuk Celery tasks.
Includes error logging dan dependent job triggering.
"""

import traceback
from typing import Optional, Dict, Any
from uuid import UUID
from sqlalchemy.orm import Session

from app.application.services.error_service import ErrorService
from app.application.services.dependency_service import DependencyService
from app.infrastructure.db.models.etl_control.error_logs import ErrorType, ErrorSeverity
from app.utils.logger import get_logger

logger = get_logger(__name__)


async def log_task_error(
    db: Session,
    exception: Exception,
    error_type: ErrorType = ErrorType.PROCESSING_ERROR,
    error_severity: ErrorSeverity = ErrorSeverity.HIGH,
    job_execution_id: Optional[UUID] = None,
    context: Optional[Dict[str, Any]] = None
) -> None:
    """
    Log an error from a Celery task to the database.
    
    Args:
        db: Database session
        exception: The exception that occurred
        error_type: Type of error
        error_severity: Severity level
        job_execution_id: Related job execution ID
        context: Additional context information
    """
    try:
        error_service = ErrorService(db)
        await error_service.log_exception(
            exception=exception,
            error_type=error_type,
            error_severity=error_severity,
            job_execution_id=job_execution_id,
            context=context
        )
        logger.info(f"Error logged to database: {str(exception)}")
    except Exception as e:
        # If we can't log to database, at least log to console
        logger.error(f"Failed to log error to database: {e}")
        logger.error(f"Original error: {exception}")
        logger.error(f"Stack trace: {traceback.format_exc()}")


async def trigger_dependent_jobs(
    db: Session,
    parent_job_id: UUID,
    parent_execution_status: str
) -> Dict[str, Any]:
    """
    Check and trigger dependent jobs after parent job completes.
    
    Args:
        db: Database session
        parent_job_id: ID of the parent job that just completed
        parent_execution_status: Status of the parent execution
        
    Returns:
        Dictionary with triggered jobs information
    """
    try:
        from app.tasks.etl_tasks import execute_etl_job
        
        dep_service = DependencyService(db)
        
        # Get child dependencies
        deps_info = await dep_service.get_job_dependencies(parent_job_id)
        child_deps = deps_info.get("child_dependencies", [])
        
        if not child_deps:
            logger.info(f"No dependent jobs for job {parent_job_id}")
            return {
                "triggered_jobs": [],
                "total_children": 0,
                "message": "No dependent jobs"
            }
        
        triggered_jobs = []
        
        for child_dep in child_deps:
            child_job_id = child_dep["child_job_id"]
            dependency_type = child_dep["dependency_type"]
            
            # Check if all dependencies for child job are met
            dep_status = await dep_service.check_dependencies_met(child_job_id)
            
            if dep_status["dependencies_met"]:
                logger.info(
                    f"Auto-triggering dependent job {child_job_id} "
                    f"(parent: {parent_job_id}, type: {dependency_type})"
                )
                
                # Trigger child job asynchronously
                task_result = execute_etl_job.delay(
                    job_id=str(child_job_id),
                    execution_id=None,  # Will be created in task
                    batch_id=None,  # Will be generated
                    parameters={}
                )
                
                triggered_jobs.append({
                    "child_job_id": str(child_job_id),
                    "dependency_type": dependency_type,
                    "task_id": task_result.id,
                    "status": "triggered"
                })
            else:
                logger.info(
                    f"Dependent job {child_job_id} not triggered - "
                    f"dependencies not met: {dep_status['unmet_dependencies']}"
                )
        
        return {
            "triggered_jobs": triggered_jobs,
            "total_children": len(child_deps),
            "total_triggered": len(triggered_jobs),
            "message": f"Triggered {len(triggered_jobs)} out of {len(child_deps)} dependent jobs"
        }
        
    except Exception as e:
        logger.error(f"Error triggering dependent jobs: {e}")
        logger.error(traceback.format_exc())
        return {
            "triggered_jobs": [],
            "total_children": 0,
            "error": str(e)
        }


def get_error_type_from_exception(exception: Exception) -> ErrorType:
    """
    Determine error type from exception class.
    
    Args:
        exception: The exception
        
    Returns:
        Appropriate ErrorType
    """
    exception_name = type(exception).__name__
    
    if "Validation" in exception_name or "Invalid" in exception_name:
        return ErrorType.VALIDATION_ERROR
    elif "Database" in exception_name or "SQL" in exception_name:
        return ErrorType.DATABASE_ERROR
    elif "Network" in exception_name or "Connection" in exception_name:
        return ErrorType.NETWORK_ERROR
    elif "Timeout" in exception_name:
        return ErrorType.TIMEOUT_ERROR
    elif "Config" in exception_name or "Setting" in exception_name:
        return ErrorType.CONFIGURATION_ERROR
    elif "Processing" in exception_name or "ETL" in exception_name:
        return ErrorType.PROCESSING_ERROR
    else:
        return ErrorType.UNKNOWN_ERROR


def get_error_severity_from_exception(exception: Exception) -> ErrorSeverity:
    """
    Determine error severity from exception class.
    
    Args:
        exception: The exception
        
    Returns:
        Appropriate ErrorSeverity
    """
    exception_name = type(exception).__name__
    
    # Critical errors
    if any(x in exception_name for x in ["Critical", "Fatal", "System"]):
        return ErrorSeverity.CRITICAL
    
    # High severity errors
    elif any(x in exception_name for x in ["Database", "Network", "Timeout"]):
        return ErrorSeverity.HIGH
    
    # Medium severity errors
    elif any(x in exception_name for x in ["Processing", "ETL", "Validation"]):
        return ErrorSeverity.MEDIUM
    
    # Low severity errors
    else:
        return ErrorSeverity.LOW
