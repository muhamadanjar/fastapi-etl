"""
ETL service untuk orchestrating extraction, transformation, dan loading operations.
Fokus pada job management dan execution control.
"""

import uuid
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import select, and_
from uuid import UUID
from app.services.base import BaseService
from app.infrastructure.db.models.etl_control.etl_jobs import EtlJob
from app.infrastructure.db.models.etl_control.job_executions import JobExecution
from app.core.exceptions import ETLError
from app.core.enums import JobStatus, JobType
from app.utils.date_utils import get_current_timestamp
from app.utils.event_publisher import get_event_publisher
from app.infrastructure.cache import cache_manager


class ETLService(BaseService):
    """Service untuk orchestrating ETL operations."""
    
    def __init__(self, db_session: Session):
        super().__init__(db_session)
    
    def get_service_name(self) -> str:
        return "ETLService"
    
    async def create_etl_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new ETL job."""
        try:
            self.validate_input(job_data, ["job_name", "job_type", "source_type"])
            self.log_operation("create_etl_job", {"job_name": job_data["job_name"]})
            
            job = EtlJob(
                job_name=job_data["job_name"],
                job_type=job_data["job_type"],
                job_category=job_data.get("job_category", "GENERAL"),
                source_type=job_data["source_type"],
                target_schema=job_data.get("target_schema"),
                target_table=job_data.get("target_table"),
                job_config=job_data.get("job_config", {}),
                schedule_expression=job_data.get("schedule_expression"),
                is_active=job_data.get("is_active", True)
            )
            
            self.db.add(job)
            self.db.commit()
            self.db.refresh(job)
            
            # Invalidate jobs list cache using existing infrastructure
            cache = await cache_manager.get_cache()
            if cache:
                try:
                    # Clear all jobs:* keys
                    keys = await cache.scan_keys("jobs:*")
                    if keys:
                        await cache.delete(*keys)
                except Exception as cache_error:
                    self.logger.warning(f"Failed to invalidate cache: {str(cache_error)}")
            
            return {
                "job_id": job.id,
                "job_name": job.job_name,
                "job_type": job.job_type,
                "status": "created"
            }
            
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "create_etl_job")
    
    async def execute_job(self, job_id: UUID, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute an ETL job."""
        try:
            from app.tasks.etl_tasks import execute_etl_job
            from app.services.dependency_service import DependencyService
            
            self.log_operation("execute_job", {"job_id": job_id})
            
            job = self.db.get(EtlJob, job_id)
            if not job:
                raise ETLError("Job not found")
            
            if not job.is_active:
                raise ETLError("Job is not active")
            
            # Check dependencies
            dependency_service = DependencyService(self.db)
            dep_status = await dependency_service.check_dependencies_met(job_id)
            
            if not dep_status["dependencies_met"]:
                unmet = dep_status["unmet_dependencies"]
                unmet_details = ", ".join([
                    f"{dep['parent_job_name']} ({dep['reason']})" 
                    for dep in unmet
                ])
                raise ETLError(
                    f"Cannot execute job: dependencies not met. "
                    f"Unmet dependencies: {unmet_details}"
                )
            
            batch_id = self._generate_batch_id()
            
            execution = JobExecution(
                job_id=job_id,
                batch_id=batch_id,
                start_time=get_current_timestamp(),
                status=JobStatus.RUNNING.value,
                records_processed=0,
                records_successful=0,
                records_failed=0,
                execution_metadata=parameters or {}
            )
            
            self.db.add(execution)
            self.db.commit()
            self.db.refresh(execution)
            
            # Publish job started event
            try:
                publisher = await get_event_publisher()
                await publisher.publish_job_started(
                    job_id=job_id,
                    execution_id=execution.id,
                    job_name=job.job_name,
                    job_type=job.job_type
                )
            except Exception as e:
                self.logger.warning(f"Failed to publish job started event: {str(e)}")
            
            # Trigger async Celery task
            task = execute_etl_job.delay(
                str(job_id),
                str(execution.id),
                batch_id,
                parameters or {}
            )
            
            return {
                "execution_id": execution.id,
                "job_id": job_id,
                "job_name": job.job_name,
                "batch_id": batch_id,
                "status": JobStatus.RUNNING.value,
                "task_id": task.id,
                "message": "Job execution started"
            }
            
        except Exception as e:
            self.db_session.rollback()
            
            # Publish job failed event
            try:
                publisher = await get_event_publisher()
                await publisher.publish_job_failed(
                    job_id=job_id,
                    execution_id=execution.id if 'execution' in locals() else None,
                    job_name=job.job_name if 'job' in locals() else "Unknown",
                    error=str(e)
                )
            except Exception as pub_error:
                self.logger.warning(f"Failed to publish job failed event: {str(pub_error)}")
            
            self.handle_error(e, "execute_job")
    
    async def get_job_status(self, job_id: int) -> Dict[str, Any]:
        """Get job status dan recent executions."""
        try:
            self.log_operation("get_job_status", {"job_id": job_id})
            
            # Try cache first using existing infrastructure
            cache = await cache_manager.get_cache()
            if cache:
                cache_key = f"jobs:{job_id}"
                try:
                    cached = await cache.get(cache_key)
                    if cached:
                        self.logger.debug(f"Cache hit for job {job_id}")
                        return cached
                except Exception as cache_error:
                    self.logger.warning(f"Cache get error: {str(cache_error)}")
            
            # Fetch from DB
            job = self.db.get(EtlJob, job_id)
            if not job:
                raise ETLError("Job not found")
            
            # Get recent executions (10 terakhir)
            stmt = (
                select(JobExecution)
                .where(JobExecution.job_id == job_id)
                .order_by(JobExecution.created_at.desc())
                .limit(10)
            )
            executions = self.db.execute(stmt).scalars().all()
            
            latest_execution = executions[0] if executions else None
            
            result = {
                "job_id": job.id,
                "job_name": job.job_name,
                "job_type": job.job_type,
                "job_category": job.job_category,
                "is_active": job.is_active,
                "latest_execution": {
                    "execution_id": latest_execution.execution_id,
                    "status": latest_execution.status,
                    "start_time": latest_execution.start_time.isoformat() if latest_execution.start_time else None,
                    "end_time": latest_execution.end_time.isoformat() if latest_execution.end_time else None,
                    "records_processed": latest_execution.records_processed
                } if latest_execution else None,
                "recent_executions": len(executions),
                "success_rate": self._calculate_success_rate(executions)
            }
            
            # Cache result (5 minutes TTL)
            if cache:
                try:
                    await cache.set(cache_key, result, ttl=300)
                except Exception as cache_error:
                    self.logger.warning(f"Cache set error: {str(cache_error)}")
            
            return result
            
        except Exception as e:
            self.handle_error(e, "get_job_status")
    
    async def get_execution_details(self, execution_id: int) -> Dict[str, Any]:
        """Get detailed execution information."""
        try:
            self.log_operation("get_execution_details", {"execution_id": execution_id})
            
            execution = self.db_session.get(JobExecution, execution_id)
            if not execution:
                raise ETLError("Execution not found")
            
            job = self.db_session.get(EtlJob, execution.job_id)
            
            return {
                "execution_id": execution.execution_id,
                "job_id": execution.job_id,
                "job_name": job.job_name if job else None,
                "batch_id": execution.batch_id,
                "status": execution.status,
                "start_time": execution.start_time,
                "end_time": execution.end_time,
                "records_processed": execution.records_processed,
                "records_successful": execution.records_successful,
                "records_failed": execution.records_failed,
                "execution_log": execution.execution_log,
                "error_details": execution.error_details,
                "performance_metrics": execution.performance_metrics,
                "duration_seconds": self._calculate_duration(execution.start_time, execution.end_time)
            }
            
        except Exception as e:
            self.handle_error(e, "get_execution_details")
    
    async def cancel_execution(self, execution_id: int) -> bool:
        """Cancel a running execution."""
        try:
            self.log_operation("cancel_execution", {"execution_id": execution_id})
            
            execution = self.db_session.get(JobExecution, execution_id)
            if not execution:
                raise ETLError("Execution not found")
            
            if execution.status != JobStatus.RUNNING.value:
                raise ETLError("Execution is not running")
            
            execution.status = JobStatus.CANCELLED.value
            execution.end_time = get_current_timestamp()
            execution.execution_log = "Execution cancelled by user"
            
            self.db_session.commit()
            return True
            
        except Exception as e:
            self.db_session.rollback()
            self.handle_error(e, "cancel_execution")
    
    async def get_jobs_list(self, job_type: str = None, is_active: bool = None) -> List[Dict[str, Any]]:
        """Get list of ETL jobs dengan optional filtering - optimized to avoid N+1 queries."""
        try:
            self.log_operation("get_jobs_list", {"job_type": job_type, "is_active": is_active})
            
            from sqlalchemy import func
            from sqlalchemy.orm import aliased
            
            # Subquery to get latest execution per job using ROW_NUMBER window function
            latest_execution_subq = (
                select(
                    JobExecution.job_id,
                    JobExecution.id.label('execution_id'),
                    JobExecution.status,
                    JobExecution.start_time,
                    JobExecution.records_processed,
                    func.row_number().over(
                        partition_by=JobExecution.job_id,
                        order_by=JobExecution.created_at.desc()
                    ).label('rn')
                )
                .subquery()
            )
            
            # Alias for the subquery
            latest_exec = aliased(latest_execution_subq)
            
            # Main query with LEFT JOIN to get jobs with their latest execution in ONE query
            stmt = (
                select(
                    EtlJob,
                    latest_exec.c.execution_id,
                    latest_exec.c.status,
                    latest_exec.c.start_time,
                    latest_exec.c.records_processed
                )
                .outerjoin(
                    latest_exec,
                    and_(
                        EtlJob.id == latest_exec.c.job_id,
                        latest_exec.c.rn == 1  # Only get the first row (latest)
                    )
                )
            )
            
            # Apply filters
            if job_type:
                stmt = stmt.where(EtlJob.job_type == job_type)
            if is_active is not None:
                stmt = stmt.where(EtlJob.is_active == is_active)
            
            stmt = stmt.order_by(EtlJob.created_at.desc())
            
            # Execute query - this is now just ONE query instead of N+1!
            rows = self.db.execute(stmt).all()
            
            # Return empty list if no jobs found
            if not rows:
                return []
            
            # Build result from the joined data
            result = []
            for row in rows:
                job = row[0]  # EtlJob object
                exec_id = row[1]
                exec_status = row[2]
                exec_start_time = row[3]
                exec_records = row[4]
                
                result.append({
                    "job_id": job.id,
                    "job_name": job.job_name,
                    "job_type": job.job_type,
                    "job_category": job.job_category,
                    "source_type": job.source_type,
                    "target_schema": job.target_schema,
                    "target_table": job.target_table,
                    "job_config": job.job_config,
                    "is_active": job.is_active,
                    "schedule_expression": job.schedule_expression,
                    "latest_execution": {
                        "execution_id": exec_id,
                        "status": exec_status,
                        "start_time": exec_start_time,
                        "records_processed": exec_records
                    } if exec_id else None,
                    "created_at": job.created_at
                })
            
            return result
            
        except Exception as e:
            self.handle_error(e, "get_jobs_list")
    
    async def update_job(self, job_id: int, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing ETL job."""
        try:
            self.log_operation("update_job", {"job_id": job_id})
            
            job = self.db_session.get(EtlJob, job_id)
            if not job:
                raise ETLError("Job not found")
            
            # Update fields
            for key, value in job_data.items():
                if hasattr(job, key):
                    setattr(job, key, value)
            
            self.db_session.commit()
            
            return {
                "job_id": job.job_id,
                "job_name": job.job_name,
                "job_type": job.job_type,
                "is_active": job.is_active,
                "status": "updated"
            }
            
        except Exception as e:
            self.db_session.rollback()
            self.handle_error(e, "update_job")
    
    async def delete_job(self, job_id: int) -> bool:
        """Delete an ETL job."""
        try:
            self.log_operation("delete_job", {"job_id": job_id})
            
            job = self.db_session.get(EtlJob, job_id)
            if not job:
                raise ETLError("Job not found")
            
            # Check for running executions
            running_stmt = (
                select(JobExecution)
                .where(and_(
                    JobExecution.job_id == job_id,
                    JobExecution.status == JobStatus.RUNNING.value
                ))
            )
            running_executions = self.db_session.execute(running_stmt).scalars().all()
            
            if running_executions:
                raise ETLError("Cannot delete job with running executions")
            
            self.db_session.delete(job)
            self.db_session.commit()
            return True
            
        except Exception as e:
            self.db_session.rollback()
            self.handle_error(e, "delete_job")
    
    # Private helper methods
    def _generate_batch_id(self) -> str:
        """Generate unique batch ID."""
        return f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
    
    def _calculate_success_rate(self, executions: List) -> float:
        """Calculate success rate from executions."""
        if not executions:
            return 0.0
        
        successful = len([e for e in executions if e.status == JobStatus.SUCCESS.value])
        return (successful / len(executions)) * 100
    
    def _calculate_duration(self, start_time: datetime, end_time: datetime) -> Optional[int]:
        """Calculate duration in seconds."""
        if not start_time or not end_time:
            return None
        return int((end_time - start_time).total_seconds())
    