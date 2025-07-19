"""
ETL service for orchestrating extraction, transformation, and loading operations.
"""

import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import select, and_
from app.services.base import BaseService
from app.core.exceptions import ETLError, ServiceError
from app.core.enums import JobStatus, JobType
from app.utils.date_utils import get_current_timestamp
from app.tasks.etl_tasks import execute_etl_job


class ETLService(BaseService):
    """Service for orchestrating ETL operations."""
    
    def __init__(self, db_session: Session):
        super().__init__(db_session)
    
    def get_service_name(self) -> str:
        return "ETLService"
    
    async def create_etl_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new ETL job."""
        try:
            self.validate_input(job_data, ["job_name", "job_type", "source_type"])
            self.log_operation("create_etl_job", {"job_name": job_data["job_name"]})
            
            # Create job record
            job = await self._create_job_record({
                "job_name": job_data["job_name"],
                "job_type": job_data["job_type"],
                "job_category": job_data.get("job_category", "GENERAL"),
                "source_type": job_data["source_type"],
                "target_schema": job_data.get("target_schema"),
                "target_table": job_data.get("target_table"),
                "job_config": job_data.get("job_config", {}),
                "schedule_expression": job_data.get("schedule_expression"),
                "is_active": job_data.get("is_active", True),
                "created_at": get_current_timestamp()
            })
            
            return {
                "job_id": job.job_id,
                "job_name": job.job_name,
                "job_type": job.job_type,
                "status": "created"
            }
            
        except Exception as e:
            self.handle_error(e, "create_etl_job")
    
    async def execute_job(self, job_id: int, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute an ETL job."""
        try:
            self.log_operation("execute_job", {"job_id": job_id})
            
            # Get job details
            job = await self._get_job_by_id(job_id)
            if not job:
                raise ETLError("Job not found")
            
            if not job.is_active:
                raise ETLError("Job is not active")
            
            # Generate batch ID
            batch_id = self._generate_batch_id()
            
            # Create execution record
            execution = await self._create_execution_record({
                "job_id": job_id,
                "batch_id": batch_id,
                "start_time": get_current_timestamp(),
                "status": JobStatus.RUNNING.value,
                "execution_log": "",
                "performance_metrics": {},
                "created_at": get_current_timestamp()
            })
            
            # Execute job asynchronously
            task_result = execute_etl_job.delay(
                job_id=job_id,
                execution_id=execution.execution_id,
                batch_id=batch_id,
                parameters=parameters or {}
            )
            
            return {
                "execution_id": execution.execution_id,
                "job_id": job_id,
                "batch_id": batch_id,
                "status": "started",
                "task_id": task_result.id
            }
            
        except Exception as e:
            self.handle_error(e, "execute_job")
    
    async def get_job_status(self, job_id: int) -> Dict[str, Any]:
        """Get job status and recent executions."""
        try:
            self.log_operation("get_job_status", {"job_id": job_id})
            
            # Get job details
            job = await self._get_job_by_id(job_id)
            if not job:
                raise ETLError("Job not found")
            
            # Get recent executions
            executions = await self._get_job_executions(job_id, limit=10)
            
            # Get latest execution
            latest_execution = executions[0] if executions else None
            
            return {
                "job_id": job.job_id,
                "job_name": job.job_name,
                "job_type": job.job_type,
                "job_category": job.job_category,
                "is_active": job.is_active,
                "latest_execution": {
                    "execution_id": latest_execution.execution_id if latest_execution else None,
                    "status": latest_execution.status if latest_execution else None,
                    "start_time": latest_execution.start_time if latest_execution else None,
                    "end_time": latest_execution.end_time if latest_execution else None,
                    "records_processed": latest_execution.records_processed if latest_execution else 0
                } if latest_execution else None,
                "recent_executions": len(executions),
                "success_rate": self._calculate_success_rate(executions)
            }
            
        except Exception as e:
            self.handle_error(e, "get_job_status")
    
    async def get_execution_details(self, execution_id: int) -> Dict[str, Any]:
        """Get detailed execution information."""
        try:
            self.log_operation("get_execution_details", {"execution_id": execution_id})
            
            execution = await self._get_execution_by_id(execution_id)
            if not execution:
                raise ETLError("Execution not found")
            
            # Get job details
            job = await self._get_job_by_id(execution.job_id)
            
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
            
            execution = await self._get_execution_by_id(execution_id)
            if not execution:
                raise ETLError("Execution not found")
            
            if execution.status != JobStatus.RUNNING.value:
                raise ETLError("Execution is not running")
            
            # Update execution status
            await self._update_execution_status(
                execution_id,
                JobStatus.CANCELLED.value,
                get_current_timestamp(),
                "Execution cancelled by user"
            )
            
            return True
            
        except Exception as e:
            self.handle_error(e, "cancel_execution")
    
    async def get_jobs_list(self, job_type: str = None, is_active: bool = None) -> List[Dict[str, Any]]:
        """Get list of ETL jobs with optional filtering."""
        try:
            self.log_operation("get_jobs_list", {"job_type": job_type, "is_active": is_active})
            
            jobs = await self._get_jobs_list(job_type, is_active)
            
            result = []
            for job in jobs:
                # Get latest execution
                latest_execution = await self._get_latest_execution(job.job_id)
                
                result.append({
                    "job_id": job.job_id,
                    "job_name": job.job_name,
                    "job_type": job.job_type,
                    "job_category": job.job_category,
                    "source_type": job.source_type,
                    "is_active": job.is_active,
                    "schedule_expression": job.schedule_expression,
                    "latest_execution": {
                        "execution_id": latest_execution.execution_id if latest_execution else None,
                        "status": latest_execution.status if latest_execution else None,
                        "start_time": latest_execution.start_time if latest_execution else None,
                        "records_processed": latest_execution.records_processed if latest_execution else 0
                    } if latest_execution else None,
                    "created_at": job.created_at
                })
            
            return result
            
        except Exception as e:
            self.handle_error(e, "get_jobs_list")
    
    async def update_job(self, job_id: int, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing ETL job."""
        try:
            self.log_operation("update_job", {"job_id": job_id})
            
            job = await self._get_job_by_id(job_id)
            if not job:
                raise ETLError("Job not found")
            
            # Update job record
            updated_job = await self._update_job_record(job_id, job_data)
            
            return {
                "job_id": updated_job.job_id,
                "job_name": updated_job.job_name,
                "job_type": updated_job.job_type,
                "is_active": updated_job.is_active,
                "status": "updated"
            }
            
        except Exception as e:
            self.handle_error(e, "update_job")
    
    async def delete_job(self, job_id: int) -> bool:
        """Delete an ETL job and its executions."""
        try:
            self.log_operation("delete_job", {"job_id": job_id})
            
            job = await self._get_job_by_id(job_id)
            if not job:
                raise ETLError("Job not found")
            
            # Check for running executions
            running_executions = await self._get_running_executions(job_id)
            if running_executions:
                raise ETLError("Cannot delete job with running executions")
            
            # Delete executions
            await self._delete_job_executions(job_id)
            
            # Delete job
            await self._delete_job_record(job_id)
            
            return True
            
        except Exception as e:
            self.handle_error(e, "delete_job")
    
    async def get_batch_status(self, batch_id: str) -> Dict[str, Any]:
        """Get status of all operations in a batch."""
        try:
            self.log_operation("get_batch_status", {"batch_id": batch_id})
            
            # Get all executions in batch
            executions = await self._get_batch_executions(batch_id)
            
            if not executions:
                raise ETLError("Batch not found")
            
            # Calculate batch statistics
            total_executions = len(executions)
            completed_executions = len([e for e in executions if e.status == JobStatus.SUCCESS.value])
            failed_executions = len([e for e in executions if e.status == JobStatus.FAILED.value])
            running_executions = len([e for e in executions if e.status == JobStatus.RUNNING.value])
            
            total_records = sum(e.records_processed or 0 for e in executions)
            successful_records = sum(e.records_successful or 0 for e in executions)
            failed_records = sum(e.records_failed or 0 for e in executions)
            
            return {
                "batch_id": batch_id,
                "total_executions": total_executions,
                "completed_executions": completed_executions,
                "failed_executions": failed_executions,
                "running_executions": running_executions,
                "total_records": total_records,
                "successful_records": successful_records,
                "failed_records": failed_records,
                "success_rate": (successful_records / total_records * 100) if total_records > 0 else 0,
                "executions": [{
                    "execution_id": e.execution_id,
                    "job_id": e.job_id,
                    "status": e.status,
                    "start_time": e.start_time,
                    "end_time": e.end_time,
                    "records_processed": e.records_processed
                } for e in executions]
            }
            
        except Exception as e:
            self.handle_error(e, "get_batch_status")
    
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
    
    # Database helper methods (implement based on your models)
    async def _create_job_record(self, job_data: Dict[str, Any]):
        """Create job record in database."""
        # Implement database insert
        pass
    
    async def _get_job_by_id(self, job_id: int):
        """Get job by ID."""
        # Implement database query
        pass
    
    async def _create_execution_record(self, execution_data: Dict[str, Any]):
        """Create execution record in database."""
        # Implement database insert
        pass
    
    async def _get_execution_by_id(self, execution_id: int):
        """Get execution by ID."""
        # Implement database query
        pass
    
    async def _get_job_executions(self, job_id: int, limit: int = None):
        """Get job executions with optional limit."""
        # Implement database query
        pass
    
    async def _get_latest_execution(self, job_id: int):
        """Get latest execution for job."""
        # Implement database query
        pass
    
    async def _update_execution_status(self, execution_id: int, status: str, end_time: datetime, log_message: str):
        """Update execution status."""
        # Implement database update
        pass
    
    async def _get_jobs_list(self, job_type: str = None, is_active: bool = None):
        """Get jobs list with filters."""
        # Implement database query
        pass
    
    async def _update_job_record(self, job_id: int, job_data: Dict[str, Any]):
        """Update job record."""
        # Implement database update
        pass
    
    async def _delete_job_record(self, job_id: int):
        """Delete job record."""
        # Implement database delete
        pass
    
    async def _delete_job_executions(self, job_id: int):
        """Delete all executions for job."""
        # Implement database delete
        pass
    
    async def _get_running_executions(self, job_id: int):
        """Get running executions for job."""
        # Implement database query
        pass
    
    async def _get_batch_executions(self, batch_id: str):
        """Get all executions in batch."""
        # Implement database query
        pass
                