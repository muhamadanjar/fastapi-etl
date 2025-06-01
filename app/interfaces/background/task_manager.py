"""
Task manager untuk high-level task orchestration.
"""
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from app.interfaces.task.task_service import TaskService

logger = logging.getLogger(__name__)

class TaskPriority(Enum):
    """Task priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"

class TaskStatus(Enum):
    """Task status types."""
    PENDING = "PENDING"
    STARTED = "STARTED"
    PROGRESS = "PROGRESS"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    RETRY = "RETRY"
    REVOKED = "REVOKED"

@dataclass
class TaskInfo:
    """Task information container."""
    task_id: str
    task_name: str
    status: str
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    progress: Optional[Dict] = None
    priority: str = TaskPriority.NORMAL.value
    queue: str = "default"
    retries: int = 0
    max_retries: int = 3

class TaskManager:
    """High-level task manager untuk orchestration."""
    
    def __init__(self):
        self.task_service = TaskService()
        self._task_registry: Dict[str, TaskInfo] = {}
    
    def _register_task(
        self,
        task_id: str,
        task_name: str,
        priority: TaskPriority = TaskPriority.NORMAL,
        queue: str = "default"
    ) -> TaskInfo:
        """Register task in internal registry."""
        task_info = TaskInfo(
            task_id=task_id,
            task_name=task_name,
            status=TaskStatus.PENDING.value,
            created_at=datetime.utcnow(),
            priority=priority.value,
            queue=queue
        )
        
        self._task_registry[task_id] = task_info
        return task_info
    
    def _update_task_info(self, task_id: str) -> Optional[TaskInfo]:
        """Update task info from Celery."""
        if task_id not in self._task_registry:
            return None
        
        task_status = self.task_service.get_task_status(task_id)
        task_info = self._task_registry[task_id]
        
        # Update status
        task_info.status = task_status.get("status", task_info.status)
        task_info.result = task_status.get("result")
        task_info.error = task_status.get("traceback")
        task_info.progress = task_status.get("meta")
        
        if task_status.get("date_done"):
            task_info.completed_at = datetime.fromisoformat(task_status["date_done"])
        
        return task_info
    
    # Email Task Management
    def queue_email(
        self,
        to_emails: List[str],
        subject: str,
        template_name: str,
        template_data: Dict = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        **kwargs
    ) -> TaskInfo:
        """Queue email with task tracking."""
        task_id = self.task_service.send_email_async(
            to_emails=to_emails,
            subject=subject,
            template_name=template_name,
            template_data=template_data,
            priority=priority.value,
            **kwargs
        )
        
        return self._register_task(
            task_id=task_id,
            task_name="send_email",
            priority=priority,
            queue="priority" if priority == TaskPriority.HIGH else "email"
        )
    
    def queue_welcome_email(
        self,
        user_id: int,
        user_email: str,
        user_name: str
    ) -> TaskInfo:
        """Queue welcome email."""
        task_id = self.task_service.send_welcome_email(user_id, user_email, user_name)
        
        return self._register_task(
            task_id=task_id,
            task_name="welcome_email",
            priority=TaskPriority.NORMAL,
            queue="email"
        )
    
    def queue_password_reset_email(
        self,
        user_email: str,
        reset_token: str,
        user_name: str
    ) -> TaskInfo:
        """Queue password reset email."""
        task_id = self.task_service.send_password_reset_email(
            user_email, reset_token, user_name
        )
        
        return self._register_task(
            task_id=task_id,
            task_name="password_reset_email",
            priority=TaskPriority.HIGH,
            queue="priority"
        )
    
    def queue_bulk_emails(self, email_batch: List[Dict]) -> TaskInfo:
        """Queue bulk emails."""
        task_id = self.task_service.send_bulk_emails(email_batch)
        
        return self._register_task(
            task_id=task_id,
            task_name="bulk_emails",
            priority=TaskPriority.NORMAL,
            queue="email"
        )
    
    # Data Management Tasks
    def queue_data_backup(self) -> TaskInfo:
        """Queue data backup."""
        task_id = self.task_service.backup_data()
        
        return self._register_task(
            task_id=task_id,
            task_name="data_backup",
            priority=TaskPriority.HIGH,
            queue="data_sync"
        )
    
    def queue_system_health_check(self) -> TaskInfo:
        """Queue system health check."""
        task_id = self.task_service.system_health_check()
        
        return self._register_task(
            task_id=task_id,
            task_name="health_check",
            priority=TaskPriority.NORMAL,
            queue="default"
        )
    
    def queue_external_sync(
        self,
        data_source: str,
        sync_options: Dict = None
    ) -> TaskInfo:
        """Queue external data sync."""
        task_id = self.task_service.sync_external_data(data_source, sync_options)
        
        return self._register_task(
            task_id=task_id,
            task_name="external_sync",
            priority=TaskPriority.NORMAL,
            queue="data_sync"
        )
    
    def queue_report_generation(
        self,
        report_type: str,
        date_range: Dict = None
    ) -> TaskInfo:
        """Queue report generation."""
        task_id = self.task_service.generate_reports(report_type, date_range)
        
        return self._register_task(
            task_id=task_id,
            task_name="generate_report",
            priority=TaskPriority.NORMAL,
            queue="data_sync"
        )
    
    def queue_file_processing(
        self,
        file_path: str,
        file_type: str,
        processing_options: Dict = None
    ) -> TaskInfo:
        """Queue file processing."""
        task_id = self.task_service.process_uploaded_file(
            file_path, file_type, processing_options
        )
        
        return self._register_task(
            task_id=task_id,
            task_name="process_file",
            priority=TaskPriority.NORMAL,
            queue="data_sync"
        )
    
    # Task Management
    def get_task_info(self, task_id: str) -> Optional[TaskInfo]:
        """Get task information."""
        if task_id in self._task_registry:
            return self._update_task_info(task_id)
        
        # Try to get from Celery directly
        task_status = self.task_service.get_task_status(task_id)
        if task_status.get("status") != "UNKNOWN":
            # Create minimal task info
            task_info = TaskInfo(
                task_id=task_id,
                task_name="unknown",
                status=task_status.get("status", "UNKNOWN"),
                created_at=datetime.utcnow(),
                result=task_status.get("result"),
                error=task_status.get("traceback")
            )
            return task_info
        
        return None
    
    def get_all_tasks(self, status_filter: Optional[str] = None) -> List[TaskInfo]:
        """Get all tracked tasks."""
        tasks = []
        for task_id in self._task_registry:
            task_info = self._update_task_info(task_id)
            if task_info and (not status_filter or task_info.status == status_filter):
                tasks.append(task_info)
        
        return sorted(tasks, key=lambda x: x.created_at, reverse=True)
    
    def cancel_task(self, task_id: str) -> bool:
        """Cancel a task."""
        success = self.task_service.cancel_task(task_id)
        
        if success and task_id in self._task_registry:
            self._task_registry[task_id].status = TaskStatus.REVOKED.value
            self._task_registry[task_id].completed_at = datetime.utcnow()
        
        return success
    
    def retry_failed_tasks(self, max_age_hours: int = 24) -> List[str]:
        """Retry failed tasks within time window."""
        cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)
        retried_tasks = []
        
        for task_id, task_info in self._task_registry.items():
            if (task_info.status == TaskStatus.FAILURE.value and 
                task_info.created_at > cutoff_time and
                task_info.retries < task_info.max_retries):
                
                # Implement retry logic based on task type
                if task_info.task_name == "send_email":
                    # Re-queue email task
                    # This would need access to original parameters
                    pass
                
                retried_tasks.append(task_id)
        
        return retried_tasks
    
    def cleanup_old_tasks(self, max_age_days: int = 7):
        """Clean up old task records."""
        cutoff_time = datetime.utcnow() - timedelta(days=max_age_days)
        
        to_remove = [
            task_id for task_id, task_info in self._task_registry.items()
            if task_info.created_at < cutoff_time
        ]
        
        for task_id in to_remove:
            del self._task_registry[task_id]
        
        logger.info(f"Cleaned up {len(to_remove)} old task records")
    
    def get_queue_stats(self) -> Dict:
        """Get queue statistics."""
        active_tasks = self.task_service.get_active_tasks()
        scheduled_tasks = self.task_service.get_scheduled_tasks()
        worker_stats = self.task_service.get_worker_stats()
        
        # Count tasks by queue
        queue_counts = {}
        for task_id, task_info in self._task_registry.items():
            queue = task_info.queue
            if queue not in queue_counts:
                queue_counts[queue] = {"pending": 0, "processing": 0, "completed": 0, "failed": 0}
            
            if task_info.status in [TaskStatus.PENDING.value, TaskStatus.STARTED.value]:
                queue_counts[queue]["processing"] += 1
            elif task_info.status == TaskStatus.SUCCESS.value:
                queue_counts[queue]["completed"] += 1
            elif task_info.status == TaskStatus.FAILURE.value:
                queue_counts[queue]["failed"] += 1
        
        return {
            "queue_counts": queue_counts,
            "active_tasks": active_tasks,
            "scheduled_tasks": scheduled_tasks,
            "worker_stats": worker_stats
        }
    
    def get_task_history(
        self,
        limit: int = 100,
        task_type: Optional[str] = None
    ) -> List[TaskInfo]:
        """Get task execution history."""
        tasks = self.get_all_tasks()
        
        if task_type:
            tasks = [t for t in tasks if t.task_name == task_type]
        
        return tasks[:limit]