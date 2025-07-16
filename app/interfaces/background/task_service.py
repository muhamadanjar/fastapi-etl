"""
Task service untuk interface dengan background tasks.
"""
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from celery.result import AsyncResult
from app.tasks import celery_app
from app.infrastructure.tasks.tasks import email_tasks, data_sync_tasks

logger = logging.getLogger(__name__)

class TaskService:
    """Service untuk mengelola background tasks."""
    
    def __init__(self):
        self.celery_app = celery_app
    
    # Email Tasks
    def send_email_async(
        self,
        to_emails: List[str],
        subject: str,
        template_name: str,
        template_data: Dict = None,
        from_email: Optional[str] = None,
        reply_to: Optional[str] = None,
        attachments: Optional[List[Dict]] = None,
        priority: str = "normal"
    ) -> str:
        """
        Queue email untuk dikirim secara asynchronous.
        
        Returns:
            Task ID
        """
        queue = "priority" if priority == "high" else "email"
        
        task = email_tasks.send_email_async.apply_async(
            kwargs={
                "to_emails": to_emails,
                "subject": subject,
                "template_name": template_name,
                "template_data": template_data,
                "from_email": from_email,
                "reply_to": reply_to,
                "attachments": attachments
            },
            queue=queue
        )
        
        logger.info(f"Email task queued: {task.id}")
        return task.id
    
    def send_bulk_emails(self, email_batch: List[Dict]) -> str:
        """Queue bulk emails."""
        task = email_tasks.send_bulk_emails.apply_async(
            args=[email_batch],
            queue="email"
        )
        
        logger.info(f"Bulk email task queued: {task.id}")
        return task.id
    
    def send_welcome_email(self, user_id: int, user_email: str, user_name: str) -> str:
        """Send welcome email to new user."""
        task = email_tasks.send_welcome_email.apply_async(
            args=[user_id, user_email, user_name],
            queue="email"
        )
        
        logger.info(f"Welcome email task queued: {task.id}")
        return task.id
    
    def send_password_reset_email(self, user_email: str, reset_token: str, user_name: str) -> str:
        """Send password reset email."""
        task = email_tasks.send_password_reset_email.apply_async(
            args=[user_email, reset_token, user_name],
            queue="priority"  # High priority for security-related emails
        )
        
        logger.info(f"Password reset email task queued: {task.id}")
        return task.id
    
    def send_email_verification(self, user_email: str, verification_token: str, user_name: str) -> str:
        """Send email verification."""
        task = email_tasks.send_email_verification.apply_async(
            args=[user_email, verification_token, user_name],
            queue="priority"  # High priority for account verification
        )
        
        logger.info(f"Email verification task queued: {task.id}")
        return task.id
    
    def send_notification_email(
        self,
        user_emails: List[str],
        notification_type: str,
        notification_data: Dict
    ) -> str:
        """Send notification email."""
        task = email_tasks.send_notification_email.apply_async(
            args=[user_emails, notification_type, notification_data],
            queue="email"
        )
        
        logger.info(f"Notification email task queued: {task.id}")
        return task.id
    
    # Data Sync Tasks
    def cleanup_expired_sessions(self) -> str:
        """Queue session cleanup task."""
        task = data_sync_tasks.cleanup_expired_sessions.apply_async(
            queue="data_sync"
        )
        
        logger.info(f"Session cleanup task queued: {task.id}")
        return task.id
    
    def system_health_check(self) -> str:
        """Queue system health check."""
        task = data_sync_tasks.system_health_check.apply_async(
            queue="default"
        )
        
        logger.info(f"Health check task queued: {task.id}")
        return task.id
    
    def backup_data(self) -> str:
        """Queue data backup task."""
        task = data_sync_tasks.backup_data.apply_async(
            queue="data_sync"
        )
        
        logger.info(f"Data backup task queued: {task.id}")
        return task.id
    
    def sync_external_data(self, data_source: str, sync_options: Dict = None) -> str:
        """Queue external data sync."""
        task = data_sync_tasks.sync_external_data.apply_async(
            args=[data_source, sync_options],
            queue="data_sync"
        )
        
        logger.info(f"External data sync task queued: {task.id}")
        return task.id
    
    def generate_reports(self, report_type: str, date_range: Dict = None) -> str:
        """Queue report generation."""
        task = data_sync_tasks.generate_reports.apply_async(
            args=[report_type, date_range],
            queue="data_sync"
        )
        
        logger.info(f"Report generation task queued: {task.id}")
        return task.id
    
    def process_uploaded_file(
        self,
        file_path: str,
        file_type: str,
        processing_options: Dict = None
    ) -> str:
        """Queue file processing task."""
        task = data_sync_tasks.process_uploaded_file.apply_async(
            args=[file_path, file_type, processing_options],
            queue="data_sync"
        )
        
        logger.info(f"File processing task queued: {task.id}")
        return task.id
    
    def cleanup_temp_files(self) -> str:
        """Queue temp files cleanup."""
        task = data_sync_tasks.cleanup_temp_files.apply_async(
            queue="data_sync"
        )
        
        logger.info(f"Temp cleanup task queued: {task.id}")
        return task.id
    
    # Task Management
    def get_task_status(self, task_id: str) -> Dict:
        """Get task status and result."""
        try:
            task_result = AsyncResult(task_id, app=self.celery_app)
            
            status_info = {
                "task_id": task_id,
                "status": task_result.status,
                "ready": task_result.ready(),
                "successful": task_result.successful() if task_result.ready() else None,
                "failed": task_result.failed() if task_result.ready() else None,
                "result": task_result.result if task_result.ready() else None,
                "traceback": task_result.traceback if task_result.failed() else None,
                "date_done": task_result.date_done.isoformat() if task_result.date_done else None
            }
            
            # Add progress info if available
            if task_result.status == "PROGRESS":
                status_info["meta"] = task_result.info
            
            return status_info
            
        except Exception as e:
            logger.error(f"Error getting task status for {task_id}: {str(e)}")
            return {
                "task_id": task_id,
                "status": "UNKNOWN",
                "error": str(e)
            }
    
    def cancel_task(self, task_id: str) -> bool:
        """Cancel a task."""
        try:
            self.celery_app.control.revoke(task_id, terminate=True)
            logger.info(f"Task cancelled: {task_id}")
            return True
        except Exception as e:
            logger.error(f"Error cancelling task {task_id}: {str(e)}")
            return False
    
    def get_active_tasks(self) -> Dict:
        """Get list of active tasks."""
        try:
            active_tasks = self.celery_app.control.inspect().active()
            return active_tasks or {}
        except Exception as e:
            logger.error(f"Error getting active tasks: {str(e)}")
            return {}
    
    def get_scheduled_tasks(self) -> Dict:
        """Get list of scheduled tasks."""
        try:
            scheduled_tasks = self.celery_app.control.inspect().scheduled()
            return scheduled_tasks or {}
        except Exception as e:
            logger.error(f"Error getting scheduled tasks: {str(e)}")
            return {}
    
    def get_worker_stats(self) -> Dict:
        """Get worker statistics."""
        try:
            stats = self.celery_app.control.inspect().stats()
            return stats or {}
        except Exception as e:
            logger.error(f"Error getting worker stats: {str(e)}")
            return {}
    
    def ping_workers(self) -> Dict:
        """Ping all workers."""
        try:
            pong = self.celery_app.control.ping(timeout=5)
            return pong or {}
        except Exception as e:
            logger.error(f"Error pinging workers: {str(e)}")
            return {}
    
    def purge_queue(self, queue_name: str) -> int:
        """Purge all messages from a queue."""
        try:
            purged = self.celery_app.control.purge()
            logger.info(f"Purged queue {queue_name}")
            return purged
        except Exception as e:
            logger.error(f"Error purging queue {queue_name}: {str(e)}")
            return 0