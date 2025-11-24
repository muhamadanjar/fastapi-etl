"""
Event handlers for processing published events.
"""

import asyncio
from typing import Dict, Any, Callable
from datetime import datetime

from app.models.events import Event, EventType
from app.utils.logger import get_logger
from app.services.notification_service import NotificationService
from app.infrastructure.db.connection import get_session

logger = get_logger(__name__)


class EventHandler:
    """Base event handler"""
    
    def __init__(self):
        self.logger = logger
        self.handlers: Dict[EventType, list[Callable]] = {}
    
    def register(self, event_type: EventType, handler: Callable):
        """Register a handler for an event type"""
        if event_type not in self.handlers:
            self.handlers[event_type] = []
        self.handlers[event_type].append(handler)
        self.logger.info(f"Registered handler for {event_type.value}")
    
    async def handle(self, event: Event):
        """Handle an event by calling all registered handlers"""
        try:
            if event.event_type in self.handlers:
                for handler in self.handlers[event.event_type]:
                    try:
                        await handler(event)
                    except Exception as e:
                        self.logger.error(f"Handler error for {event.event_type.value}: {str(e)}")
            else:
                self.logger.debug(f"No handlers registered for {event.event_type.value}")
        except Exception as e:
            self.logger.error(f"Error handling event: {str(e)}")


# Event handler functions

async def handle_job_completed(event: Event):
    """Handle job completed event - send notification"""
    try:
        job_id = event.data.get("job_id")
        job_name = event.data.get("job_name")
        stats = event.data.get("stats", {})
        
        message = (
            f"✅ Job '{job_name}' completed successfully\n"
            f"Records processed: {stats.get('records_processed', 0)}\n"
            f"Duration: {stats.get('duration_seconds', 0)}s"
        )
        
        # Send notification (email, Slack, etc.)
        with get_session() as db:
            notification_service = NotificationService(db)
            await notification_service.send_notification(
                title=f"Job Completed: {job_name}",
                message=message,
                notification_type="JOB_COMPLETION",
                severity="INFO"
            )
        
        logger.info(f"Sent completion notification for job {job_id}")
        
    except Exception as e:
        logger.error(f"Error handling job completed event: {str(e)}")


async def handle_job_failed(event: Event):
    """Handle job failed event - send alert"""
    try:
        job_id = event.data.get("job_id")
        job_name = event.data.get("job_name")
        error = event.data.get("error")
        
        message = (
            f"❌ Job '{job_name}' failed\n"
            f"Error: {error}\n"
            f"Job ID: {job_id}"
        )
        
        # Send alert notification
        with get_session() as db:
            notification_service = NotificationService(db)
            await notification_service.send_notification(
                title=f"Job Failed: {job_name}",
                message=message,
                notification_type="JOB_FAILURE",
                severity="ERROR"
            )
        
        logger.info(f"Sent failure alert for job {job_id}")
        
    except Exception as e:
        logger.error(f"Error handling job failed event: {str(e)}")


async def handle_quality_check_failed(event: Event):
    """Handle quality check failed event - send alert"""
    try:
        execution_id = event.data.get("execution_id")
        check_name = event.data.get("check_name")
        reason = event.data.get("reason")
        
        message = (
            f"⚠️ Quality check '{check_name}' failed\n"
            f"Reason: {reason}\n"
            f"Execution ID: {execution_id}"
        )
        
        # Send alert
        with get_session() as db:
            notification_service = NotificationService(db)
            await notification_service.send_notification(
                title=f"Quality Check Failed: {check_name}",
                message=message,
                notification_type="QUALITY_CHECK_FAILURE",
                severity="WARNING"
            )
        
        logger.info(f"Sent quality check failure alert for execution {execution_id}")
        
    except Exception as e:
        logger.error(f"Error handling quality check failed event: {str(e)}")


async def handle_dependency_met(event: Event):
    """Handle dependency met event - log and notify"""
    try:
        job_id = event.data.get("job_id")
        dependency_id = event.data.get("dependency_id")
        
        logger.info(f"Dependency {dependency_id} met for job {job_id}")
        
        # Could trigger dependent job execution here
        # or send notification to interested parties
        
    except Exception as e:
        logger.error(f"Error handling dependency met event: {str(e)}")


async def handle_file_processed(event: Event):
    """Handle file processed event - log statistics"""
    try:
        file_id = event.data.get("file_id")
        filename = event.data.get("filename")
        records = event.data.get("records_processed", 0)
        
        logger.info(f"File '{filename}' processed: {records} records")
        
        # Could update dashboard metrics here
        
    except Exception as e:
        logger.error(f"Error handling file processed event: {str(e)}")


async def handle_error_occurred(event: Event):
    """Handle error event - send alert for critical errors"""
    try:
        error_message = event.data.get("error_message")
        error_type = event.data.get("error_type")
        severity = event.data.get("severity")
        
        # Only send alerts for high/critical errors
        if severity in ["HIGH", "CRITICAL"]:
            message = (
                f"🚨 {severity} Error Occurred\n"
                f"Type: {error_type}\n"
                f"Message: {error_message}"
            )
            
            with get_session() as db:
                notification_service = NotificationService(db)
                await notification_service.send_notification(
                    title=f"{severity} Error: {error_type}",
                    message=message,
                    notification_type="ERROR_ALERT",
                    severity=severity
                )
            
            logger.info(f"Sent error alert for {error_type}")
        
    except Exception as e:
        logger.error(f"Error handling error event: {str(e)}")


async def handle_system_alert(event: Event):
    """Handle system alert event"""
    try:
        alert_message = event.data.get("alert_message")
        alert_type = event.data.get("alert_type")
        severity = event.data.get("severity", "MEDIUM")
        
        message = f"🔔 System Alert: {alert_message}"
        
        with get_session() as db:
            notification_service = NotificationService(db)
            await notification_service.send_notification(
                title=f"System Alert: {alert_type}",
                message=message,
                notification_type="SYSTEM_ALERT",
                severity=severity
            )
        
        logger.info(f"Sent system alert: {alert_type}")
        
    except Exception as e:
        logger.error(f"Error handling system alert event: {str(e)}")


# Initialize default event handler
default_event_handler = EventHandler()

# Register default handlers
default_event_handler.register(EventType.JOB_COMPLETED, handle_job_completed)
default_event_handler.register(EventType.JOB_FAILED, handle_job_failed)
default_event_handler.register(EventType.QUALITY_CHECK_FAILED, handle_quality_check_failed)
default_event_handler.register(EventType.DEPENDENCY_MET, handle_dependency_met)
default_event_handler.register(EventType.FILE_PROCESSED, handle_file_processed)
default_event_handler.register(EventType.ERROR_OCCURRED, handle_error_occurred)
default_event_handler.register(EventType.SYSTEM_ALERT, handle_system_alert)
