"""
Celery application configuration.
"""
import logging
from celery import Celery
from kombu import Queue
from app.core.config import get_settings


settings = get_settings()

logger = logging.getLogger(__name__)

# Create Celery instance
celery_app = Celery("fastapi-clean-arch")

# Celery configuration
celery_config = {
    # Broker settings
    "broker_url": settings.CELERY_BROKER_URL,
    "result_backend": settings.CELERY_RESULT_BACKEND,
    
    # Task settings
    "task_serializer": "json",
    "accept_content": ["json"],
    "result_serializer": "json",
    "timezone": settings.TIMEZONE,
    "enable_utc": True,
    
    # Worker settings
    "worker_prefetch_multiplier": 1,
    "task_acks_late": True,
    "worker_max_tasks_per_child": 1000,
    
    # Task routing
    "task_routes": {
        "app.infrastructure.tasks.tasks.email_tasks.*": {"queue": "email"},
        "app.infrastructure.tasks.tasks.data_sync_tasks.*": {"queue": "data_sync"},
        "app.infrastructure.tasks.tasks.*": {"queue": "default"},
    },
    
    # Queue definitions
    "task_default_queue": "default",
    "task_queues": (
        Queue("default", routing_key="default"),
        Queue("email", routing_key="email"),
        Queue("data_sync", routing_key="data_sync"),
        Queue("priority", routing_key="priority"),
    ),
    
    # Result backend settings
    "result_expires": 3600,  # 1 hour
    "result_backend_transport_options": {
        "master_name": "mymaster" if "sentinel" in settings.CELERY_RESULT_BACKEND else None,
    } if settings.CELERY_RESULT_BACKEND.startswith("redis") else {},
    
    # Task execution settings
    "task_soft_time_limit": 300,  # 5 minutes
    "task_time_limit": 600,       # 10 minutes
    "task_max_retries": 3,
    "task_default_retry_delay": 60,
    
    # Monitoring
    "worker_send_task_events": True,
    "task_send_sent_event": True,
    
    # Beat settings
    "beat_schedule": {},  # Will be populated by scheduler
    "beat_scheduler": "django_celery_beat.schedulers:DatabaseSchedule" if settings.USE_CELERY_BEAT_DB else "celery.beat:PersistentScheduler",
}

# Apply configuration
celery_app.conf.update(celery_config)

# Auto-discover tasks
celery_app.autodiscover_tasks([
    "app.interfaces.background.tasks"
])

# Error handling
@celery_app.task(bind=True)
def debug_task(self):
    """Debug task untuk testing Celery."""
    print(f"Request: {self.request!r}")
    return "Debug task completed"

# Task failure callback
@celery_app.task(bind=True)
def task_failure_handler(self, task_id, error, traceback):
    """Handle task failures."""
    logger.error(f"Task {task_id} failed: {error}")
    logger.error(f"Traceback: {traceback}")

# Task success callback
def task_success_handler(sender=None, headers=None, body=None, **kwargs):
    """Handle task success."""
    logger.info(f"Task {sender} completed successfully")

# Task retry callback
def task_retry_handler(sender=None, task_id=None, reason=None, einfo=None, **kwargs):
    """Handle task retries."""
    logger.warning(f"Task {task_id} retrying: {reason}")

# Connect signals
from celery.signals import task_success, task_retry, task_failure

task_success.connect(task_success_handler)
task_retry.connect(task_retry_handler)
task_failure.connect(task_failure_handler)

if __name__ == "__main__":
    celery_app.start()