"""
Celery configuration moved to infrastructure/background.

This module provides Celery app configuration and setup
as infrastructure implementation for background processing.
"""

import logging
from typing import Any, Dict, Optional

from celery import Celery
from celery.signals import (
    task_prerun,
    task_postrun,
    task_failure,
    task_success,
    worker_ready,
    worker_shutting_down,
)
from kombu import Queue, Exchange

from ...core.config import get_settings
from ...core.logging import get_logger

settings = get_settings()
logger = get_logger(__name__)

# Create Celery app instance
celery_app: Optional[Celery] = None


def create_celery_app() -> Celery:
    """
    Create and configure Celery application.
    
    Returns:
        Configured Celery application instance
    """
    app = Celery("fastapi-clean-arch")
    
    # Basic configuration
    app.conf.update(
        # Broker settings
        broker_url=settings.celery.broker_url,
        result_backend=settings.celery.result_backend,
        
        # Serialization
        task_serializer=settings.celery.task_serializer,
        accept_content=settings.celery.accept_content,
        result_serializer=settings.celery.result_serializer,
        
        # Timezone
        timezone=settings.celery.timezone,
        enable_utc=settings.celery.enable_utc,
        
        # Task execution
        task_always_eager=settings.is_testing,  # Run tasks synchronously in tests
        task_eager_propagates=settings.is_testing,
        task_store_eager_result=settings.is_testing,
        
        # Task routing
        task_routes={
            'app.interfaces.background.processors.email_processor.*': {'queue': 'email'},
            'app.interfaces.background.processors.notification_processor.*': {'queue': 'notifications'},
            'app.interfaces.background.processors.cleanup_processor.*': {'queue': 'cleanup'},
            'app.interfaces.background.processors.report_processor.*': {'queue': 'reports'},
        },
        
        # Queue configuration
        task_default_queue='default',
        task_default_exchange='default',
        task_default_exchange_type='direct',
        task_default_routing_key='default',
        
        # Worker configuration
        worker_prefetch_multiplier=1,
        task_acks_late=True,
        worker_disable_rate_limits=False,
        worker_max_tasks_per_child=1000,
        worker_max_memory_per_child=200000,  # 200MB
        
        # Result backend settings
        result_expires=3600,  # 1 hour
        result_persistent=True,
        result_compression='gzip',
        
        # Task execution settings
        task_time_limit=300,  # 5 minutes
        task_soft_time_limit=240,  # 4 minutes
        task_max_retries=3,
        task_default_retry_delay=60,  # 1 minute
        
        # Monitoring
        worker_send_task_events=True,
        task_send_sent_event=True,
        
        # Security
        worker_hijack_root_logger=False,
        worker_log_color=False,
        
        # Beat scheduler settings (for periodic tasks)
        beat_schedule={
            'cleanup-unverified-users': {
                'task': 'app.interfaces.background.processors.cleanup_processor.cleanup_unverified_users',
                'schedule': 86400.0,  # Daily
                'options': {'queue': 'cleanup'},
            },
            'cleanup-expired-tokens': {
                'task': 'app.interfaces.background.processors.cleanup_processor.cleanup_expired_tokens',
                'schedule': 3600.0,  # Hourly
                'options': {'queue': 'cleanup'},
            },
            'generate-daily-reports': {
                'task': 'app.interfaces.background.processors.report_processor.generate_daily_report',
                'schedule': 86400.0,  # Daily
                'options': {'queue': 'reports'},
            },
            'collect-system-metrics': {
                'task': 'app.interfaces.background.processors.monitoring_processor.collect_system_metrics',
                'schedule': 300.0,  # Every 5 minutes
                'options': {'queue': 'monitoring'},
            },
        },
        beat_schedule_filename='/tmp/celerybeat-schedule',
    )
    
    # Queue definitions
    app.conf.task_queues = (
        Queue('default', Exchange('default'), routing_key='default'),
        Queue('email', Exchange('email'), routing_key='email'),
        Queue('notifications', Exchange('notifications'), routing_key='notifications'),
        Queue('cleanup', Exchange('cleanup'), routing_key='cleanup'),
        Queue('reports', Exchange('reports'), routing_key='reports'),
        Queue('monitoring', Exchange('monitoring'), routing_key='monitoring'),
        Queue('high_priority', Exchange('high_priority'), routing_key='high_priority'),
        Queue('low_priority', Exchange('low_priority'), routing_key='low_priority'),
    )
    
    # Error handling configuration
    app.conf.task_reject_on_worker_lost = True
    app.conf.task_ignore_result = False
    
    # Auto-discover tasks
    app.autodiscover_tasks([
        'app.interfaces.background.processors.email_processor',
        'app.interfaces.background.processors.notification_processor',
        'app.interfaces.background.processors.cleanup_processor',
        'app.interfaces.background.processors.report_processor',
        'app.interfaces.background.processors.user_processor',
        'app.interfaces.background.processors.monitoring_processor',
    ])
    
    logger.info("Celery application configured successfully")
    return app


def get_celery_app() -> Celery:
    """
    Get or create Celery application instance.
    
    Returns:
        Celery application instance
    """
    global celery_app
    if celery_app is None:
        celery_app = create_celery_app()
    return celery_app


# Signal handlers for monitoring and logging
@task_prerun.connect
def task_prerun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None, **kwds):
    """Handle task pre-run signal."""
    logger.info(f"Task {task.name} (ID: {task_id}) started")


@task_postrun.connect
def task_postrun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None, retval=None, state=None, **kwds):
    """Handle task post-run signal."""
    logger.info(f"Task {task.name} (ID: {task_id}) completed with state: {state}")


@task_failure.connect
def task_failure_handler(sender=None, task_id=None, exception=None, traceback=None, einfo=None, **kwds):
    """Handle task failure signal."""
    logger.error(f"Task {sender.name} (ID: {task_id}) failed: {exception}")
    logger.error(f"Traceback: {traceback}")


@task_success.connect
def task_success_handler(sender=None, result=None, **kwds):
    """Handle task success signal."""
    logger.debug(f"Task {sender.name} succeeded with result: {result}")


@worker_ready.connect
def worker_ready_handler(sender=None, **kwds):
    """Handle worker ready signal."""
    logger.info(f"Celery worker {sender.hostname} is ready")


@worker_shutting_down.connect
def worker_shutting_down_handler(sender=None, **kwds):
    """Handle worker shutdown signal."""
    logger.info(f"Celery worker {sender.hostname} is shutting down")


# Utility functions for task management
def send_task_with_retry(
    task_name: str,
    args: tuple = (),
    kwargs: Optional[Dict[str, Any]] = None,
    queue: str = 'default',
    retry_policy: Optional[Dict[str, Any]] = None,
    **options
):
    """
    Send task with custom retry policy.
    
    Args:
        task_name: Name of the task to execute
        args: Task arguments
        kwargs: Task keyword arguments
        queue: Queue to send task to
        retry_policy: Custom retry policy
        **options: Additional task options
    """
    app = get_celery_app()
    
    default_retry_policy = {
        'max_retries': 3,
        'interval_start': 0,
        'interval_step': 0.2,
        'interval_max': 0.2,
    }
    
    if retry_policy:
        default_retry_policy.update(retry_policy)
    
    return app.send_task(
        task_name,
        args=args,
        kwargs=kwargs or {},
        queue=queue,
        retry_policy=default_retry_policy,
        **options
    )


def send_high_priority_task(task_name: str, args: tuple = (), kwargs: Optional[Dict[str, Any]] = None):
    """Send task to high priority queue."""
    return send_task_with_retry(
        task_name=task_name,
        args=args,
        kwargs=kwargs,
        queue='high_priority'
    )


def send_low_priority_task(task_name: str, args: tuple = (), kwargs: Optional[Dict[str, Any]] = None):
    """Send task to low priority queue."""
    return send_task_with_retry(
        task_name=task_name,
        args=args,
        kwargs=kwargs,
        queue='low_priority'
    )


def schedule_task(
    task_name: str,
    eta: Any,  # datetime or timedelta
    args: tuple = (),
    kwargs: Optional[Dict[str, Any]] = None,
    queue: str = 'default',
):
    """
    Schedule task for future execution.
    
    Args:
        task_name: Name of the task to execute
        eta: When to execute the task (datetime or timedelta from now)
        args: Task arguments
        kwargs: Task keyword arguments
        queue: Queue to send task to
    """
    app = get_celery_app()
    
    return app.send_task(
        task_name,
        args=args,
        kwargs=kwargs or {},
        queue=queue,
        eta=eta
    )


# Initialize the Celery app
celery_app = get_celery_app()

# Export for use in other modules
__all__ = [
    'celery_app',
    'create_celery_app',
    'get_celery_app',
    'send_task_with_retry',
    'send_high_priority_task',
    'send_low_priority_task',
    'schedule_task',
]