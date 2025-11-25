#!/usr/bin/env python
"""
Celery Worker Entry Point

Run with:
    celery -A worker worker --loglevel=info
    
Or with beat scheduler:
    celery -A worker worker --beat --loglevel=info
    
Or separate beat:
    celery -A worker beat --loglevel=info
"""

from app.tasks.celery_app import celery_app
from app.core.config import get_settings

# Import all tasks to register them
from app.tasks import etl_tasks
from app.tasks import cleanup_tasks
from app.tasks import monitoring_tasks

settings = get_settings()

# Configure Celery
celery_app.conf.update(
    broker_url=settings.celery_settings.broker_url,
    result_backend=settings.celery_settings.result_backend,
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=7200,  # 2 hours
    task_soft_time_limit=6900,  # 1 hour 55 minutes
    worker_prefetch_multiplier=4,
    worker_max_tasks_per_child=1000,
)

# Beat schedule (if using celery beat)
celery_app.conf.beat_schedule = {
    'cleanup-old-files': {
        'task': 'cleanup.cleanup_old_files',
        'schedule': 3600.0,  # Every hour
    },
    'cleanup-old-executions': {
        'task': 'cleanup.cleanup_old_executions',
        'schedule': 86400.0,  # Every day
    },
    'monitor-job-health': {
        'task': 'monitoring.monitor_job_health',
        'schedule': 300.0,  # Every 5 minutes
    },
}

if __name__ == '__main__':
    celery_app.start()
