# ==============================================
# app/tasks/celery_app.py
# ==============================================
import os
from celery import Celery
from celery.schedules import crontab
from kombu import Queue
from datetime import datetime, timedelta

from app.core.config import settings

# Create Celery instance
celery_app = Celery(
    "etl_worker",
    broker=settings.celery_settings.broker_url,
    backend=settings.celery_settings.result_backend,
    include=[
        'app.tasks.etl_tasks',
        'app.tasks.monitoring_tasks', 
        'app.tasks.cleanup_tasks'
    ]
)

# Celery configuration
celery_app.conf.update(
    # Task routing and queues
    task_routes={
        'app.tasks.etl_tasks.*': {'queue': 'etl'},
        'app.tasks.monitoring_tasks.*': {'queue': 'monitoring'},
        'app.tasks.cleanup_tasks.*': {'queue': 'cleanup'},
    },
    
    # Define queues with priorities
    task_queues=(
        Queue('etl', routing_key='etl', priority=1),
        Queue('monitoring', routing_key='monitoring', priority=2),
        Queue('cleanup', routing_key='cleanup', priority=3),
        Queue('default', routing_key='default', priority=4),
    ),
    
    # Task execution settings
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Jakarta',
    enable_utc=True,
    
    # Task result settings
    result_expires=timedelta(days=7),  # Keep results for 7 days
    result_persistent=True,
    result_compression='gzip',
    
    # Task execution limits
    task_time_limit=3600,  # 1 hour hard limit
    task_soft_time_limit=3000,  # 50 minutes soft limit
    worker_max_tasks_per_child=1000,  # Restart worker after 1000 tasks
    
    # Worker settings
    worker_prefetch_multiplier=1,  # Only prefetch 1 task at a time
    worker_max_memory_per_child=512000,  # 512MB memory limit per worker
    worker_disable_rate_limits=False,
    
    # Task acknowledgment
    task_acks_late=True,  # Acknowledge tasks after completion
    # worker_prefetch_multiplier=1,
    
    # Task retry settings
    task_reject_on_worker_lost=True,
    task_default_retry_delay=60,  # 1 minute default retry delay
    task_max_retries=3,
    
    # Monitoring and logging
    worker_send_task_events=True,
    task_send_sent_event=True,
    worker_log_format='[%(asctime)s: %(levelname)s/%(processName)s] %(message)s',
    worker_task_log_format='[%(asctime)s: %(levelname)s/%(processName)s][%(task_name)s(%(task_id)s)] %(message)s',
    
    # Security settings
    worker_hijack_root_logger=False,
    worker_log_color=False,
    
    # Database connection pooling
    broker_connection_retry_on_startup=True,
    broker_connection_retry=True,
    broker_connection_max_retries=10,
    
    # Result backend settings
    result_backend_transport_options={
        'master_name': 'mymaster',
        'retry_on_timeout': True,
        'socket_keepalive': True,
        'socket_keepalive_options': {
            'TCP_KEEPIDLE': 1,
            'TCP_KEEPINTVL': 3,
            'TCP_KEEPCNT': 5,
        },
    },
    
    # Task compression
    task_compression='gzip',
    # result_compression='gzip',
    
    # Beat schedule for periodic tasks
    beat_schedule={
        # Health monitoring every 5 minutes
        'health-check': {
            'task': 'app.tasks.monitoring_tasks.health_check_task',
            'schedule': crontab(minute='*/5'),
            'options': {'queue': 'monitoring', 'priority': 5}
        },
        
        # Collect system metrics every hour
        'collect-metrics': {
            'task': 'app.tasks.monitoring_tasks.collect_system_metrics',
            'schedule': crontab(minute=0),
            'options': {'queue': 'monitoring', 'priority': 6}
        },
        
        # Check job status every 10 minutes
        'check-job-status': {
            'task': 'app.tasks.monitoring_tasks.check_job_status',
            'schedule': crontab(minute='*/10'),
            'options': {'queue': 'monitoring', 'priority': 7}
        },
        
        # Cleanup temporary files daily at 2 AM
        'cleanup-temp-files': {
            'task': 'app.tasks.cleanup_tasks.cleanup_temporary_files',
            'schedule': crontab(hour=2, minute=0),
            'options': {'queue': 'cleanup', 'priority': 10}
        },
        
        # Cleanup old log files daily at 2:30 AM
        'cleanup-logs': {
            'task': 'app.tasks.monitoring_tasks.cleanup_old_logs',
            'schedule': crontab(hour=2, minute=30),
            'options': {'queue': 'cleanup', 'priority': 10}
        },
        
        # Backup processed data daily at 3 AM
        'backup-data': {
            'task': 'app.tasks.etl_tasks.backup_processed_data',
            'schedule': crontab(hour=3, minute=0),
            'options': {'queue': 'etl', 'priority': 9}
        },
        
        # Archive old data weekly on Sunday at 1 AM
        'archive-old-data': {
            'task': 'app.tasks.cleanup_tasks.archive_old_data',
            'schedule': crontab(hour=1, minute=0, day_of_week=0),
            'options': {'queue': 'cleanup', 'priority': 11}
        },
        
        # Generate performance report weekly on Monday at 6 AM
        'performance-report': {
            'task': 'app.tasks.monitoring_tasks.generate_performance_report',
            'schedule': crontab(hour=6, minute=0, day_of_week=1),
            'options': {'queue': 'monitoring', 'priority': 8}
        },
        
        # Optimize database monthly on 1st day at 2 AM
        'optimize-database': {
            'task': 'app.tasks.cleanup_tasks.optimize_database',
            'schedule': crontab(hour=2, minute=0, day_of_month=1),
            'options': {'queue': 'cleanup', 'priority': 12}
        },
        
        # Vacuum database monthly on 1st day at 3 AM
        'vacuum-database': {
            'task': 'app.tasks.cleanup_tasks.vacuum_database',
            'schedule': crontab(hour=3, minute=0, day_of_month=1),
            'options': {'queue': 'cleanup', 'priority': 15}
        },
        
        # Purge expired records weekly on Saturday at 11 PM
        'purge-expired-records': {
            'task': 'app.tasks.cleanup_tasks.purge_expired_records',
            'schedule': crontab(hour=23, minute=0, day_of_week=6),
            'options': {'queue': 'cleanup', 'priority': 11}
        },
        
        # Cleanup failed jobs daily at 4 AM
        'cleanup-failed-jobs': {
            'task': 'app.tasks.cleanup_tasks.cleanup_failed_jobs',
            'schedule': crontab(hour=4, minute=0),
            'options': {'queue': 'cleanup', 'priority': 10}
        },
        
        # Send alert notifications every 30 minutes
        'send-alerts': {
            'task': 'app.tasks.monitoring_tasks.send_alert_notifications',
            'schedule': crontab(minute='*/30'),
            'options': {'queue': 'monitoring', 'priority': 5}
        },
    },
    
    # Beat scheduler settings
    # beat_scheduler='django_celery_beat.schedulers:DatabaseScheduler' if settings.USE_DJANGO_CELERY_BEAT else 'celery.beat:PersistentScheduler',
    beat_schedule_filename='celerybeat-schedule',
    
    # Error handling
    task_annotations={
        '*': {
            'rate_limit': '100/m',  # 100 tasks per minute max
        },
        'app.tasks.etl_tasks.process_file_task': {
            'rate_limit': '10/m',  # Limit file processing to 10/minute
            'time_limit': 1800,    # 30 minutes for file processing
        },
        'app.tasks.etl_tasks.execute_etl_job': {
            'rate_limit': '5/m',   # Limit ETL jobs to 5/minute
            'time_limit': 3600,    # 1 hour for ETL jobs
        },
        'app.tasks.monitoring_tasks.collect_system_metrics': {
            'rate_limit': '1/m',   # Once per minute max
        },
        'app.tasks.cleanup_tasks.optimize_database': {
            'rate_limit': '1/h',   # Once per hour max
            'time_limit': 7200,    # 2 hours for database optimization
        },
    },
)

# Configuration based on environment
if settings.ENVIRONMENT == 'development':
    # Development settings
    celery_app.conf.update(
        task_always_eager=False,  # Set to True to run tasks synchronously in development
        task_eager_propagates=True,
        worker_log_level='DEBUG',
        worker_concurrency=2,  # Fewer workers in development
    )
elif settings.ENVIRONMENT == 'production':
    # Production settings
    celery_app.conf.update(
        task_always_eager=False,
        worker_log_level='INFO',
        worker_concurrency=4,  # More workers in production
        worker_pool='prefork',
        worker_max_tasks_per_child=500,  # More conservative in production
    )
elif settings.ENVIRONMENT == 'testing':
    # Testing settings
    celery_app.conf.update(
        task_always_eager=True,  # Run tasks synchronously in tests
        task_eager_propagates=True,
        broker_url='memory://',
        result_backend='cache+memory://',
    )

# Custom task base class
class BaseTask(celery_app.Task):
    """Base task class with enhanced error handling and logging"""
    
    def on_success(self, retval, task_id, args, kwargs):
        """Called when task succeeds"""
        from app.utils.logger import get_logger
        logger = get_logger(self.name)
        logger.info(f"Task {self.name}[{task_id}] succeeded with result: {retval}")
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Called when task fails"""
        from app.utils.logger import get_logger
        logger = get_logger(self.name)
        logger.error(f"Task {self.name}[{task_id}] failed: {exc}")
        
        # Send failure notification if configured
        if settings.SEND_TASK_FAILURE_NOTIFICATIONS:
            self.send_failure_notification(exc, task_id, args, kwargs)
    
    def on_retry(self, exc, task_id, args, kwargs, einfo):
        """Called when task is retried"""
        from app.utils.logger import get_logger
        logger = get_logger(self.name)
        logger.warning(f"Task {self.name}[{task_id}] retry: {exc}")
    
    def send_failure_notification(self, exc, task_id, args, kwargs):
        """Send notification about task failure"""
        try:
            # This could send email, Slack notification, etc.
            notification_data = {
                'task_name': self.name,
                'task_id': task_id,
                'error': str(exc),
                'args': args,
                'kwargs': kwargs,
                'timestamp': str(datetime.utcnow())
            }
            
            # Import here to avoid circular imports
            from app.tasks.monitoring_tasks import send_alert_notifications
            send_alert_notifications.delay(
                alert_type='task_failure',
                data=notification_data
            )
        except Exception as e:
            from app.utils.logger import get_logger
            logger = get_logger(self.name)
            logger.error(f"Failed to send failure notification: {e}")

# Set the custom base task
celery_app.Task = BaseTask

# Signal handlers
@celery_app.task(bind=True)
def debug_task(self):
    """Debug task for testing Celery functionality"""
    print(f'Request: {self.request!r}')
    return 'Debug task completed successfully'

# Celery signals
from celery.signals import worker_ready, worker_shutdown, task_prerun, task_postrun

@worker_ready.connect
def worker_ready_handler(sender=None, **kwargs):
    """Called when worker is ready"""
    from app.utils.logger import get_logger
    logger = get_logger('celery.worker')
    logger.info(f"Worker {sender} is ready")

@worker_shutdown.connect  
def worker_shutdown_handler(sender=None, **kwargs):
    """Called when worker is shutting down"""
    from app.utils.logger import get_logger
    logger = get_logger('celery.worker')
    logger.info(f"Worker {sender} is shutting down")

@task_prerun.connect
def task_prerun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None, **kwds):
    """Called before task execution"""
    from app.utils.logger import get_logger
    logger = get_logger('celery.task')
    logger.debug(f"Task {task.name}[{task_id}] starting")

@task_postrun.connect
def task_postrun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None, retval=None, state=None, **kwds):
    """Called after task execution"""
    from app.utils.logger import get_logger
    logger = get_logger('celery.task')
    logger.debug(f"Task {task.name}[{task_id}] finished with state: {state}")

# Health check endpoint for monitoring
@celery_app.task(name='celery.health_check')
def celery_health_check():
    """Simple health check task"""
    from datetime import datetime
    return {
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'worker_id': celery_health_check.request.hostname if hasattr(celery_health_check, 'request') else 'unknown'
    }

# Task discovery
celery_app.autodiscover_tasks([
    'app.tasks.etl_tasks',
    'app.tasks.monitoring_tasks',
    'app.tasks.cleanup_tasks'
])

# Configuration validation
def validate_celery_config():
    """Validate Celery configuration"""
    errors = []
    
    if not settings.celery_settings.broker_url:
        errors.append("CELERY_BROKER_URL is not configured")
    
    if not settings.celery_settings.result_backend:
        errors.append("CELERY_RESULT_BACKEND is not configured")
    
    # Test broker connection
    try:
        celery_app.control.ping(timeout=5)
    except Exception as e:
        errors.append(f"Cannot connect to broker: {e}")
    
    if errors:
        from app.utils.logger import get_logger
        logger = get_logger('celery.config')
        for error in errors:
            logger.error(f"Celery configuration error: {error}")
        
        if settings.ENVIRONMENT == 'production':
            raise RuntimeError(f"Celery configuration errors: {errors}")
    
    return len(errors) == 0

# Initialize configuration validation
if __name__ != '__main__':
    validate_celery_config()