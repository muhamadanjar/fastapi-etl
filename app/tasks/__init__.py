# ==============================================
# app/tasks/__init__.py
# ==============================================
from .celery_app import celery_app
from .etl_tasks import (
    process_file_task,
    run_transformation_pipeline,
    execute_etl_job,
    validate_data_quality,
    generate_data_lineage,
    cleanup_old_files,
    backup_processed_data
)
from .monitoring_tasks import (
    health_check_task,
    collect_system_metrics,
    generate_performance_report,
    check_job_status,
    send_alert_notifications,
    cleanup_old_logs
)
from .cleanup_tasks import (
    cleanup_temporary_files,
    archive_old_data,
    purge_expired_records,
    optimize_database,
    cleanup_failed_jobs,
    vacuum_database
)

# Task registry for dynamic task execution
TASK_REGISTRY = {
    # ETL Tasks
    'process_file': process_file_task,
    'transform_data': run_transformation_pipeline,
    'execute_job': execute_etl_job,
    'validate_quality': validate_data_quality,
    'generate_lineage': generate_data_lineage,
    'cleanup_files': cleanup_old_files,
    'backup_data': backup_processed_data,
    
    # Monitoring Tasks
    'health_check': health_check_task,
    'collect_metrics': collect_system_metrics,
    'performance_report': generate_performance_report,
    'check_jobs': check_job_status,
    'send_alerts': send_alert_notifications,
    'cleanup_logs': cleanup_old_logs,
    
    # Cleanup Tasks
    'cleanup_temp': cleanup_temporary_files,
    'archive_data': archive_old_data,
    'purge_records': purge_expired_records,
    'optimize_db': optimize_database,
    'cleanup_failed': cleanup_failed_jobs,
    'vacuum_db': vacuum_database,
}

# Task categories for organization
TASK_CATEGORIES = {
    'etl': [
        'process_file', 'transform_data', 'execute_job', 
        'validate_quality', 'generate_lineage'
    ],
    'data_management': [
        'cleanup_files', 'backup_data', 'archive_data', 'purge_records'
    ],
    'monitoring': [
        'health_check', 'collect_metrics', 'performance_report', 
        'check_jobs', 'send_alerts'
    ],
    'maintenance': [
        'cleanup_temp', 'cleanup_logs', 'optimize_db', 
        'cleanup_failed', 'vacuum_db'
    ]
}

# Task priorities (lower number = higher priority)
TASK_PRIORITIES = {
    'execute_job': 1,
    'process_file': 2,
    'validate_quality': 3,
    'transform_data': 4,
    'health_check': 5,
    'send_alerts': 5,
    'collect_metrics': 6,
    'check_jobs': 7,
    'performance_report': 8,
    'backup_data': 9,
    'cleanup_files': 10,
    'archive_data': 11,
    'optimize_db': 12,
    'cleanup_temp': 13,
    'cleanup_logs': 14,
    'vacuum_db': 15,
}

def get_task(task_name: str):
    """
    Get task function by name
    
    Args:
        task_name: Name of the task
        
    Returns:
        Task function or None if not found
    """
    return TASK_REGISTRY.get(task_name)

def get_tasks_by_category(category: str) -> List[str]:
    """
    Get list of tasks by category
    
    Args:
        category: Task category name
        
    Returns:
        List of task names in the category
    """
    return TASK_CATEGORIES.get(category, [])

def get_all_tasks() -> Dict[str, Any]:
    """Get all available tasks"""
    return TASK_REGISTRY.copy()

def get_task_priority(task_name: str) -> int:
    """Get priority for a task (lower = higher priority)"""
    return TASK_PRIORITIES.get(task_name, 99)

def schedule_task(task_name: str, *args, **kwargs):
    """
    Schedule a task for execution
    
    Args:
        task_name: Name of the task to schedule
        *args: Positional arguments for the task
        **kwargs: Keyword arguments for the task
        
    Returns:
        AsyncResult object from Celery
    """
    task_func = get_task(task_name)
    if not task_func:
        raise ValueError(f"Task '{task_name}' not found")
    
    priority = get_task_priority(task_name)
    return task_func.apply_async(args=args, kwargs=kwargs, priority=priority)

def schedule_task_chain(task_names: List[str], *args, **kwargs):
    """
    Schedule a chain of tasks
    
    Args:
        task_names: List of task names to execute in sequence
        *args: Arguments to pass to tasks
        **kwargs: Keyword arguments to pass to tasks
        
    Returns:
        Chain result from Celery
    """
    from celery import chain
    
    task_functions = []
    for task_name in task_names:
        task_func = get_task(task_name)
        if task_func:
            task_functions.append(task_func.s(*args, **kwargs))
    
    if task_functions:
        return chain(*task_functions).apply_async()
    else:
        raise ValueError("No valid tasks found in chain")

def schedule_periodic_tasks():
    """Schedule all periodic tasks"""
    from celery.schedules import crontab
    
    # This would typically be called from celery beat configuration
    periodic_schedule = {
        'health-check-every-5-minutes': {
            'task': 'app.tasks.monitoring_tasks.health_check_task',
            'schedule': crontab(minute='*/5'),
        },
        'collect-metrics-every-hour': {
            'task': 'app.tasks.monitoring_tasks.collect_system_metrics',
            'schedule': crontab(minute=0),
        },
        'cleanup-temp-files-daily': {
            'task': 'app.tasks.cleanup_tasks.cleanup_temporary_files',
            'schedule': crontab(hour=2, minute=0),
        },
        'backup-data-daily': {
            'task': 'app.tasks.etl_tasks.backup_processed_data',
            'schedule': crontab(hour=3, minute=0),
        },
        'archive-old-data-weekly': {
            'task': 'app.tasks.cleanup_tasks.archive_old_data',
            'schedule': crontab(hour=1, minute=0, day_of_week=0),
        },
        'performance-report-weekly': {
            'task': 'app.tasks.monitoring_tasks.generate_performance_report',
            'schedule': crontab(hour=6, minute=0, day_of_week=1),
        },
        'optimize-database-monthly': {
            'task': 'app.tasks.cleanup_tasks.optimize_database',
            'schedule': crontab(hour=2, minute=0, day_of_month=1),
        },
    }
    
    return periodic_schedule

__all__ = [
    "celery_app",
    "TASK_REGISTRY",
    "TASK_CATEGORIES", 
    "TASK_PRIORITIES",
    "get_task",
    "get_tasks_by_category",
    "get_all_tasks",
    "get_task_priority",
    "schedule_task",
    "schedule_task_chain",
    "schedule_periodic_tasks",
    
    # ETL Tasks
    "process_file_task",
    "run_transformation_pipeline",
    "execute_etl_job",
    "validate_data_quality",
    "generate_data_lineage",
    "cleanup_old_files",
    "backup_processed_data",
    
    # Monitoring Tasks
    "health_check_task",
    "collect_system_metrics", 
    "generate_performance_report",
    "check_job_status",
    "send_alert_notifications",
    "cleanup_old_logs",
    
    # Cleanup Tasks
    "cleanup_temporary_files",
    "archive_old_data",
    "purge_expired_records",
    "optimize_database",
    "cleanup_failed_jobs",
    "vacuum_database",
]