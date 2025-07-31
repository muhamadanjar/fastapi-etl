# ==============================================
# app/tasks/__init__.py
# ==============================================
from typing import Any, Dict, List
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
    vacuum_database,
    cleanup_orphaned_files,
    reset_stuck_jobs
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
    'cleanup_orphaned': cleanup_orphaned_files,
    'reset_stuck': reset_stuck_jobs,
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
        'cleanup_failed', 'vacuum_db', 'cleanup_orphaned', 'reset_stuck'
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
    'reset_stuck': 6,
    'collect_metrics': 7,
    'check_jobs': 8,
    'performance_report': 9,
    'backup_data': 10,
    'cleanup_files': 11,
    'archive_data': 12,
    'cleanup_orphaned': 13,
    'optimize_db': 14,
    'cleanup_temp': 15,
    'cleanup_logs': 16,
    'purge_records': 17,
    'vacuum_db': 18,
    'cleanup_failed': 19,
}

PERIODIC_SCHEDULES = {
    # High frequency tasks (every few minutes)
    'health_check': {'minutes': 5},
    'reset_stuck': {'minutes': 30},
    
    # Hourly tasks
    'collect_metrics': {'hours': 1},
    'check_jobs': {'hours': 1},
    
    # Daily tasks
    'cleanup_temp': {'hours': 24, 'at_hour': 2},
    'backup_data': {'hours': 24, 'at_hour': 3},
    'cleanup_orphaned': {'hours': 24, 'at_hour': 4},
    'cleanup_logs': {'hours': 24, 'at_hour': 1},
    
    # Weekly tasks
    'archive_data': {'days': 7, 'day_of_week': 0, 'at_hour': 1},
    'performance_report': {'days': 7, 'day_of_week': 1, 'at_hour': 6},
    'cleanup_failed': {'days': 7, 'day_of_week': 6, 'at_hour': 23},
    
    # Monthly tasks
    'optimize_db': {'days': 30, 'day_of_month': 1, 'at_hour': 2},
    'vacuum_db': {'days': 30, 'day_of_month': 15, 'at_hour': 3},
    'purge_records': {'days': 30, 'day_of_month': 28, 'at_hour': 1},
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

def get_task_schedule(task_name: str) -> Dict[str, Any]:
    """Get schedule configuration for a task"""
    return PERIODIC_SCHEDULES.get(task_name, {})

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

def schedule_task_group(task_names: List[str], *args, **kwargs):
    """
    Schedule a group of tasks to run in parallel
    
    Args:
        task_names: List of task names to execute in parallel
        *args: Arguments to pass to tasks
        **kwargs: Keyword arguments to pass to tasks
        
    Returns:
        Group result from Celery
    """
    from celery import group
    
    task_functions = []
    for task_name in task_names:
        task_func = get_task(task_name)
        if task_func:
            task_functions.append(task_func.s(*args, **kwargs))
    
    if task_functions:
        return group(*task_functions).apply_async()
    else:
        raise ValueError("No valid tasks found in group")

def schedule_periodic_tasks():
    """Schedule all periodic tasks"""
    from celery.schedules import crontab
    
    # This would typically be called from celery beat configuration
    periodic_schedule = {}
    
    for task_name, schedule_config in PERIODIC_SCHEDULES.items():
        if 'minutes' in schedule_config:
            # Every X minutes
            periodic_schedule[f'{task_name}-every-{schedule_config["minutes"]}-minutes'] = {
                'task': f'app.tasks.{get_task_module(task_name)}.{task_name}',
                'schedule': crontab(minute=f'*/{schedule_config["minutes"]}'),
            }
        elif 'hours' in schedule_config:
            # Every X hours at specific minute
            at_hour = schedule_config.get('at_hour', 0)
            if schedule_config['hours'] == 1:
                # Every hour
                periodic_schedule[f'{task_name}-hourly'] = {
                    'task': f'app.tasks.{get_task_module(task_name)}.{task_name}',
                    'schedule': crontab(minute=0),
                }
            else:
                # Every X hours
                periodic_schedule[f'{task_name}-every-{schedule_config["hours"]}-hours'] = {
                    'task': f'app.tasks.{get_task_module(task_name)}.{task_name}',
                    'schedule': crontab(hour=f'*/{schedule_config["hours"]}', minute=0),
                }
        elif 'days' in schedule_config:
            # Daily, weekly, or monthly
            if schedule_config['days'] == 7:
                # Weekly
                day_of_week = schedule_config.get('day_of_week', 0)
                at_hour = schedule_config.get('at_hour', 0)
                periodic_schedule[f'{task_name}-weekly'] = {
                    'task': f'app.tasks.{get_task_module(task_name)}.{task_name}',
                    'schedule': crontab(hour=at_hour, minute=0, day_of_week=day_of_week),
                }
            elif schedule_config['days'] == 30:
                # Monthly
                day_of_month = schedule_config.get('day_of_month', 1)
                at_hour = schedule_config.get('at_hour', 0)
                periodic_schedule[f'{task_name}-monthly'] = {
                    'task': f'app.tasks.{get_task_module(task_name)}.{task_name}',
                    'schedule': crontab(hour=at_hour, minute=0, day_of_month=day_of_month),
                }
            else:
                # Daily
                at_hour = schedule_config.get('at_hour', 0)
                periodic_schedule[f'{task_name}-daily'] = {
                    'task': f'app.tasks.{get_task_module(task_name)}.{task_name}',
                    'schedule': crontab(hour=at_hour, minute=0),
                }
    
    return periodic_schedule

def get_task_module(task_name: str) -> str:
    """Get the module name for a task based on its category"""
    for category, tasks in TASK_CATEGORIES.items():
        if task_name in tasks:
            if category == 'etl':
                return 'etl_tasks'
            elif category == 'monitoring':
                return 'monitoring_tasks'
            elif category in ['maintenance', 'data_management']:
                return 'cleanup_tasks'
    return 'etl_tasks'  # default

def get_task_info(task_name: str) -> Dict[str, Any]:
    """Get comprehensive information about a task"""
    task_func = get_task(task_name)
    if not task_func:
        return {}
    
    category = None
    for cat, tasks in TASK_CATEGORIES.items():
        if task_name in tasks:
            category = cat
            break
    
    return {
        'name': task_name,
        'function': task_func,
        'category': category,
        'priority': get_task_priority(task_name),
        'schedule': get_task_schedule(task_name),
        'module': get_task_module(task_name),
        'description': task_func.__doc__ if task_func else None
    }

def run_maintenance_tasks():
    """Run a sequence of maintenance tasks in order"""
    maintenance_sequence = [
        'cleanup_temp',
        'cleanup_orphaned', 
        'reset_stuck',
        'cleanup_failed',
        'optimize_db'
    ]
    return schedule_task_chain(maintenance_sequence)

def run_data_management_tasks():
    """Run data management tasks in parallel"""
    data_tasks = [
        'backup_data',
        'archive_data'
    ]
    return schedule_task_group(data_tasks)

__all__ = [
    "celery_app",
    "TASK_REGISTRY",
    "TASK_CATEGORIES", 
    "TASK_PRIORITIES",
    "PERIODIC_SCHEDULES",
    "get_task",
    "get_tasks_by_category",
    "get_all_tasks",
    "get_task_priority",
    "get_task_schedule",
    "get_task_info",
    "schedule_task",
    "schedule_task_chain",
    "schedule_task_group",
    "schedule_periodic_tasks",
    "run_maintenance_tasks",
    "run_data_management_tasks",
    
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
    "cleanup_orphaned_files",
    "reset_stuck_jobs",
]
