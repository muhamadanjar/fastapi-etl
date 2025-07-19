# ==============================================
# app/tasks/monitoring_tasks.py
# ==============================================
import os
import psutil
import json
import smtplib
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path
from sqlmodel import Session, select, func
import pandas as pd
import requests

from .celery_app import celery_app
from app.interfaces.dependencies import get_db
from app.infrastructure.db.models.etl_control.job_executions import JobExecutions
from app.infrastructure.db.models.etl_control.etl_jobs import ETLJobs
from app.infrastructure.db.models.raw_data.file_registry import FileRegistry
from app.infrastructure.db.models.etl_control.quality_check_results import QualityCheckResults
from app.utils.logger import get_logger
from app.core.config import settings
from app.core.exceptions import MonitoringException

logger = get_logger(__name__)

@celery_app.task(
    bind=True,
    name='monitoring.health_check',
    time_limit=300,
    soft_time_limit=240
)
async def health_check_task(self):
    """
    Comprehensive system health check
    
    Returns:
        System health status and metrics
    """
    task_id = self.request.id
    logger.info(f"Starting health check task {task_id}")
    
    health_status = {
        'timestamp': datetime.utcnow().isoformat(),
        'task_id': task_id,
        'overall_status': 'healthy',
        'components': {},
        'metrics': {},
        'alerts': []
    }
    
    db = next(get_db())
    try:
        # Database health check
        health_status['components']['database'] = await _check_database_health(db)
        
        # System resources check
        health_status['components']['system'] = await _check_system_resources()
        
        # Storage health check
        health_status['components']['storage'] = await _check_storage_health()
        
        # ETL jobs health check
        health_status['components']['etl_jobs'] = await _check_etl_jobs_health(db)
        
        # Celery workers check
        health_status['components']['celery'] = await _check_celery_health()
        
        # External dependencies check
        health_status['components']['external'] = await _check_external_dependencies()
        
        # Aggregate metrics
        health_status['metrics'] = await _aggregate_health_metrics(health_status['components'])
        
        # Determine overall status
        component_statuses = [comp['status'] for comp in health_status['components'].values()]
        if 'critical' in component_statuses:
            health_status['overall_status'] = 'critical'
        elif 'warning' in component_statuses:
            health_status['overall_status'] = 'warning'
        elif 'unhealthy' in component_statuses:
            health_status['overall_status'] = 'unhealthy'
        
        # Generate alerts for issues
        for component_name, component_data in health_status['components'].items():
            if component_data['status'] != 'healthy':
                health_status['alerts'].append({
                    'component': component_name,
                    'status': component_data['status'],
                    'message': component_data.get('message', 'Component health issue detected'),
                    'details': component_data.get('details', {})
                })
        
        logger.info(f"Health check completed: {health_status['overall_status']}")
        
        return health_status
        
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        health_status['overall_status'] = 'critical'
        health_status['error'] = str(e)
        return health_status
    
    finally:
        db.close()

@celery_app.task(
    bind=True,
    name='monitoring.collect_metrics',
    time_limit=900,
    soft_time_limit=840
)
async def collect_system_metrics(self):
    """
    Collect comprehensive system metrics
    
    Returns:
        System metrics and performance data
    """
    task_id = self.request.id
    logger.info(f"Starting metrics collection task {task_id}")
    
    db = next(get_db())
    try:
        metrics = {
            'timestamp': datetime.utcnow().isoformat(),
            'task_id': task_id,
            'system_metrics': await _collect_system_metrics(),
            'database_metrics': await _collect_database_metrics(db),
            'etl_metrics': await _collect_etl_metrics(db),
            'storage_metrics': await _collect_storage_metrics(),
            'performance_metrics': await _collect_performance_metrics(db)
        }
        
        # Store metrics in database or time series DB
        await _store_metrics(db, metrics)
        
        logger.info(f"Metrics collection completed")
        
        return {
            'status': 'success',
            'task_id': task_id,
            'metrics_collected': len(metrics),
            'timestamp': metrics['timestamp']
        }
        
    except Exception as e:
        logger.error(f"Metrics collection failed: {str(e)}")
        raise MonitoringException(f"Metrics collection failed: {str(e)}")
    
    finally:
        db.close()

@celery_app.task(
    bind=True,
    name='monitoring.performance_report',
    time_limit=1800,
    soft_time_limit=1680
)
async def generate_performance_report(self, period_days: int = 7, report_format: str = 'json'):
    """
    Generate comprehensive performance report
    
    Args:
        period_days: Number of days to include in report
        report_format: Format of report ('json', 'html', 'pdf')
        
    Returns:
        Performance report data
    """
    task_id = self.request.id
    logger.info(f"Starting performance report generation task {task_id}")
    
    db = next(get_db())
    try:
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=period_days)
        
        report = {
            'report_id': task_id,
            'generated_at': end_date.isoformat(),
            'period': {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'days': period_days
            },
            'summary': {},
            'job_performance': {},
            'system_performance': {},
            'data_quality': {},
            'alerts_summary': {},
            'recommendations': []
        }
        
        # ETL Job Performance Analysis
        report['job_performance'] = await _analyze_job_performance(db, start_date, end_date)
        
        # System Performance Analysis
        report['system_performance'] = await _analyze_system_performance(db, start_date, end_date)
        
        # Data Quality Analysis
        report['data_quality'] = await _analyze_data_quality(db, start_date, end_date)
        
        # Alerts Summary
        report['alerts_summary'] = await _analyze_alerts(db, start_date, end_date)
        
        # Generate Summary
        report['summary'] = await _generate_report_summary(report)
        
        # Generate Recommendations
        report['recommendations'] = await _generate_recommendations(report)
        
        # Format and save report
        report_path = await _save_performance_report(report, report_format)
        
        logger.info(f"Performance report generated: {report_path}")
        
        return {
            'status': 'success',
            'task_id': task_id,
            'report_path': report_path,
            'report_format': report_format,
            'period_days': period_days,
            'generated_at': end_date.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Performance report generation failed: {str(e)}")
        raise MonitoringException(f"Performance report generation failed: {str(e)}")
    
    finally:
        db.close()

@celery_app.task(
    bind=True,
    name='monitoring.check_jobs',
    time_limit=600,
    soft_time_limit=540
)
def check_job_status(self):
    """
    Check status of all ETL jobs and detect issues
    
    Returns:
        Job status analysis
    """
    task_id = self.request.id
    logger.info(f"Starting job status check task {task_id}")
    
    db = next(get_db())
    try:
        job_analysis = {
            'timestamp': datetime.utcnow().isoformat(),
            'task_id': task_id,
            'total_jobs': 0,
            'active_jobs': 0,
            'running_jobs': 0,
            'failed_jobs': 0,
            'stuck_jobs': 0,
            'job_details': [],
            'issues_detected': []
        }
        
        # Get all jobs
        jobs = db.exec(select(ETLJobs)).all()
        job_analysis['total_jobs'] = len(jobs)
        
        current_time = datetime.utcnow()
        
        for job in jobs:
            if job.is_active:
                job_analysis['active_jobs'] += 1
                
                # Get latest execution
                latest_execution = db.exec(
                    select(JobExecutions)
                    .where(JobExecutions.job_id == job.job_id)
                    .order_by(JobExecutions.start_time.desc())
                    .limit(1)
                ).first()
                
                job_detail = {
                    'job_id': str(job.job_id),
                    'job_name': job.job_name,
                    'job_type': job.job_type,
                    'last_execution': None,
                    'status': 'never_run',
                    'issues': []
                }
                
                if latest_execution:
                    job_detail['last_execution'] = {
                        'execution_id': str(latest_execution.execution_id),
                        'status': latest_execution.status,
                        'start_time': latest_execution.start_time.isoformat() if latest_execution.start_time else None,
                        'end_time': latest_execution.end_time.isoformat() if latest_execution.end_time else None,
                        'duration_minutes': None
                    }
                    
                    # Calculate duration
                    if latest_execution.start_time and latest_execution.end_time:
                        duration = latest_execution.end_time - latest_execution.start_time
                        job_detail['last_execution']['duration_minutes'] = duration.total_seconds() / 60
                    
                    job_detail['status'] = latest_execution.status
                    
                    # Check for issues
                    if latest_execution.status == 'RUNNING':
                        job_analysis['running_jobs'] += 1
                        
                        # Check if job is stuck (running for too long)
                        if latest_execution.start_time:
                            running_duration = current_time - latest_execution.start_time
                            if running_duration > timedelta(hours=4):  # 4 hours threshold
                                job_analysis['stuck_jobs'] += 1
                                job_detail['issues'].append('Job running for excessive time')
                                job_analysis['issues_detected'].append({
                                    'job_id': str(job.job_id),
                                    'issue': 'stuck_job',
                                    'details': f"Running for {running_duration}"
                                })
                    
                    elif latest_execution.status == 'FAILED':
                        job_analysis['failed_jobs'] += 1
                        job_detail['issues'].append('Last execution failed')
                        job_analysis['issues_detected'].append({
                            'job_id': str(job.job_id),
                            'issue': 'failed_execution',
                            'details': latest_execution.execution_log
                        })
                    
                    # Check for long time since last execution
                    if latest_execution.start_time:
                        time_since_last = current_time - latest_execution.start_time
                        if time_since_last > timedelta(days=7):  # 7 days threshold
                            job_detail['issues'].append('No recent execution')
                            job_analysis['issues_detected'].append({
                                'job_id': str(job.job_id),
                                'issue': 'stale_job',
                                'details': f"Last run {time_since_last} ago"
                            })
                
                job_analysis['job_details'].append(job_detail)
        
        logger.info(f"Job status check completed: {job_analysis['issues_detected'].__len__()} issues detected")
        
        return job_analysis
        
    except Exception as e:
        logger.error(f"Job status check failed: {str(e)}")
        raise MonitoringException(f"Job status check failed: {str(e)}")
    
    finally:
        db.close()

@celery_app.task(
    bind=True,
    name='monitoring.send_alerts',
    time_limit=300,
    soft_time_limit=240
)
async def send_alert_notifications(self, alert_type: str = None, data: Dict[str, Any] = None):
    """
    Send alert notifications via email, Slack, etc.
    
    Args:
        alert_type: Type of alert to send
        data: Alert data and context
        
    Returns:
        Notification sending results
    """
    task_id = self.request.id
    logger.info(f"Starting alert notification task {task_id}")
    
    try:
        notification_results = {
            'task_id': task_id,
            'timestamp': datetime.utcnow().isoformat(),
            'alert_type': alert_type,
            'notifications_sent': 0,
            'notifications_failed': 0,
            'results': []
        }
        
        # Get pending alerts if no specific alert provided
        if not alert_type or not data:
            pending_alerts = await _get_pending_alerts()
        else:
            pending_alerts = [{'type': alert_type, 'data': data}]
        
        for alert in pending_alerts:
            try:
                # Send email notification
                if settings.EMAIL_ALERTS_ENABLED:
                    email_result = await _send_email_alert(alert)
                    notification_results['results'].append(email_result)
                    if email_result['success']:
                        notification_results['notifications_sent'] += 1
                    else:
                        notification_results['notifications_failed'] += 1
                
                # Send Slack notification
                if settings.SLACK_ALERTS_ENABLED:
                    slack_result = await _send_slack_alert(alert)
                    notification_results['results'].append(slack_result)
                    if slack_result['success']:
                        notification_results['notifications_sent'] += 1
                    else:
                        notification_results['notifications_failed'] += 1
                
                # Send webhook notification
                if settings.WEBHOOK_ALERTS_ENABLED:
                    webhook_result = await _send_webhook_alert(alert)
                    notification_results['results'].append(webhook_result)
                    if webhook_result['success']:
                        notification_results['notifications_sent'] += 1
                    else:
                        notification_results['notifications_failed'] += 1
                
            except Exception as e:
                logger.error(f"Failed to send alert: {str(e)}")
                notification_results['notifications_failed'] += 1
                notification_results['results'].append({
                    'type': 'error',
                    'success': False,
                    'error': str(e)
                })
        
        logger.info(f"Alert notifications completed: {notification_results['notifications_sent']} sent, {notification_results['notifications_failed']} failed")
        
        return notification_results
        
    except Exception as e:
        logger.error(f"Alert notification task failed: {str(e)}")
        raise MonitoringException(f"Alert notification failed: {str(e)}")

@celery_app.task(
    bind=True,
    name='monitoring.cleanup_logs',
    time_limit=1800,
    soft_time_limit=1680
)
async def cleanup_old_logs(self, days_old: int = 30, log_types: List[str] = None):
    """
    Clean up old log files and entries
    
    Args:
        days_old: Number of days old logs to clean up
        log_types: Types of logs to clean up
        
    Returns:
        Cleanup results
    """
    task_id = self.request.id
    logger.info(f"Starting log cleanup task {task_id}")
    
    try:
        cutoff_date = datetime.utcnow() - timedelta(days=days_old)
        
        cleanup_results = {
            'task_id': task_id,
            'timestamp': datetime.utcnow().isoformat(),
            'cutoff_date': cutoff_date.isoformat(),
            'log_files_processed': 0,
            'log_files_deleted': 0,
            'space_freed_mb': 0,
            'database_logs_deleted': 0,
            'errors': []
        }
        
        # Clean up log files
        log_directories = [
            '/app/storage/logs',
            '/var/log/etl',
            '/tmp/etl_logs'
        ]
        
        for log_dir in log_directories:
            if os.path.exists(log_dir):
                log_cleanup = await _cleanup_log_directory(log_dir, cutoff_date, log_types)
                cleanup_results['log_files_processed'] += log_cleanup['files_processed']
                cleanup_results['log_files_deleted'] += log_cleanup['files_deleted']
                cleanup_results['space_freed_mb'] += log_cleanup['space_freed_mb']
                cleanup_results['errors'].extend(log_cleanup['errors'])
        
        # Clean up database log entries
        db = next(get_db())
        try:
            db_cleanup = await _cleanup_database_logs(db, cutoff_date)
            cleanup_results['database_logs_deleted'] = db_cleanup['records_deleted']
        finally:
            db.close()
        
        logger.info(f"Log cleanup completed: {cleanup_results['log_files_deleted']} files deleted, {cleanup_results['space_freed_mb']:.2f} MB freed")
        
        return cleanup_results
        
    except Exception as e:
        logger.error(f"Log cleanup failed: {str(e)}")
        raise MonitoringException(f"Log cleanup failed: {str(e)}")

# Helper functions for monitoring tasks

async def _check_database_health(db: Session) -> Dict[str, Any]:
    """Check database health and connectivity"""
    try:
        # Test basic connectivity
        result = db.exec(select(func.count()).select_from(ETLJobs)).first()
        
        # Check connection pool status
        pool_status = {
            'active_connections': db.get_bind().pool.checkedout(),
            'pool_size': db.get_bind().pool.size(),
            'checked_in': db.get_bind().pool.checkedin()
        }
        
        return {
            'status': 'healthy',
            'response_time_ms': 0,  # Would measure actual query time
            'pool_status': pool_status,
            'total_jobs': result
        }
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': str(e)
        }

async def _check_system_resources() -> Dict[str, Any]:
    """Check system resource usage"""
    try:
        # CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        
        # Memory usage
        memory = psutil.virtual_memory()
        
        # Disk usage
        disk = psutil.disk_usage('/')
        
        status = 'healthy'
        if cpu_percent > 90 or memory.percent > 90 or disk.percent > 90:
            status = 'critical'
        elif cpu_percent > 80 or memory.percent > 80 or disk.percent > 80:
            status = 'warning'
        
        return {
            'status': status,
            'cpu_percent': cpu_percent,
            'memory_percent': memory.percent,
            'memory_available_gb': memory.available / (1024**3),
            'disk_percent': disk.percent,
            'disk_free_gb': disk.free / (1024**3)
        }
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': str(e)
        }

async def _check_storage_health() -> Dict[str, Any]:
    """Check storage system health"""
    try:
        storage_paths = [
            '/app/storage/uploads',
            '/app/storage/processed',
            '/app/storage/logs',
            '/app/storage/backups'
        ]
        
        storage_status = {}
        overall_status = 'healthy'
        
        for path in storage_paths:
            if os.path.exists(path):
                disk_usage = psutil.disk_usage(path)
                path_status = {
                    'exists': True,
                    'total_gb': disk_usage.total / (1024**3),
                    'used_gb': disk_usage.used / (1024**3),
                    'free_gb': disk_usage.free / (1024**3),
                    'percent_used': (disk_usage.used / disk_usage.total) * 100
                }
                
                if path_status['percent_used'] > 95:
                    overall_status = 'critical'
                elif path_status['percent_used'] > 85:
                    overall_status = 'warning'
                
            else:
                path_status = {'exists': False}
                overall_status = 'warning'
            
            storage_status[path] = path_status
        
        return {
            'status': overall_status,
            'storage_paths': storage_status
        }
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': str(e)
        }

async def _check_etl_jobs_health(db: Session) -> Dict[str, Any]:
    """Check ETL jobs health"""
    try:
        # Count jobs by status
        current_time = datetime.utcnow()
        
        # Recent executions (last 24 hours)
        recent_executions = db.exec(
            select(JobExecutions)
            .where(JobExecutions.start_time >= current_time - timedelta(hours=24))
        ).all()
        
        status_counts = {
            'total': len(recent_executions),
            'success': sum(1 for e in recent_executions if e.status == 'SUCCESS'),
            'failed': sum(1 for e in recent_executions if e.status == 'FAILED'),
            'running': sum(1 for e in recent_executions if e.status == 'RUNNING')
        }
        
        # Calculate success rate
        success_rate = (status_counts['success'] / status_counts['total'] * 100) if status_counts['total'] > 0 else 100
        
        # Determine health status
        if success_rate < 50:
            status = 'critical'
        elif success_rate < 80:
            status = 'warning'
        else:
            status = 'healthy'
        
        return {
            'status': status,
            'success_rate': success_rate,
            'execution_counts': status_counts,
            'period_hours': 24
        }
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': str(e)
        }

async def _check_celery_health() -> Dict[str, Any]:
    """Check Celery workers health"""
    try:
        from celery import current_app
        
        # Get active workers
        active_workers = current_app.control.inspect().active()
        stats = current_app.control.inspect().stats()
        
        if not active_workers:
            return {
                'status': 'critical',
                'message': 'No active Celery workers found'
            }
        
        worker_count = len(active_workers)
        total_active_tasks = sum(len(tasks) for tasks in active_workers.values())
        
        return {
            'status': 'healthy',
            'active_workers': worker_count,
            'active_tasks': total_active_tasks,
            'worker_stats': stats
        }
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': str(e)
        }

async def _check_external_dependencies() -> Dict[str, Any]:
    """Check external dependencies health"""
    dependencies = {}
    overall_status = 'healthy'
    
    # Check Redis (if used)
    if hasattr(settings, 'REDIS_URL') and settings.REDIS_URL:
        try:
            import redis
            r = redis.from_url(settings.REDIS_URL)
            r.ping()
            dependencies['redis'] = {'status': 'healthy', 'response_time_ms': 0}
        except Exception as e:
            dependencies['redis'] = {'status': 'unhealthy', 'error': str(e)}
            overall_status = 'warning'
    
    # Check external APIs (if configured)
    external_apis = getattr(settings, 'EXTERNAL_APIS', {})
    for api_name, api_url in external_apis.items():
        try:
            response = requests.get(f"{api_url}/health", timeout=10)
            if response.status_code == 200:
                dependencies[api_name] = {'status': 'healthy', 'response_code': response.status_code}
            else:
                dependencies[api_name] = {'status': 'warning', 'response_code': response.status_code}
                overall_status = 'warning'
        except Exception as e:
            dependencies[api_name] = {'status': 'unhealthy', 'error': str(e)}
            overall_status = 'warning'
    
    return {
        'status': overall_status,
        'dependencies': dependencies
    }

async def _aggregate_health_metrics(components: Dict[str, Any]) -> Dict[str, Any]:
    """Aggregate health metrics from all components"""
    metrics = {
        'healthy_components': 0,
        'warning_components': 0,
        'unhealthy_components': 0,
        'critical_components': 0,
        'total_components': len(components)
    }
    
    for component_data in components.values():
        status = component_data.get('status', 'unknown')
        if status == 'healthy':
            metrics['healthy_components'] += 1
        elif status == 'warning':
            metrics['warning_components'] += 1
        elif status == 'unhealthy':
            metrics['unhealthy_components'] += 1
        elif status == 'critical':
            metrics['critical_components'] += 1
    
    # Calculate health percentage
    metrics['health_percentage'] = (metrics['healthy_components'] / metrics['total_components']) * 100
    
    return metrics

async def _collect_system_metrics() -> Dict[str, Any]:
    """Collect detailed system metrics"""
    return {
        'cpu': {
            'percent': psutil.cpu_percent(interval=1),
            'count': psutil.cpu_count(),
            'load_avg': os.getloadavg() if hasattr(os, 'getloadavg') else None
        },
        'memory': {
            'total_gb': psutil.virtual_memory().total / (1024**3),
            'available_gb': psutil.virtual_memory().available / (1024**3),
            'percent': psutil.virtual_memory().percent,
            'swap_percent': psutil.swap_memory().percent
        },
        'disk': {
            'total_gb': psutil.disk_usage('/').total / (1024**3),
            'free_gb': psutil.disk_usage('/').free / (1024**3),
            'percent': psutil.disk_usage('/').percent
        },
        'network': {
            'bytes_sent': psutil.net_io_counters().bytes_sent,
            'bytes_recv': psutil.net_io_counters().bytes_recv
        }
    }

async def _collect_database_metrics(db: Session) -> Dict[str, Any]:
    """Collect database metrics"""
    try:
        # Count records in main tables
        file_count = db.exec(select(func.count()).select_from(FileRegistry)).first()
        job_count = db.exec(select(func.count()).select_from(ETLJobs)).first()
        execution_count = db.exec(select(func.count()).select_from(JobExecutions)).first()
        
        # Recent activity (last 24 hours)
        recent_files = db.exec(
            select(func.count()).select_from(FileRegistry)
            .where(FileRegistry.upload_date >= datetime.utcnow() - timedelta(hours=24))
        ).first()
        
        recent_executions = db.exec(
            select(func.count()).select_from(JobExecutions)
            .where(JobExecutions.start_time >= datetime.utcnow() - timedelta(hours=24))
        ).first()
        
        return {
            'total_files': file_count,
            'total_jobs': job_count,
            'total_executions': execution_count,
            'recent_files_24h': recent_files,
            'recent_executions_24h': recent_executions
        }
    except Exception as e:
        return {'error': str(e)}

async def _collect_storage_metrics() -> Dict[str, Any]:
    """Collect storage-related metrics"""
    storage_paths = {
        'uploads': '/app/storage/uploads',
        'processed': '/app/storage/processed',
        'logs': '/app/storage/logs',
        'backups': '/app/storage/backups'
    }
    
    metrics = {}
    for name, path in storage_paths.items():
        if os.path.exists(path):
            # Count files and calculate sizes
            file_count = 0
            total_size = 0
            
            for root, dirs, files in os.walk(path):
                file_count += len(files)
                for file in files:
                    try:
                        file_path = os.path.join(root, file)
                        total_size += os.path.getsize(file_path)
                    except (OSError, FileNotFoundError):
                        continue
            
            metrics[name] = {
                'file_count': file_count,
                'total_size_mb': total_size / (1024 * 1024),
                'path': path
            }
        else:
            metrics[name] = {
                'file_count': 0,
                'total_size_mb': 0,
                'path': path,
                'exists': False
            }
    
    return metrics

async def _collect_performance_metrics(db: Session) -> Dict[str, Any]:
    """Collect performance metrics"""
    try:
        current_time = datetime.utcnow()
        
        # Performance over different time periods
        periods = {
            '1h': timedelta(hours=1),
            '24h': timedelta(hours=24),
            '7d': timedelta(days=7)
        }
        
        performance_data = {}
        
        for period_name, period_delta in periods.items():
            start_time = current_time - period_delta
            
            executions = db.exec(
                select(JobExecutions)
                .where(JobExecutions.start_time >= start_time)
                .where(JobExecutions.status.in_(['SUCCESS', 'FAILED']))
            ).all()
            
            if executions:
                success_count = sum(1 for e in executions if e.status == 'SUCCESS')
                total_records = sum(e.records_processed or 0 for e in executions)
                
                durations = []
                for execution in executions:
                    if execution.start_time and execution.end_time:
                        duration = (execution.end_time - execution.start_time).total_seconds()
                        durations.append(duration)
                
                performance_data[period_name] = {
                    'total_executions': len(executions),
                    'success_count': success_count,
                    'success_rate': (success_count / len(executions)) * 100,
                    'total_records_processed': total_records,
                    'avg_duration_seconds': sum(durations) / len(durations) if durations else 0,
                    'throughput_records_per_second': total_records / (period_delta.total_seconds()) if period_delta.total_seconds() > 0 else 0
                }
            else:
                performance_data[period_name] = {
                    'total_executions': 0,
                    'success_count': 0,
                    'success_rate': 100,
                    'total_records_processed': 0,
                    'avg_duration_seconds': 0,
                    'throughput_records_per_second': 0
                }
        
        return performance_data
        
    except Exception as e:
        return {'error': str(e)}

async def _store_metrics(db: Session, metrics: Dict[str, Any]):
    """Store metrics in database"""
    try:
        # This would typically store metrics in a time series database
        # For now, we'll log them or store in a simple metrics table
        logger.info(f"Metrics collected: {json.dumps(metrics, default=str)}")
        
        # Could implement storage to InfluxDB, Prometheus, or custom metrics table
        
    except Exception as e:
        logger.error(f"Failed to store metrics: {str(e)}")

async def _analyze_job_performance(db: Session, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
    """Analyze ETL job performance over time period"""
    try:
        executions = db.exec(
            select(JobExecutions)
            .where(JobExecutions.start_time >= start_date)
            .where(JobExecutions.start_time <= end_date)
        ).all()
        
        if not executions:
            return {'message': 'No executions found in the specified period'}
        
        # Group by job
        job_stats = {}
        for execution in executions:
            job_id = str(execution.job_id)
            if job_id not in job_stats:
                job_stats[job_id] = {
                    'executions': [],
                    'total_executions': 0,
                    'successful_executions': 0,
                    'failed_executions': 0,
                    'avg_duration_minutes': 0,
                    'total_records_processed': 0
                }
            
            job_stats[job_id]['executions'].append(execution)
            job_stats[job_id]['total_executions'] += 1
            
            if execution.status == 'SUCCESS':
                job_stats[job_id]['successful_executions'] += 1
            elif execution.status == 'FAILED':
                job_stats[job_id]['failed_executions'] += 1
            
            if execution.records_processed:
                job_stats[job_id]['total_records_processed'] += execution.records_processed
        
        # Calculate averages and rates
        for job_id, stats in job_stats.items():
            stats['success_rate'] = (stats['successful_executions'] / stats['total_executions']) * 100
            
            durations = []
            for execution in stats['executions']:
                if execution.start_time and execution.end_time:
                    duration = (execution.end_time - execution.start_time).total_seconds() / 60
                    durations.append(duration)
            
            stats['avg_duration_minutes'] = sum(durations) / len(durations) if durations else 0
            del stats['executions']  # Remove detailed executions for summary
        
        return {
            'period_summary': {
                'total_executions': len(executions),
                'successful_executions': sum(1 for e in executions if e.status == 'SUCCESS'),
                'failed_executions': sum(1 for e in executions if e.status == 'FAILED'),
                'overall_success_rate': (sum(1 for e in executions if e.status == 'SUCCESS') / len(executions)) * 100,
                'total_records_processed': sum(e.records_processed or 0 for e in executions)
            },
            'job_details': job_stats
        }
        
    except Exception as e:
        return {'error': str(e)}

async def _analyze_system_performance(db: Session, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
    """Analyze system performance metrics"""
    try:
        # This would analyze stored system metrics over time
        # For now, return current system status
        current_metrics = await _collect_system_metrics()
        
        return {
            'current_metrics': current_metrics,
            'analysis_period': {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat()
            },
            'trends': {
                'cpu_trend': 'stable',  # Would calculate from historical data
                'memory_trend': 'stable',
                'disk_trend': 'increasing'
            }
        }
        
    except Exception as e:
        return {'error': str(e)}

async def _analyze_data_quality(db: Session, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
    """Analyze data quality metrics"""
    try:
        quality_checks = db.exec(
            select(QualityCheckResults)
            .where(QualityCheckResults.created_at >= start_date)
            .where(QualityCheckResults.created_at <= end_date)
        ).all()
        
        if not quality_checks:
            return {'message': 'No quality checks found in the specified period'}
        
        total_checks = len(quality_checks)
        passed_checks = sum(1 for check in quality_checks if check.check_result == 'PASS')
        failed_checks = sum(1 for check in quality_checks if check.check_result == 'FAIL')
        warning_checks = sum(1 for check in quality_checks if check.check_result == 'WARNING')
        
        return {
            'total_quality_checks': total_checks,
            'passed_checks': passed_checks,
            'failed_checks': failed_checks,
            'warning_checks': warning_checks,
            'pass_rate': (passed_checks / total_checks) * 100,
            'quality_score': ((passed_checks + (warning_checks * 0.5)) / total_checks) * 100
        }
        
    except Exception as e:
        return {'error': str(e)}

async def _analyze_alerts(db: Session, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
    """Analyze alerts and incidents"""
    try:
        # This would analyze alert history from an alerts table
        # For now, return placeholder data
        return {
            'total_alerts': 0,
            'critical_alerts': 0,
            'warning_alerts': 0,
            'resolved_alerts': 0,
            'average_resolution_time_minutes': 0
        }
        
    except Exception as e:
        return {'error': str(e)}

async def _generate_report_summary(report: Dict[str, Any]) -> Dict[str, Any]:
    """Generate executive summary of the report"""
    try:
        job_perf = report.get('job_performance', {})
        data_quality = report.get('data_quality', {})
        
        summary = {
            'overall_health': 'good',
            'key_metrics': {},
            'main_issues': [],
            'achievements': []
        }
        
        # Extract key metrics
        period_summary = job_perf.get('period_summary', {})
        if period_summary:
            summary['key_metrics'] = {
                'total_job_executions': period_summary.get('total_executions', 0),
                'job_success_rate': round(period_summary.get('overall_success_rate', 0), 2),
                'total_records_processed': period_summary.get('total_records_processed', 0),
                'data_quality_score': round(data_quality.get('quality_score', 0), 2)
            }
            
            # Determine overall health
            success_rate = period_summary.get('overall_success_rate', 0)
            quality_score = data_quality.get('quality_score', 0)
            
            if success_rate < 80 or quality_score < 80:
                summary['overall_health'] = 'needs_attention'
            elif success_rate < 90 or quality_score < 90:
                summary['overall_health'] = 'fair'
        
        return summary
        
    except Exception as e:
        return {'error': str(e)}

async def _generate_recommendations(report: Dict[str, Any]) -> List[str]:
    """Generate recommendations based on report analysis"""
    recommendations = []
    
    try:
        job_perf = report.get('job_performance', {})
        system_perf = report.get('system_performance', {})
        data_quality = report.get('data_quality', {})
        
        # Job performance recommendations
        period_summary = job_perf.get('period_summary', {})
        if period_summary:
            success_rate = period_summary.get('overall_success_rate', 100)
            if success_rate < 95:
                recommendations.append(f"Job success rate is {success_rate:.1f}%. Consider reviewing failed job logs and improving error handling.")
            
            failed_count = period_summary.get('failed_executions', 0)
            if failed_count > 5:
                recommendations.append(f"{failed_count} job executions failed. Implement better monitoring and alerting for job failures.")
        
        # Data quality recommendations
        if data_quality:
            quality_score = data_quality.get('quality_score', 100)
            if quality_score < 90:
                recommendations.append(f"Data quality score is {quality_score:.1f}%. Implement additional data validation rules and monitoring.")
            
            failed_checks = data_quality.get('failed_checks', 0)
            if failed_checks > 0:
                recommendations.append(f"{failed_checks} data quality checks failed. Review and strengthen data quality rules.")
        
        # System performance recommendations
        current_metrics = system_perf.get('current_metrics', {})
        if current_metrics:
            cpu_percent = current_metrics.get('cpu', {}).get('percent', 0)
            memory_percent = current_metrics.get('memory', {}).get('percent', 0)
            disk_percent = current_metrics.get('disk', {}).get('percent', 0)
            
            if cpu_percent > 80:
                recommendations.append(f"CPU usage is high ({cpu_percent:.1f}%). Consider optimizing ETL processes or scaling up resources.")
            
            if memory_percent > 80:
                recommendations.append(f"Memory usage is high ({memory_percent:.1f}%). Consider increasing memory or optimizing memory usage in ETL processes.")
            
            if disk_percent > 85:
                recommendations.append(f"Disk usage is high ({disk_percent:.1f}%). Implement automated cleanup processes or increase storage capacity.")
        
        # General recommendations
        if not recommendations:
            recommendations.append("System is performing well. Continue monitoring and maintain current practices.")
        
        return recommendations
        
    except Exception as e:
        return [f"Error generating recommendations: {str(e)}"]

async def _save_performance_report(report: Dict[str, Any], format: str) -> str:
    """Save performance report to file"""
    try:
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        reports_dir = Path('/app/storage/reports')
        reports_dir.mkdir(parents=True, exist_ok=True)
        
        if format == 'json':
            report_path = reports_dir / f'performance_report_{timestamp}.json'
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
        
        elif format == 'html':
            report_path = reports_dir / f'performance_report_{timestamp}.html'
            html_content = await _generate_html_report(report)
            with open(report_path, 'w') as f:
                f.write(html_content)
        
        else:
            # Default to JSON
            report_path = reports_dir / f'performance_report_{timestamp}.json'
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
        
        return str(report_path)
        
    except Exception as e:
        logger.error(f"Failed to save report: {str(e)}")
        return f"Error saving report: {str(e)}"

async def _generate_html_report(report: Dict[str, Any]) -> str:
    """Generate HTML version of the performance report"""
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>ETL Performance Report</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .header { background-color: #f4f4f4; padding: 20px; border-radius: 5px; }
            .section { margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }
            .metric { display: inline-block; margin: 10px; padding: 10px; background-color: #e9f7ef; border-radius: 3px; }
            .recommendation { background-color: #fff3cd; padding: 10px; margin: 5px 0; border-radius: 3px; }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>ETL Performance Report</h1>
            <p>Generated: {generated_at}</p>
            <p>Period: {period_start} to {period_end}</p>
        </div>
        
        <div class="section">
            <h2>Summary</h2>
            <div class="metric">Overall Health: {overall_health}</div>
            <div class="metric">Job Success Rate: {job_success_rate}%</div>
            <div class="metric">Total Records Processed: {total_records}</div>
            <div class="metric">Data Quality Score: {quality_score}%</div>
        </div>
        
        <div class="section">
            <h2>Recommendations</h2>
            {recommendations_html}
        </div>
    </body>
    </html>
    """
    
    # Extract data from report
    summary = report.get('summary', {})
    key_metrics = summary.get('key_metrics', {})
    recommendations = report.get('recommendations', [])
    
    recommendations_html = ''.join([
        f'<div class="recommendation">{rec}</div>' 
        for rec in recommendations
    ])
    
    return html_template.format(
        generated_at=report.get('generated_at', ''),
        period_start=report.get('period', {}).get('start_date', ''),
        period_end=report.get('period', {}).get('end_date', ''),
        overall_health=summary.get('overall_health', 'unknown'),
        job_success_rate=key_metrics.get('job_success_rate', 0),
        total_records=key_metrics.get('total_records_processed', 0),
        quality_score=key_metrics.get('data_quality_score', 0),
        recommendations_html=recommendations_html
    )

async def _get_pending_alerts() -> List[Dict[str, Any]]:
    """Get pending alerts that need to be sent"""
    # This would typically query an alerts queue or database
    # For now, return empty list
    return []

async def _send_email_alert(alert: Dict[str, Any]) -> Dict[str, Any]:
    """Send email alert notification"""
    try:
        if not hasattr(settings, 'EMAIL_HOST') or not settings.EMAIL_HOST:
            return {'type': 'email', 'success': False, 'error': 'Email not configured'}
        
        # Create email message
        msg = MimeMultipart()
        msg['From'] = settings.EMAIL_FROM
        msg['To'] = settings.ALERT_EMAIL_TO
        msg['Subject'] = f"ETL Alert: {alert['type']}"
        
        # Create email body
        body = f"""
        ETL System Alert
        
        Alert Type: {alert['type']}
        Timestamp: {alert.get('data', {}).get('timestamp', datetime.utcnow().isoformat())}
        
        Details:
        {json.dumps(alert.get('data', {}), indent=2)}
        """
        
        msg.attach(MimeText(body, 'plain'))
        
        # Send email
        server = smtplib.SMTP(settings.EMAIL_HOST, settings.EMAIL_PORT)
        if hasattr(settings, 'EMAIL_USE_TLS') and settings.EMAIL_USE_TLS:
            server.starttls()
        if hasattr(settings, 'EMAIL_USER') and settings.EMAIL_USER:
            server.login(settings.EMAIL_USER, settings.EMAIL_PASSWORD)
        
        text = msg.as_string()
        server.sendmail(settings.EMAIL_FROM, settings.ALERT_EMAIL_TO, text)
        server.quit()
        
        return {'type': 'email', 'success': True, 'recipient': settings.ALERT_EMAIL_TO}
        
    except Exception as e:
        return {'type': 'email', 'success': False, 'error': str(e)}

async def _send_slack_alert(alert: Dict[str, Any]) -> Dict[str, Any]:
    """Send Slack alert notification"""
    try:
        if not hasattr(settings, 'SLACK_WEBHOOK_URL') or not settings.SLACK_WEBHOOK_URL:
            return {'type': 'slack', 'success': False, 'error': 'Slack not configured'}
        
        # Create Slack message
        slack_message = {
            'text': f"ETL Alert: {alert['type']}",
            'attachments': [
                {
                    'color': 'danger' if alert['type'] == 'critical' else 'warning',
                    'fields': [
                        {
                            'title': 'Alert Type',
                            'value': alert['type'],
                            'short': True
                        },
                        {
                            'title': 'Timestamp',
                            'value': alert.get('data', {}).get('timestamp', datetime.utcnow().isoformat()),
                            'short': True
                        }
                    ]
                }
            ]
        }
        
        # Send to Slack
        response = requests.post(
            settings.SLACK_WEBHOOK_URL,
            json=slack_message,
            timeout=10
        )
        
        if response.status_code == 200:
            return {'type': 'slack', 'success': True}
        else:
            return {'type': 'slack', 'success': False, 'error': f'HTTP {response.status_code}'}
        
    except Exception as e:
        return {'type': 'slack', 'success': False, 'error': str(e)}

async def _send_webhook_alert(alert: Dict[str, Any]) -> Dict[str, Any]:
    """Send webhook alert notification"""
    try:
        if not hasattr(settings, 'ALERT_WEBHOOK_URL') or not settings.ALERT_WEBHOOK_URL:
            return {'type': 'webhook', 'success': False, 'error': 'Webhook not configured'}
        
        # Send webhook
        response = requests.post(
            settings.ALERT_WEBHOOK_URL,
            json=alert,
            timeout=10,
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code in [200, 201, 202]:
            return {'type': 'webhook', 'success': True}
        else:
            return {'type': 'webhook', 'success': False, 'error': f'HTTP {response.status_code}'}
        
    except Exception as e:
        return {'type': 'webhook', 'success': False, 'error': str(e)}

async def _cleanup_log_directory(log_dir: str, cutoff_date: datetime, log_types: List[str] = None) -> Dict[str, Any]:
    """Clean up log files in a directory"""
    cleanup_result = {
        'files_processed': 0,
        'files_deleted': 0,
        'space_freed_mb': 0,
        'errors': []
    }
    
    try:
        for root, dirs, files in os.walk(log_dir):
            for file in files:
                try:
                    file_path = os.path.join(root, file)
                    file_stat = os.stat(file_path)
                    file_mtime = datetime.fromtimestamp(file_stat.st_mtime)
                    
                    cleanup_result['files_processed'] += 1
                    
                    # Check if file is old enough
                    if file_mtime < cutoff_date:
                        # Check file type filter
                        if log_types:
                            file_ext = os.path.splitext(file)[1].lower()
                            if file_ext not in log_types:
                                continue
                        
                        # Delete the file
                        file_size = file_stat.st_size
                        os.remove(file_path)
                        
                        cleanup_result['files_deleted'] += 1
                        cleanup_result['space_freed_mb'] += file_size / (1024 * 1024)
                        
                        logger.debug(f"Deleted log file: {file_path}")
                
                except Exception as e:
                    error_msg = f"Failed to process file {file_path}: {str(e)}"
                    cleanup_result['errors'].append(error_msg)
                    logger.warning(error_msg)
        
        return cleanup_result
        
    except Exception as e:
        cleanup_result['errors'].append(f"Failed to process directory {log_dir}: {str(e)}")
        return cleanup_result

async def _cleanup_database_logs(db: Session, cutoff_date: datetime) -> Dict[str, Any]:
    """Clean up old log entries from database"""
    try:
        # This would delete old log entries from database tables
        # Implementation depends on specific log table structure
        
        # Placeholder implementation
        deleted_count = 0
        
        # Example: Delete old job execution logs
        old_executions = db.exec(
            select(JobExecutions)
            .where(JobExecutions.start_time < cutoff_date)
            .where(JobExecutions.status.in_(['SUCCESS', 'FAILED']))
        ).all()
        
        for execution in old_executions:
            # Only delete very old successful executions, keep failed ones longer
            if execution.status == 'SUCCESS' and execution.start_time < cutoff_date - timedelta(days=30):
                # Clear detailed logs but keep summary
                execution.execution_log = None
                execution.performance_metrics = None
                deleted_count += 1
        
        db.commit()
        
        return {'records_deleted': deleted_count}
        
    except Exception as e:
        logger.error(f"Database log cleanup failed: {str(e)}")
        return {'records_deleted': 0, 'error': str(e)}

async def _collect_etl_metrics(db: Session) -> Dict[str, Any]:
    """Collect ETL-specific metrics"""
    try:
        current_time = datetime.utcnow()
        
        # Job execution statistics
        recent_executions = db.exec(
            select(JobExecutions)
            .where(JobExecutions.start_time >= current_time - timedelta(hours=24))
        ).all()
        
        if recent_executions:
            durations = []
            for execution in recent_executions:
                if execution.start_time and execution.end_time:
                    duration = (execution.end_time - execution.start_time).total_seconds() / 60
                    durations.append(duration)
            
            avg_duration = sum(durations) / len(durations) if durations else 0
            
            return {
                'avg_job_duration_minutes': avg_duration,
                'total_records_processed_24h': sum(e.records_processed or 0 for e in recent_executions),
                'success_rate_24h': sum(1 for e in recent_executions if e.status == 'SUCCESS') / len(recent_executions) * 100,
                'throughput_records_per_hour': sum(e.records_processed or 0 for e in recent_executions) / 24
            }
        else:
            return {
                'avg_job_duration_minutes': 0,
                'total_records_processed_24h': 0,
                'success_rate_24h': 100,
                'throughput_records_per_hour': 0
            }
    except Exception as e:
        logger.error(f"Job chain execution failed: {str(e)}")
        pass