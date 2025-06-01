"""
Task scheduler untuk periodic tasks.
"""
from datetime import timedelta
from celery.schedules import crontab
from app.core.config import settings

class TaskScheduler:
    """Task scheduler untuk mengelola periodic tasks."""
    
    def __init__(self):
        self.schedules = {}
        self._setup_default_schedules()
    
    def _setup_default_schedules(self):
        """Setup default scheduled tasks."""
        if settings.ENABLE_SCHEDULED_TASKS:
            self.schedules.update({
                # Daily cleanup task
                "cleanup-expired-sessions": {
                    "task": "app.infrastructure.tasks.tasks.data_sync_tasks.cleanup_expired_sessions",
                    "schedule": crontab(hour=2, minute=0),  # Every day at 2 AM
                    "options": {"queue": "data_sync"}
                },
                
                # Hourly health check
                "system-health-check": {
                    "task": "app.infrastructure.tasks.tasks.data_sync_tasks.system_health_check",
                    "schedule": crontab(minute=0),  # Every hour
                    "options": {"queue": "default"}
                },
                
                # Weekly report generation
                "weekly-report": {
                    "task": "app.infrastructure.tasks.tasks.email_tasks.send_weekly_report",
                    "schedule": crontab(hour=8, minute=0, day_of_week=1),  # Monday 8 AM
                    "options": {"queue": "email"}
                },
                
                # Data backup every 6 hours
                "data-backup": {
                    "task": "app.infrastructure.tasks.tasks.data_sync_tasks.backup_data",
                    "schedule": crontab(minute=0, hour="*/6"),  # Every 6 hours
                    "options": {"queue": "data_sync"}
                },
            })
    
    def add_schedule(self, name: str, task: str, schedule, **options):
        """Tambah scheduled task."""
        self.schedules[name] = {
            "task": task,
            "schedule": schedule,
            "options": options
        }
    
    def remove_schedule(self, name: str):
        """Hapus scheduled task."""
        if name in self.schedules:
            del self.schedules[name]
    
    def get_schedules(self):
        """Get all schedules."""
        return self.schedules
    
    def update_celery_beat_schedule(self, celery_app):
        """Update Celery beat schedule."""
        celery_app.conf.beat_schedule.update(self.schedules)

# Create scheduler instance
scheduler = TaskScheduler()

# Helper functions untuk common schedule patterns
def every_minutes(minutes: int):
    """Schedule setiap N menit."""
    return timedelta(minutes=minutes)

def every_hours(hours: int):
    """Schedule setiap N jam."""
    return timedelta(hours=hours)

def every_days(days: int):
    """Schedule setiap N hari."""
    return timedelta(days=days)

def daily_at(hour: int, minute: int = 0):
    """Schedule setiap hari pada jam tertentu."""
    return crontab(hour=hour, minute=minute)

def weekly_at(day_of_week: int, hour: int, minute: int = 0):
    """Schedule setiap minggu pada hari dan jam tertentu."""
    return crontab(hour=hour, minute=minute, day_of_week=day_of_week)

def monthly_at(day_of_month: int, hour: int, minute: int = 0):
    """Schedule setiap bulan pada tanggal dan jam tertentu."""
    return crontab(hour=hour, minute=minute, day_of_month=day_of_month)