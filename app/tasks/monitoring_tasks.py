from app.tasks.celery_app import celery_app
from app.services.monitoring_service import MonitoringService
from app.database import get_session

@celery_app.task(name="monitoring.generate_health_report")
def generate_health_report():
    with get_session() as session:
        service = MonitoringService(session=session)
        service.generate_report()