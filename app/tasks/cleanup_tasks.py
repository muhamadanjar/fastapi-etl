from app.tasks.celery_app import celery_app
from app.utils.file_utils import cleanup_old_files
from app.database import get_session
from app.services.etl_service import ETLService

@celery_app.task(name="cleanup.remove_old_files")
def remove_old_files(days: int = 30):
    cleanup_old_files(days=days)


@celery_app.task(name="cleanup.clean_old_executions")
def clean_old_executions(days: int = 30):
    with get_session() as session:
        service = ETLService(session=session)
        service.cleanup_old_executions(days=days)
