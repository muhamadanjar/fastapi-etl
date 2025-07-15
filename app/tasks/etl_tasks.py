from app.tasks.celery_app import celery_app
from app.services.etl_service import ETLService
from app.database import get_session
from app.models.etl_control import etl_jobs

@celery_app.task(name="etl.run_etl_job")
def run_etl_job(job_id: int):
    with get_session() as session:
        service = ETLService(session=session)
        job = session.get(etl_jobs.ETLJob, job_id)
        if not job:
            raise ValueError(f"ETL Job with ID {job_id} not found")
        service.run_job(job)