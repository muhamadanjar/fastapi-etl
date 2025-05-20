from app.core.celery_app import celery
from app.services.etl_service import ETLService
from app.infrastructure.db.etl_repo_impl import ETLRepositoryImpl

@celery.task
def run_etl_job(source: str):
    service = ETLService(ETLRepositoryImpl())
    service.run(source)


@celery.task
def run_etl_job_with_file(file_path: str):
    # Create service instance and run ETL job with uploaded file
    service = ETLService(ETLRepositoryImpl())
    service.run_with_file(file_path)