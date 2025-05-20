from app.infrastructure.workers.etl_task import run_etl_job, run_etl_job_with_file

def publish_etl_job(source: str):
    run_etl_job.delay(source)

def publish_etl_job_with_file(file_path: str):
    run_etl_job_with_file.delay(file_path) 