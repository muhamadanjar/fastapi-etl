from uuid import UUID

from app.application.services.run_executor import execute_ingestion, execute_transformation
from app.infrastructure.db.manager import database_manager
from app.tasks.celery_app import celery_app


@celery_app.task(name="etl.execute_ingestion", acks_late=True)
def execute_ingestion_run(run_id: str) -> str | None:
    with database_manager.get_session() as session:
        dataset_id = execute_ingestion(session, UUID(run_id))
    return str(dataset_id) if dataset_id else None


@celery_app.task(name="etl.execute_transformation", acks_late=True)
def execute_transformation_run(run_id: str) -> str | None:
    with database_manager.get_session() as session:
        dataset_id = execute_transformation(session, UUID(run_id))
    return str(dataset_id) if dataset_id else None
