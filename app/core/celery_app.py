from celery import Celery
from app.core.config import settings

celery = Celery("etl_worker", broker=settings.rabbitmq_broker_url, backend=settings.redis_url)
celery.conf.task_routes = {
    "app.infrastructure.workers.etl_task.run_etl_job": {"queue": "etl_queue"}
}