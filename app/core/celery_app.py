from celery import Celery
from app.core.config import settings

celery = Celery("etl_worker", broker=settings.rabbitmq_broker_url, backend=settings.redis_url)
celery.autodiscover_tasks(["app.infrastructure.workers.etl_task"])