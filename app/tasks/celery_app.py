from datetime import timedelta

from celery import Celery
from kombu import Queue

from app.core.config import get_settings


settings = get_settings()

celery_app = Celery(
    "etl_worker",
    broker=settings.celery_settings.broker_url,
    backend=settings.celery_settings.result_backend,
    include=["app.tasks.run_tasks"],
)

celery_app.conf.update(
    task_routes={"etl.*": {"queue": "etl"}},
    task_queues=(Queue("etl", routing_key="etl"),),
    task_default_queue="etl",
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_ignore_result=True,
    timezone="Asia/Jakarta",
    enable_utc=True,
    result_expires=timedelta(days=1),
    task_time_limit=3600,
    task_soft_time_limit=3300,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=100,
    broker_connection_retry_on_startup=True,
)

if settings.ENVIRONMENT == "testing":
    celery_app.conf.update(
        task_always_eager=True,
        task_eager_propagates=True,
        broker_url="memory://",
        result_backend="cache+memory://",
    )
