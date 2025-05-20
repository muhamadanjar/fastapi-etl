from app.core.celery_app import celery

if __name__ == "__main__":
    # Start the Celery worker
    celery.worker_main()