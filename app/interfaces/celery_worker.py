from app.tasks import celery_app

if __name__ == "__main__":
    # Start the Celery worker
    celery_app.worker_main()