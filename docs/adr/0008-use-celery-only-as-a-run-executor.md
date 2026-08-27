# Use Celery only as a run executor

Celery remains the asynchronous executor for exactly two public task types: `run_ingestion(run_id)` and `run_transformation(run_id)`. The API creates durable Run state in PostgreSQL before enqueueing work; Celery task IDs and states never replace the domain Run record.

## Consequences

Celery Beat, scheduled jobs, dependency triggering, monitoring tasks, notification tasks, cleanup jobs, and orchestration services are removed. Temporary resources are cleaned within task completion/failure handling rather than through a business-facing cleanup job.
