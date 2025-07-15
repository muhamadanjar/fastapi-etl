from app.services.base import BaseService
from app.models.etl_control import etl_jobs, job_executions
from app.transformers.data_cleaner import DataCleaner
from app.transformers.data_normalizer import DataNormalizer
from app.transformers.data_validator import DataValidator
from app.transformers.entity_matcher import EntityMatcher
from datetime import datetime
from sqlmodel import select

class ETLService(BaseService):
    def run_job(self, job: etl_jobs.ETLJob):
        execution = job_executions.JobExecution(
            job_id=job.job_id,
            batch_id=f"BATCH-{datetime.utcnow().isoformat()}",
            start_time=datetime.utcnow(),
            status="RUNNING",
        )
        self.session.add(execution)
        self.session.commit()

        try:
            # Placeholder ETL logic
            # Load raw records -> clean -> normalize -> validate -> match
            cleaner = DataCleaner()
            normalizer = DataNormalizer()
            validator = DataValidator(required_fields=["name", "email"])
            matcher = EntityMatcher()
            
            records = []  # assume loaded from processor
            transformed = []
            for record in records:
                r = cleaner.transform(record)
                r = normalizer.transform(r)
                r = validator.transform(r)
                r = matcher.transform(r)
                transformed.append(r)

            execution.status = "SUCCESS"
            execution.records_processed = len(records)
            execution.records_successful = len([r for r in transformed if r.get("_is_valid")])
            execution.records_failed = execution.records_processed - execution.records_successful
        except Exception as e:
            execution.status = "FAILED"
            execution.error_details = {"error": str(e)}
        finally:
            execution.end_time = datetime.utcnow()
            self.session.add(execution)
            self.session.commit()

    def cleanup_old_executions(self, days: int):
        from datetime import timedelta
        cutoff = datetime.utcnow() - timedelta(days=days)
        old = self.session.exec(
            select(job_executions.JobExecution).where(
                job_executions.JobExecution.created_at < cutoff
            )
        ).all()
        for e in old:
            self.session.delete(e)
        self.session.commit()
        