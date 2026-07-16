# ==============================================
# app/tasks/etl_tasks.py
# ==============================================
import asyncio
import os
import json
import shutil
import traceback
import hashlib
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path
from sqlmodel import Session, select
import pandas as pd

from app.core.enums import ProcessingStatus

from .celery_app import celery_app
from app.infrastructure.db.models.raw_data.file_registry import FileRegistry
from app.infrastructure.db.models.raw_data.raw_records import RawRecords
from app.infrastructure.db.models.staging.standardized_data import StandardizedData, StandardizedDataCreate
from app.infrastructure.db.models.raw_data.rejected_records import RejectedRecord
from app.infrastructure.db.models.processed.entities import Entity
from app.infrastructure.db.models.processed.entity_relationships import EntityRelationship
from app.infrastructure.db.models.etl_control.etl_jobs import EtlJob
from app.infrastructure.db.models.etl_control.job_executions import JobExecution
from app.infrastructure.db.models.etl_control.error_logs import ErrorType, ErrorLog, ErrorSeverity
from app.infrastructure.db.models.etl_control.quality_check_results import QualityCheckResult, QualityCheckResultCreate
from app.infrastructure.db.models.etl_control.quality_rules import QualityRule
from app.infrastructure.db.models.audit.data_lineage import DataLineage, DataLineageCreate
from app.infrastructure.db.models.audit.change_log import ChangeLog, ChangeLogCreate
from app.infrastructure.db.manager import get_session
from celery import group
from app.processors import get_processor
from app.transformers import create_transformation_pipeline
from app.application.services.etl_service import ETLService
from app.application.services.file_service import FileService
from app.application.services.data_quality_service import DataQualityService
from app.utils.logger import get_logger
from app.core.exceptions import ETLException, FileProcessingException
from app.utils.event_publisher import get_event_publisher
from app.tasks.task_helpers import log_task_error, get_error_type_from_exception, get_error_severity_from_exception

logger = get_logger(__name__)

def run_transformation_pipeline(self, job_execution_id: str, transformation_config: Dict[str, Any]):
    """
    Run data transformation pipeline

    Args:
        job_execution_id: ID of the job execution
        transformation_config: Transformation configuration

    Returns:
        Transformation results
    """
    task_id = self.request.id
    logger.info(f"Starting transformation pipeline task {task_id} for execution {job_execution_id}")

    with get_session() as db:
        try:
            # Get job execution record
            execution = db.exec(select(JobExecution).where(JobExecution.execution_id == job_execution_id)).first()
            if not execution:
                raise ETLException(f"Job execution not found: {job_execution_id}")

            # Update execution status
            execution.status = 'RUNNING'
            execution.start_time = datetime.utcnow()
            db.add(execution)
            db.commit()

            # Create transformation pipeline
            stages = transformation_config.get('stages', ['clean', 'validate', 'normalize'])
            pipeline = create_transformation_pipeline(stages, **transformation_config)

            # Get source data
            source_query = transformation_config.get('source_query')
            if source_query:
                source_data = pd.read_sql(source_query, db.connection())
                input_records = source_data.to_dict('records')
            else:
                # Default: get recent raw records
                recent_records = db.exec(
                    select(RawRecords)
                    .where(RawRecords.validation_status == 'VALID')
                    .limit(transformation_config.get('limit', 10000))
                ).all()
                input_records = [record.raw_data for record in recent_records]

            # Execute transformation pipeline
            total_results = {}
            for stage_idx, transformer in enumerate(pipeline):
                stage_name = stages[stage_idx] if stage_idx < len(stages) else f"stage_{stage_idx}"
                logger.info(f"Executing transformation stage: {stage_name}")

                # Transform the data
                if stage_idx == 0:
                    # First stage uses input records
                    stage_results = asyncio.run(transformer.transform_dataset(
                        input_records,
                        output_entity_type=transformation_config.get('output_entity_type')
                    ))
                else:
                    # Subsequent stages use results from previous stage
                    previous_results = total_results[stages[stage_idx - 1]]['results']
                    stage_data = [result.data for result in previous_results if result.is_success()]
                    stage_results = asyncio.run(transformer.transform_dataset(
                        stage_data,
                        output_entity_type=transformation_config.get('output_entity_type')
                    ))

                total_results[stage_name] = stage_results

                # Check if stage failed critically
                if not stage_results['success']:
                    raise ETLException(f"Transformation stage '{stage_name}' failed critically")

            # Update execution with results
            execution.status = 'SUCCESS'
            execution.end_time = datetime.utcnow()
            execution.records_processed = sum(r['statistics']['records_processed'] for r in total_results.values())
            execution.records_successful = sum(r['statistics']['records_transformed'] for r in total_results.values())
            execution.records_failed = sum(r['statistics']['records_failed'] for r in total_results.values())

            # Store performance metrics
            performance_metrics = {
                'total_processing_time': (execution.end_time - execution.start_time).total_seconds(),
                'stages_executed': len(stages),
                'stage_results': {stage: result['statistics'] for stage, result in total_results.items()},
                'task_id': task_id
            }
            execution.performance_metrics = performance_metrics

            db.add(execution)
            db.commit()

            logger.info(f"Transformation pipeline completed for execution {job_execution_id}")

            return {
                'status': 'success',
                'execution_id': job_execution_id,
                'task_id': task_id,
                'results': total_results,
                'performance_metrics': performance_metrics,
                'completed_at': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Transformation pipeline failed for execution {job_execution_id}: {str(e)}")

            # Update execution status
            try:
                if 'execution' in locals():
                    execution.status = 'FAILED'
                    execution.end_time = datetime.utcnow()
                    execution.execution_log = str(e)
                    db.add(execution)
                    db.commit()
            except Exception as commit_error:
                logger.error(f"Failed to update execution status: {commit_error}")

            # Retry if retries available
            if self.request.retries < self.max_retries:
                logger.info(f"Retrying transformation pipeline for {job_execution_id}")
                raise self.retry(exc=e, countdown=self.default_retry_delay)

            raise ETLException(f"Transformation pipeline failed: {str(e)}")


async def transform_records(
    db: Session,
    execution_id: str,
    transform_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Transform raw records through the complete transform pipeline.

    Flow:
    1. Fetch unprocessed raw records from database
    2. For each record:
        a. Data Cleansing: remove whitespace, normalize case, handle nulls
        b. Field Mapping: apply direct/calculated/lookup/constant transformations
        c. Data Validation: check completeness, uniqueness, validity, range, consistency
        d. Result Handling:
           - SUCCESS: Insert to standardized_data, mark is_processed=true
           - FAILURE: Insert to rejected_records, increment records_failed counter

    Args:
        db: Database session
        execution_id: Job execution ID
        transform_config: Transformation configuration including:
            - entity_type: Type of entity being transformed
            - batch_size: Number of records to process at once
            - cleaner_config: Data cleaner configuration
            - field_mappings: List of field mappings
            - quality_rules: Quality validation rules

    Returns:
        Dictionary with transformation statistics
    """
    from app.infrastructure.db.models.raw_data.raw_records import RawRecords
    from app.infrastructure.db.models.staging.standardized_data import StandardizedData, StandardizedDataCreate
    from app.infrastructure.db.models.raw_data.rejected_records import RejectedRecord
    from app.infrastructure.db.models.etl_control.job_executions import JobExecution
    from app.infrastructure.db.models.transformation.field_mappings import FieldMapping
    from app.infrastructure.db.models.etl_control.quality_rules import QualityRule
    from app.infrastructure.db.models.etl_control.quality_check_results import (
        QualityCheckResult,
        QualityCheckResultCreate
    )
    from app.transformers.data_cleaner import DataCleaner
    from app.transformers.data_validator import DataValidator
    from app.transformers.data_normalizer import DataNormalizer
    from app.application.services.field_mapping_service import FieldMappingService
    from app.core.enums import QualityCheckResult as QualityCheckResultEnum

    logger.info(f"[PHASE 5] Starting transform for execution {execution_id}")

    records_processed = 0
    records_successful = 0
    records_failed = 0
    errors = []
    logs = []

    try:
        # Get execution record to retrieve job_id
        execution = db.exec(select(JobExecution).where(JobExecution.execution_id == execution_id)).first()
        if not execution:
            raise ETLException(f"Job execution not found: {execution_id}")

        job_id = execution.job_id
        entity_type = transform_config.get("entity_type", "UNKNOWN")
        batch_size = transform_config.get("batch_size", 1000)

        logger.info(f"[PHASE 5] Job ID: {job_id}, Entity Type: {entity_type}")

        # Step 1: Query unprocessed raw records (validation_status = UNVALIDATED or VALID)
        logger.debug(f"[PHASE 5] Querying unprocessed raw records")
        from app.core.enums import ValidationStatus
        raw_records_query = select(RawRecords).where(
            RawRecords.validation_status.in_([ValidationStatus.UNVALIDATED, ValidationStatus.VALID])
        ).limit(batch_size)
        raw_records = db.exec(raw_records_query).all()

        if not raw_records:
            logger.info(f"[PHASE 5] No unprocessed records found")
            logs.append("No unprocessed records to transform")
            return {
                "records_processed": 0,
                "records_successful": 0,
                "records_failed": 0,
                "logs": logs,
                "performance_metrics": {}
            }

        logger.info(f"[PHASE 5] Found {len(raw_records)} unprocessed records")
        logs.append(f"Found {len(raw_records)} records to transform")

        # Initialize transformers
        cleaner_config = transform_config.get("cleaner_config", {})
        cleaner = DataCleaner(db, execution_id, **cleaner_config)

        validator_config = transform_config.get("validator_config", {})
        validator = DataValidator(db, execution_id, **validator_config)

        normalizer_config = transform_config.get("normalizer_config", {})
        normalizer = DataNormalizer(db, execution_id, **normalizer_config)

        field_mapping_service = FieldMappingService(db, job_id)

        # Fetch field mappings for this job
        field_mappings_query = select(FieldMapping).where(FieldMapping.job_id == job_id)
        field_mappings = db.exec(field_mappings_query).all()
        logger.debug(f"[PHASE 5] Loaded {len(field_mappings)} field mappings")

        # Fetch quality rules for this entity type
        quality_rules_query = select(QualityRule).where(
            QualityRule.is_active == True,
            QualityRule.entity_type == entity_type
        )
        quality_rules = db.exec(quality_rules_query).all()
        logger.debug(f"[PHASE 5] Loaded {len(quality_rules)} quality rules for {entity_type}")

        # Step 2: Process each raw record
        for raw_record in raw_records:
            records_processed += 1

            try:
                logger.debug(
                    f"[PHASE 5] Processing record {records_processed}/{len(raw_records)} "
                    f"(raw_record_id: {raw_record.id})"
                )

                # Step 2a: Data Cleansing
                logger.debug(f"[PHASE 5] Cleansing record {raw_record.id}")
                clean_result = await cleaner.transform_record(raw_record.raw_data)

                if not clean_result.is_success():
                    logger.warning(f"[PHASE 5] Data cleansing failed for record {raw_record.id}")
                    # Still continue with original data
                    cleaned_data = raw_record.raw_data
                else:
                    cleaned_data = clean_result.data

                # Step 2b: Field Mapping
                logger.debug(f"[PHASE 5] Applying field mappings to record {raw_record.id}")
                mapped_record, mapping_errors = await field_mapping_service.execute_mappings(
                    cleaned_data, field_mappings, execution_id
                )

                if mapping_errors:
                    logger.warning(
                        f"[PHASE 5] Field mapping errors for record {raw_record.id}: {mapping_errors}"
                    )
                    # If critical mapping errors, reject the record
                    if any(error for error in mapping_errors):
                        raise ETLException(f"Field mapping failed: {'; '.join(mapping_errors)}")

                # Step 2c: Data Validation
                logger.debug(f"[PHASE 5] Validating record {raw_record.id}")

                # Setup validator with quality rules
                validation_rules = {}
                for rule in quality_rules:
                    field = rule.field_name or "general"
                    if field not in validation_rules:
                        validation_rules[field] = []

                    validation_rules[field].append({
                        "type": rule.rule_type.value.lower(),
                        "severity": "error",  # Adjust based on rule configuration
                        "parameters": {"rule_id": str(rule.rule_id), "expression": rule.rule_expression},
                        "error_message": f"Quality rule '{rule.rule_name}' failed"
                    })

                # Validate the mapped record
                if validation_rules:
                    validator_with_rules = DataValidator(
                        db, execution_id,
                        validation_rules=validation_rules,
                        stop_on_first_error=False,
                        collect_all_errors=True
                    )
                    validation_result = await validator_with_rules.transform_record(mapped_record)
                else:
                    validation_result = await validator.transform_record(mapped_record)

                # Step 2d: Result Handling
                if validation_result.is_success() or validation_result.status.value == "warning":
                    # SUCCESS: Insert to standardized_data
                    logger.debug(f"[PHASE 5] Record {raw_record.id} validation PASSED")

                    standardized_record = StandardizedDataCreate(
                        source_file_id=raw_record.file_id,
                        source_record_id=raw_record.id,
                        entity_type=entity_type,
                        standardized_data=mapped_record,
                        quality_score=float(validation_result.metadata.get("quality_score", 0.95)),
                        transformation_rules_applied=[
                            f"{m.mapping_type}:{m.target_field}" for m in field_mappings[:5]  # Sample
                        ],
                        batch_id=execution_id,
                        validation_status='passed'
                    )

                    # Create StandardizedData model instance
                    standardized_data = StandardizedData.from_orm(standardized_record)
                    db.add(standardized_data)
                    db.flush()  # Flush to get the ID

                    # Insert quality check results for passed validation
                    for rule in quality_rules:
                        quality_check = QualityCheckResult(
                            execution_id=execution.id,
                            rule_id=rule.rule_id,
                            check_result=QualityCheckResultEnum.PASSED,
                            records_checked=1,
                            records_passed=1,
                            records_failed=0,
                            failure_details=None
                        )
                        db.add(quality_check)

                    # Mark raw record as processed
                    raw_record.validation_status = ValidationStatus.VALID
                    db.add(raw_record)

                    records_successful += 1
                    logger.debug(f"[PHASE 5] Record {raw_record.id} inserted to standardized_data")

                else:
                    # FAILURE: Insert to rejected_records
                    logger.warning(
                        f"[PHASE 5] Record {raw_record.id} validation FAILED: {validation_result.errors}"
                    )

                    rejected_record = RejectedRecord(
                        source_record_id=raw_record.id,
                        source_file_id=raw_record.file_id,
                        row_number=raw_record.row_number,
                        raw_data=raw_record.raw_data,
                        rejection_reason="; ".join(validation_result.errors),
                        validation_errors=[
                            {"error": error} for error in validation_result.errors
                        ],
                        batch_id=execution_id
                    )
                    db.add(rejected_record)

                    # Insert quality check results for failed validation
                    for rule in quality_rules:
                        quality_check = QualityCheckResult(
                            execution_id=execution.id,
                            rule_id=rule.rule_id,
                            check_result=QualityCheckResultEnum.FAILED,
                            records_checked=1,
                            records_passed=0,
                            records_failed=1,
                            failure_details={"errors": validation_result.errors}
                        )
                        db.add(quality_check)

                    records_failed += 1
                    logger.debug(f"[PHASE 5] Record {raw_record.id} inserted to rejected_records")

                db.commit()

            except Exception as e:
                db.rollback()
                error_msg = f"Error processing record {raw_record.id}: {str(e)}"
                logger.error(f"[PHASE 5] {error_msg}")
                errors.append(error_msg)
                records_failed += 1

                # Log the error for debugging
                continue

        # Step 3: Update job execution counters
        logger.info(
            f"[PHASE 5] Transform complete: {records_successful} successful, "
            f"{records_failed} failed out of {records_processed}"
        )

        execution.records_transformed = records_successful
        execution.records_failed = records_failed
        db.add(execution)
        db.commit()

        logs.append(f"Transformed {records_successful} records successfully")
        logs.append(f"Rejected {records_failed} records")

        return {
            "records_processed": records_processed,
            "records_successful": records_successful,
            "records_failed": records_failed,
            "logs": logs,
            "performance_metrics": {
                "success_rate": (records_successful / records_processed * 100) if records_processed > 0 else 0,
                "failure_rate": (records_failed / records_processed * 100) if records_processed > 0 else 0
            }
        }

    except Exception as e:
        logger.error(f"[PHASE 5] Transform failed: {str(e)}", exc_info=True)
        errors.append(f"Transform phase failed: {str(e)}")
        logs.append(f"Transform failed: {str(e)}")

        # Update execution status
        try:
            execution = db.exec(select(JobExecution).where(JobExecution.execution_id == execution_id)).first()
            if execution:
                execution.records_failed += records_failed
                db.add(execution)
                db.commit()
        except Exception as commit_error:
            logger.error(f"Failed to update execution counters: {commit_error}")

        raise ETLException(f"Data transformation failed: {str(e)}") from e


# Helper functions for job execution


async def _execute_transform_job(db: Session, execution_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Execute transform job - Phase 5 of ETL pipeline"""
    logger.info(f"Executing transform job for execution {execution_id}")

    # Call the transform_records function
    transform_result = await transform_records(
        db=db,
        execution_id=execution_id,
        transform_config=config
    )

    return {
        'records_processed': transform_result.get('records_processed', 0),
        'records_successful': transform_result.get('records_successful', 0),
        'records_failed': transform_result.get('records_failed', 0),
        'logs': transform_result.get('logs', [])
    }

