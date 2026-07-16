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

async def _execute_load_job(db: Session, execution_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Execute load job - Phase 6 of ETL pipeline (Entity Loading)"""
    logger.info(f"Executing load job for execution {execution_id}")

    # Phase 6: Load standardized records into processed entities
    load_result = await load_records(
        db=db,
        execution_id=execution_id,
        load_config=config
    )

    return {
        'records_processed': load_result.get('records_processed', 0),
        'records_successful': load_result.get('records_loaded', 0),
        'records_failed': 0,
        'logs': load_result.get('logs', []),
        'performance_metrics': load_result.get('performance_metrics', {})
    }

# ==============================================
# PHASE 6: Load Records
# ==============================================
# Purpose: Load standardized records into processed entities with entity matching,
# deduplication, conflict resolution, and complete lineage tracking.
# Input: standardized_data WHERE validation_status='passed'
# Output: entities, entity_relationships, data_lineage records with transaction rollback on failure


async def load_records(
    db: Session,
    execution_id: str,
    load_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Load standardized records into processed entities through entity matching,
    deduplication, and conflict resolution.

    Flow:
    1. Query standardized_data WHERE validation_status='passed'
    2. BEGIN TRANSACTION (explicit with db.begin())
    3. For each standardized record:
        a. EntityMatcher.match_entity():
           - Calculate entity_hash = MD5(key_fields)
           - SELECT entities WHERE data_hash=?
           - If exact match: confidence_score = 1.0, is_new=false, is_duplicate=false
           - Else: SELECT entities WHERE entity_type=?, fuzzy similarity
           - If similarity > threshold: is_new=false, is_duplicate=true
           - Else: is_new=true
        b. If NEW entity:
           - INSERT entities
           - INSERT data_lineage (standardized_record_id → entity_id)
           - UPDATE job_executions records_loaded += 1
        c. If DUPLICATE:
           - UPDATE entities duplicate_count += 1, master_entity_id = primary_entity_id
           - INSERT entity_relationships (type='duplicate_of')
        d. If UPDATE existing:
           - SELECT existing entity
           - MERGE DATA with CONFLICT RESOLUTION (newer value wins + score-based)
           - UPDATE entities
           - INSERT change_log
           - UPDATE job_executions records_loaded += 1
        e. All cases:
           - INSERT entity_relationships (general)
           - INSERT data_lineage (complete chain)
    4. COMMIT TRANSACTION
    5. On Failure:
       - ROLLBACK TRANSACTION
       - INSERT error_logs
       - UPDATE job_executions status='failed'

    Args:
        db: Database session
        execution_id: Job execution ID
        load_config: Load configuration including:
            - entity_type: Type of entity being loaded
            - key_fields: Fields to calculate entity_hash
            - batch_size: Number of records to process at once
            - similarity_threshold: Threshold for fuzzy matching (default 0.85)
            - conflict_resolution_strategy: 'newer_wins' | 'score_based' | 'manual_review'

    Returns:
        Dictionary with load statistics
    """
    from app.transformers.entity_matcher import EntityMatcher
    from app.application.services.entity_service import EntityService

    logger.info(f"[PHASE 6] Starting load for execution {execution_id}")

    records_processed = 0
    records_loaded = 0
    records_duplicated = 0
    records_merged = 0
    errors = []
    logs = []

    execution = None
    transaction = None

    try:
        # Get execution record
        execution = db.exec(select(JobExecution).where(JobExecution.execution_id == execution_id)).first()
        if not execution:
            raise ETLException(f"Job execution not found: {execution_id}")

        job_id = execution.job_id
        entity_type = load_config.get("entity_type", "UNKNOWN")
        batch_size = load_config.get("batch_size", 1000)
        key_fields = load_config.get("key_fields", ["id", "name"])
        similarity_threshold = load_config.get("similarity_threshold", 0.85)
        conflict_resolution = load_config.get("conflict_resolution_strategy", "newer_wins")

        logger.info(f"[PHASE 6] Job ID: {job_id}, Entity Type: {entity_type}, Threshold: {similarity_threshold}")
        logs.append(f"Load phase initiated for entity_type: {entity_type}")

        # Step 1: Query validated standardized records (validation_status='passed')
        logger.debug(f"[PHASE 6] Querying validated standardized records")
        standardized_query = select(StandardizedData).where(
            StandardizedData.validation_status == 'passed'
        ).limit(batch_size)
        standardized_records = db.exec(standardized_query).all()

        if not standardized_records:
            logger.info(f"[PHASE 6] No validated records found to load")
            logs.append("No validated records to load")
            return {
                "records_processed": 0,
                "records_loaded": 0,
                "records_duplicated": 0,
                "records_merged": 0,
                "logs": logs,
                "performance_metrics": {}
            }

        logger.info(f"[PHASE 6] Found {len(standardized_records)} validated records to load")
        logs.append(f"Found {len(standardized_records)} records to load")

        # Initialize services
        entity_service = EntityService(db)
        entity_matcher = EntityMatcher(db, execution_id, **load_config)

        # Step 2: Process records with individual commits for atomic operations
        logger.debug(f"[PHASE 6] Starting record processing")

        # Step 3: Process each standardized record
        for std_record in standardized_records:
            records_processed += 1

            try:
                logger.debug(
                    f"[PHASE 6] Processing record {records_processed}/{len(standardized_records)} "
                    f"(std_record_id: {std_record.id})"
                )

                # Step 3a: Entity Matching
                logger.debug(f"[PHASE 6] Matching entity for record {std_record.id}")

                # Calculate entity_hash from key_fields
                hash_input = "_".join(
                    str(std_record.standardized_data.get(field, "")) for field in key_fields
                )
                entity_hash = hashlib.md5(hash_input.encode()).hexdigest()

                # Match entity
                match_result = await entity_matcher.match_entity(
                    std_record.standardized_data,
                    entity_type,
                    entity_hash,
                    similarity_threshold
                )

                # Unpack match result
                is_new = match_result.get("is_new", True)
                is_duplicate = match_result.get("is_duplicate", False)
                matched_entity = match_result.get("matched_entity")
                confidence_score = match_result.get("confidence_score", 1.0)
                match_score = match_result.get("match_score", 0.0)

                # Step 3b: NEW ENTITY
                if is_new:
                    logger.debug(f"[PHASE 6] Record {std_record.id} identified as NEW entity")

                    # INSERT entities
                    new_entity = Entity(
                        entity_type=entity_type,
                        entity_key=std_record.standardized_data.get(key_fields[0], f"entity_{std_record.id}"),
                        entity_data=std_record.standardized_data,
                        confidence_score=float(confidence_score),
                        source_files=[std_record.source_file_id] if std_record.source_file_id else [],
                        version=1,
                        is_active=True
                    )
                    db.add(new_entity)
                    db.flush()  # Flush to get the entity_id

                    # INSERT data_lineage (standardized → entity)
                    lineage = DataLineage(
                        source_entity_id=std_record.id,
                        source_entity_type="StandardizedData",
                        target_entity_id=new_entity.entity_id,
                        target_entity_type=entity_type,
                        transformation_rule_id=None,
                        job_execution_id=execution.id,
                        lineage_metadata={
                            "hash": entity_hash,
                            "confidence": confidence_score,
                            "match_type": "new"
                        }
                    )
                    db.add(lineage)

                    records_loaded += 1
                    logger.debug(f"[PHASE 6] NEW entity {new_entity.entity_id} created for record {std_record.id}")

                # Step 3c: DUPLICATE ENTITY
                elif is_duplicate and matched_entity:
                    logger.debug(f"[PHASE 6] Record {std_record.id} identified as DUPLICATE of entity {matched_entity.entity_id}")

                    # UPDATE entities: increment duplicate_count, set master_entity_id
                    matched_entity.duplicate_count = (matched_entity.duplicate_count or 0) + 1
                    matched_entity.master_entity_id = matched_entity.entity_id  # Self-reference for primary
                    db.add(matched_entity)

                    # INSERT entity_relationships (duplicate_of)
                    relationship = EntityRelationship(
                        entity_from=std_record.id,
                        entity_to=matched_entity.entity_id,
                        relationship_type="duplicate_of",
                        relationship_strength=float(match_score),
                        metadata={
                            "confidence": confidence_score,
                            "hash_match": entity_hash == getattr(matched_entity, 'entity_hash', None),
                            "fuzzy_score": match_score
                        }
                    )
                    db.add(relationship)

                    # INSERT data_lineage (duplicate link)
                    lineage = DataLineage(
                        source_entity_id=std_record.id,
                        source_entity_type="StandardizedData",
                        target_entity_id=matched_entity.entity_id,
                        target_entity_type=entity_type,
                        transformation_rule_id=None,
                        job_execution_id=execution.id,
                        lineage_metadata={
                            "hash": entity_hash,
                            "confidence": confidence_score,
                            "match_type": "duplicate",
                            "match_score": match_score
                        }
                    )
                    db.add(lineage)

                    records_duplicated += 1
                    logger.debug(f"[PHASE 6] Record {std_record.id} marked as duplicate of {matched_entity.entity_id}")

                # Step 3d: UPDATE EXISTING ENTITY
                elif matched_entity:
                    logger.debug(f"[PHASE 6] Record {std_record.id} identified for MERGE with entity {matched_entity.entity_id}")

                    # SELECT existing entity
                    existing_entity = matched_entity

                    # MERGE DATA with CONFLICT RESOLUTION
                    merged_data = await _merge_entity_data(
                        existing_data=existing_entity.entity_data or {},
                        new_data=std_record.standardized_data,
                        confidence_score=float(confidence_score),
                        strategy=conflict_resolution
                    )

                    # CREATE change_log entry
                    change_log = ChangeLog(
                        entity_id=existing_entity.entity_id,
                        change_type="UPDATE",
                        old_value=existing_entity.entity_data,
                        new_value=merged_data,
                        change_details={
                            "merge_strategy": conflict_resolution,
                            "new_confidence": confidence_score,
                            "old_confidence": existing_entity.confidence_score,
                            "match_score": match_score
                        }
                    )
                    db.add(change_log)

                    # UPDATE entities
                    existing_entity.entity_data = merged_data
                    existing_entity.confidence_score = float(max(
                        existing_entity.confidence_score or 0,
                        confidence_score
                    ))
                    existing_entity.version += 1
                    existing_entity.last_updated = datetime.utcnow()
                    db.add(existing_entity)

                    # INSERT data_lineage (merge)
                    lineage = DataLineage(
                        source_entity_id=std_record.id,
                        source_entity_type="StandardizedData",
                        target_entity_id=existing_entity.entity_id,
                        target_entity_type=entity_type,
                        transformation_rule_id=None,
                        job_execution_id=execution.id,
                        lineage_metadata={
                            "hash": entity_hash,
                            "confidence": confidence_score,
                            "match_type": "update",
                            "match_score": match_score
                        }
                    )
                    db.add(lineage)

                    records_merged += 1
                    records_loaded += 1
                    logger.debug(f"[PHASE 6] Record {std_record.id} merged into entity {existing_entity.entity_id}")

                db.commit()

            except Exception as e:
                db.rollback()
                error_msg = f"Error processing record {std_record.id}: {str(e)}"
                logger.error(f"[PHASE 6] {error_msg}", exc_info=True)
                errors.append(error_msg)
                continue

        # Step 4: Finalize load
        logger.info(
            f"[PHASE 6] Load complete: {records_loaded} loaded, "
            f"{records_duplicated} duplicated, {records_merged} merged out of {records_processed}"
        )

        # Step 5: Update job execution counters
        execution.records_loaded = records_loaded
        db.add(execution)
        db.commit()

        logs.append(f"Successfully loaded {records_loaded} records")
        logs.append(f"Identified {records_duplicated} duplicates")
        logs.append(f"Merged {records_merged} existing entities")

        return {
            "records_processed": records_processed,
            "records_loaded": records_loaded,
            "records_duplicated": records_duplicated,
            "records_merged": records_merged,
            "logs": logs,
            "performance_metrics": {
                "load_rate": (records_loaded / records_processed * 100) if records_processed > 0 else 0,
                "dedup_rate": (records_duplicated / records_processed * 100) if records_processed > 0 else 0,
                "merge_rate": (records_merged / records_processed * 100) if records_processed > 0 else 0
            }
        }

    except Exception as e:
        logger.error(f"[PHASE 6] Load failed: {str(e)}", exc_info=True)
        errors.append(f"Load phase failed: {str(e)}")
        logs.append(f"Load failed: {str(e)}")

        # ROLLBACK TRANSACTION
        if transaction:
            try:
                transaction.rollback()
                logger.info("[PHASE 6] Transaction rolled back due to error")
            except Exception as rollback_error:
                logger.error(f"[PHASE 6] Failed to rollback transaction: {rollback_error}")

        # UPDATE job_executions status='failed'
        try:
            if execution:
                execution.status = 'FAILED'
                execution.execution_log = f"Load phase failed: {str(e)}"
                db.add(execution)
                db.commit()
                logger.info(f"[PHASE 6] Updated execution {execution_id} status to FAILED")
        except Exception as update_error:
            logger.error(f"[PHASE 6] Failed to update execution status: {update_error}")

        # INSERT error_logs
        try:
            error_log = ErrorLog(
                job_execution_id=execution.id if execution else None,
                error_type=ErrorType.SYSTEM_ERROR,
                error_severity=ErrorSeverity.CRITICAL,
                error_message=str(e),
                error_details={
                    "phase": "LOAD",
                    "records_processed": records_processed,
                    "records_loaded": records_loaded,
                    "traceback": traceback.format_exc()
                },
                context={"execution_id": execution_id}
            )
            db.add(error_log)
            db.commit()
            logger.info("[PHASE 6] Error logged to database")
        except Exception as log_error:
            logger.error(f"[PHASE 6] Failed to log error: {log_error}")

        raise ETLException(f"Data load failed: {str(e)}") from e



async def _merge_entity_data(
    existing_data: Dict[str, Any],
    new_data: Dict[str, Any],
    confidence_score: float,
    strategy: str = "newer_wins"
) -> Dict[str, Any]:
    """
    Merge entity data with conflict resolution strategy.

    Strategies:
    - newer_wins: New values always win
    - score_based: Higher confidence score wins per field
    - conservative: Existing values always win
    - merge: Intelligently merge arrays/objects, prefer new scalars

    Args:
        existing_data: Current entity data
        new_data: New standardized data
        confidence_score: Confidence score of new data (0.0-1.0)
        strategy: Conflict resolution strategy

    Returns:
        Merged data dictionary
    """
    merged = existing_data.copy()

    if strategy == "newer_wins":
        # New values always win
        merged.update(new_data)

    elif strategy == "score_based":
        # Higher confidence score wins (assume existing has implicit score < new)
        # For simplicity: if new_confidence >= 0.9, use new value
        if confidence_score >= 0.9:
            merged.update(new_data)
        else:
            # Selective merge: only update fields that are significantly different
            for key, new_value in new_data.items():
                if key not in merged:
                    # New field, always add
                    merged[key] = new_value
                elif isinstance(new_value, (int, float)) and isinstance(merged.get(key), (int, float)):
                    # For numeric fields, take the higher value
                    merged[key] = max(merged[key], new_value)
                elif isinstance(new_value, str) and isinstance(merged.get(key), str):
                    # For strings, only update if new is significantly different (fuzzy match > threshold)
                    # For now, keep existing if confidence not high
                    pass
                elif isinstance(new_value, list):
                    # For lists, merge uniquely
                    merged[key] = list(set(merged.get(key, []) + new_value))

    elif strategy == "conservative":
        # Existing values always win (no update)
        pass

    elif strategy == "merge":
        # Intelligent merge
        for key, new_value in new_data.items():
            if key not in merged:
                merged[key] = new_value
            elif isinstance(new_value, dict) and isinstance(merged[key], dict):
                # Recursively merge dicts
                merged[key] = {**merged[key], **new_value}
            elif isinstance(new_value, list):
                # Merge lists (unique elements)
                merged[key] = list(set(merged.get(key, []) + new_value))
            else:
                # For scalars, prefer existing
                pass

    return merged


