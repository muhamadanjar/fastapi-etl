"""
Job Orchestration Service for managing job dependencies and triggering dependent jobs.
Handles child job discovery, parent completion verification, and atomic job triggering.

This service is responsible for:
1. Discovering dependent jobs for a parent job
2. Verifying all parents of a child job are completed
3. Atomically triggering child jobs with proper transaction handling
4. Logging all orchestration events
5. Handling circular dependency detection (optional but recommended)
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from uuid import UUID
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, or_

from app.application.services.base import BaseService
from app.infrastructure.db.models.etl_control.job_dependencies import (
    JobDependency,
    DependencyType
)
from app.infrastructure.db.models.etl_control.etl_jobs import EtlJob, ExecutionStatus
from app.infrastructure.db.models.etl_control.job_executions import JobExecution
from app.core.exceptions import ETLException, ServiceError
from app.utils.logger import get_logger

logger = get_logger(__name__)


class JobOrchestrationService(BaseService):
    """
    Service for orchestrating job dependencies and triggering dependent jobs.

    Responsibilities:
    - Discover dependent (child) jobs for a parent job
    - Verify all parent jobs of a child are completed
    - Trigger child job execution atomically
    - Track orchestration events
    - Prevent infinite loops and circular dependencies
    """

    def __init__(self, db: Session):
        super().__init__(db)
        self.logger = logger

    def get_service_name(self) -> str:
        return "JobOrchestrationService"

    async def trigger_dependent_jobs(
        self,
        parent_job_id: UUID,
        parent_execution_id: Optional[UUID] = None,
        parent_status: str = "SUCCESS"
    ) -> Dict[str, Any]:
        """
        Discover and trigger all dependent jobs of a parent job.

        This method:
        1. Finds all direct children of the parent_job_id
        2. For each child, verifies all parents are completed
        3. Atomically creates execution record and queues Celery task
        4. Returns summary of triggered jobs

        Args:
            parent_job_id: UUID of the parent job that just completed
            parent_execution_id: Optional execution ID for context tracking
            parent_status: Status of parent job ('SUCCESS', 'FAILURE', etc.)

        Returns:
            Dictionary with:
            - total_triggered: Number of jobs successfully triggered
            - triggered_jobs: List of triggered job details
            - skipped_jobs: List of jobs that weren't triggered with reasons
            - errors: Any errors encountered

        Raises:
            ETLException: If unable to get parent job or other critical errors
        """
        self.logger.info(
            f"[ORCHESTRATION] Starting job orchestration for parent {parent_job_id} "
            f"(status: {parent_status}, execution: {parent_execution_id})"
        )

        triggered_jobs = []
        skipped_jobs = []
        errors = []

        try:
            # Step 1: Validate parent job exists
            parent_job = self.db.exec(
                select(EtlJob).where(EtlJob.id == parent_job_id)
            ).first()

            if not parent_job:
                raise ETLException(f"Parent job {parent_job_id} not found")

            self.logger.debug(
                f"[ORCHESTRATION] Parent job found: {parent_job.job_name} ({parent_job_id})"
            )

            # Step 2: Find all direct children
            child_dependencies = await self._get_child_jobs(parent_job_id)

            if not child_dependencies:
                self.logger.info(f"[ORCHESTRATION] No dependent jobs for {parent_job_id}")
                return {
                    "total_triggered": 0,
                    "triggered_jobs": [],
                    "skipped_jobs": [],
                    "errors": []
                }

            self.logger.info(
                f"[ORCHESTRATION] Found {len(child_dependencies)} dependent jobs for {parent_job_id}"
            )

            # Step 3: Process each dependent job
            for dependency in child_dependencies:
                child_job_id = dependency.child_job_id
                dependency_type = dependency.dependency_type

                self.logger.debug(
                    f"[ORCHESTRATION] Processing child job {child_job_id} "
                    f"(dependency_type: {dependency_type})"
                )

                try:
                    # Step 4: Determine if we should trigger this child
                    should_trigger = await self._should_trigger_job(
                        child_job_id,
                        parent_status,
                        dependency_type
                    )

                    if not should_trigger:
                        skip_reason = self._get_skip_reason(parent_status, dependency_type)
                        skipped_jobs.append({
                            "child_job_id": str(child_job_id),
                            "reason": skip_reason
                        })
                        self.logger.debug(
                            f"[ORCHESTRATION] Skipping {child_job_id}: {skip_reason}"
                        )
                        continue

                    # Step 5: Verify all parents of child are completed
                    parents_completed, incomplete_parents = await self._check_all_parents_completed(
                        child_job_id
                    )

                    if not parents_completed:
                        skip_reason = f"Incomplete parents: {[str(p) for p in incomplete_parents]}"
                        skipped_jobs.append({
                            "child_job_id": str(child_job_id),
                            "reason": skip_reason
                        })
                        self.logger.debug(
                            f"[ORCHESTRATION] Skipping {child_job_id}: {skip_reason}"
                        )
                        continue

                    # Step 6: Trigger the job
                    trigger_result = await self._trigger_job_execution(
                        child_job_id,
                        parent_job_id,
                        parent_execution_id
                    )

                    triggered_jobs.append(trigger_result)
                    self.logger.info(
                        f"[ORCHESTRATION] Triggered child job {child_job_id} "
                        f"(execution: {trigger_result['execution_id']})"
                    )

                except Exception as job_error:
                    error_msg = f"Error triggering job {child_job_id}: {str(job_error)}"
                    errors.append(error_msg)
                    self.logger.error(f"[ORCHESTRATION] {error_msg}")
                    # Continue processing other jobs
                    continue

            result = {
                "total_triggered": len(triggered_jobs),
                "triggered_jobs": triggered_jobs,
                "skipped_jobs": skipped_jobs,
                "errors": errors
            }

            self.logger.info(
                f"[ORCHESTRATION] Orchestration complete: "
                f"{len(triggered_jobs)} triggered, "
                f"{len(skipped_jobs)} skipped, "
                f"{len(errors)} errors"
            )

            return result

        except Exception as e:
            error_msg = f"Job orchestration failed for {parent_job_id}: {str(e)}"
            self.logger.error(f"[ORCHESTRATION] {error_msg}", exc_info=True)
            raise ETLException(error_msg) from e

    async def _get_child_jobs(self, parent_job_id: UUID) -> List[JobDependency]:
        """
        Get all active child dependencies for a parent job.

        Args:
            parent_job_id: Parent job ID

        Returns:
            List of JobDependency records where this job is the parent
        """
        try:
            self.logger.debug(f"[ORCHESTRATION] Querying child jobs for parent {parent_job_id}")

            dependencies = self.db.exec(
                select(JobDependency)
                .where(
                    and_(
                        JobDependency.parent_job_id == parent_job_id,
                        JobDependency.is_active == True
                    )
                )
            ).all()

            self.logger.debug(f"[ORCHESTRATION] Found {len(dependencies)} child dependencies")
            return dependencies

        except Exception as e:
            self.logger.error(f"[ORCHESTRATION] Failed to get child jobs: {str(e)}")
            raise ETLException(f"Failed to query child jobs: {str(e)}") from e

    async def _should_trigger_job(
        self,
        child_job_id: UUID,
        parent_status: str,
        dependency_type: DependencyType
    ) -> bool:
        """
        Determine if a child job should be triggered based on parent status and dependency type.

        Rules:
        - DependencyType.SUCCESS: Only trigger if parent_status == 'SUCCESS'
        - DependencyType.COMPLETION: Always trigger (regardless of parent status)
        - DependencyType.DATA_AVAILABILITY: Check if parent produced data (future enhancement)

        Args:
            child_job_id: Child job to evaluate
            parent_status: Status of parent job ('SUCCESS', 'FAILED', etc.)
            dependency_type: Type of dependency

        Returns:
            True if job should be triggered, False otherwise
        """
        self.logger.debug(
            f"[ORCHESTRATION] Evaluating trigger criteria for {child_job_id}: "
            f"parent_status={parent_status}, dependency_type={dependency_type}"
        )

        if dependency_type == DependencyType.SUCCESS:
            # Only trigger if parent succeeded
            should_trigger = parent_status.upper() == "SUCCESS"
            self.logger.debug(
                f"[ORCHESTRATION] SUCCESS dependency: should_trigger={should_trigger}"
            )
            return should_trigger

        elif dependency_type == DependencyType.COMPLETION:
            # Always trigger when parent completes (success or failure)
            self.logger.debug(
                f"[ORCHESTRATION] COMPLETION dependency: always trigger"
            )
            return True

        elif dependency_type == DependencyType.DATA_AVAILABILITY:
            # Future: Check if parent actually produced data
            # For now, always trigger
            self.logger.debug(
                f"[ORCHESTRATION] DATA_AVAILABILITY dependency: "
                f"trigger (data check not yet implemented)"
            )
            return True

        else:
            self.logger.warning(
                f"[ORCHESTRATION] Unknown dependency type: {dependency_type}"
            )
            return False

    async def _check_all_parents_completed(
        self,
        child_job_id: UUID
    ) -> Tuple[bool, List[UUID]]:
        """
        Verify that ALL parent jobs of a child have completed.

        This is critical: A child job should only be triggered when ALL its parents
        are complete, not just the immediate parent that triggered this check.

        Algorithm:
        1. Get all parent dependencies for this child
        2. For each parent, get latest execution record
        3. Check if latest execution status is 'SUCCESS' or 'COMPLETED'
        4. Return (all_complete, list_of_incomplete_parents)

        Args:
            child_job_id: Child job to check

        Returns:
            Tuple of (all_parents_completed: bool, incomplete_parent_ids: List[UUID])
        """
        self.logger.debug(f"[ORCHESTRATION] Checking parent completion for {child_job_id}")

        try:
            # Get all parent dependencies for this child
            parent_dependencies = self.db.exec(
                select(JobDependency)
                .where(
                    and_(
                        JobDependency.child_job_id == child_job_id,
                        JobDependency.is_active == True
                    )
                )
            ).all()

            if not parent_dependencies:
                self.logger.debug(
                    f"[ORCHESTRATION] No parent dependencies found for {child_job_id}"
                )
                return (True, [])

            self.logger.debug(
                f"[ORCHESTRATION] Found {len(parent_dependencies)} parent dependencies "
                f"for {child_job_id}"
            )

            incomplete_parents = []

            # Check each parent
            for parent_dep in parent_dependencies:
                parent_job_id = parent_dep.parent_job_id

                # Get latest execution of parent
                latest_execution = self.db.exec(
                    select(JobExecution)
                    .where(JobExecution.job_id == parent_job_id)
                    .order_by(JobExecution.created_at.desc())
                    .limit(1)
                ).first()

                if not latest_execution:
                    # Parent has no execution record yet
                    self.logger.warning(
                        f"[ORCHESTRATION] No execution found for parent {parent_job_id}"
                    )
                    incomplete_parents.append(parent_job_id)
                    continue

                # Check if execution is completed
                is_completed = latest_execution.status.upper() in [
                    "SUCCESS",
                    "COMPLETED",
                    "FINISHED"
                ]

                self.logger.debug(
                    f"[ORCHESTRATION] Parent {parent_job_id} status: "
                    f"{latest_execution.status} (completed={is_completed})"
                )

                if not is_completed:
                    incomplete_parents.append(parent_job_id)

            all_completed = len(incomplete_parents) == 0

            self.logger.debug(
                f"[ORCHESTRATION] Parent completion check result: "
                f"all_completed={all_completed}, "
                f"incomplete={[str(p) for p in incomplete_parents]}"
            )

            return (all_completed, incomplete_parents)

        except Exception as e:
            self.logger.error(
                f"[ORCHESTRATION] Error checking parent completion: {str(e)}",
                exc_info=True
            )
            raise ETLException(f"Failed to check parent completion: {str(e)}") from e

    async def _trigger_job_execution(
        self,
        child_job_id: UUID,
        parent_job_id: UUID,
        parent_execution_id: Optional[UUID] = None
    ) -> Dict[str, Any]:
        """
        Atomically trigger a child job execution.

        This performs:
        1. Create JobExecution record with status='PENDING'
        2. Store parent context for traceability
        3. Queue Celery task with apply_async()
        4. Return execution details

        Args:
            child_job_id: Child job to trigger
            parent_job_id: Parent job that triggered this (for traceability)
            parent_execution_id: Optional parent execution ID

        Returns:
            Dictionary with:
            - execution_id: UUID of created execution
            - child_job_id: UUID of triggered job
            - child_job_name: Name of triggered job
            - celery_task_id: Celery task ID
            - triggered_at: Timestamp
            - parent_context: Traceability info
        """
        self.logger.debug(
            f"[ORCHESTRATION] Triggering execution for job {child_job_id} "
            f"(triggered by parent {parent_job_id})"
        )

        try:
            # Step 1: Get child job details
            child_job = self.db.exec(
                select(EtlJob).where(EtlJob.id == child_job_id)
            ).first()

            if not child_job:
                raise ETLException(f"Child job {child_job_id} not found")

            if not child_job.is_active:
                raise ETLException(f"Child job {child_job_id} is not active")

            self.logger.debug(
                f"[ORCHESTRATION] Child job loaded: {child_job.job_name} "
                f"({child_job_id})"
            )

            # Step 2: Create JobExecution record atomically
            execution = JobExecution(
                job_id=child_job_id,
                status="PENDING",
                started_at=None,  # Will be set when task starts
                completed_at=None,
                execution_log=f"Triggered by dependent job orchestration (parent: {parent_job_id})",
                triggered_by_parent_job_id=parent_job_id,
                parent_execution_id=parent_execution_id
            )

            self.db.add(execution)
            self.db.commit()
            self.db.refresh(execution)

            execution_id = execution.id

            self.logger.debug(
                f"[ORCHESTRATION] JobExecution created: {execution_id} "
                f"(status=PENDING)"
            )

            # Step 3: Queue Celery task
            try:
                from app.tasks.etl_tasks import execute_etl_job

                celery_task = execute_etl_job.apply_async(
                    args=[str(child_job_id), str(execution_id)],
                    task_id=f"etl-{execution_id}",
                    retry=True,
                    retry_policy={
                        "max_retries": 2,
                        "interval_start": 10,
                        "interval_step": 0.2,
                        "interval_max": 0.2,
                    }
                )

                celery_task_id = celery_task.id

                self.logger.info(
                    f"[ORCHESTRATION] Celery task queued: {celery_task_id} "
                    f"for execution {execution_id}"
                )

            except Exception as celery_error:
                # Celery queueing failed - mark execution as failed
                self.logger.error(
                    f"[ORCHESTRATION] Failed to queue Celery task: {str(celery_error)}",
                    exc_info=True
                )

                execution.status = "FAILED"
                execution.execution_log = f"Failed to queue task: {str(celery_error)}"
                self.db.add(execution)
                self.db.commit()

                raise ETLException(
                    f"Failed to queue job execution: {str(celery_error)}"
                ) from celery_error

            # Step 4: Return execution details
            result = {
                "execution_id": str(execution_id),
                "child_job_id": str(child_job_id),
                "child_job_name": child_job.job_name,
                "celery_task_id": celery_task_id,
                "triggered_at": datetime.utcnow().isoformat(),
                "parent_context": {
                    "parent_job_id": str(parent_job_id),
                    "parent_execution_id": str(parent_execution_id) if parent_execution_id else None
                }
            }

            self.logger.info(
                f"[ORCHESTRATION] Job execution triggered successfully: "
                f"execution_id={execution_id}, celery_task_id={celery_task_id}"
            )

            return result

        except ETLException:
            raise
        except Exception as e:
            self.logger.error(
                f"[ORCHESTRATION] Failed to trigger job execution: {str(e)}",
                exc_info=True
            )
            raise ETLException(f"Failed to trigger job execution: {str(e)}") from e

    def _get_skip_reason(self, parent_status: str, dependency_type: DependencyType) -> str:
        """
        Generate a human-readable skip reason.

        Args:
            parent_status: Status of parent job
            dependency_type: Type of dependency

        Returns:
            Skip reason string
        """
        if dependency_type == DependencyType.SUCCESS:
            if parent_status.upper() != "SUCCESS":
                return f"Parent job {parent_status.lower()}, SUCCESS dependency requires SUCCESS status"

        return f"Dependency criteria not met (parent_status={parent_status}, type={dependency_type})"
