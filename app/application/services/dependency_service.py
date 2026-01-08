"""
Dependency service untuk managing job dependencies.
Handles dependency validation, circular dependency detection, dan dependency tree management.
"""

from typing import Dict, Any, List, Optional, Set
from uuid import UUID
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, or_
from app.application.services.base import BaseService
from app.infrastructure.db.models.etl_control.job_dependencies import (
    JobDependency, 
    DependencyType,
    JobDependencyCreate,
    JobDependencyUpdate
)
from app.infrastructure.db.models.etl_control.etl_jobs import EtlJob
from app.infrastructure.db.models.etl_control.job_executions import JobExecution, ExecutionStatus
from app.core.exceptions import ETLError


class DependencyError(ETLError):
    """Exception raised for dependency-related errors."""
    pass


class DependencyService(BaseService):
    """Service untuk managing job dependencies."""
    
    def __init__(self, db_session: Session):
        super().__init__(db_session)
    
    def get_service_name(self) -> str:
        return "DependencyService"
    
    async def add_dependency(
        self, 
        parent_job_id: UUID, 
        child_job_id: UUID,
        dependency_type: DependencyType = DependencyType.SUCCESS,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Add a dependency between two jobs.
        
        Args:
            parent_job_id: Parent job that must complete first
            child_job_id: Child job that depends on parent
            dependency_type: Type of dependency
            description: Optional description
            
        Returns:
            Created dependency details
            
        Raises:
            DependencyError: If dependency would create a cycle
        """
        try:
            self.log_operation("add_dependency", {
                "parent_job_id": parent_job_id,
                "child_job_id": child_job_id,
                "dependency_type": dependency_type
            })
            
            # Validate jobs exist
            parent_job = self.db_session.get(EtlJob, parent_job_id)
            child_job = self.db_session.get(EtlJob, child_job_id)
            
            if not parent_job:
                raise DependencyError(f"Parent job {parent_job_id} not found")
            if not child_job:
                raise DependencyError(f"Child job {child_job_id} not found")
            
            # Check for self-dependency
            if parent_job_id == child_job_id:
                raise DependencyError("Job cannot depend on itself")
            
            # Check for circular dependency
            if await self._would_create_cycle(parent_job_id, child_job_id):
                raise DependencyError(
                    f"Adding this dependency would create a circular dependency"
                )
            
            # Check if dependency already exists
            existing = self.db_session.execute(
                select(JobDependency).where(
                    and_(
                        JobDependency.parent_job_id == parent_job_id,
                        JobDependency.child_job_id == child_job_id,
                        JobDependency.is_active == True
                    )
                )
            ).scalar_one_or_none()
            
            if existing:
                raise DependencyError("Dependency already exists")
            
            # Create dependency
            dependency = JobDependency(
                parent_job_id=parent_job_id,
                child_job_id=child_job_id,
                dependency_type=dependency_type,
                description=description,
                is_active=True
            )
            
            self.db_session.add(dependency)
            self.db_session.commit()
            self.db_session.refresh(dependency)
            
            return {
                "dependency_id": dependency.id,
                "parent_job_id": parent_job_id,
                "parent_job_name": parent_job.job_name,
                "child_job_id": child_job_id,
                "child_job_name": child_job.job_name,
                "dependency_type": dependency_type.value,
                "description": description,
                "status": "created"
            }
            
        except DependencyError:
            self.db_session.rollback()
            raise
        except Exception as e:
            self.db_session.rollback()
            self.handle_error(e, "add_dependency")
    
    async def remove_dependency(self, dependency_id: UUID) -> bool:
        """Remove a dependency."""
        try:
            self.log_operation("remove_dependency", {"dependency_id": dependency_id})
            
            dependency = self.db_session.get(JobDependency, dependency_id)
            if not dependency:
                raise DependencyError("Dependency not found")
            
            # Soft delete by marking as inactive
            dependency.is_active = False
            self.db_session.commit()
            
            return True
            
        except Exception as e:
            self.db_session.rollback()
            self.handle_error(e, "remove_dependency")
    
    async def get_job_dependencies(
        self, 
        job_id: UUID,
        include_inactive: bool = False
    ) -> Dict[str, Any]:
        """
        Get all dependencies for a job (both parent and child).
        
        Args:
            job_id: Job ID
            include_inactive: Include inactive dependencies
            
        Returns:
            Dictionary with parents and children dependencies
        """
        try:
            self.log_operation("get_job_dependencies", {"job_id": job_id})
            
            # Get parent dependencies (jobs this job depends on)
            parent_stmt = select(JobDependency).where(
                JobDependency.child_job_id == job_id
            )
            if not include_inactive:
                parent_stmt = parent_stmt.where(JobDependency.is_active == True)
            
            parent_deps = self.db_session.execute(parent_stmt).scalars().all()
            
            # Get child dependencies (jobs that depend on this job)
            child_stmt = select(JobDependency).where(
                JobDependency.parent_job_id == job_id
            )
            if not include_inactive:
                child_stmt = child_stmt.where(JobDependency.is_active == True)
            
            child_deps = self.db_session.execute(child_stmt).scalars().all()
            
            return {
                "job_id": job_id,
                "parent_dependencies": [
                    {
                        "dependency_id": dep.id,
                        "parent_job_id": dep.parent_job_id,
                        "dependency_type": dep.dependency_type.value,
                        "description": dep.description,
                        "is_active": dep.is_active
                    }
                    for dep in parent_deps
                ],
                "child_dependencies": [
                    {
                        "dependency_id": dep.id,
                        "child_job_id": dep.child_job_id,
                        "dependency_type": dep.dependency_type.value,
                        "description": dep.description,
                        "is_active": dep.is_active
                    }
                    for dep in child_deps
                ],
                "total_parents": len(parent_deps),
                "total_children": len(child_deps)
            }
            
        except Exception as e:
            self.handle_error(e, "get_job_dependencies")
    
    async def check_dependencies_met(self, job_id: UUID) -> Dict[str, Any]:
        """
        Check if all dependencies for a job are met.
        
        Args:
            job_id: Job ID to check
            
        Returns:
            Dictionary with dependency status and details
        """
        try:
            self.log_operation("check_dependencies_met", {"job_id": job_id})
            
            # Get active parent dependencies
            parent_deps = self.db_session.execute(
                select(JobDependency).where(
                    and_(
                        JobDependency.child_job_id == job_id,
                        JobDependency.is_active == True
                    )
                )
            ).scalars().all()
            
            if not parent_deps:
                return {
                    "dependencies_met": True,
                    "total_dependencies": 0,
                    "met_dependencies": 0,
                    "unmet_dependencies": [],
                    "message": "No dependencies"
                }
            
            unmet_dependencies = []
            
            for dep in parent_deps:
                # Get latest execution of parent job
                latest_execution = self.db_session.execute(
                    select(JobExecution)
                    .where(JobExecution.job_id == dep.parent_job_id)
                    .order_by(JobExecution.created_at.desc())
                    .limit(1)
                ).scalar_one_or_none()
                
                is_met = False
                reason = ""
                
                if not latest_execution:
                    reason = "Parent job has never been executed"
                elif dep.dependency_type == DependencyType.SUCCESS:
                    if latest_execution.status == ExecutionStatus.SUCCESS:
                        is_met = True
                    else:
                        reason = f"Parent job status is {latest_execution.status.value}, expected SUCCESS"
                elif dep.dependency_type == DependencyType.COMPLETION:
                    if latest_execution.status in [ExecutionStatus.SUCCESS, ExecutionStatus.FAILED]:
                        is_met = True
                    else:
                        reason = f"Parent job is still {latest_execution.status.value}"
                elif dep.dependency_type == DependencyType.DATA_AVAILABILITY:
                    # Check if parent job has produced data
                    if latest_execution.status == ExecutionStatus.SUCCESS and latest_execution.records_successful > 0:
                        is_met = True
                    else:
                        reason = "Parent job has not produced data"
                
                if not is_met:
                    parent_job = self.db_session.get(EtlJob, dep.parent_job_id)
                    unmet_dependencies.append({
                        "parent_job_id": dep.parent_job_id,
                        "parent_job_name": parent_job.job_name if parent_job else "Unknown",
                        "dependency_type": dep.dependency_type.value,
                        "reason": reason,
                        "latest_status": latest_execution.status.value if latest_execution else None
                    })
            
            dependencies_met = len(unmet_dependencies) == 0
            
            return {
                "dependencies_met": dependencies_met,
                "total_dependencies": len(parent_deps),
                "met_dependencies": len(parent_deps) - len(unmet_dependencies),
                "unmet_dependencies": unmet_dependencies,
                "message": "All dependencies met" if dependencies_met else "Some dependencies not met"
            }
            
        except Exception as e:
            self.handle_error(e, "check_dependencies_met")
    
    async def get_dependency_tree(self, job_id: UUID, max_depth: int = 10) -> Dict[str, Any]:
        """
        Get the full dependency tree for a job.
        
        Args:
            job_id: Root job ID
            max_depth: Maximum depth to traverse
            
        Returns:
            Dependency tree structure
        """
        try:
            self.log_operation("get_dependency_tree", {"job_id": job_id})
            
            visited: Set[UUID] = set()
            
            def build_tree(current_job_id: UUID, depth: int = 0) -> Dict[str, Any]:
                if depth > max_depth or current_job_id in visited:
                    return {"truncated": True}
                
                visited.add(current_job_id)
                
                job = self.db_session.get(EtlJob, current_job_id)
                if not job:
                    return {"error": "Job not found"}
                
                # Get parent dependencies
                parent_deps = self.db_session.execute(
                    select(JobDependency).where(
                        and_(
                            JobDependency.child_job_id == current_job_id,
                            JobDependency.is_active == True
                        )
                    )
                ).scalars().all()
                
                return {
                    "job_id": current_job_id,
                    "job_name": job.job_name,
                    "job_type": job.job_type.value,
                    "is_active": job.is_active,
                    "depth": depth,
                    "parents": [
                        {
                            "dependency_type": dep.dependency_type.value,
                            "description": dep.description,
                            **build_tree(dep.parent_job_id, depth + 1)
                        }
                        for dep in parent_deps
                    ]
                }
            
            tree = build_tree(job_id)
            
            return {
                "root_job_id": job_id,
                "tree": tree,
                "total_jobs_in_tree": len(visited)
            }
            
        except Exception as e:
            self.handle_error(e, "get_dependency_tree")
    
    async def get_executable_jobs(self) -> List[Dict[str, Any]]:
        """
        Get list of jobs that can be executed (all dependencies met).
        
        Returns:
            List of executable jobs
        """
        try:
            self.log_operation("get_executable_jobs")
            
            # Get all active jobs
            active_jobs = self.db_session.execute(
                select(EtlJob).where(EtlJob.is_active == True)
            ).scalars().all()
            
            executable_jobs = []
            
            for job in active_jobs:
                dep_status = await self.check_dependencies_met(job.id)
                
                if dep_status["dependencies_met"]:
                    executable_jobs.append({
                        "job_id": job.id,
                        "job_name": job.job_name,
                        "job_type": job.job_type.value,
                        "total_dependencies": dep_status["total_dependencies"]
                    })
            
            return executable_jobs
            
        except Exception as e:
            self.handle_error(e, "get_executable_jobs")
    
    # Private helper methods
    async def _would_create_cycle(self, parent_job_id: UUID, child_job_id: UUID) -> bool:
        """
        Check if adding a dependency would create a circular dependency.
        
        Uses DFS to detect cycles.
        """
        visited: Set[UUID] = set()
        
        def has_path(from_job: UUID, to_job: UUID) -> bool:
            """Check if there's a path from from_job to to_job."""
            if from_job == to_job:
                return True
            
            if from_job in visited:
                return False
            
            visited.add(from_job)
            
            # Get all parents of from_job
            parents = self.db_session.execute(
                select(JobDependency).where(
                    and_(
                        JobDependency.child_job_id == from_job,
                        JobDependency.is_active == True
                    )
                )
            ).scalars().all()
            
            for dep in parents:
                if has_path(dep.parent_job_id, to_job):
                    return True
            
            return False
        
        # Check if there's already a path from child to parent
        # If yes, adding parent->child would create a cycle
        return has_path(child_job_id, parent_job_id)
