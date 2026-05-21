from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from sqlalchemy import func, select, and_
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

from .base import BaseService
from app.infrastructure.db.models.etl_control.job_executions import JobExecution, ExecutionStatus
from app.infrastructure.db.models.etl_control.quality_check_results import QualityCheckResult
from app.infrastructure.db.models.etl_control.performance_metrics import PerformanceMetric
from app.infrastructure.db.models.processed.entities import Entity
from app.core.exceptions import ServiceError


class ReportService(BaseService):
    """Service for ETL reporting and analytics."""

    def get_service_name(self) -> str:
        return "ReportService"

    def _parse_period(self, period: str) -> int:
        """Parse period string to days. e.g., '7d' -> 7, '30d' -> 30."""
        try:
            return int(period.rstrip('d'))
        except (ValueError, AttributeError):
            return 30

    async def get_dashboard_summary(self, period: str = "30d") -> Dict[str, Any]:
        """Get high-level ETL dashboard summary for the given period."""
        days = self._parse_period(period)
        cutoff_time = datetime.utcnow() - timedelta(days=days)

        try:
            # Query job executions within period
            stmt = select(
                func.count(JobExecution.id).label("total_jobs"),
                func.sum(func.cast(JobExecution.status == ExecutionStatus.SUCCESS, type_=int)).label("successful_jobs"),
                func.sum(func.cast(JobExecution.status == ExecutionStatus.FAILED, type_=int)).label("failed_jobs"),
                func.sum(JobExecution.records_extracted).label("total_records_extracted"),
                func.sum(JobExecution.records_transformed).label("total_records_transformed"),
                func.sum(JobExecution.records_loaded).label("total_records_loaded"),
                func.sum(JobExecution.records_failed).label("total_records_failed"),
                func.avg(func.extract('epoch', JobExecution.end_time - JobExecution.start_time)).label("avg_duration_seconds"),
            ).where(
                and_(
                    JobExecution.start_time >= cutoff_time,
                    JobExecution.status != ExecutionStatus.PENDING
                )
            )

            result = await self.db.execute(stmt) if hasattr(self.db, 'execute') else self.db.execute(stmt)
            row = result.first()

            # Calculate success rate
            total = row[0] or 0
            successful = row[1] or 0
            success_rate = (successful / total * 100) if total > 0 else 0

            return {
                "period": period,
                "total_jobs": total,
                "successful_jobs": successful,
                "failed_jobs": row[2] or 0,
                "success_rate": round(success_rate, 2),
                "total_records_extracted": row[3] or 0,
                "total_records_transformed": row[4] or 0,
                "total_records_loaded": row[5] or 0,
                "total_records_failed": row[6] or 0,
                "avg_duration_seconds": float(row[7]) if row[7] else 0,
                "generated_at": datetime.utcnow().isoformat(),
            }
        except Exception as e:
            self.handle_error(e, f"get_dashboard_summary(period={period})")

    async def get_data_processing_analytics(
        self, period: str = "30d", granularity: str = "daily"
    ) -> Dict[str, Any]:
        """Get data processing analytics: records extracted, transformed, loaded over time."""
        days = self._parse_period(period)
        cutoff_time = datetime.utcnow() - timedelta(days=days)

        try:
            stmt = select(
                JobExecution.start_time,
                func.sum(JobExecution.records_extracted).label("extracted"),
                func.sum(JobExecution.records_transformed).label("transformed"),
                func.sum(JobExecution.records_loaded).label("loaded"),
            ).where(
                and_(
                    JobExecution.start_time >= cutoff_time,
                    JobExecution.status != ExecutionStatus.PENDING
                )
            ).group_by(
                func.date(JobExecution.start_time)
            ).order_by(
                func.date(JobExecution.start_time)
            )

            result = await self.db.execute(stmt) if hasattr(self.db, 'execute') else self.db.execute(stmt)
            rows = result.all()

            data_points = [
                {
                    "timestamp": str(row[0]) if row[0] else None,
                    "extracted": row[1] or 0,
                    "transformed": row[2] or 0,
                    "loaded": row[3] or 0,
                }
                for row in rows
            ]

            return {
                "period": period,
                "granularity": granularity,
                "data_points": data_points,
                "generated_at": datetime.utcnow().isoformat(),
            }
        except Exception as e:
            self.handle_error(e, f"get_data_processing_analytics(period={period})")

    async def get_data_quality_analytics(
        self, period: str = "30d", entity_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get data quality analytics: pass/fail rates and error breakdown."""
        days = self._parse_period(period)
        cutoff_time = datetime.utcnow() - timedelta(days=days)

        try:
            stmt = select(
                func.count(QualityCheckResult.check_id).label("total_checks"),
                func.sum(
                    func.cast(
                        QualityCheckResult.records_passed >= QualityCheckResult.records_failed,
                        type_=int
                    )
                ).label("passed_checks"),
                func.avg(
                    (QualityCheckResult.records_passed * 100.0) /
                    func.nullif(QualityCheckResult.records_checked, 0)
                ).label("avg_pass_rate"),
            ).join(
                JobExecution,
                QualityCheckResult.execution_id == JobExecution.id
            ).where(
                and_(
                    JobExecution.start_time >= cutoff_time,
                    JobExecution.status != ExecutionStatus.PENDING
                )
            )

            result = await self.db.execute(stmt) if hasattr(self.db, 'execute') else self.db.execute(stmt)
            row = result.first()

            total_checks = row[0] or 0
            passed_checks = row[1] or 0
            pass_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0

            return {
                "period": period,
                "total_checks": total_checks,
                "passed_checks": passed_checks,
                "failed_checks": total_checks - passed_checks,
                "pass_rate": round(pass_rate, 2),
                "avg_pass_rate": round(float(row[2]) if row[2] else 0, 2),
                "generated_at": datetime.utcnow().isoformat(),
            }
        except Exception as e:
            self.handle_error(e, f"get_data_quality_analytics(period={period})")

    async def get_entity_growth_analytics(
        self, period: str = "90d", entity_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get entity growth over time."""
        days = self._parse_period(period)
        cutoff_time = datetime.utcnow() - timedelta(days=days)

        try:
            stmt = select(
                func.date(Entity.last_updated).label("date"),
                func.count(Entity.id).label("entity_count"),
            ).where(
                Entity.last_updated >= cutoff_time
            )

            if entity_type:
                stmt = stmt.where(Entity.entity_type == entity_type)

            stmt = stmt.group_by(
                func.date(Entity.last_updated)
            ).order_by(
                func.date(Entity.last_updated)
            )

            result = await self.db.execute(stmt) if hasattr(self.db, 'execute') else self.db.execute(stmt)
            rows = result.all()

            data_points = [
                {
                    "date": str(row[0]) if row[0] else None,
                    "count": row[1] or 0,
                }
                for row in rows
            ]

            return {
                "period": period,
                "entity_type": entity_type,
                "data_points": data_points,
                "generated_at": datetime.utcnow().isoformat(),
            }
        except Exception as e:
            self.handle_error(e, f"get_entity_growth_analytics(period={period}, entity_type={entity_type})")

    async def get_performance_analytics(
        self, period: str = "30d", job_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get performance metrics: throughput, memory, CPU over time."""
        days = self._parse_period(period)
        cutoff_time = datetime.utcnow() - timedelta(days=days)

        try:
            stmt = select(
                PerformanceMetric.recorded_at,
                func.avg(PerformanceMetric.records_per_second).label("avg_records_per_second"),
                func.avg(PerformanceMetric.memory_usage_mb).label("avg_memory_usage_mb"),
                func.avg(PerformanceMetric.cpu_usage_percent).label("avg_cpu_usage_percent"),
            ).where(
                PerformanceMetric.recorded_at >= cutoff_time
            ).group_by(
                func.date(PerformanceMetric.recorded_at)
            ).order_by(
                func.date(PerformanceMetric.recorded_at)
            )

            result = await self.db.execute(stmt) if hasattr(self.db, 'execute') else self.db.execute(stmt)
            rows = result.all()

            data_points = [
                {
                    "timestamp": str(row[0]) if row[0] else None,
                    "records_per_second": float(row[1]) if row[1] else 0,
                    "memory_usage_mb": float(row[2]) if row[2] else 0,
                    "cpu_usage_percent": float(row[3]) if row[3] else 0,
                }
                for row in rows
            ]

            return {
                "period": period,
                "job_type": job_type,
                "data_points": data_points,
                "generated_at": datetime.utcnow().isoformat(),
            }
        except Exception as e:
            self.handle_error(e, f"get_performance_analytics(period={period}, job_type={job_type})")

    async def get_report_templates(self) -> Dict[str, Any]:
        """Get available report templates (stub)."""
        raise ServiceError("Report template generation not yet implemented")

    async def generate_report(self, report_request: Any, user_id: Any, background_tasks: Any) -> Dict[str, Any]:
        """Generate report asynchronously (stub)."""
        raise ServiceError("Report generation not yet implemented")

    async def schedule_report(self, report_request: Any, schedule_expression: str, user_id: Any) -> Dict[str, Any]:
        """Schedule report for future generation (stub)."""
        raise ServiceError("Report scheduling not yet implemented")

    async def export_entities(self, entity_type: str, format: str, user_id: Any) -> Dict[str, Any]:
        """Export entities in specified format (stub)."""
        raise ServiceError("Entity export not yet implemented")

    async def export_job_executions(self, job_id: Any, period: str, format: str, user_id: Any) -> Dict[str, Any]:
        """Export job execution records (stub)."""
        raise ServiceError("Job execution export not yet implemented")

    async def export_data_lineage(self, entity_id: Any, format: str, user_id: Any) -> Dict[str, Any]:
        """Export data lineage for entity (stub)."""
        raise ServiceError("Data lineage export not yet implemented")

    async def list_reports(self, skip: int, limit: int, report_type: Optional[str], status: Optional[str], user_id: Any) -> Dict[str, Any]:
        """List user's generated reports (stub)."""
        raise ServiceError("Report listing not yet implemented")

    async def get_report(self, report_id: Any, user_id: Any) -> Dict[str, Any]:
        """Get report details (stub)."""
        raise ServiceError("Report retrieval not yet implemented")

    async def download_report(self, report_id: Any, format: str, user_id: Any) -> Dict[str, Any]:
        """Download generated report file (stub)."""
        raise ServiceError("Report download not yet implemented")

    async def delete_report(self, report_id: Any, user_id: Any) -> Dict[str, Any]:
        """Delete report (stub)."""
        raise ServiceError("Report deletion not yet implemented")
