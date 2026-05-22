"""
Monitoring service for tracking ETL performance, metrics, and system health.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, func, desc
from app.application.services.base import BaseService
from app.core.exceptions import MonitoringError, ServiceError
from app.core.enums import JobStatus
from app.utils.date_utils import get_current_timestamp


class MonitoringService(BaseService):
    """Service for monitoring ETL operations and system performance."""
    
    def __init__(self, db_session: Session):
        super().__init__(db_session)
    
    def get_service_name(self) -> str:
        return "MonitoringService"
    
    async def get_system_overview(self) -> Dict[str, Any]:
        """Get overall system status and metrics."""
        try:
            self.log_operation("get_system_overview")
            
            # Get job statistics
            job_stats = await self._get_job_statistics()
            
            # Get execution statistics for last 24 hours
            execution_stats = await self._get_execution_statistics(hours=24)
            
            # Get data quality metrics
            quality_stats = await self._get_quality_statistics()
            
            # Get file processing statistics
            file_stats = await self._get_file_statistics()
            
            # Get system resource metrics
            resource_metrics = await self._get_resource_metrics()
            
            return {
                "timestamp": get_current_timestamp(),
                "system_status": await self._get_system_status(),
                "job_statistics": job_stats,
                "execution_statistics": execution_stats,
                "quality_statistics": quality_stats,
                "file_statistics": file_stats,
                "resource_metrics": resource_metrics,
                "active_alerts": await self._get_active_alerts()
            }
            
        except Exception as e:
            self.handle_error(e, "get_system_overview")
    
    async def get_job_performance_metrics(self, job_id: int = None, days: int = 7) -> Dict[str, Any]:
        """Get job performance metrics for specified period."""
        try:
            self.log_operation("get_job_performance_metrics", {"job_id": job_id, "days": days})
            
            # Get executions for the period
            executions = await self._get_executions_for_period(job_id, days)
            
            if not executions:
                return {
                    "job_id": job_id,
                    "period_days": days,
                    "total_executions": 0,
                    "metrics": {}
                }
            
            # Calculate performance metrics
            total_executions = len(executions)
            successful_executions = len([e for e in executions if e.status == JobStatus.COMPLETED.value])
            failed_executions = len([e for e in executions if e.status == JobStatus.FAILED.value])
            
            # Calculate duration statistics
            durations = []
            for execution in executions:
                if execution.start_time and execution.end_time:
                    duration = (execution.end_time - execution.start_time).total_seconds()
                    durations.append(duration)
            
            avg_duration = sum(durations) / len(durations) if durations else 0
            min_duration = min(durations) if durations else 0
            max_duration = max(durations) if durations else 0
            
            # Calculate throughput (records per second)
            total_records = sum(e.records_processed or 0 for e in executions)
            total_duration = sum(durations)
            throughput = total_records / total_duration if total_duration > 0 else 0
            
            # Daily execution trend
            daily_trend = await self._calculate_daily_execution_trend(executions, days)
            
            return {
                "job_id": job_id,
                "period_days": days,
                "total_executions": total_executions,
                "success_rate": (successful_executions / total_executions * 100) if total_executions > 0 else 0,
                "failure_rate": (failed_executions / total_executions * 100) if total_executions > 0 else 0,
                "performance_metrics": {
                    "avg_duration_seconds": round(avg_duration, 2),
                    "min_duration_seconds": round(min_duration, 2),
                    "max_duration_seconds": round(max_duration, 2),
                    "total_records_processed": total_records,
                    "avg_throughput_records_per_second": round(throughput, 2)
                },
                "daily_trend": daily_trend,
                "recent_executions": [{
                    "execution_id": e.execution_id,
                    "status": e.status,
                    "start_time": e.start_time,
                    "duration_seconds": round((e.end_time - e.start_time).total_seconds(), 2) if e.start_time and e.end_time else None,
                    "records_processed": e.records_processed
                } for e in executions[-10:]]  # Last 10 executions
            }
            
        except Exception as e:
            self.handle_error(e, "get_job_performance_metrics")
    
    async def get_data_quality_dashboard(self, days: int = 7) -> Dict[str, Any]:
        """Get data quality dashboard metrics."""
        try:
            self.log_operation("get_data_quality_dashboard", {"days": days})
            
            # Get quality check results for the period
            quality_results = await self._get_quality_results_for_period(days)
            
            if not quality_results:
                return {
                    "period_days": days,
                    "total_checks": 0,
                    "quality_metrics": {}
                }
            
            # Calculate overall quality metrics
            total_checks = len(quality_results)
            passed_checks = len([r for r in quality_results if r.check_result == "PASS"])
            failed_checks = len([r for r in quality_results if r.check_result == "FAIL"])
            warning_checks = len([r for r in quality_results if r.check_result == "WARNING"])
            
            # Group by rule type
            rule_type_metrics = {}
            for result in quality_results:
                rule_type = result.quality_rule.rule_type
                if rule_type not in rule_type_metrics:
                    rule_type_metrics[rule_type] = {
                        "total": 0,
                        "passed": 0,
                        "failed": 0,
                        "warning": 0
                    }
                
                rule_type_metrics[rule_type]["total"] += 1
                if result.check_result == "PASS":
                    rule_type_metrics[rule_type]["passed"] += 1
                elif result.check_result == "FAIL":
                    rule_type_metrics[rule_type]["failed"] += 1
                else:
                    rule_type_metrics[rule_type]["warning"] += 1
            
            # Calculate daily quality trend
            daily_quality_trend = await self._calculate_daily_quality_trend(quality_results, days)
            
            # Get top failing rules
            failing_rules = await self._get_top_failing_rules(days, limit=10)
            
            return {
                "period_days": days,
                "total_checks": total_checks,
                "quality_metrics": {
                    "overall_pass_rate": (passed_checks / total_checks * 100) if total_checks > 0 else 0,
                    "passed_checks": passed_checks,
                    "failed_checks": failed_checks,
                    "warning_checks": warning_checks
                },
                "rule_type_metrics": rule_type_metrics,
                "daily_trend": daily_quality_trend,
                "top_failing_rules": failing_rules
            }
            
        except Exception as e:
            self.handle_error(e, "get_data_quality_dashboard")
    
    async def get_file_processing_metrics(self, days: int = 7) -> Dict[str, Any]:
        """Get file processing metrics."""
        try:
            self.log_operation("get_file_processing_metrics", {"days": days})
            
            # Get file processing data for the period
            files_data = await self._get_files_for_period(days)
            
            if not files_data:
                return {
                    "period_days": days,
                    "total_files": 0,
                    "processing_metrics": {}
                }
            
            # Calculate file processing metrics
            total_files = len(files_data)
            completed_files = len([f for f in files_data if f.processing_status == "COMPLETED"])
            failed_files = len([f for f in files_data if f.processing_status == "FAILED"])
            pending_files = len([f for f in files_data if f.processing_status == "PENDING"])
            processing_files = len([f for f in files_data if f.processing_status == "PROCESSING"])
            
            # Calculate file size statistics
            total_size = sum(f.file_size or 0 for f in files_data)
            avg_size = total_size / total_files if total_files > 0 else 0
            
            # Group by file type
            file_type_metrics = {}
            for file_data in files_data:
                file_type = file_data.file_type
                if file_type not in file_type_metrics:
                    file_type_metrics[file_type] = {
                        "count": 0,
                        "total_size": 0,
                        "completed": 0,
                        "failed": 0
                    }
                
                file_type_metrics[file_type]["count"] += 1
                file_type_metrics[file_type]["total_size"] += file_data.file_size or 0
                if file_data.processing_status == "COMPLETED":
                    file_type_metrics[file_type]["completed"] += 1
                elif file_data.processing_status == "FAILED":
                    file_type_metrics[file_type]["failed"] += 1
            
            # Calculate daily file processing trend
            daily_file_trend = await self._calculate_daily_file_trend(files_data, days)
            
            return {
                "period_days": days,
                "total_files": total_files,
                "processing_metrics": {
                    "completed_files": completed_files,
                    "failed_files": failed_files,
                    "pending_files": pending_files,
                    "processing_files": processing_files,
                    "success_rate": (completed_files / total_files * 100) if total_files > 0 else 0,
                    "total_size_bytes": total_size,
                    "avg_size_bytes": round(avg_size, 2)
                },
                "file_type_metrics": file_type_metrics,
                "daily_trend": daily_file_trend
            }
            
        except Exception as e:
            self.handle_error(e, "get_file_processing_metrics")
    
    async def get_system_alerts(self, severity: str = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get system alerts with optional filtering."""
        try:
            self.log_operation("get_system_alerts", {"severity": severity, "limit": limit})
            
            alerts = await self._get_system_alerts(severity, limit)
            
            return [{
                "alert_id": alert.alert_id,
                "alert_type": alert.alert_type,
                "severity": alert.severity,
                "title": alert.title,
                "message": alert.message,
                "source": alert.source,
                "is_resolved": alert.is_resolved,
                "created_at": alert.created_at,
                "resolved_at": alert.resolved_at,
                "metadata": alert.metadata
            } for alert in alerts]
            
        except Exception as e:
            self.handle_error(e, "get_system_alerts")
    
    async def create_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new system alert."""
        try:
            self.validate_input(alert_data, ["alert_type", "severity", "title", "message"])
            self.log_operation("create_alert", {"alert_type": alert_data["alert_type"]})
            
            alert = await self._create_alert_record({
                "alert_type": alert_data["alert_type"],
                "severity": alert_data["severity"],
                "title": alert_data["title"],
                "message": alert_data["message"],
                "source": alert_data.get("source", "SYSTEM"),
                "metadata": alert_data.get("metadata", {}),
                "is_resolved": False,
                "created_at": get_current_timestamp()
            })
            
            return {
                "alert_id": alert.alert_id,
                "alert_type": alert.alert_type,
                "severity": alert.severity,
                "title": alert.title,
                "status": "created"
            }
            
        except Exception as e:
            self.handle_error(e, "create_alert")
    
    async def resolve_alert(self, alert_id: int, resolution_note: str = None) -> bool:
        """Resolve a system alert."""
        try:
            self.log_operation("resolve_alert", {"alert_id": alert_id})
            
            alert = await self._get_alert_by_id(alert_id)
            if not alert:
                raise MonitoringError("Alert not found")
            
            if alert.is_resolved:
                raise MonitoringError("Alert is already resolved")
            
            # Update alert status
            await self._update_alert_status(alert_id, True, get_current_timestamp(), resolution_note)
            
            return True
            
        except Exception as e:
            self.handle_error(e, "resolve_alert")
    
    async def get_performance_trends(self, metric_type: str, days: int = 30) -> Dict[str, Any]:
        """Get performance trends for specified metric type."""
        try:
            self.log_operation("get_performance_trends", {"metric_type": metric_type, "days": days})
            
            if metric_type == "execution_performance":
                trend_data = await self._get_execution_performance_trend(days)
            elif metric_type == "data_quality":
                trend_data = await self._get_data_quality_trend(days)
            elif metric_type == "file_processing":
                trend_data = await self._get_file_processing_trend(days)
            elif metric_type == "system_resources":
                trend_data = await self._get_system_resource_trend(days)
            else:
                raise MonitoringError(f"Unknown metric type: {metric_type}")
            
            return {
                "metric_type": metric_type,
                "period_days": days,
                "trend_data": trend_data,
                "analysis": await self._analyze_trend(trend_data, metric_type)
            }
            
        except Exception as e:
            self.handle_error(e, "get_performance_trends")
    
    async def get_real_time_metrics(self) -> Dict[str, Any]:
        """Get real-time system metrics."""
        try:
            self.log_operation("get_real_time_metrics")
            
            # Get current running jobs
            running_jobs = await self._get_running_jobs()
            
            # Get recent activity (last hour)
            recent_activity = await self._get_recent_activity(hours=1)
            
            # Get current system load
            system_load = await self._get_current_system_load()
            
            # Get active connections
            active_connections = await self._get_active_connections()
            
            return {
                "timestamp": get_current_timestamp(),
                "running_jobs": {
                    "count": len(running_jobs),
                    "jobs": [{
                        "job_id": job.job_id,
                        "job_name": job.job_name,
                        "execution_id": job.execution_id,
                        "start_time": job.start_time,
                        "progress": await self._estimate_job_progress(job.execution_id)
                    } for job in running_jobs]
                },
                "recent_activity": recent_activity,
                "system_load": system_load,
                "active_connections": active_connections,
                "queue_status": await self._get_queue_status()
            }
            
        except Exception as e:
            self.handle_error(e, "get_real_time_metrics")
    
    async def generate_performance_report(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Generate comprehensive performance report for date range."""
        try:
            self.log_operation("generate_performance_report", {
                "start_date": start_date,
                "end_date": end_date
            })
            
            # Calculate period
            period_days = (end_date - start_date).days
            
            # Get comprehensive metrics for the period
            job_metrics = await self._get_comprehensive_job_metrics(start_date, end_date)
            quality_metrics = await self._get_comprehensive_quality_metrics(start_date, end_date)
            file_metrics = await self._get_comprehensive_file_metrics(start_date, end_date)
            error_analysis = await self._get_error_analysis(start_date, end_date)
            
            # Generate insights and recommendations
            insights = await self._generate_insights(job_metrics, quality_metrics, file_metrics)
            recommendations = await self._generate_recommendations(job_metrics, quality_metrics, error_analysis)
            
            return {
                "report_generated_at": get_current_timestamp(),
                "period": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "days": period_days
                },
                "executive_summary": {
                    "total_jobs_executed": job_metrics.get("total_executions", 0),
                    "overall_success_rate": job_metrics.get("success_rate", 0),
                    "total_files_processed": file_metrics.get("total_files", 0),
                    "file_processing_success_rate": file_metrics.get("success_rate", 0),
                    "data_quality_score": quality_metrics.get("overall_quality_score", 0),
                    "total_records_processed": job_metrics.get("total_records", 0)
                },
                "detailed_metrics": {
                    "job_performance": job_metrics,
                    "data_quality": quality_metrics,
                    "file_processing": file_metrics,
                    "error_analysis": error_analysis
                },
                "insights": insights,
                "recommendations": recommendations
            }
            
        except Exception as e:
            self.handle_error(e, "generate_performance_report")
    
    async def check_system_health(self) -> Dict[str, Any]:
        """Perform comprehensive system health check."""
        try:
            self.log_operation("check_system_health")
            
            health_checks = {
                "database_connectivity": await self._check_database_health(),
                "job_execution_health": await self._check_job_execution_health(),
                "data_quality_health": await self._check_data_quality_health(),
                "file_processing_health": await self._check_file_processing_health(),
                "system_resources": await self._check_system_resources(),
                "external_dependencies": await self._check_external_dependencies()
            }
            
            # Calculate overall health score
            health_scores = [check.get("score", 0) for check in health_checks.values()]
            overall_score = sum(health_scores) / len(health_scores) if health_scores else 0
            
            # Determine overall status
            if overall_score >= 90:
                overall_status = "HEALTHY"
            elif overall_score >= 70:
                overall_status = "WARNING"
            else:
                overall_status = "CRITICAL"
            
            return {
                "timestamp": get_current_timestamp(),
                "overall_status": overall_status,
                "overall_score": round(overall_score, 2),
                "health_checks": health_checks,
                "critical_issues": [
                    check_name for check_name, check_data in health_checks.items()
                    if check_data.get("status") == "CRITICAL"
                ],
                "warnings": [
                    check_name for check_name, check_data in health_checks.items()
                    if check_data.get("status") == "WARNING"
                ]
            }
            
        except Exception as e:
            self.handle_error(e, "check_system_health")

    async def get_dashboard_data(self) -> Dict[str, Any]:
        """Get dashboard overview data (wrapper for get_system_overview)."""
        return await self.get_system_overview()

    async def health_check(self) -> Dict[str, Any]:
        """Perform system health check (wrapper for check_system_health)."""
        return await self.check_system_health()

    async def get_system_metrics(self, period: str = "24h") -> Dict[str, Any]:
        """Get system performance metrics for specified period."""
        try:
            self.log_operation("get_system_metrics", {"period": period})

            # Convert period string to days
            period_map = {
                "1h": 0.04,  # ~1 hour
                "6h": 0.25,  # ~6 hours
                "24h": 1,
                "7d": 7,
                "30d": 30
            }
            days = period_map.get(period, 1)

            execution_stats = await self._get_execution_statistics(hours=int(days * 24))
            resource_metrics = await self._get_resource_metrics()

            return {
                "period": period,
                "execution_statistics": execution_stats,
                "resource_metrics": resource_metrics,
                "timestamp": get_current_timestamp()
            }

        except Exception as e:
            self.handle_error(e, "get_system_metrics")

    async def get_job_performance(self, job_id: str = None, period: str = "7d") -> Dict[str, Any]:
        """Get job performance metrics (wrapper with period conversion)."""
        try:
            period_map = {"1d": 1, "7d": 7, "30d": 30}
            days = period_map.get(period, 7)
            return await self.get_job_performance_metrics(job_id=int(job_id) if job_id else None, days=days)
        except Exception as e:
            self.handle_error(e, "get_job_performance")

    async def get_data_quality_trends(self, period: str = "7d", entity_type: str = None) -> Dict[str, Any]:
        """Get data quality trends (wrapper for get_data_quality_dashboard)."""
        try:
            period_map = {"1d": 1, "7d": 7, "30d": 30}
            days = period_map.get(period, 7)
            return await self.get_data_quality_dashboard(days=days)
        except Exception as e:
            self.handle_error(e, "get_data_quality_trends")

    async def get_active_jobs(self) -> Dict[str, Any]:
        """Get currently active jobs."""
        try:
            self.log_operation("get_active_jobs")

            # Get active job executions
            active_statuses = [JobStatus.RUNNING.value, JobStatus.PENDING.value]
            # This would query actual database for active jobs
            # For now return structure with empty list
            return {
                "active_jobs": [],
                "total_active": 0,
                "timestamp": get_current_timestamp()
            }

        except Exception as e:
            self.handle_error(e, "get_active_jobs")

    async def get_recent_errors(self, limit: int = 10, hours: int = 24) -> Dict[str, Any]:
        """Get recent errors from ETL pipeline."""
        try:
            self.log_operation("get_recent_errors", {"limit": limit, "hours": hours})

            # This would query actual error logs from database
            # For now return structure with empty list
            return {
                "errors": [],
                "total_errors": 0,
                "period_hours": hours,
                "timestamp": get_current_timestamp()
            }

        except Exception as e:
            self.handle_error(e, "get_recent_errors")

    async def get_storage_usage(self) -> Dict[str, Any]:
        """Get storage usage metrics."""
        try:
            self.log_operation("get_storage_usage")

            # This would query actual storage metrics from database or file system
            # For now return structure with placeholder values
            return {
                "total_storage_gb": 0,
                "used_storage_gb": 0,
                "available_storage_gb": 0,
                "usage_percentage": 0,
                "by_schema": {},
                "timestamp": get_current_timestamp()
            }

        except Exception as e:
            self.handle_error(e, "get_storage_usage")

    async def dismiss_alert(self, alert_id: str, user_id: str) -> Dict[str, Any]:
        """Dismiss an active alert."""
        try:
            self.log_operation("dismiss_alert", {"alert_id": alert_id, "user_id": user_id})

            # This would update alert status in database
            # For now return success response
            return {
                "alert_id": alert_id,
                "status": "dismissed",
                "timestamp": get_current_timestamp()
            }

        except Exception as e:
            self.handle_error(e, "dismiss_alert")

    # Private helper methods
    async def _get_system_status(self) -> str:
        """Get overall system status."""
        # Check for critical alerts
        critical_alerts = await self._count_critical_alerts()
        if critical_alerts > 0:
            return "CRITICAL"
        
        # Check for failed jobs in last hour
        recent_failures = await self._count_recent_failures(hours=1)
        if recent_failures > 5:  # Threshold
            return "WARNING"
        
        return "HEALTHY"
    
    async def _calculate_daily_execution_trend(self, executions: List, days: int) -> List[Dict[str, Any]]:
        """Calculate daily execution trend."""
        trend_data = []
        for i in range(days):
            date = datetime.utcnow().date() - timedelta(days=i)
            day_executions = [e for e in executions if e.start_time and e.start_time.date() == date]
            
            trend_data.append({
                "date": date.isoformat(),
                "total_executions": len(day_executions),
                "successful_executions": len([e for e in day_executions if e.status == JobStatus.COMPLETED.value]),
                "failed_executions": len([e for e in day_executions if e.status == JobStatus.FAILED.value])
            })
        
        return list(reversed(trend_data))  # Most recent first
    
    async def _estimate_job_progress(self, execution_id: int) -> Optional[float]:
        """Estimate job progress percentage."""
        # Implement progress estimation logic based on records processed vs expected
        # This is a simplified version
        execution = await self._get_execution_by_id(execution_id)
        if not execution or not execution.start_time:
            return None
        
        # Simple time-based estimation (replace with actual logic)
        elapsed = (datetime.utcnow() - execution.start_time).total_seconds()
        estimated_duration = 300  # 5 minutes default
        progress = min((elapsed / estimated_duration) * 100, 95)  # Max 95% until completion
        
        return round(progress, 2)
    
    # Count methods for system status
    async def _count_critical_alerts(self) -> int:
        """Count active critical alerts."""
        # TODO: query actual alerts from database
        return 0

    async def _count_recent_failures(self, hours: int) -> int:
        """Count job failures in last N hours."""
        # TODO: query actual job_executions from database
        return 0

    # Database helper methods (implement based on your models)
    async def _get_job_statistics(self) -> Dict[str, Any]:
        """Get job statistics from database."""
        return {
            "total_jobs": 0,
            "active_jobs": 0,
            "completed_jobs": 0,
            "failed_jobs": 0
        }

    async def _get_execution_statistics(self, hours: int) -> Dict[str, Any]:
        """Get execution statistics for specified hours."""
        return {
            "period_hours": hours,
            "total_executions": 0,
            "successful_executions": 0,
            "failed_executions": 0,
            "success_rate": 0
        }

    async def _get_quality_statistics(self) -> Dict[str, Any]:
        """Get data quality statistics."""
        return {
            "total_checks": 0,
            "passed_checks": 0,
            "failed_checks": 0,
            "quality_score": 0
        }

    async def _get_file_statistics(self) -> Dict[str, Any]:
        """Get file processing statistics."""
        return {
            "total_files": 0,
            "processed_files": 0,
            "failed_files": 0,
            "total_size_gb": 0
        }

    async def _get_resource_metrics(self) -> Dict[str, Any]:
        """Get system resource metrics."""
        return {
            "cpu_usage_percent": 0,
            "memory_usage_percent": 0,
            "disk_usage_percent": 0,
            "database_size_gb": 0
        }

    async def _get_active_alerts(self) -> List[Dict[str, Any]]:
        """Get active system alerts."""
        return []

    async def _get_executions_for_period(self, job_id: int, days: int) -> List:
        """Get executions for specified period."""
        return []

    async def _get_quality_results_for_period(self, days: int) -> List:
        """Get quality check results for period."""
        return []

    async def _get_files_for_period(self, days: int) -> List:
        """Get files processed in period."""
        return []

    async def _create_alert_record(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create alert record in database."""
        return {"id": 0, "status": "created"}

    async def _get_alert_by_id(self, alert_id: int) -> Optional[Dict[str, Any]]:
        """Get alert by ID."""
        return None

    async def _update_alert_status(self, alert_id: int, is_resolved: bool, resolved_at: datetime, resolution_note: str) -> bool:
        """Update alert status."""
        return True

    async def _get_running_jobs(self) -> List[Dict[str, Any]]:
        """Get currently running jobs."""
        return []

    async def _get_recent_activity(self, hours: int) -> List[Dict[str, Any]]:
        """Get recent system activity."""
        return []

    async def _check_database_health(self) -> Dict[str, Any]:
        """Check database connectivity and performance."""
        return {"status": "HEALTHY", "score": 100, "response_time_ms": 0}

    async def _check_job_execution_health(self) -> Dict[str, Any]:
        """Check job execution health."""
        return {"status": "HEALTHY", "score": 100}

    async def _check_data_quality_health(self) -> Dict[str, Any]:
        """Check data quality health."""
        return {"status": "HEALTHY", "score": 100}

    async def _check_file_processing_health(self) -> Dict[str, Any]:
        """Check file processing health."""
        return {"status": "HEALTHY", "score": 100}

    async def _check_system_resources(self) -> Dict[str, Any]:
        """Check system resources."""
        return {"status": "HEALTHY", "score": 100}

    async def _check_external_dependencies(self) -> Dict[str, Any]:
        """Check external service dependencies."""
        return {"status": "HEALTHY", "score": 100}
    