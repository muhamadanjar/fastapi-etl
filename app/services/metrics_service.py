"""
Service for managing performance metrics.
"""

from typing import Dict, List, Any, Optional
from uuid import UUID
from datetime import datetime, timedelta
from sqlmodel import Session, select, func, and_, desc
from decimal import Decimal

from app.infrastructure.db.models.etl_control.performance_metrics import (
    PerformanceMetric,
    PerformanceMetricRead,
    PerformanceMetricSummary
)
from app.core.exceptions import ETLError
from app.utils.logger import get_logger

logger = get_logger(__name__)


class MetricsService:
    """Service for querying and analyzing performance metrics"""
    
    def __init__(self, db_session: Session):
        self.db = db_session
        self.logger = logger
    
    async def get_metrics(
        self,
        execution_id: Optional[UUID] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[PerformanceMetric]:
        """
        Get performance metrics with optional filters.
        
        Args:
            execution_id: Filter by execution ID
            start_date: Filter by start date
            end_date: Filter by end date
            limit: Maximum records to return
            offset: Offset for pagination
            
        Returns:
            List of PerformanceMetric instances
        """
        try:
            query = select(PerformanceMetric)
            
            # Apply filters
            conditions = []
            if execution_id:
                conditions.append(PerformanceMetric.execution_id == execution_id)
            if start_date:
                conditions.append(PerformanceMetric.recorded_at >= start_date)
            if end_date:
                conditions.append(PerformanceMetric.recorded_at <= end_date)
            
            if conditions:
                query = query.where(and_(*conditions))
            
            # Order by most recent first
            query = query.order_by(desc(PerformanceMetric.recorded_at))
            
            # Apply pagination
            query = query.limit(limit).offset(offset)
            
            results = self.db.exec(query).all()
            
            self.logger.info(f"Retrieved {len(results)} performance metrics")
            return results
            
        except Exception as e:
            self.logger.error(f"Error retrieving metrics: {str(e)}")
            raise ETLError(f"Failed to retrieve metrics: {str(e)}")
    
    async def get_execution_metrics(
        self,
        execution_id: UUID
    ) -> List[PerformanceMetric]:
        """
        Get all metrics for a specific execution.
        
        Args:
            execution_id: Execution ID
            
        Returns:
            List of PerformanceMetric instances
        """
        return await self.get_metrics(execution_id=execution_id, limit=1000)
    
    async def get_metric_summary(
        self,
        execution_id: Optional[UUID] = None,
        days: int = 7
    ) -> Dict[str, Any]:
        """
        Get aggregated metrics summary.
        
        Args:
            execution_id: Optional execution ID filter
            days: Number of days to analyze
            
        Returns:
            Dictionary with summary statistics
        """
        try:
            start_date = datetime.utcnow() - timedelta(days=days)
            
            query = select(PerformanceMetric).where(
                PerformanceMetric.recorded_at >= start_date
            )
            
            if execution_id:
                query = query.where(PerformanceMetric.execution_id == execution_id)
            
            metrics = self.db.exec(query).all()
            
            if not metrics:
                return {
                    "total_metrics": 0,
                    "period_days": days,
                    "message": "No metrics found"
                }
            
            # Calculate statistics
            records_per_second = [m.records_per_second for m in metrics if m.records_per_second]
            memory_usage = [m.memory_usage_mb for m in metrics if m.memory_usage_mb]
            cpu_usage = [m.cpu_usage_percent for m in metrics if m.cpu_usage_percent]
            durations = [m.duration_seconds for m in metrics if m.duration_seconds]
            error_rates = [m.error_rate for m in metrics if m.error_rate]
            
            summary = {
                "total_metrics": len(metrics),
                "period_days": days,
                "start_date": start_date.isoformat(),
                "end_date": datetime.utcnow().isoformat(),
            }
            
            if records_per_second:
                summary["avg_records_per_second"] = float(sum(records_per_second) / len(records_per_second))
                summary["max_records_per_second"] = float(max(records_per_second))
                summary["min_records_per_second"] = float(min(records_per_second))
            
            if memory_usage:
                summary["avg_memory_mb"] = float(sum(memory_usage) / len(memory_usage))
                summary["peak_memory_mb"] = float(max(memory_usage))
            
            if cpu_usage:
                summary["avg_cpu_percent"] = float(sum(cpu_usage) / len(cpu_usage))
                summary["peak_cpu_percent"] = float(max(cpu_usage))
            
            if durations:
                summary["avg_duration_seconds"] = float(sum(durations) / len(durations))
                summary["total_duration_seconds"] = sum(durations)
            
            if error_rates:
                summary["avg_error_rate"] = float(sum(error_rates) / len(error_rates))
            
            self.logger.info(f"Generated metrics summary for {days} days")
            return summary
            
        except Exception as e:
            self.logger.error(f"Error generating metrics summary: {str(e)}")
            raise ETLError(f"Failed to generate metrics summary: {str(e)}")
    
    async def get_metric_trends(
        self,
        metric_name: str = "records_per_second",
        days: int = 30,
        interval: str = "day"
    ) -> List[Dict[str, Any]]:
        """
        Get metric trends over time.
        
        Args:
            metric_name: Name of metric to analyze
            days: Number of days to analyze
            interval: Grouping interval (hour, day, week)
            
        Returns:
            List of trend data points
        """
        try:
            start_date = datetime.utcnow() - timedelta(days=days)
            
            query = select(PerformanceMetric).where(
                PerformanceMetric.recorded_at >= start_date
            ).order_by(PerformanceMetric.recorded_at)
            
            metrics = self.db.exec(query).all()
            
            if not metrics:
                return []
            
            # Group by interval
            trends = []
            current_group = []
            current_date = None
            
            for metric in metrics:
                # Determine group date based on interval
                if interval == "hour":
                    group_date = metric.recorded_at.replace(minute=0, second=0, microsecond=0)
                elif interval == "day":
                    group_date = metric.recorded_at.replace(hour=0, minute=0, second=0, microsecond=0)
                else:  # week
                    group_date = metric.recorded_at.replace(hour=0, minute=0, second=0, microsecond=0)
                    group_date = group_date - timedelta(days=group_date.weekday())
                
                if current_date is None:
                    current_date = group_date
                
                if group_date == current_date:
                    current_group.append(metric)
                else:
                    # Process current group
                    if current_group:
                        trends.append(self._calculate_group_average(current_group, metric_name, current_date))
                    
                    current_group = [metric]
                    current_date = group_date
            
            # Process last group
            if current_group:
                trends.append(self._calculate_group_average(current_group, metric_name, current_date))
            
            self.logger.info(f"Generated {len(trends)} trend data points")
            return trends
            
        except Exception as e:
            self.logger.error(f"Error generating metric trends: {str(e)}")
            raise ETLError(f"Failed to generate metric trends: {str(e)}")
    
    def _calculate_group_average(
        self,
        metrics: List[PerformanceMetric],
        metric_name: str,
        date: datetime
    ) -> Dict[str, Any]:
        """Calculate average for a group of metrics"""
        values = []
        
        for metric in metrics:
            if metric_name == "records_per_second" and metric.records_per_second:
                values.append(float(metric.records_per_second))
            elif metric_name == "memory_usage" and metric.memory_usage_mb:
                values.append(float(metric.memory_usage_mb))
            elif metric_name == "cpu_usage" and metric.cpu_usage_percent:
                values.append(float(metric.cpu_usage_percent))
            elif metric_name == "duration" and metric.duration_seconds:
                values.append(float(metric.duration_seconds))
        
        avg_value = sum(values) / len(values) if values else 0
        
        return {
            "date": date.isoformat(),
            "metric_name": metric_name,
            "average": avg_value,
            "count": len(metrics),
            "min": min(values) if values else 0,
            "max": max(values) if values else 0
        }
    
    async def get_system_metrics(self) -> Dict[str, Any]:
        """
        Get current system metrics.
        
        Returns:
            Dictionary with current system metrics
        """
        try:
            import psutil
            
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "cpu": {
                    "percent": cpu_percent,
                    "count": psutil.cpu_count()
                },
                "memory": {
                    "total_mb": memory.total / (1024 * 1024),
                    "used_mb": memory.used / (1024 * 1024),
                    "percent": memory.percent
                },
                "disk": {
                    "total_gb": disk.total / (1024 * 1024 * 1024),
                    "used_gb": disk.used / (1024 * 1024 * 1024),
                    "percent": disk.percent
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting system metrics: {str(e)}")
            raise ETLError(f"Failed to get system metrics: {str(e)}")
