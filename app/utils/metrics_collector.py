"""
Metrics collector for recording performance metrics during ETL operations.
"""

import time
import psutil
from typing import Dict, Any, Optional
from uuid import UUID, uuid4
from datetime import datetime
from decimal import Decimal
from sqlmodel import Session

from app.infrastructure.db.models.etl_control.performance_metrics import PerformanceMetric
from app.utils.logger import get_logger

logger = get_logger(__name__)


class MetricsCollector:
    """
    Utility class for collecting and recording performance metrics.
    Tracks execution time, resource usage, and processing rates.
    """
    
    def __init__(self, db: Session, execution_id: Optional[UUID] = None):
        """
        Initialize metrics collector.
        
        Args:
            db: Database session
            execution_id: Optional execution ID to associate metrics with
        """
        self.db = db
        self.execution_id = execution_id
        self.start_time: Optional[float] = None
        self.metrics: list[PerformanceMetric] = []
        self.logger = logger
        
        # Track initial system state
        self.initial_cpu = psutil.cpu_percent(interval=0.1)
        self.initial_memory = psutil.virtual_memory().percent
        
    def start(self):
        """Start timing the operation"""
        self.start_time = time.time()
        self.logger.debug("Metrics collection started")
    
    def record(
        self,
        metric_name: str,
        value: float,
        metric_type: str = "CUSTOM"
    ):
        """
        Record a custom metric.
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            metric_type: Type of metric (EXECUTION, SYSTEM, CUSTOM)
        """
        try:
            # Create metric based on name
            metric_data = {
                "metric_id": uuid4(),
                "execution_id": self.execution_id,
                "recorded_at": datetime.utcnow()
            }
            
            # Map metric name to appropriate field
            if "records_per_second" in metric_name.lower():
                metric_data["records_per_second"] = Decimal(str(value))
            elif "memory" in metric_name.lower():
                metric_data["memory_usage_mb"] = Decimal(str(value))
            elif "cpu" in metric_name.lower():
                metric_data["cpu_usage_percent"] = Decimal(str(value))
            elif "disk" in metric_name.lower():
                metric_data["disk_io_mb"] = Decimal(str(value))
            elif "network" in metric_name.lower():
                metric_data["network_io_mb"] = Decimal(str(value))
            elif "duration" in metric_name.lower():
                metric_data["duration_seconds"] = int(value)
            
            metric = PerformanceMetric(**metric_data)
            self.metrics.append(metric)
            
            self.logger.debug(f"Recorded metric: {metric_name} = {value}")
            
        except Exception as e:
            self.logger.warning(f"Failed to record metric {metric_name}: {str(e)}")
    
    def record_duration(self, metric_name: str = "execution_time"):
        """
        Record duration since start.
        
        Args:
            metric_name: Name for the duration metric
        """
        if self.start_time is None:
            self.logger.warning("Cannot record duration: timer not started")
            return
        
        duration = time.time() - self.start_time
        self.record(metric_name, duration)
    
    def record_count(self, metric_name: str, count: int):
        """
        Record a count metric.
        
        Args:
            metric_name: Name of the metric
            count: Count value
        """
        self.record(metric_name, float(count))
    
    def record_rate(
        self,
        metric_name: str,
        count: int,
        duration: Optional[float] = None
    ):
        """
        Record a rate metric (items per second).
        
        Args:
            metric_name: Name of the metric
            count: Number of items
            duration: Duration in seconds (uses elapsed time if not provided)
        """
        if duration is None:
            if self.start_time is None:
                self.logger.warning("Cannot calculate rate: timer not started")
                return
            duration = time.time() - self.start_time
        
        rate = count / duration if duration > 0 else 0
        self.record(metric_name, rate)
    
    def record_system_metrics(self):
        """Record current system resource usage"""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=0.1)
            self.record("cpu_usage", cpu_percent)
            
            # Memory usage
            memory = psutil.virtual_memory()
            memory_mb = memory.used / (1024 * 1024)
            memory_percent = memory.percent
            self.record("memory_usage", memory_mb)
            self.record("memory_percent", memory_percent)
            
            # Disk I/O
            disk_io = psutil.disk_io_counters()
            if disk_io:
                read_mb = disk_io.read_bytes / (1024 * 1024)
                write_mb = disk_io.write_bytes / (1024 * 1024)
                self.record("disk_read", read_mb)
                self.record("disk_write", write_mb)
            
            # Network I/O
            net_io = psutil.net_io_counters()
            if net_io:
                sent_mb = net_io.bytes_sent / (1024 * 1024)
                recv_mb = net_io.bytes_recv / (1024 * 1024)
                self.record("network_sent", sent_mb)
                self.record("network_received", recv_mb)
            
            self.logger.debug("System metrics recorded")
            
        except Exception as e:
            self.logger.warning(f"Failed to record system metrics: {str(e)}")
    
    def record_execution_metrics(
        self,
        records_processed: int,
        records_successful: int,
        records_failed: int
    ):
        """
        Record execution-specific metrics.
        
        Args:
            records_processed: Total records processed
            records_successful: Successfully processed records
            records_failed: Failed records
        """
        try:
            if self.start_time is None:
                self.logger.warning("Cannot record execution metrics: timer not started")
                return
            
            duration = time.time() - self.start_time
            
            # Create comprehensive metric
            metric = PerformanceMetric(
                metric_id=uuid4(),
                execution_id=self.execution_id,
                records_per_second=Decimal(str(records_processed / duration)) if duration > 0 else Decimal(0),
                duration_seconds=int(duration),
                memory_usage_mb=Decimal(str(psutil.virtual_memory().used / (1024 * 1024))),
                cpu_usage_percent=Decimal(str(psutil.cpu_percent(interval=0.1))),
                error_rate=Decimal(str((records_failed / records_processed * 100) if records_processed > 0 else 0)),
                recorded_at=datetime.utcnow()
            )
            
            self.metrics.append(metric)
            
            self.logger.info(
                f"Execution metrics: {records_processed} records in {duration:.2f}s "
                f"({metric.records_per_second:.2f} rec/s)"
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to record execution metrics: {str(e)}")
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary of collected metrics.
        
        Returns:
            Dictionary with metric summary
        """
        if not self.metrics:
            return {"total_metrics": 0}
        
        summary = {
            "total_metrics": len(self.metrics),
            "execution_id": str(self.execution_id) if self.execution_id else None,
            "duration": time.time() - self.start_time if self.start_time else 0,
        }
        
        # Calculate averages for numeric metrics
        if self.metrics:
            cpu_values = [m.cpu_usage_percent for m in self.metrics if m.cpu_usage_percent]
            memory_values = [m.memory_usage_mb for m in self.metrics if m.memory_usage_mb]
            
            if cpu_values:
                summary["avg_cpu_percent"] = float(sum(cpu_values) / len(cpu_values))
            if memory_values:
                summary["avg_memory_mb"] = float(sum(memory_values) / len(memory_values))
        
        return summary
    
    def commit(self):
        """Save all collected metrics to database"""
        try:
            if not self.metrics:
                self.logger.debug("No metrics to commit")
                return
            
            for metric in self.metrics:
                self.db.add(metric)
            
            self.db.commit()
            
            self.logger.info(f"Committed {len(self.metrics)} metrics to database")
            
        except Exception as e:
            self.db.rollback()
            self.logger.error(f"Failed to commit metrics: {str(e)}")
            raise
    
    def __enter__(self):
        """Context manager entry"""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - auto-commit metrics"""
        try:
            if exc_type is None:
                # Record final system metrics
                self.record_system_metrics()
                self.record_duration()
            
            # Commit metrics even if there was an exception
            self.commit()
        except Exception as e:
            self.logger.error(f"Error in metrics collector cleanup: {str(e)}")
        
        return False  # Don't suppress exceptions
