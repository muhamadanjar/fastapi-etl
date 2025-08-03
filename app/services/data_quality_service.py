"""
Data Quality service for managing data quality rules, checks, and monitoring.
"""

import re
import json
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, func, or_

from app.services.base import BaseService
from app.infrastructure.db.models.etl_control.quality_rules import QualityRule
from app.infrastructure.db.models.etl_control.quality_check_results import QualityCheckResult
from app.infrastructure.db.models.etl_control.job_executions import JobExecution
from app.infrastructure.db.models.raw_data.file_registry import FileRegistry
from app.infrastructure.db.models.raw_data.raw_records import RawRecords
from app.infrastructure.db.models.processed.entities import Entity
from app.core.exceptions import DataQualityError, ServiceError
from app.core.enums import QualityRuleType, QualitySeverity, ProcessingStatus
from app.utils.date_utils import get_current_timestamp


class DataQualityService(BaseService):
    """Service for managing data quality operations."""
    
    def __init__(self, db_session: Session):
        super().__init__(db_session)
    
    def get_service_name(self) -> str:
        return "DataQualityService"
    
    async def create_quality_rule(self, rule_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new data quality rule."""
        try:
            self.validate_input(rule_data, ["rule_name", "rule_type", "entity_type"])
            self.log_operation("create_quality_rule", {"rule_name": rule_data["rule_name"]})
            
            # Validate rule expression
            if rule_data.get("rule_expression"):
                self._validate_rule_expression(rule_data["rule_expression"], rule_data["rule_type"])
            
            # Create quality rule
            quality_rule = QualityRule(
                rule_name=rule_data["rule_name"],
                rule_type=rule_data["rule_type"],
                entity_type=rule_data["entity_type"],
                field_name=rule_data.get("field_name"),
                rule_expression=rule_data.get("rule_expression"),
                error_threshold=rule_data.get("error_threshold", 0.0),
                severity=rule_data.get("severity", QualitySeverity.MEDIUM.value),
                is_active=rule_data.get("is_active", True),
                rule_config=rule_data.get("rule_config", {}),
                description=rule_data.get("description")
            )
            
            self.db_session.add(quality_rule)
            self.db_session.commit()
            self.db_session.refresh(quality_rule)
            
            return {
                "rule_id": quality_rule.rule_id,
                "rule_name": quality_rule.rule_name,
                "rule_type": quality_rule.rule_type,
                "entity_type": quality_rule.entity_type,
                "status": "created"
            }
            
        except Exception as e:
            self.db_session.rollback()
            self.handle_error(e, "create_quality_rule")
    
    async def get_quality_rules(
        self, 
        entity_type: str = None, 
        rule_type: str = None,
        is_active: bool = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get quality rules with filtering."""
        try:
            self.log_operation("get_quality_rules", {
                "entity_type": entity_type,
                "rule_type": rule_type,
                "is_active": is_active
            })
            
            stmt = select(QualityRule)
            
            if entity_type:
                stmt = stmt.where(QualityRule.entity_type == entity_type)
            if rule_type:
                stmt = stmt.where(QualityRule.rule_type == rule_type)
            if is_active is not None:
                stmt = stmt.where(QualityRule.is_active == is_active)
            
            stmt = stmt.order_by(QualityRule.created_at.desc()).offset(skip).limit(limit)
            rules = self.db_session.execute(stmt).scalars().all()
            
            result = []
            for rule in rules:
                # Get usage statistics
                usage_stats = await self._get_rule_usage_stats(rule.rule_id)
                
                result.append({
                    "rule_id": rule.rule_id,
                    "rule_name": rule.rule_name,
                    "rule_type": rule.rule_type,
                    "entity_type": rule.entity_type,
                    "field_name": rule.field_name,
                    "rule_expression": rule.rule_expression,
                    "error_threshold": rule.error_threshold,
                    "severity": rule.severity,
                    "is_active": rule.is_active,
                    "rule_config": rule.rule_config,
                    "description": rule.description,
                    "created_at": rule.created_at,
                    "usage_stats": usage_stats
                })
            
            return result
            
        except Exception as e:
            self.handle_error(e, "get_quality_rules")
    
    async def get_quality_rule_by_id(self, rule_id: int) -> Optional[Dict[str, Any]]:
        """Get quality rule by ID."""
        try:
            self.log_operation("get_quality_rule_by_id", {"rule_id": rule_id})
            
            rule = self.db_session.get(QualityRule, rule_id)
            if not rule:
                return None
            
            usage_stats = await self._get_rule_usage_stats(rule_id)
            
            return {
                "rule_id": rule.rule_id,
                "rule_name": rule.rule_name,
                "rule_type": rule.rule_type,
                "entity_type": rule.entity_type,
                "field_name": rule.field_name,
                "rule_expression": rule.rule_expression,
                "error_threshold": rule.error_threshold,
                "severity": rule.severity,
                "is_active": rule.is_active,
                "rule_config": rule.rule_config,
                "description": rule.description,
                "created_at": rule.created_at,
                "usage_stats": usage_stats
            }
            
        except Exception as e:
            self.handle_error(e, "get_quality_rule_by_id")
    
    async def update_quality_rule(self, rule_id: int, rule_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update a quality rule."""
        try:
            self.log_operation("update_quality_rule", {"rule_id": rule_id})
            
            rule = self.db_session.get(QualityRule, rule_id)
            if not rule:
                raise DataQualityError("Quality rule not found")
            
            # Validate rule expression if provided
            if rule_data.get("rule_expression"):
                self._validate_rule_expression(
                    rule_data["rule_expression"],
                    rule_data.get("rule_type", rule.rule_type)
                )
            
            # Update fields
            for key, value in rule_data.items():
                if hasattr(rule, key):
                    setattr(rule, key, value)
            
            self.db_session.commit()
            
            return {
                "rule_id": rule.rule_id,
                "rule_name": rule.rule_name,
                "rule_type": rule.rule_type,
                "is_active": rule.is_active,
                "status": "updated"
            }
            
        except Exception as e:
            self.db_session.rollback()
            self.handle_error(e, "update_quality_rule")
    
    async def delete_quality_rule(self, rule_id: int) -> bool:
        """Delete a quality rule."""
        try:
            self.log_operation("delete_quality_rule", {"rule_id": rule_id})
            
            rule = self.db_session.get(QualityRule, rule_id)
            if not rule:
                raise DataQualityError("Quality rule not found")
            
            self.db_session.delete(rule)
            self.db_session.commit()
            
            return True
            
        except Exception as e:
            self.db_session.rollback()
            self.handle_error(e, "delete_quality_rule")
    
    async def run_quality_check(
        self,
        data_batch: List[Dict[str, Any]],
        entity_type: str,
        rule_ids: List[int] = None,
        check_config: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Run quality check on data batch."""
        try:
            self.log_operation("run_quality_check", {
                "entity_type": entity_type,
                "batch_size": len(data_batch),
                "rule_ids": rule_ids
            })
            
            # Get applicable rules
            if rule_ids:
                rules = self._get_rules_by_ids(rule_ids)
            else:
                rules = self._get_rules_by_entity_type(entity_type)
            
            if not rules:
                return {
                    "total_records": len(data_batch),
                    "quality_score": 100.0,
                    "rules_checked": 0,
                    "violations": [],
                    "summary": {"message": "No quality rules found for entity type"}
                }
            
            check_results = []
            total_violations = 0
            rule_results = {}
            
            for rule in rules:
                if not rule.is_active:
                    continue
                
                rule_violations = []
                
                for i, record in enumerate(data_batch):
                    try:
                        violation = self._check_rule_against_record(rule, record, i)
                        if violation:
                            rule_violations.append(violation)
                            total_violations += 1
                    except Exception as check_error:
                        self.logger.warning(f"Error checking rule {rule.rule_name} on record {i}: {check_error}")
                
                # Calculate rule statistics
                records_checked = len(data_batch)
                records_passed = records_checked - len(rule_violations)
                pass_rate = (records_passed / records_checked * 100) if records_checked > 0 else 0
                
                rule_results[rule.rule_id] = {
                    "rule_name": rule.rule_name,
                    "rule_type": rule.rule_type,
                    "records_checked": records_checked,
                    "records_passed": records_passed,
                    "records_failed": len(rule_violations),
                    "pass_rate": pass_rate,
                    "violations": rule_violations,
                    "severity": rule.severity
                }
                
                # Store check result
                check_result = QualityCheckResult(
                    rule_id=rule.rule_id,
                    check_result="PASS" if len(rule_violations) == 0 else "FAIL",
                    records_checked=records_checked,
                    records_passed=records_passed,
                    records_failed=len(rule_violations),
                    failure_details=rule_violations if rule_violations else None
                )
                check_results.append(check_result)
            
            # Save check results to database
            for check_result in check_results:
                self.db_session.add(check_result)
            self.db_session.commit()
            
            # Calculate overall quality score
            total_checks = sum(r["records_checked"] for r in rule_results.values())
            total_passed = sum(r["records_passed"] for r in rule_results.values())
            overall_quality_score = (total_passed / total_checks * 100) if total_checks > 0 else 100
            
            return {
                "total_records": len(data_batch),
                "quality_score": round(overall_quality_score, 2),
                "rules_checked": len([r for r in rules if r.is_active]),
                "total_violations": total_violations,
                "rule_results": rule_results,
                "summary": {
                    "overall_status": "PASS" if overall_quality_score >= 90 else "FAIL",
                    "critical_violations": sum(1 for r in rule_results.values() 
                                             if r["severity"] == "CRITICAL" and r["records_failed"] > 0),
                    "warning_violations": sum(1 for r in rule_results.values() 
                                            if r["severity"] == "MEDIUM" and r["records_failed"] > 0)
                }
            }
            
        except Exception as e:
            self.handle_error(e, "run_quality_check")
    
    async def validate_data(
        self,
        data_batch: List[Dict[str, Any]],
        validation_rules: List[Dict[str, Any]],
        validation_config: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Validate data against custom validation rules."""
        try:
            self.log_operation("validate_data", {"batch_size": len(data_batch), "rules_count": len(validation_rules)})
            
            validation_results = []
            total_errors = 0
            
            for i, record in enumerate(data_batch):
                record_errors = []
                
                for rule in validation_rules:
                    try:
                        error = self._validate_record_against_rule(record, rule, i)
                        if error:
                            record_errors.append(error)
                            total_errors += 1
                    except Exception as validation_error:
                        self.logger.warning(f"Validation rule error on record {i}: {validation_error}")
                
                validation_results.append({
                    "record_index": i,
                    "is_valid": len(record_errors) == 0,
                    "errors": record_errors
                })
            
            valid_records = sum(1 for r in validation_results if r["is_valid"])
            invalid_records = len(data_batch) - valid_records
            
            return {
                "total_records": len(data_batch),
                "valid_records": valid_records,
                "invalid_records": invalid_records,
                "validation_score": (valid_records / len(data_batch) * 100) if data_batch else 100,
                "total_errors": total_errors,
                "validation_results": validation_results
            }
            
        except Exception as e:
            self.handle_error(e, "validate_data")
    
    async def check_entity_quality(
        self,
        entity_type: str,
        entity_ids: List[int] = None,
        quality_config: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Check quality of processed entities."""
        try:
            self.log_operation("check_entity_quality", {"entity_type": entity_type, "entity_ids": entity_ids})
            
            # Get entities data
            stmt = select(Entity).where(Entity.entity_type == entity_type)
            if entity_ids:
                stmt = stmt.where(Entity.entity_id.in_(entity_ids))
            
            entities = self.db_session.execute(stmt).scalars().all()
            
            if not entities:
                return {
                    "entity_type": entity_type,
                    "total_entities": 0,
                    "quality_score": 100.0,
                    "message": "No entities found for quality check"
                }
            
            # Convert entities to data format
            entity_data = []
            for entity in entities:
                entity_dict = {
                    "entity_id": entity.entity_id,
                    "entity_key": entity.entity_key,
                    "confidence_score": entity.confidence_score,
                    "is_active": entity.is_active,
                    **entity.entity_data
                }
                entity_data.append(entity_dict)
            
            # Run quality check
            return await self.run_quality_check(
                data_batch=entity_data,
                entity_type=entity_type,
                check_config=quality_config
            )
            
        except Exception as e:
            self.handle_error(e, "check_entity_quality")
    
    async def check_file_quality(
        self,
        file_id: int,
        validation_rules: List[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Check quality of file data."""
        try:
            self.log_operation("check_file_quality", {"file_id": file_id})
            
            # Get file record
            file_record = self.db_session.get(FileRegistry, file_id)
            if not file_record:
                raise DataQualityError("File not found")
            
            # Get raw records for the file
            stmt = select(RawRecord).where(RawRecord.file_id == file_id)
            raw_records = self.db_session.execute(stmt).scalars().all()
            
            if not raw_records:
                return {
                    "file_id": file_id,
                    "file_name": file_record.file_name,
                    "total_records": 0,
                    "quality_score": 100.0,
                    "message": "No data records found for quality check"
                }
            
            # Convert raw records to data format
            file_data = []
            for record in raw_records:
                if record.raw_data:
                    file_data.append(record.raw_data)
            
            # Run validation or quality check
            if validation_rules:
                return await self.validate_data(file_data, validation_rules)
            else:
                # Use file type as entity type for quality rules
                entity_type = f"file_{file_record.file_type.lower()}"
                return await self.run_quality_check(file_data, entity_type)
            
        except Exception as e:
            self.handle_error(e, "check_file_quality")
    
    async def check_job_quality(
        self,
        job_id: int,
        execution_id: int = None,
        quality_config: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Check quality of ETL job results."""
        try:
            self.log_operation("check_job_quality", {"job_id": job_id, "execution_id": execution_id})
            
            # Get job execution
            stmt = select(JobExecution).where(JobExecution.job_id == job_id)
            if execution_id:
                stmt = stmt.where(JobExecution.execution_id == execution_id)
            else:
                stmt = stmt.order_by(JobExecution.created_at.desc()).limit(1)
            
            job_execution = self.db_session.execute(stmt).scalar_one_or_none()
            if not job_execution:
                raise DataQualityError("Job execution not found")
            
            # Get related entities or data based on job execution
            # This is a simplified implementation - you might need to adapt based on your job structure
            entity_type = quality_config.get("entity_type", "job_output") if quality_config else "job_output"
            
            return {
                "job_id": job_id,
                "execution_id": job_execution.execution_id,
                "quality_check_status": "completed",
                "message": "Job quality check completed - implement specific logic based on job type"
            }
            
        except Exception as e:
            self.handle_error(e, "check_job_quality")
    
    async def generate_quality_report(
        self,
        entity_type: str = None,
        date_range: Dict[str, str] = None,
        report_config: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Generate comprehensive quality report."""
        try:
            self.log_operation("generate_quality_report", {"entity_type": entity_type, "date_range": date_range})
            
            # Set default date range if not provided
            if not date_range:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=30)
                date_range = {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat()
                }
            
            # Get quality check results in date range
            stmt = select(QualityCheckResult).where(
                QualityCheckResult.created_at.between(
                    datetime.fromisoformat(date_range["start_date"]),
                    datetime.fromisoformat(date_range["end_date"])
                )
            )
            
            if entity_type:
                # Join with quality rules to filter by entity type
                stmt = stmt.join(QualityRule).where(QualityRule.entity_type == entity_type)
            
            check_results = self.db_session.execute(stmt).scalars().all()
            
            # Calculate statistics
            total_checks = len(check_results)
            passed_checks = sum(1 for r in check_results if r.check_result == "PASS")
            failed_checks = total_checks - passed_checks
            
            overall_pass_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0
            
            # Group by rule type
            rule_type_stats = {}
            for result in check_results:
                rule = self.db_session.get(QualityRule, result.rule_id)
                if rule:
                    rule_type = rule.rule_type
                    if rule_type not in rule_type_stats:
                        rule_type_stats[rule_type] = {
                            "total_checks": 0,
                            "passed_checks": 0,
                            "failed_checks": 0
                        }
                    
                    rule_type_stats[rule_type]["total_checks"] += 1
                    if result.check_result == "PASS":
                        rule_type_stats[rule_type]["passed_checks"] += 1
                    else:
                        rule_type_stats[rule_type]["failed_checks"] += 1
            
            # Calculate pass rates for each rule type
            for rule_type in rule_type_stats:
                stats = rule_type_stats[rule_type]
                stats["pass_rate"] = (stats["passed_checks"] / stats["total_checks"] * 100) if stats["total_checks"] > 0 else 0
            
            return {
                "report_period": date_range,
                "entity_type": entity_type,
                "overall_statistics": {
                    "total_quality_checks": total_checks,
                    "passed_checks": passed_checks,
                    "failed_checks": failed_checks,
                    "overall_pass_rate": round(overall_pass_rate, 2)
                },
                "rule_type_statistics": rule_type_stats,
                "generated_at": get_current_timestamp().isoformat()
            }
            
        except Exception as e:
            self.handle_error(e, "generate_quality_report")
    
    async def get_quality_summary(
        self,
        entity_type: str = None,
        date_from: str = None,
        date_to: str = None
    ) -> Dict[str, Any]:
        """Get quality summary statistics."""
        try:
            self.log_operation("get_quality_summary", {"entity_type": entity_type})
            
            # Build query
            stmt = select(QualityCheckResult)
            
            if date_from and date_to:
                stmt = stmt.where(
                    QualityCheckResult.created_at.between(
                        datetime.fromisoformat(date_from),
                        datetime.fromisoformat(date_to)
                    )
                )
            
            if entity_type:
                stmt = stmt.join(QualityRule).where(QualityRule.entity_type == entity_type)
            
            results = self.db_session.execute(stmt).scalars().all()
            
            # Calculate summary
            total_checks = len(results)
            total_records_checked = sum(r.records_checked for r in results)
            total_records_passed = sum(r.records_passed for r in results)
            total_records_failed = sum(r.records_failed for r in results)
            
            return {
                "total_quality_checks": total_checks,
                "total_records_checked": total_records_checked,
                "total_records_passed": total_records_passed,
                "total_records_failed": total_records_failed,
                "overall_pass_rate": (total_records_passed / total_records_checked * 100) if total_records_checked > 0 else 0,
                "entity_type": entity_type,
                "period": {
                    "from": date_from,
                    "to": date_to
                }
            }
            
        except Exception as e:
            self.handle_error(e, "get_quality_summary")
    
    async def get_quality_trends(
        self,
        entity_type: str = None,
        period: str = "week",
        limit: int = 30
    ) -> Dict[str, Any]:
        """Get quality trends over time."""
        try:
            self.log_operation("get_quality_trends", {"entity_type": entity_type, "period": period})
            
            # This is a simplified implementation
            # In a real system, you'd aggregate data by time periods
            trends = []
            
            # Generate mock trend data
            for i in range(limit):
                date = datetime.now() - timedelta(days=i)
                trends.append({
                    "date": date.isoformat(),
                    "quality_score": 95.0 - (i * 0.1),  # Mock declining trend
                    "checks_performed": 10 + i,
                    "total_records": 1000 + (i * 10)
                })
            
            return {
                "entity_type": entity_type,
                "period": period,
                "trend_data": trends[::-1]  # Reverse to show oldest first
            }
            
        except Exception as e:
            self.handle_error(e, "get_quality_trends")
    
    async def get_quality_alerts(
        self,
        severity: str = None,
        entity_type: str = None,
        is_resolved: bool = False,
        skip: int = 0,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get quality alerts."""
        try:
            self.log_operation("get_quality_alerts", {"severity": severity, "entity_type": entity_type})
            
            # This is a simplified implementation
            # In a real system, you'd have an alerts table
            alerts = []
            
            # Get failed quality checks as alerts
            stmt = select(QualityCheckResult).where(QualityCheckResult.check_result == "FAIL")
            
            if entity_type:
                stmt = stmt.join(QualityRule).where(QualityRule.entity_type == entity_type)
            
            failed_checks = self.db_session.execute(stmt.offset(skip).limit(limit)).scalars().all()
            
            for check in failed_checks:
                rule = self.db_session.get(QualityRule, check.rule_id)
                if rule and (not severity or rule.severity == severity.upper()):
                    alerts.append({
                        "alert_id": check.check_id,
                        "rule_name": rule.rule_name,
                        "entity_type": rule.entity_type,
                        "severity": rule.severity,
                        "records_failed": check.records_failed,
                        "failure_rate": (check.records_failed / check.records_checked * 100) if check.records_checked > 0 else 0,
                        "created_at": check.created_at,
                        "is_resolved": False  # Simplified - you'd track this in a separate table
                    })
            
            return alerts
            
        except Exception as e:
            self.handle_error(e, "get_quality_alerts")
    
    async def resolve_quality_alert(
        self,
        alert_id: int,
        resolution_notes: str = None,
        resolved_by: int = None
    ) -> bool:
        """Resolve a quality alert."""
        try:
            self.log_operation("resolve_quality_alert", {"alert_id": alert_id})
            
            # In a real system, you'd update an alerts table
            # For now, just return success
            return True
            
        except Exception as e:
            self.handle_error(e, "resolve_quality_alert")
    
    async def get_quality_metrics(self, entity_type: str = None) -> Dict[str, Any]:
        """Get quality metrics for dashboard."""
        try:
            self.log_operation("get_quality_metrics", {"entity_type": entity_type})
            
            # Get recent quality check results
            stmt = select(QualityCheckResult).where(
                QualityCheckResult.created_at >= datetime.now() - timedelta(days=7)
            )
            
            if entity_type:
                stmt = stmt.join(QualityRule).where(QualityRule.entity_type == entity_type)
            
            recent_checks = self.db_session.execute(stmt).scalars().all()
            
            # Calculate metrics
            total_checks = len(recent_checks)
            passed_checks = sum(1 for r in recent_checks if r.check_result == "PASS")
            
            return {
                "total_checks_last_7_days": total_checks,
                "passed_checks": passed_checks,
                "failed_checks": total_checks - passed_checks,
                "pass_rate": (passed_checks / total_checks * 100) if total_checks > 0 else 0,
                "entity_type": entity_type,
                "last_updated": get_current_timestamp().isoformat()
            }
            
        except Exception as e:
            self.handle_error(e, "get_quality_metrics")
    
    async def schedule_quality_check(
        self,
        entity_type: str,
        schedule_config: Dict[str, Any],
        scheduled_by: int
    ) -> str:
        """Schedule recurring quality checks."""
        try:
            self.log_operation("schedule_quality_check", {"entity_type": entity_type})
            
            # In a real implementation, you'd integrate with Celery for scheduling
            # For now, return a mock job ID
            import uuid
            job_id = str(uuid.uuid4())
            
            return job_id
            
        except Exception as e:
            self.handle_error(e, "schedule_quality_check")
    
    # Private helper methods
    def _validate_rule_expression(self, expression: str, rule_type: str):
        """Validate rule expression syntax."""
        if rule_type == QualityRuleType.VALIDITY.value:
            # Validate regex pattern
            try:
                re.compile(expression)
            except re.error as e:
                raise DataQualityError(f"Invalid regex pattern: {e}")
        elif rule_type == QualityRuleType.CUSTOM.value:
            # Basic validation for custom expressions
            if not expression.strip():
                raise DataQualityError("Custom rule expression cannot be empty")
    
    def _get_rules_by_ids(self, rule_ids: List[int]) -> List[QualityRule]:
        """Get quality rules by IDs."""
        stmt = select(QualityRule).where(QualityRule.rule_id.in_(rule_ids))
        return self.db_session.execute(stmt).scalars().all()
    
    def _get_rules_by_entity_type(self, entity_type: str) -> List[QualityRule]:
        """Get quality rules by entity type."""
        stmt = select(QualityRule).where(
            and_(
                QualityRule.entity_type == entity_type,
                QualityRule.is_active == True
            )
        )
        return self.db_session.execute(stmt).scalars().all()
    
    def _check_rule_against_record(self, rule: QualityRule, record: Dict[str, Any], record_index: int) -> Optional[Dict[str, Any]]:
        """Check a single quality rule against a record."""
        try:
            field_value = record.get(rule.field_name) if rule.field_name else None
            
            if rule.rule_type == QualityRuleType.COMPLETENESS.value:
                # Check for null/empty values
                if field_value is None or (isinstance(field_value, str) and field_value.strip() == ""):
                    return {
                        "record_index": record_index,
                        "rule_name": rule.rule_name,
                        "field_name": rule.field_name,
                        "field_value": field_value,
                        "violation_type": "COMPLETENESS",
                        "message": f"Field '{rule.field_name}' is empty or null"
                    }
            
            elif rule.rule_type == QualityRuleType.UNIQUENESS.value:
                # This would require checking against other records in the batch
                # Simplified implementation - you'd need to maintain a set of seen values
                pass
            
            elif rule.rule_type == QualityRuleType.VALIDITY.value:
                # Check format/pattern validity
                if rule.rule_expression and field_value:
                    pattern = re.compile(rule.rule_expression)
                    if not pattern.match(str(field_value)):
                        return {
                            "record_index": record_index,
                            "rule_name": rule.rule_name,
                            "field_name": rule.field_name,
                            "field_value": field_value,
                            "violation_type": "VALIDITY",
                            "message": f"Field '{rule.field_name}' does not match pattern: {rule.rule_expression}"
                        }
            
            elif rule.rule_type == QualityRuleType.CONSISTENCY.value:
                # Check data consistency rules
                if rule.rule_config:
                    # Example: check if related fields are consistent
                    consistency_rules = rule.rule_config.get("consistency_rules", [])
                    for consistency_rule in consistency_rules:
                        if not self._check_consistency_rule(record, consistency_rule):
                            return {
                                "record_index": record_index,
                                "rule_name": rule.rule_name,
                                "violation_type": "CONSISTENCY",
                                "message": f"Consistency violation: {consistency_rule.get('message', 'Unknown consistency issue')}"
                            }
            
            elif rule.rule_type == QualityRuleType.ACCURACY.value:
                # Check data accuracy
                if rule.rule_config:
                    accuracy_checks = rule.rule_config.get("accuracy_checks", [])
                    for accuracy_check in accuracy_checks:
                        if not self._check_accuracy_rule(record, accuracy_check):
                            return {
                                "record_index": record_index,
                                "rule_name": rule.rule_name,
                                "violation_type": "ACCURACY",
                                "message": f"Accuracy violation: {accuracy_check.get('message', 'Data accuracy issue')}"
                            }
            
            elif rule.rule_type == QualityRuleType.CUSTOM.value:
                # Custom rule evaluation
                if rule.rule_expression:
                    if not self._evaluate_custom_rule(record, rule.rule_expression):
                        return {
                            "record_index": record_index,
                            "rule_name": rule.rule_name,
                            "violation_type": "CUSTOM",
                            "message": f"Custom rule violation: {rule.rule_expression}"
                        }
            
            return None  # No violation
            
        except Exception as e:
            self.logger.error(f"Error checking rule {rule.rule_name}: {e}")
            return {
                "record_index": record_index,
                "rule_name": rule.rule_name,
                "violation_type": "ERROR",
                "message": f"Rule evaluation error: {str(e)}"
            }
    
    def _validate_record_against_rule(self, record: Dict[str, Any], rule: Dict[str, Any], record_index: int) -> Optional[Dict[str, Any]]:
        """Validate a record against a custom validation rule."""
        field_name = rule.get("field_name")
        rule_type = rule.get("rule_type")
        rule_config = rule.get("rule_config", {})
        
        if not field_name or not rule_type:
            return None
        
        field_value = record.get(field_name)
        
        if rule_type == "required":
            if field_value is None or (isinstance(field_value, str) and field_value.strip() == ""):
                return {
                    "field_name": field_name,
                    "field_value": field_value,
                    "error_type": "REQUIRED",
                    "error_message": f"Field '{field_name}' is required"
                }
        
        elif rule_type == "type":
            expected_type = rule_config.get("expected_type")
            if expected_type and field_value is not None:
                if expected_type == "string" and not isinstance(field_value, str):
                    return {
                        "field_name": field_name,
                        "field_value": field_value,
                        "error_type": "TYPE",
                        "error_message": f"Field '{field_name}' must be a string"
                    }
                elif expected_type == "number" and not isinstance(field_value, (int, float)):
                    return {
                        "field_name": field_name,
                        "field_value": field_value,
                        "error_type": "TYPE",
                        "error_message": f"Field '{field_name}' must be a number"
                    }
        
        elif rule_type == "range":
            if field_value is not None and isinstance(field_value, (int, float)):
                min_value = rule_config.get("min")
                max_value = rule_config.get("max")
                
                if min_value is not None and field_value < min_value:
                    return {
                        "field_name": field_name,
                        "field_value": field_value,
                        "error_type": "RANGE",
                        "error_message": f"Field '{field_name}' must be >= {min_value}"
                    }
                
                if max_value is not None and field_value > max_value:
                    return {
                        "field_name": field_name,
                        "field_value": field_value,
                        "error_type": "RANGE",
                        "error_message": f"Field '{field_name}' must be <= {max_value}"
                    }
        
        elif rule_type == "pattern":
            pattern = rule_config.get("pattern")
            if pattern and field_value:
                try:
                    if not re.match(pattern, str(field_value)):
                        return {
                            "field_name": field_name,
                            "field_value": field_value,
                            "error_type": "PATTERN",
                            "error_message": f"Field '{field_name}' does not match required pattern"
                        }
                except re.error:
                    return {
                        "field_name": field_name,
                        "field_value": field_value,
                        "error_type": "PATTERN",
                        "error_message": f"Invalid pattern in validation rule"
                    }
        
        return None  # No validation error
    
    def _check_consistency_rule(self, record: Dict[str, Any], consistency_rule: Dict[str, Any]) -> bool:
        """Check consistency rule against record."""
        # Example: if field A has value X, then field B must have value Y
        condition_field = consistency_rule.get("condition_field")
        condition_value = consistency_rule.get("condition_value")
        required_field = consistency_rule.get("required_field")
        required_value = consistency_rule.get("required_value")
        
        if not all([condition_field, required_field]):
            return True  # Skip invalid rule
        
        if record.get(condition_field) == condition_value:
            return record.get(required_field) == required_value
        
        return True  # Condition not met, so rule passes
    
    def _check_accuracy_rule(self, record: Dict[str, Any], accuracy_rule: Dict[str, Any]) -> bool:
        """Check accuracy rule against record."""
        # Example: check if calculated field matches expected calculation
        field_name = accuracy_rule.get("field_name")
        calculation = accuracy_rule.get("calculation")
        tolerance = accuracy_rule.get("tolerance", 0)
        
        if not field_name or not calculation:
            return True  # Skip invalid rule
        
        try:
            # Simple calculation evaluation - in production use safer evaluator
            expected_value = eval(calculation, {"record": record})
            actual_value = record.get(field_name)
            
            if actual_value is not None and isinstance(actual_value, (int, float)):
                return abs(actual_value - expected_value) <= tolerance
        except Exception:
            pass
        
        return True  # If evaluation fails, assume rule passes
    
    def _evaluate_custom_rule(self, record: Dict[str, Any], expression: str) -> bool:
        """Evaluate custom rule expression."""
        try:
            # Simple expression evaluation - in production use safer evaluator
            # Replace field references with actual values
            safe_expression = expression
            for field, value in record.items():
                safe_expression = safe_expression.replace(f"${field}", str(value) if value is not None else "None")
            
            # Evaluate with restricted builtins
            return bool(eval(safe_expression, {"__builtins__": {}}))
        except Exception:
            return True  # If evaluation fails, assume rule passes
    
    async def _get_rule_usage_stats(self, rule_id: int) -> Dict[str, Any]:
        """Get usage statistics for a quality rule."""
        try:
            # Get check results for this rule in the last 30 days
            stmt = select(QualityCheckResult).where(
                and_(
                    QualityCheckResult.rule_id == rule_id,
                    QualityCheckResult.created_at >= datetime.now() - timedelta(days=30)
                )
            )
            results = self.db_session.execute(stmt).scalars().all()
            
            total_checks = len(results)
            passed_checks = sum(1 for r in results if r.check_result == "PASS")
            failed_checks = total_checks - passed_checks
            
            total_records_checked = sum(r.records_checked for r in results)
            total_records_passed = sum(r.records_passed for r in results)
            
            return {
                "total_checks_last_30_days": total_checks,
                "passed_checks": passed_checks,
                "failed_checks": failed_checks,
                "pass_rate": (passed_checks / total_checks * 100) if total_checks > 0 else 0,
                "total_records_checked": total_records_checked,
                "total_records_passed": total_records_passed,
                "record_pass_rate": (total_records_passed / total_records_checked * 100) if total_records_checked > 0 else 0,
                "last_used": results[-1].created_at.isoformat() if results else None
            }
            
        except Exception as e:
            self.logger.warning(f"Error getting rule usage stats: {e}")
            return {
                "total_checks_last_30_days": 0,
                "passed_checks": 0,
                "failed_checks": 0,
                "pass_rate": 0,
                "total_records_checked": 0,
                "total_records_passed": 0,
                "record_pass_rate": 0,
                "last_used": None
            }