"""
Data quality service for managing data quality rules and validation.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, func
from app.services.base import BaseService
from app.core.exceptions import DataQualityError, ServiceError
from app.core.enums import QualityRuleType, QualityCheckResult
from app.utils.validation_utils import validate_data_completeness, validate_data_uniqueness, validate_data_format
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
            if "rule_expression" in rule_data:
                await self._validate_rule_expression(rule_data["rule_expression"], rule_data["rule_type"])
            
            # Create rule record
            rule = await self._create_quality_rule_record({
                "rule_name": rule_data["rule_name"],
                "rule_type": rule_data["rule_type"],
                "entity_type": rule_data["entity_type"],
                "field_name": rule_data.get("field_name"),
                "rule_expression": rule_data.get("rule_expression"),
                "error_threshold": rule_data.get("error_threshold", 0.05),  # 5% default
                "is_active": rule_data.get("is_active", True),
                "created_at": get_current_timestamp()
            })
            
            return {
                "rule_id": rule.rule_id,
                "rule_name": rule.rule_name,
                "rule_type": rule.rule_type,
                "entity_type": rule.entity_type,
                "status": "created"
            }
            
        except Exception as e:
            self.handle_error(e, "create_quality_rule")
    
    async def execute_quality_checks(self, execution_id: int, data_batch: List[Dict[str, Any]], entity_type: str) -> Dict[str, Any]:
        """Execute quality checks on a data batch."""
        try:
            self.log_operation("execute_quality_checks", {
                "execution_id": execution_id,
                "entity_type": entity_type,
                "batch_size": len(data_batch)
            })
            
            # Get active rules for entity type
            rules = await self._get_active_rules_for_entity(entity_type)
            
            if not rules:
                self.logger.warning(f"No quality rules found for entity type: {entity_type}")
                return {"total_checks": 0, "passed_checks": 0, "failed_checks": 0}
            
            check_results = []
            total_passed = 0
            total_failed = 0
            
            # Execute each rule
            for rule in rules:
                try:
                    result = await self._execute_single_rule(rule, data_batch)
                    
                    # Save check result
                    check_result = await self._save_quality_check_result({
                        "execution_id": execution_id,
                        "rule_id": rule.rule_id,
                        "check_result": result["status"],
                        "records_checked": result["records_checked"],
                        "records_passed": result["records_passed"],
                        "records_failed": result["records_failed"],
                        "failure_details": result.get("failure_details", {}),
                        "created_at": get_current_timestamp()
                    })
                    
                    check_results.append({
                        "rule_name": rule.rule_name,
                        "rule_type": rule.rule_type,
                        "status": result["status"],
                        "pass_rate": result["pass_rate"],
                        "records_checked": result["records_checked"],
                        "records_passed": result["records_passed"],
                        "records_failed": result["records_failed"]
                    })
                    
                    if result["status"] == QualityCheckResult.PASS.value:
                        total_passed += 1
                    else:
                        total_failed += 1
                        
                except Exception as rule_error:
                    self.logger.error(f"Error executing rule {rule.rule_name}: {rule_error}")
                    total_failed += 1
                    
                    # Save failed check result
                    await self._save_quality_check_result({
                        "execution_id": execution_id,
                        "rule_id": rule.rule_id,
                        "check_result": QualityCheckResult.FAIL.value,
                        "records_checked": len(data_batch),
                        "records_passed": 0,
                        "records_failed": len(data_batch),
                        "failure_details": {"error": str(rule_error)},
                        "created_at": get_current_timestamp()
                    })
            
            return {
                "total_checks": len(rules),
                "passed_checks": total_passed,
                "failed_checks": total_failed,
                "success_rate": (total_passed / len(rules) * 100) if rules else 0,
                "check_details": check_results
            }
            
        except Exception as e:
            self.handle_error(e, "execute_quality_checks")
    
    async def get_quality_report(self, execution_id: int = None, entity_type: str = None) -> Dict[str, Any]:
        """Get quality report for execution or entity type."""
        try:
            self.log_operation("get_quality_report", {
                "execution_id": execution_id,
                "entity_type": entity_type
            })
            
            # Get quality check results
            check_results = await self._get_quality_check_results(execution_id, entity_type)
            
            if not check_results:
                return {
                    "total_checks": 0,
                    "summary": {},
                    "rule_results": []
                }
            
            # Calculate summary statistics
            total_checks = len(check_results)
            passed_checks = len([r for r in check_results if r.check_result == QualityCheckResult.PASS.value])
            failed_checks = len([r for r in check_results if r.check_result == QualityCheckResult.FAIL.value])
            warning_checks = len([r for r in check_results if r.check_result == QualityCheckResult.WARNING.value])
            
            total_records = sum(r.records_checked for r in check_results)
            total_passed_records = sum(r.records_passed for r in check_results)
            total_failed_records = sum(r.records_failed for r in check_results)
            
            # Group by rule type
            rule_type_summary = {}
            for result in check_results:
                rule_type = result.quality_rule.rule_type
                if rule_type not in rule_type_summary:
                    rule_type_summary[rule_type] = {
                        "total": 0,
                        "passed": 0,
                        "failed": 0,
                        "warning": 0
                    }
                
                rule_type_summary[rule_type]["total"] += 1
                if result.check_result == QualityCheckResult.PASS.value:
                    rule_type_summary[rule_type]["passed"] += 1
                elif result.check_result == QualityCheckResult.FAIL.value:
                    rule_type_summary[rule_type]["failed"] += 1
                else:
                    rule_type_summary[rule_type]["warning"] += 1
            
            return {
                "total_checks": total_checks,
                "summary": {
                    "passed_checks": passed_checks,
                    "failed_checks": failed_checks,
                    "warning_checks": warning_checks,
                    "success_rate": (passed_checks / total_checks * 100) if total_checks > 0 else 0,
                    "total_records": total_records,
                    "passed_records": total_passed_records,
                    "failed_records": total_failed_records,
                    "record_pass_rate": (total_passed_records / total_records * 100) if total_records > 0 else 0
                },
                "rule_type_summary": rule_type_summary,
                "rule_results": [{
                    "rule_id": r.rule_id,
                    "rule_name": r.quality_rule.rule_name,
                    "rule_type": r.quality_rule.rule_type,
                    "entity_type": r.quality_rule.entity_type,
                    "field_name": r.quality_rule.field_name,
                    "check_result": r.check_result,
                    "records_checked": r.records_checked,
                    "records_passed": r.records_passed,
                    "records_failed": r.records_failed,
                    "pass_rate": (r.records_passed / r.records_checked * 100) if r.records_checked > 0 else 0,
                    "failure_details": r.failure_details,
                    "created_at": r.created_at
                } for r in check_results]
            }
            
        except Exception as e:
            self.handle_error(e, "get_quality_report")
    
    async def get_quality_rules(self, entity_type: str = None, rule_type: str = None) -> List[Dict[str, Any]]:
        """Get quality rules with optional filtering."""
        try:
            self.log_operation("get_quality_rules", {
                "entity_type": entity_type,
                "rule_type": rule_type
            })
            
            rules = await self._get_quality_rules(entity_type, rule_type)
            
            result = []
            for rule in rules:
                # Get recent execution stats
                recent_stats = await self._get_rule_recent_stats(rule.rule_id)
                
                result.append({
                    "rule_id": rule.rule_id,
                    "rule_name": rule.rule_name,
                    "rule_type": rule.rule_type,
                    "entity_type": rule.entity_type,
                    "field_name": rule.field_name,
                    "rule_expression": rule.rule_expression,
                    "error_threshold": rule.error_threshold,
                    "is_active": rule.is_active,
                    "created_at": rule.created_at,
                    "recent_stats": recent_stats
                })
            
            return result
            
        except Exception as e:
            self.handle_error(e, "get_quality_rules")
    
    async def update_quality_rule(self, rule_id: int, rule_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing quality rule."""
        try:
            self.log_operation("update_quality_rule", {"rule_id": rule_id})
            
            rule = await self._get_quality_rule_by_id(rule_id)
            if not rule:
                raise DataQualityError("Quality rule not found")
            
            # Validate rule expression if provided
            if "rule_expression" in rule_data:
                await self._validate_rule_expression(
                    rule_data["rule_expression"],
                    rule_data.get("rule_type", rule.rule_type)
                )
            
            # Update rule
            updated_rule = await self._update_quality_rule_record(rule_id, rule_data)
            
            return {
                "rule_id": updated_rule.rule_id,
                "rule_name": updated_rule.rule_name,
                "rule_type": updated_rule.rule_type,
                "is_active": updated_rule.is_active,
                "status": "updated"
            }
            
        except Exception as e:
            self.handle_error(e, "update_quality_rule")
    
    async def delete_quality_rule(self, rule_id: int) -> bool:
        """Delete a quality rule."""
        try:
            self.log_operation("delete_quality_rule", {"rule_id": rule_id})
            
            rule = await self._get_quality_rule_by_id(rule_id)
            if not rule:
                raise DataQualityError("Quality rule not found")
            
            # Delete rule (cascade should handle check results)
            await self._delete_quality_rule_record(rule_id)
            
            return True
            
        except Exception as e:
            self.handle_error(e, "delete_quality_rule")
    
    async def validate_data_batch(self, data_batch: List[Dict[str, Any]], entity_type: str) -> Dict[str, Any]:
        """Validate a data batch without saving results."""
        try:
            self.log_operation("validate_data_batch", {
                "entity_type": entity_type,
                "batch_size": len(data_batch)
            })
            
            # Get active rules for entity type
            rules = await self._get_active_rules_for_entity(entity_type)
            
            validation_results = []
            total_passed = 0
            total_failed = 0
            
            for rule in rules:
                try:
                    result = await self._execute_single_rule(rule, data_batch)
                    validation_results.append({
                        "rule_name": rule.rule_name,
                        "rule_type": rule.rule_type,
                        "field_name": rule.field_name,
                        "status": result["status"],
                        "pass_rate": result["pass_rate"],
                        "records_checked": result["records_checked"],
                        "records_passed": result["records_passed"],
                        "records_failed": result["records_failed"],
                        "failure_details": result.get("failure_details", {})
                    })
                    
                    if result["status"] == QualityCheckResult.PASS.value:
                        total_passed += 1
                    else:
                        total_failed += 1
                        
                except Exception as rule_error:
                    self.logger.error(f"Error validating rule {rule.rule_name}: {rule_error}")
                    total_failed += 1
            
            return {
                "entity_type": entity_type,
                "batch_size": len(data_batch),
                "total_rules": len(rules),
                "passed_rules": total_passed,
                "failed_rules": total_failed,
                "overall_status": "PASS" if total_failed == 0 else "FAIL",
                "validation_results": validation_results
            }
            
        except Exception as e:
            self.handle_error(e, "validate_data_batch")
    
    # Private helper methods
    async def _execute_single_rule(self, rule, data_batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute a single quality rule on data batch."""
        records_checked = len(data_batch)
        records_passed = 0
        records_failed = 0
        failure_details = {}
        
        if rule.rule_type == QualityRuleType.COMPLETENESS.value:
            # Check for non-null values
            if rule.field_name:
                for record in data_batch:
                    if rule.field_name in record and record[rule.field_name] is not None and str(record[rule.field_name]).strip():
                        records_passed += 1
                    else:
                        records_failed += 1
            
        elif rule.rule_type == QualityRuleType.UNIQUENESS.value:
            # Check for unique values
            if rule.field_name:
                seen_values = set()
                duplicates = []
                for i, record in enumerate(data_batch):
                    value = record.get(rule.field_name)
                    if value in seen_values:
                        records_failed += 1
                        duplicates.append({"row": i, "value": value})
                    else:
                        records_passed += 1
                        seen_values.add(value)
                
                if duplicates:
                    failure_details["duplicates"] = duplicates[:10]  # Limit to first 10
            
        elif rule.rule_type == QualityRuleType.VALIDITY.value:
            # Check format/pattern validity
            if rule.rule_expression and rule.field_name:
                import re
                pattern = re.compile(rule.rule_expression)
                invalid_values = []
                
                for i, record in enumerate(data_batch):
                    value = str(record.get(rule.field_name, ""))
                    if pattern.match(value):
                        records_passed += 1
                    else:
                        records_failed += 1
                        invalid_values.append({"row": i, "value": value})
                
                if invalid_values:
                    failure_details["invalid_values"] = invalid_values[:10]  # Limit to first 10
        
        # Calculate pass rate
        pass_rate = (records_passed / records_checked * 100) if records_checked > 0 else 0
        
        # Determine status based on threshold
        error_rate = (records_failed / records_checked) if records_checked > 0 else 0
        if error_rate <= rule.error_threshold:
            status = QualityCheckResult.PASS.value
        elif error_rate <= rule.error_threshold * 2:  # Warning threshold
            status = QualityCheckResult.WARNING.value
        else:
            status = QualityCheckResult.FAIL.value
        
        return {
            "status": status,
            "records_checked": records_checked,
            "records_passed": records_passed,
            "records_failed": records_failed,
            "pass_rate": pass_rate,
            "failure_details": failure_details
        }
    
    async def _validate_rule_expression(self, expression: str, rule_type: str):
        """Validate rule expression syntax."""
        if rule_type == QualityRuleType.VALIDITY.value:
            # Validate regex pattern
            try:
                import re
                re.compile(expression)
            except re.error as e:
                raise DataQualityError(f"Invalid regex pattern: {e}")
    
    # Database helper methods (implement based on your models)
    async def _create_quality_rule_record(self, rule_data: Dict[str, Any]):
        """Create quality rule record in database."""
        # Implement database insert
        pass
    
    async def _get_active_rules_for_entity(self, entity_type: str):
        """Get active quality rules for entity type."""
        # Implement database query
        pass
    
    async def _save_quality_check_result(self, result_data: Dict[str, Any]):
        """Save quality check result to database."""
        # Implement database insert
        pass
    
    async def _get_quality_check_results(self, execution_id: int = None, entity_type: str = None):
        """Get quality check results with filters."""
        # Implement database query
        pass
    
    async def _get_quality_rules(self, entity_type: str = None, rule_type: str = None):
        """Get quality rules with filters."""
        # Implement database query
        pass
    
    async def _get_quality_rule_by_id(self, rule_id: int):
        """Get quality rule by ID."""
        # Implement database query
        pass
    
    async def _update_quality_rule_record(self, rule_id: int, rule_data: Dict[str, Any]):
        """Update quality rule record."""
        # Implement database update
        pass
    
    async def _delete_quality_rule_record(self, rule_id: int):
        """Delete quality rule record."""
        # Implement database delete
        pass
    
    async def _get_rule_recent_stats(self, rule_id: int):
        """Get recent statistics for a rule."""
        # Implement database query for recent check results
        pass