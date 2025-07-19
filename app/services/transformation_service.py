"""
Transformation service for managing data transformation rules and operations.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import select, and_
from app.services.base import BaseService
from app.core.exceptions import TransformationError, ServiceError
from app.core.enums import TransformationType, MappingType
from app.utils.date_utils import get_current_timestamp
from app.transformers import get_transformer


class TransformationService(BaseService):
    """Service for managing data transformation operations."""
    
    def __init__(self, db_session: Session):
        super().__init__(db_session)
    
    def get_service_name(self) -> str:
        return "TransformationService"
    
    async def create_transformation_rule(self, rule_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new transformation rule."""
        try:
            self.validate_input(rule_data, ["rule_name", "source_format", "target_format", "transformation_type"])
            self.log_operation("create_transformation_rule", {"rule_name": rule_data["rule_name"]})
            
            # Validate transformation logic
            if "rule_logic" in rule_data:
                await self._validate_transformation_logic(rule_data["rule_logic"], rule_data["transformation_type"])
            
            # Create transformation rule record
            rule = await self._create_transformation_rule_record({
                "rule_name": rule_data["rule_name"],
                "source_format": rule_data["source_format"],
                "target_format": rule_data["target_format"],
                "transformation_type": rule_data["transformation_type"],
                "rule_logic": rule_data.get("rule_logic"),
                "rule_parameters": rule_data.get("rule_parameters", {}),
                "priority": rule_data.get("priority", 1),
                "is_active": rule_data.get("is_active", True),
                "created_at": get_current_timestamp()
            })
            
            return {
                "rule_id": rule.rule_id,
                "rule_name": rule.rule_name,
                "transformation_type": rule.transformation_type,
                "status": "created"
            }
            
        except Exception as e:
            self.handle_error(e, "create_transformation_rule")
    
    async def create_field_mapping(self, mapping_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new field mapping."""
        try:
            self.validate_input(mapping_data, ["source_entity", "source_field", "target_entity", "target_field"])
            self.log_operation("create_field_mapping", {
                "source": f"{mapping_data['source_entity']}.{mapping_data['source_field']}",
                "target": f"{mapping_data['target_entity']}.{mapping_data['target_field']}"
            })
            
            # Create field mapping record
            mapping = await self._create_field_mapping_record({
                "source_entity": mapping_data["source_entity"],
                "source_field": mapping_data["source_field"],
                "target_entity": mapping_data["target_entity"],
                "target_field": mapping_data["target_field"],
                "mapping_type": mapping_data.get("mapping_type", MappingType.DIRECT.value),
                "mapping_expression": mapping_data.get("mapping_expression"),
                "data_type": mapping_data.get("data_type"),
                "is_required": mapping_data.get("is_required", False),
                "default_value": mapping_data.get("default_value"),
                "created_at": get_current_timestamp()
            })
            
            return {
                "mapping_id": mapping.mapping_id,
                "source_field": mapping.source_field,
                "target_field": mapping.target_field,
                "mapping_type": mapping.mapping_type,
                "status": "created"
            }
            
        except Exception as e:
            self.handle_error(e, "create_field_mapping")
    
    async def transform_data_batch(self, data_batch: List[Dict[str, Any]], source_entity: str, target_entity: str) -> Dict[str, Any]:
        """Transform a batch of data using configured rules and mappings."""
        try:
            self.log_operation("transform_data_batch", {
                "source_entity": source_entity,
                "target_entity": target_entity,
                "batch_size": len(data_batch)
            })
            
            # Get field mappings for source to target
            mappings = await self._get_field_mappings(source_entity, target_entity)
            
            if not mappings:
                raise TransformationError(f"No field mappings found for {source_entity} -> {target_entity}")
            
            # Get transformation rules
            transformation_rules = await self._get_transformation_rules(source_entity, target_entity)
            
            transformed_records = []
            failed_records = []
            
            for i, record in enumerate(data_batch):
                try:
                    # Apply field mappings
                    transformed_record = await self._apply_field_mappings(record, mappings)
                    
                    # Apply transformation rules
                    if transformation_rules:
                        transformed_record = await self._apply_transformation_rules(transformed_record, transformation_rules)
                    
                    # Validate required fields
                    await self._validate_required_fields(transformed_record, mappings)
                    
                    transformed_records.append({
                        "source_row": i,
                        "transformed_data": transformed_record,
                        "transformation_metadata": {
                            "mappings_applied": len(mappings),
                            "rules_applied": len(transformation_rules),
                            "transformed_at": get_current_timestamp()
                        }
                    })
                    
                except Exception as transform_error:
                    self.logger.error(f"Error transforming record {i}: {transform_error}")
                    failed_records.append({
                        "source_row": i,
                        "source_data": record,
                        "error": str(transform_error)
                    })
            
            return {
                "total_records": len(data_batch),
                "successful_records": len(transformed_records),
                "failed_records": len(failed_records),
                "success_rate": (len(transformed_records) / len(data_batch) * 100) if data_batch else 0,
                "transformed_data": transformed_records,
                "failed_data": failed_records,
                "mappings_used": len(mappings),
                "rules_used": len(transformation_rules)
            }
            
        except Exception as e:
            self.handle_error(e, "transform_data_batch")
    
    async def apply_custom_transformation(self, data_batch: List[Dict[str, Any]], transformation_logic: str, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Apply custom transformation logic to data batch."""
        try:
            self.log_operation("apply_custom_transformation", {"batch_size": len(data_batch)})
            
            # Validate transformation logic
            await self._validate_transformation_logic(transformation_logic, "CUSTOM")
            
            # Get appropriate transformer
            transformer = get_transformer("CUSTOM")
            
            # Apply transformation
            result = await transformer.transform(
                data_batch=data_batch,
                transformation_logic=transformation_logic,
                parameters=parameters or {}
            )
            
            return {
                "total_records": len(data_batch),
                "successful_records": result.get("successful_count", 0),
                "failed_records": result.get("failed_count", 0),
                "transformed_data": result.get("transformed_data", []),
                "transformation_log": result.get("log", [])
            }
            
        except Exception as e:
            self.handle_error(e, "apply_custom_transformation")
    
    async def get_transformation_rules(self, source_format: str = None, target_format: str = None) -> List[Dict[str, Any]]:
        """Get transformation rules with optional filtering."""
        try:
            self.log_operation("get_transformation_rules", {
                "source_format": source_format,
                "target_format": target_format
            })
            
            rules = await self._get_transformation_rules_list(source_format, target_format)
            
            result = []
            for rule in rules:
                # Get usage statistics
                usage_stats = await self._get_rule_usage_stats(rule.rule_id)
                
                result.append({
                    "rule_id": rule.rule_id,
                    "rule_name": rule.rule_name,
                    "source_format": rule.source_format,
                    "target_format": rule.target_format,
                    "transformation_type": rule.transformation_type,
                    "rule_logic": rule.rule_logic,
                    "rule_parameters": rule.rule_parameters,
                    "priority": rule.priority,
                    "is_active": rule.is_active,
                    "created_at": rule.created_at,
                    "usage_stats": usage_stats
                })
            
            return result
            
        except Exception as e:
            self.handle_error(e, "get_transformation_rules")
    
    async def get_field_mappings(self, source_entity: str = None, target_entity: str = None) -> List[Dict[str, Any]]:
        """Get field mappings with optional filtering."""
        try:
            self.log_operation("get_field_mappings", {
                "source_entity": source_entity,
                "target_entity": target_entity
            })
            
            mappings = await self._get_field_mappings_list(source_entity, target_entity)
            
            return [{
                "mapping_id": mapping.mapping_id,
                "source_entity": mapping.source_entity,
                "source_field": mapping.source_field,
                "target_entity": mapping.target_entity,
                "target_field": mapping.target_field,
                "mapping_type": mapping.mapping_type,
                "mapping_expression": mapping.mapping_expression,
                "data_type": mapping.data_type,
                "is_required": mapping.is_required,
                "default_value": mapping.default_value,
                "created_at": mapping.created_at
            } for mapping in mappings]
            
        except Exception as e:
            self.handle_error(e, "get_field_mappings")
    
    async def update_transformation_rule(self, rule_id: int, rule_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing transformation rule."""
        try:
            self.log_operation("update_transformation_rule", {"rule_id": rule_id})
            
            rule = await self._get_transformation_rule_by_id(rule_id)
            if not rule:
                raise TransformationError("Transformation rule not found")
            
            # Validate transformation logic if provided
            if "rule_logic" in rule_data:
                await self._validate_transformation_logic(
                    rule_data["rule_logic"],
                    rule_data.get("transformation_type", rule.transformation_type)
                )
            
            # Update rule
            updated_rule = await self._update_transformation_rule_record(rule_id, rule_data)
            
            return {
                "rule_id": updated_rule.rule_id,
                "rule_name": updated_rule.rule_name,
                "transformation_type": updated_rule.transformation_type,
                "is_active": updated_rule.is_active,
                "status": "updated"
            }
            
        except Exception as e:
            self.handle_error(e, "update_transformation_rule")
    
    async def delete_transformation_rule(self, rule_id: int) -> bool:
        """Delete a transformation rule."""
        try:
            self.log_operation("delete_transformation_rule", {"rule_id": rule_id})
            
            rule = await self._get_transformation_rule_by_id(rule_id)
            if not rule:
                raise TransformationError("Transformation rule not found")
            
            # Delete rule
            await self._delete_transformation_rule_record(rule_id)
            
            return True
            
        except Exception as e:
            self.handle_error(e, "delete_transformation_rule")
    
    async def test_transformation(self, sample_data: List[Dict[str, Any]], transformation_config: Dict[str, Any]) -> Dict[str, Any]:
        """Test transformation configuration on sample data."""
        try:
            self.log_operation("test_transformation", {"sample_size": len(sample_data)})
            
            # Create temporary transformation context
            test_result = {
                "sample_size": len(sample_data),
                "transformed_samples": [],
                "errors": [],
                "warnings": []
            }
            
            for i, record in enumerate(sample_data[:10]):  # Limit to first 10 records
                try:
                    # Apply transformation based on config
                    if transformation_config.get("field_mappings"):
                        transformed_record = await self._apply_test_mappings(record, transformation_config["field_mappings"])
                    else:
                        transformed_record = record.copy()
                    
                    # Apply transformation rules if provided
                    if transformation_config.get("transformation_rules"):
                        transformed_record = await self._apply_test_rules(transformed_record, transformation_config["transformation_rules"])
                    
                    test_result["transformed_samples"].append({
                        "original": record,
                        "transformed": transformed_record
                    })
                    
                except Exception as test_error:
                    test_result["errors"].append({
                        "record_index": i,
                        "error": str(test_error)
                    })
            
            return test_result
            
        except Exception as e:
            self.handle_error(e, "test_transformation")
    
    # Private helper methods
    async def _apply_field_mappings(self, record: Dict[str, Any], mappings: List) -> Dict[str, Any]:
        """Apply field mappings to a single record."""
        transformed_record = {}
        
        for mapping in mappings:
            try:
                source_value = record.get(mapping.source_field)
                
                if mapping.mapping_type == MappingType.DIRECT.value:
                    # Direct mapping
                    transformed_record[mapping.target_field] = source_value
                    
                elif mapping.mapping_type == MappingType.CALCULATED.value:
                    # Apply calculation/expression
                    if mapping.mapping_expression:
                        transformed_record[mapping.target_field] = await self._evaluate_expression(
                            mapping.mapping_expression,
                            record
                        )
                    else:
                        transformed_record[mapping.target_field] = source_value
                
                elif mapping.mapping_type == MappingType.LOOKUP.value:
                    # Lookup value from lookup table
                    transformed_record[mapping.target_field] = await self._lookup_value(
                        mapping.mapping_expression,
                        source_value
                    )
                
                # Apply default value if result is None and default is specified
                if transformed_record[mapping.target_field] is None and mapping.default_value:
                    transformed_record[mapping.target_field] = mapping.default_value
                    
            except Exception as mapping_error:
                self.logger.warning(f"Error applying mapping {mapping.source_field} -> {mapping.target_field}: {mapping_error}")
                if mapping.is_required:
                    raise TransformationError(f"Failed to map required field {mapping.target_field}")
        
        return transformed_record
    
    async def _apply_transformation_rules(self, record: Dict[str, Any], rules: List) -> Dict[str, Any]:
        """Apply transformation rules to a record."""
        transformed_record = record.copy()
        
        # Sort rules by priority
        sorted_rules = sorted(rules, key=lambda r: r.priority)
        
        for rule in sorted_rules:
            if rule.is_active:
                try:
                    if rule.transformation_type == TransformationType.MAPPING.value:
                        # Apply mapping transformation
                        transformed_record = await self._apply_mapping_transformation(transformed_record, rule)
                    
                    elif rule.transformation_type == TransformationType.CALCULATION.value:
                        # Apply calculation transformation
                        transformed_record = await self._apply_calculation_transformation(transformed_record, rule)
                    
                    elif rule.transformation_type == TransformationType.VALIDATION.value:
                        # Apply validation transformation
                        await self._apply_validation_transformation(transformed_record, rule)
                    
                    elif rule.transformation_type == TransformationType.ENRICHMENT.value:
                        # Apply enrichment transformation
                        transformed_record = await self._apply_enrichment_transformation(transformed_record, rule)
                        
                except Exception as rule_error:
                    self.logger.error(f"Error applying transformation rule {rule.rule_name}: {rule_error}")
                    # Continue with other rules unless it's a critical error
        
        return transformed_record
    
    async def _validate_required_fields(self, record: Dict[str, Any], mappings: List):
        """Validate that all required fields are present."""
        for mapping in mappings:
            if mapping.is_required and (mapping.target_field not in record or record[mapping.target_field] is None):
                raise TransformationError(f"Required field {mapping.target_field} is missing or null")
    
    async def _validate_transformation_logic(self, logic: str, transformation_type: str):
        """Validate transformation logic syntax."""
        if transformation_type == "CALCULATION":
            # Validate calculation expression
            try:
                # Basic syntax validation for mathematical expressions
                import ast
                ast.parse(logic, mode='eval')
            except SyntaxError as e:
                raise TransformationError(f"Invalid calculation expression: {e}")
    
    async def _evaluate_expression(self, expression: str, record: Dict[str, Any]) -> Any:
        """Safely evaluate transformation expression."""
        # Implement safe expression evaluation
        # This is a simplified version - in production, use a safer evaluator
        try:
            # Replace field references with actual values
            safe_expression = expression
            for field, value in record.items():
                safe_expression = safe_expression.replace(f"${field}", str(value) if value is not None else "")
            
            # Evaluate simple expressions
            return eval(safe_expression, {"__builtins__": {}})
        except Exception as e:
            raise TransformationError(f"Error evaluating expression '{expression}': {e}")
    
    async def _lookup_value(self, lookup_expression: str, source_value: Any) -> Any:
        """Lookup value from lookup table."""
        # Get lookup table and key from expression
        # Format: "lookup_table_name:lookup_key"
        if ":" in lookup_expression:
            table_name, lookup_key = lookup_expression.split(":", 1)
            return await self._get_lookup_value(table_name, lookup_key, source_value)
        return source_value
    
    # Database helper methods (implement based on your models)
    async def _create_transformation_rule_record(self, rule_data: Dict[str, Any]):
        """Create transformation rule record in database."""
        # Implement database insert
        pass
    
    async def _create_field_mapping_record(self, mapping_data: Dict[str, Any]):
        """Create field mapping record in database."""
        # Implement database insert
        pass
    
    async def _get_field_mappings(self, source_entity: str, target_entity: str):
        """Get field mappings for source to target entity."""
        # Implement database query
        pass
    
    async def _get_transformation_rules(self, source_entity: str, target_entity: str):
        """Get transformation rules for entity transformation."""
        # Implement database query
        pass
    
    async def _get_transformation_rules_list(self, source_format: str = None, target_format: str = None):
        """Get transformation rules list with filters."""
        # Implement database query
        pass
    
    async def _get_field_mappings_list(self, source_entity: str = None, target_entity: str = None):
        """Get field mappings list with filters."""
        # Implement database query
        pass
    
    async def _get_transformation_rule_by_id(self, rule_id: int):
        """Get transformation rule by ID."""
        # Implement database query
        pass
    
    async def _update_transformation_rule_record(self, rule_id: int, rule_data: Dict[str, Any]):
        """Update transformation rule record."""
        # Implement database update
        pass
    
    async def _delete_transformation_rule_record(self, rule_id: int):
        """Delete transformation rule record."""
        # Implement database delete
        pass
    
    async def _get_rule_usage_stats(self, rule_id: int):
        """Get usage statistics for transformation rule."""
        # Implement database query for usage stats
        pass
    
    async def _get_lookup_value(self, table_name: str, lookup_key: str, source_value: Any):
        """Get value from lookup table."""
        # Implement lookup table query
        pass