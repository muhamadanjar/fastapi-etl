"""
Transformation service for managing data transformation rules and operations.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import select, and_

from app.services.base import BaseService
from app.infrastructure.db.models.transformation.transformation_rules import TransformationRule
from app.infrastructure.db.models.transformation.field_mappings import FieldMapping
from app.infrastructure.db.models.staging.lookup_tables import LookupTable
from app.core.exceptions import TransformationError, ServiceError
from app.core.enums import TransformationType, MappingType
from app.utils.date_utils import get_current_timestamp


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
                self._validate_transformation_logic(rule_data["rule_logic"], rule_data["transformation_type"])
            
            # Create transformation rule record
            rule = TransformationRule(
                rule_name=rule_data["rule_name"],
                source_format=rule_data["source_format"],
                target_format=rule_data["target_format"],
                transformation_type=rule_data["transformation_type"],
                rule_logic=rule_data.get("rule_logic"),
                rule_parameters=rule_data.get("rule_parameters", {}),
                priority=rule_data.get("priority", 1),
                is_active=rule_data.get("is_active", True)
            )
            
            self.db_session.add(rule)
            self.db_session.commit()
            self.db_session.refresh(rule)
            
            return {
                "rule_id": rule.rule_id,
                "rule_name": rule.rule_name,
                "transformation_type": rule.transformation_type,
                "status": "created"
            }
            
        except Exception as e:
            self.db_session.rollback()
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
            mapping = FieldMapping(
                source_entity=mapping_data["source_entity"],
                source_field=mapping_data["source_field"],
                target_entity=mapping_data["target_entity"],
                target_field=mapping_data["target_field"],
                mapping_type=mapping_data.get("mapping_type", MappingType.DIRECT.value),
                mapping_expression=mapping_data.get("mapping_expression"),
                data_type=mapping_data.get("data_type"),
                is_required=mapping_data.get("is_required", False),
                default_value=mapping_data.get("default_value")
            )
            
            self.db_session.add(mapping)
            self.db_session.commit()
            self.db_session.refresh(mapping)
            
            return {
                "mapping_id": mapping.mapping_id,
                "source_field": mapping.source_field,
                "target_field": mapping.target_field,
                "mapping_type": mapping.mapping_type,
                "status": "created"
            }
            
        except Exception as e:
            self.db_session.rollback()
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
            mappings = self._get_field_mappings(source_entity, target_entity)
            
            if not mappings:
                raise TransformationError(f"No field mappings found for {source_entity} -> {target_entity}")
            
            # Get transformation rules
            transformation_rules = self._get_transformation_rules(source_entity, target_entity)
            
            transformed_records = []
            failed_records = []
            
            for i, record in enumerate(data_batch):
                try:
                    # Apply field mappings
                    transformed_record = self._apply_field_mappings(record, mappings)
                    
                    # Apply transformation rules
                    if transformation_rules:
                        transformed_record = self._apply_transformation_rules(transformed_record, transformation_rules)
                    
                    # Validate required fields
                    self._validate_required_fields(transformed_record, mappings)
                    
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
            self._validate_transformation_logic(transformation_logic, "CUSTOM")
            
            transformed_records = []
            failed_records = []
            
            for i, record in enumerate(data_batch):
                try:
                    # Apply custom transformation
                    transformed_record = self._apply_custom_logic(record, transformation_logic, parameters or {})
                    transformed_records.append(transformed_record)
                except Exception as e:
                    failed_records.append({
                        "record_index": i,
                        "error": str(e),
                        "original_data": record
                    })
            
            return {
                "total_records": len(data_batch),
                "successful_records": len(transformed_records),
                "failed_records": len(failed_records),
                "transformed_data": transformed_records,
                "transformation_log": []
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
            
            stmt = select(TransformationRule).where(TransformationRule.is_active == True)
            
            if source_format:
                stmt = stmt.where(TransformationRule.source_format == source_format)
            if target_format:
                stmt = stmt.where(TransformationRule.target_format == target_format)
            
            rules = self.db_session.execute(stmt).scalars().all()
            
            result = []
            for rule in rules:
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
                    "created_at": rule.created_at
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
            
            stmt = select(FieldMapping)
            
            if source_entity:
                stmt = stmt.where(FieldMapping.source_entity == source_entity)
            if target_entity:
                stmt = stmt.where(FieldMapping.target_entity == target_entity)
            
            mappings = self.db_session.execute(stmt).scalars().all()
            
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
            
            rule = self.db_session.get(TransformationRule, rule_id)
            if not rule:
                raise TransformationError("Transformation rule not found")
            
            # Validate transformation logic if provided
            if "rule_logic" in rule_data:
                self._validate_transformation_logic(
                    rule_data["rule_logic"],
                    rule_data.get("transformation_type", rule.transformation_type)
                )
            
            # Update fields
            for key, value in rule_data.items():
                if hasattr(rule, key):
                    setattr(rule, key, value)
            
            self.db_session.commit()
            
            return {
                "rule_id": rule.rule_id,
                "rule_name": rule.rule_name,
                "transformation_type": rule.transformation_type,
                "is_active": rule.is_active,
                "status": "updated"
            }
            
        except Exception as e:
            self.db_session.rollback()
            self.handle_error(e, "update_transformation_rule")
    
    async def delete_transformation_rule(self, rule_id: int) -> bool:
        """Delete a transformation rule."""
        try:
            self.log_operation("delete_transformation_rule", {"rule_id": rule_id})
            
            rule = self.db_session.get(TransformationRule, rule_id)
            if not rule:
                raise TransformationError("Transformation rule not found")
            
            self.db_session.delete(rule)
            self.db_session.commit()
            
            return True
            
        except Exception as e:
            self.db_session.rollback()
            self.handle_error(e, "delete_transformation_rule")
    
    async def test_transformation(self, sample_data: List[Dict[str, Any]], transformation_config: Dict[str, Any]) -> Dict[str, Any]:
        """Test transformation configuration on sample data."""
        try:
            self.log_operation("test_transformation", {"sample_size": len(sample_data)})
            
            test_result = {
                "sample_size": len(sample_data),
                "transformed_samples": [],
                "errors": [],
                "warnings": []
            }
            
            for i, record in enumerate(sample_data[:10]):  # Limit to first 10 records
                try:
                    transformed_record = record.copy()
                    
                    # Apply test mappings if provided
                    if transformation_config.get("field_mappings"):
                        transformed_record = self._apply_test_mappings(transformed_record, transformation_config["field_mappings"])
                    
                    # Apply test rules if provided
                    if transformation_config.get("transformation_rules"):
                        transformed_record = self._apply_test_rules(transformed_record, transformation_config["transformation_rules"])
                    
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
    def _get_field_mappings(self, source_entity: str, target_entity: str) -> List[FieldMapping]:
        """Get field mappings for source to target entity."""
        stmt = select(FieldMapping).where(
            and_(
                FieldMapping.source_entity == source_entity,
                FieldMapping.target_entity == target_entity
            )
        )
        return self.db_session.execute(stmt).scalars().all()
    
    def _get_transformation_rules(self, source_entity: str, target_entity: str) -> List[TransformationRule]:
        """Get transformation rules for entity transformation."""
        stmt = select(TransformationRule).where(
            and_(
                TransformationRule.source_format == source_entity,
                TransformationRule.target_format == target_entity,
                TransformationRule.is_active == True
            )
        ).order_by(TransformationRule.priority)
        return self.db_session.execute(stmt).scalars().all()
    
    def _apply_field_mappings(self, record: Dict[str, Any], mappings: List[FieldMapping]) -> Dict[str, Any]:
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
                        transformed_record[mapping.target_field] = self._evaluate_expression(
                            mapping.mapping_expression,
                            record
                        )
                    else:
                        transformed_record[mapping.target_field] = source_value
                
                elif mapping.mapping_type == MappingType.LOOKUP.value:
                    # Lookup value from lookup table
                    transformed_record[mapping.target_field] = self._lookup_value(
                        mapping.mapping_expression,
                        source_value
                    )
                
                # Apply default value if result is None and default is specified
                if transformed_record.get(mapping.target_field) is None and mapping.default_value:
                    transformed_record[mapping.target_field] = mapping.default_value
                    
            except Exception as mapping_error:
                self.logger.warning(f"Error applying mapping {mapping.source_field} -> {mapping.target_field}: {mapping_error}")
                if mapping.is_required:
                    raise TransformationError(f"Failed to map required field {mapping.target_field}")
        
        return transformed_record
    
    def _apply_transformation_rules(self, record: Dict[str, Any], rules: List[TransformationRule]) -> Dict[str, Any]:
        """Apply transformation rules to a record."""
        transformed_record = record.copy()
        
        for rule in rules:
            if rule.is_active:
                try:
                    if rule.transformation_type == TransformationType.MAPPING.value:
                        transformed_record = self._apply_mapping_transformation(transformed_record, rule)
                    elif rule.transformation_type == TransformationType.CALCULATION.value:
                        transformed_record = self._apply_calculation_transformation(transformed_record, rule)
                    elif rule.transformation_type == TransformationType.VALIDATION.value:
                        self._apply_validation_transformation(transformed_record, rule)
                    elif rule.transformation_type == TransformationType.ENRICHMENT.value:
                        transformed_record = self._apply_enrichment_transformation(transformed_record, rule)
                        
                except Exception as rule_error:
                    self.logger.error(f"Error applying transformation rule {rule.rule_name}: {rule_error}")
        
        return transformed_record
    
    def _validate_required_fields(self, record: Dict[str, Any], mappings: List[FieldMapping]):
        """Validate that all required fields are present."""
        for mapping in mappings:
            if mapping.is_required and (mapping.target_field not in record or record[mapping.target_field] is None):
                raise TransformationError(f"Required field {mapping.target_field} is missing or null")
    
    def _validate_transformation_logic(self, logic: str, transformation_type: str):
        """Validate transformation logic syntax."""
        if transformation_type == "CALCULATION":
            try:
                import ast
                ast.parse(logic, mode='eval')
            except SyntaxError as e:
                raise TransformationError(f"Invalid calculation expression: {e}")
    
    def _evaluate_expression(self, expression: str, record: Dict[str, Any]) -> Any:
        """Safely evaluate transformation expression."""
        try:
            # Simple evaluation - in production use safer evaluator
            safe_expression = expression
            for field, value in record.items():
                safe_expression = safe_expression.replace(f"${field}", str(value) if value is not None else "")
            
            # Basic evaluation with restricted builtins
            return eval(safe_expression, {"__builtins__": {}})
        except Exception as e:
            raise TransformationError(f"Error evaluating expression '{expression}': {e}")
    
    def _lookup_value(self, lookup_expression: str, source_value: Any) -> Any:
        """Lookup value from lookup table."""
        if ":" in lookup_expression:
            table_name, lookup_key = lookup_expression.split(":", 1)
            
            stmt = select(LookupTable).where(
                and_(
                    LookupTable.lookup_name == table_name,
                    LookupTable.lookup_key == str(source_value),
                    LookupTable.is_active == True
                )
            )
            lookup_record = self.db_session.execute(stmt).scalar_one_or_none()
            
            if lookup_record:
                return lookup_record.lookup_value
        
        return source_value
    
    def _apply_custom_logic(self, record: Dict[str, Any], logic: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Apply custom transformation logic."""
        # Simple implementation - extend as needed
        transformed_record = record.copy()
        
        # Apply parameters-based transformations
        for key, value in parameters.items():
            if key in transformed_record:
                if value.get("type") == "uppercase":
                    transformed_record[key] = str(transformed_record[key]).upper()
                elif value.get("type") == "lowercase":
                    transformed_record[key] = str(transformed_record[key]).lower()
                elif value.get("type") == "multiply":
                    transformed_record[key] = float(transformed_record[key]) * value.get("factor", 1)
        
        return transformed_record
    
    def _apply_test_mappings(self, record: Dict[str, Any], mappings: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply test field mappings."""
        transformed_record = {}
        
        for mapping in mappings:
            source_field = mapping.get("source_field")
            target_field = mapping.get("target_field")
            mapping_type = mapping.get("mapping_type", "DIRECT")
            
            if source_field in record:
                if mapping_type == "DIRECT":
                    transformed_record[target_field] = record[source_field]
                elif mapping_type == "CALCULATED" and mapping.get("expression"):
                    transformed_record[target_field] = self._evaluate_expression(mapping["expression"], record)
        
        return transformed_record
    
    def _apply_test_rules(self, record: Dict[str, Any], rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply test transformation rules."""
        transformed_record = record.copy()
        
        for rule in rules:
            rule_type = rule.get("type")
            if rule_type == "CALCULATION" and rule.get("logic"):
                # Apply calculation rule
                transformed_record = self._apply_custom_logic(transformed_record, rule["logic"], rule.get("parameters", {}))
        
        return transformed_record
    
    def _apply_mapping_transformation(self, record: Dict[str, Any], rule: TransformationRule) -> Dict[str, Any]:
        """Apply mapping transformation rule."""
        # Implementation for mapping transformation
        return record
    
    def _apply_calculation_transformation(self, record: Dict[str, Any], rule: TransformationRule) -> Dict[str, Any]:
        """Apply calculation transformation rule."""
        # Implementation for calculation transformation
        if rule.rule_logic:
            try:
                result = self._evaluate_expression(rule.rule_logic, record)
                # Apply result based on rule parameters
                if rule.rule_parameters and rule.rule_parameters.get("target_field"):
                    record[rule.rule_parameters["target_field"]] = result
            except Exception as e:
                self.logger.error(f"Calculation transformation failed: {e}")
        return record
    
    def _apply_validation_transformation(self, record: Dict[str, Any], rule: TransformationRule):
        """Apply validation transformation rule."""
        # Implementation for validation transformation
        if rule.rule_logic:
            try:
                is_valid = self._evaluate_expression(rule.rule_logic, record)
                if not is_valid:
                    raise TransformationError(f"Validation failed for rule: {rule.rule_name}")
            except Exception as e:
                raise TransformationError(f"Validation error: {e}")
    
    def _apply_enrichment_transformation(self, record: Dict[str, Any], rule: TransformationRule) -> Dict[str, Any]:
        """Apply enrichment transformation rule."""
        # Implementation for enrichment transformation
        if rule.rule_parameters:
            for key, value in rule.rule_parameters.items():
                if key.startswith("add_"):
                    field_name = key.replace("add_", "")
                    record[field_name] = value
        return record