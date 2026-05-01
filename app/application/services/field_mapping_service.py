# ==============================================
# app/application/services/field_mapping_service.py
# ==============================================
"""
Field Mapping Service

Executes field mapping transformations:
- DIRECT: Direct field copy
- CALCULATED: Expression-based calculation
- LOOKUP: Lookup table translation
- CONSTANT: Constant value assignment
"""

from typing import Dict, List, Any, Optional, Tuple
from sqlmodel import Session, select
import re
from decimal import Decimal

from app.infrastructure.db.models.transformation.field_mappings import FieldMapping
from app.utils.logger import get_logger
from app.core.exceptions import DataTransformationException

logger = get_logger(__name__)


class FieldMappingService:
    """
    Service for executing field mappings

    Supports 4 mapping types:
    1. DIRECT: target[field] = source[field]
    2. CALCULATED: target[field] = eval(expression)
    3. LOOKUP: target[field] = lookup_table[source[field]]
    4. CONSTANT: target[field] = constant_value
    """

    def __init__(self, db_session: Session, job_id: Optional[str] = None):
        """
        Initialize field mapping service

        Args:
            db_session: Database session
            job_id: Optional job ID for filtering mappings
        """
        self.db_session = db_session
        self.job_id = job_id
        self.logger = logger

        # Cache for lookup tables loaded from database
        self._lookup_cache = {}

    async def execute_mappings(
        self,
        source_record: Dict[str, Any],
        field_mappings: List[FieldMapping],
        execution_id: Optional[str] = None
    ) -> Tuple[Dict[str, Any], List[str]]:
        """
        Execute field mappings on a source record

        Args:
            source_record: Source data record with original fields
            field_mappings: List of FieldMapping objects defining transformations
            execution_id: Optional execution ID for logging

        Returns:
            Tuple of (target_record, errors)
            - target_record: Transformed record with mapped fields
            - errors: List of mapping errors (empty if successful)
        """
        target_record = {}
        errors = []

        self.logger.debug(
            f"[{execution_id}] Starting field mapping execution with {len(field_mappings)} mappings"
        )

        # Sort mappings by priority (lower priority first)
        sorted_mappings = sorted(field_mappings, key=lambda m: m.priority if hasattr(m, 'priority') else 1)

        for mapping in sorted_mappings:
            try:
                # Map source field to target field based on mapping type
                mapping_type = mapping.mapping_type.upper() if mapping.mapping_type else "DIRECT"

                if mapping_type == "DIRECT":
                    target_value = await self._execute_direct_mapping(source_record, mapping)

                elif mapping_type == "CALCULATED":
                    target_value = await self._execute_calculated_mapping(source_record, mapping, execution_id)

                elif mapping_type == "LOOKUP":
                    target_value = await self._execute_lookup_mapping(source_record, mapping, execution_id)

                elif mapping_type == "CONSTANT":
                    target_value = await self._execute_constant_mapping(mapping)

                else:
                    errors.append(
                        f"Unknown mapping type '{mapping_type}' for field '{mapping.target_field}'"
                    )
                    continue

                # Assign to target record if value is not None or explicitly set
                if target_value is not None or "allow_null" in mapping.mapping_expression or "":
                    target_record[mapping.target_field] = target_value

                self.logger.debug(
                    f"[{execution_id}] Mapped {mapping.source_field} -> {mapping.target_field} "
                    f"(type: {mapping_type}, value: {target_value})"
                )

            except Exception as e:
                error_msg = (
                    f"Field mapping failed for '{mapping.target_field}' "
                    f"(source: {mapping.source_field}, type: {mapping.mapping_type}): {str(e)}"
                )
                errors.append(error_msg)
                self.logger.error(f"[{execution_id}] {error_msg}")

                # Decide whether to continue or fail on error
                if mapping.is_required if hasattr(mapping, 'is_required') else True:
                    # For required fields, fail the entire operation
                    raise DataTransformationException(error_msg) from e

        if errors:
            self.logger.warning(
                f"[{execution_id}] Field mapping completed with {len(errors)} error(s)"
            )
        else:
            self.logger.debug(f"[{execution_id}] Field mapping completed successfully")

        return target_record, errors

    async def _execute_direct_mapping(
        self, source_record: Dict[str, Any], mapping: FieldMapping
    ) -> Any:
        """
        Execute DIRECT mapping: direct copy of field value

        Args:
            source_record: Source data record
            mapping: FieldMapping with DIRECT type

        Returns:
            Mapped value
        """
        source_field = mapping.source_field
        default_value = mapping.default_value

        if source_field not in source_record:
            if default_value is not None:
                return self._convert_to_type(default_value, mapping.data_type)
            elif mapping.is_required:
                raise DataTransformationException(
                    f"Required source field '{source_field}' not found in record"
                )
            return None

        value = source_record[source_field]

        # Convert to target data type if specified
        if mapping.data_type:
            value = self._convert_to_type(value, mapping.data_type)

        return value

    async def _execute_calculated_mapping(
        self, source_record: Dict[str, Any], mapping: FieldMapping, execution_id: Optional[str]
    ) -> Any:
        """
        Execute CALCULATED mapping: expression-based calculation

        Args:
            source_record: Source data record
            mapping: FieldMapping with CALCULATED type
            execution_id: Execution ID for logging

        Returns:
            Calculated value
        """
        expression = mapping.mapping_expression

        if not expression:
            raise DataTransformationException(
                f"CALCULATED mapping for '{mapping.target_field}' missing expression"
            )

        try:
            # Build evaluation context from source record
            eval_context = source_record.copy()

            # Add common functions
            eval_context["len"] = len
            eval_context["str"] = str
            eval_context["int"] = int
            eval_context["float"] = float
            eval_context["bool"] = bool
            eval_context["upper"] = lambda x: str(x).upper() if x else ""
            eval_context["lower"] = lambda x: str(x).lower() if x else ""
            eval_context["title"] = lambda x: str(x).title() if x else ""
            eval_context["concat"] = lambda *args: "".join(str(a) for a in args if a)
            eval_context["replace"] = lambda s, o, n: s.replace(o, n) if isinstance(s, str) else s

            # Evaluate expression
            # WARNING: eval() is dangerous in production - consider using safer alternatives
            result = eval(expression, {"__builtins__": {}}, eval_context)

            # Convert to target data type if specified
            if mapping.data_type:
                result = self._convert_to_type(result, mapping.data_type)

            self.logger.debug(
                f"[{execution_id}] CALCULATED: {expression} = {result}"
            )

            return result

        except Exception as e:
            raise DataTransformationException(
                f"Failed to evaluate calculated expression '{expression}': {str(e)}"
            ) from e

    async def _execute_lookup_mapping(
        self, source_record: Dict[str, Any], mapping: FieldMapping, execution_id: Optional[str]
    ) -> Any:
        """
        Execute LOOKUP mapping: lookup table translation

        Args:
            source_record: Source data record
            mapping: FieldMapping with LOOKUP type
            execution_id: Execution ID for logging

        Returns:
            Looked-up value
        """
        source_field = mapping.source_field

        if source_field not in source_record:
            if mapping.default_value is not None:
                return self._convert_to_type(mapping.default_value, mapping.data_type)
            return None

        source_value = source_record[source_field]

        try:
            # Parse lookup configuration from mapping_expression
            # Format: "table_name:key_field:return_field" or "table_name" (uses id as key, returns code)
            lookup_config = mapping.mapping_expression or ""
            parts = lookup_config.split(":")

            if len(parts) < 1:
                raise DataTransformationException("Invalid lookup configuration format")

            table_name = parts[0].strip()
            key_field = parts[1].strip() if len(parts) > 1 else "id"
            return_field = parts[2].strip() if len(parts) > 2 else "code"

            # Load lookup table data
            lookup_table = await self._load_lookup_table(table_name, key_field, return_field)

            # Normalize lookup key (case-insensitive for strings)
            lookup_key = str(source_value).lower() if isinstance(source_value, str) else source_value

            # Perform lookup
            if lookup_key in lookup_table:
                result = lookup_table[lookup_key]
            else:
                # Use default value if lookup fails
                result = mapping.default_value

            self.logger.debug(
                f"[{execution_id}] LOOKUP: {table_name}[{lookup_key}] = {result}"
            )

            if result and mapping.data_type:
                result = self._convert_to_type(result, mapping.data_type)

            return result

        except Exception as e:
            if mapping.is_required:
                raise DataTransformationException(f"Lookup mapping failed: {str(e)}") from e
            else:
                self.logger.warning(f"Lookup mapping failed for '{mapping.target_field}': {str(e)}")
                return mapping.default_value

    async def _execute_constant_mapping(self, mapping: FieldMapping) -> Any:
        """
        Execute CONSTANT mapping: assign constant value

        Args:
            mapping: FieldMapping with CONSTANT type

        Returns:
            Constant value
        """
        constant_value = mapping.mapping_expression or mapping.default_value

        if constant_value is None:
            raise DataTransformationException(
                f"CONSTANT mapping for '{mapping.target_field}' missing constant value"
            )

        # Convert to target data type if specified
        if mapping.data_type:
            constant_value = self._convert_to_type(constant_value, mapping.data_type)

        return constant_value

    async def _load_lookup_table(self, table_name: str, key_field: str, return_field: str) -> Dict[str, Any]:
        """
        Load lookup table data from database cache

        Args:
            table_name: Name of the lookup table
            key_field: Field to use as lookup key
            return_field: Field to return as value

        Returns:
            Dictionary mapping keys to values
        """
        cache_key = f"{table_name}:{key_field}:{return_field}"

        # Return cached result if available
        if cache_key in self._lookup_cache:
            return self._lookup_cache[cache_key]

        try:
            # Load lookup table data from database
            # This is a simplified implementation - adapt based on your lookup table structure
            lookup_data = {}

            # For now, return empty dict - implement based on your lookup table schema
            # Example: query from a lookup_values table
            self.logger.warning(
                f"Lookup table '{table_name}' not found in cache - implement database loading"
            )

            # Cache the result
            self._lookup_cache[cache_key] = lookup_data

            return lookup_data

        except Exception as e:
            self.logger.error(f"Failed to load lookup table '{table_name}': {str(e)}")
            return {}

    def _convert_to_type(self, value: Any, target_type: str) -> Any:
        """
        Convert value to target data type

        Args:
            value: Value to convert
            target_type: Target data type

        Returns:
            Converted value
        """
        if value is None:
            return None

        target_type = target_type.lower() if target_type else "string"

        try:
            if target_type in ("string", "str", "text"):
                return str(value)

            elif target_type in ("integer", "int"):
                return int(float(str(value))) if value else 0

            elif target_type in ("float", "double", "decimal"):
                return float(str(value)) if value else 0.0

            elif target_type in ("boolean", "bool"):
                if isinstance(value, bool):
                    return value
                return str(value).lower() in ("true", "1", "yes", "on")

            elif target_type in ("date", "datetime", "timestamp"):
                # Return as-is for now, implement date parsing if needed
                return value

            else:
                return value

        except (ValueError, TypeError) as e:
            self.logger.warning(
                f"Failed to convert value '{value}' to type '{target_type}': {str(e)}"
            )
            return value
