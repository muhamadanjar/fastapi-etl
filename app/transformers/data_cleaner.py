# ==============================================
# app/transformers/data_cleaner.py
# ==============================================
"""
Data Cleansing Transformer

Handles fundamental data cleaning operations:
- Whitespace removal/normalization
- Case normalization
- Null/empty value handling
- Type coercion for common types
"""

from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

from .base_transformer import BaseTransformer, TransformationResult, TransformationStatus
from app.utils.logger import get_logger

logger = get_logger(__name__)


class DataCleaner(BaseTransformer):
    """
    Data cleaning transformer that handles:
    - Whitespace normalization (trim, collapse multiple spaces)
    - Case normalization (uppercase, lowercase, title case)
    - Null/empty value handling (replace with defaults)
    - Common data type corrections
    - Field-level cleaning rules
    """

    def __init__(self, db_session, job_execution_id: Optional[str] = None, **kwargs):
        """
        Initialize data cleaner

        Args:
            db_session: Database session
            job_execution_id: Job execution ID for tracking
            **kwargs: Additional configuration
        """
        super().__init__(db_session, job_execution_id, **kwargs)

        # Cleaning strategies
        self.remove_leading_trailing_spaces = kwargs.get("remove_leading_trailing_spaces", True)
        self.collapse_multiple_spaces = kwargs.get("collapse_multiple_spaces", True)
        self.handle_empty_strings = kwargs.get("handle_empty_strings", True)
        self.empty_string_replacement = kwargs.get("empty_string_replacement", None)
        self.handle_null_values = kwargs.get("handle_null_values", True)
        self.null_replacement = kwargs.get("null_replacement", None)
        self.case_normalization = kwargs.get("case_normalization", {})  # field -> 'lower'|'upper'|'title'
        self.remove_special_characters = kwargs.get("remove_special_characters", False)
        self.special_chars_to_remove = kwargs.get("special_chars_to_remove", "")
        self.convert_types = kwargs.get("convert_types", True)

        # Field-specific cleaning rules
        self.field_cleaning_rules = kwargs.get("field_cleaning_rules", {})

    async def validate_config(self) -> Tuple[bool, List[str]]:
        """Validate cleaner configuration"""
        errors = []

        # Validate case normalization values
        valid_cases = {"lower", "upper", "title"}
        for field_name, case_type in self.case_normalization.items():
            if case_type not in valid_cases:
                errors.append(
                    f"Invalid case normalization type '{case_type}' for field '{field_name}'. "
                    f"Must be one of: {valid_cases}"
                )

        # Validate field cleaning rules
        for field_name, rules in self.field_cleaning_rules.items():
            if not isinstance(rules, dict):
                errors.append(f"Field cleaning rules for '{field_name}' must be a dictionary")

        return len(errors) == 0, errors

    async def transform_record(self, record: Dict[str, Any]) -> TransformationResult:
        """
        Clean a single record

        Args:
            record: Input record to clean

        Returns:
            TransformationResult with cleaned data
        """
        try:
            cleaned_record = {}
            warnings = []
            metadata = {
                "fields_cleaned": [],
                "null_values_handled": [],
                "empty_values_handled": [],
                "case_normalized": [],
                "special_chars_removed": [],
                "type_conversions": [],
            }

            # Process each field in the record
            for field_name, field_value in record.items():
                try:
                    cleaned_value = await self._clean_field(
                        field_name, field_value, metadata, warnings
                    )
                    cleaned_record[field_name] = cleaned_value
                except Exception as e:
                    self.logger.warning(f"Failed to clean field '{field_name}': {str(e)}")
                    warnings.append(f"Cleaning failed for field '{field_name}': {str(e)}")
                    # Keep original value if cleaning fails
                    cleaned_record[field_name] = field_value

            # Calculate quality score
            quality_score = self._calculate_cleaning_quality_score(record, cleaned_record, metadata)
            metadata["quality_score"] = quality_score

            return TransformationResult(
                status=TransformationStatus.SUCCESS,
                data=cleaned_record,
                warnings=warnings,
                metadata=metadata,
            )

        except Exception as e:
            self.logger.error(f"Error cleaning record: {str(e)}")
            return TransformationResult(
                status=TransformationStatus.FAILED,
                errors=[f"Data cleaning failed: {str(e)}"],
            )

    async def _clean_field(
        self, field_name: str, value: Any, metadata: Dict[str, Any], warnings: List[str]
    ) -> Any:
        """
        Clean a single field value

        Args:
            field_name: Name of the field
            value: Original value
            metadata: Metadata to track cleaning operations
            warnings: List to accumulate warnings

        Returns:
            Cleaned value
        """
        # Check field-specific cleaning rules first
        if field_name in self.field_cleaning_rules:
            rule = self.field_cleaning_rules[field_name]
            return await self._apply_field_cleaning_rule(field_name, value, rule, metadata)

        # Handle None/null values
        if value is None:
            if self.handle_null_values:
                metadata["null_values_handled"].append(field_name)
                return self.null_replacement
            return value

        # Handle empty strings
        if isinstance(value, str):
            if not value and self.handle_empty_strings:
                metadata["empty_values_handled"].append(field_name)
                return self.empty_string_replacement

            # Remove leading/trailing whitespace
            if self.remove_leading_trailing_spaces:
                original = value
                value = value.strip()
                if value != original:
                    metadata["fields_cleaned"].append(field_name)

            # Collapse multiple spaces
            if self.collapse_multiple_spaces:
                original = value
                value = " ".join(value.split())
                if value != original:
                    metadata["fields_cleaned"].append(field_name)

            # Remove special characters if configured
            if self.remove_special_characters and self.special_chars_to_remove:
                original = value
                for char in self.special_chars_to_remove:
                    value = value.replace(char, "")
                if value != original:
                    metadata["special_chars_removed"].append(field_name)

            # Apply case normalization
            if field_name in self.case_normalization:
                original = value
                case_type = self.case_normalization[field_name]
                if case_type == "lower":
                    value = value.lower()
                elif case_type == "upper":
                    value = value.upper()
                elif case_type == "title":
                    value = value.title()

                if value != original:
                    metadata["case_normalized"].append(field_name)

        return value

    async def _apply_field_cleaning_rule(
        self, field_name: str, value: Any, rule: Dict[str, Any], metadata: Dict[str, Any]
    ) -> Any:
        """
        Apply field-specific cleaning rule

        Args:
            field_name: Name of the field
            value: Original value
            rule: Cleaning rule configuration
            metadata: Metadata to track operations

        Returns:
            Cleaned value
        """
        # Get rule configuration
        rule_type = rule.get("type", "general")

        if rule_type == "trim":
            if isinstance(value, str):
                original = value
                value = value.strip()
                if value != original:
                    metadata["fields_cleaned"].append(field_name)

        elif rule_type == "lowercase":
            if isinstance(value, str):
                original = value
                value = value.lower()
                if value != original:
                    metadata["case_normalized"].append(field_name)

        elif rule_type == "uppercase":
            if isinstance(value, str):
                original = value
                value = value.upper()
                if value != original:
                    metadata["case_normalized"].append(field_name)

        elif rule_type == "titlecase":
            if isinstance(value, str):
                original = value
                value = value.title()
                if value != original:
                    metadata["case_normalized"].append(field_name)

        elif rule_type == "remove_chars":
            if isinstance(value, str):
                chars_to_remove = rule.get("chars", "")
                original = value
                for char in chars_to_remove:
                    value = value.replace(char, "")
                if value != original:
                    metadata["special_chars_removed"].append(field_name)

        elif rule_type == "replace":
            if isinstance(value, str):
                original = value
                search = rule.get("search", "")
                replacement = rule.get("replacement", "")
                value = value.replace(search, replacement)
                if value != original:
                    metadata["fields_cleaned"].append(field_name)

        elif rule_type == "default_if_empty":
            default_value = rule.get("default")
            if not value or (isinstance(value, str) and not value.strip()):
                metadata["empty_values_handled"].append(field_name)
                value = default_value

        elif rule_type == "default_if_null":
            default_value = rule.get("default")
            if value is None:
                metadata["null_values_handled"].append(field_name)
                value = default_value

        return value

    def _calculate_cleaning_quality_score(
        self, original_record: Dict[str, Any], cleaned_record: Dict[str, Any], metadata: Dict[str, Any]
    ) -> float:
        """
        Calculate quality score based on cleaning operations

        Args:
            original_record: Original record before cleaning
            cleaned_record: Record after cleaning
            metadata: Metadata with cleaning statistics

        Returns:
            Quality score between 0.0 and 1.0
        """
        # Base score starts at 1.0
        score = 1.0

        # Check if data was lost or changed significantly
        null_handling = len(metadata.get("null_values_handled", []))
        empty_handling = len(metadata.get("empty_values_handled", []))
        fields_changed = len(metadata.get("fields_cleaned", []))

        total_fields = len(original_record)
        if total_fields == 0:
            return 1.0

        # Penalize for null/empty replacements
        if null_handling > 0:
            score -= 0.05 * min(null_handling, total_fields)

        if empty_handling > 0:
            score -= 0.02 * min(empty_handling, total_fields)

        # Bonus for successful cleaning
        if fields_changed > 0:
            score += 0.01 * min(fields_changed, total_fields)

        # Ensure score stays between 0.0 and 1.0
        return max(0.0, min(1.0, score))
