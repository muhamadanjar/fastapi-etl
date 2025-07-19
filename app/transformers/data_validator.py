# ==============================================
# app/transformers/data_validator.py
# ==============================================
import re
from typing import Dict, List, Any, Optional, Tuple, Union, Callable
from datetime import datetime, date
from dataclasses import dataclass
from enum import Enum
import json
from decimal import Decimal, InvalidOperation

from .base_transformer import BaseTransformer, TransformationResult, TransformationStatus
from app.utils.logger import get_logger

logger = get_logger(__name__)

class ValidationSeverity(Enum):
    """Validation severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class ValidationType(Enum):
    """Types of validations"""
    REQUIRED = "required"
    TYPE = "type"
    FORMAT = "format"
    RANGE = "range"
    LENGTH = "length"
    PATTERN = "pattern"
    CUSTOM = "custom"
    BUSINESS_RULE = "business_rule"
    REFERENTIAL = "referential"
    UNIQUENESS = "uniqueness"

@dataclass
class ValidationRule:
    """Configuration for a validation rule"""
    field_name: str
    validation_type: ValidationType
    severity: ValidationSeverity
    parameters: Dict[str, Any]
    error_message: str
    is_enabled: bool = True
    priority: int = 1

@dataclass
class ValidationResult:
    """Result of a validation check"""
    rule: ValidationRule
    is_valid: bool
    error_message: Optional[str] = None
    warning_message: Optional[str] = None
    validated_value: Any = None
    metadata: Dict[str, Any] = None

class DataValidator(BaseTransformer):
    """
    Data validation transformer that handles:
    - Required field validation
    - Data type validation
    - Format validation (email, phone, date, etc.)
    - Range and length validation
    - Pattern matching (regex)
    - Custom validation functions
    - Business rule validation
    - Referential integrity checks
    - Uniqueness validation
    - Cross-field validation
    """
    
    def __init__(self, db_session, job_execution_id: Optional[str] = None, **kwargs):
        """
        Initialize data validator
        
        Args:
            db_session: Database session
            job_execution_id: Job execution ID for tracking
            **kwargs: Additional configuration
        """
        super().__init__(db_session, job_execution_id, **kwargs)
        
        # Validation configuration
        self.validation_rules = self._parse_validation_rules(kwargs.get('validation_rules', {}))
        self.stop_on_first_error = kwargs.get('stop_on_first_error', False)
        self.validate_cross_fields = kwargs.get('validate_cross_fields', True)
        self.collect_all_errors = kwargs.get('collect_all_errors', True)
        self.auto_fix_errors = kwargs.get('auto_fix_errors', False)
        
        # Error handling
        self.max_errors_per_record = kwargs.get('max_errors_per_record', 50)
        self.fail_on_critical_errors = kwargs.get('fail_on_critical_errors', True)
        self.fail_on_error_threshold = kwargs.get('fail_on_error_threshold', 0.1)  # 10%
        
        # Custom validation functions
        self.custom_validators = kwargs.get('custom_validators', {})
        
        # Business rules
        self.business_rules = kwargs.get('business_rules', {})
        
        # Reference data for referential validation
        self.reference_data = kwargs.get('reference_data', {})
        
        # Uniqueness tracking
        self.uniqueness_cache = {}
        
        # Built-in format validators
        self.format_validators = self._initialize_format_validators()
        
        # Cross-field validation rules
        self.cross_field_rules = kwargs.get('cross_field_rules', {})
    
    def _parse_validation_rules(self, rules_config: Dict) -> List[ValidationRule]:
        """Parse validation rules from configuration"""
        rules = []
        
        for field_name, field_rules in rules_config.items():
            if not isinstance(field_rules, list):
                field_rules = [field_rules]
            
            for rule_config in field_rules:
                rule = ValidationRule(
                    field_name=field_name,
                    validation_type=ValidationType(rule_config.get('type', 'required')),
                    severity=ValidationSeverity(rule_config.get('severity', 'error')),
                    parameters=rule_config.get('parameters', {}),
                    error_message=rule_config.get('error_message', ''),
                    is_enabled=rule_config.get('enabled', True),
                    priority=rule_config.get('priority', 1)
                )
                rules.append(rule)
        
        # Sort by priority
        rules.sort(key=lambda x: x.priority)
        return rules
    
    def _initialize_format_validators(self) -> Dict[str, Callable]:
        """Initialize built-in format validators"""
        return {
            'email': self._validate_email_format,
            'phone': self._validate_phone_format,
            'date': self._validate_date_format,
            'datetime': self._validate_datetime_format,
            'url': self._validate_url_format,
            'ipv4': self._validate_ipv4_format,
            'ipv6': self._validate_ipv6_format,
            'uuid': self._validate_uuid_format,
            'credit_card': self._validate_credit_card_format,
            'iban': self._validate_iban_format,
            'postal_code': self._validate_postal_code_format,
            'social_security': self._validate_social_security_format,
            'passport': self._validate_passport_format,
            'license_plate': self._validate_license_plate_format,
        }
    
    async def validate_config(self) -> Tuple[bool, List[str]]:
        """Validate validator configuration"""
        errors = []
        
        # Validate validation rules
        for rule in self.validation_rules:
            # Check if custom validator exists
            if rule.validation_type == ValidationType.CUSTOM:
                validator_name = rule.parameters.get('validator')
                if validator_name and validator_name not in self.custom_validators:
                    errors.append(f"Custom validator '{validator_name}' not found for field '{rule.field_name}'")
            
            # Check if format validator exists
            elif rule.validation_type == ValidationType.FORMAT:
                format_type = rule.parameters.get('format')
                if format_type and format_type not in self.format_validators:
                    errors.append(f"Format validator '{format_type}' not found for field '{rule.field_name}'")
            
            # Check range parameters
            elif rule.validation_type == ValidationType.RANGE:
                if 'min' not in rule.parameters and 'max' not in rule.parameters:
                    errors.append(f"Range validation for field '{rule.field_name}' requires 'min' or 'max' parameter")
        
        # Validate cross-field rules
        for rule_name, rule_config in self.cross_field_rules.items():
            if 'fields' not in rule_config:
                errors.append(f"Cross-field rule '{rule_name}' missing 'fields' parameter")
            if 'condition' not in rule_config:
                errors.append(f"Cross-field rule '{rule_name}' missing 'condition' parameter")
        
        return len(errors) == 0, errors
    
    async def transform_record(self, record: Dict[str, Any]) -> TransformationResult:
        """
        Validate a single record
        
        Args:
            record: Input record to validate
            
        Returns:
            TransformationResult with validation results
        """
        try:
            validated_record = record.copy()
            validation_results = []
            errors = []
            warnings = []
            
            metadata = {
                'validation_rules_applied': [],
                'fields_validated': [],
                'auto_fixes_applied': [],
                'validation_summary': {
                    'total_rules': 0,
                    'passed_rules': 0,
                    'failed_rules': 0,
                    'warning_rules': 0
                }
            }
            
            # Apply field-level validation rules
            for rule in self.validation_rules:
                if not rule.is_enabled:
                    continue
                
                field_value = validated_record.get(rule.field_name)
                validation_result = await self._apply_validation_rule(field_value, rule, validated_record)
                
                validation_results.append(validation_result)
                metadata['validation_rules_applied'].append(f"{rule.field_name}:{rule.validation_type.value}")
                metadata['validation_summary']['total_rules'] += 1
                
                if validation_result.is_valid:
                    metadata['validation_summary']['passed_rules'] += 1
                    
                    # Update field value if auto-fix was applied
                    if validation_result.validated_value is not None:
                        validated_record[rule.field_name] = validation_result.validated_value
                        metadata['auto_fixes_applied'].append(rule.field_name)
                else:
                    if rule.severity == ValidationSeverity.CRITICAL or rule.severity == ValidationSeverity.ERROR:
                        metadata['validation_summary']['failed_rules'] += 1
                        error_msg = validation_result.error_message or rule.error_message or f"Validation failed for field '{rule.field_name}'"
                        errors.append(error_msg)
                        
                        # Stop on first error if configured
                        if self.stop_on_first_error:
                            break
                    else:
                        metadata['validation_summary']['warning_rules'] += 1
                        warning_msg = validation_result.warning_message or f"Validation warning for field '{rule.field_name}'"
                        warnings.append(warning_msg)
                
                if rule.field_name not in metadata['fields_validated']:
                    metadata['fields_validated'].append(rule.field_name)
                
                # Check max errors per record
                if len(errors) >= self.max_errors_per_record:
                    errors.append(f"Maximum error limit ({self.max_errors_per_record}) reached for record")
                    break
            
            # Apply cross-field validation if enabled
            if self.validate_cross_fields and not errors:
                cross_field_errors = await self._validate_cross_fields(validated_record)
                errors.extend(cross_field_errors)
            
            # Apply business rule validation
            if self.business_rules and not errors:
                business_rule_errors = await self._validate_business_rules(validated_record)
                errors.extend(business_rule_errors)
            
            # Calculate quality score
            quality_score = self._calculate_validation_quality_score(validation_results, metadata)
            metadata['quality_score'] = quality_score
            
            # Determine final status
            if errors:
                # Check if we should fail on critical errors
                critical_errors = [r for r in validation_results if not r.is_valid and r.rule.severity == ValidationSeverity.CRITICAL]
                if critical_errors and self.fail_on_critical_errors:
                    return TransformationResult(
                        status=TransformationStatus.FAILED,
                        errors=errors,
                        warnings=warnings,
                        metadata=metadata
                    )
                else:
                    return TransformationResult(
                        status=TransformationStatus.WARNING,
                        data=validated_record,
                        errors=errors,
                        warnings=warnings,
                        metadata=metadata
                    )
            else:
                return TransformationResult(
                    status=TransformationStatus.SUCCESS,
                    data=validated_record,
                    warnings=warnings,
                    metadata=metadata
                )
                
        except Exception as e:
            self.logger.error(f"Error validating record: {str(e)}")
            return TransformationResult(
                status=TransformationStatus.FAILED,
                errors=[f"Data validation failed: {str(e)}"]
            )
    
    async def _apply_validation_rule(self, value: Any, rule: ValidationRule, record: Dict[str, Any]) -> ValidationResult:
        """Apply a single validation rule"""
        try:
            if rule.validation_type == ValidationType.REQUIRED:
                return await self._validate_required(value, rule)
            
            elif rule.validation_type == ValidationType.TYPE:
                return await self._validate_type(value, rule)
            
            elif rule.validation_type == ValidationType.FORMAT:
                return await self._validate_format(value, rule)
            
            elif rule.validation_type == ValidationType.RANGE:
                return await self._validate_range(value, rule)
            
            elif rule.validation_type == ValidationType.LENGTH:
                return await self._validate_length(value, rule)
            
            elif rule.validation_type == ValidationType.PATTERN:
                return await self._validate_pattern(value, rule)
            
            elif rule.validation_type == ValidationType.CUSTOM:
                return await self._validate_custom(value, rule, record)
            
            elif rule.validation_type == ValidationType.REFERENTIAL:
                return await self._validate_referential(value, rule)
            
            elif rule.validation_type == ValidationType.UNIQUENESS:
                return await self._validate_uniqueness(value, rule)
            
            else:
                return ValidationResult(
                    rule=rule,
                    is_valid=False,
                    error_message=f"Unknown validation type: {rule.validation_type}"
                )
                
        except Exception as e:
            self.logger.error(f"Error applying validation rule: {str(e)}")
            return ValidationResult(
                rule=rule,
                is_valid=False,
                error_message=f"Validation rule execution failed: {str(e)}"
            )
    
    async def _validate_required(self, value: Any, rule: ValidationRule) -> ValidationResult:
        """Validate required field"""
        is_valid = value is not None and value != "" and value != []
        
        if not is_valid and self.auto_fix_errors:
            default_value = rule.parameters.get('default')
            if default_value is not None:
                return ValidationResult(
                    rule=rule,
                    is_valid=True,
                    validated_value=default_value,
                    metadata={'auto_fix': 'default_value_applied'}
                )
        
        return ValidationResult(
            rule=rule,
            is_valid=is_valid,
            error_message=f"Field '{rule.field_name}' is required but missing or empty" if not is_valid else None
        )
    
    async def _validate_type(self, value: Any, rule: ValidationRule) -> ValidationResult:
        """Validate data type"""
        if value is None:
            return ValidationResult(rule=rule, is_valid=True)
        
        expected_type = rule.parameters.get('type')
        type_mapping = {
            'string': str,
            'int': int,
            'float': float,
            'bool': bool,
            'list': list,
            'dict': dict,
            'datetime': (datetime, date),
            'decimal': Decimal
        }
        
        expected_python_type = type_mapping.get(expected_type)
        if not expected_python_type:
            return ValidationResult(
                rule=rule,
                is_valid=False,
                error_message=f"Unknown type: {expected_type}"
            )
        
        is_valid = isinstance(value, expected_python_type)
        
        # Try auto-conversion if enabled
        if not is_valid and self.auto_fix_errors:
            try:
                if expected_type == 'int':
                    converted_value = int(float(str(value)))
                elif expected_type == 'float':
                    converted_value = float(str(value))
                elif expected_type == 'bool':
                    converted_value = str(value).lower() in ('true', '1', 'yes', 'on')
                elif expected_type == 'string':
                    converted_value = str(value)
                elif expected_type == 'decimal':
                    converted_value = Decimal(str(value))
                else:
                    converted_value = None
                
                if converted_value is not None:
                    return ValidationResult(
                        rule=rule,
                        is_valid=True,
                        validated_value=converted_value,
                        metadata={'auto_fix': 'type_conversion_applied'}
                    )
            except (ValueError, InvalidOperation):
                pass
        
        return ValidationResult(
            rule=rule,
            is_valid=is_valid,
            error_message=f"Field '{rule.field_name}' expected type {expected_type}, got {type(value).__name__}" if not is_valid else None
        )
    
    async def _validate_format(self, value: Any, rule: ValidationRule) -> ValidationResult:
        """Validate format using built-in validators"""
        if value is None or value == "":
            return ValidationResult(rule=rule, is_valid=True)
        
        format_type = rule.parameters.get('format')
        validator = self.format_validators.get(format_type)
        
        if not validator:
            return ValidationResult(
                rule=rule,
                is_valid=False,
                error_message=f"Unknown format type: {format_type}"
            )
        
        is_valid, fixed_value = await validator(str(value), rule.parameters)
        
        if not is_valid and self.auto_fix_errors and fixed_value is not None:
            return ValidationResult(
                rule=rule,
                is_valid=True,
                validated_value=fixed_value,
                metadata={'auto_fix': 'format_correction_applied'}
            )
        
        return ValidationResult(
            rule=rule,
            is_valid=is_valid,
            error_message=f"Field '{rule.field_name}' has invalid {format_type} format" if not is_valid else None
        )
    
    async def _validate_range(self, value: Any, rule: ValidationRule) -> ValidationResult:
        """Validate numeric range"""
        if value is None:
            return ValidationResult(rule=rule, is_valid=True)
        
        try:
            numeric_value = float(value)
            min_val = rule.parameters.get('min')
            max_val = rule.parameters.get('max')
            
            is_valid = True
            error_parts = []
            
            if min_val is not None and numeric_value < min_val:
                is_valid = False
                error_parts.append(f"minimum {min_val}")
            
            if max_val is not None and numeric_value > max_val:
                is_valid = False
                error_parts.append(f"maximum {max_val}")
            
            # Auto-fix by clamping to range
            if not is_valid and self.auto_fix_errors:
                clamp_mode = rule.parameters.get('clamp', False)
                if clamp_mode:
                    clamped_value = numeric_value
                    if min_val is not None and numeric_value < min_val:
                        clamped_value = min_val
                    if max_val is not None and numeric_value > max_val:
                        clamped_value = max_val
                    
                    return ValidationResult(
                        rule=rule,
                        is_valid=True,
                        validated_value=clamped_value,
                        metadata={'auto_fix': 'range_clamping_applied'}
                    )
            
            error_message = f"Field '{rule.field_name}' must be between {' and '.join(error_parts)}" if not is_valid else None
            
            return ValidationResult(
                rule=rule,
                is_valid=is_valid,
                error_message=error_message
            )
            
        except (ValueError, TypeError):
            return ValidationResult(
                rule=rule,
                is_valid=False,
                error_message=f"Field '{rule.field_name}' must be numeric for range validation"
            )
    
    async def _validate_length(self, value: Any, rule: ValidationRule) -> ValidationResult:
        """Validate string/list length"""
        if value is None:
            return ValidationResult(rule=rule, is_valid=True)
        
        if not hasattr(value, '__len__'):
            return ValidationResult(
                rule=rule,
                is_valid=False,
                error_message=f"Field '{rule.field_name}' does not support length validation"
            )
        
        length = len(value)
        min_length = rule.parameters.get('min')
        max_length = rule.parameters.get('max')
        
        is_valid = True
        error_parts = []
        
        if min_length is not None and length < min_length:
            is_valid = False
            error_parts.append(f"minimum {min_length}")
        
        if max_length is not None and length > max_length:
            is_valid = False
            error_parts.append(f"maximum {max_length}")
        
        # Auto-fix by truncating
        if not is_valid and self.auto_fix_errors and isinstance(value, str):
            if max_length is not None and length > max_length:
                truncated_value = value[:max_length]
                return ValidationResult(
                    rule=rule,
                    is_valid=True,
                    validated_value=truncated_value,
                    metadata={'auto_fix': 'string_truncation_applied'}
                )
        
        error_message = f"Field '{rule.field_name}' length must be between {' and '.join(error_parts)}" if not is_valid else None
        
        return ValidationResult(
            rule=rule,
            is_valid=is_valid,
            error_message=error_message
        )
    
    async def _validate_pattern(self, value: Any, rule: ValidationRule) -> ValidationResult:
        """Validate regex pattern"""
        if value is None or value == "":
            return ValidationResult(rule=rule, is_valid=True)
        
        pattern = rule.parameters.get('pattern')
        if not pattern:
            return ValidationResult(
                rule=rule,
                is_valid=False,
                error_message="Pattern validation requires 'pattern' parameter"
            )
        
        try:
            is_valid = bool(re.match(pattern, str(value)))
            
            return ValidationResult(
                rule=rule,
                is_valid=is_valid,
                error_message=f"Field '{rule.field_name}' does not match required pattern" if not is_valid else None
            )
            
        except re.error as e:
            return ValidationResult(
                rule=rule,
                is_valid=False,
                error_message=f"Invalid regex pattern: {str(e)}"
            )
    
    async def _validate_custom(self, value: Any, rule: ValidationRule, record: Dict[str, Any]) -> ValidationResult:
        """Validate using custom function"""
        validator_name = rule.parameters.get('validator')
        validator_func = self.custom_validators.get(validator_name)
        
        if not validator_func:
            return ValidationResult(
                rule=rule,
                is_valid=False,
                error_message=f"Custom validator '{validator_name}' not found"
            )
        
        try:
            # Call custom validator
            result = await validator_func(value, rule.parameters, record)
            
            if isinstance(result, bool):
                return ValidationResult(
                    rule=rule,
                    is_valid=result,
                    error_message=f"Custom validation failed for field '{rule.field_name}'" if not result else None
                )
            elif isinstance(result, dict):
                return ValidationResult(
                    rule=rule,
                    is_valid=result.get('is_valid', False),
                    error_message=result.get('error_message'),
                    warning_message=result.get('warning_message'),
                    validated_value=result.get('validated_value'),
                    metadata=result.get('metadata')
                )
            else:
                return ValidationResult(
                    rule=rule,
                    is_valid=False,
                    error_message="Custom validator returned unexpected result format"
                )
                
        except Exception as e:
            return ValidationResult(
                rule=rule,
                is_valid=False,
                error_message=f"Custom validator error: {str(e)}"
            )
    
    async def _validate_referential(self, value: Any, rule: ValidationRule) -> ValidationResult:
        """Validate referential integrity"""
        if value is None:
            return ValidationResult(rule=rule, is_valid=True)
        
        reference_table = rule.parameters.get('reference_table')
        reference_field = rule.parameters.get('reference_field', 'id')
        
        if not reference_table or reference_table not in self.reference_data:
            return ValidationResult(
                rule=rule,
                is_valid=False,
                error_message=f"Reference table '{reference_table}' not found"
            )
        
        reference_values = self.reference_data[reference_table]
        
        # Check if value exists in reference data
        if isinstance(reference_values, list):
            is_valid = value in reference_values
        elif isinstance(reference_values, dict):
            is_valid = value in reference_values.get(reference_field, [])
        else:
            is_valid = False
        
        return ValidationResult(
            rule=rule,
            is_valid=is_valid,
            error_message=f"Field '{rule.field_name}' value '{value}' not found in reference table '{reference_table}'" if not is_valid else None
        )
    
    async def _validate_uniqueness(self, value: Any, rule: ValidationRule) -> ValidationResult:
        """Validate uniqueness within the dataset"""
        if value is None:
            return ValidationResult(rule=rule, is_valid=True)
        
        # Initialize cache for this field if not exists
        if rule.field_name not in self.uniqueness_cache:
            self.uniqueness_cache[rule.field_name] = set()
        
        value_str = str(value)
        is_valid = value_str not in self.uniqueness_cache[rule.field_name]
        
        if is_valid:
            # Add to cache
            self.uniqueness_cache[rule.field_name].add(value_str)
        
        return ValidationResult(
            rule=rule,
            is_valid=is_valid,
            error_message=f"Field '{rule.field_name}' value '{value}' is not unique" if not is_valid else None
        )
    
    async def _validate_cross_fields(self, record: Dict[str, Any]) -> List[str]:
        """Validate cross-field rules"""
        errors = []
        
        for rule_name, rule_config in self.cross_field_rules.items():
            try:
                fields = rule_config.get('fields', [])
                condition = rule_config.get('condition')
                
                # Check if all required fields exist
                if not all(field in record for field in fields):
                    continue
                
                # Evaluate condition
                field_values = {field: record[field] for field in fields}
                
                # Replace field references in condition
                condition_expr = condition
                for field, value in field_values.items():
                    if isinstance(value, str):
                        condition_expr = condition_expr.replace(f"${field}", f"'{value}'")
                    else:
                        condition_expr = condition_expr.replace(f"${field}", str(value))
                
                # Evaluate condition (WARNING: eval is dangerous in production)
                try:
                    is_valid = eval(condition_expr)
                    if not is_valid:
                        error_message = rule_config.get('error_message', f"Cross-field validation failed: {rule_name}")
                        errors.append(error_message)
                except Exception as e:
                    errors.append(f"Cross-field rule '{rule_name}' evaluation error: {str(e)}")
                    
            except Exception as e:
                errors.append(f"Cross-field rule '{rule_name}' error: {str(e)}")
        
        return errors
    
    async def _validate_business_rules(self, record: Dict[str, Any]) -> List[str]:
        """Validate business rules"""
        errors = []
        
        for rule_name, rule_config in self.business_rules.items():
            try:
                condition = rule_config.get('condition')
                
                # Replace field references in condition
                condition_expr = condition
                for field_name, value in record.items():
                    if isinstance(value, str):
                        condition_expr = condition_expr.replace(f"${field_name}", f"'{value}'")
                    else:
                        condition_expr = condition_expr.replace(f"${field_name}", str(value))
                
                # Evaluate condition (WARNING: eval is dangerous in production)
                try:
                    is_valid = eval(condition_expr)
                    if not is_valid:
                        error_message = rule_config.get('error_message', f"Business rule validation failed: {rule_name}")
                        errors.append(error_message)
                except Exception as e:
                    errors.append(f"Business rule '{rule_name}' evaluation error: {str(e)}")
                    
            except Exception as e:
                errors.append(f"Business rule '{rule_name}' error: {str(e)}")
        
        return errors
    
    # Format validation functions
    async def _validate_email_format(self, value: str, parameters: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate email format"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        is_valid = bool(re.match(pattern, value))
        return is_valid, None
    
    async def _validate_phone_format(self, value: str, parameters: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate phone format"""
        # Remove common separators
        cleaned = re.sub(r'[\s\-\(\)]', '', value)
        # Check if it's a valid phone number (basic check)
        pattern = r'^\+?[\d]{8,15}$'
        is_valid = bool(re.match(pattern, cleaned))
        return is_valid, cleaned if is_valid else None
    
    async def _validate_date_format(self, value: str, parameters: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate date format"""
        date_formats = parameters.get('formats', ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y'])
        
        for date_format in date_formats:
            try:
                parsed_date = datetime.strptime(value, date_format)
                return True, parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        return False, None
    
    async def _validate_datetime_format(self, value: str, parameters: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate datetime format"""
        datetime_formats = parameters.get('formats', ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S'])
        
        for datetime_format in datetime_formats:
            try:
                parsed_datetime = datetime.strptime(value, datetime_format)
                return True, parsed_datetime.strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                continue
        
        return False, None
    
    async def _validate_url_format(self, value: str, parameters: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate URL format"""
        url_pattern = r'^https?://(?:[-\w.])+(?::[0-9]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:#(?:[\w.])*)?)?'
        is_valid = bool(re.match(url_pattern, value))
        return is_valid, None
    
    async def _validate_ipv4_format(self, value: str, parameters: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate IPv4 format"""
        parts = value.split('.')
        if len(parts) != 4:
            return False, None
        
        try:
            for part in parts:
                if not 0 <= int(part) <= 255:
                    return False, None
            return True, None
        except ValueError:
            return False, None
    
    async def _validate_ipv6_format(self, value: str, parameters: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate IPv6 format"""
        ipv6_pattern = r'^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$'
        is_valid = bool(re.match(ipv6_pattern, value))
        return is_valid, None
    
    async def _validate_uuid_format(self, value: str, parameters: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate UUID format"""
        uuid_pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'
        is_valid = bool(re.match(uuid_pattern, value))
        return is_valid, None
    
    async def _validate_credit_card_format(self, value: str, parameters: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate credit card format"""
        # Remove spaces and dashes
        cleaned = re.sub(r'[\s\-]', '', value)
        
        # Check if it's all digits and has valid length
        if not cleaned.isdigit() or len(cleaned) not in [13, 14, 15, 16, 17, 18, 19]:
            return False, None
        
        # Luhn algorithm check
        def luhn_check(card_num):
            def digits_of(n):
                return [int(d) for d in str(n)]
            
            digits = digits_of(card_num)
            odd_digits = digits[-1::-2]
            even_digits = digits[-2::-2]
            checksum = sum(odd_digits)
            for d in even_digits:
                checksum += sum(digits_of(d*2))
            return checksum % 10 == 0
        
        is_valid = luhn_check(cleaned)
        return is_valid, cleaned if is_valid else None
    
    async def _validate_iban_format(self, value: str, parameters: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate IBAN format"""
        # Remove spaces and convert to uppercase
        cleaned = re.sub(r'\s', '', value.upper())
        
        # Basic IBAN format check
        iban_pattern = r'^[A-Z]{2}[0-9]{2}[A-Z0-9]+'
        if not re.match(iban_pattern, cleaned):
            return False, None
        
        # Length check (varies by country)
        if len(cleaned) < 15 or len(cleaned) > 34:
            return False, None
        
        return True, cleaned
    
    async def _validate_postal_code_format(self, value: str, parameters: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate postal code format"""
        country = parameters.get('country', 'ID')
        
        patterns = {
            'ID': r'^\d{5}',  # Indonesia'
            'US': r'^\d{5}(-\d{4})?',  # United States'
            'GB': r'^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}',  # United Kingdom
            'CA': r'^[A-Z]\d[A-Z] \d[A-Z]\d',  # Canada
        }
        
        pattern = patterns.get(country, r'^\d{5}')
        is_valid = bool(re.match(pattern, value))
        return is_valid, None
    
    async def _validate_social_security_format(self, value: str, parameters: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate social security number format"""
        country = parameters.get('country', 'US')
        
        if country == 'US':
            # US SSN format: XXX-XX-XXXX
            pattern = r'^\d{3}-\d{2}-\d{4}'
        else:
            # Generic pattern
            pattern = r'^\d{9,11}'
        
        is_valid = bool(re.match(pattern, value))
        return is_valid, None
    
    async def _validate_passport_format(self, value: str, parameters: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate passport format"""
        country = parameters.get('country', 'ID')
        
        patterns = {
            'ID': r'^[A-Z]\d{7}',  # Indonesia
            'US': r'^[A-Z0-9]{6,9}',  # United States
            'GB': r'^[0-9]{9}',  # United Kingdom
        }
        
        pattern = patterns.get(country, r'^[A-Z0-9]{6,9}')
        is_valid = bool(re.match(pattern, value.upper()))
        return is_valid, value.upper() if is_valid else None
    
    async def _validate_license_plate_format(self, value: str, parameters: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate license plate format"""
        country = parameters.get('country', 'ID')
        
        patterns = {
            'ID': r'^[A-Z]{1,2} \d{1,4} [A-Z]{1,3}',  # Indonesia
            'US': r'^[A-Z0-9]{2,8}',  # United States (varies by state)
        }
        
        pattern = patterns.get(country, r'^[A-Z0-9\s]{2,10}')
        is_valid = bool(re.match(pattern, value.upper()))
        return is_valid, value.upper() if is_valid else None
    
    def _calculate_validation_quality_score(self, validation_results: List[ValidationResult], metadata: Dict[str, Any]) -> float:
        """Calculate quality score based on validation results"""
        if not validation_results:
            return 1.0
        
        summary = metadata.get('validation_summary', {})
        total_rules = summary.get('total_rules', 0)
        passed_rules = summary.get('passed_rules', 0)
        failed_rules = summary.get('failed_rules', 0)
        warning_rules = summary.get('warning_rules', 0)
        
        if total_rules == 0:
            return 1.0
        
        # Calculate base score
        base_score = passed_rules / total_rules
        
        # Apply penalties for failures and warnings
        critical_failures = sum(1 for r in validation_results if not r.is_valid and r.rule.severity == ValidationSeverity.CRITICAL)
        error_failures = sum(1 for r in validation_results if not r.is_valid and r.rule.severity == ValidationSeverity.ERROR)
        
        # Heavy penalty for critical failures
        if critical_failures > 0:
            base_score *= 0.5
        
        # Moderate penalty for error failures
        if error_failures > 0:
            base_score *= 0.8
        
        # Light penalty for warnings
        if warning_rules > 0:
            base_score *= 0.95
        
        # Bonus for auto-fixes
        auto_fixes = len(metadata.get('auto_fixes_applied', []))
        if auto_fixes > 0:
            base_score += 0.05 * min(auto_fixes, 3)  # Max 0.15 bonus
        
        return max(0.0, min(1.0, base_score))
