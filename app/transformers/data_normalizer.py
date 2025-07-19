# ==============================================
# app/transformers/data_normalizer.py
# ==============================================
import re
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import json
import hashlib

from .base_transformer import BaseTransformer, TransformationResult, TransformationStatus
from app.utils.logger import get_logger

logger = get_logger(__name__)

class NormalizationStrategy(Enum):
    """Data normalization strategies"""
    STANDARDIZE = "standardize"  # Convert to standard format
    CATEGORIZE = "categorize"    # Group into categories
    ENCODE = "encode"           # Encode categorical values
    SCALE = "scale"             # Scale numeric values
    LOOKUP = "lookup"           # Use lookup tables

@dataclass
class FieldNormalizationRule:
    """Configuration for field normalization"""
    field_name: str
    strategy: NormalizationStrategy
    parameters: Dict[str, Any]
    priority: int = 1
    is_required: bool = True

class DataNormalizer(BaseTransformer):
    """
    Data normalization transformer that handles:
    - Value standardization (country codes, currencies, etc.)
    - Categorical data encoding
    - Numeric scaling and normalization
    - Lookup table transformations
    - Business rule applications
    - Data type standardization
    - Reference data matching
    """
    
    def __init__(self, db_session, job_execution_id: Optional[str] = None, **kwargs):
        """
        Initialize data normalizer
        
        Args:
            db_session: Database session
            job_execution_id: Job execution ID for tracking
            **kwargs: Additional configuration
        """
        super().__init__(db_session, job_execution_id, **kwargs)
        
        # Normalization configuration
        self.normalization_rules = self._parse_normalization_rules(kwargs.get('normalization_rules', {}))
        self.apply_business_rules = kwargs.get('apply_business_rules', True)
        self.use_lookup_tables = kwargs.get('use_lookup_tables', True)
        self.create_derived_fields = kwargs.get('create_derived_fields', True)
        self.preserve_original_values = kwargs.get('preserve_original_values', False)
        
        # Default lookup tables
        self.lookup_tables = self._initialize_lookup_tables(kwargs.get('custom_lookup_tables', {}))
        
        # Business rules
        self.business_rules = kwargs.get('business_rules', {})
        
        # Derived field definitions
        self.derived_fields = kwargs.get('derived_fields', {})
        
        # Standardization mappings
        self.standardization_mappings = self._initialize_standardization_mappings()
    
    def _initialize_lookup_tables(self, custom_tables: Dict[str, Dict]) -> Dict[str, Dict]:
        """Initialize lookup tables with defaults and custom additions"""
        default_tables = {
            'countries': {
                'indonesia': {'code': 'ID', 'name': 'Indonesia', 'currency': 'IDR'},
                'id': {'code': 'ID', 'name': 'Indonesia', 'currency': 'IDR'},
                'ind': {'code': 'ID', 'name': 'Indonesia', 'currency': 'IDR'},
                'singapore': {'code': 'SG', 'name': 'Singapore', 'currency': 'SGD'},
                'sg': {'code': 'SG', 'name': 'Singapore', 'currency': 'SGD'},
                'malaysia': {'code': 'MY', 'name': 'Malaysia', 'currency': 'MYR'},
                'my': {'code': 'MY', 'name': 'Malaysia', 'currency': 'MYR'},
                'thailand': {'code': 'TH', 'name': 'Thailand', 'currency': 'THB'},
                'th': {'code': 'TH', 'name': 'Thailand', 'currency': 'THB'},
                'united states': {'code': 'US', 'name': 'United States', 'currency': 'USD'},
                'us': {'code': 'US', 'name': 'United States', 'currency': 'USD'},
                'usa': {'code': 'US', 'name': 'United States', 'currency': 'USD'},
            },
            'currencies': {
                'rupiah': {'code': 'IDR', 'symbol': 'Rp', 'name': 'Indonesian Rupiah'},
                'idr': {'code': 'IDR', 'symbol': 'Rp', 'name': 'Indonesian Rupiah'},
                'rp': {'code': 'IDR', 'symbol': 'Rp', 'name': 'Indonesian Rupiah'},
                'dollar': {'code': 'USD', 'symbol': '$', 'name': 'US Dollar'},
                'usd': {'code': 'USD', 'symbol': '$', 'name': 'US Dollar'},
                'sgd': {'code': 'SGD', 'symbol': 'S$', 'name': 'Singapore Dollar'},
                'euro': {'code': 'EUR', 'symbol': '€', 'name': 'Euro'},
                'eur': {'code': 'EUR', 'symbol': '€', 'name': 'Euro'},
            },
            'gender': {
                'male': {'code': 'M', 'name': 'Male'},
                'm': {'code': 'M', 'name': 'Male'},
                'man': {'code': 'M', 'name': 'Male'},
                'laki-laki': {'code': 'M', 'name': 'Male'},
                'l': {'code': 'M', 'name': 'Male'},
                'female': {'code': 'F', 'name': 'Female'},
                'f': {'code': 'F', 'name': 'Female'},
                'woman': {'code': 'F', 'name': 'Female'},
                'perempuan': {'code': 'F', 'name': 'Female'},
                'p': {'code': 'F', 'name': 'Female'},
            },
            'education_level': {
                'sd': {'code': 'ELEMENTARY', 'name': 'Elementary School', 'level': 1},
                'elementary': {'code': 'ELEMENTARY', 'name': 'Elementary School', 'level': 1},
                'smp': {'code': 'MIDDLE', 'name': 'Middle School', 'level': 2},
                'middle school': {'code': 'MIDDLE', 'name': 'Middle School', 'level': 2},
                'sma': {'code': 'HIGH', 'name': 'High School', 'level': 3},
                'high school': {'code': 'HIGH', 'name': 'High School', 'level': 3},
                'diploma': {'code': 'DIPLOMA', 'name': 'Diploma', 'level': 4},
                'd3': {'code': 'DIPLOMA', 'name': 'Diploma', 'level': 4},
                'bachelor': {'code': 'BACHELOR', 'name': 'Bachelor Degree', 'level': 5},
                's1': {'code': 'BACHELOR', 'name': 'Bachelor Degree', 'level': 5},
                'master': {'code': 'MASTER', 'name': 'Master Degree', 'level': 6},
                's2': {'code': 'MASTER', 'name': 'Master Degree', 'level': 6},
                'phd': {'code': 'PHD', 'name': 'PhD', 'level': 7},
                'doctorate': {'code': 'PHD', 'name': 'PhD', 'level': 7},
                's3': {'code': 'PHD', 'name': 'PhD', 'level': 7},
            },
            'marital_status': {
                'single': {'code': 'S', 'name': 'Single'},
                'belum menikah': {'code': 'S', 'name': 'Single'},
                'married': {'code': 'M', 'name': 'Married'},
                'menikah': {'code': 'M', 'name': 'Married'},
                'divorced': {'code': 'D', 'name': 'Divorced'},
                'cerai': {'code': 'D', 'name': 'Divorced'},
                'widowed': {'code': 'W', 'name': 'Widowed'},
                'janda': {'code': 'W', 'name': 'Widowed'},
                'duda': {'code': 'W', 'name': 'Widowed'},
            }
        }
        
        # Merge with custom tables
        for table_name, table_data in custom_tables.items():
            if table_name in default_tables:
                default_tables[table_name].update(table_data)
            else:
                default_tables[table_name] = table_data
        
        return default_tables
    
    def _initialize_standardization_mappings(self) -> Dict[str, Dict]:
        """Initialize standardization mappings"""
        return {
            'phone_prefixes': {
                'indonesia': ['+62', '62', '0'],
                'singapore': ['+65', '65'],
                'malaysia': ['+60', '60'],
                'thailand': ['+66', '66'],
                'us': ['+1', '1'],
            },
            'address_abbreviations': {
                'street': ['st', 'str', 'street'],
                'avenue': ['ave', 'av', 'avenue'],
                'road': ['rd', 'road'],
                'boulevard': ['blvd', 'boulevard'],
                'jalan': ['jl', 'jalan'],
                'gang': ['gg', 'gang'],
                'komplek': ['komp', 'komplek', 'complex'],
                'perumahan': ['perum', 'perumahan'],
            },
            'business_types': {
                'pt': ['pt', 'perseroan terbatas', 'incorporated', 'inc'],
                'cv': ['cv', 'commanditaire vennootschap'],
                'ltd': ['ltd', 'limited', 'terbatas'],
                'corp': ['corp', 'corporation', 'korporasi'],
                'llc': ['llc', 'limited liability company'],
            }
        }
    
    def _parse_normalization_rules(self, rules_config: Dict) -> List[FieldNormalizationRule]:
        """Parse normalization rules from configuration"""
        rules = []
        
        for field_name, rule_config in rules_config.items():
            if isinstance(rule_config, dict):
                rule = FieldNormalizationRule(
                    field_name=field_name,
                    strategy=NormalizationStrategy(rule_config.get('strategy', 'standardize')),
                    parameters=rule_config.get('parameters', {}),
                    priority=rule_config.get('priority', 1),
                    is_required=rule_config.get('is_required', True)
                )
                rules.append(rule)
        
        # Sort by priority
        rules.sort(key=lambda x: x.priority)
        return rules
    
    async def validate_config(self) -> Tuple[bool, List[str]]:
        """Validate data normalizer configuration"""
        errors = []
        
        # Validate normalization rules
        for rule in self.normalization_rules:
            if rule.strategy == NormalizationStrategy.LOOKUP:
                table_name = rule.parameters.get('table_name')
                if table_name and table_name not in self.lookup_tables:
                    errors.append(f"Lookup table '{table_name}' not found for field '{rule.field_name}'")
            
            elif rule.strategy == NormalizationStrategy.SCALE:
                method = rule.parameters.get('method', 'minmax')
                if method not in ['minmax', 'zscore', 'robust']:
                    errors.append(f"Invalid scaling method '{method}' for field '{rule.field_name}'")
        
        # Validate business rules
        for rule_name, rule_config in self.business_rules.items():
            if 'condition' not in rule_config:
                errors.append(f"Business rule '{rule_name}' missing condition")
            if 'action' not in rule_config:
                errors.append(f"Business rule '{rule_name}' missing action")
        
        return len(errors) == 0, errors
    
    async def transform_record(self, record: Dict[str, Any]) -> TransformationResult:
        """
        Normalize a single record
        
        Args:
            record: Input record to normalize
            
        Returns:
            TransformationResult with normalized data
        """
        try:
            normalized_record = record.copy()
            warnings = []
            metadata = {
                'original_fields': list(record.keys()),
                'normalization_rules_applied': [],
                'fields_modified': [],
                'derived_fields_created': [],
                'business_rules_applied': []
            }
            
            # Preserve original values if configured
            if self.preserve_original_values:
                for field_name, value in record.items():
                    normalized_record[f"_original_{field_name}"] = value
            
            # Apply field-specific normalization rules
            for rule in self.normalization_rules:
                if rule.field_name in normalized_record:
                    original_value = normalized_record[rule.field_name]
                    
                    try:
                        normalized_value, rule_warnings = await self._apply_normalization_rule(
                            original_value, rule
                        )
                        
                        if normalized_value != original_value:
                            normalized_record[rule.field_name] = normalized_value
                            metadata['fields_modified'].append(rule.field_name)
                            metadata['normalization_rules_applied'].append(f"{rule.field_name}:{rule.strategy.value}")
                        
                        warnings.extend(rule_warnings)
                        
                    except Exception as e:
                        if rule.is_required:
                            raise
                        else:
                            warnings.append(f"Failed to normalize field '{rule.field_name}': {str(e)}")
            
            # Apply business rules
            if self.apply_business_rules:
                for rule_name, rule_config in self.business_rules.items():
                    try:
                        if await self._evaluate_business_rule_condition(normalized_record, rule_config['condition']):
                            await self._apply_business_rule_action(normalized_record, rule_config['action'])
                            metadata['business_rules_applied'].append(rule_name)
                    except Exception as e:
                        warnings.append(f"Failed to apply business rule '{rule_name}': {str(e)}")
            
            # Create derived fields
            if self.create_derived_fields:
                for field_name, field_config in self.derived_fields.items():
                    try:
                        derived_value = await self._create_derived_field(normalized_record, field_config)
                        if derived_value is not None:
                            normalized_record[field_name] = derived_value
                            metadata['derived_fields_created'].append(field_name)
                    except Exception as e:
                        warnings.append(f"Failed to create derived field '{field_name}': {str(e)}")
            
            # Calculate quality score
            quality_score = self._calculate_quality_score(normalized_record, record)
            metadata['quality_score'] = quality_score
            
            return TransformationResult(
                status=TransformationStatus.SUCCESS,
                data=normalized_record,
                warnings=warnings,
                metadata=metadata
            )
            
        except Exception as e:
            self.logger.error(f"Error normalizing record: {str(e)}")
            return TransformationResult(
                status=TransformationStatus.FAILED,
                errors=[f"Data normalization failed: {str(e)}"]
            )
    
    async def _apply_normalization_rule(self, value: Any, rule: FieldNormalizationRule) -> Tuple[Any, List[str]]:
        """Apply a single normalization rule"""
        warnings = []
        
        if value is None:
            return value, warnings
        
        if rule.strategy == NormalizationStrategy.STANDARDIZE:
            return await self._standardize_value(value, rule.parameters), warnings
        
        elif rule.strategy == NormalizationStrategy.CATEGORIZE:
            return await self._categorize_value(value, rule.parameters), warnings
        
        elif rule.strategy == NormalizationStrategy.ENCODE:
            return await self._encode_value(value, rule.parameters), warnings
        
        elif rule.strategy == NormalizationStrategy.SCALE:
            return await self._scale_value(value, rule.parameters), warnings
        
        elif rule.strategy == NormalizationStrategy.LOOKUP:
            return await self._lookup_value(value, rule.parameters), warnings
        
        else:
            warnings.append(f"Unknown normalization strategy: {rule.strategy}")
            return value, warnings
    
    async def _standardize_value(self, value: Any, parameters: Dict[str, Any]) -> Any:
        """Standardize value based on parameters"""
        if not isinstance(value, str):
            return value
        
        value = value.strip().lower()
        
        # Apply standardization type
        standardization_type = parameters.get('type', 'general')
        
        if standardization_type == 'country':
            return self._standardize_country(value)
        elif standardization_type == 'currency':
            return self._standardize_currency(value)
        elif standardization_type == 'gender':
            return self._standardize_gender(value)
        elif standardization_type == 'education':
            return self._standardize_education(value)
        elif standardization_type == 'marital_status':
            return self._standardize_marital_status(value)
        elif standardization_type == 'phone':
            return self._standardize_phone(value, parameters)
        elif standardization_type == 'address':
            return self._standardize_address(value)
        elif standardization_type == 'business_type':
            return self._standardize_business_type(value)
        else:
            return value
    
    async def _categorize_value(self, value: Any, parameters: Dict[str, Any]) -> Any:
        """Categorize value into predefined categories"""
        if not isinstance(value, (str, int, float)):
            return value
        
        categories = parameters.get('categories', {})
        
        if isinstance(value, str):
            value_lower = value.lower()
            for category, keywords in categories.items():
                if any(keyword.lower() in value_lower for keyword in keywords):
                    return category
        
        elif isinstance(value, (int, float)):
            ranges = parameters.get('ranges', {})
            for category, (min_val, max_val) in ranges.items():
                if min_val <= value <= max_val:
                    return category
        
        return parameters.get('default_category', 'OTHER')
    
    async def _encode_value(self, value: Any, parameters: Dict[str, Any]) -> Any:
        """Encode categorical value"""
        encoding_type = parameters.get('type', 'label')
        
        if encoding_type == 'label':
            # Label encoding
            mapping = parameters.get('mapping', {})
            return mapping.get(str(value).lower(), value)
        
        elif encoding_type == 'onehot':
            # One-hot encoding (return dictionary)
            categories = parameters.get('categories', [])
            result = {}
            for category in categories:
                result[f"{parameters.get('prefix', 'encoded')}_{category}"] = 1 if str(value).lower() == category.lower() else 0
            return result
        
        else:
            return value
    
    async def _scale_value(self, value: Any, parameters: Dict[str, Any]) -> Any:
        """Scale numeric value"""
        if not isinstance(value, (int, float)):
            return value
        
        method = parameters.get('method', 'minmax')
        
        if method == 'minmax':
            min_val = parameters.get('min', 0)
            max_val = parameters.get('max', 1)
            old_min = parameters.get('old_min', 0)
            old_max = parameters.get('old_max', 100)
            
            if old_max - old_min != 0:
                scaled = ((value - old_min) / (old_max - old_min)) * (max_val - min_val) + min_val
                return round(scaled, parameters.get('decimal_places', 2))
        
        elif method == 'zscore':
            mean = parameters.get('mean', 0)
            std = parameters.get('std', 1)
            
            if std != 0:
                scaled = (value - mean) / std
                return round(scaled, parameters.get('decimal_places', 2))
        
        return value
    
    async def _lookup_value(self, value: Any, parameters: Dict[str, Any]) -> Any:
        """Look up value in lookup table"""
        table_name = parameters.get('table_name')
        if not table_name or table_name not in self.lookup_tables:
            return value
        
        lookup_table = self.lookup_tables[table_name]
        value_key = str(value).lower()
        
        if value_key in lookup_table:
            lookup_result = lookup_table[value_key]
            return_field = parameters.get('return_field', 'code')
            
            if isinstance(lookup_result, dict) and return_field in lookup_result:
                return lookup_result[return_field]
            else:
                return lookup_result
        
        return parameters.get('default_value', value)
    
    def _standardize_country(self, value: str) -> str:
        """Standardize country name"""
        return self.lookup_tables['countries'].get(value, {}).get('code', value.upper())
    
    def _standardize_currency(self, value: str) -> str:
        """Standardize currency"""
        return self.lookup_tables['currencies'].get(value, {}).get('code', value.upper())
    
    def _standardize_gender(self, value: str) -> str:
        """Standardize gender"""
        return self.lookup_tables['gender'].get(value, {}).get('code', value.upper())
    
    def _standardize_education(self, value: str) -> str:
        """Standardize education level"""
        return self.lookup_tables['education_level'].get(value, {}).get('code', value.upper())
    
    def _standardize_marital_status(self, value: str) -> str:
        """Standardize marital status"""
        return self.lookup_tables['marital_status'].get(value, {}).get('code', value.upper())
    
    def _standardize_phone(self, value: str, parameters: Dict[str, Any]) -> str:
        """Standardize phone number format"""
        country = parameters.get('country', 'indonesia')
        prefixes = self.standardization_mappings['phone_prefixes'].get(country, [])
        
        # Remove common separators
        cleaned = re.sub(r'[\s\-\(\)]', '', value)
        
        # Add country prefix if missing
        if not any(cleaned.startswith(prefix) for prefix in prefixes):
            if prefixes:
                cleaned = prefixes[0] + cleaned.lstrip('0')
        
        return cleaned
    
    def _standardize_address(self, value: str) -> str:
        """Standardize address abbreviations"""
        words = value.lower().split()
        standardized_words = []
        
        for word in words:
            # Check if word is an abbreviation
            for standard, abbreviations in self.standardization_mappings['address_abbreviations'].items():
                if word in abbreviations:
                    standardized_words.append(standard)
                    break
            else:
                standardized_words.append(word)
        
        return ' '.join(standardized_words).title()
    
    def _standardize_business_type(self, value: str) -> str:
        """Standardize business type"""
        value_lower = value.lower()
        
        for standard, variations in self.standardization_mappings['business_types'].items():
            if any(variation in value_lower for variation in variations):
                return standard.upper()
        
        return value
    
    async def _evaluate_business_rule_condition(self, record: Dict[str, Any], condition: str) -> bool:
        """Evaluate business rule condition"""
        try:
            # Simple condition evaluation
            # In production, you might want to use a more sophisticated rule engine
            
            # Replace field references with actual values
            condition_expr = condition
            for field_name, value in record.items():
                if isinstance(value, str):
                    condition_expr = condition_expr.replace(f"${field_name}", f"'{value}'")
                else:
                    condition_expr = condition_expr.replace(f"${field_name}", str(value))
            
            # Evaluate the condition (WARNING: eval is dangerous in production)
            # Consider using a proper rule engine like python-rules-engine
            return eval(condition_expr)
            
        except Exception as e:
            self.logger.warning(f"Failed to evaluate business rule condition: {str(e)}")
            return False
    
    async def _apply_business_rule_action(self, record: Dict[str, Any], action: Dict[str, Any]):
        """Apply business rule action"""
        action_type = action.get('type')
        
        if action_type == 'set_field':
            field_name = action.get('field')
            value = action.get('value')
            if field_name:
                record[field_name] = value
        
        elif action_type == 'calculate_field':
            field_name = action.get('field')
            expression = action.get('expression')
            if field_name and expression:
                # Replace field references in expression
                for ref_field, ref_value in record.items():
                    if isinstance(ref_value, (int, float)):
                        expression = expression.replace(f"${ref_field}", str(ref_value))
                
                try:
                    result = eval(expression)  # WARNING: eval is dangerous
                    record[field_name] = result
                except Exception as e:
                    self.logger.warning(f"Failed to calculate field '{field_name}': {str(e)}")
        
        elif action_type == 'copy_field':
            source_field = action.get('source_field')
            target_field = action.get('target_field')
            if source_field and target_field and source_field in record:
                record[target_field] = record[source_field]
    
    async def _create_derived_field(self, record: Dict[str, Any], field_config: Dict[str, Any]) -> Any:
        """Create derived field based on configuration"""
        derivation_type = field_config.get('type')
        
        if derivation_type == 'concatenate':
            fields = field_config.get('fields', [])
            separator = field_config.get('separator', ' ')
            values = [str(record.get(field, '')) for field in fields if record.get(field)]
            return separator.join(values) if values else None
        
        elif derivation_type == 'calculate':
            expression = field_config.get('expression')
            if expression:
                # Replace field references
                for field_name, value in record.items():
                    if isinstance(value, (int, float)):
                        expression = expression.replace(f"${field_name}", str(value))
                
                try:
                    return eval(expression)  # WARNING: eval is dangerous
                except Exception:
                    return None
        
        elif derivation_type == 'lookup':
            source_field = field_config.get('source_field')
            table_name = field_config.get('table_name')
            return_field = field_config.get('return_field', 'code')
            
            if source_field and table_name and source_field in record:
                source_value = str(record[source_field]).lower()
                lookup_table = self.lookup_tables.get(table_name, {})
                lookup_result = lookup_table.get(source_value, {})
                
                if isinstance(lookup_result, dict) and return_field in lookup_result:
                    return lookup_result[return_field]
                else:
                    return lookup_result
        
        elif derivation_type == 'conditional':
            condition = field_config.get('condition')
            true_value = field_config.get('true_value')
            false_value = field_config.get('false_value')
            
            if condition:
                try:
                    # Replace field references
                    condition_expr = condition
                    for field_name, value in record.items():
                        if isinstance(value, str):
                            condition_expr = condition_expr.replace(f"${field_name}", f"'{value}'")
                        else:
                            condition_expr = condition_expr.replace(f"${field_name}", str(value))
                    
                    result = eval(condition_expr)  # WARNING: eval is dangerous
                    return true_value if result else false_value
                except Exception:
                    return false_value
        
        return None
    
    def _calculate_quality_score(self, normalized_record: Dict[str, Any], original_record: Dict[str, Any]) -> float:
        """Calculate quality score for normalized record"""
        base_score = super()._calculate_quality_score(normalized_record, original_record)
        
        # Additional scoring for normalization-specific metrics
        normalization_score = 1.0
        
        # Check for successful standardizations
        standardized_fields = 0
        total_applicable_fields = 0
        
        for rule in self.normalization_rules:
            if rule.field_name in normalized_record:
                total_applicable_fields += 1
                original_value = original_record.get(rule.field_name)
                normalized_value = normalized_record.get(rule.field_name)
                
                # Check if normalization was successful
                if original_value != normalized_value and normalized_value is not None:
                    standardized_fields += 1
        
        # Calculate standardization success rate
        if total_applicable_fields > 0:
            standardization_rate = standardized_fields / total_applicable_fields
            normalization_score = 0.7 + (standardization_rate * 0.3)
        
        # Combine with base score
        final_score = (base_score * 0.6) + (normalization_score * 0.4)
        
        return max(0.0, min(1.0, final_score))