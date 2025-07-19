"""
Validation utilities for the ETL system.
Provides functions for data validation, sanitization, and format checking.
"""

import re
import json
import csv
import io
from typing import Any, Dict, List, Optional, Union, Tuple
from email_validator import validate_email as email_validate, EmailNotValidError
import phonenumbers
from phonenumbers import NumberParseException
import ipaddress
from urllib.parse import urlparse
import pandas as pd

from app.utils.logger import get_logger

logger = get_logger(__name__)

# Regular expressions for common validations
PATTERNS = {
    'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
    'phone_simple': r'^\+?[\d\s\-\(\)]{7,15}$',
    'url': r'^https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)$',
    'ipv4': r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$',
    'mac_address': r'^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$',
    'credit_card': r'^\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}$',
    'social_security': r'^\d{3}-?\d{2}-?\d{4}$',
    'postal_code_us': r'^\d{5}(-\d{4})?$',
    'postal_code_uk': r'^[A-Z]{1,2}\d[A-Z\d]? ?\d[A-Z]{2}$',
    'isbn': r'^(?:ISBN(?:-1[03])?:? )?(?=[0-9X]{10}$|(?=(?:[0-9]+[- ]){3})[- 0-9X]{13}$|97[89][0-9]{10}$|(?=(?:[0-9]+[- ]){4})[- 0-9]{17}$)(?:97[89][- ]?)?[0-9]{1,5}[- ]?[0-9]+[- ]?[0-9]+[- ]?[0-9X]$',
    'hex_color': r'^#(?:[0-9a-fA-F]{3}){1,2}$',
    'uuid': r'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$',
    'slug': r'^[a-z0-9]+(?:-[a-z0-9]+)*$',
    'username': r'^[a-zA-Z0-9_]{3,20}$',
    'password_strong': r'^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]{8,}$'
}


def validate_email(email: str, check_deliverability: bool = False) -> Dict[str, Any]:
    """
    Validate email address format and optionally check deliverability.
    
    Args:
        email: Email address to validate
        check_deliverability: Whether to check if email is deliverable
        
    Returns:
        Dictionary with validation results
    """
    try:
        if not email or not isinstance(email, str):
            return {
                "is_valid": False,
                "error": "Email is required and must be a string"
            }
        
        email = email.strip().lower()
        
        # Basic regex check first
        if not re.match(PATTERNS['email'], email):
            return {
                "is_valid": False,
                "error": "Invalid email format"
            }
        
        # Use email-validator for more thorough validation
        try:
            validation = email_validate(
                email,
                check_deliverability=check_deliverability
            )
            
            return {
                "is_valid": True,
                "normalized_email": validation.email,
                "local_part": validation.local,
                "domain": validation.domain,
                "ascii_email": validation.ascii_email,
                "smtputf8": validation.smtputf8
            }
            
        except EmailNotValidError as e:
            return {
                "is_valid": False,
                "error": str(e)
            }
            
    except Exception as e:
        logger.log_error("validate_email", e, {"email": email})
        return {
            "is_valid": False,
            "error": f"Validation error: {str(e)}"
        }


def validate_phone(phone: str, country_code: Optional[str] = None) -> Dict[str, Any]:
    """
    Validate phone number using phonenumbers library.
    
    Args:
        phone: Phone number to validate
        country_code: Country code (e.g., 'US', 'ID')
        
    Returns:
        Dictionary with validation results
    """
    try:
        if not phone or not isinstance(phone, str):
            return {
                "is_valid": False,
                "error": "Phone number is required and must be a string"
            }
        
        phone = phone.strip()
        
        # Parse phone number
        try:
            parsed_number = phonenumbers.parse(phone, country_code)
        except NumberParseException as e:
            return {
                "is_valid": False,
                "error": f"Failed to parse phone number: {e}"
            }
        
        # Validate the parsed number
        is_valid = phonenumbers.is_valid_number(parsed_number)
        is_possible = phonenumbers.is_possible_number(parsed_number)
        
        if not is_valid:
            return {
                "is_valid": False,
                "error": "Invalid phone number"
            }
        
        # Get formatted versions
        international_format = phonenumbers.format_number(
            parsed_number, phonenumbers.PhoneNumberFormat.INTERNATIONAL
        )
        national_format = phonenumbers.format_number(
            parsed_number, phonenumbers.PhoneNumberFormat.NATIONAL
        )
        e164_format = phonenumbers.format_number(
            parsed_number, phonenumbers.PhoneNumberFormat.E164
        )
        
        # Get number info
        number_type = phonenumbers.number_type(parsed_number)
        carrier = phonenumbers.carrier.name_for_number(parsed_number, 'en')
        geocoder = phonenumbers.geocoder.description_for_number(parsed_number, 'en')
        
        return {
            "is_valid": True,
            "is_possible": is_possible,
            "international_format": international_format,
            "national_format": national_format,
            "e164_format": e164_format,
            "country_code": parsed_number.country_code,
            "national_number": parsed_number.national_number,
            "number_type": phonenumbers.PhoneNumberType.to_string(number_type),
            "carrier": carrier,
            "location": geocoder
        }
        
    except Exception as e:
        logger.log_error("validate_phone", e, {"phone": phone, "country_code": country_code})
        return {
            "is_valid": False,
            "error": f"Validation error: {str(e)}"
        }


def validate_url(url: str, allowed_schemes: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Validate URL format and components.
    
    Args:
        url: URL to validate
        allowed_schemes: List of allowed schemes (default: ['http', 'https'])
        
    Returns:
        Dictionary with validation results
    """
    try:
        if not url or not isinstance(url, str):
            return {
                "is_valid": False,
                "error": "URL is required and must be a string"
            }
        
        url = url.strip()
        
        if allowed_schemes is None:
            allowed_schemes = ['http', 'https']
        
        # Parse URL
        try:
            parsed = urlparse(url)
        except Exception as e:
            return {
                "is_valid": False,
                "error": f"Failed to parse URL: {e}"
            }
        
        # Validate components
        if not parsed.scheme:
            return {
                "is_valid": False,
                "error": "URL must have a scheme (http/https)"
            }
        
        if parsed.scheme.lower() not in allowed_schemes:
            return {
                "is_valid": False,
                "error": f"Scheme '{parsed.scheme}' not allowed. Allowed: {allowed_schemes}"
            }
        
        if not parsed.netloc:
            return {
                "is_valid": False,
                "error": "URL must have a domain"
            }
        
        # Additional validation with regex
        if not re.match(PATTERNS['url'], url):
            return {
                "is_valid": False,
                "error": "Invalid URL format"
            }
        
        return {
            "is_valid": True,
            "scheme": parsed.scheme,
            "netloc": parsed.netloc,
            "path": parsed.path,
            "params": parsed.params,
            "query": parsed.query,
            "fragment": parsed.fragment,
            "hostname": parsed.hostname,
            "port": parsed.port,
            "username": parsed.username,
            "password": parsed.password
        }
        
    except Exception as e:
        logger.log_error("validate_url", e, {"url": url})
        return {
            "is_valid": False,
            "error": f"Validation error: {str(e)}"
        }


def validate_ip_address(ip: str, version: Optional[int] = None) -> Dict[str, Any]:
    """
    Validate IP address (IPv4 or IPv6).
    
    Args:
        ip: IP address to validate
        version: IP version (4 or 6, None for both)
        
    Returns:
        Dictionary with validation results
    """
    try:
        if not ip or not isinstance(ip, str):
            return {
                "is_valid": False,
                "error": "IP address is required and must be a string"
            }
        
        ip = ip.strip()
        
        try:
            ip_obj = ipaddress.ip_address(ip)
        except ValueError as e:
            return {
                "is_valid": False,
                "error": f"Invalid IP address: {e}"
            }
        
        # Check version if specified
        if version and ip_obj.version != version:
            return {
                "is_valid": False,
                "error": f"Expected IPv{version}, got IPv{ip_obj.version}"
            }
        
        return {
            "is_valid": True,
            "version": ip_obj.version,
            "is_private": ip_obj.is_private,
            "is_global": ip_obj.is_global,
            "is_reserved": ip_obj.is_reserved,
            "is_multicast": ip_obj.is_multicast,
            "is_loopback": ip_obj.is_loopback,
            "compressed": str(ip_obj),
            "exploded": ip_obj.exploded if ip_obj.version == 6 else str(ip_obj)
        }
        
    except Exception as e:
        logger.log_error("validate_ip_address", e, {"ip": ip})
        return {
            "is_valid": False,
            "error": f"Validation error: {str(e)}"
        }


def validate_json(json_string: str, schema: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Validate JSON string and optionally validate against schema.
    
    Args:
        json_string: JSON string to validate
        schema: Optional JSON schema for validation
        
    Returns:
        Dictionary with validation results
    """
    try:
        if not json_string or not isinstance(json_string, str):
            return {
                "is_valid": False,
                "error": "JSON string is required and must be a string"
            }
        
        json_string = json_string.strip()
        
        # Parse JSON
        try:
            parsed_json = json.loads(json_string)
        except json.JSONDecodeError as e:
            return {
                "is_valid": False,
                "error": f"Invalid JSON: {e}"
            }
        
        result = {
            "is_valid": True,
            "parsed_json": parsed_json,
            "json_type": type(parsed_json).__name__
        }
        
        # Schema validation if provided
        if schema:
            try:
                import jsonschema
                jsonschema.validate(parsed_json, schema)
                result["schema_valid"] = True
            except ImportError:
                result["schema_validation_error"] = "jsonschema library not available"
            except jsonschema.ValidationError as e:
                result["schema_valid"] = False
                result["schema_error"] = str(e)
        
        return result
        
    except Exception as e:
        logger.log_error("validate_json", e, {"json_string": json_string[:100]})
        return {
            "is_valid": False,
            "error": f"Validation error: {str(e)}"
        }


def validate_csv_headers(csv_content: str, required_headers: List[str], 
                        case_sensitive: bool = False, delimiter: str = ',') -> Dict[str, Any]:
    """
    Validate CSV headers against required headers.
    
    Args:
        csv_content: CSV content as string
        required_headers: List of required header names
        case_sensitive: Whether header matching is case sensitive
        delimiter: CSV delimiter
        
    Returns:
        Dictionary with validation results
    """
    try:
        if not csv_content or not isinstance(csv_content, str):
            return {
                "is_valid": False,
                "error": "CSV content is required and must be a string"
            }
        
        # Read CSV headers
        try:
            csv_reader = csv.reader(io.StringIO(csv_content), delimiter=delimiter)
            headers = next(csv_reader)
        except Exception as e:
            return {
                "is_valid": False,
                "error": f"Failed to read CSV headers: {e}"
            }
        
        # Clean headers
        headers = [h.strip() for h in headers]
        
        # Normalize case if not case sensitive
        if not case_sensitive:
            headers_normalized = [h.lower() for h in headers]
            required_normalized = [h.lower() for h in required_headers]
        else:
            headers_normalized = headers
            required_normalized = required_headers
        
        # Check for required headers
        missing_headers = []
        for required_header in required_normalized:
            if required_header not in headers_normalized:
                original_header = required_headers[required_normalized.index(required_header)]
                missing_headers.append(original_header)
        
        # Find extra headers
        extra_headers = []
        for header in headers_normalized:
            if header not in required_normalized:
                original_header = headers[headers_normalized.index(header)]
                extra_headers.append(original_header)
        
        is_valid = len(missing_headers) == 0
        
        return {
            "is_valid": is_valid,
            "found_headers": headers,
            "required_headers": required_headers,
            "missing_headers": missing_headers,
            "extra_headers": extra_headers,
            "header_count": len(headers),
            "required_count": len(required_headers)
        }
        
    except Exception as e:
        logger.log_error("validate_csv_headers", e, {
            "required_headers": required_headers,
            "delimiter": delimiter
        })
        return {
            "is_valid": False,
            "error": f"Validation error: {str(e)}"
        }


def validate_csv_data(csv_content: str, validation_rules: Dict[str, Dict], 
                     delimiter: str = ',', max_errors: int = 100) -> Dict[str, Any]:
    """
    Validate CSV data against validation rules.
    
    Args:
        csv_content: CSV content as string
        validation_rules: Dict with column names as keys and validation rules as values
        delimiter: CSV delimiter
        max_errors: Maximum number of errors to collect
        
    Returns:
        Dictionary with validation results
    """
    try:
        if not csv_content or not isinstance(csv_content, str):
            return {
                "is_valid": False,
                "error": "CSV content is required and must be a string"
            }
        
        # Read CSV
        try:
            df = pd.read_csv(io.StringIO(csv_content), delimiter=delimiter)
        except Exception as e:
            return {
                "is_valid": False,
                "error": f"Failed to read CSV: {e}"
            }
        
        errors = []
        warnings = []
        total_rows = len(df)
        
        # Validate each column
        for column, rules in validation_rules.items():
            if column not in df.columns:
                errors.append({
                    "type": "missing_column",
                    "column": column,
                    "message": f"Required column '{column}' not found"
                })
                continue
            
            column_data = df[column]
            
            # Check for required values
            if rules.get('required', False):
                null_rows = column_data.isnull() | (column_data == '')
                null_indices = df.index[null_rows].tolist()
                
                for idx in null_indices[:max_errors]:
                    errors.append({
                        "type": "required_field",
                        "column": column,
                        "row": idx + 2,  # +2 for header and 0-based index
                        "message": f"Required field '{column}' is empty"
                    })
                    
                    if len(errors) >= max_errors:
                        break
            
            # Data type validation
            if 'data_type' in rules:
                expected_type = rules['data_type']
                
                for idx, value in column_data.items():
                    if pd.isnull(value) or value == '':
                        continue
                    
                    try:
                        if expected_type == 'int':
                            int(value)
                        elif expected_type == 'float':
                            float(value)
                        elif expected_type == 'email':
                            result = validate_email(str(value))
                            if not result['is_valid']:
                                raise ValueError(result['error'])
                        elif expected_type == 'phone':
                            result = validate_phone(str(value))
                            if not result['is_valid']:
                                raise ValueError(result['error'])
                        elif expected_type == 'url':
                            result = validate_url(str(value))
                            if not result['is_valid']:
                                raise ValueError(result['error'])
                        # Add more type validations as needed
                        
                    except (ValueError, TypeError) as e:
                        errors.append({
                            "type": "data_type",
                            "column": column,
                            "row": idx + 2,
                            "value": str(value),
                            "expected_type": expected_type,
                            "message": f"Invalid {expected_type} format: {value}"
                        })
                        
                        if len(errors) >= max_errors:
                            break
            
            # Range validation
            if 'min_value' in rules or 'max_value' in rules:
                min_val = rules.get('min_value')
                max_val = rules.get('max_value')
                
                for idx, value in column_data.items():
                    if pd.isnull(value) or value == '':
                        continue
                    
                    try:
                        numeric_value = float(value)
                        
                        if min_val is not None and numeric_value < min_val:
                            errors.append({
                                "type": "range_validation",
                                "column": column,
                                "row": idx + 2,
                                "value": value,
                                "message": f"Value {value} is below minimum {min_val}"
                            })
                        
                        if max_val is not None and numeric_value > max_val:
                            errors.append({
                                "type": "range_validation",
                                "column": column,
                                "row": idx + 2,
                                "value": value,
                                "message": f"Value {value} is above maximum {max_val}"
                            })
                            
                    except (ValueError, TypeError):
                        # Skip if value can't be converted to numeric
                        pass
                    
                    if len(errors) >= max_errors:
                        break
            
            # Pattern validation
            if 'pattern' in rules:
                pattern = rules['pattern']
                
                for idx, value in column_data.items():
                    if pd.isnull(value) or value == '':
                        continue
                    
                    if not re.match(pattern, str(value)):
                        errors.append({
                            "type": "pattern_validation",
                            "column": column,
                            "row": idx + 2,
                            "value": str(value),
                            "pattern": pattern,
                            "message": f"Value '{value}' doesn't match required pattern"
                        })
                        
                        if len(errors) >= max_errors:
                            break
        
        is_valid = len(errors) == 0
        
        return {
            "is_valid": is_valid,
            "total_rows": total_rows,
            "total_columns": len(df.columns),
            "errors": errors,
            "warnings": warnings,
            "error_count": len(errors),
            "warning_count": len(warnings),
            "columns_validated": list(validation_rules.keys())
        }
        
    except Exception as e:
        logger.log_error("validate_csv_data", e, {
            "validation_rules": list(validation_rules.keys()) if validation_rules else []
        })
        return {
            "is_valid": False,
            "error": f"Validation error: {str(e)}"
        }


def sanitize_input(input_string: str, allowed_chars: Optional[str] = None, 
                  max_length: Optional[int] = None, strip_html: bool = True) -> str:
    """
    Sanitize input string by removing/escaping dangerous characters.
    
    Args:
        input_string: String to sanitize
        allowed_chars: Regex pattern of allowed characters
        max_length: Maximum allowed length
        strip_html: Whether to strip HTML tags
        
    Returns:
        Sanitized string
    """
    try:
        if not isinstance(input_string, str):
            return ""
        
        sanitized = input_string
        
        # Strip HTML tags if requested
        if strip_html:
            import html
            sanitized = html.escape(sanitized)
            # Remove HTML tags
            sanitized = re.sub(r'<[^>]+>', '', sanitized)
        
        # Remove null bytes and control characters
        sanitized = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', sanitized)
        
        # Apply character whitelist if provided
        if allowed_chars:
            sanitized = re.sub(f'[^{allowed_chars}]', '', sanitized)
        
        # Truncate if too long
        if max_length and len(sanitized) > max_length:
            sanitized = sanitized[:max_length]
        
        # Strip leading/trailing whitespace
        sanitized = sanitized.strip()
        
        return sanitized
        
    except Exception as e:
        logger.log_error("sanitize_input", e, {"input_string": input_string[:100]})
        return ""


def validate_with_pattern(value: str, pattern_name: str) -> Dict[str, Any]:
    """
    Validate value against predefined patterns.
    
    Args:
        value: Value to validate
        pattern_name: Name of the pattern to use
        
    Returns:
        Dictionary with validation results
    """
    try:
        if not value or not isinstance(value, str):
            return {
                "is_valid": False,
                "error": "Value is required and must be a string"
            }
        
        if pattern_name not in PATTERNS:
            return {
                "is_valid": False,
                "error": f"Unknown pattern: {pattern_name}"
            }
        
        pattern = PATTERNS[pattern_name]
        is_valid = bool(re.match(pattern, value.strip()))
        
        return {
            "is_valid": is_valid,
            "pattern_name": pattern_name,
            "pattern": pattern,
            "value": value.strip()
        }
        
    except Exception as e:
        logger.log_error("validate_with_pattern", e, {
            "value": value[:50] if value else None,
            "pattern_name": pattern_name
        })
        return {
            "is_valid": False,
            "error": f"Validation error: {str(e)}"
        }


def validate_data_completeness(data: Union[Dict, List], required_fields: List[str]) -> Dict[str, Any]:
    """
    Validate data completeness against required fields.
    
    Args:
        data: Data to validate (dict or list of dicts)
        required_fields: List of required field names
        
    Returns:
        Dictionary with validation results
    """
    try:
        if isinstance(data, dict):
            data_list = [data]
        elif isinstance(data, list):
            data_list = data
        else:
            return {
                "is_valid": False,
                "error": "Data must be a dictionary or list of dictionaries"
            }
        
        missing_fields_summary = {}
        incomplete_records = []
        
        for i, record in enumerate(data_list):
            if not isinstance(record, dict):
                incomplete_records.append({
                    "index": i,
                    "error": "Record is not a dictionary"
                })
                continue
            
            missing_fields = []
            for field in required_fields:
                if field not in record or record[field] is None or record[field] == '':
                    missing_fields.append(field)
            
            if missing_fields:
                incomplete_records.append({
                    "index": i,
                    "missing_fields": missing_fields
                })
                
                # Update summary
                for field in missing_fields:
                    missing_fields_summary[field] = missing_fields_summary.get(field, 0) + 1
        
        total_records = len(data_list)
        complete_records = total_records - len(incomplete_records)
        completeness_rate = (complete_records / total_records * 100) if total_records > 0 else 0
        
        return {
            "is_valid": len(incomplete_records) == 0,
            "total_records": total_records,
            "complete_records": complete_records,
            "incomplete_records": len(incomplete_records),
            "completeness_rate": round(completeness_rate, 2),
            "missing_fields_summary": missing_fields_summary,
            "incomplete_record_details": incomplete_records[:100]  # Limit to first 100
        }
        
    except Exception as e:
        logger.log_error("validate_data_completeness", e, {
            "data_type": type(data).__name__,
            "required_fields": required_fields
        })
        return {
            "is_valid": False,
            "error": f"Validation error: {str(e)}"
        }


def validate_data_uniqueness(data: List[Dict], unique_fields: List[str]) -> Dict[str, Any]:
    """
    Validate data uniqueness for specified fields.
    
    Args:
        data: List of dictionaries to validate
        unique_fields: List of field names that should be unique
        
    Returns:
        Dictionary with validation results
    """
    try:
        if not isinstance(data, list):
            return {
                "is_valid": False,
                "error": "Data must be a list of dictionaries"
            }
        
        duplicates = {}
        field_values = {field: {} for field in unique_fields}
        
        for i, record in enumerate(data):
            if not isinstance(record, dict):
                continue
            
            for field in unique_fields:
                if field in record and record[field] is not None:
                    value = record[field]
                    
                    if value in field_values[field]:
                        # Duplicate found
                        if field not in duplicates:
                            duplicates[field] = []
                        
                        duplicates[field].append({
                            "value": value,
                            "indices": [field_values[field][value], i]
                        })
                    else:
                        field_values[field][value] = i
        
        # Calculate uniqueness statistics
        uniqueness_stats = {}
        for field in unique_fields:
            total_values = len([r for r in data if isinstance(r, dict) and field in r and r[field] is not None])
            unique_values = len(field_values[field])
            duplicate_count = total_values - unique_values
            
            uniqueness_stats[field] = {
                "total_values": total_values,
                "unique_values": unique_values,
                "duplicate_count": duplicate_count,
                "uniqueness_rate": (unique_values / total_values * 100) if total_values > 0 else 0
            }
        
        is_valid = len(duplicates) == 0
        
        return {
            "is_valid": is_valid,
            "duplicates": duplicates,
            "uniqueness_stats": uniqueness_stats,
            "total_records": len(data)
        }
        
    except Exception as e:
        logger.log_error("validate_data_uniqueness", e, {
            "data_length": len(data) if isinstance(data, list) else "not_list",
            "unique_fields": unique_fields
        })
        return {
            "is_valid": False,
            "error": f"Validation error: {str(e)}"
        }
    