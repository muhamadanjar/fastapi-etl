# ==============================================
# app/processors/json_processor.py
# ==============================================
import json
import ijson
from typing import Dict, List, Any, Optional, Tuple, Iterator, Union
from pathlib import Path
from datetime import datetime
import pandas as pd
from collections import defaultdict

from .base_processor import BaseProcessor
from app.core.exceptions import FileProcessingException
from app.infrastructure.db.models.raw_data.file_registry import FileRegistry
from app.utils.logger import get_logger

logger = get_logger(__name__)

class JSONProcessor(BaseProcessor):
    """
    JSON file processor supporting various JSON structures:
    - Array of objects
    - Nested JSON objects
    - Line-delimited JSON (JSONL)
    - Streaming JSON processing for large files
    """
    
    def __init__(self, db_session, batch_id: Optional[str] = None, **kwargs):
        """
        Initialize JSON processor
        
        Args:
            db_session: Database session
            batch_id: Optional batch ID
            **kwargs: Additional configuration options
        """
        super().__init__(db_session, batch_id)
        
        # JSON-specific configuration
        self.json_path = kwargs.get('json_path', None)      # JSONPath for nested data
        self.flatten_nested = kwargs.get('flatten_nested', True)  # Flatten nested objects
        self.array_handling = kwargs.get('array_handling', 'separate_records')  # 'separate_records', 'join_string', 'json_string'
        self.max_depth = kwargs.get('max_depth', 10)       # Maximum nesting depth
        self.chunk_size = kwargs.get('chunk_size', 5000)   # Records per chunk
        self.encoding = kwargs.get('encoding', 'utf-8')    # File encoding
        self.strict_mode = kwargs.get('strict_mode', False) # Strict JSON parsing
        
        # Supported JSON structures
        self.json_types = {
            'array_of_objects': 'Array of objects [{}, {}, ...]',
            'single_object': 'Single object with nested data',
            'nested_object': 'Nested object structure',
            'jsonl': 'Line-delimited JSON (JSONL)',
            'mixed': 'Mixed JSON structure'
        }
        
    async def validate_file_format(self, file_path: str) -> Tuple[bool, str]:
        """
        Validate if file is valid JSON format
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            file_path_obj = Path(file_path)
            
            # Check file exists
            if not file_path_obj.exists():
                return False, "File does not exist"
            
            # Check file extension
            valid_extensions = ['.json', '.jsonl', '.ndjson', '.txt']
            if file_path_obj.suffix.lower() not in valid_extensions:
                return False, f"Invalid file extension: {file_path_obj.suffix}. Expected: {valid_extensions}"
            
            # Check file size
            file_size = file_path_obj.stat().st_size
            if file_size == 0:
                return False, "File is empty"
            
            if file_size > 1024 * 1024 * 1024:  # 1GB limit
                return False, "File too large (>1GB)"
            
            # Detect JSON structure and validate
            json_structure = await self._detect_json_structure(file_path)
            
            if json_structure['type'] == 'invalid':
                return False, f"Invalid JSON format: {json_structure['error']}"
            
            # Try to parse first few records
            try:
                sample_records = await self._get_sample_records(file_path, 5)
                if not sample_records:
                    return False, "No valid JSON records found"
                
                return True, f"Valid JSON file ({json_structure['type']})"
                
            except Exception as e:
                return False, f"JSON parsing error: {str(e)}"
            
        except Exception as e:
            return False, f"JSON validation error: {str(e)}"
    
    async def detect_structure(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Detect JSON structure and analyze data schema
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            List of field information dictionaries
        """
        try:
            # Detect JSON structure type
            json_structure = await self._detect_json_structure(file_path)
            
            # Get sample records for analysis
            sample_records = await self._get_sample_records(file_path, 1000)
            
            if not sample_records:
                raise FileProcessingException("No records found for structure analysis")
            
            # Analyze schema from sample records
            schema_info = await self._analyze_json_schema(sample_records)
            
            # Convert schema to column-like structure
            columns_info = []
            
            for field_path, field_info in schema_info.items():
                column_info = {
                    "name": field_path,
                    "position": len(columns_info),
                    "data_type": field_info['detected_type'],
                    "sample_values": field_info['sample_values'][:10],
                    "null_count": field_info['null_count'],
                    "unique_count": len(field_info['unique_values']),
                    "total_count": field_info['total_count'],
                    "min_length": field_info.get('min_length'),
                    "max_length": field_info.get('max_length'),
                    "null_percentage": round((field_info['null_count'] / field_info['total_count']) * 100, 2),
                    "json_path": field_path,
                    "nested_level": field_path.count('.'),
                    "field_type": field_info['field_type']  # 'primitive', 'object', 'array'
                }
                
                columns_info.append(column_info)
            
            # Add JSON structure metadata
            structure_metadata = {
                "json_structure_type": json_structure['type'],
                "total_fields": len(columns_info),
                "max_nesting_level": max([c['nested_level'] for c in columns_info] + [0]),
                "estimated_records": json_structure.get('estimated_records', 0)
            }
            
            # Add metadata as first "column"
            columns_info.insert(0, {
                "name": "_json_metadata",
                "position": -1,
                "data_type": "METADATA",
                "metadata": structure_metadata
            })
            
            self.logger.info(f"Detected JSON structure with {len(columns_info)} fields")
            return columns_info
            
        except Exception as e:
            self.logger.error(f"Error detecting JSON structure: {str(e)}")
            raise FileProcessingException(f"Failed to detect JSON structure: {str(e)}")
    
    async def preview_data(self, file_path: str, rows: int = 10) -> Dict[str, Any]:
        """
        Generate preview of JSON data
        
        Args:
            file_path: Path to JSON file
            rows: Number of records to preview
            
        Returns:
            Dictionary containing preview data and metadata
        """
        try:
            # Detect JSON structure
            json_structure = await self._detect_json_structure(file_path)
            
            # Get sample records
            sample_records = await self._get_sample_records(file_path, rows)
            
            if not sample_records:
                raise FileProcessingException("No records found for preview")
            
            # Process records for preview
            processed_records = []
            for record in sample_records:
                if self.flatten_nested:
                    flattened = self._flatten_json(record)
                    processed_records.append(flattened)
                else:
                    processed_records.append(record)
            
            # Create DataFrame for column analysis
            df_preview = pd.json_normalize(processed_records)
            
            preview_data = {
                "columns": df_preview.columns.tolist(),
                "data": processed_records,
                "metadata": {
                    "total_records_estimated": json_structure.get('estimated_records', 0),
                    "preview_records": len(processed_records),
                    "total_fields": len(df_preview.columns),
                    "file_size": self._get_file_size(file_path),
                    "json_structure_type": json_structure['type'],
                    "encoding": self.encoding,
                    "flatten_nested": self.flatten_nested,
                    "max_nesting_level": self._get_max_nesting_level(processed_records)
                }
            }
            
            return preview_data
            
        except Exception as e:
            self.logger.error(f"Error generating JSON preview: {str(e)}")
            raise FileProcessingException(f"Failed to generate JSON preview: {str(e)}")
    
    async def process_file(self, file_path: str, file_registry: FileRegistry) -> Dict[str, Any]:
        """
        Process JSON file and extract all data
        
        Args:
            file_path: Path to JSON file
            file_registry: File registry record
            
        Returns:
            Processing results and statistics
        """
        try:
            self.logger.info(f"Starting JSON processing for file: {file_path}")
            
            # Detect JSON structure
            json_structure = await self._detect_json_structure(file_path)
            
            # Update file metadata
            metadata = file_registry.file_metadata or {}
            metadata.update({
                "json_structure_type": json_structure['type'],
                "encoding": self.encoding,
                "flatten_nested": self.flatten_nested,
                "array_handling": self.array_handling,
                "estimated_records": json_structure.get('estimated_records', 0),
                "processor": "JSONProcessor"
            })
            file_registry.file_metadata = metadata
            self.db_session.add(file_registry)
            
            # Detect and save structure
            structure_info = await self.detect_structure(file_path)
            await self.save_column_structure(file_registry, structure_info)
            
            # Process records based on JSON structure type
            if json_structure['type'] == 'jsonl':
                record_iterator = self._read_jsonl_chunks(file_path)
            else:
                record_iterator = self._read_json_chunks(file_path, json_structure)
            
            processing_stats = await self.process_records(record_iterator, file_registry)
            
            # Update processing statistics
            processing_stats.update({
                "file_type": "JSON",
                "json_structure_type": json_structure['type'],
                "encoding": self.encoding,
                "fields_detected": len(structure_info)
            })
            
            self.logger.info(f"JSON processing completed: {processing_stats}")
            return processing_stats
            
        except Exception as e:
            self.logger.error(f"Error processing JSON file: {str(e)}")
            raise FileProcessingException(f"Failed to process JSON file: {str(e)}")
    
    async def _detect_json_structure(self, file_path: str) -> Dict[str, Any]:
        """
        Detect the type of JSON structure in the file
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            Dictionary with structure type and metadata
        """
        try:
            with open(file_path, 'r', encoding=self.encoding) as file:
                # Read first few characters to determine structure
                first_char = file.read(1)
                file.seek(0)
                
                if first_char == '[':
                    # Array of objects
                    try:
                        # Try to count items in array for estimation
                        item_count = 0
                        parser = ijson.parse(file)
                        for prefix, event, value in parser:
                            if event == 'start_array':
                                continue
                            elif event == 'start_map':
                                item_count += 1
                                if item_count > 100:  # Stop counting after 100 for estimation
                                    break
                        
                        return {
                            'type': 'array_of_objects',
                            'estimated_records': item_count * 10 if item_count == 100 else item_count
                        }
                    except:
                        return {'type': 'array_of_objects', 'estimated_records': 0}
                
                elif first_char == '{':
                    # Check if it's JSONL or single object
                    first_line = file.readline()
                    second_line = file.readline()
                    
                    if second_line.strip() and second_line.strip().startswith('{'):
                        # JSONL format
                        file.seek(0)
                        line_count = sum(1 for line in file if line.strip())
                        return {
                            'type': 'jsonl',
                            'estimated_records': line_count
                        }
                    else:
                        # Single object or nested structure
                        return {
                            'type': 'single_object',
                            'estimated_records': 1
                        }
                
                else:
                    return {
                        'type': 'invalid',
                        'error': f"Unexpected first character: {first_char}"
                    }
                    
        except Exception as e:
            return {
                'type': 'invalid',
                'error': str(e)
            }
    
    async def _get_sample_records(self, file_path: str, max_records: int) -> List[Dict[str, Any]]:
        """
        Get sample records from JSON file for analysis
        
        Args:
            file_path: Path to JSON file
            max_records: Maximum number of records to sample
            
        Returns:
            List of sample records
        """
        try:
            json_structure = await self._detect_json_structure(file_path)
            sample_records = []
            
            if json_structure['type'] == 'jsonl':
                # Line-delimited JSON
                with open(file_path, 'r', encoding=self.encoding) as file:
                    for i, line in enumerate(file):
                        if i >= max_records:
                            break
                        try:
                            record = json.loads(line.strip())
                            if self.flatten_nested:
                                record = self._flatten_json(record)
                            sample_records.append(record)
                        except json.JSONDecodeError:
                            continue
            
            elif json_structure['type'] == 'array_of_objects':
                # Array of objects
                with open(file_path, 'r', encoding=self.encoding) as file:
                    parser = ijson.items(file, 'item')
                    for i, record in enumerate(parser):
                        if i >= max_records:
                            break
                        if self.flatten_nested:
                            record = self._flatten_json(record)
                        sample_records.append(record)
            
            elif json_structure['type'] == 'single_object':
                # Single object - extract records from nested structure
                with open(file_path, 'r', encoding=self.encoding) as file:
                    data = json.load(file)
                    
                if self.json_path:
                    # Use JSONPath to extract data
                    records = self._extract_by_path(data, self.json_path)
                else:
                    # Auto-detect arrays in the object
                    records = self._auto_extract_records(data)
                
                for i, record in enumerate(records[:max_records]):
                    if self.flatten_nested:
                        record = self._flatten_json(record)
                    sample_records.append(record)
            
            return sample_records
            
        except Exception as e:
            self.logger.error(f"Error getting sample records: {str(e)}")
            return []
    
    def _read_json_chunks(self, file_path: str, json_structure: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        Read JSON file in chunks based on structure type
        
        Args:
            file_path: Path to JSON file
            json_structure: Detected JSON structure info
            
        Yields:
            Dictionary records from JSON
        """
        try:
            if json_structure['type'] == 'array_of_objects':
                # Stream array of objects
                with open(file_path, 'r', encoding=self.encoding) as file:
                    parser = ijson.items(file, 'item')
                    chunk = []
                    
                    for record in parser:
                        if self.flatten_nested:
                            record = self._flatten_json(record)
                        
                        chunk.append(record)
                        
                        if len(chunk) >= self.chunk_size:
                            for item in chunk:
                                yield item
                            chunk = []
                    
                    # Yield remaining items
                    for item in chunk:
                        yield item
            
            elif json_structure['type'] == 'single_object':
                # Extract records from single object
                with open(file_path, 'r', encoding=self.encoding) as file:
                    data = json.load(file)
                
                if self.json_path:
                    records = self._extract_by_path(data, self.json_path)
                else:
                    records = self._auto_extract_records(data)
                
                for record in records:
                    if self.flatten_nested:
                        record = self._flatten_json(record)
                    yield record
                    
        except Exception as e:
            self.logger.error(f"Error reading JSON chunks: {str(e)}")
            raise FileProcessingException(f"Failed to read JSON file: {str(e)}")
    
    def _read_jsonl_chunks(self, file_path: str) -> Iterator[Dict[str, Any]]:
        """
        Read JSONL (line-delimited JSON) file in chunks
        
        Args:
            file_path: Path to JSONL file
            
        Yields:
            Dictionary records from JSONL
        """
        try:
            with open(file_path, 'r', encoding=self.encoding) as file:
                chunk = []
                
                for line_number, line in enumerate(file, 1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        record = json.loads(line)
                        if self.flatten_nested:
                            record = self._flatten_json(record)
                        
                        chunk.append(record)
                        
                        if len(chunk) >= self.chunk_size:
                            for item in chunk:
                                yield item
                            chunk = []
                            
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"Invalid JSON on line {line_number}: {str(e)}")
                        continue
                
                # Yield remaining items
                for item in chunk:
                    yield item
                    
        except Exception as e:
            self.logger.error(f"Error reading JSONL chunks: {str(e)}")
            raise FileProcessingException(f"Failed to read JSONL file: {str(e)}")
    
    def _flatten_json(self, obj: Any, parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
        """
        Flatten nested JSON object
        
        Args:
            obj: JSON object to flatten
            parent_key: Parent key for nested objects
            sep: Separator for nested keys
            
        Returns:
            Flattened dictionary
        """
        items = []
        
        if isinstance(obj, dict):
            for k, v in obj.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                
                if isinstance(v, dict) and len(str(v)) < 1000:  # Avoid very deep nesting
                    items.extend(self._flatten_json(v, new_key, sep=sep).items())
                elif isinstance(v, list):
                    items.append((new_key, self._handle_array(v)))
                else:
                    items.append((new_key, v))
        elif isinstance(obj, list):
            return self._handle_array(obj)
        else:
            return obj
        
        return dict(items)
    
    def _handle_array(self, arr: List[Any]) -> Any:
        """
        Handle array values based on configuration
        
        Args:
            arr: Array to handle
            
        Returns:
            Processed array value
        """
        if not arr:
            return None
        
        if self.array_handling == 'separate_records':
            # For now, convert to JSON string (could be expanded to create separate records)
            return json.dumps(arr)
        elif self.array_handling == 'join_string':
            # Join array elements as string
            return ', '.join(str(item) for item in arr)
        else:  # json_string
            return json.dumps(arr)
    
    def _auto_extract_records(self, data: Any) -> List[Dict[str, Any]]:
        """
        Auto-extract records from nested JSON structure
        
        Args:
            data: JSON data to extract from
            
        Returns:
            List of extracted records
        """
        records = []
        
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            # Look for arrays in the dictionary
            for key, value in data.items():
                if isinstance(value, list) and value:
                    # Check if it's an array of objects
                    if isinstance(value[0], dict):
                        records.extend(value)
                    else:
                        # Convert array items to records
                        for i, item in enumerate(value):
                            records.append({key: item, '_index': i})
            
            # If no arrays found, treat the object itself as a record
            if not records:
                records = [data]
        
        return records
    
    def _extract_by_path(self, data: Any, json_path: str) -> List[Dict[str, Any]]:
        """
        Extract data using JSONPath-like syntax
        
        Args:
            data: JSON data
            json_path: Path to extract (simplified JSONPath)
            
        Returns:
            List of extracted records
        """
        try:
            # Simple JSONPath implementation
            keys = json_path.split('.')
            current_data = data
            
            for key in keys:
                if isinstance(current_data, dict):
                    current_data = current_data.get(key, [])
                elif isinstance(current_data, list) and key.isdigit():
                    index = int(key)
                    current_data = current_data[index] if index < len(current_data) else []
            
            if isinstance(current_data, list):
                return current_data
            else:
                return [current_data] if current_data else []
                
        except Exception as e:
            self.logger.error(f"Error extracting by path '{json_path}': {str(e)}")
            return []
    
    async def _analyze_json_schema(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze JSON schema from sample records
        
        Args:
            records: Sample records to analyze
            
        Returns:
            Schema analysis results
        """
        schema = defaultdict(lambda: {
            'field_type': 'primitive',
            'detected_type': 'STRING',
            'sample_values': [],
            'unique_values': set(),
            'null_count': 0,
            'total_count': 0
        })
        
        for record in records:
            self._analyze_record_schema(record, schema, '')
        
        # Convert sets to lists and finalize analysis
        for field_path, field_info in schema.items():
            field_info['unique_values'] = list(field_info['unique_values'])
            field_info['sample_values'] = list(field_info['sample_values'])[:10]
            
            # Detect final data type
            if field_info['sample_values']:
                field_info['detected_type'] = self._detect_data_type(field_info['sample_values'])
            
            # Calculate string lengths for string fields
            if field_info['detected_type'] == 'STRING':
                lengths = [len(str(val)) for val in field_info['sample_values'] if val]
                if lengths:
                    field_info['min_length'] = min(lengths)
                    field_info['max_length'] = max(lengths)
        
        return dict(schema)
    
    def _analyze_record_schema(self, record: Any, schema: Dict, prefix: str):
        """
        Recursively analyze record schema
        
        Args:
            record: Record to analyze
            schema: Schema dictionary to update
            prefix: Field prefix for nested objects
        """
        if isinstance(record, dict):
            for key, value in record.items():
                field_path = f"{prefix}.{key}" if prefix else key
                
                if value is None:
                    schema[field_path]['null_count'] += 1
                elif isinstance(value, dict):
                    schema[field_path]['field_type'] = 'object'
                    self._analyze_record_schema(value, schema, field_path)
                elif isinstance(value, list):
                    schema[field_path]['field_type'] = 'array'
                    schema[field_path]['sample_values'].append(str(value)[:100])
                else:
                    schema[field_path]['field_type'] = 'primitive'
                    schema[field_path]['sample_values'].append(value)
                    schema[field_path]['unique_values'].add(value)
                
                schema[field_path]['total_count'] += 1
    
    def _get_max_nesting_level(self, records: List[Dict[str, Any]]) -> int:
        """
        Get maximum nesting level in records
        
        Args:
            records: List of records to analyze
            
        Returns:
            Maximum nesting level
        """
        max_level = 0
        for record in records:
            for key in record.keys():
                level = key.count('.')
                max_level = max(max_level, level)
        return max_level
    
    async def _custom_record_validation(self, record: Dict[str, Any], row_number: int) -> Dict[str, Any]:
        """
        JSON-specific record validation
        
        Args:
            record: Record to validate
            row_number: Row number for error reporting
            
        Returns:
            Validation result
        """
        validation_result = {"is_valid": True, "errors": []}
        
        try:
            # Check for extremely deep nesting
            max_depth = self._calculate_nesting_depth(record)
            if max_depth > self.max_depth:
                validation_result["errors"].append(f"Nesting depth ({max_depth}) exceeds maximum ({self.max_depth})")
            
            # Check for circular references in JSON strings
            for field_name, value in record.items():
                if isinstance(value, str) and len(value) > 1000:
                    try:
                        json.loads(value)  # Check if it's valid JSON
                    except json.JSONDecodeError:
                        pass  # It's just a long string, not JSON
            
            # Check for empty objects after flattening
            non_empty_values = [v for v in record.values() if v is not None and v != "" and v != {} and v != []]
            if not non_empty_values:
                validation_result["is_valid"] = False
                validation_result["errors"].append("Record contains only empty values")
            
        except Exception as e:
            validation_result["is_valid"] = False
            validation_result["errors"].append(f"Validation error: {str(e)}")
        
        return validation_result
    
    def _calculate_nesting_depth(self, obj: Any, depth: int = 0) -> int:
        """
        Calculate maximum nesting depth of an object
        
        Args:
            obj: Object to analyze
            depth: Current depth
            
        Returns:
            Maximum nesting depth
        """
        if isinstance(obj, dict):
            return max([self._calculate_nesting_depth(v, depth + 1) for v in obj.values()] + [depth])
        elif isinstance(obj, list) and obj:
            return max([self._calculate_nesting_depth(item, depth + 1) for item in obj] + [depth])
        else:
            return depth