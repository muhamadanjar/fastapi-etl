# ==============================================
# app/processors/xml_processor.py
# ==============================================
import xml.etree.ElementTree as ET
from xml.dom import minidom
import xmltodict
from typing import Dict, List, Any, Optional, Tuple, Iterator
from pathlib import Path
from datetime import datetime
import json
import re
from collections import defaultdict

from .base_processor import BaseProcessor
from app.core.exceptions import FileProcessingException
from app.infrastructure.db.models.raw_data.file_registry import FileRegistry
from app.utils.logger import get_logger

logger = get_logger(__name__)

class XMLProcessor(BaseProcessor):
    """
    XML file processor supporting various XML structures:
    - Simple XML with repeating elements
    - Nested XML hierarchies
    - XML with attributes
    - XML with mixed content
    - Large XML files with streaming processing
    """
    
    def __init__(self, db_session, batch_id: Optional[str] = None, **kwargs):
        """
        Initialize XML processor
        
        Args:
            db_session: Database session
            batch_id: Optional batch ID
            **kwargs: Additional configuration options
        """
        super().__init__(db_session, batch_id)
        
        # XML-specific configuration
        self.record_xpath = kwargs.get('record_xpath', None)  # XPath for record elements
        self.namespace_map = kwargs.get('namespace_map', {})  # Namespace prefix mapping
        self.flatten_attributes = kwargs.get('flatten_attributes', True)  # Include attributes in flattened structure
        self.flatten_nested = kwargs.get('flatten_nested', True)  # Flatten nested elements
        self.preserve_text_content = kwargs.get('preserve_text_content', True)  # Preserve text content of elements
        self.chunk_size = kwargs.get('chunk_size', 5000)  # Records per chunk
        self.encoding = kwargs.get('encoding', 'utf-8')  # File encoding
        self.validate_xml = kwargs.get('validate_xml', True)  # Validate XML structure
        self.strip_whitespace = kwargs.get('strip_whitespace', True)  # Strip whitespace from text content
        
        # XML parsing options
        self.ignore_comments = kwargs.get('ignore_comments', True)
        self.ignore_processing_instructions = kwargs.get('ignore_processing_instructions', True)
        self.handle_mixed_content = kwargs.get('handle_mixed_content', True)
        
    async def validate_file_format(self, file_path: str) -> Tuple[bool, str]:
        """
        Validate if file is valid XML format
        
        Args:
            file_path: Path to XML file
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            file_path_obj = Path(file_path)
            
            # Check file exists
            if not file_path_obj.exists():
                return False, "File does not exist"
            
            # Check file extension
            valid_extensions = ['.xml', '.xsd', '.xsl', '.xslt', '.rss', '.atom', '.svg']
            if file_path_obj.suffix.lower() not in valid_extensions:
                return False, f"Invalid file extension: {file_path_obj.suffix}. Expected: {valid_extensions}"
            
            # Check file size
            file_size = file_path_obj.stat().st_size
            if file_size == 0:
                return False, "File is empty"
            
            if file_size > 1024 * 1024 * 1024:  # 1GB limit
                return False, "File too large (>1GB)"
            
            # Try to parse XML
            try:
                tree = ET.parse(file_path)
                root = tree.getroot()
                
                # Check if root element exists
                if root is None:
                    return False, "No root element found"
                
                # Detect XML structure
                structure_info = await self._analyze_xml_structure(file_path)
                
                if structure_info['record_count'] == 0:
                    return False, "No data records found in XML"
                
                return True, f"Valid XML file ({structure_info['structure_type']})"
                
            except ET.ParseError as e:
                return False, f"Invalid XML format: {str(e)}"
            
        except Exception as e:
            return False, f"XML validation error: {str(e)}"
    
    async def detect_structure(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Detect XML structure and analyze data schema
        
        Args:
            file_path: Path to XML file
            
        Returns:
            List of field information dictionaries
        """
        try:
            # Analyze XML structure
            structure_info = await self._analyze_xml_structure(file_path)
            
            # Get sample records for schema analysis
            sample_records = await self._get_sample_records(file_path, 1000)
            
            if not sample_records:
                raise FileProcessingException("No records found for structure analysis")
            
            # Analyze schema from sample records
            schema_info = await self._analyze_xml_schema(sample_records)
            
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
                    "xml_path": field_path,
                    "element_type": field_info['element_type'],  # 'element', 'attribute', 'text'
                    "namespace": field_info.get('namespace')
                }
                
                columns_info.append(column_info)
            
            # Add XML structure metadata
            structure_metadata = {
                "xml_structure_type": structure_info['structure_type'],
                "root_element": structure_info['root_element'],
                "namespaces": structure_info['namespaces'],
                "total_fields": len(columns_info),
                "estimated_records": structure_info['record_count']
            }
            
            # Add metadata as first "column"
            columns_info.insert(0, {
                "name": "_xml_metadata",
                "position": -1,
                "data_type": "METADATA",
                "metadata": structure_metadata
            })
            
            self.logger.info(f"Detected XML structure with {len(columns_info)} fields")
            return columns_info
            
        except Exception as e:
            self.logger.error(f"Error detecting XML structure: {str(e)}")
            raise FileProcessingException(f"Failed to detect XML structure: {str(e)}")
    
    async def preview_data(self, file_path: str, rows: int = 10) -> Dict[str, Any]:
        """
        Generate preview of XML data
        
        Args:
            file_path: Path to XML file
            rows: Number of records to preview
            
        Returns:
            Dictionary containing preview data and metadata
        """
        try:
            # Analyze XML structure
            structure_info = await self._analyze_xml_structure(file_path)
            
            # Get sample records
            sample_records = await self._get_sample_records(file_path, rows)
            
            if not sample_records:
                raise FileProcessingException("No records found for preview")
            
            # Process records for preview
            processed_records = []
            for record in sample_records:
                if self.flatten_nested:
                    flattened = self._flatten_xml_record(record)
                    processed_records.append(flattened)
                else:
                    processed_records.append(record)
            
            # Extract field names
            all_fields = set()
            for record in processed_records:
                all_fields.update(record.keys())
            
            preview_data = {
                "columns": sorted(list(all_fields)),
                "data": processed_records,
                "metadata": {
                    "total_records_estimated": structure_info['record_count'],
                    "preview_records": len(processed_records),
                    "total_fields": len(all_fields),
                    "file_size": self._get_file_size(file_path),
                    "xml_structure_type": structure_info['structure_type'],
                    "root_element": structure_info['root_element'],
                    "namespaces": structure_info['namespaces'],
                    "encoding": self.encoding,
                    "flatten_nested": self.flatten_nested,
                    "flatten_attributes": self.flatten_attributes
                }
            }
            
            return preview_data
            
        except Exception as e:
            self.logger.error(f"Error generating XML preview: {str(e)}")
            raise FileProcessingException(f"Failed to generate XML preview: {str(e)}")
    
    async def process_file(self, file_path: str, file_registry: FileRegistry) -> Dict[str, Any]:
        """
        Process XML file and extract all data
        
        Args:
            file_path: Path to XML file
            file_registry: File registry record
            
        Returns:
            Processing results and statistics
        """
        try:
            self.logger.info(f"Starting XML processing for file: {file_path}")
            
            # Analyze XML structure
            structure_info = await self._analyze_xml_structure(file_path)
            
            # Update file metadata
            metadata = file_registry.metadata or {}
            metadata.update({
                "xml_structure_type": structure_info['structure_type'],
                "root_element": structure_info['root_element'],
                "namespaces": structure_info['namespaces'],
                "encoding": self.encoding,
                "flatten_nested": self.flatten_nested,
                "flatten_attributes": self.flatten_attributes,
                "estimated_records": structure_info['record_count'],
                "processor": "XMLProcessor"
            })
            file_registry.metadata = metadata
            self.db_session.add(file_registry)
            
            # Detect and save structure
            structure_columns = await self.detect_structure(file_path)
            await self.save_column_structure(file_registry, structure_columns)
            
            # Process records
            record_iterator = self._read_xml_records(file_path, structure_info)
            processing_stats = await self.process_records(record_iterator, file_registry)
            
            # Update processing statistics
            processing_stats.update({
                "file_type": "XML",
                "xml_structure_type": structure_info['structure_type'],
                "root_element": structure_info['root_element'],
                "encoding": self.encoding,
                "fields_detected": len(structure_columns)
            })
            
            self.logger.info(f"XML processing completed: {processing_stats}")
            return processing_stats
            
        except Exception as e:
            self.logger.error(f"Error processing XML file: {str(e)}")
            raise FileProcessingException(f"Failed to process XML file: {str(e)}")
    
    async def _analyze_xml_structure(self, file_path: str) -> Dict[str, Any]:
        """
        Analyze XML file structure to determine record patterns
        
        Args:
            file_path: Path to XML file
            
        Returns:
            Dictionary with structure analysis results
        """
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Extract namespace information
            namespaces = self._extract_namespaces(root)
            
            # Analyze element structure
            element_counts = defaultdict(int)
            max_depth = 0
            
            def analyze_element(elem, depth=0):
                nonlocal max_depth
                max_depth = max(max_depth, depth)
                
                # Count elements at each level
                element_counts[f"{depth}:{elem.tag}"] += 1
                
                for child in elem:
                    analyze_element(child, depth + 1)
            
            analyze_element(root)
            
            # Determine structure type and record pattern
            structure_type = "single_record"
            record_xpath = None
            record_count = 1
            
            # Look for repeating patterns that might indicate records
            level_1_elements = {tag.split(':', 1)[1]: count for tag, count in element_counts.items() 
                              if tag.startswith('1:') and count > 1}
            
            if level_1_elements:
                # Multiple elements at level 1 - likely array structure
                most_common_element = max(level_1_elements, key=level_1_elements.get)
                structure_type = "array_of_records"
                record_xpath = f".//{most_common_element}"
                record_count = level_1_elements[most_common_element]
            else:
                # Look for deeper repeating patterns
                for level in range(2, max_depth + 1):
                    level_elements = {tag.split(':', 1)[1]: count for tag, count in element_counts.items() 
                                    if tag.startswith(f'{level}:') and count > 1}
                    
                    if level_elements:
                        most_common_element = max(level_elements, key=level_elements.get)
                        structure_type = "nested_records"
                        record_xpath = f".//{most_common_element}"
                        record_count = level_elements[most_common_element]
                        break
            
            # If user provided record_xpath, use it
            if self.record_xpath:
                record_xpath = self.record_xpath
                # Count elements matching the xpath
                matching_elements = root.findall(record_xpath, namespaces)
                record_count = len(matching_elements)
                structure_type = "custom_xpath"
            
            return {
                "structure_type": structure_type,
                "root_element": root.tag,
                "namespaces": namespaces,
                "record_xpath": record_xpath,
                "record_count": record_count,
                "max_depth": max_depth,
                "element_counts": dict(element_counts)
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing XML structure: {str(e)}")
            raise FileProcessingException(f"Failed to analyze XML structure: {str(e)}")
    
    async def _get_sample_records(self, file_path: str, max_records: int) -> List[Dict[str, Any]]:
        """
        Get sample records from XML file for analysis
        
        Args:
            file_path: Path to XML file
            max_records: Maximum number of records to sample
            
        Returns:
            List of sample records
        """
        try:
            structure_info = await self._analyze_xml_structure(file_path)
            
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            sample_records = []
            
            if structure_info['record_xpath']:
                # Use xpath to find record elements
                record_elements = root.findall(structure_info['record_xpath'], structure_info['namespaces'])
                
                for i, element in enumerate(record_elements[:max_records]):
                    record = self._element_to_dict(element)
                    if self.flatten_nested:
                        record = self._flatten_xml_record(record)
                    sample_records.append(record)
            else:
                # Treat root as single record
                record = self._element_to_dict(root)
                if self.flatten_nested:
                    record = self._flatten_xml_record(record)
                sample_records.append(record)
            
            return sample_records
            
        except Exception as e:
            self.logger.error(f"Error getting sample records: {str(e)}")
            return []
    
    def _read_xml_records(self, file_path: str, structure_info: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        Read XML records in chunks for processing
        
        Args:
            file_path: Path to XML file
            structure_info: XML structure analysis results
            
        Yields:
            Dictionary records from XML
        """
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            if structure_info['record_xpath']:
                # Process records using xpath
                record_elements = root.findall(structure_info['record_xpath'], structure_info['namespaces'])
                
                chunk = []
                for element in record_elements:
                    record = self._element_to_dict(element)
                    
                    if self.flatten_nested:
                        record = self._flatten_xml_record(record)
                    
                    chunk.append(record)
                    
                    if len(chunk) >= self.chunk_size:
                        for item in chunk:
                            yield item
                        chunk = []
                
                # Yield remaining items
                for item in chunk:
                    yield item
            else:
                # Single record (root element)
                record = self._element_to_dict(root)
                if self.flatten_nested:
                    record = self._flatten_xml_record(record)
                yield record
                
        except Exception as e:
            self.logger.error(f"Error reading XML records: {str(e)}")
            raise FileProcessingException(f"Failed to read XML file: {str(e)}")
    
    def _element_to_dict(self, element: ET.Element) -> Dict[str, Any]:
        """
        Convert XML element to dictionary
        
        Args:
            element: XML element to convert
            
        Returns:
            Dictionary representation of element
        """
        result = {}
        
        # Add attributes if configured
        if self.flatten_attributes and element.attrib:
            for attr_name, attr_value in element.attrib.items():
                result[f"@{attr_name}"] = attr_value
        
        # Handle text content
        text_content = element.text
        if text_content and self.strip_whitespace:
            text_content = text_content.strip()
        
        # Handle child elements
        children = list(element)
        
        if not children:
            # Leaf element - return text content
            if text_content:
                return text_content
            else:
                return None
        
        # Add text content if it exists alongside children
        if text_content and self.preserve_text_content:
            result['_text'] = text_content
        
        # Process child elements
        for child in children:
            child_data = self._element_to_dict(child)
            child_tag = self._clean_tag_name(child.tag)
            
            if child_tag in result:
                # Multiple elements with same tag - convert to array
                if not isinstance(result[child_tag], list):
                    result[child_tag] = [result[child_tag]]
                result[child_tag].append(child_data)
            else:
                result[child_tag] = child_data
        
        return result
    
    def _flatten_xml_record(self, record: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
        """
        Flatten nested XML record
        
        Args:
            record: XML record to flatten
            parent_key: Parent key for nested elements
            sep: Separator for nested keys
            
        Returns:
            Flattened dictionary
        """
        items = []
        
        if isinstance(record, dict):
            for k, v in record.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                
                if isinstance(v, dict) and len(str(v)) < 1000:  # Avoid very deep nesting
                    items.extend(self._flatten_xml_record(v, new_key, sep=sep).items())
                elif isinstance(v, list):
                    # Handle arrays in XML
                    if len(v) == 1 and not isinstance(v[0], (dict, list)):
                        # Single item array - flatten to single value
                        items.append((new_key, v[0]))
                    else:
                        # Multiple items or complex items - convert to JSON
                        items.append((new_key, json.dumps(v)))
                else:
                    items.append((new_key, v))
        else:
            return record
        
        return dict(items)
    
    def _extract_namespaces(self, root: ET.Element) -> Dict[str, str]:
        """
        Extract namespace declarations from XML
        
        Args:
            root: Root XML element
            
        Returns:
            Dictionary of namespace prefixes and URIs
        """
        namespaces = {}
        
        # Extract from root element
        for prefix, uri in root.attrib.items():
            if prefix.startswith('xmlns'):
                if prefix == 'xmlns':
                    namespaces[''] = uri  # Default namespace
                else:
                    namespaces[prefix[6:]] = uri  # Remove 'xmlns:' prefix
        
        # Add user-defined namespace mappings
        namespaces.update(self.namespace_map)
        
        return namespaces
    
    def _clean_tag_name(self, tag: str) -> str:
        """
        Clean XML tag name (remove namespace prefix)
        
        Args:
            tag: XML tag name
            
        Returns:
            Cleaned tag name
        """
        # Remove namespace prefix if present
        if '}' in tag:
            return tag.split('}', 1)[1]
        return tag
    
    async def _analyze_xml_schema(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze XML schema from sample records
        
        Args:
            records: Sample records to analyze
            
        Returns:
            Schema analysis results
        """
        schema = defaultdict(lambda: {
            'element_type': 'element',
            'detected_type': 'STRING',
            'sample_values': [],
            'unique_values': set(),
            'null_count': 0,
            'total_count': 0,
            'namespace': None
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
            
            # Determine element type
            if field_path.startswith('@'):
                field_info['element_type'] = 'attribute'
            elif field_path.endswith('_text'):
                field_info['element_type'] = 'text'
            else:
                field_info['element_type'] = 'element'
        
        return dict(schema)
    
    def _analyze_record_schema(self, record: Any, schema: Dict, prefix: str):
        """
        Recursively analyze record schema
        
        Args:
            record: Record to analyze
            schema: Schema dictionary to update
            prefix: Field prefix for nested elements
        """
        if isinstance(record, dict):
            for key, value in record.items():
                field_path = f"{prefix}.{key}" if prefix else key
                
                if value is None:
                    schema[field_path]['null_count'] += 1
                elif isinstance(value, dict):
                    self._analyze_record_schema(value, schema, field_path)
                elif isinstance(value, list):
                    schema[field_path]['sample_values'].append(str(value)[:100])
                else:
                    schema[field_path]['sample_values'].append(value)
                    schema[field_path]['unique_values'].add(value)
                
                schema[field_path]['total_count'] += 1
        elif isinstance(record, list):
            for i, item in enumerate(record):
                item_path = f"{prefix}[{i}]" if prefix else f"[{i}]"
                self._analyze_record_schema(item, schema, item_path)
    
    def _detect_xml_encoding(self, file_path: str) -> str:
        """
        Detect XML file encoding from XML declaration
        
        Args:
            file_path: Path to XML file
            
        Returns:
            Detected encoding or default
        """
        try:
            with open(file_path, 'rb') as f:
                first_line = f.readline()
                
            # Look for XML declaration
            if first_line.startswith(b'<?xml'):
                first_line_str = first_line.decode('utf-8', errors='ignore')
                
                # Extract encoding from XML declaration
                encoding_match = re.search(r'encoding\s*=\s*["\']([^"\']+)["\']', first_line_str)
                if encoding_match:
                    return encoding_match.group(1)
            
            return self.encoding
            
        except Exception:
            return self.encoding
    
    async def _custom_record_validation(self, record: Dict[str, Any], row_number: int) -> Dict[str, Any]:
        """
        XML-specific record validation
        
        Args:
            record: Record to validate
            row_number: Row number for error reporting
            
        Returns:
            Validation result
        """
        validation_result = {"is_valid": True, "errors": []}
        
        try:
            # Check for empty XML records
            non_empty_values = [v for v in record.values() if v is not None and v != "" and v != {} and v != []]
            if not non_empty_values:
                validation_result["is_valid"] = False
                validation_result["errors"].append("Record contains only empty values")
            
            # Check for XML-specific issues
            for field_name, value in record.items():
                if isinstance(value, str):
                    # Check for unescaped XML characters
                    if any(char in value for char in ['<', '>', '&']) and not self._is_valid_xml_content(value):
                        validation_result["errors"].append(f"Field '{field_name}' contains unescaped XML characters")
                    
                    # Check for extremely long text content
                    if len(value) > 50000:  # 50KB limit
                        validation_result["errors"].append(f"Field '{field_name}' exceeds maximum text length")
            
            # Check for attribute naming conflicts
            element_names = {k for k in record.keys() if not k.startswith('@')}
            attribute_names = {k[1:] for k in record.keys() if k.startswith('@')}
            
            conflicts = element_names & attribute_names
            if conflicts:
                validation_result["errors"].append(f"Attribute/element naming conflicts: {conflicts}")
            
        except Exception as e:
            validation_result["is_valid"] = False
            validation_result["errors"].append(f"Validation error: {str(e)}")
        
        return validation_result
    
    def _is_valid_xml_content(self, content: str) -> bool:
        """
        Check if string content is valid XML (properly escaped)
        
        Args:
            content: String content to check
            
        Returns:
            True if content is valid XML
        """
        try:
            # Try to parse as XML fragment
            ET.fromstring(f"<root>{content}</root>")
            return True
        except ET.ParseError:
            return False
        