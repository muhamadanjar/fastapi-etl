# ==============================================
# app/processors/api_processor.py
# ==============================================
import asyncio
import aiohttp
import json
import time
from typing import Dict, List, Any, Optional, Tuple, Iterator, Union
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
import base64
from dataclasses import dataclass
import xml.etree.ElementTree as ET

from .base_processor import BaseProcessor
from app.core.exceptions import FileProcessingException
from app.infrastructure.db.models.raw_data.file_registry import FileRegistry
from app.utils.logger import get_logger

logger = get_logger(__name__)

@dataclass
class APIConfig:
    """Configuration for API data source"""
    base_url: str
    endpoint: str
    method: str = "GET"
    headers: Dict[str, str] = None
    auth_type: str = None  # 'basic', 'bearer', 'api_key', 'oauth2'
    auth_credentials: Dict[str, str] = None
    pagination_type: str = None  # 'offset', 'cursor', 'page', 'link_header'
    pagination_params: Dict[str, Any] = None
    rate_limit: int = 100  # requests per minute
    timeout: int = 30
    retry_attempts: int = 3
    retry_delay: int = 1

class APIProcessor(BaseProcessor):
    """
    API data processor supporting various REST APIs with different authentication,
    pagination, and response formats (JSON, XML)
    """
    
    def __init__(self, db_session, batch_id: Optional[str] = None, **kwargs):
        """
        Initialize API processor
        
        Args:
            db_session: Database session
            batch_id: Optional batch ID
            **kwargs: Additional configuration options including API config
        """
        super().__init__(db_session, batch_id)
        
        # API-specific configuration
        self.api_config = self._parse_api_config(kwargs)
        self.session = None
        self.requests_made = 0
        self.rate_limit_window_start = time.time()
        self.chunk_size = kwargs.get('chunk_size', 1000)
        
        # Response processing configuration
        self.response_format = kwargs.get('response_format', 'json')  # 'json', 'xml'
        self.data_path = kwargs.get('data_path', None)  # JSONPath or XPath for data extraction
        self.flatten_response = kwargs.get('flatten_response', True)
        self.include_metadata = kwargs.get('include_metadata', True)
        
    def _parse_api_config(self, kwargs: Dict[str, Any]) -> APIConfig:
        """Parse API configuration from kwargs"""
        return APIConfig(
            base_url=kwargs.get('base_url', ''),
            endpoint=kwargs.get('endpoint', ''),
            method=kwargs.get('method', 'GET'),
            headers=kwargs.get('headers', {}),
            auth_type=kwargs.get('auth_type'),
            auth_credentials=kwargs.get('auth_credentials', {}),
            pagination_type=kwargs.get('pagination_type'),
            pagination_params=kwargs.get('pagination_params', {}),
            rate_limit=kwargs.get('rate_limit', 100),
            timeout=kwargs.get('timeout', 30),
            retry_attempts=kwargs.get('retry_attempts', 3),
            retry_delay=kwargs.get('retry_delay', 1)
        )
    
    async def validate_file_format(self, file_path: str) -> Tuple[bool, str]:
        """
        Validate API configuration and connectivity
        Note: file_path contains API configuration JSON for API sources
        
        Args:
            file_path: Path to API configuration file
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # For API processor, file_path contains API configuration
            if file_path.endswith('.json'):
                # Load API config from file
                with open(file_path, 'r') as f:
                    api_config_data = json.load(f)
                self.api_config = self._parse_api_config(api_config_data)
            
            # Validate required API configuration
            if not self.api_config.base_url:
                return False, "Base URL is required"
            
            if not self.api_config.endpoint:
                return False, "API endpoint is required"
            
            # Test API connectivity
            try:
                test_result = await self._test_api_connectivity()
                if not test_result['success']:
                    return False, f"API connectivity test failed: {test_result['error']}"
                
                return True, f"API connection successful ({test_result['status_code']})"
                
            except Exception as e:
                return False, f"API connection failed: {str(e)}"
            
        except Exception as e:
            return False, f"API validation error: {str(e)}"
    
    async def detect_structure(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Detect API response structure by making sample requests
        
        Args:
            file_path: Path to API configuration file
            
        Returns:
            List of field information dictionaries
        """
        try:
            # Make sample request to analyze response structure
            sample_data = await self._get_sample_data(limit=100)
            
            if not sample_data:
                raise FileProcessingException("No sample data retrieved from API")
            
            # Analyze response structure
            structure_info = await self._analyze_response_structure(sample_data)
            
            self.logger.info(f"Detected API response structure with {len(structure_info)} fields")
            return structure_info
            
        except Exception as e:
            self.logger.error(f"Error detecting API structure: {str(e)}")
            raise FileProcessingException(f"Failed to detect API structure: {str(e)}")
    
    async def preview_data(self, file_path: str, rows: int = 10) -> Dict[str, Any]:
        """
        Generate preview of API response data
        
        Args:
            file_path: Path to API configuration file
            rows: Number of records to preview
            
        Returns:
            Dictionary containing preview data and metadata
        """
        try:
            # Get sample data from API
            sample_data = await self._get_sample_data(limit=rows)
            
            if not sample_data:
                raise FileProcessingException("No data retrieved from API")
            
            # Process sample data
            processed_records = []
            for record in sample_data:
                if self.flatten_response:
                    flattened = self._flatten_response_data(record)
                    processed_records.append(flattened)
                else:
                    processed_records.append(record)
            
            # Estimate total records if pagination is supported
            total_estimate = await self._estimate_total_records()
            
            preview_data = {
                "data": processed_records,
                "metadata": {
                    "api_endpoint": urljoin(self.api_config.base_url, self.api_config.endpoint),
                    "response_format": self.response_format,
                    "preview_records": len(processed_records),
                    "estimated_total_records": total_estimate,
                    "pagination_supported": self.api_config.pagination_type is not None,
                    "auth_type": self.api_config.auth_type,
                    "rate_limit": self.api_config.rate_limit,
                    "last_request_time": datetime.utcnow().isoformat()
                }
            }
            
            return preview_data
            
        except Exception as e:
            self.logger.error(f"Error generating API preview: {str(e)}")
            raise FileProcessingException(f"Failed to generate API preview: {str(e)}")
    
    async def process_file(self, file_path: str, file_registry: FileRegistry) -> Dict[str, Any]:
        """
        Process API data source and extract all available data
        
        Args:
            file_path: Path to API configuration file
            file_registry: File registry record
            
        Returns:
            Processing results and statistics
        """
        try:
            self.logger.info(f"Starting API data processing from: {self.api_config.base_url}/{self.api_config.endpoint}")
            
            # Update file metadata
            metadata = file_registry.file_metadata or {}
            metadata.update({
                "api_endpoint": urljoin(self.api_config.base_url, self.api_config.endpoint),
                "response_format": self.response_format,
                "auth_type": self.api_config.auth_type,
                "pagination_type": self.api_config.pagination_type,
                "rate_limit": self.api_config.rate_limit,
                "processor": "APIProcessor"
            })
            file_registry.file_metadata = metadata
            self.db_session.add(file_registry)
            
            # Detect and save structure
            structure_info = await self.detect_structure(file_path)
            await self.save_column_structure(file_registry, structure_info)
            
            # Process all data from API
            record_iterator = self._fetch_all_data()
            processing_stats = await self.process_records(record_iterator, file_registry)
            
            # Update processing statistics
            processing_stats.update({
                "file_type": "API",
                "api_endpoint": urljoin(self.api_config.base_url, self.api_config.endpoint),
                "response_format": self.response_format,
                "requests_made": self.requests_made,
                "fields_detected": len(structure_info)
            })
            
            self.logger.info(f"API processing completed: {processing_stats}")
            return processing_stats
            
        except Exception as e:
            self.logger.error(f"Error processing API data: {str(e)}")
            raise FileProcessingException(f"Failed to process API data: {str(e)}")
        finally:
            await self._close_session()
    
    async def _test_api_connectivity(self) -> Dict[str, Any]:
        """Test API connectivity and authentication"""
        try:
            await self._ensure_session()
            
            url = urljoin(self.api_config.base_url, self.api_config.endpoint)
            headers = await self._get_auth_headers()
            
            # Make a simple request with minimal parameters
            test_params = {}
            if self.api_config.pagination_type == 'offset':
                test_params = {'limit': 1, 'offset': 0}
            elif self.api_config.pagination_type == 'page':
                test_params = {'page': 1, 'per_page': 1}
            
            async with self.session.request(
                method=self.api_config.method,
                url=url,
                headers=headers,
                params=test_params,
                timeout=aiohttp.ClientTimeout(total=self.api_config.timeout)
            ) as response:
                return {
                    'success': response.status < 400,
                    'status_code': response.status,
                    'content_type': response.headers.get('content-type', '')
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'status_code': None
            }
    
    async def _get_sample_data(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get sample data from API for analysis"""
        try:
            await self._ensure_session()
            
            url = urljoin(self.api_config.base_url, self.api_config.endpoint)
            headers = await self._get_auth_headers()
            
            # Build request parameters
            params = {}
            if self.api_config.pagination_type == 'offset':
                params = {'limit': limit, 'offset': 0}
            elif self.api_config.pagination_type == 'page':
                params = {'page': 1, 'per_page': limit}
            elif self.api_config.pagination_params:
                params.update(self.api_config.pagination_params)
            
            # Make request with rate limiting
            await self._enforce_rate_limit()
            
            async with self.session.request(
                method=self.api_config.method,
                url=url,
                headers=headers,
                params=params,
                timeout=aiohttp.ClientTimeout(total=self.api_config.timeout)
            ) as response:
                
                if response.status >= 400:
                    raise FileProcessingException(f"API request failed: {response.status} - {await response.text()}")
                
                # Parse response based on format
                if self.response_format == 'json':
                    data = await response.json()
                elif self.response_format == 'xml':
                    text = await response.text()
                    data = self._parse_xml_response(text)
                else:
                    raise FileProcessingException(f"Unsupported response format: {self.response_format}")
                
                # Extract records from response
                records = self._extract_records_from_response(data)
                self.requests_made += 1
                
                return records
                
        except Exception as e:
            self.logger.error(f"Error getting sample data: {str(e)}")
            return []
    
    async def _fetch_all_data(self) -> Iterator[Dict[str, Any]]:
        """Fetch all data from API with pagination support"""
        try:
            await self._ensure_session()
            
            url = urljoin(self.api_config.base_url, self.api_config.endpoint)
            headers = await self._get_auth_headers()
            
            # Initialize pagination
            if self.api_config.pagination_type == 'offset':
                offset = 0
                limit = self.chunk_size
                has_more = True
                
                while has_more:
                    params = {'limit': limit, 'offset': offset}
                    records, has_more = await self._fetch_page(url, headers, params)
                    
                    for record in records:
                        yield record
                    
                    offset += limit
                    
            elif self.api_config.pagination_type == 'page':
                page = 1
                per_page = self.chunk_size
                has_more = True
                
                while has_more:
                    params = {'page': page, 'per_page': per_page}
                    records, has_more = await self._fetch_page(url, headers, params)
                    
                    for record in records:
                        yield record
                    
                    page += 1
                    
            elif self.api_config.pagination_type == 'cursor':
                cursor = None
                has_more = True
                
                while has_more:
                    params = {'limit': self.chunk_size}
                    if cursor:
                        params['cursor'] = cursor
                    
                    records, has_more, cursor = await self._fetch_cursor_page(url, headers, params)
                    
                    for record in records:
                        yield record
                        
            else:
                # No pagination - single request
                params = self.api_config.pagination_params or {}
                records, _ = await self._fetch_page(url, headers, params)
                
                for record in records:
                    yield record
                    
        except Exception as e:
            self.logger.error(f"Error fetching API data: {str(e)}")
            raise FileProcessingException(f"Failed to fetch API data: {str(e)}")
    
    async def _fetch_page(self, url: str, headers: Dict[str, str], params: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], bool]:
        """Fetch a single page of data"""
        await self._enforce_rate_limit()
        
        for attempt in range(self.api_config.retry_attempts):
            try:
                async with self.session.request(
                    method=self.api_config.method,
                    url=url,
                    headers=headers,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=self.api_config.timeout)
                ) as response:
                    
                    if response.status >= 400:
                        if response.status == 429:  # Rate limited
                            await asyncio.sleep(self.api_config.retry_delay * (attempt + 1))
                            continue
                        raise FileProcessingException(f"API request failed: {response.status}")
                    
                    # Parse response
                    if self.response_format == 'json':
                        data = await response.json()
                    elif self.response_format == 'xml':
                        text = await response.text()
                        data = self._parse_xml_response(text)
                    
                    records = self._extract_records_from_response(data)
                    self.requests_made += 1
                    
                    # Determine if there are more pages
                    has_more = self._has_more_pages(data, records)
                    
                    return records, has_more
                    
            except asyncio.TimeoutError:
                if attempt == self.api_config.retry_attempts - 1:
                    raise FileProcessingException("API request timeout")
                await asyncio.sleep(self.api_config.retry_delay * (attempt + 1))
            except Exception as e:
                if attempt == self.api_config.retry_attempts - 1:
                    raise
                await asyncio.sleep(self.api_config.retry_delay * (attempt + 1))
        
        return [], False
    
    async def _fetch_cursor_page(self, url: str, headers: Dict[str, str], params: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], bool, Optional[str]]:
        """Fetch a single page using cursor pagination"""
        records, has_more = await self._fetch_page(url, headers, params)
        
        # Extract next cursor from last record or response metadata
        next_cursor = None
        if records and has_more:
            # This would need to be customized based on API structure
            last_record = records[-1]
            next_cursor = last_record.get('id') or last_record.get('cursor')
        
        return records, has_more, next_cursor
    
    def _extract_records_from_response(self, data: Any) -> List[Dict[str, Any]]:
        """Extract records from API response"""
        if self.data_path:
            # Use data path to extract records
            records = self._extract_by_path(data, self.data_path)
        else:
            # Auto-detect records structure
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                # Look for common array field names
                possible_keys = ['data', 'results', 'items', 'records', 'response', 'content']
                records = []
                
                for key in possible_keys:
                    if key in data and isinstance(data[key], list):
                        records = data[key]
                        break
                
                if not records:
                    # If no array found, treat the object as a single record
                    records = [data]
            else:
                records = [data]
        
        # Process each record
        processed_records = []
        for record in records:
            if self.flatten_response and isinstance(record, dict):
                record = self._flatten_response_data(record)
            
            # Add API metadata if requested
            if self.include_metadata:
                record['_api_source'] = urljoin(self.api_config.base_url, self.api_config.endpoint)
                record['_fetch_timestamp'] = datetime.utcnow().isoformat()
            
            processed_records.append(record)
        
        return processed_records
    
    def _has_more_pages(self, response_data: Any, records: List[Dict[str, Any]]) -> bool:
        """Determine if there are more pages available"""
        if not records:
            return False
        
        # Check common pagination indicators
        if isinstance(response_data, dict):
            # Check for pagination metadata
            pagination_keys = ['has_more', 'has_next', 'next_page', 'total_pages', 'total_count']
            for key in pagination_keys:
                if key in response_data:
                    value = response_data[key]
                    if key in ['has_more', 'has_next']:
                        return bool(value)
                    elif key == 'next_page':
                        return value is not None
            
            # Check if we got a full page
            if len(records) == self.chunk_size:
                return True
        
        return False
    
    def _flatten_response_data(self, data: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
        """Flatten nested response data"""
        items = []
        
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, dict) and len(str(v)) < 1000:
                items.extend(self._flatten_response_data(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Convert arrays to JSON strings for now
                items.append((new_key, json.dumps(v) if v else None))
            else:
                items.append((new_key, v))
        
        return dict(items)
    
    def _parse_xml_response(self, xml_text: str) -> Dict[str, Any]:
        """Parse XML response to dictionary"""
        try:
            root = ET.fromstring(xml_text)
            return self._xml_to_dict(root)
        except ET.ParseError as e:
            raise FileProcessingException(f"Invalid XML response: {str(e)}")
    
    def _xml_to_dict(self, element) -> Dict[str, Any]:
        """Convert XML element to dictionary"""
        result = {}
        
        # Add attributes
        if element.attrib:
            result.update({f"@{k}": v for k, v in element.attrib.items()})
        
        # Add text content
        if element.text and element.text.strip():
            if len(element) == 0:  # Leaf node
                return element.text.strip()
            result['text'] = element.text.strip()
        
        # Add child elements
        for child in element:
            child_data = self._xml_to_dict(child)
            
            if child.tag in result:
                # Convert to list if multiple elements with same tag
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data
        
        return result
    
    def _extract_by_path(self, data: Any, path: str) -> List[Dict[str, Any]]:
        """Extract data using JSONPath-like syntax"""
        try:
            keys = path.split('.')
            current_data = data
            
            for key in keys:
                if isinstance(current_data, dict):
                    current_data = current_data.get(key, [])
                elif isinstance(current_data, list) and key.isdigit():
                    index = int(key)
                    current_data = current_data[index] if index < len(current_data) else []
            
            return current_data if isinstance(current_data, list) else [current_data]
        except Exception:
            return []
    
    async def _analyze_response_structure(self, sample_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Analyze API response structure"""
        if not sample_data:
            return []
        
        # Use the same schema analysis as JSON processor
        from collections import defaultdict
        
        schema = defaultdict(lambda: {
            'sample_values': [],
            'unique_values': set(),
            'null_count': 0,
            'total_count': 0
        })
        
        for record in sample_data:
            for field_name, value in record.items():
                if value is None:
                    schema[field_name]['null_count'] += 1
                else:
                    schema[field_name]['sample_values'].append(value)
                    schema[field_name]['unique_values'].add(value)
                
                schema[field_name]['total_count'] += 1
        
        # Convert to column info format
        columns_info = []
        for position, (field_name, field_info) in enumerate(schema.items()):
            sample_values = list(field_info['sample_values'])[:10]
            
            column_info = {
                "name": field_name,
                "position": position,
                "data_type": self._detect_data_type(sample_values),
                "sample_values": [str(v) for v in sample_values],
                "null_count": field_info['null_count'],
                "unique_count": len(field_info['unique_values']),
                "total_count": field_info['total_count'],
                "null_percentage": round((field_info['null_count'] / field_info['total_count']) * 100, 2)
            }
            
            columns_info.append(column_info)
        
        return columns_info
    
    async def _estimate_total_records(self) -> Optional[int]:
        """Estimate total number of records available from API"""
        try:
            # Make a request to get metadata about total records
            await self._ensure_session()
            
            url = urljoin(self.api_config.base_url, self.api_config.endpoint)
            headers = await self._get_auth_headers()
            
            params = {'limit': 1}  # Minimal request
            
            async with self.session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Look for total count indicators
                    total_keys = ['total', 'total_count', 'count', 'total_records']
                    for key in total_keys:
                        if key in data:
                            return int(data[key])
                
                return None
                
        except Exception:
            return None
    
    async def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers based on auth type"""
        headers = self.api_config.headers.copy() if self.api_config.headers else {}
        
        if self.api_config.auth_type == 'bearer':
            token = self.api_config.auth_credentials.get('token')
            if token:
                headers['Authorization'] = f'Bearer {token}'
        
        elif self.api_config.auth_type == 'basic':
            username = self.api_config.auth_credentials.get('username')
            password = self.api_config.auth_credentials.get('password')
            if username and password:
                credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
                headers['Authorization'] = f'Basic {credentials}'
        
        elif self.api_config.auth_type == 'api_key':
            api_key = self.api_config.auth_credentials.get('api_key')
            key_header = self.api_config.auth_credentials.get('header', 'X-API-Key')
            if api_key:
                headers[key_header] = api_key
        
        return headers
    
    async def _ensure_session(self):
        """Ensure aiohttp session is available"""
        if self.session is None:
            self.session = aiohttp.ClientSession()
    
    async def _close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None
    
    async def _enforce_rate_limit(self):
        """Enforce rate limiting"""
        current_time = time.time()
        
        # Reset counter if window has passed
        if current_time - self.rate_limit_window_start >= 60:
            self.requests_made = 0
            self.rate_limit_window_start = current_time
        
        # Wait if rate limit exceeded
        if self.requests_made >= self.api_config.rate_limit:
            sleep_time = 60 - (current_time - self.rate_limit_window_start)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
                self.requests_made = 0
                self.rate_limit_window_start = time.time()
    
    async def _custom_record_validation(self, record: Dict[str, Any], row_number: int) -> Dict[str, Any]:
        """API-specific record validation"""
        validation_result = {"is_valid": True, "errors": []}
        
        try:
            # Remove API metadata for validation
            record_copy = {k: v for k, v in record.items() if not k.startswith('_api')}
            
            # Check for empty API responses
            if not record_copy:
                validation_result["is_valid"] = False
                validation_result["errors"].append("Empty API response record")
            
            # Check for API error indicators
            error_indicators = ['error', 'error_message', 'status', 'message']
            for indicator in error_indicators:
                if indicator in record_copy:
                    value = record_copy[indicator]
                    if isinstance(value, str) and 'error' in value.lower():
                        validation_result["errors"].append(f"API error in response: {value}")
            
        except Exception as e:
            validation_result["is_valid"] = False
            validation_result["errors"].append(f"Validation error: {str(e)}")
        
        return validation_result