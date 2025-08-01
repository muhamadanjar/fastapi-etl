# ==============================================
# app/processors/csv_processor.py
# ==============================================
import csv
import io
import chardet
from typing import Dict, List, Any, Optional, Tuple, Iterator
from pathlib import Path
from datetime import datetime
import pandas as pd

from .base_processor import BaseProcessor
from app.core.exceptions import FileProcessingException
from app.infrastructure.db.models.raw_data.file_registry import FileRegistry
from app.utils.logger import get_logger

logger = get_logger(__name__)

class CSVProcessor(BaseProcessor):
    """
    CSV file processor with automatic delimiter detection,
    encoding detection, and robust data parsing
    """
    
    def __init__(self, db_session, batch_id: Optional[str] = None, **kwargs):
        """
        Initialize CSV processor
        
        Args:
            db_session: Database session
            batch_id: Optional batch ID
            **kwargs: Additional configuration options
        """
        super().__init__(db_session, batch_id)
        
        # CSV-specific configuration
        self.delimiter = kwargs.get('delimiter', None)  # Auto-detect if None
        self.encoding = kwargs.get('encoding', None)    # Auto-detect if None
        self.quote_char = kwargs.get('quote_char', '"')
        self.escape_char = kwargs.get('escape_char', None)
        self.skip_initial_space = kwargs.get('skip_initial_space', True)
        self.max_sample_rows = kwargs.get('max_sample_rows', 1000)
        self.chunk_size = kwargs.get('chunk_size', 10000)
        
        # Common delimiters to try for auto-detection
        self.delimiter_candidates = [',', ';', '\t', '|', ':']
        
    async def validate_file_format(self, file_path: str) -> Tuple[bool, str]:
        """
        Validate if file is a valid CSV format
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            file_path_obj = Path(file_path)
            
            # Check file exists
            if not file_path_obj.exists():
                return False, "File does not exist"
            
            # Check file extension
            if file_path_obj.suffix.lower() not in ['.csv', '.txt', '.tsv']:
                return False, f"Invalid file extension: {file_path_obj.suffix}"
            
            # Check file size
            file_size = file_path_obj.stat().st_size
            if file_size == 0:
                return False, "File is empty"
            
            if file_size > 1024 * 1024 * 1024:  # 1GB limit
                return False, "File too large (>1GB)"
            
            # Try to read first few lines
            encoding = await self._detect_encoding(file_path)
            delimiter = await self._detect_delimiter(file_path, encoding)
            
            with open(file_path, 'r', encoding=encoding) as file:
                # Read first few lines to validate CSV structure
                sample_lines = []
                for i, line in enumerate(file):
                    if i >= 10:  # Check first 10 lines
                        break
                    sample_lines.append(line.strip())
                
                if not sample_lines:
                    return False, "File appears to be empty"
                
                # Try to parse with detected delimiter
                reader = csv.reader(sample_lines, delimiter=delimiter)
                rows = list(reader)
                
                if len(rows) < 1:
                    return False, "No valid CSV rows found"
                
                # Check if all rows have similar column counts
                if len(rows) > 1:
                    col_counts = [len(row) for row in rows]
                    if max(col_counts) - min(col_counts) > 5:  # Allow some variance
                        return False, "Inconsistent column counts across rows"
            
            return True, "Valid CSV file"
            
        except Exception as e:
            return False, f"CSV validation error: {str(e)}"
    
    async def detect_structure(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Detect CSV structure including columns, data types, and statistics
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            List of column information dictionaries
        """
        try:
            encoding = await self._detect_encoding(file_path)
            delimiter = await self._detect_delimiter(file_path, encoding)
            
            # Read sample data for analysis
            df_sample = pd.read_csv(
                file_path,
                delimiter=delimiter,
                encoding=encoding,
                nrows=self.max_sample_rows,
                low_memory=False
            )
            
            columns_info = []
            
            for position, column_name in enumerate(df_sample.columns):
                column_data = df_sample[column_name]
                
                # Get sample values (non-null)
                sample_values = column_data.dropna().head(10).astype(str).tolist()
                
                # Calculate statistics
                null_count = column_data.isnull().sum()
                unique_count = column_data.nunique()
                
                # Detect data type
                data_type = self._detect_data_type(sample_values)
                
                # Calculate string lengths for string columns
                min_length = None
                max_length = None
                if data_type == "STRING":
                    str_lengths = [len(str(val)) for val in sample_values if val]
                    if str_lengths:
                        min_length = min(str_lengths)
                        max_length = max(str_lengths)
                
                column_info = {
                    "name": column_name,
                    "position": position,
                    "data_type": data_type,
                    "sample_values": sample_values,
                    "null_count": int(null_count),
                    "unique_count": int(unique_count),
                    "total_count": len(column_data),
                    "min_length": min_length,
                    "max_length": max_length,
                    "null_percentage": round((null_count / len(column_data)) * 100, 2)
                }
                
                columns_info.append(column_info)
            
            self.logger.info(f"Detected {len(columns_info)} columns in CSV file")
            return columns_info
            
        except Exception as e:
            self.logger.error(f"Error detecting CSV structure: {str(e)}")
            raise FileProcessingException(f"Failed to detect CSV structure: {str(e)}")
    
    async def preview_data(self, file_path: str, rows: int = 10) -> Dict[str, Any]:
        """
        Generate preview of CSV data
        
        Args:
            file_path: Path to CSV file
            rows: Number of rows to preview
            
        Returns:
            Dictionary containing preview data and metadata
        """
        try:
            encoding = await self._detect_encoding(file_path)
            delimiter = await self._detect_delimiter(file_path, encoding)
            
            # Read preview data
            df_preview = pd.read_csv(
                file_path,
                delimiter=delimiter,
                encoding=encoding,
                nrows=rows,
                low_memory=False
            )
            
            # Convert to records for JSON serialization
            preview_records = df_preview.fillna("").to_dict('records')
            
            # Get file statistics
            total_rows = sum(1 for _ in open(file_path, 'r', encoding=encoding)) - 1  # Exclude header
            file_size = self._get_file_size(file_path)
            
            preview_data = {
                "columns": df_preview.columns.tolist(),
                "data": preview_records,
                "metadata": {
                    "total_rows": total_rows,
                    "preview_rows": len(preview_records),
                    "total_columns": len(df_preview.columns),
                    "file_size": file_size,
                    "encoding": encoding,
                    "delimiter": delimiter,
                    "delimiter_name": self._get_delimiter_name(delimiter)
                }
            }
            
            return preview_data
            
        except Exception as e:
            self.logger.error(f"Error generating CSV preview: {str(e)}")
            raise FileProcessingException(f"Failed to generate CSV preview: {str(e)}")
    
    async def process_file(self, file_path: str, file_registry: FileRegistry) -> Dict[str, Any]:
        """
        Process CSV file and extract all data
        
        Args:
            file_path: Path to CSV file
            file_registry: File registry record
            
        Returns:
            Processing results and statistics
        """
        try:
            self.logger.info(f"Starting CSV processing for file: {file_path}")
            
            # Detect file characteristics
            encoding = await self._detect_encoding(file_path)
            delimiter = await self._detect_delimiter(file_path, encoding)
            
            # Update file metadata
            metadata = file_registry.file_metadata or {}
            metadata.update({
                "encoding": encoding,
                "delimiter": delimiter,
                "delimiter_name": self._get_delimiter_name(delimiter),
                "processor": "CSVProcessor"
            })
            file_registry.file_metadata = metadata
            self.db_session.add(file_registry)
            
            # Detect and save column structure
            columns_info = await self.detect_structure(file_path)
            await self.save_column_structure(file_registry, columns_info)
            
            # Process records in chunks for memory efficiency
            record_iterator = self._read_csv_chunks(file_path, encoding, delimiter)
            processing_stats = await self.process_records(record_iterator, file_registry)
            
            # Update processing statistics
            processing_stats.update({
                "file_type": "CSV",
                "encoding": encoding,
                "delimiter": delimiter,
                "columns_detected": len(columns_info)
            })
            
            self.logger.info(f"CSV processing completed: {processing_stats}")
            return processing_stats
            
        except Exception as e:
            self.logger.error(f"Error processing CSV file: {str(e)}")
            raise FileProcessingException(f"Failed to process CSV file: {str(e)}")
    
    def _read_csv_chunks(self, file_path: str, encoding: str, delimiter: str) -> Iterator[Dict[str, Any]]:
        """
        Read CSV file in chunks to handle large files efficiently
        
        Args:
            file_path: Path to CSV file
            encoding: File encoding
            delimiter: CSV delimiter
            
        Yields:
            Dictionary records from CSV
        """
        try:
            # Use pandas for efficient chunked reading
            chunk_reader = pd.read_csv(
                file_path,
                delimiter=delimiter,
                encoding=encoding,
                chunksize=self.chunk_size,
                low_memory=False,
                dtype=str  # Read everything as string initially
            )
            
            for chunk in chunk_reader:
                # Convert chunk to records
                chunk_records = chunk.fillna("").to_dict('records')
                
                for record in chunk_records:
                    # Clean up the record
                    cleaned_record = {
                        key.strip(): str(value).strip() if value else None
                        for key, value in record.items()
                    }
                    yield cleaned_record
                    
        except Exception as e:
            self.logger.error(f"Error reading CSV chunks: {str(e)}")
            raise FileProcessingException(f"Failed to read CSV file: {str(e)}")
    
    async def _detect_encoding(self, file_path: str) -> str:
        """
        Auto-detect file encoding
        
        Args:
            file_path: Path to file
            
        Returns:
            Detected encoding string
        """
        if self.encoding:
            return self.encoding
        
        try:
            # Read sample of file for encoding detection
            with open(file_path, 'rb') as file:
                sample = file.read(10000)  # Read first 10KB
            
            detected = chardet.detect(sample)
            encoding = detected.get('encoding', 'utf-8')
            confidence = detected.get('confidence', 0)
            
            # Use utf-8 as fallback for low confidence
            if confidence < 0.7:
                encoding = 'utf-8'
            
            self.logger.info(f"Detected encoding: {encoding} (confidence: {confidence})")
            return encoding
            
        except Exception as e:
            self.logger.warning(f"Encoding detection failed: {str(e)}, using utf-8")
            return 'utf-8'
    
    async def _detect_delimiter(self, file_path: str, encoding: str) -> str:
        """
        Auto-detect CSV delimiter
        
        Args:
            file_path: Path to CSV file
            encoding: File encoding
            
        Returns:
            Detected delimiter character
        """
        if self.delimiter:
            return self.delimiter
        
        try:
            with open(file_path, 'r', encoding=encoding) as file:
                # Read first few lines for delimiter detection
                sample_lines = []
                for i, line in enumerate(file):
                    if i >= 5:  # Check first 5 lines
                        break
                    sample_lines.append(line)
                
                sample_text = '\n'.join(sample_lines)
            
            # Use csv.Sniffer to detect delimiter
            sniffer = csv.Sniffer()
            try:
                dialect = sniffer.sniff(sample_text, delimiters=''.join(self.delimiter_candidates))
                delimiter = dialect.delimiter
                self.logger.info(f"CSV Sniffer detected delimiter: '{delimiter}'")
                return delimiter
            except csv.Error:
                pass
            
            # Fallback: count occurrences of each delimiter
            delimiter_counts = {}
            for delimiter in self.delimiter_candidates:
                count = sample_text.count(delimiter)
                if count > 0:
                    delimiter_counts[delimiter] = count
            
            if delimiter_counts:
                # Choose delimiter with highest count
                detected_delimiter = max(delimiter_counts, key=delimiter_counts.get)
                self.logger.info(f"Delimiter detection by count: '{detected_delimiter}'")
                return detected_delimiter
            
            # Final fallback
            self.logger.warning("Could not detect delimiter, using comma")
            return ','
            
        except Exception as e:
            self.logger.warning(f"Delimiter detection failed: {str(e)}, using comma")
            return ','
    
    def _get_delimiter_name(self, delimiter: str) -> str:
        """Get human-readable name for delimiter"""
        delimiter_names = {
            ',': 'comma',
            ';': 'semicolon',
            '\t': 'tab',
            '|': 'pipe',
            ':': 'colon'
        }
        return delimiter_names.get(delimiter, f"'{delimiter}'")
    
    async def _custom_record_validation(self, record: Dict[str, Any], row_number: int) -> Dict[str, Any]:
        """
        CSV-specific record validation
        
        Args:
            record: Record to validate
            row_number: Row number for error reporting
            
        Returns:
            Validation result
        """
        validation_result = {"is_valid": True, "errors": []}
        
        try:
            # Check for records with only empty strings or whitespace
            non_empty_values = [v for v in record.values() if v and str(v).strip()]
            if not non_empty_values:
                validation_result["is_valid"] = False
                validation_result["errors"].append("Record contains only empty values")
            
            # Check for extremely long field values (potential data corruption)
            for field_name, value in record.items():
                if value and len(str(value)) > 10000:  # 10KB per field limit
                    validation_result["is_valid"] = False
                    validation_result["errors"].append(f"Field '{field_name}' exceeds maximum length")
            
            # Check for malformed CSV escape sequences
            for field_name, value in record.items():
                if value and isinstance(value, str):
                    if value.count('"') % 2 != 0:  # Unmatched quotes
                        validation_result["errors"].append(f"Field '{field_name}' has unmatched quotes")
            
        except Exception as e:
            validation_result["is_valid"] = False
            validation_result["errors"].append(f"Validation error: {str(e)}")
        
        return validation_result