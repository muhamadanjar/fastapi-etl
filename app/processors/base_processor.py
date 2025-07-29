# ==============================================
# app/processors/base_processor.py
# ==============================================
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Tuple, Iterator
from datetime import datetime
import hashlib
import json
from sqlmodel import Session
from pathlib import Path

from app.utils.logger import get_logger
from app.core.exceptions import FileProcessingException
from app.infrastructure.db.models.raw_data.file_registry import FileRegistry
from app.infrastructure.db.models.raw_data.raw_records import RawRecords
from app.infrastructure.db.models.raw_data.column_structure import ColumnStructure

logger = get_logger(__name__)

class BaseProcessor(ABC):
    """
    Abstract base class for all file processors.
    Defines common interface and shared functionality for processing different file types.
    """
    
    def __init__(self, db_session: Session, batch_id: Optional[str] = None):
        """
        Initialize base processor
        
        Args:
            db_session: Database session for data operations
            batch_id: Optional batch ID for grouping processed records
        """
        self.db_session = db_session
        self.batch_id = batch_id or self._generate_batch_id()
        self.logger = logger
        self.processed_records = 0
        self.failed_records = 0
        self.validation_errors = []
        
    def _generate_batch_id(self) -> str:
        """Generate unique batch ID"""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return f"BATCH_{timestamp}_{hashlib.md5(str(datetime.utcnow()).encode()).hexdigest()[:8]}"
    
    @abstractmethod
    async def process_file(self, file_path: str, file_registry: FileRegistry) -> Dict[str, Any]:
        """
        Process a file and extract data
        
        Args:
            file_path: Path to the file to process
            file_registry: File registry record from database
            
        Returns:
            Dictionary containing processing results and statistics
        """
        pass
    
    @abstractmethod
    async def validate_file_format(self, file_path: str) -> Tuple[bool, str]:
        """
        Validate if file format is correct for this processor
        
        Args:
            file_path: Path to the file to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        pass
    
    @abstractmethod
    async def detect_structure(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Detect and analyze file structure
        
        Args:
            file_path: Path to the file to analyze
            
        Returns:
            List of column/field information
        """
        pass
    
    @abstractmethod
    async def preview_data(self, file_path: str, rows: int = 10) -> Dict[str, Any]:
        """
        Generate preview of file data
        
        Args:
            file_path: Path to the file to preview
            rows: Number of rows/records to preview
            
        Returns:
            Dictionary containing preview data and metadata
        """
        pass
    
    async def process_records(self, records: Iterator[Dict[str, Any]], file_registry: FileRegistry) -> Dict[str, Any]:
        """
        Process individual records and store in database
        
        Args:
            records: Iterator of record dictionaries
            file_registry: File registry record
            
        Returns:
            Processing statistics
        """
        processing_stats = {
            "total_records": 0,
            "successful_records": 0,
            "failed_records": 0,
            "validation_errors": [],
            "processing_time": 0
        }
        
        start_time = datetime.utcnow()
        
        try:
            for row_number, record in enumerate(records, 1):
                try:
                    # Validate record
                    validation_result = await self._validate_record(record, row_number)
                    
                    # Generate record hash for deduplication
                    record_hash = self._generate_record_hash(record)
                    
                    # Create raw record
                    raw_record = RawRecords(
                        file_id=file_registry.id,
                        row_number=row_number,
                        raw_data=record,
                        data_hash=record_hash,
                        validation_status="VALID" if validation_result["is_valid"] else "INVALID",
                        validation_errors=validation_result["errors"],
                        batch_id=self.batch_id
                    )
                    
                    # Save to database
                    self.db_session.add(raw_record)
                    
                    if validation_result["is_valid"]:
                        processing_stats["successful_records"] += 1
                    else:
                        processing_stats["failed_records"] += 1
                        processing_stats["validation_errors"].extend(validation_result["errors"])
                    
                    processing_stats["total_records"] += 1
                    
                    # Commit in batches for performance
                    if row_number % 1000 == 0:
                        self.db_session.commit()
                        self.logger.info(f"Processed {row_number} records")
                
                except Exception as e:
                    processing_stats["failed_records"] += 1
                    processing_stats["total_records"] += 1
                    error_msg = f"Row {row_number}: {str(e)}"
                    processing_stats["validation_errors"].append(error_msg)
                    self.logger.error(f"Error processing record {row_number}: {str(e)}")
            
            # Final commit
            self.db_session.commit()
            
            # Calculate processing time
            end_time = datetime.utcnow()
            processing_stats["processing_time"] = (end_time - start_time).total_seconds()
            
            self.logger.info(f"Processing completed: {processing_stats}")
            return processing_stats
            
        except Exception as e:
            self.db_session.rollback()
            self.logger.error(f"Error during record processing: {str(e)}")
            raise FileProcessingException(f"Failed to process records: {str(e)}")
    
    async def save_column_structure(self, file_registry: FileRegistry, columns: List[Dict[str, Any]]):
        """
        Save detected column structure to database
        
        Args:
            file_registry: File registry record
            columns: List of column information
        """
        try:
            for position, column_info in enumerate(columns):
                column_structure = ColumnStructure(
                    file_id=file_registry.id,
                    column_name=column_info.get("name", f"column_{position}"),
                    column_position=position,
                    data_type=column_info.get("data_type", "STRING"),
                    sample_values=column_info.get("sample_values", []),
                    null_count=column_info.get("null_count", 0),
                    unique_count=column_info.get("unique_count", 0),
                    min_length=column_info.get("min_length"),
                    max_length=column_info.get("max_length")
                )
                
                self.db_session.add(column_structure)
            
            self.db_session.commit()
            self.logger.info(f"Saved column structure for {len(columns)} columns")
            
        except Exception as e:
            self.db_session.rollback()
            self.logger.error(f"Error saving column structure: {str(e)}")
            raise FileProcessingException(f"Failed to save column structure: {str(e)}")
    
    async def _validate_record(self, record: Dict[str, Any], row_number: int) -> Dict[str, Any]:
        """
        Validate individual record
        
        Args:
            record: Record data to validate
            row_number: Row number for error reporting
            
        Returns:
            Validation result with is_valid flag and errors list
        """
        validation_result = {
            "is_valid": True,
            "errors": []
        }
        
        try:
            # Basic validation checks
            if not record:
                validation_result["is_valid"] = False
                validation_result["errors"].append("Empty record")
                return validation_result
            
            # Check for completely null record
            if all(value is None or value == "" for value in record.values()):
                validation_result["is_valid"] = False
                validation_result["errors"].append("All fields are empty")
                return validation_result
            
            # Additional custom validation can be implemented in child classes
            custom_validation = await self._custom_record_validation(record, row_number)
            if not custom_validation["is_valid"]:
                validation_result["is_valid"] = False
                validation_result["errors"].extend(custom_validation["errors"])
            
            return validation_result
            
        except Exception as e:
            validation_result["is_valid"] = False
            validation_result["errors"].append(f"Validation error: {str(e)}")
            return validation_result
    
    async def _custom_record_validation(self, record: Dict[str, Any], row_number: int) -> Dict[str, Any]:
        """
        Custom validation logic to be implemented by child classes
        
        Args:
            record: Record data to validate
            row_number: Row number for error reporting
            
        Returns:
            Validation result
        """
        return {"is_valid": True, "errors": []}
    
    def _generate_record_hash(self, record: Dict[str, Any]) -> str:
        """
        Generate hash for record deduplication
        
        Args:
            record: Record data
            
        Returns:
            SHA-256 hash of record
        """
        try:
            # Sort keys for consistent hashing
            sorted_record = {k: v for k, v in sorted(record.items())}
            record_string = json.dumps(sorted_record, sort_keys=True, default=str)
            return hashlib.sha256(record_string.encode()).hexdigest()
        except Exception as e:
            self.logger.warning(f"Error generating record hash: {str(e)}")
            return hashlib.sha256(str(record).encode()).hexdigest()
    
    def _detect_data_type(self, values: List[Any]) -> str:
        """
        Detect data type from sample values
        
        Args:
            values: List of sample values
            
        Returns:
            Detected data type (STRING, NUMBER, DATE, BOOLEAN)
        """
        # Remove null values for type detection
        non_null_values = [v for v in values if v is not None and v != ""]
        
        if not non_null_values:
            return "STRING"
        
        # Check for boolean
        boolean_values = {"true", "false", "1", "0", "yes", "no", "y", "n"}
        if all(str(v).lower() in boolean_values for v in non_null_values):
            return "BOOLEAN"
        
        # Check for number
        numeric_count = 0
        for value in non_null_values[:100]:  # Check first 100 values
            try:
                float(str(value).replace(",", ""))
                numeric_count += 1
            except ValueError:
                pass
        
        if numeric_count / len(non_null_values) > 0.8:  # 80% are numeric
            return "NUMBER"
        
        # Check for date
        date_count = 0
        for value in non_null_values[:100]:
            if self._is_date_like(str(value)):
                date_count += 1
        
        if date_count / len(non_null_values) > 0.8:  # 80% are date-like
            return "DATE"
        
        return "STRING"
    
    def _is_date_like(self, value: str) -> bool:
        """
        Check if value looks like a date
        
        Args:
            value: String value to check
            
        Returns:
            True if value appears to be a date
        """
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # 2023-01-01
            r'\d{2}/\d{2}/\d{4}',  # 01/01/2023
            r'\d{2}-\d{2}-\d{4}',  # 01-01-2023
            r'\d{4}/\d{2}/\d{2}',  # 2023/01/01
        ]
        
        import re
        return any(re.match(pattern, value.strip()) for pattern in date_patterns)
    
    def _get_file_size(self, file_path: str) -> int:
        """Get file size in bytes"""
        try:
            return Path(file_path).stat().st_size
        except Exception:
            return 0
    
    def _safe_convert(self, value: Any, target_type: str) -> Any:
        """
        Safely convert value to target type
        
        Args:
            value: Value to convert
            target_type: Target data type
            
        Returns:
            Converted value or original value if conversion fails
        """
        if value is None or value == "":
            return None
        
        try:
            if target_type == "NUMBER":
                return float(str(value).replace(",", ""))
            elif target_type == "BOOLEAN":
                return str(value).lower() in {"true", "1", "yes", "y"}
            elif target_type == "DATE":
                from dateutil import parser
                return parser.parse(str(value))
            else:
                return str(value)
        except Exception:
            return value
    
    async def cleanup_temp_files(self, file_paths: List[str]):
        """
        Clean up temporary files created during processing
        
        Args:
            file_paths: List of file paths to clean up
        """
        for file_path in file_paths:
            try:
                Path(file_path).unlink(missing_ok=True)
                self.logger.debug(f"Cleaned up temp file: {file_path}")
            except Exception as e:
                self.logger.warning(f"Failed to clean up temp file {file_path}: {str(e)}")