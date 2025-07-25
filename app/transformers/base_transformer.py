# ==============================================
# app/transformers/base_transformer.py
# ==============================================
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime
from sqlmodel import Session
import hashlib
import json
from enum import Enum

from app.utils.logger import get_logger
from app.core.exceptions import DataTransformationException
from app.infrastructure.db.models.staging.standardized_data import StandardizedData
from app.infrastructure.db.models.etl_control.job_executions import JobExecution

logger = get_logger(__name__)

class TransformationStatus(Enum):
    """Status enumeration for transformation results"""
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    WARNING = "warning"

class TransformationResult:
    """Result object for transformation operations"""
    
    def __init__(self, 
                 status: TransformationStatus,
                 data: Any = None,
                 errors: List[str] = None,
                 warnings: List[str] = None,
                 metadata: Dict[str, Any] = None):
        self.status = status
        self.data = data
        self.errors = errors or []
        self.warnings = warnings or []
        self.metadata = metadata or {}
        self.timestamp = datetime.utcnow()
    
    def is_success(self) -> bool:
        return self.status == TransformationStatus.SUCCESS
    
    def is_failed(self) -> bool:
        return self.status == TransformationStatus.FAILED
    
    def has_warnings(self) -> bool:
        return len(self.warnings) > 0
    
    def add_error(self, error: str):
        self.errors.append(error)
        if self.status == TransformationStatus.SUCCESS:
            self.status = TransformationStatus.FAILED
    
    def add_warning(self, warning: str):
        self.warnings.append(warning)
        if self.status == TransformationStatus.SUCCESS:
            self.status = TransformationStatus.WARNING
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "data": self.data,
            "errors": self.errors,
            "warnings": self.warnings,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat()
        }

class BaseTransformer(ABC):
    """
    Abstract base class for all data transformers.
    Provides common functionality and interface for data transformation operations.
    """
    
    def __init__(self, 
                 db_session: Session,
                 job_execution_id: Optional[str] = None,
                 **kwargs):
        """
        Initialize base transformer
        
        Args:
            db_session: Database session for data operations
            job_execution_id: Optional job execution ID for tracking
            **kwargs: Additional configuration parameters
        """
        self.db_session = db_session
        self.job_execution_id = job_execution_id
        self.logger = logger
        
        # Transformation configuration
        self.batch_size = kwargs.get('batch_size', 1000)
        self.max_errors = kwargs.get('max_errors', 100)
        self.error_threshold = kwargs.get('error_threshold', 0.05)  # 5% error rate
        self.skip_invalid_records = kwargs.get('skip_invalid_records', True)
        self.preserve_source_data = kwargs.get('preserve_source_data', False)
        
        # Transformation statistics
        self.records_processed = 0
        self.records_transformed = 0
        self.records_failed = 0
        self.records_skipped = 0
        self.transformation_errors = []
        self.transformation_warnings = []
        
        # Performance tracking
        self.start_time = None
        self.end_time = None
        
        # Custom configuration
        self.config = kwargs
    
    @abstractmethod
    async def transform_record(self, record: Dict[str, Any]) -> TransformationResult:
        """
        Transform a single record
        
        Args:
            record: Input record to transform
            
        Returns:
            TransformationResult with transformed data
        """
        pass
    
    @abstractmethod
    async def validate_config(self) -> Tuple[bool, List[str]]:
        """
        Validate transformer configuration
        
        Returns:
            Tuple of (is_valid, error_messages)
        """
        pass
    
    async def transform_batch(self, records: List[Dict[str, Any]]) -> List[TransformationResult]:
        """
        Transform a batch of records
        
        Args:
            records: List of records to transform
            
        Returns:
            List of TransformationResult objects
        """
        results = []
        
        for record in records:
            try:
                result = await self.transform_record(record)
                results.append(result)
                
                # Update statistics
                self.records_processed += 1
                
                if result.is_success():
                    self.records_transformed += 1
                elif result.is_failed():
                    self.records_failed += 1
                    self.transformation_errors.extend(result.errors)
                else:  # SKIPPED
                    self.records_skipped += 1
                
                if result.has_warnings():
                    self.transformation_warnings.extend(result.warnings)
                
                # Check error threshold
                if self.records_processed > 0:
                    error_rate = self.records_failed / self.records_processed
                    if error_rate > self.error_threshold:
                        raise DataTransformationException(
                            f"Error rate ({error_rate:.2%}) exceeds threshold ({self.error_threshold:.2%})"
                        )
                
                # Check max errors
                if len(self.transformation_errors) > self.max_errors:
                    raise DataTransformationException(
                        f"Maximum error count ({self.max_errors}) exceeded"
                    )
                
            except Exception as e:
                self.logger.error(f"Error transforming record: {str(e)}")
                
                if self.skip_invalid_records:
                    error_result = TransformationResult(
                        status=TransformationStatus.FAILED,
                        errors=[str(e)]
                    )
                    results.append(error_result)
                    self.records_failed += 1
                    self.transformation_errors.append(str(e))
                else:
                    raise DataTransformationException(f"Record transformation failed: {str(e)}")
        
        return results
    
    async def transform_dataset(self, 
                              input_data: List[Dict[str, Any]], 
                              output_entity_type: str = None) -> Dict[str, Any]:
        """
        Transform entire dataset with batching and statistics
        
        Args:
            input_data: List of input records
            output_entity_type: Entity type for output records
            
        Returns:
            Dictionary with transformation statistics and results
        """
        try:
            self.start_time = datetime.utcnow()
            
            # Validate configuration
            is_valid, validation_errors = await self.validate_config()
            if not is_valid:
                raise DataTransformationException(f"Invalid configuration: {validation_errors}")
            
            # Reset statistics
            self._reset_statistics()
            
            # Process data in batches
            all_results = []
            
            for i in range(0, len(input_data), self.batch_size):
                batch = input_data[i:i + self.batch_size]
                
                self.logger.info(f"Processing batch {i//self.batch_size + 1}: {len(batch)} records")
                
                batch_results = await self.transform_batch(batch)
                all_results.extend(batch_results)
                
                # Save intermediate results to database
                if output_entity_type:
                    await self._save_batch_results(batch_results, output_entity_type)
                
                # Commit batch to database
                self.db_session.commit()
            
            self.end_time = datetime.utcnow()
            
            # Generate final statistics
            statistics = self._generate_statistics()
            
            self.logger.info(f"Transformation completed: {statistics}")
            
            return {
                "statistics": statistics,
                "results": all_results,
                "success": self.records_failed == 0 or (self.records_failed / self.records_processed) <= self.error_threshold
            }
            
        except Exception as e:
            self.logger.error(f"Dataset transformation failed: {str(e)}")
            raise DataTransformationException(f"Dataset transformation failed: {str(e)}")
    
    async def _save_batch_results(self, 
                                 results: List[TransformationResult], 
                                 entity_type: str):
        """
        Save batch transformation results to database
        
        Args:
            results: List of transformation results
            entity_type: Entity type for the results
        """
        try:
            for result in results:
                if result.is_success() and result.data:
                    # Create standardized data record
                    standardized_record = StandardizedData(
                        entity_type=entity_type,
                        standardized_data=result.data,
                        quality_score=result.metadata.get('quality_score', 1.0),
                        transformation_rules_applied=result.metadata.get('rules_applied', []),
                        batch_id=result.metadata.get('batch_id'),
                        job_execution_id=self.job_execution_id
                    )
                    
                    self.db_session.add(standardized_record)
                    
        except Exception as e:
            self.logger.error(f"Error saving batch results: {str(e)}")
            raise DataTransformationException(f"Failed to save batch results: {str(e)}")
    
    def _reset_statistics(self):
        """Reset transformation statistics"""
        self.records_processed = 0
        self.records_transformed = 0
        self.records_failed = 0
        self.records_skipped = 0
        self.transformation_errors = []
        self.transformation_warnings = []
    
    def _generate_statistics(self) -> Dict[str, Any]:
        """Generate transformation statistics"""
        processing_time = (self.end_time - self.start_time).total_seconds() if self.start_time and self.end_time else 0
        
        return {
            "records_processed": self.records_processed,
            "records_transformed": self.records_transformed,
            "records_failed": self.records_failed,
            "records_skipped": self.records_skipped,
            "success_rate": (self.records_transformed / max(self.records_processed, 1)) * 100,
            "error_rate": (self.records_failed / max(self.records_processed, 1)) * 100,
            "processing_time_seconds": processing_time,
            "throughput_records_per_second": self.records_processed / max(processing_time, 1),
            "total_errors": len(self.transformation_errors),
            "total_warnings": len(self.transformation_warnings),
            "transformer_type": self.__class__.__name__,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None
        }
    
    def get_transformation_summary(self) -> Dict[str, Any]:
        """Get current transformation summary"""
        return {
            "statistics": self._generate_statistics(),
            "recent_errors": self.transformation_errors[-10:],  # Last 10 errors
            "recent_warnings": self.transformation_warnings[-10:],  # Last 10 warnings
            "configuration": self.config
        }
    
    def _calculate_quality_score(self, 
                                record: Dict[str, Any],
                                original_record: Dict[str, Any] = None) -> float:
        """
        Calculate quality score for transformed record
        
        Args:
            record: Transformed record
            original_record: Original record for comparison
            
        Returns:
            Quality score between 0.0 and 1.0
        """
        score = 1.0
        
        # Check for completeness
        total_fields = len(record)
        non_empty_fields = sum(1 for v in record.values() if v is not None and v != "")
        completeness_score = non_empty_fields / max(total_fields, 1)
        
        # Check for data consistency
        consistency_score = 1.0
        for field, value in record.items():
            if isinstance(value, str):
                # Check for inconsistent formatting
                if field.lower() in ['email', 'phone', 'date'] and not self._is_well_formatted(field, value):
                    consistency_score -= 0.1
        
        # Combine scores
        score = (completeness_score * 0.6) + (consistency_score * 0.4)
        
        return max(0.0, min(1.0, score))
    
    def _is_well_formatted(self, field_type: str, value: str) -> bool:
        """Check if value is well-formatted for its type"""
        import re
        
        if 'email' in field_type.lower():
            return re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', value) is not None
        elif 'phone' in field_type.lower():
            return re.match(r'^[\d\s\-\+\(\)]+$', value) is not None
        elif 'date' in field_type.lower():
            return re.match(r'^\d{4}-\d{2}-\d{2}', value) is not None
        
        return True
    
    def _generate_record_hash(self, record: Dict[str, Any]) -> str:
        """Generate hash for record deduplication"""
        try:
            sorted_record = {k: v for k, v in sorted(record.items())}
            record_string = json.dumps(sorted_record, sort_keys=True, default=str)
            return hashlib.sha256(record_string.encode()).hexdigest()
        except Exception as e:
            self.logger.warning(f"Error generating record hash: {str(e)}")
            return hashlib.sha256(str(record).encode()).hexdigest()
    
    def _safe_convert_type(self, value: Any, target_type: type) -> Tuple[Any, bool]:
        """
        Safely convert value to target type
        
        Args:
            value: Value to convert
            target_type: Target type
            
        Returns:
            Tuple of (converted_value, success)
        """
        if value is None:
            return None, True
        
        try:
            if target_type == str:
                return str(value), True
            elif target_type == int:
                if isinstance(value, str):
                    # Remove common formatting
                    cleaned = value.replace(',', '').replace(' ', '')
                    return int(float(cleaned)), True
                return int(value), True
            elif target_type == float:
                if isinstance(value, str):
                    cleaned = value.replace(',', '').replace(' ', '')
                    return float(cleaned), True
                return float(value), True
            elif target_type == bool:
                if isinstance(value, str):
                    return value.lower() in ('true', '1', 'yes', 'y', 'on'), True
                return bool(value), True
            else:
                return value, True
                
        except (ValueError, TypeError):
            return value, False
    
    def _normalize_string(self, value: str) -> str:
        """Normalize string value"""
        if not isinstance(value, str):
            return value
        
        # Basic normalization
        normalized = value.strip()
        
        # Remove extra whitespace
        normalized = ' '.join(normalized.split())
        
        # Convert to title case for names
        if self._looks_like_name(normalized):
            normalized = normalized.title()
        
        return normalized
    
    def _looks_like_name(self, value: str) -> bool:
        """Check if string looks like a person name"""
        import re
        
        # Simple heuristic: contains only letters, spaces, and common name characters
        return bool(re.match(r'^[a-zA-Z\s\-\.\']+$', value)) and len(value.split()) >= 2
    
    def _extract_metadata(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata from record"""
        metadata = {}
        
        # Extract source information
        source_fields = [k for k in record.keys() if k.startswith('_source') or k.startswith('_origin')]
        for field in source_fields:
            metadata[field] = record.get(field)
        
        # Extract timestamps
        timestamp_fields = [k for k in record.keys() if 'timestamp' in k.lower() or 'time' in k.lower()]
        for field in timestamp_fields:
            metadata[f"original_{field}"] = record.get(field)
        
        return metadata
    
    async def cleanup_transformation(self):
        """Cleanup resources after transformation"""
        try:
            # Close database connections if needed
            if hasattr(self, 'temp_connections'):
                for conn in self.temp_connections:
                    conn.close()
            
            # Clean up temporary files
            if hasattr(self, 'temp_files'):
                import os
                for file_path in self.temp_files:
                    try:
                        os.remove(file_path)
                    except OSError:
                        pass
            
            self.logger.info("Transformation cleanup completed")
            
        except Exception as e:
            self.logger.warning(f"Error during transformation cleanup: {str(e)}")
    
    def __str__(self) -> str:
        return f"{self.__class__.__name__}(processed={self.records_processed}, transformed={self.records_transformed})"
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(config={self.config})"
    