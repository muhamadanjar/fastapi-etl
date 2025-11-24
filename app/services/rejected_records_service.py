"""
Service for managing rejected records during ETL processing.
"""

from typing import Dict, List, Any, Optional
from uuid import UUID, uuid4
from datetime import datetime
from sqlmodel import Session, select, func, and_, or_
from sqlalchemy import desc

from app.infrastructure.db.models.raw_data.rejected_records import (
    RejectedRecord,
    RejectedRecordCreate,
    RejectedRecordUpdate,
    RejectedRecordSummary
)
from app.core.exceptions import ETLError
from app.utils.logger import get_logger

logger = get_logger(__name__)


class RejectedRecordsService:
    """Service for managing rejected records"""
    
    def __init__(self, db_session: Session):
        self.db = db_session
        self.logger = logger
    
    async def store_rejected_record(
        self,
        source_file_id: UUID,
        raw_data: Dict[str, Any],
        rejection_reason: str,
        row_number: Optional[int] = None,
        source_record_id: Optional[UUID] = None,
        validation_errors: Optional[List[Dict[str, Any]]] = None,
        batch_id: Optional[str] = None,
        can_retry: bool = True
    ) -> RejectedRecord:
        """
        Store a rejected record in the database.
        
        Args:
            source_file_id: ID of the source file
            raw_data: The raw data that was rejected
            rejection_reason: Main reason for rejection
            row_number: Row number in source file
            source_record_id: ID of raw record if available
            validation_errors: Detailed validation errors
            batch_id: Batch identifier
            can_retry: Whether record can be retried
            
        Returns:
            Created RejectedRecord instance
        """
        try:
            rejected_record = RejectedRecord(
                rejection_id=uuid4(),
                source_file_id=source_file_id,
                source_record_id=source_record_id,
                row_number=row_number,
                raw_data=raw_data,
                rejection_reason=rejection_reason,
                validation_errors=validation_errors,
                can_retry=can_retry,
                retry_count=0,
                is_resolved=False,
                batch_id=batch_id,
                rejected_at=datetime.utcnow()
            )
            
            self.db.add(rejected_record)
            self.db.commit()
            self.db.refresh(rejected_record)
            
            self.logger.info(f"Stored rejected record: {rejected_record.rejection_id}")
            return rejected_record
            
        except Exception as e:
            self.db.rollback()
            self.logger.error(f"Error storing rejected record: {str(e)}")
            raise ETLError(f"Failed to store rejected record: {str(e)}")
    
    async def get_rejected_records(
        self,
        source_file_id: Optional[UUID] = None,
        batch_id: Optional[str] = None,
        is_resolved: Optional[bool] = None,
        can_retry: Optional[bool] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[RejectedRecord]:
        """
        Get rejected records with optional filters.
        
        Args:
            source_file_id: Filter by source file
            batch_id: Filter by batch
            is_resolved: Filter by resolution status
            can_retry: Filter by retry capability
            limit: Maximum records to return
            offset: Offset for pagination
            
        Returns:
            List of RejectedRecord instances
        """
        try:
            query = select(RejectedRecord)
            
            # Apply filters
            conditions = []
            if source_file_id:
                conditions.append(RejectedRecord.source_file_id == source_file_id)
            if batch_id:
                conditions.append(RejectedRecord.batch_id == batch_id)
            if is_resolved is not None:
                conditions.append(RejectedRecord.is_resolved == is_resolved)
            if can_retry is not None:
                conditions.append(RejectedRecord.can_retry == can_retry)
            
            if conditions:
                query = query.where(and_(*conditions))
            
            # Order by most recent first
            query = query.order_by(desc(RejectedRecord.rejected_at))
            
            # Apply pagination
            query = query.limit(limit).offset(offset)
            
            results = self.db.exec(query).all()
            
            self.logger.info(f"Retrieved {len(results)} rejected records")
            return results
            
        except Exception as e:
            self.logger.error(f"Error retrieving rejected records: {str(e)}")
            raise ETLError(f"Failed to retrieve rejected records: {str(e)}")
    
    async def get_rejection_summary(
        self,
        source_file_id: Optional[UUID] = None,
        batch_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get summary statistics for rejected records.
        
        Args:
            source_file_id: Filter by source file
            batch_id: Filter by batch
            
        Returns:
            Dictionary with summary statistics
        """
        try:
            query = select(RejectedRecord)
            
            # Apply filters
            conditions = []
            if source_file_id:
                conditions.append(RejectedRecord.source_file_id == source_file_id)
            if batch_id:
                conditions.append(RejectedRecord.batch_id == batch_id)
            
            if conditions:
                query = query.where(and_(*conditions))
            
            records = self.db.exec(query).all()
            
            # Calculate statistics
            total_rejected = len(records)
            can_retry_count = sum(1 for r in records if r.can_retry)
            resolved_count = sum(1 for r in records if r.is_resolved)
            
            # Get common rejection reasons
            reason_counts = {}
            for record in records:
                reason = record.rejection_reason
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
            
            # Sort by frequency
            common_reasons = [
                {"reason": reason, "count": count}
                for reason, count in sorted(
                    reason_counts.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:10]  # Top 10
            ]
            
            summary = {
                "total_rejected": total_rejected,
                "can_retry_count": can_retry_count,
                "resolved_count": resolved_count,
                "unresolved_count": total_rejected - resolved_count,
                "resolution_rate": round((resolved_count / total_rejected * 100), 2) if total_rejected > 0 else 0,
                "common_rejection_reasons": common_reasons
            }
            
            self.logger.info(f"Generated rejection summary: {summary}")
            return summary
            
        except Exception as e:
            self.logger.error(f"Error generating rejection summary: {str(e)}")
            raise ETLError(f"Failed to generate rejection summary: {str(e)}")
    
    async def mark_as_resolved(
        self,
        rejection_id: UUID,
        resolved: bool = True
    ) -> RejectedRecord:
        """
        Mark a rejected record as resolved or unresolved.
        
        Args:
            rejection_id: ID of rejected record
            resolved: Whether to mark as resolved
            
        Returns:
            Updated RejectedRecord instance
        """
        try:
            record = self.db.get(RejectedRecord, rejection_id)
            
            if not record:
                raise ETLError(f"Rejected record not found: {rejection_id}")
            
            record.is_resolved = resolved
            record.resolved_at = datetime.utcnow() if resolved else None
            
            self.db.add(record)
            self.db.commit()
            self.db.refresh(record)
            
            self.logger.info(f"Marked record {rejection_id} as {'resolved' if resolved else 'unresolved'}")
            return record
            
        except Exception as e:
            self.db.rollback()
            self.logger.error(f"Error marking record as resolved: {str(e)}")
            raise ETLError(f"Failed to mark record as resolved: {str(e)}")
    
    async def retry_rejected_record(
        self,
        rejection_id: UUID
    ) -> RejectedRecord:
        """
        Increment retry count for a rejected record.
        
        Args:
            rejection_id: ID of rejected record
            
        Returns:
            Updated RejectedRecord instance
        """
        try:
            record = self.db.get(RejectedRecord, rejection_id)
            
            if not record:
                raise ETLError(f"Rejected record not found: {rejection_id}")
            
            if not record.can_retry:
                raise ETLError(f"Record {rejection_id} cannot be retried")
            
            record.retry_count += 1
            record.last_retry_at = datetime.utcnow()
            
            self.db.add(record)
            self.db.commit()
            self.db.refresh(record)
            
            self.logger.info(f"Incremented retry count for record {rejection_id} to {record.retry_count}")
            return record
            
        except Exception as e:
            self.db.rollback()
            self.logger.error(f"Error retrying record: {str(e)}")
            raise ETLError(f"Failed to retry record: {str(e)}")
    
    async def delete_rejected_record(
        self,
        rejection_id: UUID
    ) -> bool:
        """
        Delete a rejected record.
        
        Args:
            rejection_id: ID of rejected record
            
        Returns:
            True if deleted successfully
        """
        try:
            record = self.db.get(RejectedRecord, rejection_id)
            
            if not record:
                raise ETLError(f"Rejected record not found: {rejection_id}")
            
            self.db.delete(record)
            self.db.commit()
            
            self.logger.info(f"Deleted rejected record: {rejection_id}")
            return True
            
        except Exception as e:
            self.db.rollback()
            self.logger.error(f"Error deleting rejected record: {str(e)}")
            raise ETLError(f"Failed to delete rejected record: {str(e)}")
    
    async def export_rejected_records(
        self,
        source_file_id: Optional[UUID] = None,
        batch_id: Optional[str] = None,
        format: str = "csv"
    ) -> str:
        """
        Export rejected records to file.
        
        Args:
            source_file_id: Filter by source file
            batch_id: Filter by batch
            format: Export format (csv, json)
            
        Returns:
            Path to exported file
        """
        try:
            records = await self.get_rejected_records(
                source_file_id=source_file_id,
                batch_id=batch_id,
                limit=100000  # Large limit for export
            )
            
            if format == "csv":
                import csv
                from pathlib import Path
                
                export_dir = Path("storage/exports/rejected_records")
                export_dir.mkdir(parents=True, exist_ok=True)
                
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                filename = f"rejected_records_{timestamp}.csv"
                filepath = export_dir / filename
                
                with open(filepath, 'w', newline='') as csvfile:
                    if records:
                        fieldnames = ['rejection_id', 'source_file_id', 'row_number', 
                                    'rejection_reason', 'rejected_at', 'is_resolved']
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        writer.writeheader()
                        
                        for record in records:
                            writer.writerow({
                                'rejection_id': str(record.rejection_id),
                                'source_file_id': str(record.source_file_id),
                                'row_number': record.row_number,
                                'rejection_reason': record.rejection_reason,
                                'rejected_at': record.rejected_at.isoformat(),
                                'is_resolved': record.is_resolved
                            })
                
                self.logger.info(f"Exported {len(records)} records to {filepath}")
                return str(filepath)
            
            else:
                raise ETLError(f"Unsupported export format: {format}")
                
        except Exception as e:
            self.logger.error(f"Error exporting rejected records: {str(e)}")
            raise ETLError(f"Failed to export rejected records: {str(e)}")
