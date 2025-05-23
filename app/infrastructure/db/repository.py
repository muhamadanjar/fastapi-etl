"""
Simplified SQLModel repository for CRUD operations.

This module provides a simple, type-safe repository pattern
using SQLModel for easy database operations.
"""

import logging
from typing import Generic, TypeVar, Type, Optional, List, Dict, Any, Sequence
from uuid import UUID

from sqlmodel import Session, SQLModel, select, func, or_, and_, desc, asc
from sqlalchemy.exc import IntegrityError

from ...core.exceptions import DatabaseError, NotFoundError, ConflictError
from .models.base import BaseModel

logger = logging.getLogger(__name__)

# Type variable for SQLModel
ModelType = TypeVar("ModelType", bound=SQLModel)


class Repository(Generic[ModelType]):
    """
    Generic repository for SQLModel CRUD operations.
    
    Provides simple, type-safe database operations with automatic
    error handling and logging.
    """
    
    def __init__(self, session: Session, model: Type[ModelType]):
        """
        Initialize repository.
        
        Args:
            session: Database session
            model: SQLModel class
        """
        self.session = session
        self.model = model
    
    def create(self, obj: ModelType) -> ModelType:
        """
        Create new record.
        
        Args:
            obj: Model instance to create
            
        Returns:
            Created model with generated ID
            
        Raises:
            ConflictError: If record conflicts with existing data
            DatabaseError: If database operation fails
        """
        try:
            self.session.add(obj)
            self.session.commit()
            self.session.refresh(obj)
            
            logger.debug(f"Created {self.model.__name__} with ID: {getattr(obj, 'id', 'N/A')}")
            return obj
            
        except IntegrityError as e:
            self.session.rollback()
            logger.error(f"Integrity error creating {self.model.__name__}: {e}")
            raise ConflictError(f"Record with conflicting data already exists: {e}")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error creating {self.model.__name__}: {e}")
            raise DatabaseError(f"Failed to create record: {e}")
    
    def get(self, id: UUID) -> Optional[ModelType]:
        """
        Get record by ID.
        
        Args:
            id: Record ID
            
        Returns:
            Model instance if found, None otherwise
        """
        try:
            statement = select(self.model).where(self.model.id == id)
            
            # Add soft delete filter if model has is_deleted field
            if hasattr(self.model, 'is_deleted'):
                statement = statement.where(self.model.is_deleted == False)
            
            result = self.session.exec(statement).first()
            
            if result:
                logger.debug(f"Found {self.model.__name__} with ID: {id}")
            else:
                logger.debug(f"{self.model.__name__} not found with ID: {id}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting {self.model.__name__} by ID {id}: {e}")
            raise DatabaseError(f"Failed to get record: {e}")
    
    def get_or_404(self, id: UUID) -> ModelType:
        """
        Get record by ID or raise NotFoundError.
        
        Args:
            id: Record ID
            
        Returns:
            Model instance
            
        Raises:
            NotFoundError: If record not found
        """
        result = self.get(id)
        if not result:
            raise NotFoundError(resource=self.model.__name__, resource_id=str(id))
        return result
    
    def update(self, id: UUID, data: Dict[str, Any]) -> ModelType:
        """
        Update record.
        
        Args:
            id: Record ID
            data: Fields to update
            
        Returns:
            Updated model
            
        Raises:
            NotFoundError: If record not found
            ConflictError: If update conflicts with existing data
            DatabaseError: If database operation fails
        """
        try:
            obj = self.get_or_404(id)
            
            # Update fields
            for field, value in data.items():
                if hasattr(obj, field):
                    setattr(obj, field, value)
            
            # Update version if available
            if hasattr(obj, 'update_version'):
                obj.update_version()
            
            self.session.add(obj)
            self.session.commit()
            self.session.refresh(obj)
            
            logger.debug(f"Updated {self.model.__name__} with ID: {id}")
            return obj
            
        except (NotFoundError, ConflictError):
            raise
        except IntegrityError as e:
            self.session.rollback()
            logger.error(f"Integrity error updating {self.model.__name__}: {e}")
            raise ConflictError(f"Update violates data constraints: {e}")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error updating {self.model.__name__}: {e}")
            raise DatabaseError(f"Failed to update record: {e}")
    
    def delete(self, id: UUID, soft_delete: bool = True) -> bool:
        """
        Delete record.
        
        Args:
            id: Record ID
            soft_delete: Whether to use soft delete if available
            
        Returns:
            True if deleted, False if not found
            
        Raises:
            DatabaseError: If database operation fails
        """
        try:
            obj = self.get(id)
            if not obj:
                return False
            
            if soft_delete and hasattr(obj, 'mark_as_deleted'):
                obj.mark_as_deleted()
                self.session.add(obj)
                logger.debug(f"Soft deleted {self.model.__name__} with ID: {id}")
            else:
                self.session.delete(obj)
                logger.debug(f"Hard deleted {self.model.__name__} with ID: {id}")
            
            self.session.commit()
            return True
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error deleting {self.model.__name__}: {e}")
            raise DatabaseError(f"Failed to delete record: {e}")
    
    def list(
        self,
        offset: int = 0,
        limit: int = 100,
        order_by: Optional[str] = None,
        order_desc: bool = False,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[ModelType]:
        """
        List records with pagination and filtering.
        
        Args:
            offset: Number of records to skip
            limit: Maximum records to return
            order_by: Field to order by
            order_desc: Whether to order descending
            filters: Filters to apply
            
        Returns:
            List of model instances
        """
        try:
            statement = select(self.model)
            
            # Add soft delete filter if model has is_deleted field
            if hasattr(self.model, 'is_deleted'):
                statement = statement.where(self.model.is_deleted == False)
            
            # Apply filters
            if filters:
                for field, value in filters.items():
                    if hasattr(self.model, field):
                        column = getattr(self.model, field)
                        if isinstance(value, list):
                            statement = statement.where(column.in_(value))
                        else:
                            statement = statement.where(column == value)
            
            # Apply ordering
            if order_by and hasattr(self.model, order_by):
                order_column = getattr(self.model, order_by)
                if order_desc:
                    statement = statement.order_by(desc(order_column))
                else:
                    statement = statement.order_by(asc(order_column))
            
            # Apply pagination
            statement = statement.offset(offset).limit(limit)
            
            results = self.session.exec(statement).all()
            
            logger.debug(f"Listed {len(results)} {self.model.__name__} records")
            return results
            
        except Exception as e:
            logger.error(f"Error listing {self.model.__name__}: {e}")
            raise DatabaseError(f"Failed to list records: {e}")
    
    def count(self, filters: Optional[Dict[str, Any]] = None) -> int:
        """
        Count records matching filters.
        
        Args:
            filters: Filters to apply
            
        Returns:
            Number of matching records
        """
        try:
            statement = select(func.count(self.model.id))
            
            # Add soft delete filter if model has is_deleted field
            if hasattr(self.model, 'is_deleted'):
                statement = statement.where(self.model.is_deleted == False)
            
            # Apply filters
            if filters:
                for field, value in filters.items():
                    if hasattr(self.model, field):
                        column = getattr(self.model, field)
                        if isinstance(value, list):
                            statement = statement.where(column.in_(value))
                        else:
                            statement = statement.where(column == value)
            
            result = self.session.exec(statement).one()
            
            logger.debug(f"Counted {result} {self.model.__name__} records")
            return result
            
        except Exception as e:
            logger.error(f"Error counting {self.model.__name__}: {e}")
            raise DatabaseError(f"Failed to count records: {e}")
    
    def exists(self, id: UUID) -> bool:
        """
        Check if record exists.
        
        Args:
            id: Record ID
            
        Returns:
            True if record exists
        """
        try:
            statement = select(func.count(self.model.id)).where(self.model.id == id)
            
            # Add soft delete filter if model has is_deleted field
            if hasattr(self.model, 'is_deleted'):
                statement = statement.where(self.model.is_deleted == False)
            
            result = self.session.exec(statement).one()
            return result > 0
            
        except Exception as e:
            logger.error(f"Error checking existence of {self.model.__name__}: {e}")
            raise DatabaseError(f"Failed to check record existence: {e}")
    
    def find_by(self, **kwargs) -> Optional[ModelType]:
        """
        Find single record by field values.
        
        Args:
            **kwargs: Field values to search for
            
        Returns:
            Model instance if found, None otherwise
        """
        try:
            statement = select(self.model)
            
            # Add soft delete filter if model has is_deleted field
            if hasattr(self.model, 'is_deleted'):
                statement = statement.where(self.model.is_deleted == False)
            
            # Add search conditions
            for field, value in kwargs.items():
                if hasattr(self.model, field):
                    column = getattr(self.model, field)
                    statement = statement.where(column == value)
            
            result = self.session.exec(statement).first()
            
            if result:
                logger.debug(f"Found {self.model.__name__} with conditions: {kwargs}")
            else:
                logger.debug(f"{self.model.__name__} not found with conditions: {kwargs}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error finding {self.model.__name__} by {kwargs}: {e}")
            raise DatabaseError(f"Failed to find record: {e}")
    
    def find_all_by(self, **kwargs) -> List[ModelType]:
        """
        Find all records by field values.
        
        Args:
            **kwargs: Field values to search for
            
        Returns:
            List of matching model instances
        """
        try:
            statement = select(self.model)
            
            # Add soft delete filter if model has is_deleted field
            if hasattr(self.model, 'is_deleted'):
                statement = statement.where(self.model.is_deleted == False)
            
            # Add search conditions
            for field, value in kwargs.items():
                if hasattr(self.model, field):
                    column = getattr(self.model, field)
                    if isinstance(value, list):
                        statement = statement.where(column.in_(value))
                    else:
                        statement = statement.where(column == value)
            
            results = self.session.exec(statement).all()
            
            logger.debug(f"Found {len(results)} {self.model.__name__} records with conditions: {kwargs}")
            return results
            
        except Exception as e:
            logger.error(f"Error finding {self.model.__name__} records by {kwargs}: {e}")
            raise DatabaseError(f"Failed to find records: {e}")
    
    def search(self, query: str, fields: List[str]) -> List[ModelType]:
        """
        Search records by text in specified fields.
        
        Args:
            query: Search query
            fields: List of field names to search in
            
        Returns:
            List of matching records
        """
        try:
            statement = select(self.model)
            
            # Add soft delete filter if model has is_deleted field
            if hasattr(self.model, 'is_deleted'):
                statement = statement.where(self.model.is_deleted == False)
            
            # Build search conditions
            search_conditions = []
            search_term = f"%{query.lower()}%"
            
            for field in fields:
                if hasattr(self.model, field):
                    column = getattr(self.model, field)
                    search_conditions.append(column.ilike(search_term))
            
            if search_conditions:
                statement = statement.where(or_(*search_conditions))
            
            results = self.session.exec(statement).all()
            
            logger.debug(f"Search for '{query}' in {fields} returned {len(results)} {self.model.__name__} records")
            return results
            
        except Exception as e:
            logger.error(f"Error searching {self.model.__name__}: {e}")
            raise DatabaseError(f"Failed to search records: {e}")
    
    def bulk_create(self, objects: List[ModelType]) -> List[ModelType]:
        """
        Create multiple records in batch.
        
        Args:
            objects: List of model instances to create
            
        Returns:
            List of created models
        """
        try:
            self.session.add_all(objects)
            self.session.commit()
            
            # Refresh all objects to get generated IDs
            for obj in objects:
                self.session.refresh(obj)
            
            logger.debug(f"Bulk created {len(objects)} {self.model.__name__} records")
            return objects
            
        except IntegrityError as e:
            self.session.rollback()
            logger.error(f"Integrity error in bulk create {self.model.__name__}: {e}")
            raise ConflictError(f"Bulk create conflicts with existing data: {e}")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error in bulk create {self.model.__name__}: {e}")
            raise DatabaseError(f"Failed to bulk create records: {e}")
    
    def bulk_update(self, updates: List[Dict[str, Any]]) -> int:
        """
        Update multiple records in batch.
        
        Args:
            updates: List of dictionaries with 'id' and update fields
            
        Returns:
            Number of records updated
        """
        try:
            updated_count = 0
            
            for update_data in updates:
                if 'id' not in update_data:
                    continue
                
                record_id = update_data.pop('id')
                obj = self.get(record_id)
                
                if obj:
                    for field, value in update_data.items():
                        if hasattr(obj, field):
                            setattr(obj, field, value)
                    
                    if hasattr(obj, 'update_version'):
                        obj.update_version()
                    
                    self.session.add(obj)
                    updated_count += 1
            
            self.session.commit()
            
            logger.debug(f"Bulk updated {updated_count} {self.model.__name__} records")
            return updated_count
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error in bulk update {self.model.__name__}: {e}")
            raise DatabaseError(f"Failed to bulk update records: {e}")
    
    def bulk_delete(self, ids: List[UUID], soft_delete: bool = True) -> int:
        """
        Delete multiple records in batch.
        
        Args:
            ids: List of record IDs to delete
            soft_delete: Whether to use soft delete if available
            
        Returns:
            Number of records deleted
        """
        try:
            deleted_count = 0
            
            for record_id in ids:
                obj = self.get(record_id)
                if obj:
                    if soft_delete and hasattr(obj, 'mark_as_deleted'):
                        obj.mark_as_deleted()
                        self.session.add(obj)
                    else:
                        self.session.delete(obj)
                    deleted_count += 1
            
            self.session.commit()
            
            logger.debug(f"Bulk deleted {deleted_count} {self.model.__name__} records")
            return deleted_count
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error in bulk delete {self.model.__name__}: {e}")
            raise DatabaseError(f"Failed to bulk delete records: {e}")


class UserRepository(Repository):
    """
    Specialized repository for User model with user-specific operations.
    """
    
    def __init__(self, session: Session):
        from .models.user import User
        super().__init__(session, User)
    
    def find_by_email(self, email: str) -> Optional:
        """Find user by email address."""
        return self.find_by(email=email)
    
    def find_by_email_or_404(self, email: str):
        """Find user by email or raise NotFoundError."""
        user = self.find_by_email(email)
        if not user:
            raise NotFoundError(resource="User", resource_id=email)
        return user
    
    def exists_by_email(self, email: str) -> bool:
        """Check if user exists with given email."""
        try:
            statement = select(func.count(self.model.id)).where(
                and_(
                    self.model.email == email,
                    self.model.is_deleted == False
                )
            )
            result = self.session.exec(statement).one()
            return result > 0
        except Exception as e:
            logger.error(f"Error checking user existence by email: {e}")
            raise DatabaseError(f"Failed to check user existence: {e}")
    
    def find_by_role(self, role: str, active_only: bool = True) -> List:
        """Find users by role."""
        filters = {'role': role}
        if active_only:
            filters['is_active'] = True
        return self.find_all_by(**filters)
    
    def find_active_users(self, limit: int = 100, offset: int = 0) -> List:
        """Find all active users."""
        return self.list(
            limit=limit,
            offset=offset,
            filters={'is_active': True, 'is_verified': True}
        )
    
    def search_users(self, query: str, limit: int = 100) -> List:
        """Search users by name or email."""
        try:
            statement = select(self.model).where(
                and_(
                    self.model.is_deleted == False,
                    or_(
                        self.model.email.ilike(f"%{query}%"),
                        self.model.first_name.ilike(f"%{query}%"),
                        self.model.last_name.ilike(f"%{query}%"),
                    )
                )
            ).limit(limit)
            
            results = self.session.exec(statement).all()
            
            logger.debug(f"User search for '{query}' returned {len(results)} results")
            return results
            
        except Exception as e:
            logger.error(f"Error searching users: {e}")
            raise DatabaseError(f"Failed to search users: {e}")
    
    def get_user_stats(self) -> Dict[str, Any]:
        """Get user statistics."""
        try:
            # Total users
            total = self.count()
            
            # Active users
            active = self.count({'is_active': True})
            
            # Verified users
            verified = self.count({'is_verified': True})
            
            # Users by role
            roles_stmt = select(self.model.role, func.count(self.model.id)).where(
                and_(
                    self.model.is_deleted == False,
                    self.model.is_active == True
                )
            ).group_by(self.model.role)
            
            role_results = self.session.exec(roles_stmt).all()
            roles_count = {role: count for role, count in role_results}
            
            return {
                'total_users': total,
                'active_users': active,
                'verified_users': verified,
                'users_by_role': roles_count,
            }
            
        except Exception as e:
            logger.error(f"Error getting user stats: {e}")
            raise DatabaseError(f"Failed to get user statistics: {e}")