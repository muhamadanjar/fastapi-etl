import logging
from typing import Generic, TypeVar, Type, Optional, List, Dict, Any, Union
from uuid import UUID

from sqlmodel import SQLModel, Session, select, delete, update
from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError

from ....core.exceptions import DatabaseError, NotFoundError

logger = logging.getLogger(__name__)

ModelType = TypeVar("ModelType", bound=SQLModel)


class BaseRepository(Generic[ModelType]):
    """
    Base repository class with common CRUD operations.
    """
    
    def __init__(self, model: Type[ModelType], session: Session):
        self.model = model
        self.session = session
    
    async def create(self, obj_in: Union[ModelType, Dict[str, Any]]) -> ModelType:
        """
        Create a new record.
        
        Args:
            obj_in: Model instance or dictionary with field values
            
        Returns:
            Created model instance
            
        Raises:
            DatabaseError: If creation fails
        """
        try:
            if isinstance(obj_in, dict):
                db_obj = self.model(**obj_in)
            else:
                db_obj = obj_in
            
            self.session.add(db_obj)
            self.session.flush()
            self.session.refresh(db_obj)
            
            logger.debug(f"Created {self.model.__name__} with ID: {db_obj.id}")
            return db_obj
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to create {self.model.__name__}: {e}")
            raise DatabaseError(f"Failed to create {self.model.__name__}: {str(e)}")
    
    async def get(self, id: UUID) -> Optional[ModelType]:
        """
        Get a record by ID.
        
        Args:
            id: Record ID
            
        Returns:
            Model instance or None if not found
        """
        try:
            statement = select(self.model).where(self.model.id == id)
            result = self.session.exec(statement)
            return result.first()
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to get {self.model.__name__} by ID {id}: {e}")
            raise DatabaseError(f"Failed to get {self.model.__name__}: {str(e)}")
    
    async def get_or_404(self, id: UUID) -> ModelType:
        """
        Get a record by ID or raise 404 error.
        
        Args:
            id: Record ID
            
        Returns:
            Model instance
            
        Raises:
            NotFoundError: If record not found
        """
        obj = await self.get(id)
        if not obj:
            raise NotFoundError(f"{self.model.__name__} with ID {id} not found")
        return obj
    
    async def get_multi(
        self,
        *,
        skip: int = 0,
        limit: int = 100,
        order_by: Optional[str] = None,
        **filters
    ) -> List[ModelType]:
        """
        Get multiple records with pagination and filtering.
        
        Args:
            skip: Number of records to skip
            limit: Maximum number of records to return
            order_by: Field name for ordering
            **filters: Field filters
            
        Returns:
            List of model instances
        """
        try:
            statement = select(self.model)
            
            # Apply filters
            for field, value in filters.items():
                if hasattr(self.model, field) and value is not None:
                    statement = statement.where(getattr(self.model, field) == value)
            
            # Apply ordering
            if order_by and hasattr(self.model, order_by):
                statement = statement.order_by(getattr(self.model, order_by))
            
            # Apply pagination
            statement = statement.offset(skip).limit(limit)
            
            result = self.session.exec(statement)
            return list(result.all())
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to get multiple {self.model.__name__}: {e}")
            raise DatabaseError(f"Failed to get {self.model.__name__} list: {str(e)}")
    
    async def update(
        self,
        *,
        id: UUID,
        obj_in: Union[ModelType, Dict[str, Any]]
    ) -> Optional[ModelType]:
        """
        Update a record.
        
        Args:
            id: Record ID
            obj_in: Model instance or dictionary with updated values
            
        Returns:
            Updated model instance or None if not found
            
        Raises:
            DatabaseError: If update fails
        """
        try:
            # Get existing record
            db_obj = await self.get(id)
            if not db_obj:
                return None
            
            # Update fields
            if isinstance(obj_in, dict):
                update_data = obj_in
            else:
                update_data = obj_in.dict(exclude_unset=True)
            
            for field, value in update_data.items():
                if hasattr(db_obj, field):
                    setattr(db_obj, field, value)
            
            self.session.add(db_obj)
            self.session.flush()
            self.session.refresh(db_obj)
            
            logger.debug(f"Updated {self.model.__name__} with ID: {id}")
            return db_obj
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to update {self.model.__name__} {id}: {e}")
            raise DatabaseError(f"Failed to update {self.model.__name__}: {str(e)}")
    
    async def delete(self, id: UUID) -> bool:
        """
        Delete a record.
        
        Args:
            id: Record ID
            
        Returns:
            True if deleted, False if not found
            
        Raises:
            DatabaseError: If deletion fails
        """
        try:
            statement = delete(self.model).where(self.model.id == id)
            result = self.session.exec(statement)
            
            deleted = result.rowcount > 0
            if deleted:
                logger.debug(f"Deleted {self.model.__name__} with ID: {id}")
            
            return deleted
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to delete {self.model.__name__} {id}: {e}")
            raise DatabaseError(f"Failed to delete {self.model.__name__}: {str(e)}")
    
    async def count(self, **filters) -> int:
        """
        Count records with optional filtering.
        
        Args:
            **filters: Field filters
            
        Returns:
            Number of records
        """
        try:
            statement = select(func.count(self.model.id))
            
            # Apply filters
            for field, value in filters.items():
                if hasattr(self.model, field) and value is not None:
                    statement = statement.where(getattr(self.model, field) == value)
            
            result = self.session.exec(statement)
            return result.one()
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to count {self.model.__name__}: {e}")
            raise DatabaseError(f"Failed to count {self.model.__name__}: {str(e)}")
    
    async def exists(self, id: UUID) -> bool:
        """
        Check if a record exists.
        
        Args:
            id: Record ID
            
        Returns:
            True if exists, False otherwise
        """
        try:
            statement = select(self.model.id).where(self.model.id == id)
            result = self.session.exec(statement)
            return result.first() is not None
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to check {self.model.__name__} existence {id}: {e}")
            raise DatabaseError(f"Failed to check {self.model.__name__} existence: {str(e)}")
