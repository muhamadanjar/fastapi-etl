import logging
from typing import Generic, TypeVar, Type, Optional, List, Dict, Any, Union
from uuid import UUID

from sqlmodel import SQLModel, Session, select, delete, update
from sqlmodel import and_, asc, desc, inspect, or_, select, func
# from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError

from ....core.exceptions import DatabaseError, NotFoundError

logger = logging.getLogger(__name__)

ModelType = TypeVar("ModelType", bound=SQLModel)


class BaseRepository(Generic[ModelType]):
    """
    Base repository class with common CRUD operations.
    """

    # Example usage:
    """
    # Simple filtering with short syntax
    criteria = {
        'and': [
            ['name', 'like', 'john'],
            ['age', '>=', 18],
            ['is_active', True]  # Short form for ['is_active', '=', True]
        ]
    }

    # Complex nested filtering
    criteria = {
        'and': [
            ['is_active', True],
            {
                'or': [
                    ['user.email', 'like', 'admin'],
                    ['user.role', 'admin']  # Short form
                ]
            }
        ]
    }

    # Mixed usage (backward compatibility)
    criteria = {
        'and': [
            ['name', 'like', 'john'],  # New array format
            {'field': 'age', 'operator': '>=', 'value': 18},  # Old dict format
            ['status', 'active']  # Short array format
        ]
    }

    # Sorting (unchanged)
    sort_by = [
        {'field': 'user.name', 'direction': 'asc'},
        {'field': 'created_at', 'direction': 'desc'}
    ]

    # Eager loading (unchanged)
    load = ['user', 'user.profile', 'tags']

    # Pagination examples
    # Example usage:

    # Basic pagination
    result = await repo.paginate(
        skip=0,
        limit=10,
        criteria={
            'and': [
                ['is_active', True],
                ['name', 'like', 'john']
            ]
        }
    )

    # Result structure:
    {
        "data": [...],
        "meta": {
            "total": 150,
            "per_page": 10,
            "current_page": 1,
            "total_pages": 15,
            "from": 1,
            "to": 10,
            "has_next": True,
            "has_prev": False
        }
    }

    # High-performance pagination (without total count)
    result = await repo.paginate(
        skip=20,
        limit=10,
        criteria={'and': [['status', 'active']]},
        include_total=False  # Faster, no count query
    )

    # Cursor-based pagination (best for large datasets)
    result = await repo.paginate_cursor(
        cursor_field="created_at",
        cursor_value="2024-01-01T00:00:00",
        limit=10,
        direction="next",
        criteria={'and': [['is_published', True]]}
    )

    # Result structure:
    {
        "data": [...],
        "next_cursor": "2024-01-15T10:30:00",
        "prev_cursor": "2024-01-01T08:15:00",
        "has_next": True,
        "has_prev": True
    }


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
    


    def _get_column_by_path(self, path: str):
        """Get column from nested path like 'user.email'."""
        parts = path.split('.')
        current_model = self.model
        column = None
        
        for part in parts:
            mapper = inspect(current_model)
            if part in mapper.columns:
                column = mapper.columns[part]
            elif part in mapper.relationships:
                rel = mapper.relationships[part]
                current_model = rel.mapper.class_
            else:
                raise ValueError(f"'{part}' is not a valid field or relation on '{current_model.__name__}'")
        
        if column is None:
            raise ValueError(f"Column not found for path '{path}'")
        
        return column
    
    def _get_load_options(self, load: List[str]):
        """Get SQLAlchemy load options for eager loading."""
        options = []
        mapper = inspect(self.model)
        
        for relation_path in load:
            if '.' in relation_path:
                # Handle nested relations like 'user.profile'
                parts = relation_path.split('.')
                current_option = None
                current_model = self.model
                
                for part in parts:
                    current_mapper = inspect(current_model)
                    if part in current_mapper.relationships:
                        rel = current_mapper.relationships[part]
                        if current_option is None:
                            current_option = selectinload(getattr(current_model, part))
                        else:
                            current_option = current_option.selectinload(getattr(current_model, part))
                        current_model = rel.mapper.class_
                    else:
                        raise ValueError(f"Relation '{part}' not found in {current_model.__name__}")
                
                if current_option:
                    options.append(current_option)
            else:
                # Simple relation
                if relation_path in mapper.relationships:
                    options.append(selectinload(getattr(self.model, relation_path)))
                else:
                    raise ValueError(f"Relation '{relation_path}' not found in {self.model.__name__}")
        
        return options
    
    def _apply_condition(self, column, operator: str, value: Any):
        """Apply condition based on operator."""
        try:
            # Get column type for proper casting
            try:
                python_type = column.type.python_type
            except (NotImplementedError, AttributeError):
                python_type = str
            
            # Convert value to appropriate type
            if python_type is bool and isinstance(value, str):
                converted_value = value.lower() in ("true", "1", "yes", "on")
            elif python_type is not str and value is not None:
                try:
                    converted_value = python_type(value)
                except (ValueError, TypeError):
                    converted_value = value
            else:
                converted_value = value
            
            # Apply operator
            if operator == '=':
                return column == converted_value
            elif operator == '!=':
                return column != converted_value
            elif operator == '>':
                return column > converted_value
            elif operator == '>=':
                return column >= converted_value
            elif operator == '<':
                return column < converted_value
            elif operator == '<=':
                return column <= converted_value
            elif operator == 'like':
                return column.ilike(f"%{converted_value}%")
            elif operator == 'not_like':
                return ~column.ilike(f"%{converted_value}%")
            elif operator == 'in':
                if isinstance(converted_value, (list, tuple)):
                    return column.in_(converted_value)
                else:
                    return column.in_([converted_value])
            elif operator == 'not_in':
                if isinstance(converted_value, (list, tuple)):
                    return ~column.in_(converted_value)
                else:
                    return ~column.in_([converted_value])
            elif operator == 'is_null':
                return column.is_(None)
            elif operator == 'is_not_null':
                return column.is_not(None)
            else:
                raise ValueError(f"Unsupported operator: {operator}")
                
        except Exception as e:
            raise ValueError(f"Error applying condition for column '{column.name}': {str(e)}")
    
    def _parse_condition(self, condition):
        """
        Parse condition in multiple formats:
        1. List: ['field', 'operator', 'value'] or ['field', 'value'] (assumes '=' operator)
        2. Dict: {'field': 'user.email', 'operator': '=', 'value': 'test@example.com'} (backward compatibility)
        """
        if isinstance(condition, list):
            if len(condition) == 2:
                # ['field', 'value'] - assume '=' operator
                field, value = condition
                operator = '='
            elif len(condition) == 3:
                # ['field', 'operator', 'value']
                field, operator, value = condition
            else:
                raise ValueError("List condition must have 2 or 3 elements: ['field', 'value'] or ['field', 'operator', 'value']")
        
        elif isinstance(condition, dict):
            # Backward compatibility with dict format
            required_keys = {'field', 'operator', 'value'}
            if not required_keys.issubset(condition.keys()):
                raise ValueError(f"Dict condition must contain keys: {required_keys}")
            
            field = condition['field']
            operator = condition['operator']
            value = condition['value']
        
        else:
            raise ValueError("Condition must be a list or dictionary")
        
        column = self._get_column_by_path(field)
        return self._apply_condition(column, operator, value)
    
    def _parse_criteria(self, criteria: Dict[str, Any]):
        """Parse criteria structure recursively."""
        conditions = []
        
        # Handle 'and' conditions
        if 'and' in criteria and criteria['and']:
            and_conditions = []
            for condition in criteria['and']:
                if isinstance(condition, dict) and ('and' in condition or 'or' in condition):
                    # Nested logical operator
                    and_conditions.append(self._parse_criteria(condition))
                else:
                    # Simple condition
                    and_conditions.append(self._parse_condition(condition))
            
            if and_conditions:
                conditions.append(and_(*and_conditions))
        
        # Handle 'or' conditions
        if 'or' in criteria and criteria['or']:
            or_conditions = []
            for condition in criteria['or']:
                if isinstance(condition, dict) and ('and' in condition or 'or' in condition):
                    # Nested logical operator
                    or_conditions.append(self._parse_criteria(condition))
                else:
                    # Simple condition
                    or_conditions.append(self._parse_condition(condition))
            
            if or_conditions:
                conditions.append(or_(*or_conditions))
        
        # If both 'and' and 'or' exist at the same level, combine with AND
        if len(conditions) > 1:
            return and_(*conditions)
        elif len(conditions) == 1:
            return conditions[0]
        else:
            return None
    
    def _parse_sort(self, sort_by: List[Dict[str, str]]):
        """Parse sort criteria: [{'field': 'user.name', 'direction': 'asc'}]"""
        order_items = []
        
        for sort_item in sort_by:
            if not isinstance(sort_item, dict) or 'field' not in sort_item:
                raise ValueError("Sort item must be a dict with 'field' key")
            
            field = sort_item['field']
            direction = sort_item.get('direction', 'asc').lower()
            
            column = self._get_column_by_path(field)
            
            if direction == 'asc':
                order_items.append(asc(column))
            elif direction == 'desc':
                order_items.append(desc(column))
            else:
                raise ValueError(f"Invalid sort direction: {direction}. Use 'asc' or 'desc'")
        
        return order_items
    
    def build_query(
        self,
        criteria: Optional[Dict[str, Any]] = None,
        sort_by: Optional[List[Dict[str, str]]] = None,
        load: Optional[List[str]] = None
    ):
        """
        Build query with flexible filtering, sorting, and loading.
        
        Args:
            criteria: Filtering criteria structure
            sort_by: List of sort specifications
            load: List of relations to eager load
            
        Example criteria:
        {
            'and': [
                ['name', 'like', 'john'],
                ['age', '>=', 18],
                ['status', 'active'],  # Short form, assumes '=' operator
                {
                    'or': [
                        ['user.email', '=', 'admin@example.com'],
                        ['user.role', 'admin']  # Short form
                    ]
                }
            ]
        }
        
        Example sort_by:
        [
            {'field': 'user.name', 'direction': 'asc'},
            {'field': 'created_at', 'direction': 'desc'}
        ]
        
        Example load:
        ['user', 'user.profile', 'tags']
        """
        query = select(self.model)
        
        # Apply eager loading
        if load:
            load_options = self._get_load_options(load)
            if load_options:
                query = query.options(*load_options)
        
        # Apply filtering
        if criteria:
            where_condition = self._parse_criteria(criteria)
            if where_condition is not None:
                query = query.where(where_condition)
        
        # Apply sorting
        if sort_by:
            order_items = self._parse_sort(sort_by)
            if order_items:
                query = query.order_by(*order_items)
        
        return query
    

    async def count_filtered(
        self,
        criteria: Optional[Dict[str, Any]] = None
    ) -> int:
        """Count records with filtering (optimized version)."""
        try:
            # Build base query for counting
            query = select(func.count(self.model.id))
            
            # Apply filtering
            if criteria:
                where_condition = self._parse_criteria(criteria)
                if where_condition is not None:
                    query = query.where(where_condition)
            
            result = self.session.exec(query)
            return result.one()
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to count filtered {self.model.__name__}: {e}")
            raise DatabaseError(f"Failed to count {self.model.__name__}: {str(e)}")
        except ValueError as e:
            logger.error(f"Invalid criteria: {e}")
            raise DatabaseError(f"Invalid criteria: {str(e)}")
    
    async def paginate(
        self,
        *,
        skip: int = 0,
        limit: int = 10,
        criteria: Optional[Dict[str, Any]] = None,
        sort_by: Optional[List[Dict[str, str]]] = None,
        load: Optional[List[str]] = None,
        include_total: bool = True
    ) -> Dict[str, Any]:
        """
        Paginate records with filtering, sorting, and metadata.
        
        Args:
            skip: Number of records to skip
            limit: Maximum number of records per page
            criteria: Filtering criteria
            sort_by: Sort specifications
            load: Relations to eager load
            include_total: Whether to include total count (set False for better performance)
            
        Returns:
            Dict with 'data' and 'meta' keys
        """
        try:
            # Get paginated data
            query = self.build_query(criteria=criteria, sort_by=sort_by, load=load)
            query = query.offset(skip).limit(limit)
            
            result = self.session.exec(query)
            data = list(result.all())
            
            # Calculate metadata
            current_page = (skip // limit) + 1 if limit > 0 else 1
            
            meta = {
                "per_page": limit,
                "current_page": current_page,
                "from": skip + 1 if data else 0,
                "to": skip + len(data) if data else 0,
                "has_next": len(data) == limit,  # If we got full page, likely has more
                "has_prev": skip > 0
            }
            
            # Include total count if requested (slower but complete info)
            if include_total:
                total = await self.count_filtered(criteria=criteria)
                meta.update({
                    "total": total,
                    "total_pages": ceil(total / limit) if limit > 0 else 1,
                    "has_next": (skip + limit) < total,  # More accurate with total
                })
            
            return {
                "data": data,
                "meta": meta
            }
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to paginate {self.model.__name__}: {e}")
            raise DatabaseError(f"Failed to paginate {self.model.__name__}: {str(e)}")
        except ValueError as e:
            logger.error(f"Invalid pagination parameters: {e}")
            raise DatabaseError(f"Invalid pagination parameters: {str(e)}")
    
    async def paginate_cursor(
        self,
        *,
        cursor_field: str = "id",
        cursor_value: Optional[Any] = None,
        limit: int = 10,
        direction: str = "next",  # "next" or "prev"
        criteria: Optional[Dict[str, Any]] = None,
        sort_by: Optional[List[Dict[str, str]]] = None,
        load: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Cursor-based pagination (more efficient for large datasets).
        
        Args:
            cursor_field: Field to use for cursor (should be indexed)
            cursor_value: Value to start from
            limit: Maximum number of records
            direction: "next" or "prev"
            criteria: Filtering criteria
            sort_by: Sort specifications
            load: Relations to eager load
            
        Returns:
            Dict with 'data', 'next_cursor', 'prev_cursor'
        """
        try:
            # Build base query
            query = self.build_query(criteria=criteria, sort_by=sort_by, load=load)
            
            # Add cursor condition
            if cursor_value is not None:
                cursor_column = self._get_column_by_path(cursor_field)
                if direction == "next":
                    query = query.where(cursor_column > cursor_value)
                else:  # prev
                    query = query.where(cursor_column < cursor_value)
            
            # Apply limit
            query = query.limit(limit + 1)  # +1 to check if there are more records
            
            result = self.session.exec(query)
            data = list(result.all())
            
            # Check if there are more records
            has_more = len(data) > limit
            if has_more:
                data = data[:limit]  # Remove extra record
            
            # Get cursor values
            next_cursor = None
            prev_cursor = None
            
            if data:
                last_record = data[-1]
                first_record = data[0]
                
                # Get cursor values from records
                cursor_parts = cursor_field.split('.')
                next_cursor_value = last_record
                prev_cursor_value = first_record
                
                for part in cursor_parts:
                    next_cursor_value = getattr(next_cursor_value, part)
                    prev_cursor_value = getattr(prev_cursor_value, part)
                
                if has_more or direction == "prev":
                    next_cursor = next_cursor_value
                if cursor_value is not None or direction == "next":
                    prev_cursor = prev_cursor_value
            
            return {
                "data": data,
                "next_cursor": next_cursor,
                "prev_cursor": prev_cursor,
                "has_next": has_more if direction == "next" else None,
                "has_prev": cursor_value is not None
            }
            
        except SQLAlchemyError as e:
            logger.error(f"Failed cursor pagination for {self.model.__name__}: {e}")
            raise DatabaseError(f"Failed cursor pagination: {str(e)}")
        except ValueError as e:
            logger.error(f"Invalid cursor pagination parameters: {e}")

# Dari root directory proyek
