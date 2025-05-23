from abc import ABC
from datetime import datetime
from typing import Any, Optional
from uuid import UUID, uuid4

class BaseEntity(ABC):

    def __init__(self, entity_id: Optional[UUID] = None):
        """
        Initialize entity with optional ID.
        
        Args:
            entity_id: Unique identifier for the entity.
                      If not provided, a new UUID will be generated.
        """
        self._id: UUID = entity_id or uuid4()
        self._created_at: datetime = datetime.utcnow()
        self._updated_at: datetime = datetime.utcnow()
        self._version: int = 1
        self._deleted_at: Optional[datetime] = None
        
    @property
    def id(self) -> UUID:
        """Get entity ID."""
        return self._id
    
    @property
    def created_at(self) -> datetime:
        """Get creation timestamp."""
        return self._created_at
    
    @property
    def updated_at(self) -> datetime:
        """Get last update timestamp."""
        return self._updated_at
    
    @property
    def version(self) -> int:
        """Get entity version for optimistic locking."""
        return self._version
    
    def mark_as_updated(self) -> None:
        """Mark entity as updated and increment version."""
        self._updated_at = datetime.utcnow()
        self._version += 1
    
    def __eq__(self, other: Any) -> bool:
        """
        Compare entities by identity.
        
        Args:
            other: Object to compare with
            
        Returns:
            True if entities have the same ID and type
        """
        if not isinstance(other, Entity):
            return False
        
        return self._id == other._id and type(self) is type(other)
    
    def __hash__(self) -> int:
        """
        Hash entity by its ID and type.
        
        Returns:
            Hash value based on entity ID and type
        """
        return hash((self._id, type(self)))
    
    def __repr__(self) -> str:
        """
        String representation of entity.
        
        Returns:
            String representation showing class name and ID
        """
        return f"{self.__class__.__name__}(id={self._id})"