"""
Entity service untuk mengelola processed entities dan relationships.
"""

from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, or_, func

from app.application.services.base import BaseService
from app.infrastructure.db.models.processed.entities import Entity
from app.infrastructure.db.models.processed.entity_relationships import EntityRelationship
from app.core.exceptions import EntityError
from app.utils.date_utils import get_current_timestamp


class EntityService(BaseService):
    """Service untuk mengelola entities dan relationships."""
    
    def __init__(self, db_session: Session):
        super().__init__(db_session)
    
    def get_service_name(self) -> str:
        return "EntityService"
    
    async def create_entity(self, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new entity."""
        try:
            self.validate_input(entity_data, ["entity_type", "entity_key", "entity_data"])
            self.log_operation("create_entity", {"entity_type": entity_data["entity_type"]})
            
            # Check if entity already exists
            existing_entity = await self.get_entity_by_key(
                entity_data["entity_key"], 
                entity_data["entity_type"]
            )
            
            if existing_entity:
                raise EntityError("Entity already exists")
            
            entity = Entity(
                entity_type=entity_data["entity_type"],
                entity_key=entity_data["entity_key"],
                entity_data=entity_data["entity_data"],
                source_files=entity_data.get("source_files", []),
                confidence_score=entity_data.get("confidence_score", 1.0),
                version=1,
                is_active=True
            )
            
            self.db.add(entity)
            self.db.commit()
            self.db.refresh(entity)
            
            return {
                "entity_id": entity.entity_id,
                "entity_type": entity.entity_type,
                "entity_key": entity.entity_key,
                "version": entity.version,
                "status": "created"
            }
            
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "create_entity")
    
    async def get_entity_by_id(self, entity_id: int) -> Optional[Dict[str, Any]]:
        """Get entity by ID."""
        try:
            self.log_operation("get_entity_by_id", {"entity_id": entity_id})
            
            entity = self.db.get(Entity, entity_id)
            if not entity:
                return None
            
            return {
                "entity_id": entity.entity_id,
                "entity_type": entity.entity_type,
                "entity_key": entity.entity_key,
                "entity_data": entity.entity_data,
                "source_files": entity.source_files,
                "confidence_score": entity.confidence_score,
                "version": entity.version,
                "is_active": entity.is_active,
                "last_updated": entity.last_updated
            }
            
        except Exception as e:
            self.handle_error(e, "get_entity_by_id")
    
    async def get_entity_by_key(self, entity_key: str, entity_type: str) -> Optional[Dict[str, Any]]:
        """Get entity by key and type."""
        try:
            self.log_operation("get_entity_by_key", {"entity_key": entity_key, "entity_type": entity_type})
            
            stmt = select(Entity).where(and_(
                Entity.entity_key == entity_key,
                Entity.entity_type == entity_type,
                Entity.is_active == True
            ))
            
            entity = self.db.execute(stmt).scalar_one_or_none()
            if not entity:
                return None
            
            return {
                "entity_id": entity.entity_id,
                "entity_type": entity.entity_type,
                "entity_key": entity.entity_key,
                "entity_data": entity.entity_data,
                "source_files": entity.source_files,
                "confidence_score": entity.confidence_score,
                "version": entity.version,
                "is_active": entity.is_active,
                "last_updated": entity.last_updated
            }
            
        except Exception as e:
            self.handle_error(e, "get_entity_by_key")
    
    async def update_entity(self, entity_id: int, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing entity."""
        try:
            self.log_operation("update_entity", {"entity_id": entity_id})
            
            entity = self.db.get(Entity, entity_id)
            if not entity:
                raise EntityError("Entity not found")
            
            # Update fields
            if "entity_data" in entity_data:
                entity.entity_data = entity_data["entity_data"]
            if "source_files" in entity_data:
                entity.source_files = entity_data["source_files"]
            if "confidence_score" in entity_data:
                entity.confidence_score = entity_data["confidence_score"]
            
            # Increment version
            entity.version += 1
            entity.last_updated = get_current_timestamp()
            
            self.db.commit()
            
            return {
                "entity_id": entity.entity_id,
                "entity_type": entity.entity_type,
                "entity_key": entity.entity_key,
                "version": entity.version,
                "status": "updated"
            }
            
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "update_entity")
    
    async def delete_entity(self, entity_id: int, hard_delete: bool = False) -> bool:
        """Delete an entity (soft or hard delete)."""
        try:
            self.log_operation("delete_entity", {"entity_id": entity_id, "hard_delete": hard_delete})
            
            entity = self.db.get(Entity, entity_id)
            if not entity:
                raise EntityError("Entity not found")
            
            if hard_delete:
                # Delete all relationships
                await self._delete_entity_relationships(entity_id)
                # Delete entity
                self.db.delete(entity)
            else:
                # Soft delete
                entity.is_active = False
                entity.last_updated = get_current_timestamp()
            
            self.db.commit()
            return True
            
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "delete_entity")
    
    async def get_entities_list(
        self, 
        entity_type: str = None, 
        is_active: bool = True,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """Get list of entities with pagination."""
        try:
            self.log_operation("get_entities_list", {
                "entity_type": entity_type, 
                "is_active": is_active,
                "limit": limit,
                "offset": offset
            })
            
            stmt = select(Entity)
            
            if entity_type:
                stmt = stmt.where(Entity.entity_type == entity_type)
            if is_active is not None:
                stmt = stmt.where(Entity.is_active == is_active)
            
            # Count total
            count_stmt = select(func.count(Entity.entity_id)).select_from(stmt.alias())
            total = self.db_session.execute(count_stmt).scalar()
            
            # Get data with pagination
            stmt = stmt.order_by(Entity.last_updated.desc()).limit(limit).offset(offset)
            entities = self.db_session.execute(stmt).scalars().all()
            
            return {
                "entities": [{
                    "entity_id": entity.entity_id,
                    "entity_type": entity.entity_type,
                    "entity_key": entity.entity_key,
                    "entity_data": entity.entity_data,
                    "confidence_score": entity.confidence_score,
                    "version": entity.version,
                    "is_active": entity.is_active,
                    "last_updated": entity.last_updated
                } for entity in entities],
                "pagination": {
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                    "has_next": offset + limit < total,
                    "has_prev": offset > 0
                }
            }
            
        except Exception as e:
            self.handle_error(e, "get_entities_list")
    
    async def create_relationship(self, relationship_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create relationship between entities."""
        try:
            self.validate_input(relationship_data, ["entity_from", "entity_to", "relationship_type"])
            self.log_operation("create_relationship", {
                "entity_from": relationship_data["entity_from"],
                "entity_to": relationship_data["entity_to"],
                "relationship_type": relationship_data["relationship_type"]
            })
            
            # Validate entities exist
            entity_from = self.db_session.get(Entity, relationship_data["entity_from"])
            entity_to = self.db_session.get(Entity, relationship_data["entity_to"])
            
            if not entity_from or not entity_to:
                raise EntityError("One or both entities not found")
            
            relationship = EntityRelationship(
                entity_from=relationship_data["entity_from"],
                entity_to=relationship_data["entity_to"],
                relationship_type=relationship_data["relationship_type"],
                relationship_strength=relationship_data.get("relationship_strength", 1.0),
                metadata=relationship_data.get("metadata", {})
            )
            
            self.db_session.add(relationship)
            self.db_session.commit()
            self.db_session.refresh(relationship)
            
            return {
                "relationship_id": relationship.relationship_id,
                "entity_from": relationship.entity_from,
                "entity_to": relationship.entity_to,
                "relationship_type": relationship.relationship_type,
                "status": "created"
            }
            
        except Exception as e:
            self.db_session.rollback()
            self.handle_error(e, "create_relationship")
    
    async def get_entity_relationships(self, entity_id: int, relationship_type: str = None) -> List[Dict[str, Any]]:
        """Get all relationships for an entity."""
        try:
            self.log_operation("get_entity_relationships", {"entity_id": entity_id})
            
            stmt = select(EntityRelationship).where(or_(
                EntityRelationship.entity_from == entity_id,
                EntityRelationship.entity_to == entity_id
            ))
            
            if relationship_type:
                stmt = stmt.where(EntityRelationship.relationship_type == relationship_type)
            
            relationships = self.db_session.execute(stmt).scalars().all()
            
            result = []
            for rel in relationships:
                # Get related entity info
                related_entity_id = rel.entity_to if rel.entity_from == entity_id else rel.entity_from
                related_entity = self.db_session.get(Entity, related_entity_id)
                
                result.append({
                    "relationship_id": rel.relationship_id,
                    "relationship_type": rel.relationship_type,
                    "relationship_strength": rel.relationship_strength,
                    "direction": "outgoing" if rel.entity_from == entity_id else "incoming",
                    "related_entity": {
                        "entity_id": related_entity.entity_id,
                        "entity_type": related_entity.entity_type,
                        "entity_key": related_entity.entity_key
                    } if related_entity else None,
                    "metadata": rel.metadata,
                    "created_at": rel.created_at
                })
            
            return result
            
        except Exception as e:
            self.handle_error(e, "get_entity_relationships")
    
    async def search_entities(self, search_term: str, entity_type: str = None) -> List[Dict[str, Any]]:
        """Search entities by key or data content."""
        try:
            self.log_operation("search_entities", {"search_term": search_term, "entity_type": entity_type})
            
            stmt = select(Entity).where(Entity.is_active == True)
            
            if entity_type:
                stmt = stmt.where(Entity.entity_type == entity_type)
            
            # Search in entity_key or entity_data
            search_condition = or_(
                Entity.entity_key.ilike(f"%{search_term}%"),
                Entity.entity_data.astext.ilike(f"%{search_term}%")
            )
            stmt = stmt.where(search_condition)
            
            entities = self.db_session.execute(stmt).scalars().all()
            
            return [{
                "entity_id": entity.entity_id,
                "entity_type": entity.entity_type,
                "entity_key": entity.entity_key,
                "entity_data": entity.entity_data,
                "confidence_score": entity.confidence_score,
                "last_updated": entity.last_updated
            } for entity in entities]
            
        except Exception as e:
            self.handle_error(e, "search_entities")
    
    async def get_entity_types(self) -> List[Dict[str, Any]]:
        """Get all entity types with counts."""
        try:
            self.log_operation("get_entity_types", {})
            
            stmt = select(
                Entity.entity_type,
                func.count(Entity.entity_id).label('count')
            ).where(Entity.is_active == True).group_by(Entity.entity_type)
            
            result = self.db_session.execute(stmt).all()
            
            return [{
                "entity_type": row.entity_type,
                "count": row.count
            } for row in result]
            
        except Exception as e:
            self.handle_error(e, "get_entity_types")
    
    # Private helper methods
    async def _delete_entity_relationships(self, entity_id: int):
        """Delete all relationships for an entity."""
        stmt = select(EntityRelationship).where(or_(
            EntityRelationship.entity_from == entity_id,
            EntityRelationship.entity_to == entity_id
        ))
        relationships = self.db_session.execute(stmt).scalars().all()
        
        for rel in relationships:
            self.db_session.delete(rel)
