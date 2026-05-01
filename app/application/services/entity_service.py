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
    
    async def merge_entity_data(
        self,
        entity_id: int,
        new_data: Dict[str, Any],
        conflict_strategy: str = "newer_wins",
        confidence_score: float = 1.0
    ) -> Dict[str, Any]:
        """
        Merge new data into existing entity with conflict resolution.

        Strategies:
        - newer_wins: New values always replace existing
        - score_based: Use value with higher confidence score
        - conservative: Keep existing values
        - merge: Intelligently merge objects/arrays

        Args:
            entity_id: ID of entity to merge into
            new_data: New data to merge
            conflict_strategy: Strategy for conflict resolution
            confidence_score: Confidence of new data (0.0-1.0)

        Returns:
            Updated entity data
        """
        try:
            self.log_operation("merge_entity_data", {
                "entity_id": entity_id,
                "strategy": conflict_strategy,
                "confidence": confidence_score
            })

            entity = self.db_session.get(Entity, entity_id)
            if not entity:
                raise EntityError(f"Entity {entity_id} not found")

            existing_data = entity.entity_data or {}
            merged_data = self._apply_merge_strategy(
                existing_data, new_data, conflict_strategy, confidence_score
            )

            return merged_data

        except Exception as e:
            self.handle_error(e, "merge_entity_data")

    def _apply_merge_strategy(
        self,
        existing_data: Dict[str, Any],
        new_data: Dict[str, Any],
        strategy: str,
        confidence: float
    ) -> Dict[str, Any]:
        """Apply conflict resolution strategy to merge data."""
        merged = existing_data.copy()

        if strategy == "newer_wins":
            # New values always win
            merged.update(new_data)

        elif strategy == "score_based":
            # Higher confidence wins - use new data if confidence >= 0.9
            if confidence >= 0.9:
                merged.update(new_data)
            else:
                # Selective merge for moderate confidence
                for key, new_value in new_data.items():
                    if key not in merged:
                        merged[key] = new_value
                    elif isinstance(new_value, (int, float)) and isinstance(merged.get(key), (int, float)):
                        merged[key] = max(merged[key], new_value)
                    elif isinstance(new_value, list):
                        merged[key] = list(set(merged.get(key, []) + (new_value or [])))

        elif strategy == "conservative":
            # Keep existing values, only add new fields
            for key, new_value in new_data.items():
                if key not in merged:
                    merged[key] = new_value

        elif strategy == "merge":
            # Intelligent merge: dicts recursively, lists uniquely
            for key, new_value in new_data.items():
                if key not in merged:
                    merged[key] = new_value
                elif isinstance(new_value, dict) and isinstance(merged[key], dict):
                    merged[key] = {**merged[key], **new_value}
                elif isinstance(new_value, list):
                    merged[key] = list(set(merged.get(key, []) + (new_value or [])))

        return merged

    async def update_entity_with_lineage(
        self,
        entity_id: int,
        new_data: Dict[str, Any],
        source_record_id: int,
        job_execution_id: int,
        change_reason: str = "Data update"
    ) -> Dict[str, Any]:
        """
        Update entity and create lineage + change log records.

        Args:
            entity_id: Entity to update
            new_data: New data to merge
            source_record_id: Source record ID for lineage
            job_execution_id: Job execution ID
            change_reason: Reason for change

        Returns:
            Updated entity info
        """
        try:
            from app.infrastructure.db.models.audit.change_log import ChangeLog
            from app.infrastructure.db.models.audit.data_lineage import DataLineage

            self.log_operation("update_entity_with_lineage", {"entity_id": entity_id})

            entity = self.db_session.get(Entity, entity_id)
            if not entity:
                raise EntityError(f"Entity {entity_id} not found")

            # Merge data
            old_data = entity.entity_data or {}
            merged_data = self._apply_merge_strategy(old_data, new_data, "newer_wins", 1.0)

            # Create change log
            change_log = ChangeLog(
                entity_id=entity_id,
                change_type="UPDATE",
                old_value=old_data,
                new_value=merged_data,
                change_details={"reason": change_reason}
            )
            self.db_session.add(change_log)

            # Create lineage
            lineage = DataLineage(
                source_entity_id=source_record_id,
                source_entity_type="StandardizedData",
                target_entity_id=entity_id,
                target_entity_type=entity.entity_type,
                job_execution_id=job_execution_id,
                lineage_metadata={"reason": change_reason}
            )
            self.db_session.add(lineage)

            # Update entity
            entity.entity_data = merged_data
            entity.version += 1
            entity.last_updated = get_current_timestamp()
            self.db_session.add(entity)
            self.db_session.commit()

            return {
                "entity_id": entity_id,
                "version": entity.version,
                "status": "updated_with_lineage"
            }

        except Exception as e:
            self.db_session.rollback()
            self.handle_error(e, "update_entity_with_lineage")

    async def mark_as_duplicate(
        self,
        duplicate_entity_id: int,
        master_entity_id: int,
        match_score: float = 0.0,
        match_metadata: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Mark entity as duplicate of another entity.

        Args:
            duplicate_entity_id: Entity identified as duplicate
            master_entity_id: Primary/master entity
            match_score: Similarity score (0.0-1.0)
            match_metadata: Additional match information

        Returns:
            Duplicate marking result
        """
        try:
            from app.infrastructure.db.models.processed.entity_relationships import EntityRelationship

            self.log_operation("mark_as_duplicate", {
                "duplicate_id": duplicate_entity_id,
                "master_id": master_entity_id
            })

            duplicate = self.db_session.get(Entity, duplicate_entity_id)
            master = self.db_session.get(Entity, master_entity_id)

            if not duplicate or not master:
                raise EntityError("One or both entities not found")

            # Update master: increment duplicate_count
            master.duplicate_count = (master.duplicate_count or 0) + 1
            if not master.master_entity_id:
                master.master_entity_id = master_entity_id
            self.db_session.add(master)

            # Create duplicate_of relationship
            relationship = EntityRelationship(
                entity_from=duplicate_entity_id,
                entity_to=master_entity_id,
                relationship_type="duplicate_of",
                relationship_strength=float(match_score),
                metadata=match_metadata or {}
            )
            self.db_session.add(relationship)
            self.db_session.commit()

            return {
                "duplicate_entity_id": duplicate_entity_id,
                "master_entity_id": master_entity_id,
                "status": "marked_duplicate"
            }

        except Exception as e:
            self.db_session.rollback()
            self.handle_error(e, "mark_as_duplicate")

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
