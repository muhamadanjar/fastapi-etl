from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Optional, Dict, Any
from uuid import UUID

from sqlmodel import Session
from app.interfaces.dependencies import get_current_user
from app.infrastructure.db.manager import get_session_dependency
from app.schemas.entity_schemas import (
    EntityCreate, EntityUpdate, EntityResponse, EntityRelationshipResponse,
    EntitySearchRequest, EntityMergeRequest
)
from app.application.services.entity_service import EntityService
from app.schemas.remote_user import RemoteUserInfo as User

router = APIRouter()

@router.post("/", response_model=EntityResponse)
async def create_entity(
    entity_data: EntityCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> EntityResponse:
    """Create a new entity"""
    entity_service = EntityService(db)
    entity = await entity_service.create_entity(entity_data.dict())
    return EntityResponse.from_orm(entity)

@router.get("/", response_model=Dict[str, Any])
async def list_entities(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> Dict[str, Any]:
    """List entities with pagination and filters"""
    entity_service = EntityService(db)
    result = await entity_service.get_entities_list(entity_type, is_active, limit, skip)
    return result

# IMPORTANT: These non-ID routes must come BEFORE /{entity_id} to avoid UUID parsing errors
@router.post("/search", response_model=List[EntityResponse])
async def search_entities(
    search_request: EntitySearchRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> List[EntityResponse]:
    """Advanced entity search with filters and full-text search"""
    entity_service = EntityService(db)
    results = await entity_service.search_entities(search_request.search_query, search_request.entity_type)
    return results

@router.post("/merge")
async def merge_entities(
    merge_request: EntityMergeRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> Dict[str, Any]:
    """Merge multiple entities into one"""
    entity_service = EntityService(db)
    # Mark source entities as duplicates of target, then merge data
    target_id = merge_request.target_entity_id
    for source_id in merge_request.source_entity_ids:
        await entity_service.mark_as_duplicate(source_id, target_id, match_score=1.0)
    result = {"message": f"Merged {len(merge_request.source_entity_ids)} entities into {target_id}"}
    return result

@router.get("/types")
async def get_entity_types(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> List[Dict[str, Any]]:
    """Get list of available entity types with counts"""
    entity_service = EntityService(db)
    return await entity_service.get_entity_types()

# ID-based routes come AFTER non-ID routes
@router.get("/{entity_id}", response_model=EntityResponse)
async def get_entity(
    entity_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> EntityResponse:
    """Get specific entity by ID"""
    entity_service = EntityService(db)
    entity = await entity_service.get_entity_by_id(int(entity_id))
    if not entity:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Entity not found"
        )
    return EntityResponse.from_orm(entity)

@router.put("/{entity_id}", response_model=EntityResponse)
async def update_entity(
    entity_id: UUID,
    entity_data: EntityUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> EntityResponse:
    """Update entity data"""
    entity_service = EntityService(db)
    entity = await entity_service.update_entity(int(entity_id), entity_data.dict(exclude_unset=True))
    return EntityResponse.from_orm(entity)

@router.delete("/{entity_id}")
async def delete_entity(
    entity_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> Dict[str, str]:
    """Delete entity (soft delete)"""
    entity_service = EntityService(db)
    await entity_service.delete_entity(int(entity_id), hard_delete=False)
    return {"message": "Entity deleted successfully"}

@router.get("/{entity_id}/relationships", response_model=List[EntityRelationshipResponse])
async def get_entity_relationships(
    entity_id: UUID,
    relationship_type: Optional[str] = Query(None, description="Filter by relationship type"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> List[EntityRelationshipResponse]:
    """Get entity relationships"""
    entity_service = EntityService(db)
    relationships = await entity_service.get_entity_relationships(int(entity_id), relationship_type)
    return relationships

@router.post("/{entity_id}/relationships")
async def create_entity_relationship(
    entity_id: UUID,
    target_entity_id: UUID,
    relationship_type: str,
    relationship_strength: Optional[float] = Query(1.0, ge=0.0, le=1.0),
    metadata: Optional[Dict[str, Any]] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> Dict[str, str]:
    """Create relationship between entities"""
    entity_service = EntityService(db)
    relationship_data = {
        "entity_from": int(entity_id),
        "entity_to": int(target_entity_id),
        "relationship_type": relationship_type,
        "relationship_strength": relationship_strength,
        "metadata": metadata
    }
    await entity_service.create_relationship(relationship_data)
    return {"message": "Relationship created successfully"}

@router.get("/{entity_id}/history")
async def get_entity_history(
    entity_id: UUID,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> List[Dict[str, Any]]:
    """Get entity change history (placeholder - not yet implemented in service)"""
    # TODO: implement entity history tracking in EntityService
    return []

@router.post("/{entity_id}/duplicate-check")
async def check_for_duplicates(
    entity_id: UUID,
    threshold: float = Query(0.8, ge=0.0, le=1.0, description="Similarity threshold"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> List[Dict[str, Any]]:
    """Check for potential duplicate entities (placeholder - not yet implemented in service)"""
    # TODO: implement duplicate detection in EntityService
    return []
