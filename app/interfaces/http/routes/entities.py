from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlmodel import Session
from typing import List, Optional, Dict, Any
from uuid import UUID

from app.dependencies import get_db, get_current_user
from app.schemas.entity_schemas import (
    EntityCreate, EntityUpdate, EntityResponse, EntityRelationshipResponse,
    EntitySearchRequest, EntityMergeRequest
)
from app.services.entity_service import EntityService
from app.models.base import User

router = APIRouter()

@router.post("/", response_model=EntityResponse)
async def create_entity(
    entity_data: EntityCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> EntityResponse:
    """Create a new entity"""
    entity_service = EntityService(db)
    entity = await entity_service.create_entity(entity_data, current_user.id)
    return EntityResponse.from_orm(entity)

@router.get("/", response_model=List[EntityResponse])
async def list_entities(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> List[EntityResponse]:
    """List entities with pagination and filters"""
    entity_service = EntityService(db)
    return await entity_service.list_entities(skip, limit, entity_type, is_active)

@router.get("/{entity_id}", response_model=EntityResponse)
async def get_entity(
    entity_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> EntityResponse:
    """Get specific entity by ID"""
    entity_service = EntityService(db)
    entity = await entity_service.get_entity(entity_id)
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
    db: Session = Depends(get_db)
) -> EntityResponse:
    """Update entity data"""
    entity_service = EntityService(db)
    entity = await entity_service.update_entity(entity_id, entity_data, current_user.id)
    return EntityResponse.from_orm(entity)

@router.delete("/{entity_id}")
async def delete_entity(
    entity_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Delete entity (soft delete)"""
    entity_service = EntityService(db)
    await entity_service.delete_entity(entity_id, current_user.id)
    return {"message": "Entity deleted successfully"}

@router.post("/search", response_model=List[EntityResponse])
async def search_entities(
    search_request: EntitySearchRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> List[EntityResponse]:
    """Advanced entity search with filters and full-text search"""
    entity_service = EntityService(db)
    return await entity_service.search_entities(search_request)

@router.get("/{entity_id}/relationships", response_model=List[EntityRelationshipResponse])
async def get_entity_relationships(
    entity_id: UUID,
    relationship_type: Optional[str] = Query(None, description="Filter by relationship type"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> List[EntityRelationshipResponse]:
    """Get entity relationships"""
    entity_service = EntityService(db)
    return await entity_service.get_entity_relationships(entity_id, relationship_type)

@router.post("/{entity_id}/relationships")
async def create_entity_relationship(
    entity_id: UUID,
    target_entity_id: UUID,
    relationship_type: str,
    relationship_strength: Optional[float] = Query(1.0, ge=0.0, le=1.0),
    metadata: Optional[Dict[str, Any]] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Create relationship between entities"""
    entity_service = EntityService(db)
    await entity_service.create_entity_relationship(
        entity_id, target_entity_id, relationship_type, relationship_strength, metadata, current_user.id
    )
    return {"message": "Relationship created successfully"}

@router.post("/merge")
async def merge_entities(
    merge_request: EntityMergeRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Merge multiple entities into one"""
    entity_service = EntityService(db)
    result = await entity_service.merge_entities(merge_request, current_user.id)
    return result

@router.get("/types")
async def get_entity_types(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> List[Dict[str, Any]]:
    """Get list of available entity types with counts"""
    entity_service = EntityService(db)
    return await entity_service.get_entity_types()

@router.get("/{entity_id}/history")
async def get_entity_history(
    entity_id: UUID,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> List[Dict[str, Any]]:
    """Get entity change history"""
    entity_service = EntityService(db)
    return await entity_service.get_entity_history(entity_id, skip, limit)

@router.post("/{entity_id}/duplicate-check")
async def check_for_duplicates(
    entity_id: UUID,
    threshold: float = Query(0.8, ge=0.0, le=1.0, description="Similarity threshold"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> List[Dict[str, Any]]:
    """Check for potential duplicate entities"""
    entity_service = EntityService(db)
    return await entity_service.check_for_duplicates(entity_id, threshold)
