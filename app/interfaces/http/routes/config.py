from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Optional, Dict, Any
from uuid import UUID

from sqlmodel import Session
from app.interfaces.dependencies import get_current_user, require_roles
from app.infrastructure.db.manager import get_session_dependency
from app.application.services.config_service import ConfigService
from app.schemas.remote_user import RemoteUserInfo as User
from app.core.response import APIResponse

router = APIRouter()

# ============================================================================
# DATA SOURCES ROUTES
# ============================================================================

@router.get("/data-sources/types")
async def get_data_source_types(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
):
    """Get available data source types"""
    from app.infrastructure.db.models.config.data_sources import SourceType
    return APIResponse.success(data={
        "types": [st.value for st in SourceType]
    })


@router.post("/data-sources/test-connection")
async def test_data_source_connection(
    data: Dict[str, Any],
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
):
    """Test connection for a data source"""
    config_service = ConfigService(db)
    source_id = data.get("source_id")
    if not source_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="source_id is required"
        )
    result = await config_service.test_connection(UUID(source_id))
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data source not found"
        )
    return APIResponse.success(data=result)


@router.get("/data-sources")
async def list_data_sources(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    source_type: Optional[str] = Query(None, description="Filter by source type"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
):
    """List data sources with pagination and filters"""
    config_service = ConfigService(db)
    result = await config_service.list_data_sources(
        source_type=source_type,
        is_active=is_active,
        limit=limit,
        offset=skip
    )
    return APIResponse.success(data=result)


@router.post("/data-sources")
async def create_data_source(
    data: Dict[str, Any],
    current_user: User = Depends(require_roles()),
    db: Session = Depends(get_session_dependency)
):
    """Create a new data source"""
    config_service = ConfigService(db)
    result = await config_service.create_data_source(data)
    return APIResponse.success(data=result)


@router.get("/data-sources/{source_id}")
async def get_data_source(
    source_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
):
    """Get a specific data source"""
    config_service = ConfigService(db)
    result = await config_service.get_data_source(source_id)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data source not found"
        )
    return APIResponse.success(data=result)


@router.put("/data-sources/{source_id}")
async def update_data_source(
    source_id: UUID,
    data: Dict[str, Any],
    current_user: User = Depends(require_roles()),
    db: Session = Depends(get_session_dependency)
):
    """Update a data source"""
    config_service = ConfigService(db)
    result = await config_service.update_data_source(source_id, data)
    return APIResponse.success(data=result)


@router.delete("/data-sources/{source_id}")
async def delete_data_source(
    source_id: UUID,
    current_user: User = Depends(require_roles()),
    db: Session = Depends(get_session_dependency)
):
    """Delete a data source (soft delete)"""
    config_service = ConfigService(db)
    success = await config_service.delete_data_source(source_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data source not found"
        )
    return APIResponse.success(data={"message": "Data source deleted successfully"})


# ============================================================================
# DATA DICTIONARY ROUTES
# ============================================================================

@router.get("/data-dictionary")
async def list_data_dictionary(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    entity_name: Optional[str] = Query(None, description="Filter by entity name"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
):
    """List data dictionary entries with pagination"""
    config_service = ConfigService(db)
    result = await config_service.list_data_dictionary(
        entity_name=entity_name,
        limit=limit,
        offset=skip
    )
    return APIResponse.success(data=result)


@router.post("/data-dictionary")
async def create_data_dictionary_entry(
    data: Dict[str, Any],
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
):
    """Create a data dictionary entry"""
    config_service = ConfigService(db)
    result = await config_service.create_data_dictionary_entry(data)
    return APIResponse.success(data=result)


@router.get("/data-dictionary/{dict_id}")
async def get_data_dictionary_entry(
    dict_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
):
    """Get a specific data dictionary entry"""
    config_service = ConfigService(db)
    result = await config_service.get_data_dictionary_entry(dict_id)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dictionary entry not found"
        )
    return APIResponse.success(data=result)


@router.put("/data-dictionary/{dict_id}")
async def update_data_dictionary_entry(
    dict_id: UUID,
    data: Dict[str, Any],
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
):
    """Update a data dictionary entry"""
    config_service = ConfigService(db)
    result = await config_service.update_data_dictionary_entry(dict_id, data)
    return APIResponse.success(data=result)


@router.delete("/data-dictionary/{dict_id}")
async def delete_data_dictionary_entry(
    dict_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
):
    """Delete a data dictionary entry"""
    config_service = ConfigService(db)
    success = await config_service.delete_data_dictionary_entry(dict_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dictionary entry not found"
        )
    return APIResponse.success(data={"message": "Dictionary entry deleted successfully"})


# ============================================================================
# SYSTEM CONFIG ROUTES
# ============================================================================

@router.get("/system-config/by-key")
async def get_config_by_key(
    config_category: str = Query(..., description="Config category"),
    config_key: str = Query(..., description="Config key"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
):
    """Get configuration by category and key"""
    config_service = ConfigService(db)
    result = await config_service.get_config_by_key(config_category, config_key)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Configuration not found"
        )
    return APIResponse.success(data=result)


@router.get("/system-config")
async def list_system_config(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    config_category: Optional[str] = Query(None, description="Filter by config category"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
):
    """List system configuration with pagination"""
    config_service = ConfigService(db)
    result = await config_service.list_system_config(
        config_category=config_category,
        limit=limit,
        offset=skip
    )
    return APIResponse.success(data=result)


@router.post("/system-config")
async def create_system_config(
    data: Dict[str, Any],
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
):
    """Create a system configuration"""
    config_service = ConfigService(db)
    result = await config_service.create_config(data)
    return APIResponse.success(data=result)


@router.get("/system-config/{config_id}")
async def get_system_config(
    config_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
):
    """Get a specific system configuration"""
    config_service = ConfigService(db)
    result = await config_service.get_config(config_id)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Configuration not found"
        )
    return APIResponse.success(data=result)


@router.put("/system-config/{config_id}")
async def update_system_config(
    config_id: UUID,
    data: Dict[str, Any],
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
):
    """Update a system configuration"""
    config_service = ConfigService(db)
    result = await config_service.update_config(config_id, data)
    return APIResponse.success(data=result)


@router.delete("/system-config/{config_id}")
async def delete_system_config(
    config_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
):
    """Delete a system configuration"""
    config_service = ConfigService(db)
    success = await config_service.delete_config(config_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Configuration not found"
        )
    return APIResponse.success(data={"message": "Configuration deleted successfully"})
