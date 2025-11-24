"""
API routes for cache management.
"""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session

from app.infrastructure.db.connection import get_session
from app.services.cache_service import get_cache_service
from app.interfaces.dependencies import get_current_user
from app.core.response import APIResponse

router = APIRouter(prefix="/cache", tags=["Cache Management"])


@router.get("/stats", response_model=APIResponse[dict])
async def get_cache_stats(
    current_user = Depends(get_current_user)
):
    """
    Get cache statistics.
    """
    try:
        cache = await get_cache_service()
        stats = await cache.get_stats()
        
        return APIResponse.success(
            data=stats,
            message="Cache statistics retrieved successfully"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health", response_model=APIResponse[dict])
async def get_cache_health(
    current_user = Depends(get_current_user)
):
    """
    Check cache health status.
    """
    try:
        cache = await get_cache_service()
        health = await cache.health_check()
        
        return APIResponse.success(
            data=health,
            message="Cache health check completed"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/clear", response_model=APIResponse[bool])
async def clear_all_cache(
    current_user = Depends(get_current_user)
):
    """
    Clear all cache entries.
    """
    try:
        cache = await get_cache_service()
        success = await cache.clear_all()
        
        return APIResponse.success(
            data=success,
            message="All cache cleared successfully"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/clear/{namespace}", response_model=APIResponse[int])
async def clear_namespace_cache(
    namespace: str,
    current_user = Depends(get_current_user)
):
    """
    Clear cache for a specific namespace.
    """
    try:
        cache = await get_cache_service()
        deleted = await cache.clear_namespace(namespace)
        
        return APIResponse.success(
            data=deleted,
            message=f"Cleared {deleted} keys from namespace '{namespace}'"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
