"""
Cache infrastructure package - Public Interface.

This module provides the main interface for caching functionality
including Redis cache, memory cache, decorators, and utilities.
"""

from .base import CacheInterface
from .redis_cache import RedisCache, redis_manager
from .memory_cache import MemoryCache
from .manager import CacheManager, cache_manager
from .decorators import (
    cache_result,
    invalidate_cache,
    cache_for,
    cache_for_minutes,
    cache_for_hours,
    cache_user_data,
    cache_query_result,
)
from .utils import (
    cache_key,
    CacheKeyBuilder,
    serialize_cache_args,
    get_cache_namespace,
    get_cache_stats_summary,
    warm_cache,
    CacheMetrics,
)

# Version info
__version__ = "1.0.0"

# Public API
__all__ = [
    # Core interfaces
    "CacheInterface",
    
    # Cache implementations
    "RedisCache", 
    "redis_manager",
    "MemoryCache",
    
    # Manager
    "CacheManager",
    "cache_manager",
    
    # Decorators
    "cache_result",
    "invalidate_cache", 
    "cache_for",
    "cache_for_minutes",
    "cache_for_hours",
    "cache_user_data",
    "cache_query_result",
    
    # Utilities
    "cache_key",
    "CacheKeyBuilder", 
    "serialize_cache_args",
    "get_cache_namespace",
    "get_cache_stats_summary",
    "warm_cache",
    "CacheMetrics",
]


# Convenience function for getting cache instance
async def get_cache():
    """
    Get the current cache instance.
    
    Returns:
        Cache instance or None if not available
    """
    return await cache_manager.get_cache()