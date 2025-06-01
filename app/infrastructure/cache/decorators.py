"""
Cache decorators for easy caching of function results.
"""

import asyncio
import functools
import logging
from typing import Any, Callable, Optional, Union, TypeVar, Dict, List
from datetime import timedelta
from inspect import signature, iscoroutinefunction

from .redis_cache import get_redis_cache
from .memory_cache import MemoryCache
from .utils import serialize_cache_args, get_cache_namespace

logger = logging.getLogger(__name__)

F = TypeVar('F', bound=Callable[..., Any])

# Fallback cache for when Redis is unavailable
_fallback_cache = MemoryCache(max_size=1000, default_ttl=300)


def cache_result(
    ttl: Optional[Union[int, timedelta]] = None,
    key_prefix: Optional[str] = None,
    skip_cache: Optional[Callable[..., bool]] = None,
    use_fallback: bool = True,
    serialize_args: bool = True,
) -> Callable[[F], F]:
    """
    Decorator to cache function results.
    
    Args:
        ttl: Cache TTL (seconds or timedelta)
        key_prefix: Custom key prefix (defaults to function name)
        skip_cache: Function to determine if cache should be skipped
        use_fallback: Whether to use fallback cache when Redis unavailable
        serialize_args: Whether to serialize function arguments for cache key
        
    Returns:
        Decorated function
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Get cache instance
            cache = get_redis_cache()
            if not cache and use_fallback:
                cache = _fallback_cache
            
            if not cache:
                # No cache available, execute function directly
                return await func(*args, **kwargs)
            
            # Check if we should skip cache
            if skip_cache and skip_cache(*args, **kwargs):
                return await func(*args, **kwargs)
            
            # Generate cache key
            prefix = key_prefix or get_cache_namespace(func)
            if serialize_args:
                args_key = serialize_cache_args(*args, **kwargs)
                cache_key = f"{prefix}:{args_key}"
            else:
                cache_key = f"{prefix}:static"
            
            # Try to get from cache
            try:
                cached_result = await cache.get(cache_key)
                if cached_result is not None:
                    logger.debug(f"Cache hit for {func.__name__}: {cache_key}")
                    return cached_result
            except Exception as e:
                logger.warning(f"Cache get error for {func.__name__}: {e}")
            
            # Execute function
            logger.debug(f"Cache miss for {func.__name__}: {cache_key}")
            result = await func(*args, **kwargs)
            
            # Store in cache
            try:
                await cache.set(cache_key, result, ttl=ttl)
                logger.debug(f"Cached result for {func.__name__}: {cache_key}")
            except Exception as e:
                logger.warning(f"Cache set error for {func.__name__}: {e}")
            
            return result
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            # For sync functions, we need to handle caching differently
            # This is a simplified version - in practice, you might want to
            # run the async cache operations in an event loop
            result = func(*args, **kwargs)
            return result
        
        # Return appropriate wrapper based on function type
        if iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def invalidate_cache(
    patterns: Union[str, List[str]],
    wait_for_completion: bool = False
) -> Callable[[F], F]:
    """
    Decorator to invalidate cache patterns after function execution.
    
    Args:
        patterns: Cache key patterns to invalidate
        wait_for_completion: Whether to wait for cache invalidation
        
    Returns:
        Decorated function
    """
    if isinstance(patterns, str):
        patterns = [patterns]
    
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Execute function first
            result = await func(*args, **kwargs)
            
            # Invalidate cache patterns
            cache = get_redis_cache()
            if cache:
                invalidation_tasks = []
                
                for pattern in patterns:
                    task = asyncio.create_task(_invalidate_pattern(cache, pattern))
                    invalidation_tasks.append(task)
                
                if wait_for_completion:
                    await asyncio.gather(*invalidation_tasks, return_exceptions=True)
                else:
                    # Fire and forget
                    asyncio.create_task(
                        asyncio.gather(*invalidation_tasks, return_exceptions=True)
                    )
            
            return result
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            # For sync functions, we can't easily do async cache invalidation
            # This would need to be handled differently in practice
            return result
        
        if iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


async def _invalidate_pattern(cache, pattern: str) -> None:
    """Helper function to invalidate cache pattern."""
    try:
        # Use scan_keys for production safety
        keys = await cache.scan_keys(pattern)
        if keys:
            await cache.delete(*keys)
            logger.debug(f"Invalidated {len(keys)} keys matching pattern: {pattern}")
    except Exception as e:
        logger.error(f"Error invalidating cache pattern {pattern}: {e}")


# Convenience decorators for common use cases
def cache_for(seconds: int):
    """Cache result for specified seconds."""
    return cache_result(ttl=seconds)


def cache_for_minutes(minutes: int):
    """Cache result for specified minutes."""
    return cache_result(ttl=timedelta(minutes=minutes))


def cache_for_hours(hours: int):
    """Cache result for specified hours."""
    return cache_result(ttl=timedelta(hours=hours))


def cache_user_data(ttl: Optional[Union[int, timedelta]] = None):
    """Cache user-specific data with user ID in key."""
    def skip_if_no_user(*args, **kwargs):
        # Skip cache if no user_id in arguments
        return 'user_id' not in kwargs and (not args or not hasattr(args[0], 'user_id'))
    
    return cache_result(
        ttl=ttl or timedelta(minutes=15),
        key_prefix="user_data",
        skip_cache=skip_if_no_user
    )


def cache_query_result(ttl: Optional[Union[int, timedelta]] = None):
    """Cache database query results."""
    return cache_result(
        ttl=ttl or timedelta(minutes=5),
        key_prefix="query"
    )
