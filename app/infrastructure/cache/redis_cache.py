"""
Redis cache implementation.

This module provides Redis-based caching functionality with
JSON serialization, TTL support, and connection management.
"""

import json
import logging
from typing import Any, Optional, Union, List, Dict
from datetime import timedelta

import redis
from redis.connection import ConnectionPool
from redis.exceptions import RedisError, ConnectionError

from ...core.config import get_settings
from ...core.exceptions import CacheError

logger = logging.getLogger(__name__)
settings = get_settings()


class RedisCache:
    """
    Redis cache implementation with JSON serialization.
    
    Provides caching functionality with automatic serialization/deserialization,
    TTL support, and connection pooling.
    """
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        password: Optional[str] = None,
        db: Optional[int] = None,
        url: Optional[str] = None,
        max_connections: Optional[int] = None,
        decode_responses: bool = True,
    ):
        """
        Initialize Redis cache.
        
        Args:
            host: Redis host
            port: Redis port
            password: Redis password
            db: Redis database number
            url: Redis URL (overrides other connection params)
            max_connections: Maximum connections in pool
            decode_responses: Whether to decode byte responses to strings
        """
        self.host = host or settings.redis.host
        self.port = port or settings.redis.port
        self.password = password or settings.redis.password
        self.db = db or settings.redis.db
        self.url = url or settings.redis.url
        self.max_connections = max_connections or settings.redis.max_connections
        self.decode_responses = decode_responses
        
        self._client: Optional[redis.Redis] = None
        self._pool: Optional[ConnectionPool] = None
        
        self._connect()
    
    def _connect(self) -> None:
        """Establish Redis connection with connection pooling."""
        try:
            if self.url:
                # Use URL-based connection
                self._pool = ConnectionPool.from_url(
                    self.url,
                    max_connections=self.max_connections,
                    decode_responses=self.decode_responses,
                )
            else:
                # Use individual parameters
                self._pool = ConnectionPool(
                    host=self.host,
                    port=self.port,
                    password=self.password,
                    db=self.db,
                    max_connections=self.max_connections,
                    decode_responses=self.decode_responses,
                )
            
            self._client = redis.Redis(connection_pool=self._pool)
            
            # Test connection
            self._client.ping()
            logger.info(f"Connected to Redis at {self.host}:{self.port}")
            
        except RedisError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise CacheError(f"Redis connection failed: {e}")
    
    def _serialize(self, value: Any) -> str:
        """
        Serialize value to JSON string.
        
        Args:
            value: Value to serialize
            
        Returns:
            JSON string
        """
        try:
            return json.dumps(value, default=str, ensure_ascii=False)
        except (TypeError, ValueError) as e:
            raise CacheError(f"Failed to serialize value: {e}")
    
    def _deserialize(self, value: str) -> Any:
        """
        Deserialize JSON string to Python object.
        
        Args:
            value: JSON string to deserialize
            
        Returns:
            Deserialized Python object
        """
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError) as e:
            raise CacheError(f"Failed to deserialize value: {e}")
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found
            
        Raises:
            CacheError: If cache operation fails
        """
        try:
            value = self._client.get(key)
            if value is None:
                logger.debug(f"Cache miss for key: {key}")
                return None
            
            logger.debug(f"Cache hit for key: {key}")
            return self._deserialize(value)
            
        except RedisError as e:
            logger.error(f"Redis get error for key {key}: {e}")
            raise CacheError(f"Failed to get cache value: {e}", key=key)
    
    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[Union[int, timedelta]] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """
        Set value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live (seconds or timedelta)
            nx: Only set if key doesn't exist
            xx: Only set if key exists
            
        Returns:
            True if value was set, False otherwise
            
        Raises:
            CacheError: If cache operation fails
        """
        try:
            serialized_value = self._serialize(value)
            
            # Convert timedelta to seconds
            if isinstance(ttl, timedelta):
                ttl = int(ttl.total_seconds())
            
            result = self._client.set(
                key,
                serialized_value,
                ex=ttl,
                nx=nx,
                xx=xx,
            )
            
            if result:
                logger.debug(f"Cache set for key: {key} (TTL: {ttl})")
            else:
                logger.debug(f"Cache set failed for key: {key}")
            
            return bool(result)
            
        except RedisError as e:
            logger.error(f"Redis set error for key {key}: {e}")
            raise CacheError(f"Failed to set cache value: {e}", key=key)
    
    def delete(self, *keys: str) -> int:
        """
        Delete keys from cache.
        
        Args:
            keys: Cache keys to delete
            
        Returns:
            Number of keys that were deleted
            
        Raises:
            CacheError: If cache operation fails
        """
        try:
            if not keys:
                return 0
            
            deleted_count = self._client.delete(*keys)
            logger.debug(f"Deleted {deleted_count} keys from cache")
            return deleted_count
            
        except RedisError as e:
            logger.error(f"Redis delete error for keys {keys}: {e}")
            raise CacheError(f"Failed to delete cache keys: {e}")
    
    def exists(self, *keys: str) -> int:
        """
        Check if keys exist in cache.
        
        Args:
            keys: Cache keys to check
            
        Returns:
            Number of keys that exist
            
        Raises:
            CacheError: If cache operation fails
        """
        try:
            if not keys:
                return 0
            
            exist_count = self._client.exists(*keys)
            logger.debug(f"{exist_count} of {len(keys)} keys exist in cache")
            return exist_count
            
        except RedisError as e:
            logger.error(f"Redis exists error for keys {keys}: {e}")
            raise CacheError(f"Failed to check key existence: {e}")
    
    def expire(self, key: str, ttl: Union[int, timedelta]) -> bool:
        """
        Set TTL for existing key.
        
        Args:
            key: Cache key
            ttl: Time to live (seconds or timedelta)
            
        Returns:
            True if TTL was set, False if key doesn't exist
            
        Raises:
            CacheError: If cache operation fails
        """
        try:
            # Convert timedelta to seconds
            if isinstance(ttl, timedelta):
                ttl = int(ttl.total_seconds())
            
            result = self._client.expire(key, ttl)
            logger.debug(f"Set TTL {ttl} for key: {key}")
            return bool(result)
            
        except RedisError as e:
            logger.error(f"Redis expire error for key {key}: {e}")
            raise CacheError(f"Failed to set key expiration: {e}", key=key)
    
    def ttl(self, key: str) -> int:
        """
        Get TTL for key.
        
        Args:
            key: Cache key
            
        Returns:
            TTL in seconds (-1 if no TTL, -2 if key doesn't exist)
            
        Raises:
            CacheError: If cache operation fails
        """
        try:
            ttl_value = self._client.ttl(key)
            logger.debug(f"TTL for key {key}: {ttl_value}")
            return ttl_value
            
        except RedisError as e:
            logger.error(f"Redis TTL error for key {key}: {e}")
            raise CacheError(f"Failed to get key TTL: {e}", key=key)
    
    def increment(self, key: str, amount: int = 1) -> int:
        """
        Increment numeric value in cache.
        
        Args:
            key: Cache key
            amount: Amount to increment by
            
        Returns:
            New value after increment
            
        Raises:
            CacheError: If cache operation fails
        """
        try:
            result = self._client.incrby(key, amount)
            logger.debug(f"Incremented key {key} by {amount}, new value: {result}")
            return result
            
        except RedisError as e:
            logger.error(f"Redis increment error for key {key}: {e}")
            raise CacheError(f"Failed to increment key: {e}", key=key)
    
    def decrement(self, key: str, amount: int = 1) -> int:
        """
        Decrement numeric value in cache.
        
        Args:
            key: Cache key
            amount: Amount to decrement by
            
        Returns:
            New value after decrement
            
        Raises:
            CacheError: If cache operation fails
        """
        try:
            result = self._client.decrby(key, amount)
            logger.debug(f"Decremented key {key} by {amount}, new value: {result}")
            return result
            
        except RedisError as e:
            logger.error(f"Redis decrement error for key {key}: {e}")
            raise CacheError(f"Failed to decrement key: {e}", key=key)
    
    def get_many(self, *keys: str) -> Dict[str, Any]:
        """
        Get multiple values from cache.
        
        Args:
            keys: Cache keys to retrieve
            
        Returns:
            Dictionary mapping keys to values (missing keys are excluded)
            
        Raises:
            CacheError: If cache operation fails
        """
        try:
            if not keys:
                return {}
            
            values = self._client.mget(keys)
            result = {}
            
            for i, value in enumerate(values):
                if value is not None:
                    result[keys[i]] = self._deserialize(value)
            
            logger.debug(f"Retrieved {len(result)} of {len(keys)} keys from cache")
            return result
            
        except RedisError as e:
            logger.error(f"Redis mget error for keys {keys}: {e}")
            raise CacheError(f"Failed to get multiple cache values: {e}")
    
    def set_many(
        self,
        mapping: Dict[str, Any],
        ttl: Optional[Union[int, timedelta]] = None,
    ) -> bool:
        """
        Set multiple values in cache.
        
        Args:
            mapping: Dictionary of key-value pairs to set
            ttl: Time to live for all keys
            
        Returns:
            True if all values were set
            
        Raises:
            CacheError: If cache operation fails
        """
        try:
            if not mapping:
                return True
            
            # Serialize all values
            serialized_mapping = {
                key: self._serialize(value)
                for key, value in mapping.items()
            }
            
            # Set all values
            result = self._client.mset(serialized_mapping)
            
            # Set TTL if specified
            if ttl and result:
                if isinstance(ttl, timedelta):
                    ttl = int(ttl.total_seconds())
                
                pipe = self._client.pipeline()
                for key in mapping.keys():
                    pipe.expire(key, ttl)
                pipe.execute()
            
            logger.debug(f"Set {len(mapping)} keys in cache (TTL: {ttl})")
            return bool(result)
            
        except RedisError as e:
            logger.error(f"Redis mset error: {e}")
            raise CacheError(f"Failed to set multiple cache values: {e}")
    
    def flush_all(self) -> bool:
        """
        Clear all keys from current database.
        
        Returns:
            True if database was flushed
            
        Raises:
            CacheError: If cache operation fails
        """
        try:
            result = self._client.flushdb()
            logger.warning("Flushed all keys from Redis database")
            return bool(result)
            
        except RedisError as e:
            logger.error(f"Redis flush error: {e}")
            raise CacheError(f"Failed to flush cache: {e}")
    
    def keys(self, pattern: str = "*") -> List[str]:
        """
        Get keys matching pattern.
        
        Args:
            pattern: Pattern to match (default: all keys)
            
        Returns:
            List of matching keys
            
        Raises:
            CacheError: If cache operation fails
        """
        try:
            keys = self._client.keys(pattern)
            logger.debug(f"Found {len(keys)} keys matching pattern: {pattern}")
            return keys
            
        except RedisError as e:
            logger.error(f"Redis keys error with pattern {pattern}: {e}")
            raise CacheError(f"Failed to get keys: {e}")
    
    def info(self) -> Dict[str, Any]:
        """
        Get Redis server information.
        
        Returns:
            Dictionary with server information
            
        Raises:
            CacheError: If cache operation fails
        """
        try:
            info = self._client.info()
            return dict(info)
            
        except RedisError as e:
            logger.error(f"Redis info error: {e}")
            raise CacheError(f"Failed to get server info: {e}")
    
    def ping(self) -> bool:
        """
        Ping Redis server to check connectivity.
        
        Returns:
            True if server responds to ping
            
        Raises:
            CacheError: If cache operation fails
        """
        try:
            result = self._client.ping()
            return bool(result)
            
        except RedisError as e:
            logger.error(f"Redis ping error: {e}")
            raise CacheError(f"Failed to ping Redis server: {e}")
    
    def pipeline(self):
        """
        Create Redis pipeline for batch operations.
        
        Returns:
            Redis pipeline object
        """
        return self._client.pipeline()
    
    def close(self) -> None:
        """Close Redis connection pool."""
        try:
            if self._pool:
                self._pool.disconnect()
                logger.info("Redis connection pool closed")
        except Exception as e:
            logger.error(f"Error closing Redis connection pool: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Utility functions for common caching patterns
def cache_key(*parts: str, prefix: str = "", separator: str = ":") -> str:
    """
    Generate cache key from parts.
    
    Args:
        parts: Key parts to join
        prefix: Optional prefix for the key
        separator: Separator to use between parts
        
    Returns:
        Generated cache key
    """
    key_parts = [str(part) for part in parts if part]
    key = separator.join(key_parts)
    
    if prefix:
        key = f"{prefix}{separator}{key}"
    
    return key


def cache_decorator(
    cache: RedisCache,
    ttl: Optional[Union[int, timedelta]] = None,
    key_prefix: str = "",
):
    """
    Decorator for caching function results.
    
    Args:
        cache: RedisCache instance
        ttl: Cache TTL
        key_prefix: Prefix for cache keys
        
    Returns:
        Decorator function
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Generate cache key from function name and arguments
            import hashlib
            
            args_str = str(args) + str(sorted(kwargs.items()))
            args_hash = hashlib.md5(args_str.encode()).hexdigest()
            
            cache_key_str = cache_key(
                func.__name__,
                args_hash,
                prefix=key_prefix
            )
            
            # Try to get from cache
            try:
                result = cache.get(cache_key_str)
                if result is not None:
                    logger.debug(f"Cache hit for function {func.__name__}")
                    return result
            except CacheError:
                logger.warning(f"Cache error for function {func.__name__}, executing function")
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            
            try:
                cache.set(cache_key_str, result, ttl=ttl)
                logger.debug(f"Cached result for function {func.__name__}")
            except CacheError:
                logger.warning(f"Failed to cache result for function {func.__name__}")
            
            return result
        
        return wrapper
    return decorator