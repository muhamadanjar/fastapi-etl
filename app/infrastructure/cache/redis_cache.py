"""
Redis cache implementation with async support and improved features.
"""

import asyncio
import json
import logging
from typing import Any, Optional, Union, List, Dict, Callable
from datetime import timedelta
from contextlib import asynccontextmanager

import redis.asyncio as redis
from redis.asyncio import ConnectionPool
from redis.exceptions import RedisError, ConnectionError
from redis.typing import ExpiryT

from .base import CacheInterface
from ...core.config import get_settings
from ...core.exceptions import CacheError

logger = logging.getLogger(__name__)


class RedisCache(CacheInterface):
    """
    Async Redis cache implementation with enhanced features.
    
    Features:
    - Async/await support
    - Connection pooling
    - JSON serialization with custom encoders
    - Circuit breaker pattern
    - Distributed locking
    - Cache warming
    - Metrics collection
    """
    
    def __init__(
        self,
        url: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        password: Optional[str] = None,
        db: Optional[int] = None,
        max_connections: Optional[int] = None,
        retry_on_timeout: bool = True,
        socket_timeout: Optional[float] = None,
        socket_connect_timeout: Optional[float] = None,
        health_check_interval: int = 30,
        custom_serializer: Optional[Callable] = None,
        custom_deserializer: Optional[Callable] = None,
    ):
        """
        Initialize Redis cache.
        
        Args:
            url: Redis URL (overrides individual parameters)
            host: Redis host
            port: Redis port
            password: Redis password
            db: Redis database number
            max_connections: Maximum connections in pool
            retry_on_timeout: Whether to retry on timeout
            socket_timeout: Socket timeout in seconds
            socket_connect_timeout: Socket connect timeout in seconds
            health_check_interval: Health check interval in seconds
            custom_serializer: Custom serialization function
            custom_deserializer: Custom deserialization function
        """
        settings = get_settings()
        
        # Connection parameters
        self.url = url or getattr(settings, 'REDIS_URL', None)
        self.host = host or getattr(settings, 'REDIS_HOST', 'localhost')
        self.port = port or getattr(settings, 'REDIS_PORT', 6379)
        self.password = password or getattr(settings, 'REDIS_PASSWORD', None)
        self.db = db or getattr(settings, 'REDIS_DB', 0)
        self.max_connections = max_connections or getattr(settings, 'REDIS_POOL_SIZE', 10)
        
        # Connection behavior
        self.retry_on_timeout = retry_on_timeout
        self.socket_timeout = socket_timeout or getattr(settings, 'REDIS_TIMEOUT', 5.0)
        self.socket_connect_timeout = socket_connect_timeout or 5.0
        self.health_check_interval = health_check_interval
        
        # Serialization
        self.custom_serializer = custom_serializer
        self.custom_deserializer = custom_deserializer
        
        # State
        self._client: Optional[redis.Redis] = None
        self._pool: Optional[ConnectionPool] = None
        self._is_connected = False
        self._circuit_breaker_failures = 0
        self._circuit_breaker_threshold = 5
        self._circuit_breaker_reset_timeout = 60
        self._last_failure_time = 0
        
        # Metrics
        self._metrics = {
            'hits': 0,
            'misses': 0,
            'sets': 0,
            'deletes': 0,
            'errors': 0,
        }
    
    async def connect(self) -> None:
        """Establish Redis connection with connection pooling."""
        try:
            if self.url:
                # Use URL-based connection
                self._pool = ConnectionPool.from_url(
                    self.url,
                    max_connections=self.max_connections,
                    retry_on_timeout=self.retry_on_timeout,
                    socket_timeout=self.socket_timeout,
                    socket_connect_timeout=self.socket_connect_timeout,
                    decode_responses=True,
                )
            else:
                # Use individual parameters
                self._pool = ConnectionPool(
                    host=self.host,
                    port=self.port,
                    password=self.password,
                    db=self.db,
                    max_connections=self.max_connections,
                    retry_on_timeout=self.retry_on_timeout,
                    socket_timeout=self.socket_timeout,
                    socket_connect_timeout=self.socket_connect_timeout,
                    decode_responses=True,
                )
            
            self._client = redis.Redis(connection_pool=self._pool)
            
            # Test connection
            await self._client.ping()
            self._is_connected = True
            self._circuit_breaker_failures = 0
            
            logger.info(f"Connected to Redis at {self.host}:{self.port}")
            
        except RedisError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self._handle_error()
            raise CacheError(f"Redis connection failed: {e}")
    
    async def disconnect(self) -> None:
        """Close Redis connection pool."""
        try:
            if self._client:
                await self._client.aclose()
            if self._pool:
                await self._pool.aclose()
            
            self._client = None
            self._pool = None
            self._is_connected = False
            
            logger.info("Redis connection closed")
            
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")
    
    def _handle_error(self) -> None:
        """Handle Redis errors for circuit breaker pattern."""
        import time
        
        self._circuit_breaker_failures += 1
        self._last_failure_time = time.time()
        self._metrics['errors'] += 1
        
        if self._circuit_breaker_failures >= self._circuit_breaker_threshold:
            self._is_connected = False
            logger.warning(
                f"Redis circuit breaker activated after {self._circuit_breaker_failures} failures"
            )
    
    def _should_attempt_operation(self) -> bool:
        """Check if operation should be attempted based on circuit breaker state."""
        import time
        
        if self._is_connected:
            return True
        
        # Check if we should try to reconnect
        time_since_failure = time.time() - self._last_failure_time
        if time_since_failure > self._circuit_breaker_reset_timeout:
            logger.info("Attempting to reset Redis circuit breaker")
            return True
        
        return False
    
    def _serialize(self, value: Any) -> str:
        """
        Serialize value to JSON string with custom serializer support.
        
        Args:
            value: Value to serialize
            
        Returns:
            JSON string
        """
        try:
            if self.custom_serializer:
                return self.custom_serializer(value)
            
            return json.dumps(
                value,
                default=str,
                ensure_ascii=False,
                separators=(',', ':')  # More compact JSON
            )
        except (TypeError, ValueError) as e:
            raise CacheError(f"Failed to serialize value: {e}")
    
    def _deserialize(self, value: str) -> Any:
        """
        Deserialize JSON string to Python object with custom deserializer support.
        
        Args:
            value: JSON string to deserialize
            
        Returns:
            Deserialized Python object
        """
        try:
            if self.custom_deserializer:
                return self.custom_deserializer(value)
            
            return json.loads(value)
        except (json.JSONDecodeError, TypeError) as e:
            raise CacheError(f"Failed to deserialize value: {e}")
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found
        """
        if not self._should_attempt_operation():
            return None
        
        try:
            if not self._client:
                await self.connect()
            
            value = await self._client.get(key)
            if value is None:
                self._metrics['misses'] += 1
                logger.debug(f"Cache miss for key: {key}")
                return None
            
            self._metrics['hits'] += 1
            logger.debug(f"Cache hit for key: {key}")
            return self._deserialize(value)
            
        except RedisError as e:
            logger.error(f"Redis get error for key {key}: {e}")
            self._handle_error()
            return None  # Graceful degradation
    
    async def set(
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
        """
        if not self._should_attempt_operation():
            return False
        
        try:
            if not self._client:
                await self.connect()
            
            serialized_value = self._serialize(value)
            
            # Convert timedelta to seconds
            if isinstance(ttl, timedelta):
                ttl = int(ttl.total_seconds())
            
            result = await self._client.set(
                key,
                serialized_value,
                ex=ttl,
                nx=nx,
                xx=xx,
            )
            
            if result:
                self._metrics['sets'] += 1
                logger.debug(f"Cache set for key: {key} (TTL: {ttl})")
            
            return bool(result)
            
        except RedisError as e:
            logger.error(f"Redis set error for key {key}: {e}")
            self._handle_error()
            return False
    
    async def delete(self, *keys: str) -> int:
        """
        Delete keys from cache.
        
        Args:
            keys: Cache keys to delete
            
        Returns:
            Number of keys that were deleted
        """
        if not self._should_attempt_operation() or not keys:
            return 0
        
        try:
            if not self._client:
                await self.connect()
            
            deleted_count = await self._client.delete(*keys)
            self._metrics['deletes'] += deleted_count
            logger.debug(f"Deleted {deleted_count} keys from cache")
            return deleted_count
            
        except RedisError as e:
            logger.error(f"Redis delete error for keys {keys}: {e}")
            self._handle_error()
            return 0
    
    async def exists(self, *keys: str) -> int:
        """
        Check if keys exist in cache.
        
        Args:
            keys: Cache keys to check
            
        Returns:
            Number of keys that exist
        """
        if not self._should_attempt_operation() or not keys:
            return 0
        
        try:
            if not self._client:
                await self.connect()
            
            exist_count = await self._client.exists(*keys)
            logger.debug(f"{exist_count} of {len(keys)} keys exist in cache")
            return exist_count
            
        except RedisError as e:
            logger.error(f"Redis exists error for keys {keys}: {e}")
            self._handle_error()
            return 0
    
    async def expire(self, key: str, ttl: Union[int, timedelta]) -> bool:
        """
        Set TTL for existing key.
        
        Args:
            key: Cache key
            ttl: Time to live (seconds or timedelta)
            
        Returns:
            True if TTL was set, False if key doesn't exist
        """
        if not self._should_attempt_operation():
            return False
        
        try:
            if not self._client:
                await self.connect()
            
            # Convert timedelta to seconds
            if isinstance(ttl, timedelta):
                ttl = int(ttl.total_seconds())
            
            result = await self._client.expire(key, ttl)
            logger.debug(f"Set TTL {ttl} for key: {key}")
            return bool(result)
            
        except RedisError as e:
            logger.error(f"Redis expire error for key {key}: {e}")
            self._handle_error()
            return False
    
    async def ttl(self, key: str) -> int:
        """
        Get TTL for key.
        
        Args:
            key: Cache key
            
        Returns:
            TTL in seconds (-1 if no TTL, -2 if key doesn't exist)
        """
        if not self._should_attempt_operation():
            return -2
        
        try:
            if not self._client:
                await self.connect()
            
            ttl_value = await self._client.ttl(key)
            logger.debug(f"TTL for key {key}: {ttl_value}")
            return ttl_value
            
        except RedisError as e:
            logger.error(f"Redis TTL error for key {key}: {e}")
            self._handle_error()
            return -2
    
    async def increment(self, key: str, amount: int = 1, ttl: Optional[Union[int, timedelta]] = None) -> int:
        """
        Increment numeric value in cache.
        
        Args:
            key: Cache key
            amount: Amount to increment by
            ttl: TTL to set if key is created
            
        Returns:
            New value after increment
        """
        if not self._should_attempt_operation():
            return 0
        
        try:
            if not self._client:
                await self.connect()
            
            # Use pipeline for atomic operation
            async with self._client.pipeline() as pipe:
                result = await pipe.incrby(key, amount).execute()
                new_value = result[0]
                
                # Set TTL if specified and this is a new key
                if ttl and new_value == amount:
                    if isinstance(ttl, timedelta):
                        ttl = int(ttl.total_seconds())
                    await self._client.expire(key, ttl)
            
            logger.debug(f"Incremented key {key} by {amount}, new value: {new_value}")
            return new_value
            
        except RedisError as e:
            logger.error(f"Redis increment error for key {key}: {e}")
            self._handle_error()
            return 0
    
    async def get_many(self, *keys: str) -> Dict[str, Any]:
        """
        Get multiple values from cache efficiently.
        
        Args:
            keys: Cache keys to retrieve
            
        Returns:
            Dictionary mapping keys to values (missing keys are excluded)
        """
        if not self._should_attempt_operation() or not keys:
            return {}
        
        try:
            if not self._client:
                await self.connect()
            
            values = await self._client.mget(keys)
            result = {}
            
            for i, value in enumerate(values):
                if value is not None:
                    try:
                        result[keys[i]] = self._deserialize(value)
                        self._metrics['hits'] += 1
                    except CacheError:
                        logger.warning(f"Failed to deserialize value for key: {keys[i]}")
                else:
                    self._metrics['misses'] += 1
            
            logger.debug(f"Retrieved {len(result)} of {len(keys)} keys from cache")
            return result
            
        except RedisError as e:
            logger.error(f"Redis mget error for keys {keys}: {e}")
            self._handle_error()
            return {}
    
    async def set_many(
        self,
        mapping: Dict[str, Any],
        ttl: Optional[Union[int, timedelta]] = None,
    ) -> bool:
        """
        Set multiple values in cache efficiently.
        
        Args:
            mapping: Dictionary of key-value pairs to set
            ttl: Time to live for all keys
            
        Returns:
            True if all values were set
        """
        if not self._should_attempt_operation() or not mapping:
            return True
        
        try:
            if not self._client:
                await self.connect()
            
            # Serialize all values
            serialized_mapping = {
                key: self._serialize(value)
                for key, value in mapping.items()
            }
            
            # Use pipeline for efficient batch operation
            async with self._client.pipeline() as pipe:
                await pipe.mset(serialized_mapping)
                
                # Set TTL if specified
                if ttl:
                    if isinstance(ttl, timedelta):
                        ttl = int(ttl.total_seconds())
                    
                    for key in mapping.keys():
                        await pipe.expire(key, ttl)
                
                results = await pipe.execute()
            
            self._metrics['sets'] += len(mapping)
            logger.debug(f"Set {len(mapping)} keys in cache (TTL: {ttl})")
            return all(results)
            
        except RedisError as e:
            logger.error(f"Redis mset error: {e}")
            self._handle_error()
            return False
    
    async def clear(self) -> bool:
        """
        Clear all keys from current database.
        
        Returns:
            True if database was flushed
        """
        if not self._should_attempt_operation():
            return False
        
        try:
            if not self._client:
                await self.connect()
            
            result = await self._client.flushdb()
            logger.warning("Flushed all keys from Redis database")
            return bool(result)
            
        except RedisError as e:
            logger.error(f"Redis flush error: {e}")
            self._handle_error()
            return False
    
    async def keys(self, pattern: str = "*") -> List[str]:
        """
        Get keys matching pattern (use with caution in production).
        
        Args:
            pattern: Pattern to match (default: all keys)
            
        Returns:
            List of matching keys
        """
        if not self._should_attempt_operation():
            return []
        
        try:
            if not self._client:
                await self.connect()
            
            keys = await self._client.keys(pattern)
            logger.debug(f"Found {len(keys)} keys matching pattern: {pattern}")
            return keys
            
        except RedisError as e:
            logger.error(f"Redis keys error with pattern {pattern}: {e}")
            self._handle_error()
            return []
    
    async def scan_keys(self, pattern: str = "*", count: int = 100) -> List[str]:
        """
        Scan keys matching pattern (production-safe alternative to keys).
        
        Args:
            pattern: Pattern to match
            count: Number of keys to return per iteration
            
        Returns:
            List of matching keys
        """
        if not self._should_attempt_operation():
            return []
        
        try:
            if not self._client:
                await self.connect()
            
            keys = []
            cursor = 0
            
            while True:
                cursor, batch = await self._client.scan(
                    cursor=cursor,
                    match=pattern,
                    count=count
                )
                keys.extend(batch)
                
                if cursor == 0:
                    break
            
            logger.debug(f"Scanned {len(keys)} keys matching pattern: {pattern}")
            return keys
            
        except RedisError as e:
            logger.error(f"Redis scan error with pattern {pattern}: {e}")
            self._handle_error()
            return []
    
    @asynccontextmanager
    async def lock(self, key: str, timeout: int = 10, blocking: bool = True):
        """
        Distributed lock context manager.
        
        Args:
            key: Lock key
            timeout: Lock timeout in seconds
            blocking: Whether to block until lock is acquired
        """
        if not self._should_attempt_operation():
            yield False
            return
        
        try:
            if not self._client:
                await self.connect()
            
            lock = self._client.lock(key, timeout=timeout, blocking=blocking)
            
            async with lock:
                yield True
                
        except RedisError as e:
            logger.error(f"Redis lock error for key {key}: {e}")
            self._handle_error()
            yield False
    
    async def health_check(self) -> bool:
        """
        Check Redis server health.
        
        Returns:
            True if server is healthy
        """
        try:
            if not self._client:
                await self.connect()
            
            result = await self._client.ping()
            if result:
                # Reset circuit breaker on successful health check
                if not self._is_connected:
                    self._is_connected = True
                    self._circuit_breaker_failures = 0
                    logger.info("Redis circuit breaker reset - connection restored")
            
            return bool(result)
            
        except RedisError as e:
            logger.error(f"Redis health check failed: {e}")
            self._handle_error()
            return False
    
    async def info(self) -> Dict[str, Any]:
        """
        Get Redis server information.
        
        Returns:
            Dictionary with server information
        """
        if not self._should_attempt_operation():
            return {}
        
        try:
            if not self._client:
                await self.connect()
            
            info = await self._client.info()
            return dict(info)
            
        except RedisError as e:
            logger.error(f"Redis info error: {e}")
            self._handle_error()
            return {}
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get cache metrics.
        
        Returns:
            Dictionary with cache metrics
        """
        total_operations = self._metrics['hits'] + self._metrics['misses']
        hit_rate = (self._metrics['hits'] / total_operations * 100) if total_operations > 0 else 0
        
        return {
            **self._metrics,
            'hit_rate': round(hit_rate, 2),
            'total_operations': total_operations,
            'is_connected': self._is_connected,
            'circuit_breaker_failures': self._circuit_breaker_failures,
        }
    
    def reset_metrics(self) -> None:
        """Reset cache metrics."""
        self._metrics = {
            'hits': 0,
            'misses': 0,
            'sets': 0,
            'deletes': 0,
            'errors': 0,
        }


class RedisManager:
    """
    Redis connection manager with lifecycle management.
    """
    
    def __init__(self):
        self._cache: Optional[RedisCache] = None
        self._health_check_task: Optional[asyncio.Task] = None
    
    async def connect(self) -> None:
        """Initialize Redis connection."""
        self._cache = RedisCache()
        await self._cache.connect()
        
        # Start health check task
        self._health_check_task = asyncio.create_task(self._health_check_loop())
    
    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
        
        if self._cache:
            await self._cache.disconnect()
            self._cache = None
    
    async def _health_check_loop(self) -> None:
        """Periodic health check loop."""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                if self._cache:
                    await self._cache.health_check()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check loop error: {e}")
    
    def get_cache(self) -> Optional[RedisCache]:
        """Get cache instance."""
        return self._cache
    
    async def health_check(self) -> bool:
        """Check Redis health."""
        if not self._cache:
            return False
        return await self._cache.health_check()


# Global Redis manager instance
redis_manager = RedisManager()


# Backward compatibility function
def get_redis_cache() -> Optional[RedisCache]:
    """Get Redis cache instance."""
    return redis_manager.get_cache()
