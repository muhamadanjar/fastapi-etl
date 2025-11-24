"""
Cache service for Redis-based caching operations.
"""

import json
import pickle
from typing import Any, Dict, List, Optional, Union
from datetime import timedelta
import redis.asyncio as redis

from app.core.config import get_settings
from app.utils.logger import get_logger

logger = get_logger(__name__)
settings = get_settings()


class CacheService:
    """
    Service for managing Redis cache operations.
    Provides get, set, delete, and batch operations with TTL support.
    """
    
    def __init__(self, redis_url: Optional[str] = None, namespace: str = "etl:cache"):
        """
        Initialize cache service.
        
        Args:
            redis_url: Redis connection URL (defaults to settings)
            namespace: Cache key namespace prefix
        """
        self.redis_url = redis_url or settings.CELERY_BROKER_URL
        self.namespace = namespace
        self.redis_client: Optional[redis.Redis] = None
        self.logger = logger
        self._connected = False
        
        # Default TTLs (in seconds)
        self.default_ttl = 3600  # 1 hour
        self.ttl_config = {
            'jobs': 300,           # 5 minutes
            'files': 600,          # 10 minutes
            'metrics': 1800,       # 30 minutes
            'dependencies': 300,   # 5 minutes
            'transformations': 3600, # 1 hour
            'validations': 1800,   # 30 minutes
            'executions': 180,     # 3 minutes
        }
    
    async def connect(self):
        """Establish Redis connection"""
        try:
            if not self._connected:
                self.redis_client = await redis.from_url(
                    self.redis_url,
                    encoding="utf-8",
                    decode_responses=False  # We'll handle encoding ourselves
                )
                # Test connection
                await self.redis_client.ping()
                self._connected = True
                self.logger.info("Cache service connected to Redis")
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {str(e)}")
            raise
    
    async def disconnect(self):
        """Close Redis connection"""
        try:
            if self.redis_client and self._connected:
                await self.redis_client.close()
                self._connected = False
                self.logger.info("Cache service disconnected from Redis")
        except Exception as e:
            self.logger.error(f"Error disconnecting from Redis: {str(e)}")
    
    def _make_key(self, key: str, sub_namespace: Optional[str] = None) -> str:
        """
        Create full cache key with namespace.
        
        Args:
            key: Cache key
            sub_namespace: Optional sub-namespace (e.g., 'jobs', 'files')
            
        Returns:
            Full cache key
        """
        if sub_namespace:
            return f"{self.namespace}:{sub_namespace}:{key}"
        return f"{self.namespace}:{key}"
    
    def _serialize(self, value: Any) -> bytes:
        """Serialize value for storage"""
        try:
            # Try JSON first (faster, more readable in Redis)
            return json.dumps(value).encode('utf-8')
        except (TypeError, ValueError):
            # Fall back to pickle for complex objects
            return pickle.dumps(value)
    
    def _deserialize(self, value: bytes) -> Any:
        """Deserialize value from storage"""
        try:
            # Try JSON first
            return json.loads(value.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            # Fall back to pickle
            return pickle.loads(value)
    
    async def get(
        self,
        key: str,
        sub_namespace: Optional[str] = None,
        default: Any = None
    ) -> Optional[Any]:
        """
        Get value from cache.
        
        Args:
            key: Cache key
            sub_namespace: Optional sub-namespace
            default: Default value if key not found
            
        Returns:
            Cached value or default
        """
        try:
            if not self._connected:
                await self.connect()
            
            full_key = self._make_key(key, sub_namespace)
            value = await self.redis_client.get(full_key)
            
            if value is None:
                self.logger.debug(f"Cache miss: {full_key}")
                return default
            
            self.logger.debug(f"Cache hit: {full_key}")
            return self._deserialize(value)
            
        except Exception as e:
            self.logger.warning(f"Cache get error for {key}: {str(e)}")
            return default
    
    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        sub_namespace: Optional[str] = None
    ) -> bool:
        """
        Set value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds (uses default if not provided)
            sub_namespace: Optional sub-namespace
            
        Returns:
            True if successful
        """
        try:
            if not self._connected:
                await self.connect()
            
            full_key = self._make_key(key, sub_namespace)
            serialized = self._serialize(value)
            
            # Use sub_namespace TTL if available, otherwise use provided or default
            if ttl is None and sub_namespace:
                ttl = self.ttl_config.get(sub_namespace, self.default_ttl)
            elif ttl is None:
                ttl = self.default_ttl
            
            await self.redis_client.setex(full_key, ttl, serialized)
            
            self.logger.debug(f"Cache set: {full_key} (TTL: {ttl}s)")
            return True
            
        except Exception as e:
            self.logger.error(f"Cache set error for {key}: {str(e)}")
            return False
    
    async def delete(
        self,
        key: str,
        sub_namespace: Optional[str] = None
    ) -> bool:
        """
        Delete value from cache.
        
        Args:
            key: Cache key
            sub_namespace: Optional sub-namespace
            
        Returns:
            True if deleted
        """
        try:
            if not self._connected:
                await self.connect()
            
            full_key = self._make_key(key, sub_namespace)
            result = await self.redis_client.delete(full_key)
            
            self.logger.debug(f"Cache delete: {full_key}")
            return result > 0
            
        except Exception as e:
            self.logger.error(f"Cache delete error for {key}: {str(e)}")
            return False
    
    async def exists(
        self,
        key: str,
        sub_namespace: Optional[str] = None
    ) -> bool:
        """
        Check if key exists in cache.
        
        Args:
            key: Cache key
            sub_namespace: Optional sub-namespace
            
        Returns:
            True if exists
        """
        try:
            if not self._connected:
                await self.connect()
            
            full_key = self._make_key(key, sub_namespace)
            result = await self.redis_client.exists(full_key)
            
            return result > 0
            
        except Exception as e:
            self.logger.error(f"Cache exists error for {key}: {str(e)}")
            return False
    
    async def get_many(
        self,
        keys: List[str],
        sub_namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get multiple values from cache.
        
        Args:
            keys: List of cache keys
            sub_namespace: Optional sub-namespace
            
        Returns:
            Dictionary of key-value pairs
        """
        try:
            if not self._connected:
                await self.connect()
            
            full_keys = [self._make_key(k, sub_namespace) for k in keys]
            values = await self.redis_client.mget(full_keys)
            
            result = {}
            for key, value in zip(keys, values):
                if value is not None:
                    result[key] = self._deserialize(value)
            
            self.logger.debug(f"Cache get_many: {len(result)}/{len(keys)} hits")
            return result
            
        except Exception as e:
            self.logger.error(f"Cache get_many error: {str(e)}")
            return {}
    
    async def set_many(
        self,
        items: Dict[str, Any],
        ttl: Optional[int] = None,
        sub_namespace: Optional[str] = None
    ) -> bool:
        """
        Set multiple values in cache.
        
        Args:
            items: Dictionary of key-value pairs
            ttl: Time to live in seconds
            sub_namespace: Optional sub-namespace
            
        Returns:
            True if successful
        """
        try:
            if not self._connected:
                await self.connect()
            
            # Use pipeline for efficiency
            pipe = self.redis_client.pipeline()
            
            # Determine TTL
            if ttl is None and sub_namespace:
                ttl = self.ttl_config.get(sub_namespace, self.default_ttl)
            elif ttl is None:
                ttl = self.default_ttl
            
            for key, value in items.items():
                full_key = self._make_key(key, sub_namespace)
                serialized = self._serialize(value)
                pipe.setex(full_key, ttl, serialized)
            
            await pipe.execute()
            
            self.logger.debug(f"Cache set_many: {len(items)} items (TTL: {ttl}s)")
            return True
            
        except Exception as e:
            self.logger.error(f"Cache set_many error: {str(e)}")
            return False
    
    async def clear_namespace(
        self,
        sub_namespace: str
    ) -> int:
        """
        Clear all keys in a namespace.
        
        Args:
            sub_namespace: Sub-namespace to clear
            
        Returns:
            Number of keys deleted
        """
        try:
            if not self._connected:
                await self.connect()
            
            pattern = self._make_key("*", sub_namespace)
            
            # Scan for keys (more efficient than KEYS)
            deleted = 0
            async for key in self.redis_client.scan_iter(match=pattern):
                await self.redis_client.delete(key)
                deleted += 1
            
            self.logger.info(f"Cleared namespace '{sub_namespace}': {deleted} keys")
            return deleted
            
        except Exception as e:
            self.logger.error(f"Cache clear_namespace error: {str(e)}")
            return 0
    
    async def clear_all(self) -> bool:
        """
        Clear all cache keys in this namespace.
        
        Returns:
            True if successful
        """
        try:
            if not self._connected:
                await self.connect()
            
            pattern = f"{self.namespace}:*"
            
            deleted = 0
            async for key in self.redis_client.scan_iter(match=pattern):
                await self.redis_client.delete(key)
                deleted += 1
            
            self.logger.info(f"Cleared all cache: {deleted} keys")
            return True
            
        except Exception as e:
            self.logger.error(f"Cache clear_all error: {str(e)}")
            return False
    
    async def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache stats
        """
        try:
            if not self._connected:
                await self.connect()
            
            info = await self.redis_client.info('stats')
            memory = await self.redis_client.info('memory')
            
            # Count keys in namespace
            pattern = f"{self.namespace}:*"
            key_count = 0
            async for _ in self.redis_client.scan_iter(match=pattern):
                key_count += 1
            
            return {
                "namespace": self.namespace,
                "total_keys": key_count,
                "memory_used_mb": memory.get('used_memory', 0) / (1024 * 1024),
                "total_connections": info.get('total_connections_received', 0),
                "total_commands": info.get('total_commands_processed', 0),
                "keyspace_hits": info.get('keyspace_hits', 0),
                "keyspace_misses": info.get('keyspace_misses', 0),
                "hit_rate": self._calculate_hit_rate(
                    info.get('keyspace_hits', 0),
                    info.get('keyspace_misses', 0)
                )
            }
            
        except Exception as e:
            self.logger.error(f"Cache get_stats error: {str(e)}")
            return {"error": str(e)}
    
    def _calculate_hit_rate(self, hits: int, misses: int) -> float:
        """Calculate cache hit rate percentage"""
        total = hits + misses
        if total == 0:
            return 0.0
        return round((hits / total) * 100, 2)
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Check cache health.
        
        Returns:
            Health status dictionary
        """
        try:
            if not self._connected:
                await self.connect()
            
            # Test ping
            await self.redis_client.ping()
            
            # Test set/get
            test_key = self._make_key("health_check")
            test_value = {"timestamp": "test"}
            await self.redis_client.setex(test_key, 10, json.dumps(test_value).encode())
            result = await self.redis_client.get(test_key)
            await self.redis_client.delete(test_key)
            
            return {
                "status": "healthy",
                "connected": self._connected,
                "ping": "ok",
                "read_write": "ok" if result else "failed"
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "connected": False,
                "error": str(e)
            }


# Global cache service instance
_cache_service: Optional[CacheService] = None


async def get_cache_service() -> CacheService:
    """Get or create global cache service instance"""
    global _cache_service
    if _cache_service is None:
        _cache_service = CacheService()
        await _cache_service.connect()
    return _cache_service


async def cleanup_cache_service():
    """Cleanup global cache service"""
    global _cache_service
    if _cache_service:
        await _cache_service.disconnect()
        _cache_service = None
