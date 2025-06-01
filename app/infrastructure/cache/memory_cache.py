
"""
Async in-memory cache implementation with enhanced features.

This module provides thread-safe and async-compatible in-memory caching
for development, testing, or when Redis is not available.
"""

import asyncio
import logging
import threading
import time
import fnmatch
from typing import Any, Optional, Dict, List, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import OrderedDict
from contextlib import asynccontextmanager

from .base import CacheInterface
from ...core.exceptions import CacheError

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """
    Enhanced cache entry with comprehensive metadata.
    """
    value: Any
    created_at: datetime
    expires_at: Optional[datetime] = None
    access_count: int = 0
    last_accessed: Optional[datetime] = None
    size_bytes: int = 0
    tags: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """Calculate entry size after initialization."""
        if self.size_bytes == 0:
            self.size_bytes = self._calculate_size()
    
    def is_expired(self) -> bool:
        """Check if cache entry has expired."""
        if self.expires_at is None:
            return False
        return datetime.utcnow() > self.expires_at
    
    def access(self) -> None:
        """Record access to this cache entry."""
        self.access_count += 1
        self.last_accessed = datetime.utcnow()
    
    def _calculate_size(self) -> int:
        """Estimate memory size of the cached value."""
        try:
            import sys
            return sys.getsizeof(self.value)
        except Exception:
            return 0
    
    def time_to_expire(self) -> Optional[int]:
        """Get time remaining until expiration in seconds."""
        if self.expires_at is None:
            return None
        
        remaining = self.expires_at - datetime.utcnow()
        return max(0, int(remaining.total_seconds()))


class MemoryCache(CacheInterface):
    """
    Async-compatible thread-safe in-memory cache with enhanced features.
    
    Features:
    - Async/await support
    - LRU eviction with size-based eviction
    - TTL support with background cleanup
    - Cache tagging for bulk operations
    - Memory usage tracking
    - Statistics and monitoring
    - Pattern-based key operations
    """
    
    def __init__(
        self,
        max_size: int = 1000,
        max_memory_mb: Optional[int] = None,
        default_ttl: Optional[Union[int, timedelta]] = None,
        cleanup_interval: int = 300,  # 5 minutes
        eviction_policy: str = "lru",  # lru, lfu, random
        enable_stats: bool = True,
    ):
        """
        Initialize enhanced memory cache.
        
        Args:
            max_size: Maximum number of entries
            max_memory_mb: Maximum memory usage in MB
            default_ttl: Default TTL for cache entries
            cleanup_interval: Cleanup interval in seconds
            eviction_policy: Eviction policy (lru, lfu, random)
            enable_stats: Whether to collect statistics
        """
        self.max_size = max_size
        self.max_memory_bytes = max_memory_mb * 1024 * 1024 if max_memory_mb else None
        self.default_ttl = default_ttl
        self.cleanup_interval = cleanup_interval
        self.eviction_policy = eviction_policy.lower()
        self.enable_stats = enable_stats
        
        # Cache storage
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._tags: Dict[str, List[str]] = {}  # tag -> [keys]
        
        # Thread safety
        self._lock = asyncio.Lock()
        self._thread_lock = threading.RLock()
        
        # Statistics
        self._stats = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "deletes": 0,
            "evictions": 0,
            "expirations": 0,
            "memory_usage_bytes": 0,
            "cleanup_runs": 0,
        } if enable_stats else {}
        
        # Background cleanup
        self._cleanup_task: Optional[asyncio.Task] = None
        self._is_running = True
        
        logger.info(
            f"Memory cache initialized: max_size={max_size}, "
            f"max_memory_mb={max_memory_mb}, policy={eviction_policy}"
        )
    
    async def start_cleanup_task(self) -> None:
        """Start background cleanup task."""
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
    
    async def stop_cleanup_task(self) -> None:
        """Stop background cleanup task."""
        self._is_running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
    
    async def _cleanup_loop(self) -> None:
        """Background cleanup loop for expired entries."""
        while self._is_running:
            try:
                await asyncio.sleep(self.cleanup_interval)
                if self._is_running:
                    await self._remove_expired_entries()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cache cleanup loop: {e}")
    
    async def _remove_expired_entries(self) -> None:
        """Remove expired entries from cache."""
        async with self._lock:
            expired_keys = []
            current_time = datetime.utcnow()
            
            for key, entry in self._cache.items():
                if entry.expires_at and current_time > entry.expires_at:
                    expired_keys.append(key)
            
            for key in expired_keys:
                entry = self._cache[key]
                del self._cache[key]
                self._update_memory_usage(-entry.size_bytes)
                self._remove_from_tags(key)
                
                if self.enable_stats:
                    self._stats["expirations"] += 1
            
            if self.enable_stats:
                self._stats["cleanup_runs"] += 1
            
            if expired_keys:
                logger.debug(f"Cleaned up {len(expired_keys)} expired entries")
    
    def _update_memory_usage(self, delta: int) -> None:
        """Update memory usage statistics."""
        if self.enable_stats:
            self._stats["memory_usage_bytes"] = max(0, self._stats["memory_usage_bytes"] + delta)
    
    def _add_to_tags(self, key: str, tags: List[str]) -> None:
        """Add key to tag mappings."""
        for tag in tags:
            if tag not in self._tags:
                self._tags[tag] = []
            if key not in self._tags[tag]:
                self._tags[tag].append(key)
    
    def _remove_from_tags(self, key: str) -> None:
        """Remove key from all tag mappings."""
        for tag, keys in self._tags.items():
            if key in keys:
                keys.remove(key)
        
        # Clean up empty tag lists
        empty_tags = [tag for tag, keys in self._tags.items() if not keys]
        for tag in empty_tags:
            del self._tags[tag]
    
    async def _evict_entries(self) -> None:
        """Evict entries based on eviction policy."""
        evicted_count = 0
        
        while (
            (self.max_size and len(self._cache) >= self.max_size) or
            (self.max_memory_bytes and self._stats.get("memory_usage_bytes", 0) > self.max_memory_bytes)
        ):
            if not self._cache:
                break
            
            key_to_evict = self._select_eviction_key()
            if key_to_evict:
                entry = self._cache[key_to_evict]
                del self._cache[key_to_evict]
                self._update_memory_usage(-entry.size_bytes)
                self._remove_from_tags(key_to_evict)
                evicted_count += 1
                
                if self.enable_stats:
                    self._stats["evictions"] += 1
            else:
                break
        
        if evicted_count > 0:
            logger.debug(f"Evicted {evicted_count} entries using {self.eviction_policy} policy")
    
    def _select_eviction_key(self) -> Optional[str]:
        """Select key for eviction based on policy."""
        if not self._cache:
            return None
        
        if self.eviction_policy == "lru":
            # First item in OrderedDict is least recently used
            return next(iter(self._cache))
        
        elif self.eviction_policy == "lfu":
            # Find key with lowest access count
            min_access_key = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].access_count
            )
            return min_access_key
        
        elif self.eviction_policy == "random":
            import random
            return random.choice(list(self._cache.keys()))
        
        else:
            # Default to LRU
            return next(iter(self._cache))
    
    def _convert_ttl(self, ttl: Optional[Union[int, timedelta]]) -> Optional[datetime]:
        """Convert TTL to expiration datetime."""
        if ttl is None:
            return None
        
        if isinstance(ttl, timedelta):
            ttl = int(ttl.total_seconds())
        
        if ttl <= 0:
            return None
        
        return datetime.utcnow() + timedelta(seconds=ttl)
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        async with self._lock:
            entry = self._cache.get(key)
            
            if entry is None:
                if self.enable_stats:
                    self._stats["misses"] += 1
                logger.debug(f"Cache miss for key: {key}")
                return None
            
            if entry.is_expired():
                del self._cache[key]
                self._update_memory_usage(-entry.size_bytes)
                self._remove_from_tags(key)
                
                if self.enable_stats:
                    self._stats["misses"] += 1
                    self._stats["expirations"] += 1
                
                logger.debug(f"Cache expired for key: {key}")
                return None
            
            # Update access information
            entry.access()
            
            # Move to end for LRU (only if using LRU policy)
            if self.eviction_policy == "lru":
                self._cache.move_to_end(key)
            
            if self.enable_stats:
                self._stats["hits"] += 1
            
            logger.debug(f"Cache hit for key: {key}")
            return entry.value
    
    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[Union[int, timedelta]] = None,
        nx: bool = False,
        xx: bool = False,
        tags: Optional[List[str]] = None,
    ) -> bool:
        """Set value in cache with optional tags."""
        async with self._lock:
            existing_entry = self._cache.get(key)
            
            # Check nx condition
            if nx and existing_entry is not None and not existing_entry.is_expired():
                return False
            
            # Check xx condition
            if xx and (existing_entry is None or existing_entry.is_expired()):
                return False
            
            # Remove old entry if exists
            if existing_entry:
                self._update_memory_usage(-existing_entry.size_bytes)
                self._remove_from_tags(key)
            
            # Use provided TTL or default
            effective_ttl = ttl if ttl is not None else self.default_ttl
            expires_at = self._convert_ttl(effective_ttl)
            
            # Create new entry
            entry = CacheEntry(
                value=value,
                created_at=datetime.utcnow(),
                expires_at=expires_at,
                tags=tags or [],
            )
            
            # Add to cache
            self._cache[key] = entry
            self._update_memory_usage(entry.size_bytes)
            
            # Add to tags
            if tags:
                self._add_to_tags(key, tags)
            
            # Move to end for LRU
            if self.eviction_policy == "lru":
                self._cache.move_to_end(key)
            
            # Evict if necessary
            await self._evict_entries()
            
            if self.enable_stats:
                self._stats["sets"] += 1
            
            logger.debug(f"Cache set for key: {key} (TTL: {effective_ttl}, tags: {tags})")
            return True
    
    async def delete(self, *keys: str) -> int:
        """Delete keys from cache."""
        async with self._lock:
            deleted_count = 0
            
            for key in keys:
                entry = self._cache.get(key)
                if entry:
                    del self._cache[key]
                    self._update_memory_usage(-entry.size_bytes)
                    self._remove_from_tags(key)
                    deleted_count += 1
                    
                    if self.enable_stats:
                        self._stats["deletes"] += 1
            
            logger.debug(f"Deleted {deleted_count} keys from cache")
            return deleted_count
    
    async def exists(self, *keys: str) -> int:
        """Check if keys exist in cache."""
        async with self._lock:
            exist_count = 0
            current_time = datetime.utcnow()
            
            for key in keys:
                entry = self._cache.get(key)
                if entry and (entry.expires_at is None or entry.expires_at > current_time):
                    exist_count += 1
            
            logger.debug(f"{exist_count} of {len(keys)} keys exist in cache")
            return exist_count
    
    async def expire(self, key: str, ttl: Union[int, timedelta]) -> bool:
        """Set TTL for existing key."""
        async with self._lock:
            entry = self._cache.get(key)
            
            if entry is None or entry.is_expired():
                return False
            
            entry.expires_at = self._convert_ttl(ttl)
            logger.debug(f"Set TTL {ttl} for key: {key}")
            return True
    
    async def ttl(self, key: str) -> int:
        """Get TTL for key."""
        async with self._lock:
            entry = self._cache.get(key)
            
            if entry is None or entry.is_expired():
                return -2  # Key doesn't exist
            
            if entry.expires_at is None:
                return -1  # No TTL set
            
            remaining = entry.expires_at - datetime.utcnow()
            ttl_seconds = int(remaining.total_seconds())
            return max(0, ttl_seconds)
    
    async def increment(self, key: str, amount: int = 1, ttl: Optional[Union[int, timedelta]] = None) -> int:
        """Increment numeric value in cache."""
        async with self._lock:
            entry = self._cache.get(key)
            
            if entry is None or entry.is_expired():
                # Create new entry with initial value
                new_value = amount
                await self.set(key, new_value, ttl=ttl)
            else:
                try:
                    old_size = entry.size_bytes
                    new_value = int(entry.value) + amount
                    entry.value = new_value
                    entry.access()
                    
                    # Update size
                    new_size = entry._calculate_size()
                    self._update_memory_usage(new_size - old_size)
                    entry.size_bytes = new_size
                    
                    # Move to end for LRU
                    if self.eviction_policy == "lru":
                        self._cache.move_to_end(key)
                        
                    # Set TTL if provided and this is a new key
                    if ttl and entry.access_count == 1:
                        entry.expires_at = self._convert_ttl(ttl)
                        
                except (ValueError, TypeError):
                    raise CacheError(f"Cannot increment non-numeric value for key: {key}")
            
            logger.debug(f"Incremented key {key} by {amount}, new value: {new_value}")
            return new_value
    
    async def get_many(self, *keys: str) -> Dict[str, Any]:
        """Get multiple values from cache efficiently."""
        result = {}
        
        for key in keys:
            value = await self.get(key)
            if value is not None:
                result[key] = value
        
        logger.debug(f"Retrieved {len(result)} of {len(keys)} keys from cache")
        return result
    
    async def set_many(
        self,
        mapping: Dict[str, Any],
        ttl: Optional[Union[int, timedelta]] = None,
    ) -> bool:
        """Set multiple values in cache efficiently."""
        success_count = 0
        
        for key, value in mapping.items():
            if await self.set(key, value, ttl=ttl):
                success_count += 1
        
        logger.debug(f"Set {success_count}/{len(mapping)} keys in cache (TTL: {ttl})")
        return success_count == len(mapping)
    
    async def clear(self) -> bool:
        """Clear all keys from cache."""
        async with self._lock:
            count = len(self._cache)
            self._cache.clear()
            self._tags.clear()
            
            if self.enable_stats:
                self._stats["memory_usage_bytes"] = 0
            
            logger.warning(f"Cleared {count} keys from memory cache")
            return True
    
    async def keys(self, pattern: str = "*") -> List[str]:
        """Get keys matching pattern."""
        async with self._lock:
            # Remove expired entries first
            current_time = datetime.utcnow()
            valid_keys = [
                key for key, entry in self._cache.items()
                if entry.expires_at is None or entry.expires_at > current_time
            ]
            
            # Filter by pattern
            if pattern == "*":
                matching_keys = valid_keys
            else:
                matching_keys = [
                    key for key in valid_keys
                    if fnmatch.fnmatch(key, pattern)
                ]
            
            logger.debug(f"Found {len(matching_keys)} keys matching pattern: {pattern}")
            return matching_keys
    
    async def scan_keys(self, pattern: str = "*", count: int = 100) -> List[str]:
        """Scan keys matching pattern (for compatibility with Redis interface)."""
        # For memory cache, this is the same as keys() but with limit
        all_keys = await self.keys(pattern)
        return all_keys[:count]
    
    async def delete_by_tags(self, *tags: str) -> int:
        """Delete all keys associated with given tags."""
        async with self._lock:
            keys_to_delete = set()
            
            for tag in tags:
                if tag in self._tags:
                    keys_to_delete.update(self._tags[tag])
            
            deleted_count = 0
            for key in keys_to_delete:
                if key in self._cache:
                    entry = self._cache[key]
                    del self._cache[key]
                    self._update_memory_usage(-entry.size_bytes)
                    deleted_count += 1
            
            # Clean up tag mappings
            for tag in tags:
                if tag in self._tags:
                    del self._tags[tag]
            
            if self.enable_stats:
                self._stats["deletes"] += deleted_count
            
            logger.debug(f"Deleted {deleted_count} keys by tags: {tags}")
            return deleted_count
    
    async def get_by_tags(self, *tags: str) -> Dict[str, Any]:
        """Get all values associated with given tags."""
        async with self._lock:
            keys_to_get = set()
            
            for tag in tags:
                if tag in self._tags:
                    keys_to_get.update(self._tags[tag])
            
            result = {}
            for key in keys_to_get:
                value = await self.get(key)
                if value is not None:
                    result[key] = value
            
            logger.debug(f"Retrieved {len(result)} keys by tags: {tags}")
            return result
    
    @asynccontextmanager
    async def lock(self, key: str, timeout: int = 10, blocking: bool = True):
        """Simple in-memory lock implementation."""
        lock_key = f"__lock__{key}"
        
        if blocking:
            # Wait for lock to be available
            start_time = time.time()
            while await self.exists(lock_key) and (time.time() - start_time) < timeout:
                await asyncio.sleep(0.01)  # 10ms
        
        if await self.exists(lock_key):
            yield False
            return
        
        # Acquire lock
        await self.set(lock_key, True, ttl=timeout)
        
        try:
            yield True
        finally:
            # Release lock
            await self.delete(lock_key)
    
    async def health_check(self) -> bool:
        """Check cache health."""
        try:
            # Simple health check - try to set and get a value
            test_key = "__health_check__"
            await self.set(test_key, "ok", ttl=1)
            value = await self.get(test_key)
            await self.delete(test_key)
            return value == "ok"
        except Exception as e:
            logger.error(f"Memory cache health check failed: {e}")
            return False
    
    async def info(self) -> Dict[str, Any]:
        """Get comprehensive cache information."""
        async with self._lock:
            total_entries = len(self._cache)
            expired_count = sum(
                1 for entry in self._cache.values()
                if entry.is_expired()
            )
            
            hit_rate = 0.0
            if self.enable_stats:
                total_requests = self._stats["hits"] + self._stats["misses"]
                if total_requests > 0:
                    hit_rate = self._stats["hits"] / total_requests
            
            # Calculate memory usage if not tracked
            if not self.enable_stats:
                memory_usage = sum(entry.size_bytes for entry in self._cache.values())
            else:
                memory_usage = self._stats["memory_usage_bytes"]
            
            return {
                "type": "memory",
                "max_size": self.max_size,
                "max_memory_bytes": self.max_memory_bytes,
                "current_size": total_entries,
                "expired_entries": expired_count,
                "memory_usage_bytes": memory_usage,
                "memory_usage_mb": round(memory_usage / (1024 * 1024), 2),
                "hit_rate": round(hit_rate * 100, 2),
                "eviction_policy": self.eviction_policy,
                "stats": self._stats.copy() if self.enable_stats else {},
                "default_ttl": self.default_ttl,
                "cleanup_interval": self.cleanup_interval,
                "tag_count": len(self._tags),
                "is_running": self._is_running,
            }
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get cache metrics (sync version)."""
        if not self.enable_stats:
            return {"stats_disabled": True}
        
        total_requests = self._stats["hits"] + self._stats["misses"]
        hit_rate = (self._stats["hits"] / total_requests * 100) if total_requests > 0 else 0
        
        return {
            **self._stats,
            "hit_rate": round(hit_rate, 2),
            "total_requests": total_requests,
            "current_size": len(self._cache),
            "memory_usage_mb": round(self._stats["memory_usage_bytes"] / (1024 * 1024), 2),
        }
    
    def reset_metrics(self) -> None:
        """Reset cache metrics."""
        if self.enable_stats:
            # Preserve current memory usage
            current_memory = self._stats["memory_usage_bytes"]
            self._stats = {
                "hits": 0,
                "misses": 0,
                "sets": 0,
                "deletes": 0,
                "evictions": 0,
                "expirations": 0,
                "memory_usage_bytes": current_memory,
                "cleanup_runs": 0,
            }
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start_cleanup_task()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.stop_cleanup_task()
        await self.clear()