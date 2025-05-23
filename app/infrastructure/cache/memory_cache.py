"""
In-memory cache implementation.

This module provides thread-safe in-memory caching functionality
for development and testing purposes, or when Redis is not available.
"""

import logging
import threading
import time
from typing import Any, Optional, Dict, List, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
from collections import OrderedDict

from ...core.exceptions import CacheError

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """
    Cache entry with value and expiration information.
    """
    value: Any
    created_at: datetime
    expires_at: Optional[datetime] = None
    access_count: int = 0
    last_accessed: Optional[datetime] = None
    
    def is_expired(self) -> bool:
        """Check if cache entry has expired."""
        if self.expires_at is None:
            return False
        return datetime.utcnow() > self.expires_at
    
    def access(self) -> None:
        """Record access to this cache entry."""
        self.access_count += 1
        self.last_accessed = datetime.utcnow()


class MemoryCache:
    """
    Thread-safe in-memory cache with TTL support and LRU eviction.
    
    This implementation provides similar functionality to Redis cache
    but stores data in memory. Suitable for development, testing,
    or single-process applications.
    """
    
    def __init__(
        self,
        max_size: int = 1000,
        default_ttl: Optional[Union[int, timedelta]] = None,
        cleanup_interval: int = 300,  # 5 minutes
    ):
        """
        Initialize memory cache.
        
        Args:
            max_size: Maximum number of entries to store
            default_ttl: Default TTL for cache entries
            cleanup_interval: Interval in seconds for cleanup of expired entries
        """
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.cleanup_interval = cleanup_interval
        
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = threading.RLock()
        self._stats = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "deletes": 0,
            "evictions": 0,
            "expirations": 0,
        }
        
        # Start cleanup thread
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_expired,
            daemon=True
        )
        self._cleanup_thread.start()
        
        logger.info(f"Memory cache initialized with max_size={max_size}")
    
    def _cleanup_expired(self) -> None:
        """Background thread to clean up expired entries."""
        while True:
            try:
                time.sleep(self.cleanup_interval)
                self._remove_expired_entries()
            except Exception as e:
                logger.error(f"Error in cache cleanup thread: {e}")
    
    def _remove_expired_entries(self) -> None:
        """Remove expired entries from cache."""
        with self._lock:
            expired_keys = []
            
            for key, entry in self._cache.items():
                if entry.is_expired():
                    expired_keys.append(key)
            
            for key in expired_keys:
                del self._cache[key]
                self._stats["expirations"] += 1
            
            if expired_keys:
                logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")
    
    def _evict_lru(self) -> None:
        """Evict least recently used entry."""
        if self._cache:
            # OrderedDict maintains insertion order, move accessed items to end
            # So first item is least recently used
            key, _ = self._cache.popitem(last=False)
            self._stats["evictions"] += 1
            logger.debug(f"Evicted LRU cache entry: {key}")
    
    def _convert_ttl(self, ttl: Optional[Union[int, timedelta]]) -> Optional[datetime]:
        """
        Convert TTL to expiration datetime.
        
        Args:
            ttl: TTL in seconds or timedelta
            
        Returns:
            Expiration datetime or None for no expiration
        """
        if ttl is None:
            return None
        
        if isinstance(ttl, timedelta):
            ttl = int(ttl.total_seconds())
        
        if ttl <= 0:
            return None
        
        return datetime.utcnow() + timedelta(seconds=ttl)
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found/expired
        """
        with self._lock:
            entry = self._cache.get(key)
            
            if entry is None:
                self._stats["misses"] += 1
                logger.debug(f"Cache miss for key: {key}")
                return None
            
            if entry.is_expired():
                del self._cache[key]
                self._stats["misses"] += 1
                self._stats["expirations"] += 1
                logger.debug(f"Cache expired for key: {key}")
                return None
            
            # Move to end (mark as recently used)
            entry.access()
            self._cache.move_to_end(key)
            
            self._stats["hits"] += 1
            logger.debug(f"Cache hit for key: {key}")
            return entry.value
    
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
        """
        with self._lock:
            existing_entry = self._cache.get(key)
            
            # Check nx condition
            if nx and existing_entry is not None and not existing_entry.is_expired():
                return False
            
            # Check xx condition
            if xx and (existing_entry is None or existing_entry.is_expired()):
                return False
            
            # Use provided TTL or default
            effective_ttl = ttl if ttl is not None else self.default_ttl
            expires_at = self._convert_ttl(effective_ttl)
            
            # Create new entry
            entry = CacheEntry(
                value=value,
                created_at=datetime.utcnow(),
                expires_at=expires_at,
            )
            
            # Add to cache
            self._cache[key] = entry
            self._cache.move_to_end(key)  # Mark as recently used
            
            # Evict if necessary
            while len(self._cache) > self.max_size:
                self._evict_lru()
            
            self._stats["sets"] += 1
            logger.debug(f"Cache set for key: {key} (TTL: {effective_ttl})")
            return True
    
    def delete(self, *keys: str) -> int:
        """
        Delete keys from cache.
        
        Args:
            keys: Cache keys to delete
            
        Returns:
            Number of keys that were deleted
        """
        with self._lock:
            deleted_count = 0
            
            for key in keys:
                if key in self._cache:
                    del self._cache[key]
                    deleted_count += 1
                    self._stats["deletes"] += 1
            
            logger.debug(f"Deleted {deleted_count} keys from cache")
            return deleted_count
    
    def exists(self, *keys: str) -> int:
        """
        Check if keys exist in cache.
        
        Args:
            keys: Cache keys to check
            
        Returns:
            Number of keys that exist and are not expired
        """
        with self._lock:
            exist_count = 0
            
            for key in keys:
                entry = self._cache.get(key)
                if entry is not None and not entry.is_expired():
                    exist_count += 1
            
            logger.debug(f"{exist_count} of {len(keys)} keys exist in cache")
            return exist_count
    
    def expire(self, key: str, ttl: Union[int, timedelta]) -> bool:
        """
        Set TTL for existing key.
        
        Args:
            key: Cache key
            ttl: Time to live (seconds or timedelta)
            
        Returns:
            True if TTL was set, False if key doesn't exist
        """
        with self._lock:
            entry = self._cache.get(key)
            
            if entry is None or entry.is_expired():
                return False
            
            entry.expires_at = self._convert_ttl(ttl)
            logger.debug(f"Set TTL {ttl} for key: {key}")
            return True
    
    def ttl(self, key: str) -> int:
        """
        Get TTL for key.
        
        Args:
            key: Cache key
            
        Returns:
            TTL in seconds (-1 if no TTL, -2 if key doesn't exist)
        """
        with self._lock:
            entry = self._cache.get(key)
            
            if entry is None:
                return -2  # Key doesn't exist
            
            if entry.is_expired():
                return -2  # Key expired (doesn't exist)
            
            if entry.expires_at is None:
                return -1  # No TTL set
            
            remaining = entry.expires_at - datetime.utcnow()
            ttl_seconds = int(remaining.total_seconds())
            
            logger.debug(f"TTL for key {key}: {ttl_seconds}")
            return max(0, ttl_seconds)
    
    def increment(self, key: str, amount: int = 1) -> int:
        """
        Increment numeric value in cache.
        
        Args:
            key: Cache key
            amount: Amount to increment by
            
        Returns:
            New value after increment
            
        Raises:
            CacheError: If value is not numeric
        """
        with self._lock:
            entry = self._cache.get(key)
            
            if entry is None or entry.is_expired():
                # Create new entry with initial value
                new_value = amount
                self.set(key, new_value)
            else:
                try:
                    new_value = int(entry.value) + amount
                    entry.value = new_value
                    entry.access()
                    self._cache.move_to_end(key)
                except (ValueError, TypeError):
                    raise CacheError(f"Cannot increment non-numeric value for key: {key}")
            
            logger.debug(f"Incremented key {key} by {amount}, new value: {new_value}")
            return new_value
    
    def decrement(self, key: str, amount: int = 1) -> int:
        """
        Decrement numeric value in cache.
        
        Args:
            key: Cache key
            amount: Amount to decrement by
            
        Returns:
            New value after decrement
            
        Raises:
            CacheError: If value is not numeric
        """
        return self.increment(key, -amount)
    
    def get_many(self, *keys: str) -> Dict[str, Any]:
        """
        Get multiple values from cache.
        
        Args:
            keys: Cache keys to retrieve
            
        Returns:
            Dictionary mapping keys to values (missing keys are excluded)
        """
        result = {}
        
        for key in keys:
            value = self.get(key)
            if value is not None:
                result[key] = value
        
        logger.debug(f"Retrieved {len(result)} of {len(keys)} keys from cache")
        return result
    
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
            True (always succeeds for memory cache)
        """
        for key, value in mapping.items():
            self.set(key, value, ttl=ttl)
        
        logger.debug(f"Set {len(mapping)} keys in cache (TTL: {ttl})")
        return True
    
    def flush_all(self) -> bool:
        """
        Clear all keys from cache.
        
        Returns:
            True (always succeeds)
        """
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            logger.warning(f"Flushed {count} keys from memory cache")
            return True
    
    def keys(self, pattern: str = "*") -> List[str]:
        """
        Get keys matching pattern.
        
        Args:
            pattern: Pattern to match (supports * wildcard)
            
        Returns:
            List of matching keys
        """
        import fnmatch
        
        with self._lock:
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
    
    def info(self) -> Dict[str, Any]:
        """
        Get cache information and statistics.
        
        Returns:
            Dictionary with cache information
        """
        with self._lock:
            total_entries = len(self._cache)
            expired_count = sum(
                1 for entry in self._cache.values()
                if entry.is_expired()
            )
            
            hit_rate = 0.0
            total_requests = self._stats["hits"] + self._stats["misses"]
            if total_requests > 0:
                hit_rate = self._stats["hits"] / total_requests
            
            return {
                "type": "memory",
                "max_size": self.max_size,
                "current_size": total_entries,
                "expired_entries": expired_count,
                "hit_rate": hit_rate,
                "stats": self._stats.copy(),
                "default_ttl": self.default_ttl,
                "cleanup_interval": self.cleanup_interval,
            }
    
    def ping(self) -> bool:
        """
        Check if cache is available.
        
        Returns:
            True (memory cache is always available)
        """
        return True
    
    def close(self) -> None:
        """
        Close cache (cleanup resources).
        
        For memory cache, this clears all data.
        """
        with self._lock:
            self._cache.clear()
            logger.info("Memory cache closed and cleared")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def __len__(self) -> int:
        """Get number of entries in cache."""
        with self._lock:
            return len(self._cache)
    
    def __contains__(self, key: str) -> bool:
        """Check if key exists in cache."""
        return self.exists(key) > 0