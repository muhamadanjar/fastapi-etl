"""
Enhanced base interfaces for cache implementations.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional, Union, List, Dict
from datetime import timedelta


class CacheInterface(ABC):
    """
    Enhanced abstract cache interface.
    
    Defines the contract for all cache implementations with
    comprehensive async operations.
    """
    
    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        pass
    
    @abstractmethod
    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[Union[int, timedelta]] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """Set value in cache with conditional options."""
        pass
    
    @abstractmethod
    async def delete(self, *keys: str) -> int:
        """Delete keys from cache."""
        pass
    
    @abstractmethod
    async def exists(self, *keys: str) -> int:
        """Check if keys exist."""
        pass
    
    @abstractmethod
    async def clear(self) -> bool:
        """Clear all cache entries."""
        pass
    
    @abstractmethod
    async def health_check(self) -> bool:
        """Check cache health."""
        pass
    
    # Optional methods that implementations should provide
    async def expire(self, key: str, ttl: Union[int, timedelta]) -> bool:
        """Set TTL for existing key."""
        raise NotImplementedError("expire method not implemented")
    
    async def ttl(self, key: str) -> int:
        """Get TTL for key."""
        raise NotImplementedError("ttl method not implemented")
    
    async def increment(self, key: str, amount: int = 1, ttl: Optional[Union[int, timedelta]] = None) -> int:
        """Increment numeric value."""
        raise NotImplementedError("increment method not implemented")
    
    async def get_many(self, *keys: str) -> Dict[str, Any]:
        """Get multiple values efficiently."""
        result = {}
        for key in keys:
            value = await self.get(key)
            if value is not None:
                result[key] = value
        return result
    
    async def set_many(
        self,
        mapping: Dict[str, Any],
        ttl: Optional[Union[int, timedelta]] = None,
    ) -> bool:
        """Set multiple values efficiently."""
        success_count = 0
        for key, value in mapping.items():
            if await self.set(key, value, ttl=ttl):
                success_count += 1
        return success_count == len(mapping)
    
    async def keys(self, pattern: str = "*") -> List[str]:
        """Get keys matching pattern."""
        raise NotImplementedError("keys method not implemented")
    
    async def info(self) -> Dict[str, Any]:
        """Get cache information and statistics."""
        return {
            "type": "unknown",
            "status": "available" if await self.health_check() else "unavailable"
        }


class CacheBackend(ABC):
    """
    Abstract cache backend for different storage implementations.
    
    This is a higher-level interface for cache backends that might
    need additional lifecycle management.
    """
    
    @abstractmethod
    async def connect(self) -> None:
        """Connect to cache backend."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from cache backend."""
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if backend is connected."""
        pass
    """
    Abstract cache interface.
    
    Defines the contract for cache implementations.
    """
    
    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        pass
    
    @abstractmethod
    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[Union[int, timedelta]] = None,
    ) -> bool:
        """Set value in cache."""
        pass
    
    @abstractmethod
    async def delete(self, *keys: str) -> int:
        """Delete keys from cache."""
        pass
    
    @abstractmethod
    async def exists(self, *keys: str) -> int:
        """Check if keys exist."""
        pass
    
    @abstractmethod
    async def clear(self) -> bool:
        """Clear all cache entries."""
        pass
    
    @abstractmethod
    async def health_check(self) -> bool:
        """Check cache health."""
        pass