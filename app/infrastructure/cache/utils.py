"""
Cache utilities and helper functions.
"""

from typing import Any, List, Optional
from hashlib import md5
import json


def cache_key(*parts: Any, prefix: str = "", separator: str = ":", hash_long_keys: bool = True) -> str:
    """
    Generate cache key from parts with optional hashing for long keys.
    
    Args:
        parts: Key parts to join
        prefix: Optional prefix for the key
        separator: Separator to use between parts
        hash_long_keys: Whether to hash keys longer than 250 characters
        
    Returns:
        Generated cache key
    """
    # Convert all parts to strings
    key_parts = [str(part) for part in parts if part is not None and str(part)]
    
    # Add prefix if provided
    if prefix:
        key_parts.insert(0, prefix)
    
    # Join parts
    key = separator.join(key_parts)
    
    # Hash long keys to avoid Redis key length limits
    if hash_long_keys and len(key) > 250:
        key_hash = md5(key.encode()).hexdigest()
        # Keep some readable prefix
        readable_prefix = key[:100] if len(key) > 100 else key
        key = f"{readable_prefix}:hash:{key_hash}"
    
    return key


class CacheKeyBuilder:
    """
    Builder class for constructing cache keys with consistent patterns.
    """
    
    def __init__(self, base_prefix: str = ""):
        """
        Initialize key builder.
        
        Args:
            base_prefix: Base prefix for all keys
        """
        self.base_prefix = base_prefix
        self.parts: List[str] = []
    
    def add(self, *parts: Any) -> "CacheKeyBuilder":
        """
        Add parts to the key.
        
        Args:
            parts: Parts to add
            
        Returns:
            Self for chaining
        """
        self.parts.extend(str(part) for part in parts if part is not None)
        return self
    
    def user(self, user_id: Any) -> "CacheKeyBuilder":
        """Add user-specific part."""
        return self.add("user", user_id)
    
    def session(self, session_id: Any) -> "CacheKeyBuilder":
        """Add session-specific part."""
        return self.add("session", session_id)
    
    def model(self, model_name: str, model_id: Any = None) -> "CacheKeyBuilder":
        """Add model-specific part."""
        if model_id is not None:
            return self.add(model_name, model_id)
        return self.add(model_name)
    
    def list(self, page: int = 1, limit: int = 10, **filters) -> "CacheKeyBuilder":
        """Add list pagination and filters."""
        self.add("list", f"page:{page}", f"limit:{limit}")
        
        # Add sorted filters for consistent keys
        if filters:
            filter_parts = []
            for key, value in sorted(filters.items()):
                if value is not None:
                    filter_parts.append(f"{key}:{value}")
            if filter_parts:
                self.add("filters", ":".join(filter_parts))
        
        return self
    
    def build(self, separator: str = ":") -> str:
        """
        Build the final cache key.
        
        Args:
            separator: Separator to use between parts
            
        Returns:
            Generated cache key
        """
        return cache_key(*self.parts, prefix=self.base_prefix, separator=separator)
    
    def reset(self) -> "CacheKeyBuilder":
        """Reset the builder for reuse."""
        self.parts.clear()
        return self


def serialize_cache_args(*args, **kwargs) -> str:
    """
    Serialize function arguments to create consistent cache keys.
    
    Args:
        args: Positional arguments
        kwargs: Keyword arguments
        
    Returns:
        Serialized string suitable for cache key
    """
    # Create a deterministic representation
    cache_data = {
        'args': args,
        'kwargs': sorted(kwargs.items()) if kwargs else []
    }
    
    # Serialize to JSON with sorted keys for consistency
    serialized = json.dumps(
        cache_data,
        sort_keys=True,
        default=str,
        separators=(',', ':')
    )
    
    # Hash if too long
    if len(serialized) > 200:
        return md5(serialized.encode()).hexdigest()
    
    return serialized


def get_cache_namespace(obj: Any) -> str:
    """
    Get cache namespace from object (class name, module, etc.).
    
    Args:
        obj: Object to get namespace from
        
    Returns:
        Cache namespace string
    """
    if hasattr(obj, '__class__'):
        return f"{obj.__class__.__module__}.{obj.__class__.__name__}"
    elif hasattr(obj, '__name__'):
        return obj.__name__
    else:
        return str(type(obj).__name__)