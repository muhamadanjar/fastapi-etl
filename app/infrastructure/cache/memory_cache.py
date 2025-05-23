from cachetools import TTLCache
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class MemoryCache:
    def __init__(self, maxsize: int = 1000, ttl: int = 300):
        self.cache = TTLCache(maxsize=maxsize, ttl=ttl)

    def set(self, key: str, value: str) -> None:
        try:
            self.cache[key] = value
        except Exception as e:
            logger.error(f"MemoryCache SET error: {e}")

    def get(self, key: str) -> Optional[str]:
        try:
            return self.cache.get(key)
        except Exception as e:
            logger.error(f"MemoryCache GET error: {e}")
            return None

    def delete(self, key: str) -> bool:
        try:
            return self.cache.pop(key, None) is not None
        except Exception as e:
            logger.error(f"MemoryCache DELETE error: {e}")
            return False

# Optional singleton
memory_cache = MemoryCache()
