import os
import logging
from redis.asyncio import Redis
from typing import Optional

logger = logging.getLogger(__name__)

class RedisCache:
    def __init__(self, url: Optional[str] = None, default_expire: int = 3600):
        self.redis = Redis.from_url(url or os.getenv("REDIS_URL", "redis://localhost:6379"))
        self.default_expire = default_expire

    async def set(self, key: str, value: str, expire: Optional[int] = None) -> bool:
        try:
            return await self.redis.set(key, value, ex=expire or self.default_expire)
        except Exception as e:
            logger.error(f"Redis SET error: {e}")
            return False

    async def get(self, key: str) -> Optional[str]:
        try:
            value = await self.redis.get(key)
            return value.decode() if value else None
        except Exception as e:
            logger.error(f"Redis GET error: {e}")
            return None

    async def delete(self, key: str) -> bool:
        try:
            return await self.redis.delete(key) > 0
        except Exception as e:
            logger.error(f"Redis DELETE error: {e}")
            return False

    async def close(self):
        await self.redis.close()

# Optional singleton
redis_cache = RedisCache()
