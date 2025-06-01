"""
Cache Manager - Orchestrates cache instances with fallback logic.

This module provides high-level cache management with automatic
fallback from Redis to memory cache when Redis is unavailable.
"""

import asyncio
import logging
from typing import Optional, Dict, Any

from .base import CacheInterface
from .redis_cache import RedisCache
from .memory_cache import MemoryCache

logger = logging.getLogger(__name__)


class CacheManager:
    """
    Cache manager with automatic fallback support.
    
    Features:
    - Primary cache with fallback to secondary cache
    - Automatic health monitoring and switching
    - Graceful degradation when primary cache fails
    - Background recovery attempts
    """
    
    def __init__(self):
        self._primary_cache: Optional[CacheInterface] = None
        self._fallback_cache: Optional[MemoryCache] = None
        self._use_fallback = False
        self._recovery_task: Optional[asyncio.Task] = None
        self._recovery_interval = 60  # seconds
        self._is_running = False
    
    async def initialize(
        self,
        primary_cache: Optional[CacheInterface] = None,
        enable_fallback: bool = True,
        fallback_config: Optional[Dict[str, Any]] = None,
        recovery_interval: int = 60,
    ) -> None:
        """
        Initialize cache manager.
        
        Args:
            primary_cache: Primary cache instance (e.g., Redis)
            enable_fallback: Whether to enable memory cache fallback
            fallback_config: Configuration for fallback cache
            recovery_interval: Interval for recovery attempts in seconds
        """
        self._primary_cache = primary_cache
        self._recovery_interval = recovery_interval
        self._is_running = True
        
        # Initialize fallback cache
        if enable_fallback:
            config = fallback_config or {
                'max_size': 1000,
                'max_memory_mb': 50,
                'default_ttl': 300,
                'eviction_policy': 'lru',
                'enable_stats': True,
            }
            
            self._fallback_cache = MemoryCache(**config)
            await self._fallback_cache.start_cleanup_task()
            logger.info("Fallback memory cache initialized")
        
        # Check primary cache health
        if self._primary_cache:
            try:
                if await self._primary_cache.health_check():
                    self._use_fallback = False
                    logger.info("Primary cache is healthy")
                else:
                    self._use_fallback = True
                    logger.warning("Primary cache unhealthy, using fallback")
            except Exception as e:
                logger.error(f"Primary cache check failed: {e}, using fallback")
                self._use_fallback = True
        else:
            self._use_fallback = True
            logger.info("No primary cache provided, using fallback only")
        
        # Start recovery monitoring
        if self._primary_cache and enable_fallback:
            self._recovery_task = asyncio.create_task(self._recovery_loop())
    
    async def shutdown(self) -> None:
        """Shutdown cache manager and cleanup resources."""
        self._is_running = False
        
        # Stop recovery task
        if self._recovery_task:
            self._recovery_task.cancel()
            try:
                await self._recovery_task
            except asyncio.CancelledError:
                pass
            self._recovery_task = None
        
        # Shutdown fallback cache
        if self._fallback_cache:
            await self._fallback_cache.stop_cleanup_task()
            await self._fallback_cache.clear()
            self._fallback_cache = None
        
        # Clear primary cache reference
        self._primary_cache = None
        
        logger.info("Cache manager shutdown completed")
    
    async def get_cache(self) -> Optional[CacheInterface]:
        """
        Get available cache instance.
        
        Returns:
            Cache instance or None if no cache available
        """
        # Try primary cache first (if not in fallback mode)
        if self._primary_cache and not self._use_fallback:
            try:
                if await self._primary_cache.health_check():
                    return self._primary_cache
                else:
                    logger.warning("Primary cache health check failed, switching to fallback")
                    self._use_fallback = True
            except Exception as e:
                logger.error(f"Primary cache error: {e}, switching to fallback")
                self._use_fallback = True
        
        # Return fallback cache
        if self._fallback_cache:
            return self._fallback_cache
        
        logger.error("No cache available")
        return None
    
    async def force_primary(self) -> bool:
        """
        Force attempt to use primary cache.
        
        Returns:
            True if successfully switched to primary cache
        """
        if not self._primary_cache:
            return False
        
        try:
            if await self._primary_cache.health_check():
                self._use_fallback = False
                logger.info("Successfully forced switch to primary cache")
                return True
        except Exception as e:
            logger.error(f"Failed to force primary cache: {e}")
        
        return False
    
    async def force_fallback(self) -> bool:
        """
        Force switch to fallback cache.
        
        Returns:
            True if fallback cache is available
        """
        if self._fallback_cache:
            self._use_fallback = True
            logger.info("Forced switch to fallback cache")
            return True
        
        logger.error("No fallback cache available")
        return False
    
    async def get_status(self) -> Dict[str, Any]:
        """
        Get detailed cache manager status.
        
        Returns:
            Status information dictionary
        """
        status = {
            "primary_cache_available": self._primary_cache is not None,
            "fallback_cache_available": self._fallback_cache is not None,
            "using_fallback": self._use_fallback,
            "is_running": self._is_running,
            "recovery_interval": self._recovery_interval,
        }
        
        # Check primary cache health
        if self._primary_cache:
            try:
                primary_healthy = await self._primary_cache.health_check()
                status["primary_cache_healthy"] = primary_healthy
                
                if primary_healthy:
                    primary_info = await self._primary_cache.info()
                    status["primary_cache_info"] = primary_info
            except Exception as e:
                status["primary_cache_healthy"] = False
                status["primary_cache_error"] = str(e)
        
        # Check fallback cache health
        if self._fallback_cache:
            try:
                fallback_healthy = await self._fallback_cache.health_check()
                status["fallback_cache_healthy"] = fallback_healthy
                
                if fallback_healthy:
                    fallback_info = await self._fallback_cache.info()
                    status["fallback_cache_info"] = fallback_info
            except Exception as e:
                status["fallback_cache_healthy"] = False
                status["fallback_cache_error"] = str(e)
        
        return status
    
    async def _recovery_loop(self) -> None:
        """Background loop to attempt recovery to primary cache."""
        while self._is_running:
            try:
                await asyncio.sleep(self._recovery_interval)
                
                if self._is_running and self._use_fallback and self._primary_cache:
                    await self._attempt_recovery()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cache recovery loop: {e}")
    
    async def _attempt_recovery(self) -> None:
        """Attempt to recover to primary cache."""
        try:
            if await self._primary_cache.health_check():
                self._use_fallback = False
                logger.info("Successfully recovered to primary cache")
                
                # Optionally sync some data from fallback to primary
                await self._sync_fallback_to_primary()
                
        except Exception as e:
            logger.debug(f"Recovery attempt failed: {e}")
    
    async def _sync_fallback_to_primary(self) -> None:
        """Sync important data from fallback to primary cache."""
        if not self._fallback_cache or not self._primary_cache:
            return
        
        try:
            # Get recent keys from fallback cache
            recent_keys = await self._fallback_cache.keys("*")
            
            # Limit sync to avoid overwhelming primary cache
            sync_limit = 100
            keys_to_sync = recent_keys[:sync_limit]
            
            synced_count = 0
            for key in keys_to_sync:
                try:
                    value = await self._fallback_cache.get(key)
                    if value is not None:
                        ttl = await self._fallback_cache.ttl(key)
                        ttl_value = ttl if ttl > 0 else None
                        
                        await self._primary_cache.set(key, value, ttl=ttl_value)
                        synced_count += 1
                except Exception as e:
                    logger.debug(f"Failed to sync key {key}: {e}")
            
            if synced_count > 0:
                logger.info(f"Synced {synced_count} keys from fallback to primary cache")
                
        except Exception as e:
            logger.error(f"Error syncing fallback to primary: {e}")


# Global cache manager instance
cache_manager = CacheManager()
