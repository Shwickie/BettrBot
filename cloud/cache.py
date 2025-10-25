#!/usr/bin/env python3
"""
Simple thread-safe in-memory cache for Flask app
Speeds up dashboard by caching predictions, power rankings, etc.
"""

from datetime import datetime, timedelta
from threading import Lock
import logging

logger = logging.getLogger(__name__)

class SimpleCache:
    """
    Thread-safe in-memory cache with TTL (time to live)

    Usage:
        cache = SimpleCache()
        cache.set('predictions', data, ttl_seconds=3600)  # Cache for 1 hour
        result = cache.get('predictions')  # Returns data or None if expired
    """

    def __init__(self):
        self._cache = {}
        self._lock = Lock()
        logger.info("Cache initialized")

    def get(self, key):
        """
        Get cached value if it exists and hasn't expired

        Args:
            key: Cache key string

        Returns:
            Cached value or None if not found/expired
        """
        with self._lock:
            if key in self._cache:
                value, expiry = self._cache[key]
                if datetime.now() < expiry:
                    logger.debug(f"Cache HIT: {key}")
                    return value
                else:
                    # Expired - remove it
                    del self._cache[key]
                    logger.debug(f"Cache EXPIRED: {key}")
            else:
                logger.debug(f"Cache MISS: {key}")
            return None

    def set(self, key, value, ttl_seconds=3600):
        """
        Set cache value with TTL (time to live)

        Args:
            key: Cache key string
            value: Any Python object to cache
            ttl_seconds: Time to live in seconds (default 1 hour)
        """
        with self._lock:
            expiry = datetime.now() + timedelta(seconds=ttl_seconds)
            self._cache[key] = (value, expiry)
            logger.info(f"Cache SET: {key} (expires in {ttl_seconds}s)")

    def clear(self, key=None):
        """
        Clear specific key or entire cache

        Args:
            key: If provided, only clear this key. If None, clear all.
        """
        with self._lock:
            if key:
                if key in self._cache:
                    del self._cache[key]
                    logger.info(f"Cache CLEARED: {key}")
            else:
                count = len(self._cache)
                self._cache.clear()
                logger.info(f"Cache CLEARED ALL ({count} items)")

    def stats(self):
        """Get cache statistics"""
        with self._lock:
            total_items = len(self._cache)
            expired_items = sum(
                1 for _, (_, expiry) in self._cache.items()
                if datetime.now() >= expiry
            )
            return {
                'total_items': total_items,
                'active_items': total_items - expired_items,
                'expired_items': expired_items
            }

    def cleanup_expired(self):
        """Remove all expired items from cache"""
        with self._lock:
            now = datetime.now()
            expired_keys = [
                key for key, (_, expiry) in self._cache.items()
                if now >= expiry
            ]
            for key in expired_keys:
                del self._cache[key]

            if expired_keys:
                logger.info(f"Cache cleanup: Removed {len(expired_keys)} expired items")


# Global cache instance
cache = SimpleCache()


# Cache TTL configurations (in seconds)
CACHE_TTL = {
    'predictions': 3600,        # 1 hour - predictions change when model retrains
    'power_rankings': 21600,    # 6 hours - rankings change when games complete
    'betting_recs': 1800,       # 30 minutes - recommendations update with odds
    'dashboard_stats': 300,     # 5 minutes - general stats
    'live_opportunities': 900,  # 15 minutes - live betting changes faster
    'odds_summary': 600,        # 10 minutes - odds data summary
}


def get_ttl(cache_type):
    """Get TTL for a specific cache type"""
    return CACHE_TTL.get(cache_type, 3600)  # Default 1 hour
