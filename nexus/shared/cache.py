"""
L1 / L2 / L3 Cache-Aside implementation.

Layers
------
L1  – In-process ``OrderedDict`` with LRU eviction and asyncio lock.
L2  – Redis with per-key TTL and JSON serialisation.
L3  – Caller-supplied async loader function (DB, API, …).

Usage
-----
    from nexus.shared.cache import cache

    value = await cache.get("my_key", loader=fetch_from_db, l1_ttl=30, l2_ttl=300)
    await cache.invalidate("my_key")
    await cache.invalidate_tags(["user:42", "session:abc"])
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Awaitable

import structlog

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Enums & data classes
# ---------------------------------------------------------------------------

class CacheLevel(Enum):
    L1_MEMORY = "l1_memory"
    L2_REDIS = "l2_redis"
    L3_DB = "l3_db"


@dataclass
class CacheEntry:
    key: str
    value: Any
    created_at: float = field(default_factory=time.monotonic)
    ttl: float = 30.0
    hits: int = 0

    @property
    def is_expired(self) -> bool:
        return (time.monotonic() - self.created_at) >= self.ttl


# ---------------------------------------------------------------------------
# L1 – In-process LRU cache
# ---------------------------------------------------------------------------

class L1Cache:
    """Thread-safe in-process LRU cache backed by ``collections.OrderedDict``."""

    def __init__(self, max_size: int = 1000) -> None:
        self.max_size = max_size
        self._store: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = asyncio.Lock()
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    # ------------------------------------------------------------------ #
    async def get(self, key: str) -> Any | None:
        async with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self._misses += 1
                return None
            if entry.is_expired:
                del self._store[key]
                self._misses += 1
                return None
            # Move to end (most-recently used)
            self._store.move_to_end(key)
            entry.hits += 1
            self._hits += 1
            return entry.value

    async def set(self, key: str, value: Any, ttl: float = 30.0) -> None:
        async with self._lock:
            if key in self._store:
                self._store.move_to_end(key)
            self._store[key] = CacheEntry(key=key, value=value, ttl=ttl)
            while len(self._store) > self.max_size:
                evicted_key, _ = self._store.popitem(last=False)
                self._evictions += 1
                log.debug("l1_cache.eviction", evicted_key=evicted_key)

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._store.pop(key, None)

    async def clear(self) -> None:
        async with self._lock:
            self._store.clear()

    async def stats(self) -> dict:
        async with self._lock:
            total = self._hits + self._misses
            hit_rate = self._hits / total if total else 0.0
            return {
                "hit_rate": round(hit_rate, 4),
                "hits": self._hits,
                "misses": self._misses,
                "size": len(self._store),
                "max_size": self.max_size,
                "evictions": self._evictions,
            }


# ---------------------------------------------------------------------------
# L2 – Redis cache
# ---------------------------------------------------------------------------

class L2Cache:
    """Redis-backed cache with JSON serialisation and per-key TTL."""

    def __init__(self, redis_client=None) -> None:
        # redis_client is injected at runtime; falls back to shared pool lazily
        self._redis = redis_client

    def _client(self):
        if self._redis is not None:
            return self._redis
        # Lazy import to avoid circular dependency at module load time
        from nexus.shared.config import settings
        import redis.asyncio as aioredis
        self._redis = aioredis.from_url(settings.redis_url, decode_responses=True)
        return self._redis

    # ------------------------------------------------------------------ #
    async def get(self, key: str) -> Any | None:
        try:
            raw = await self._client().get(key)
            if raw is None:
                return None
            return json.loads(raw)
        except Exception:
            log.warning("l2_cache.get_error", key=key, exc_info=True)
            return None

    async def set(self, key: str, value: Any, ttl: int = 60) -> None:
        try:
            serialised = json.dumps(value, default=str)
            await self._client().set(key, serialised, ex=ttl)
        except Exception:
            log.warning("l2_cache.set_error", key=key, exc_info=True)

    async def delete(self, key: str) -> None:
        try:
            await self._client().delete(key)
        except Exception:
            log.warning("l2_cache.delete_error", key=key, exc_info=True)

    async def invalidate_pattern(self, pattern: str) -> int:
        """Scan Redis and delete all keys matching *pattern*. Returns deleted count."""
        deleted = 0
        try:
            client = self._client()
            cursor = 0
            while True:
                cursor, keys = await client.scan(cursor=cursor, match=pattern, count=100)
                if keys:
                    await client.delete(*keys)
                    deleted += len(keys)
                if cursor == 0:
                    break
        except Exception:
            log.warning("l2_cache.invalidate_pattern_error", pattern=pattern, exc_info=True)
        return deleted

    async def stats(self) -> dict:
        try:
            info = await self._client().info("memory")
            return {
                "used_memory": info.get("used_memory", 0),
                "used_memory_human": info.get("used_memory_human", "?"),
                "used_memory_peak_human": info.get("used_memory_peak_human", "?"),
                "mem_fragmentation_ratio": info.get("mem_fragmentation_ratio", 0),
            }
        except Exception:
            log.warning("l2_cache.stats_error", exc_info=True)
            return {}


# ---------------------------------------------------------------------------
# CacheAside – unified L1 → L2 → DB pattern
# ---------------------------------------------------------------------------

_PUBSUB_CHANNEL = "nexus:cache:invalidation"


class CacheAside:
    """
    Unified cache-aside (L1 memory → L2 Redis → L3 loader) with:
    - Tag-based invalidation over Redis pub/sub
    - Warm-up helper
    - Combined stats
    """

    def __init__(
        self,
        l1: L1Cache | None = None,
        l2: L2Cache | None = None,
        redis_client=None,
    ) -> None:
        self.l1 = l1 or L1Cache()
        self.l2 = l2 or L2Cache(redis_client=redis_client)
        self._redis = redis_client
        self._pubsub_task: asyncio.Task | None = None

    def _client(self):
        if self._redis is not None:
            return self._redis
        from nexus.shared.config import settings
        import redis.asyncio as aioredis
        self._redis = aioredis.from_url(settings.redis_url, decode_responses=True)
        return self._redis

    # ------------------------------------------------------------------ #
    async def get(
        self,
        key: str,
        loader: Callable[[], Awaitable[Any]],
        l1_ttl: float = 30.0,
        l2_ttl: int = 300,
    ) -> Any:
        """
        1. Check L1 → return if hit.
        2. Check L2 → populate L1, return if hit.
        3. Call loader() → populate L2 + L1, return result.
        """
        # L1 check
        value = await self.l1.get(key)
        if value is not None:
            log.debug("cache.hit", level="L1", key=key)
            return value

        # L2 check
        value = await self.l2.get(key)
        if value is not None:
            log.debug("cache.hit", level="L2", key=key)
            await self.l1.set(key, value, ttl=l1_ttl)
            return value

        # L3 – call loader
        log.debug("cache.miss", level="L3", key=key)
        value = await loader()
        if value is not None:
            await self.l2.set(key, value, ttl=l2_ttl)
            await self.l1.set(key, value, ttl=l1_ttl)
        return value

    async def invalidate(self, key: str) -> None:
        """Remove *key* from both L1 and L2."""
        await self.l1.delete(key)
        await self.l2.delete(key)
        log.debug("cache.invalidated", key=key)

    async def invalidate_tags(self, tags: list[str]) -> None:
        """
        Publish invalidation events for each tag.
        Subscribers (including this instance) delete matching keys from their L1
        cache; L2 is invalidated by pattern scan.
        """
        client = self._client()
        for tag in tags:
            payload = json.dumps({"tag": tag})
            await client.publish(_PUBSUB_CHANNEL, payload)
            # Also eagerly sweep L2 with a pattern based on the tag
            pattern = f"*{tag}*"
            deleted = await self.l2.invalidate_pattern(pattern)
            log.info("cache.invalidate_tag", tag=tag, l2_deleted=deleted)

    async def start_pubsub_listener(self) -> None:
        """
        Start background task that listens on ``nexus:cache:invalidation`` and
        evicts matching L1 entries when a tag-based invalidation is received.
        """
        if self._pubsub_task and not self._pubsub_task.done():
            return
        self._pubsub_task = asyncio.create_task(self._pubsub_loop())
        log.info("cache.pubsub_listener.started", channel=_PUBSUB_CHANNEL)

    async def _pubsub_loop(self) -> None:
        try:
            import redis.asyncio as aioredis
            from nexus.shared.config import settings
            client = aioredis.from_url(settings.redis_url, decode_responses=True)
            pubsub = client.pubsub()
            await pubsub.subscribe(_PUBSUB_CHANNEL)
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                try:
                    event = json.loads(message["data"])
                    tag = event.get("tag", "")
                    if tag:
                        # Sweep L1 for keys that contain the tag
                        async with self.l1._lock:
                            to_delete = [k for k in self.l1._store if tag in k]
                        for k in to_delete:
                            await self.l1.delete(k)
                        if to_delete:
                            log.debug("cache.pubsub.l1_evicted", tag=tag, count=len(to_delete))
                except Exception:
                    log.warning("cache.pubsub.parse_error", exc_info=True)
        except asyncio.CancelledError:
            log.info("cache.pubsub_listener.stopped")
        except Exception:
            log.error("cache.pubsub_listener.error", exc_info=True)

    async def warm(self, keys: dict[str, Callable]) -> None:
        """
        Pre-populate the cache for *keys* map ``{cache_key: loader_callable}``.
        Intended to be called during application startup.
        """
        log.info("cache.warm.start", count=len(keys))
        results = await asyncio.gather(
            *[
                self.get(key, loader=loader)
                for key, loader in keys.items()
            ],
            return_exceptions=True,
        )
        errors = sum(1 for r in results if isinstance(r, Exception))
        log.info("cache.warm.done", total=len(keys), errors=errors)

    async def stats(self) -> dict:
        l1_stats = await self.l1.stats()
        l2_stats = await self.l2.stats()
        return {
            "l1": l1_stats,
            "l2": l2_stats,
        }


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

cache = CacheAside()
