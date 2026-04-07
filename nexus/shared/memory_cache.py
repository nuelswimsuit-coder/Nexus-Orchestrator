"""
Process-local TTL caches (RAM). Complements Redis: cuts repeated GETs and JSON work
within a single API/worker process.

Thread-safe for use from async FastAPI handlers (sync lock around dict only; no await inside lock).
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from typing import Generic, TypeVar

T = TypeVar("T")


class TTLMemoryCache(Generic[T]):
    """LRU-bounded key→value cache with monotonic TTL expiry."""

    __slots__ = ("_max_entries", "_data", "_lock")

    def __init__(self, *, max_entries: int = 1024) -> None:
        self._max_entries = max(8, int(max_entries))
        self._data: OrderedDict[str, tuple[float, T]] = OrderedDict()
        self._lock = threading.Lock()

    def _purge_expired_unlocked(self, now: float) -> None:
        dead = [k for k, (exp, _) in self._data.items() if exp <= now]
        for k in dead:
            del self._data[k]

    def get(self, key: str) -> T | None:
        now = time.monotonic()
        with self._lock:
            self._purge_expired_unlocked(now)
            item = self._data.get(key)
            if item is None:
                return None
            exp, val = item
            if exp <= now:
                del self._data[key]
                return None
            self._data.move_to_end(key)
            return val

    def set(self, key: str, value: T, ttl_seconds: float) -> None:
        ttl = max(0.05, float(ttl_seconds))
        now = time.monotonic()
        with self._lock:
            self._purge_expired_unlocked(now)
            while len(self._data) >= self._max_entries and key not in self._data:
                self._data.popitem(last=False)
            self._data[key] = (now + ttl, value)
            self._data.move_to_end(key)

    def delete(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()
