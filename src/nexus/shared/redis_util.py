"""
Redis connection utilities with Windows-safe retry logic.

Uses ::1 (IPv6 loopback) on Windows — the bundled redis-server binds [::] which
covers IPv6. Avoid 127.0.0.1 on Windows as port-proxy rules (WSL2/Hyper-V) can
hijack that address and cause WinError 64/10054 connection drops.
Retries up to 5 times with exponential back-off before raising.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from typing import Any

__all__ = [
    "get_redis_url",
    "create_redis_pool",
    "create_redis_pool_sync",
]

_DEFAULT_HOST = "[::1]" if sys.platform == "win32" else "127.0.0.1"
_DEFAULT_PORT = 6379
_DEFAULT_DB = 0
_MAX_RETRIES = 5
_RETRY_BASE_DELAY = 0.5  # seconds; doubles each attempt


def get_redis_url(host: str | None = None, port: int | None = None, db: int | None = None) -> str:
    """Return a Redis DSN using ::1 on Windows to bypass IPv4 port-proxy issues."""
    env_url = (os.getenv("REDIS_URL") or "").strip()
    if env_url:
        if sys.platform == "win32":
            for old in ("redis://localhost", "redis://127.0.0.1"):
                env_url = env_url.replace(old, "redis://[::1]")
        return env_url

    _host = (host or os.getenv("REDIS_HOST") or _DEFAULT_HOST).strip()
    if sys.platform == "win32" and _host.lower() in ("localhost", "127.0.0.1"):
        _host = "[::1]"

    _port = port or int(os.getenv("REDIS_PORT", str(_DEFAULT_PORT)))
    _db = db if db is not None else int(os.getenv("REDIS_DB", str(_DEFAULT_DB)))
    return f"redis://{_host}:{_port}/{_db}"


async def create_redis_pool(
    url: str | None = None,
    *,
    max_connections: int = 10,
    decode_responses: bool = True,
) -> Any:
    """
    Create an async Redis connection pool with retry logic.

    Retries up to _MAX_RETRIES times to handle transient 'Error 22' (WSAEINVAL)
    on Windows caused by IPv6 resolution of 'localhost'.
    """
    import redis.asyncio as aioredis  # type: ignore[import]

    dsn = url or get_redis_url()
    last_exc: Exception | None = None

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            pool = aioredis.ConnectionPool.from_url(
                dsn,
                max_connections=max_connections,
                decode_responses=decode_responses,
            )
            client = aioredis.Redis(connection_pool=pool)
            await client.ping()
            return client
        except Exception as exc:
            last_exc = exc
            delay = _RETRY_BASE_DELAY * (2 ** (attempt - 1))
            if attempt < _MAX_RETRIES:
                await asyncio.sleep(delay)

    raise ConnectionError(
        f"Redis connection failed after {_MAX_RETRIES} attempts to {dsn}: {last_exc}"
    ) from last_exc


def create_redis_pool_sync(
    url: str | None = None,
    *,
    max_connections: int = 10,
    decode_responses: bool = True,
) -> Any:
    """
    Create a synchronous Redis connection pool with retry logic.

    Same retry semantics as create_redis_pool but for sync contexts.
    """
    import redis  # type: ignore[import]

    dsn = url or get_redis_url()
    last_exc: Exception | None = None

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            client = redis.Redis.from_url(
                dsn,
                max_connections=max_connections,
                decode_responses=decode_responses,
            )
            client.ping()
            return client
        except Exception as exc:
            last_exc = exc
            delay = _RETRY_BASE_DELAY * (2 ** (attempt - 1))
            if attempt < _MAX_RETRIES:
                time.sleep(delay)

    raise ConnectionError(
        f"Redis connection failed after {_MAX_RETRIES} attempts to {dsn}: {last_exc}"
    ) from last_exc
