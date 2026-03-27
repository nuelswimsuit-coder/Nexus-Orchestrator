"""
Redis connection utilities with Windows-safe retry logic.

Uses ::1 (IPv6 loopback) on Windows — the bundled redis-server binds [::] which
covers IPv6. Avoid 127.0.0.1 on Windows as port-proxy rules (WSL2/Hyper-V) can
hijack that address and cause WinError 64/10054 connection drops.
Retries up to 5 times with exponential back-off before raising.

Remote-worker resilience parameters (socket_keepalive, health_check_interval,
socket_timeout, retry_on_timeout, and Retry on broken-pipe) are applied to all
pools so that workers on the Windows laptop survive WinError 64 / broken pipes.
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

    Resilience parameters (keepalive, health-check, timeout, retry-on-timeout,
    and Retry on broken-pipe / WinError 64) are applied unconditionally so that
    remote Windows workers survive long-lived connection drops.
    """
    import redis.asyncio as aioredis  # type: ignore[import]
    from redis.asyncio.retry import Retry as AioRetry  # type: ignore[import]
    from redis.exceptions import BusyLoadingError, ConnectionError as RedisConnError, TimeoutError as RedisTimeoutError  # type: ignore[import]

    dsn = url or get_redis_url()
    last_exc: Exception | None = None

    _retry_policy = AioRetry(
        backoff=None,  # uses default exponential back-off
        retries=3,
        supported_errors=(RedisConnError, RedisTimeoutError, BusyLoadingError, ConnectionResetError, BrokenPipeError),
    )

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            pool = aioredis.ConnectionPool.from_url(
                dsn,
                max_connections=max_connections,
                decode_responses=decode_responses,
                socket_keepalive=True,
                health_check_interval=30,
                socket_timeout=20,
                retry_on_timeout=True,
                retry=_retry_policy,
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
    Resilience parameters mirror the async pool.
    """
    import redis  # type: ignore[import]
    from redis.backoff import ExponentialBackoff  # type: ignore[import]
    from redis.exceptions import BusyLoadingError, ConnectionError as RedisConnError, TimeoutError as RedisTimeoutError  # type: ignore[import]
    from redis.retry import Retry as SyncRetry  # type: ignore[import]

    dsn = url or get_redis_url()
    last_exc: Exception | None = None

    _retry_policy = SyncRetry(
        backoff=ExponentialBackoff(),
        retries=3,
        supported_errors=(RedisConnError, RedisTimeoutError, BusyLoadingError, ConnectionResetError, BrokenPipeError),
    )

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            client = redis.Redis.from_url(
                dsn,
                max_connections=max_connections,
                decode_responses=decode_responses,
                socket_keepalive=True,
                health_check_interval=30,
                socket_timeout=20,
                retry_on_timeout=True,
                retry=_retry_policy,
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
