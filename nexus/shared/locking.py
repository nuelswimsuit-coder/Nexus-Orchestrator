"""
Distributed locking primitives.

Two complementary strategies are provided:

OptimisticLock  (OCC / check-and-set)
    Read → mutate locally → write back atomically with a Lua version check.
    Best for low-contention scenarios (most reads succeed without conflict).

DistributedLock (pessimistic / SET NX EX)
    Exclusive lock backed by a Redis key.  Supports timeout, safe release
    (owner token prevents accidental release by another holder), TTL extension,
    and async context-manager syntax.
    Best for high-contention or non-idempotent critical sections.

@with_optimistic_lock(key_fn)  decorator
    Wraps a coroutine so that the OCC read/mutate/write loop is handled
    transparently.  The decorated function receives the current ``data`` dict
    and returns the mutated dict.

Usage
-----
    from nexus.shared.locking import OptimisticLock, DistributedLock, with_optimistic_lock

    lock = OptimisticLock()
    record = await lock.create("inventory:42", {"qty": 100})
    record = await lock.retry_write("inventory:42", lambda d: {**d, "qty": d["qty"] - 1})

    async with DistributedLock() as dl:
        acquired = await dl.acquire("job:critical", ttl=30)
        ...
        await dl.release("job:critical")
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable

import structlog
from pydantic import BaseModel

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class OptimisticLockError(Exception):
    """Raised when a write is attempted with a stale expected_version."""

    def __init__(self, key: str, expected: int, actual: int) -> None:
        self.key = key
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"Optimistic lock conflict on '{key}': "
            f"expected version {expected}, found {actual}."
        )


# ---------------------------------------------------------------------------
# Pydantic model
# ---------------------------------------------------------------------------

class VersionedRecord(BaseModel):
    key: str
    data: dict
    version: int
    updated_at: datetime
    updated_by: str | None = None

    model_config = {"arbitrary_types_allowed": True}


# ---------------------------------------------------------------------------
# Lua scripts
# ---------------------------------------------------------------------------

# Atomic check-and-set: only writes if current version == expected_version.
_CAS_SCRIPT = """
local raw = redis.call('GET', KEYS[1])
if not raw then
    return redis.error_reply('NOT_FOUND')
end
local record = cjson.decode(raw)
local current_version = tonumber(record['version'])
local expected_version = tonumber(ARGV[1])
if current_version ~= expected_version then
    return redis.error_reply('VERSION_CONFLICT:' .. current_version)
end
redis.call('SET', KEYS[1], ARGV[2])
return 'OK'
"""

# Safe delete: only deletes if current version == expected_version.
_DELETE_SCRIPT = """
local raw = redis.call('GET', KEYS[1])
if not raw then
    return 0
end
local record = cjson.decode(raw)
local current_version = tonumber(record['version'])
local expected_version = tonumber(ARGV[1])
if current_version ~= expected_version then
    return -1
end
redis.call('DEL', KEYS[1])
return 1
"""

# Safe lock release: only deletes the key if its value matches ARGV[1] (owner token).
_RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return 1
end
return 0
"""

# Safe lock extend: only updates TTL if the key is still owned by ARGV[1].
_EXTEND_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
    return 1
end
return 0
"""


# ---------------------------------------------------------------------------
# OptimisticLock
# ---------------------------------------------------------------------------

class OptimisticLock:
    """
    Optimistic concurrency control backed by Redis JSON records.

    Each record stored at *key* is a JSON object containing the ``VersionedRecord``
    fields.  Writes are gated by a Lua CAS script that rejects the write if the
    stored version differs from ``expected_version``.
    """

    def __init__(self, redis_client=None) -> None:
        self._redis = redis_client
        self._cas_sha: str | None = None
        self._del_sha: str | None = None

    def _client(self):
        if self._redis is not None:
            return self._redis
        from nexus.shared.config import settings
        import redis.asyncio as aioredis
        self._redis = aioredis.from_url(settings.redis_url, decode_responses=True)
        return self._redis

    async def _ensure_scripts(self) -> None:
        client = self._client()
        if self._cas_sha is None:
            self._cas_sha = await client.script_load(_CAS_SCRIPT)
        if self._del_sha is None:
            self._del_sha = await client.script_load(_DELETE_SCRIPT)

    def _serialise(self, record: VersionedRecord) -> str:
        return record.model_dump_json()

    def _deserialise(self, raw: str) -> VersionedRecord:
        return VersionedRecord.model_validate_json(raw)

    # ------------------------------------------------------------------ #
    async def read(self, key: str) -> VersionedRecord | None:
        raw = await self._client().get(key)
        if raw is None:
            return None
        try:
            return self._deserialise(raw)
        except Exception:
            log.warning("optimistic_lock.deserialise_error", key=key, exc_info=True)
            return None

    async def create(self, key: str, data: dict, created_by: str | None = None) -> VersionedRecord:
        """Create a new record at *key*. Fails if the key already exists."""
        record = VersionedRecord(
            key=key,
            data=data,
            version=1,
            updated_at=datetime.now(timezone.utc),
            updated_by=created_by,
        )
        serialised = self._serialise(record)
        created = await self._client().set(key, serialised, nx=True)
        if not created:
            raise OptimisticLockError(key, 0, -1)
        return record

    async def write(
        self,
        key: str,
        data: dict,
        expected_version: int,
        updated_by: str | None = None,
    ) -> VersionedRecord:
        """
        Atomically update *key* with *data* if the stored version equals
        *expected_version*.  Raises ``OptimisticLockError`` on conflict.
        """
        await self._ensure_scripts()
        new_record = VersionedRecord(
            key=key,
            data=data,
            version=expected_version + 1,
            updated_at=datetime.now(timezone.utc),
            updated_by=updated_by,
        )
        serialised = self._serialise(new_record)
        try:
            result = await self._client().evalsha(
                self._cas_sha,
                1,
                key,
                str(expected_version),
                serialised,
            )
        except Exception as exc:
            err_msg = str(exc)
            if "NOT_FOUND" in err_msg:
                raise KeyError(f"Key '{key}' does not exist.") from exc
            if "VERSION_CONFLICT" in err_msg:
                # Extract actual version from error message
                try:
                    actual = int(err_msg.split("VERSION_CONFLICT:")[-1].split()[0])
                except (ValueError, IndexError):
                    actual = -1
                raise OptimisticLockError(key, expected_version, actual) from exc
            raise
        return new_record

    async def delete(self, key: str, expected_version: int) -> bool:
        """
        Delete *key* only if its version matches *expected_version*.

        Returns ``True`` on success, ``False`` if key was absent.
        Raises ``OptimisticLockError`` if version conflict.
        """
        await self._ensure_scripts()
        try:
            result = await self._client().evalsha(
                self._del_sha,
                1,
                key,
                str(expected_version),
            )
            rc = int(result)
            if rc == 1:
                return True
            if rc == 0:
                return False
            if rc == -1:
                raise OptimisticLockError(key, expected_version, -1)
        except OptimisticLockError:
            raise
        except Exception:
            log.error("optimistic_lock.delete_error", key=key, exc_info=True)
            raise
        return False

    async def retry_write(
        self,
        key: str,
        updater: Callable[[dict], dict],
        max_retries: int = 3,
        updated_by: str | None = None,
    ) -> VersionedRecord:
        """
        Read → apply *updater* → write with current version.
        Retries up to *max_retries* times on ``OptimisticLockError``.
        """
        for attempt in range(max_retries):
            record = await self.read(key)
            if record is None:
                raise KeyError(f"Key '{key}' does not exist.")
            new_data = updater(dict(record.data))
            try:
                return await self.write(
                    key,
                    data=new_data,
                    expected_version=record.version,
                    updated_by=updated_by,
                )
            except OptimisticLockError:
                if attempt == max_retries - 1:
                    raise
                log.debug(
                    "optimistic_lock.retry",
                    key=key,
                    attempt=attempt + 1,
                    max_retries=max_retries,
                )
                await asyncio.sleep(0.01 * (2 ** attempt))  # brief exponential back-off
        # unreachable, but satisfies type checkers
        raise OptimisticLockError(key, -1, -1)


# ---------------------------------------------------------------------------
# DistributedLock (pessimistic, SET NX EX)
# ---------------------------------------------------------------------------

class DistributedLock:
    """
    Pessimistic distributed lock using Redis ``SET key token NX EX ttl``.

    The lock value is a UUID (owner token) — release is safe because the
    Lua script only deletes the key if its value matches the owner token
    acquired by this instance.

    Context manager usage::

        dl = DistributedLock()
        async with dl.lock("job:critical", ttl=30):
            ...  # exclusive section

    Or manual::

        dl = DistributedLock()
        acquired = await dl.acquire("job:critical", ttl=30, timeout=5.0)
        if acquired:
            try:
                ...
            finally:
                await dl.release("job:critical")
    """

    _KEY_PREFIX = "nexus:lock:"

    def __init__(self, redis_client=None) -> None:
        self._redis = redis_client
        self._tokens: dict[str, str] = {}   # name → owner token
        self._release_sha: str | None = None
        self._extend_sha: str | None = None

    def _client(self):
        if self._redis is not None:
            return self._redis
        from nexus.shared.config import settings
        import redis.asyncio as aioredis
        self._redis = aioredis.from_url(settings.redis_url, decode_responses=True)
        return self._redis

    async def _ensure_scripts(self) -> None:
        client = self._client()
        if self._release_sha is None:
            self._release_sha = await client.script_load(_RELEASE_SCRIPT)
        if self._extend_sha is None:
            self._extend_sha = await client.script_load(_EXTEND_SCRIPT)

    def _redis_key(self, name: str) -> str:
        return f"{self._KEY_PREFIX}{name}"

    # ------------------------------------------------------------------ #
    async def acquire(self, name: str, ttl: int = 30, timeout: float = 10.0) -> bool:
        """
        Try to acquire *name* with a deadline of *timeout* seconds.

        Returns ``True`` if the lock was obtained, ``False`` on timeout.
        """
        token = str(uuid.uuid4())
        rkey = self._redis_key(name)
        deadline = asyncio.get_event_loop().time() + timeout
        delay = 0.05

        while True:
            acquired = await self._client().set(rkey, token, nx=True, ex=ttl)
            if acquired:
                self._tokens[name] = token
                log.debug("distributed_lock.acquired", name=name, ttl=ttl)
                return True
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                log.warning("distributed_lock.timeout", name=name, timeout=timeout)
                return False
            await asyncio.sleep(min(delay, remaining))
            delay = min(delay * 1.5, 1.0)  # cap at 1 s

    async def release(self, name: str) -> bool:
        """
        Release the lock *name*.  Only releases if this instance holds the token.
        Returns ``True`` if released, ``False`` if the lock was not ours or absent.
        """
        token = self._tokens.pop(name, None)
        if token is None:
            return False
        await self._ensure_scripts()
        result = await self._client().evalsha(
            self._release_sha,
            1,
            self._redis_key(name),
            token,
        )
        released = int(result) == 1
        if not released:
            log.warning("distributed_lock.release_failed", name=name)
        return released

    async def extend(self, name: str, ttl: int = 30) -> bool:
        """Extend the TTL of a lock we hold.  Returns ``True`` on success."""
        token = self._tokens.get(name)
        if token is None:
            return False
        await self._ensure_scripts()
        result = await self._client().evalsha(
            self._extend_sha,
            1,
            self._redis_key(name),
            token,
            str(ttl),
        )
        return int(result) == 1

    # ------------------------------------------------------------------ #
    # Context manager — usage: async with dl.lock("name", ttl=30): ...
    # ------------------------------------------------------------------ #

    def lock(self, name: str, ttl: int = 30, timeout: float = 10.0) -> "_LockContext":
        return _LockContext(self, name, ttl, timeout)

    async def __aenter__(self) -> "DistributedLock":
        return self

    async def __aexit__(self, *_: Any) -> None:
        pass


class _LockContext:
    """Async context manager returned by ``DistributedLock.lock()``."""

    def __init__(self, dl: DistributedLock, name: str, ttl: int, timeout: float) -> None:
        self._dl = dl
        self._name = name
        self._ttl = ttl
        self._timeout = timeout

    async def __aenter__(self) -> "_LockContext":
        acquired = await self._dl.acquire(self._name, ttl=self._ttl, timeout=self._timeout)
        if not acquired:
            raise TimeoutError(f"Could not acquire distributed lock '{self._name}' within {self._timeout}s.")
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self._dl.release(self._name)


# ---------------------------------------------------------------------------
# @with_optimistic_lock decorator
# ---------------------------------------------------------------------------

def with_optimistic_lock(key_fn: Callable[..., str], max_retries: int = 3):
    """
    Decorator that wraps an async function with optimistic concurrency control.

    The decorated function must accept ``data: dict`` as its first argument (after
    *self* / *cls* if applicable) and return the mutated ``dict``.

    ``key_fn`` receives the same positional / keyword arguments as the decorated
    function and must return the Redis key to lock.

    Example::

        olock = OptimisticLock()

        @with_optimistic_lock(key_fn=lambda self, user_id, **_: f"user:{user_id}")
        async def update_balance(self, user_id: str, amount: float) -> dict:
            record = await olock.read(f"user:{user_id}")
            data = dict(record.data)
            data["balance"] += amount
            return data
    """
    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            _lock = OptimisticLock()
            key = key_fn(*args, **kwargs)
            return await _lock.retry_write(
                key,
                updater=lambda data: fn.__wrapped__(data, *args, **kwargs)
                    if hasattr(fn, "__wrapped__") else data,
                max_retries=max_retries,
            )
        wrapper.__wrapped__ = fn  # type: ignore[attr-defined]
        return wrapper
    return decorator
