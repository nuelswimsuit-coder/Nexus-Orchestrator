"""
nexus/shared/circuit_breaker.py — Circuit Breaker, Dead-Letter Queue, Exponential Backoff
==========================================================================================

Provides three resilience primitives for the Nexus distributed system:

CircuitBreaker
    Per-worker state machine (CLOSED → OPEN → HALF_OPEN) persisted atomically
    in Redis via Lua scripts.  Prevents cascading failures when a worker is
    unhealthy.

DeadLetterQueue
    Redis-backed append-only list for tasks that have exhausted all retries.
    Supports re-enqueueing via an external dispatcher.

ExponentialBackoff
    Stateless delay calculator with optional full-jitter for retry loops.

Module-level singletons
    ``circuit_breaker``, ``dlq``, and ``backoff`` — import and use directly.
"""

from __future__ import annotations

import asyncio
import json
import math
import random
import time
from enum import Enum
from typing import Any

import structlog

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_redis() -> Any:
    """
    Lazy-import the shared Redis client so this module can be imported before
    the event loop starts without triggering a connection attempt.
    """
    from nexus.shared.config import settings  # noqa: PLC0415 — lazy import
    import redis.asyncio as aioredis  # noqa: PLC0415
    # Re-use the process-wide pool if already created; otherwise build one.
    import nexus.shared._redis_pool as _pool  # noqa: PLC0415
    return _pool.get_client()


# ---------------------------------------------------------------------------
# CircuitState
# ---------------------------------------------------------------------------

class CircuitState(str, Enum):
    CLOSED    = "closed"     # Normal operation — all calls pass through.
    OPEN      = "open"       # Worker presumed dead — calls are short-circuited.
    HALF_OPEN = "half_open"  # Recovery probe — limited calls allowed.


# ---------------------------------------------------------------------------
# CircuitBreaker
# ---------------------------------------------------------------------------

# Lua script — atomic read + conditional write.
# KEYS[1] = nexus:cb:<worker_id>
# ARGV[1] = current unix timestamp (float as string)
# ARGV[2] = recovery_timeout (seconds, float as string)
# ARGV[3] = half_open_max_calls (int as string)
# Returns a flat list: [state, failures, opened_at, last_attempt_at]
_LUA_GET_STATE = """
local raw = redis.call('GET', KEYS[1])
if not raw then
    return {'closed', '0', '0', '0'}
end
local d = cjson.decode(raw)
local state = d['state'] or 'closed'
local failures = tonumber(d['failures'] or 0)
local opened_at = tonumber(d['opened_at'] or 0)
local last_attempt_at = tonumber(d['last_attempt_at'] or 0)
local now = tonumber(ARGV[1])
local recovery_timeout = tonumber(ARGV[2])

if state == 'open' and (now - opened_at) >= recovery_timeout then
    -- Transition to HALF_OPEN: update state in Redis.
    d['state'] = 'half_open'
    d['last_attempt_at'] = now
    redis.call('SET', KEYS[1], cjson.encode(d))
    return {'half_open', tostring(failures), tostring(opened_at), tostring(now)}
end
return {state, tostring(failures), tostring(opened_at), tostring(last_attempt_at)}
"""

# Lua script — record a success.
# KEYS[1] = nexus:cb:<worker_id>
_LUA_RECORD_SUCCESS = """
local raw = redis.call('GET', KEYS[1])
local d = {}
if raw then d = cjson.decode(raw) end
d['state'] = 'closed'
d['failures'] = 0
d['opened_at'] = 0
d['last_attempt_at'] = tonumber(ARGV[1])
redis.call('SET', KEYS[1], cjson.encode(d))
return 1
"""

# Lua script — record a failure.
# KEYS[1] = nexus:cb:<worker_id>
# ARGV[1] = now (float string)
# ARGV[2] = failure_threshold (int string)
_LUA_RECORD_FAILURE = """
local raw = redis.call('GET', KEYS[1])
local d = {}
if raw then d = cjson.decode(raw) end
local failures = tonumber(d['failures'] or 0) + 1
d['failures'] = failures
d['last_attempt_at'] = tonumber(ARGV[1])
local threshold = tonumber(ARGV[2])
if failures >= threshold then
    d['state'] = 'open'
    d['opened_at'] = tonumber(ARGV[1])
else
    d['state'] = d['state'] or 'closed'
end
redis.call('SET', KEYS[1], cjson.encode(d))
return failures
"""

# Lua script — check availability and bump half-open attempt counter.
# KEYS[1] = nexus:cb:<worker_id>
# ARGV[1] = now (float string)
# ARGV[2] = recovery_timeout (float string)
# ARGV[3] = half_open_max_calls (int string)
# Returns: '1' if available, '0' if not
_LUA_IS_AVAILABLE = """
local raw = redis.call('GET', KEYS[1])
if not raw then return '1' end
local d = cjson.decode(raw)
local state = d['state'] or 'closed'
local now = tonumber(ARGV[1])
local recovery_timeout = tonumber(ARGV[2])
local half_open_max = tonumber(ARGV[3])

if state == 'closed' then return '1' end

if state == 'open' then
    if (now - tonumber(d['opened_at'] or 0)) >= recovery_timeout then
        -- Transition to half_open
        d['state'] = 'half_open'
        d['half_open_calls'] = 0
        redis.call('SET', KEYS[1], cjson.encode(d))
        state = 'half_open'
    else
        return '0'
    end
end

if state == 'half_open' then
    local calls = tonumber(d['half_open_calls'] or 0)
    if calls < half_open_max then
        d['half_open_calls'] = calls + 1
        redis.call('SET', KEYS[1], cjson.encode(d))
        return '1'
    end
    return '0'
end

return '0'
"""


class CircuitBreaker:
    """
    Per-worker circuit breaker backed by Redis.

    All state lives under ``nexus:cb:<worker_id>`` as a JSON hash.
    Mutations are performed atomically with Lua scripts so concurrent
    workers never corrupt shared state.
    """

    def __init__(
        self,
        *,
        failure_threshold: int = 3,
        recovery_timeout: float = 60.0,
        half_open_max_calls: int = 1,
    ) -> None:
        self.failure_threshold   = failure_threshold
        self.recovery_timeout    = recovery_timeout
        self.half_open_max_calls = half_open_max_calls

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _key(worker_id: str) -> str:
        return f"nexus:cb:{worker_id}"

    async def _redis(self) -> Any:
        return _get_redis()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def record_success(self, worker_id: str) -> None:
        """Reset failure counter and close the circuit."""
        r = await self._redis()
        key = self._key(worker_id)
        now = str(time.time())
        await r.eval(_LUA_RECORD_SUCCESS, 1, key, now)
        log.debug("circuit_breaker.success", worker_id=worker_id)

    async def record_failure(self, worker_id: str) -> None:
        """Increment failure counter; open circuit when threshold is reached."""
        r = await self._redis()
        key = self._key(worker_id)
        now = str(time.time())
        failures = await r.eval(
            _LUA_RECORD_FAILURE, 1, key, now, str(self.failure_threshold)
        )
        failures = int(failures)
        if failures >= self.failure_threshold:
            log.warning(
                "circuit_breaker.opened",
                worker_id=worker_id,
                failures=failures,
            )
        else:
            log.debug(
                "circuit_breaker.failure_recorded",
                worker_id=worker_id,
                failures=failures,
            )

    async def is_available(self, worker_id: str) -> bool:
        """
        Return True if the circuit allows calls.

        CLOSED  → True.
        OPEN    → False (unless recovery_timeout elapsed → transition to HALF_OPEN).
        HALF_OPEN → True only while under half_open_max_calls budget.
        """
        r = await self._redis()
        key = self._key(worker_id)
        result = await r.eval(
            _LUA_IS_AVAILABLE,
            1,
            key,
            str(time.time()),
            str(self.recovery_timeout),
            str(self.half_open_max_calls),
        )
        available = result in (b"1", "1", 1)
        log.debug(
            "circuit_breaker.availability_check",
            worker_id=worker_id,
            available=available,
        )
        return available

    async def get_state(self, worker_id: str) -> CircuitState:
        """Return the current circuit state for a worker."""
        r = await self._redis()
        key = self._key(worker_id)
        result = await r.eval(
            _LUA_GET_STATE,
            1,
            key,
            str(time.time()),
            str(self.recovery_timeout),
            str(self.half_open_max_calls),
        )
        # result is a list: [state, failures, opened_at, last_attempt_at]
        raw_state = result[0]
        if isinstance(raw_state, bytes):
            raw_state = raw_state.decode()
        try:
            return CircuitState(raw_state)
        except ValueError:
            return CircuitState.CLOSED

    async def reset(self, worker_id: str) -> None:
        """Forcefully clear circuit state (useful for tests / manual recovery)."""
        r = await self._redis()
        await r.delete(self._key(worker_id))
        log.info("circuit_breaker.reset", worker_id=worker_id)


# ---------------------------------------------------------------------------
# DeadLetterQueue
# ---------------------------------------------------------------------------

class DeadLetterQueue:
    """
    Redis list-based Dead-Letter Queue.

    Items are RPUSH-ed as JSON blobs.  LRANGE is used for inspection.
    Re-enqueueing delegates to an external dispatcher callable.
    """

    REDIS_KEY = "nexus:dlq"

    def __init__(self) -> None:
        pass

    async def _redis(self) -> Any:
        return _get_redis()

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    async def push(
        self,
        task_payload: dict[str, Any],
        reason: str,
        worker_id: str,
    ) -> None:
        """Append a failed task to the DLQ with metadata."""
        r = await self._redis()
        entry = {
            "task_payload": task_payload,
            "reason": reason,
            "worker_id": worker_id,
            "failed_at": time.time(),
        }
        await r.rpush(self.REDIS_KEY, json.dumps(entry))
        log.warning(
            "dlq.pushed",
            task_id=task_payload.get("task_id"),
            worker_id=worker_id,
            reason=reason,
        )

    async def drain(self, limit: int = 100) -> list[dict[str, Any]]:
        """Return up to *limit* DLQ entries (oldest first) without removing them."""
        r = await self._redis()
        raw_items = await r.lrange(self.REDIS_KEY, 0, limit - 1)
        result: list[dict[str, Any]] = []
        for raw in raw_items:
            if isinstance(raw, bytes):
                raw = raw.decode()
            try:
                result.append(json.loads(raw))
            except json.JSONDecodeError:
                log.error("dlq.invalid_json", raw=raw[:200])
        return result

    async def retry_all(self, dispatcher: Any) -> int:
        """
        Re-enqueue every item currently in the DLQ via *dispatcher*.

        *dispatcher* must be an async callable that accepts a ``dict``
        (the original task_payload).

        Returns the number of tasks successfully re-enqueued.  Items are
        consumed from the DLQ left-to-right; on error the item remains.
        """
        r = await self._redis()
        count = 0
        while True:
            raw = await r.lpop(self.REDIS_KEY)
            if raw is None:
                break
            if isinstance(raw, bytes):
                raw = raw.decode()
            try:
                entry = json.loads(raw)
                await dispatcher(entry["task_payload"])
                count += 1
                log.info(
                    "dlq.retried",
                    task_id=entry.get("task_payload", {}).get("task_id"),
                )
            except Exception as exc:  # noqa: BLE001
                # Failed to re-enqueue — push back to avoid data loss.
                await r.rpush(self.REDIS_KEY, raw)
                log.error("dlq.retry_failed", exc=str(exc))
                break
        return count

    async def size(self) -> int:
        """Return the current number of items in the DLQ."""
        r = await self._redis()
        return int(await r.llen(self.REDIS_KEY))

    async def clear(self) -> None:
        """Remove all items from the DLQ (use with care)."""
        r = await self._redis()
        await r.delete(self.REDIS_KEY)
        log.warning("dlq.cleared")


# ---------------------------------------------------------------------------
# ExponentialBackoff
# ---------------------------------------------------------------------------

class ExponentialBackoff:
    """
    Stateless exponential-backoff calculator.

    Uses full-jitter by default (AWS best-practice) to spread retries across
    the fleet and avoid thundering-herd on Redis/API endpoints.

    Formula (with jitter):
        delay = random.uniform(0, min(max_delay, base_delay * multiplier ** attempt))

    Formula (without jitter):
        delay = min(max_delay, base_delay * multiplier ** attempt)
    """

    def __init__(
        self,
        *,
        base_delay: float = 1.0,
        max_delay: float = 300.0,
        multiplier: float = 2.0,
        jitter: bool = True,
    ) -> None:
        self.base_delay  = base_delay
        self.max_delay   = max_delay
        self.multiplier  = multiplier
        self.jitter      = jitter

    def get_delay(self, attempt: int) -> float:
        """
        Compute the sleep duration for the given *attempt* (0-indexed).

        *attempt* = 0 → shortest delay; higher values → longer, capped at
        *max_delay*.
        """
        attempt = max(0, attempt)
        cap = min(self.max_delay, self.base_delay * (self.multiplier ** attempt))
        if self.jitter:
            return random.uniform(0.0, cap)
        return cap

    async def wait(self, attempt: int) -> None:
        """Async sleep for the duration returned by :meth:`get_delay`."""
        delay = self.get_delay(attempt)
        log.debug("backoff.sleeping", attempt=attempt, delay_s=round(delay, 3))
        await asyncio.sleep(delay)


# ---------------------------------------------------------------------------
# Lazy Redis pool shim
# ---------------------------------------------------------------------------
# nexus/shared/_redis_pool.py is expected to expose get_client().
# If it does not exist (e.g. during unit tests with fakeredis) we fall back
# to building a client from settings directly.

def _build_fallback_client() -> Any:
    """Build a bare redis.asyncio.Redis from settings if pool module absent."""
    from nexus.shared.config import settings  # noqa: PLC0415
    import redis.asyncio as aioredis  # noqa: PLC0415
    return aioredis.from_url(
        settings.redis_url,
        encoding="utf-8",
        decode_responses=False,
        socket_keepalive=True,
        health_check_interval=30,
    )


# Inject a minimal _redis_pool module if not already present so that imports
# inside _get_redis() succeed on first call.
import importlib  # noqa: E402
import sys         # noqa: E402
import types       # noqa: E402

if "nexus.shared._redis_pool" not in sys.modules:
    _mod = types.ModuleType("nexus.shared._redis_pool")
    _mod._client = None  # type: ignore[attr-defined]

    def _get_client_impl() -> Any:
        if _mod._client is None:
            _mod._client = _build_fallback_client()
        return _mod._client

    _mod.get_client = _get_client_impl  # type: ignore[attr-defined]
    sys.modules["nexus.shared._redis_pool"] = _mod


# ---------------------------------------------------------------------------
# Module-level singletons
# ---------------------------------------------------------------------------

circuit_breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=60.0,
    half_open_max_calls=1,
)

dlq = DeadLetterQueue()

backoff = ExponentialBackoff(
    base_delay=1.0,
    max_delay=300.0,
    multiplier=2.0,
    jitter=True,
)

__all__ = [
    "CircuitState",
    "CircuitBreaker",
    "DeadLetterQueue",
    "ExponentialBackoff",
    "circuit_breaker",
    "dlq",
    "backoff",
]
