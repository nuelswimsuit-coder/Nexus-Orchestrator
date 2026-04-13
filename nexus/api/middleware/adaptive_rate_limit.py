"""
Adaptive Token-Bucket Rate Limiting — per-session / per-IP.

Architecture
------------
* Redis Hash stores ``{tokens, last_refill, capacity}`` per bucket key.
* A single Lua script performs the atomic refill-and-consume, eliminating
  race conditions without needing WATCH/MULTI/EXEC round-trips.
* ``AdaptiveRateLimitMiddleware`` is a Starlette ``BaseHTTPMiddleware`` and can
  be added directly to any FastAPI app.
* A FastAPI ``APIRouter`` (``RateLimitRouter``) exposes
  ``GET /api/ratelimit/status`` for observability.

Route-specific limits (tokens/minute)
--------------------------------------
  /api/sessions/*  →  30
  /api/hitl/*      →  60
  /api/metrics     →  300
  default          →  100

VIP sessions (Redis SET ``nexus:ratelimit:vip``) receive 10× capacity.
Internal IPs (RFC-1918) and health-check paths are exempt.
"""

from __future__ import annotations

import ipaddress
import json
import re
import time
from dataclasses import dataclass, field
from typing import Callable

import structlog
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PRIVATE_RANGES = [
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("127.0.0.0/8"),
]

_HEALTH_PATHS = {"/health", "/healthz", "/ping", "/ready", "/live"}


def _is_private_ip(ip_str: str) -> bool:
    try:
        addr = ipaddress.ip_address(ip_str)
        return any(addr in net for net in _PRIVATE_RANGES)
    except ValueError:
        return False


def _extract_client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class RateLimitConfig:
    capacity: int = 100           # max tokens in the bucket
    refill_rate: float = 1.67    # tokens per second  ≈ 100/min
    burst_multiplier: float = 2.0  # allow 2× burst


# ---------------------------------------------------------------------------
# Lua script — atomic refill + consume
# ---------------------------------------------------------------------------

_CONSUME_SCRIPT = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(bucket[1]) or capacity
local last_refill = tonumber(bucket[2]) or now

local elapsed = now - last_refill
tokens = math.min(capacity, tokens + elapsed * refill_rate)

if tokens >= cost then
    tokens = tokens - cost
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('EXPIRE', key, 3600)
    return {1, math.floor(tokens)}
else
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('EXPIRE', key, 3600)
    return {0, math.floor(tokens)}
end
"""


# ---------------------------------------------------------------------------
# TokenBucket
# ---------------------------------------------------------------------------

class TokenBucket:
    """Atomic token-bucket implemented via a single Redis Lua script."""

    _KEY_PREFIX = "nexus:ratelimit:"

    def __init__(self, redis_client=None) -> None:
        self._redis = redis_client
        self._script_sha: str | None = None

    def _client(self):
        if self._redis is not None:
            return self._redis
        from nexus.shared.config import settings
        import redis.asyncio as aioredis
        self._redis = aioredis.from_url(settings.redis_url, decode_responses=True)
        return self._redis

    async def _get_sha(self) -> str:
        if self._script_sha is None:
            self._script_sha = await self._client().script_load(_CONSUME_SCRIPT)
        return self._script_sha

    def _redis_key(self, bucket_key: str) -> str:
        return f"{self._KEY_PREFIX}{bucket_key}"

    async def consume(
        self,
        bucket_key: str,
        config: RateLimitConfig,
        cost: int = 1,
    ) -> tuple[bool, int]:
        """
        Try to consume *cost* tokens from *bucket_key*.

        Returns
        -------
        (allowed, remaining_tokens)
        """
        sha = await self._get_sha()
        now = time.time()
        redis_key = self._redis_key(bucket_key)
        try:
            result = await self._client().evalsha(
                sha,
                1,
                redis_key,
                now,
                config.refill_rate,
                config.capacity,
                cost,
            )
            allowed = bool(int(result[0]))
            remaining = int(result[1])
            return allowed, remaining
        except Exception:
            log.error("rate_limit.consume_error", bucket_key=bucket_key, exc_info=True)
            # Fail open — don't block requests on Redis errors
            return True, config.capacity

    async def reset(self, bucket_key: str) -> None:
        """Delete the bucket, effectively resetting it to full capacity."""
        await self._client().delete(self._redis_key(bucket_key))

    async def get_remaining(self, bucket_key: str) -> int:
        """Return current token count without consuming any."""
        try:
            raw = await self._client().hget(self._redis_key(bucket_key), "tokens")
            return int(float(raw)) if raw else -1
        except Exception:
            return -1

    async def get_bucket_state(self, bucket_key: str) -> dict:
        """Return full bucket state for observability."""
        try:
            data = await self._client().hgetall(self._redis_key(bucket_key))
            return {k: v for k, v in data.items()}
        except Exception:
            return {}


# ---------------------------------------------------------------------------
# Route limit table
# ---------------------------------------------------------------------------

_ROUTE_LIMITS: list[tuple[str, int]] = [
    # (prefix, tokens_per_minute)
    ("/api/sessions/", 30),
    ("/api/hitl/", 60),
    ("/api/metrics", 300),
]
_DEFAULT_LIMIT = 100  # tokens/min


def _tokens_per_min_for_path(path: str) -> int:
    for prefix, limit in _ROUTE_LIMITS:
        if path.startswith(prefix):
            return limit
    return _DEFAULT_LIMIT


def _refill_rate_for_tpm(tpm: int) -> float:
    return tpm / 60.0


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

class AdaptiveRateLimitMiddleware(BaseHTTPMiddleware):
    """
    Per-session (falling back to per-IP) adaptive rate limiting.

    Adds the following response headers when a request is **allowed**:
        X-RateLimit-Limit      — bucket capacity
        X-RateLimit-Remaining  — tokens remaining after this request

    When **denied** (HTTP 429):
        Retry-After            — seconds until ~50% of bucket refills
        X-RateLimit-Remaining  — always 0
    """

    _VIP_SET_KEY = "nexus:ratelimit:vip"
    _VIP_MULTIPLIER = 10

    def __init__(self, app: ASGIApp, redis_client=None) -> None:
        super().__init__(app)
        self._bucket = TokenBucket(redis_client=redis_client)
        self._redis = redis_client

    def _redis_client(self):
        if self._redis is not None:
            return self._redis
        from nexus.shared.config import settings
        import redis.asyncio as aioredis
        self._redis = aioredis.from_url(settings.redis_url, decode_responses=True)
        return self._redis

    async def _is_vip(self, session_id: str) -> bool:
        try:
            return bool(await self._redis_client().sismember(self._VIP_SET_KEY, session_id))
        except Exception:
            return False

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        path = request.url.path

        # 1. Health-check exemption
        if path in _HEALTH_PATHS or path.rstrip("/") in _HEALTH_PATHS:
            return await call_next(request)

        # 2. Internal-IP exemption
        client_ip = _extract_client_ip(request)
        if _is_private_ip(client_ip):
            return await call_next(request)

        # 3. Determine bucket identity
        session_id = (
            request.headers.get("X-Session-ID")
            or request.cookies.get("session_id")
        )
        bucket_key = f"session:{session_id}" if session_id else f"ip:{client_ip}"

        # 4. Build config for this route
        tpm = _tokens_per_min_for_path(path)
        config = RateLimitConfig(
            capacity=tpm,
            refill_rate=_refill_rate_for_tpm(tpm),
            burst_multiplier=2.0,
        )

        # 5. VIP sessions get 10× capacity
        if session_id and await self._is_vip(session_id):
            config.capacity *= self._VIP_MULTIPLIER
            config.refill_rate *= self._VIP_MULTIPLIER

        # 6. Consume one token
        allowed, remaining = await self._bucket.consume(bucket_key, config, cost=1)

        if allowed:
            response = await call_next(request)
            response.headers["X-RateLimit-Limit"] = str(config.capacity)
            response.headers["X-RateLimit-Remaining"] = str(remaining)
            return response

        # 7. Rate limited — 429
        retry_after = max(1, int((config.capacity * 0.5) / config.refill_rate))
        log.info(
            "rate_limit.rejected",
            bucket_key=bucket_key,
            path=path,
            remaining=remaining,
        )
        return JSONResponse(
            status_code=429,
            content={
                "error": "rate_limit_exceeded",
                "message": "Too many requests. Please slow down.",
                "retry_after_seconds": retry_after,
            },
            headers={
                "Retry-After": str(retry_after),
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Limit": str(config.capacity),
            },
        )


# ---------------------------------------------------------------------------
# Status router
# ---------------------------------------------------------------------------

RateLimitRouter = APIRouter(prefix="/api/ratelimit", tags=["rate-limit"])

_shared_bucket = TokenBucket()


@RateLimitRouter.get("/status")
async def ratelimit_status(request: Request, bucket_key: str | None = None):
    """
    Return the current token-bucket state.

    Pass ``?bucket_key=session:abc123`` or ``?bucket_key=ip:1.2.3.4`` to
    inspect a specific bucket.  Omit to inspect the caller's own bucket.
    """
    if bucket_key is None:
        client_ip = _extract_client_ip(request)
        session_id = (
            request.headers.get("X-Session-ID")
            or request.cookies.get("session_id")
        )
        bucket_key = f"session:{session_id}" if session_id else f"ip:{client_ip}"

    state = await _shared_bucket.get_bucket_state(bucket_key)
    remaining = await _shared_bucket.get_remaining(bucket_key)
    return {
        "bucket_key": bucket_key,
        "remaining_tokens": remaining,
        "raw_state": state,
    }
