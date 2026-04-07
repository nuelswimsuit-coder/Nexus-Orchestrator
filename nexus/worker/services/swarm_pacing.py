"""
Global swarm traffic pacing — one outbound factory action at a time (Redis-coordinated),
burst vs quiet hours, and per-news-responder jitter.

Environment (optional)
----------------------
SWARM_GLOBAL_PACING_ENABLED   — default ``1`` (set ``0`` to disable).
SWARM_GLOBAL_POST_MIN_SECONDS — default ``75``; baseline gap between posts (non-news).
SWARM_BURST_POST_MIN_SECONDS  — default ``18``; gap during active hours / news replies.
SWARM_QUIET_POST_MIN_SECONDS  — default ``210``; gap during quiet hours.
SWARM_PACING_TZ               — default ``Asia/Jerusalem`` for quiet-hour detection.
SWARM_QUIET_HOURS             — comma-separated local hours, default ``23,0,1,2,3,4,5,6``.
SWARM_NEWS_JITTER_MIN_SEC     — default ``5``; floor for news-reply presleep.
SWARM_NEWS_JITTER_MAX_SEC     — default ``900`` (15 minutes); ceiling.
SWARM_NEWS_JITTER_QUIET_MIN_SEC — default ``120``; raises jitter floor in quiet hours.
SWARM_NEWS_JITTER_BURST_MAX_SEC — default ``900``; upper cap when not quiet (lower e.g. ``180`` for faster daytime replies).
SWARM_PACING_MAX_SPIN         — max Redis wait iterations (default ``5000``).
"""

from __future__ import annotations

import asyncio
import os
import random
import time
from datetime import datetime
from typing import Any

import structlog

log = structlog.get_logger(__name__)

KEY_GLOBAL_LAST_POST = "nexus:swarm:factory:global_last_post_ts"

_LUA_WAIT_TURN = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local gap = tonumber(ARGV[2])
local v = redis.call('GET', key)
if v == false or v == nil or v == '' then
  redis.call('SET', key, ARGV[1], 'EX', 172800)
  return '0'
end
local last = tonumber(v)
if last == nil then
  redis.call('SET', key, ARGV[1], 'EX', 172800)
  return '0'
end
local elapsed = now - last
if elapsed >= gap then
  redis.call('SET', key, ARGV[1], 'EX', 172800)
  return '0'
end
return tostring(gap - elapsed)
"""


def _env_float(name: str, default: str) -> float:
    try:
        return float((os.getenv(name) or default).strip())
    except ValueError:
        return float(default)


def _env_int(name: str, default: str) -> int:
    try:
        return int((os.getenv(name) or default).strip())
    except ValueError:
        return int(default)


def pacing_enabled() -> bool:
    v = (os.getenv("SWARM_GLOBAL_PACING_ENABLED", "1") or "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def _local_hour(tz_name: str) -> int:
    try:
        from zoneinfo import ZoneInfo

        z = ZoneInfo(tz_name)
        return datetime.now(z).hour
    except Exception:
        return datetime.now().hour


def is_quiet_hours() -> bool:
    tz = (os.getenv("SWARM_PACING_TZ") or "Asia/Jerusalem").strip() or "Asia/Jerusalem"
    h = _local_hour(tz)
    raw = (os.getenv("SWARM_QUIET_HOURS") or "23,0,1,2,3,4,5,6").strip()
    try:
        quiet_set = {int(x.strip()) for x in raw.split(",") if x.strip()}
    except ValueError:
        quiet_set = {23, 0, 1, 2, 3, 4, 5, 6}
    return h in quiet_set


def global_min_interval_sec(*, news_response_context: bool = False) -> float:
    """Minimum wall-clock seconds between global factory posts (enforced in Redis)."""
    base = _env_float("SWARM_GLOBAL_POST_MIN_SECONDS", "75")
    burst_s = _env_float("SWARM_BURST_POST_MIN_SECONDS", "18")
    quiet_s = _env_float("SWARM_QUIET_POST_MIN_SECONDS", "210")
    quiet = is_quiet_hours()
    if news_response_context:
        return max(3.0, quiet_s if quiet else burst_s)
    if quiet:
        return max(5.0, max(base, quiet_s))
    return max(5.0, base)


def news_responder_jitter_seconds() -> float:
    """
    Presleep before each news-linked reply. Overall band is configurable; quiet hours
    skew toward longer waits, active hours cap the upper end for a “burst” feel.
    """
    quiet = is_quiet_hours()
    lo = _env_float("SWARM_NEWS_JITTER_MIN_SEC", "5")
    hi = _env_float("SWARM_NEWS_JITTER_MAX_SEC", "900")
    if quiet:
        lo = max(lo, _env_float("SWARM_NEWS_JITTER_QUIET_MIN_SEC", "120"))
    else:
        burst_hi = _env_float("SWARM_NEWS_JITTER_BURST_MAX_SEC", "900")
        hi = min(hi, max(lo, burst_hi))
    if hi < lo:
        lo, hi = hi, lo
    return random.uniform(lo, hi)


async def acquire_global_post_turn(redis: Any, min_interval_sec: float) -> None:
    """
    Block until this worker may perform one globally spaced outbound action, then
    reserve the timestamp in Redis (same clock as other workers via ``time.time()``).
    """
    if not pacing_enabled() or redis is None or min_interval_sec <= 0:
        return
    cap = max(0.05, float(min_interval_sec))
    max_iter = max(1, _env_int("SWARM_PACING_MAX_SPIN", "5000"))
    for _ in range(max_iter):
        now = time.time()
        try:
            raw = await redis.eval(
                _LUA_WAIT_TURN,
                1,
                KEY_GLOBAL_LAST_POST,
                str(now),
                str(cap),
            )
        except Exception as exc:
            log.debug("swarm_pacing_lua_failed", error=str(exc))
            await asyncio.sleep(0.5)
            continue
        if raw is None:
            await asyncio.sleep(0.2)
            continue
        s = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        try:
            wait = float(s)
        except ValueError:
            wait = 0.0
        if wait <= 0.001:
            return
        await asyncio.sleep(min(wait, 30.0))
    log.warning("swarm_pacing_spin_exhausted", min_interval_sec=cap)
