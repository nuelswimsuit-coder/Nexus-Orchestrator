"""
Global swarm traffic pacing — one Telegram send at a time across processes (Redis).

Used by community_factory (worker) and Israeli community engine so bots do not
post in the same wall-clock second.

Environment (optional)
----------------------
SWARM_GLOBAL_POST_INTERVAL_S   Min seconds between any two Telegram sends (default 45).
SWARM_PACING_TZ                IANA timezone for quiet hours (default Asia/Jerusalem).
SWARM_QUIET_HOURS              Comma-separated local hours 0-23 in quiet mode (default 1,2,3,4,5,6).
SWARM_QUIET_INTERVAL_MULT      Multiplier on interval during quiet hours (default 3.0).
SWARM_BURST_INTERVAL_MULT      Multiplier when ``major_news`` (default 0.35).
SWARM_MAJOR_NEWS_SCORE         OpenClaw score ≥ this ⇒ major news burst (default 7.5).
SWARM_NEWS_JITTER_MIN_S        Lower bound for per-bot news-response delay (default 5).
SWARM_NEWS_JITTER_MAX_S        Upper bound (default 900 = 15 minutes).
SWARM_NEWS_JITTER_BURST_MAX_S  Upper bound during major news / non-quiet (default 120).
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
from datetime import datetime
from typing import Any

OPENCLAW_NEWS_SENTIMENT_KEY = "nexus:openclaw:news_sentiment"
PACING_LAST_SEND_KEY = "nexus:swarm:pacing:last_global_send"

_LUA_ACQUIRE = """
local last_raw = redis.call('GET', KEYS[1])
local now = tonumber(ARGV[1])
local gap = tonumber(ARGV[2])
local last = 0
if last_raw then last = tonumber(last_raw) or 0 end
if last <= 0 or (now - last) >= gap then
  redis.call('SET', KEYS[1], tostring(now))
  return 0
end
return last + gap - now
"""


def _float_env(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _int_list_env(name: str, default: list[int]) -> list[int]:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return list(default)
    out: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            h = int(part)
        except ValueError:
            continue
        if 0 <= h <= 23:
            out.append(h)
    return out or list(default)


def _timezone():
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo((os.getenv("SWARM_PACING_TZ") or "Asia/Jerusalem").strip())
    except Exception:
        return None


def local_hour() -> int:
    tz = _timezone()
    if tz is not None:
        return datetime.now(tz).hour
    return datetime.now().hour


def is_quiet_hours() -> bool:
    quiet = _int_list_env("SWARM_QUIET_HOURS", [1, 2, 3, 4, 5, 6])
    return local_hour() in quiet


def effective_global_interval_s(*, major_news: bool = False) -> float:
    base = _float_env("SWARM_GLOBAL_POST_INTERVAL_S", 45.0)
    base = max(5.0, min(600.0, base))
    if is_quiet_hours():
        base *= _float_env("SWARM_QUIET_INTERVAL_MULT", 3.0)
    if major_news:
        base *= _float_env("SWARM_BURST_INTERVAL_MULT", 0.35)
    return max(5.0, base)


async def openclaw_news_score(redis: Any) -> float | None:
    if redis is None:
        return None
    try:
        raw = await redis.get(OPENCLAW_NEWS_SENTIMENT_KEY)
        if not raw:
            return None
        data = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(data, dict):
            return None
        return float(data.get("score"))
    except Exception:
        return None


def is_major_news_score(score: float | None) -> bool:
    if score is None:
        return False
    threshold = _float_env("SWARM_MAJOR_NEWS_SCORE", 7.5)
    return score >= threshold


async def resolve_major_news(redis: Any | None) -> bool:
    s = await openclaw_news_score(redis)
    return is_major_news_score(s)


def news_response_jitter_s(*, major_news: bool, quiet: bool) -> float:
    lo = _float_env("SWARM_NEWS_JITTER_MIN_S", 5.0)
    hi = _float_env("SWARM_NEWS_JITTER_MAX_S", 900.0)
    burst_hi = _float_env("SWARM_NEWS_JITTER_BURST_MAX_S", 120.0)
    lo = max(1.0, lo)
    hi = max(lo, hi)
    burst_hi = max(lo, min(burst_hi, hi))
    if quiet:
        return random.uniform(max(lo, 60.0), hi)
    if major_news:
        return random.uniform(lo, burst_hi)
    return random.uniform(lo, min(600.0, hi))


async def wait_global_telegram_send_turn(
    redis: Any | None,
    *,
    major_news: bool = False,
) -> None:
    """
    Block until this process may perform the next Telegram send (global spacing).
    """
    if redis is None:
        await asyncio.sleep(random.uniform(0.4, 2.0))
        return

    gap = float(effective_global_interval_s(major_news=major_news))
    while True:
        now = time.time()
        try:
            wait = await redis.eval(_LUA_ACQUIRE, 1, PACING_LAST_SEND_KEY, str(now), str(gap))
        except Exception:
            await asyncio.sleep(1.0)
            continue
        try:
            w = float(wait)
        except (TypeError, ValueError):
            w = 0.0
        if w <= 0:
            return
        sleep_s = min(max(w, 0.05), 5.0) + random.uniform(0, 0.35)
        await asyncio.sleep(sleep_s)


FACTORY_NEWS_OPENER_KEY = "nexus:swarm:factory:news_opener_ticket:{gid}"


async def try_claim_factory_news_opener(redis: Any | None, group_id: int) -> bool:
    """
    Exactly one concurrent factory slot per group may act as the RSS/news opener
    until the ticket expires (avoids dozens of parallel ``news_opener`` turns).
    """
    if redis is None:
        return True
    key = FACTORY_NEWS_OPENER_KEY.format(gid=int(group_id))
    try:
        ok = await redis.set(key, "1", nx=True, ex=300)
        return bool(ok)
    except Exception:
        return True
