"""
Swarm signal ingestion — shared by OpenClaw (worker) and Strategy Brain (master).

Keeps keyword/consensus state in Redis without importing master from worker code.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import structlog

log = structlog.get_logger(__name__)

SWARM_SIGNAL_KEY = "nexus:swarm:signals"
SWARM_KEYWORD_HASH = "nexus:swarm:keyword_counts"


async def ingest_text_for_swarm(
    redis_client: Any,
    text: str,
    agent_fingerprint: str,
) -> dict[str, int]:
    """
    When news/sentiment text arrives, increment per-agent keyword hits and
    append a short line to the rolling swarm list for whale/flash detection.
    """
    low = (text or "").lower()
    keywords = ("whale", "flash", "breaking", "surge", "liquidat", "sec", "etf")
    matched: dict[str, int] = {}
    for kw in keywords:
        if kw in low:
            matched[kw] = 1
    if not matched:
        return {}

    try:
        pipe = redis_client.pipeline(transaction=True)
        for kw in matched:
            pipe.hincrby(SWARM_KEYWORD_HASH, f"{agent_fingerprint}:{kw}", 1)
        pipe.expire(SWARM_KEYWORD_HASH, 86400)
        line = (
            f"agent={agent_fingerprint[:16]} "
            f"hits={','.join(sorted(matched.keys()))} "
            f"ts={datetime.now(timezone.utc).isoformat()}"
        )
        pipe.lpush(SWARM_SIGNAL_KEY, line)
        pipe.ltrim(SWARM_SIGNAL_KEY, 0, 499)
        await pipe.execute()
    except Exception as exc:
        log.debug("ingest_swarm_failed", error=str(exc))
    return matched


def ingest_text_for_swarm_sync(
    redis_client: Any,
    text: str,
    agent_fingerprint: str,
) -> dict[str, int]:
    """
    Same semantics as ``ingest_text_for_swarm`` for a synchronous redis-py client
    (e.g. OpenClaw file bridge). Do not pass an async Redis client here.
    """
    low = (text or "").lower()
    keywords = ("whale", "flash", "breaking", "surge", "liquidat", "sec", "etf")
    matched: dict[str, int] = {}
    for kw in keywords:
        if kw in low:
            matched[kw] = 1
    if not matched:
        return {}

    try:
        pipe = redis_client.pipeline(transaction=True)
        for kw in matched:
            pipe.hincrby(SWARM_KEYWORD_HASH, f"{agent_fingerprint}:{kw}", 1)
        pipe.expire(SWARM_KEYWORD_HASH, 86400)
        line = (
            f"agent={agent_fingerprint[:16]} "
            f"hits={','.join(sorted(matched.keys()))} "
            f"ts={datetime.now(timezone.utc).isoformat()}"
        )
        pipe.lpush(SWARM_SIGNAL_KEY, line)
        pipe.ltrim(SWARM_SIGNAL_KEY, 0, 499)
        pipe.execute()
    except Exception as exc:
        log.debug("ingest_swarm_sync_failed", error=str(exc))
    return matched
