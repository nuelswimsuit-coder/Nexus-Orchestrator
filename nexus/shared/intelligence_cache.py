"""
Redis-backed “Master Thought” cache for swarm news: one paid LLM analysis per
``news_id``, many bots reuse it and rephrase locally (Ollama).

Keys
----
- ``nexus:cache:thought:<news_id>`` — JSON “Core Analysis” (1h TTL)
- ``nexus:cache:thought:lock:<news_id>`` — short lock so only one process calls Gemini/GPT-4
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from typing import Any

import structlog

log = structlog.get_logger(__name__)

THOUGHT_KEY_PREFIX = "nexus:cache:thought:"
THOUGHT_LOCK_PREFIX = "nexus:cache:thought:lock:"
DEFAULT_CORE_TTL_SEC = 3600
DEFAULT_LOCK_TTL_SEC = 90
POLL_INTERVAL_SEC = 0.25
MASTER_WAIT_SEC = float(os.getenv("NEXUS_MASTER_THOUGHT_WAIT_SEC") or "90")


def thought_cache_key(news_id: str) -> str:
    safe = (news_id or "").strip()
    if not safe:
        raise ValueError("news_id required")
    return f"{THOUGHT_KEY_PREFIX}{safe}"


def thought_lock_key(news_id: str) -> str:
    safe = (news_id or "").strip()
    if not safe:
        raise ValueError("news_id required")
    return f"{THOUGHT_LOCK_PREFIX}{safe}"


def compute_news_id(anchor_title: str, anchor_link: str, digest_text: str) -> str:
    """
    Stable id for the same OpenClaw / digest item across all bots.
    """
    parts = [
        (anchor_title or "").strip()[:800],
        (anchor_link or "").strip()[:2000],
        (digest_text or "").strip()[:4000],
    ]
    raw = "\n".join(parts).encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()[:32]


def is_valid_core_analysis(obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False
    mp = obj.get("main_points")
    if not isinstance(mp, list) or not mp:
        return False
    if not all(isinstance(x, str) and x.strip() for x in mp[:20]):
        return False
    tone = obj.get("tone")
    sent = obj.get("sentiment")
    return isinstance(tone, str) and tone.strip() and isinstance(sent, str) and sent.strip()


def augment_user_prompt_with_core_analysis(base_user_he: str, core: dict[str, Any]) -> str:
    """Prefix factory news-opener user text with shared core analysis for local rephrasing."""
    block = json.dumps(core, ensure_ascii=False)
    return (
        "ניתוח ליבה (מרכזי — לא להעתיק מילה במילה; גוון וניסוח לפי האישיות שלך בלבד):\n"
        f"{block}\n\n---\n\n{(base_user_he or '').strip()}"
    )


async def _master_gemini_core(
    api_key: str,
    *,
    anchor_title: str,
    anchor_link: str,
    digest_snippet: str,
) -> dict[str, Any] | None:
    from nexus.modules.community_vibe import _gemini_json  # noqa: PLC0415

    sys_prompt = (
        "You are a concise news analyst. Given Hebrew/English headlines and context, "
        "extract structured analysis. Reply with JSON only, no markdown:\n"
        '{"main_points":["3-6 short factual bullets"],"tone":"one short phrase",'
        '"sentiment":"negative|neutral|positive|mixed"}'
    )
    user_payload = json.dumps(
        {
            "anchor_title": (anchor_title or "").strip()[:500],
            "anchor_link": (anchor_link or "").strip()[:2000],
            "digest_excerpt": (digest_snippet or "").strip()[:3500],
        },
        ensure_ascii=False,
    )
    try:
        out = await _gemini_json(
            api_key,
            sys_prompt,
            user_payload,
            temperature=0.35,
            max_tokens=384,
        )
    except Exception as exc:
        log.warning("master_thought_gemini_failed", error=str(exc))
        return None
    if isinstance(out, dict) and is_valid_core_analysis(out):
        return {
            "main_points": [str(x).strip() for x in out["main_points"] if str(x).strip()][:8],
            "tone": str(out["tone"]).strip()[:200],
            "sentiment": str(out["sentiment"]).strip()[:80],
        }
    return None


async def get_cached_core_analysis(redis: Any, news_id: str) -> dict[str, Any] | None:
    if redis is None:
        return None
    try:
        raw = await redis.get(thought_cache_key(news_id))
        if not raw:
            return None
        data = json.loads(raw)
        if is_valid_core_analysis(data):
            return data  # type: ignore[return-value]
    except Exception as exc:
        log.debug("thought_cache_get_failed", news_id=news_id[:16], error=str(exc))
    return None


async def store_core_analysis(
    redis: Any,
    news_id: str,
    core: dict[str, Any],
    *,
    ttl_sec: int = DEFAULT_CORE_TTL_SEC,
) -> None:
    if redis is None or not is_valid_core_analysis(core):
        return
    try:
        await redis.set(
            thought_cache_key(news_id),
            json.dumps(core, ensure_ascii=False),
            ex=int(ttl_sec),
        )
        log.info("master_thought_stored", news_id=news_id[:16], ttl_sec=ttl_sec)
    except Exception as exc:
        log.warning("thought_cache_set_failed", news_id=news_id[:16], error=str(exc))


async def ensure_core_analysis(
    redis: Any,
    *,
    news_id: str,
    anchor_title: str,
    anchor_link: str,
    digest_snippet: str,
    gemini_api_key: str,
    holder_id: str,
    core_ttl_sec: int = DEFAULT_CORE_TTL_SEC,
    lock_ttl_sec: int = DEFAULT_LOCK_TTL_SEC,
) -> dict[str, Any] | None:
    """
    Return Core Analysis for ``news_id``: cache hit, or this process is master and
    calls Gemini once, or wait for another master to populate the key.
    """
    if redis is None:
        return None

    cached = await get_cached_core_analysis(redis, news_id)
    if cached is not None:
        return cached

    lock_k = thought_lock_key(news_id)
    holder = (holder_id or "anon")[:120]
    acquired = False
    try:
        acquired = bool(await redis.set(lock_k, holder, nx=True, ex=int(lock_ttl_sec)))
    except Exception as exc:
        log.debug("thought_lock_set_failed", news_id=news_id[:16], error=str(exc))
        return None

    if acquired:
        try:
            if not (gemini_api_key or "").strip():
                log.debug("master_thought_skip_no_gemini", news_id=news_id[:16])
                return None
            core = await _master_gemini_core(
                gemini_api_key.strip(),
                anchor_title=anchor_title,
                anchor_link=anchor_link,
                digest_snippet=digest_snippet,
            )
            if core is not None:
                await store_core_analysis(redis, news_id, core, ttl_sec=core_ttl_sec)
            return core
        finally:
            try:
                await redis.delete(lock_k)
            except Exception as exc:
                log.debug("thought_lock_delete_failed", error=str(exc))

    deadline = time.monotonic() + MASTER_WAIT_SEC
    while time.monotonic() < deadline:
        await asyncio.sleep(POLL_INTERVAL_SEC)
        hit = await get_cached_core_analysis(redis, news_id)
        if hit is not None:
            return hit
    log.debug("master_thought_wait_timeout", news_id=news_id[:16])
    return None
