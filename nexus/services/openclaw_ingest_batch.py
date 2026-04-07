"""
OpenClaw ingestion batching: one LLM call per up-to-10 news items, then fan-out to Redis.

Buffers items until OPENCLAW_INGEST_BATCH_MAX (default 10) or OPENCLAW_INGEST_FLUSH_SEC (300s).
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import structlog
from redis import Redis
from redis.exceptions import RedisError

from nexus.services.recent_news_digest import SWARM_NEWS_DIGEST_CHANNEL
from nexus.shared.swarm_pacing import OPENCLAW_NEWS_SENTIMENT_KEY
from nexus.shared.swarm_signals import ingest_text_for_swarm_sync

log = structlog.get_logger(__name__)

OPENCLAW_INGEST_ITEM_KEY_PREFIX = "nexus:openclaw:swarm:ingest:item"
OPENCLAW_INGEST_BATCH_LIST_KEY = "nexus:openclaw:swarm:ingest:recent_batches"
OPENCLAW_NEWS_SENTIMENT_TTL = 900

_DEFAULT_MAX_ITEMS = 10
_DEFAULT_FLUSH_SEC = 300.0


def _batch_max_items() -> int:
    raw = (os.getenv("OPENCLAW_INGEST_BATCH_MAX") or str(_DEFAULT_MAX_ITEMS)).strip()
    try:
        n = int(raw)
    except ValueError:
        n = _DEFAULT_MAX_ITEMS
    return max(1, min(25, n))


def _batch_flush_sec() -> float:
    raw = (os.getenv("OPENCLAW_INGEST_FLUSH_SEC") or str(int(_DEFAULT_FLUSH_SEC))).strip()
    try:
        s = float(raw)
    except ValueError:
        s = _DEFAULT_FLUSH_SEC
    return max(30.0, min(3600.0, s))


def _openai_model() -> str:
    return (os.getenv("OPENCLAW_BATCH_LLM_MODEL") or "gpt-4o-mini").strip() or "gpt-4o-mini"


def _openai_key() -> str:
    return (os.getenv("OPENAI_API_KEY") or "").strip()


@dataclass
class PendingOpenClawItem:
    path: Path
    headline: str
    content: str
    payload: dict[str, str]


class OpenClawIngestBuffer:
    """Collect OpenClaw JSON-derived items; flush on count or time window."""

    def __init__(self) -> None:
        self._items: list[PendingOpenClawItem] = []
        self._window_start_monotonic: float | None = None

    def append(self, item: PendingOpenClawItem) -> None:
        if not self._items:
            self._window_start_monotonic = time.monotonic()
        self._items.append(item)

    def should_flush_count(self) -> bool:
        return len(self._items) >= _batch_max_items()

    def should_flush_time(self) -> bool:
        if not self._items or self._window_start_monotonic is None:
            return False
        return (time.monotonic() - self._window_start_monotonic) >= _batch_flush_sec()

    def drain(self) -> list[PendingOpenClawItem]:
        out = self._items
        self._items = []
        self._window_start_monotonic = None
        return out

    def prepend(self, items: list[PendingOpenClawItem]) -> None:
        """Restore a failed flush back to the front of the queue (FIFO for those items)."""
        if not items:
            return
        self._items = items + self._items
        if self._window_start_monotonic is None:
            self._window_start_monotonic = time.monotonic()

    def peek_len(self) -> int:
        return len(self._items)


def _strip_code_fence(text: str) -> str:
    t = (text or "").strip()
    if t.startswith("```"):
        lines = t.split("\n")
        if len(lines) >= 2:
            inner = "\n".join(lines[1:])
            if "```" in inner:
                inner = inner.rsplit("```", 1)[0]
            return inner.strip()
    return t


def _parse_json_array(raw: str) -> list[dict[str, Any]]:
    t = _strip_code_fence(raw)
    start = t.find("[")
    end = t.rfind("]")
    if start < 0 or end <= start:
        raise ValueError("no JSON array in model output")
    chunk = t[start : end + 1]
    data = json.loads(chunk)
    if not isinstance(data, list):
        raise ValueError("expected JSON array")
    out: list[dict[str, Any]] = []
    for el in data:
        if isinstance(el, dict):
            out.append(el)
    return out


def _fallback_analyses(items: list[PendingOpenClawItem]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for it in items:
        blob = f"{it.headline}\n{it.content}".strip()
        summary = (blob.replace("\n", " ")[:220] + ("…" if len(blob) > 220 else "")).strip() or "(empty)"
        out.append({"summary": summary, "sentiment_score": 5.0})
    return out


def llm_analyze_news_batch(items: list[PendingOpenClawItem]) -> list[dict[str, Any]]:
    """
    Single chat completion: one user message (instruction + numbered items + JSON shape).
    No system message — reduces repeated token overhead vs per-item calls.
    """
    n = len(items)
    if n == 0:
        return []

    lines: list[str] = []
    for i, it in enumerate(items, start=1):
        h = (it.headline or "").strip()[:800]
        c = (it.content or "").strip()[:4000]
        lines.append(f"{i}. headline: {h}\n   body: {c}")

    instruction = (
        f"Analyze these {n} items. For each, give a 1-sentence summary and a sentiment score.\n\n"
        f"You must respond with ONLY a valid JSON array of exactly {n} objects, in order (index 1 = item 1). "
        'Each object must have keys "summary" (string, one sentence) and '
        '"sentiment_score" (number from 0 to 10, where 10 is maximally positive).\n\n'
        "Items:\n"
        + "\n\n".join(lines)
    )

    key = _openai_key()
    if not key:
        log.info("openclaw_batch_llm_skipped_no_key", items=n)
        return _fallback_analyses(items)

    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    payload = {
        "model": _openai_model(),
        "messages": [{"role": "user", "content": instruction}],
        "temperature": 0.25,
        "max_tokens": min(4096, 180 * n + 200),
    }

    try:
        with httpx.Client(timeout=120.0) as client:
            r = client.post(url, json=payload, headers=headers)
            r.raise_for_status()
            data = r.json()
        choice = (data.get("choices") or [{}])[0]
        raw_msg = (choice.get("message") or {}).get("content") or ""
        parsed = _parse_json_array(raw_msg)
    except Exception as exc:
        log.warning("openclaw_batch_llm_failed", error=str(exc), items=n)
        return _fallback_analyses(items)

    merged: list[dict[str, Any]] = []
    for i in range(n):
        src = parsed[i] if i < len(parsed) else {}
        summ = str(src.get("summary", "")).strip()
        if not summ:
            summ = _fallback_analyses([items[i]])[0]["summary"]
        try:
            score = float(src.get("sentiment_score", 5.0))
        except (TypeError, ValueError):
            score = 5.0
        score = max(0.0, min(10.0, score))
        merged.append({"summary": summ, "sentiment_score": score})

    if len(merged) != n:
        return _fallback_analyses(items)
    return merged


def _agent_fingerprint_for_item(it: PendingOpenClawItem) -> str:
    base = f"{it.headline}|{it.path}"
    return hashlib.sha256(base.encode("utf-8", errors="ignore")).hexdigest()[:20]


def flush_buffer_to_redis(redis_client: Redis, batch: list[PendingOpenClawItem]) -> None:
    """Run one LLM batch, write N Redis entries, publish, optional aggregate sentiment."""
    if not batch:
        return

    analyses = llm_analyze_news_batch(batch)
    batch_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{secrets.token_hex(4)}"
    ts = datetime.now(timezone.utc).isoformat()
    scores: list[float] = []

    for idx, (it, row) in enumerate(zip(batch, analyses), start=1):
        summary = str(row.get("summary", ""))
        try:
            sentiment = float(row.get("sentiment_score", 5.0))
        except (TypeError, ValueError):
            sentiment = 5.0
        sentiment = max(0.0, min(10.0, sentiment))
        scores.append(sentiment)

        body = {
            "schema": "nexus.openclaw.news_item.v1",
            "headline": it.payload.get("headline", ""),
            "content": it.payload.get("content", ""),
            "timestamp": it.payload.get("timestamp", ts),
            "summary": summary,
            "sentiment_score": sentiment,
            "engine": "openclaw_bridge_batch",
            "batch_id": batch_id,
            "item_index": idx,
        }
        redis_key = f"{OPENCLAW_INGEST_ITEM_KEY_PREFIX}:{batch_id}:{idx}"
        try:
            redis_client.set(redis_key, json.dumps(body, ensure_ascii=False), ex=86400)
            redis_client.lpush(OPENCLAW_INGEST_BATCH_LIST_KEY, redis_key)
            redis_client.ltrim(OPENCLAW_INGEST_BATCH_LIST_KEY, 0, 199)
            redis_client.publish(SWARM_NEWS_DIGEST_CHANNEL, json.dumps(body, ensure_ascii=False))
        except RedisError as exc:
            log.error("openclaw_batch_redis_write_failed", key=redis_key, error=str(exc))
            raise

        fp = _agent_fingerprint_for_item(it)
        blob = f"{it.headline}\n{summary}"
        matched = ingest_text_for_swarm_sync(redis_client, blob, fp)
        if matched:
            log.info("openclaw_batch_swarm_keywords", item=idx, matches=list(matched.keys()))

    if scores:
        mean_score = sum(scores) / len(scores)
        excerpt_parts = [str(a.get("summary", ""))[:200] for a in analyses[:3]]
        excerpt = " | ".join(x for x in excerpt_parts if x)
        agg = {
            "score": round(mean_score, 2),
            "channel_title": f"openclaw_batch:{batch_id}",
            "excerpt": excerpt[:1200],
            "source": "openclaw_bridge_batch",
            "updated_at": ts,
        }
        try:
            redis_client.set(
                OPENCLAW_NEWS_SENTIMENT_KEY,
                json.dumps(agg, ensure_ascii=False),
                ex=OPENCLAW_NEWS_SENTIMENT_TTL,
            )
        except RedisError as exc:
            log.debug("openclaw_batch_sentiment_aggregate_failed", error=str(exc))

    log.info(
        "openclaw_batch_flushed",
        batch_id=batch_id,
        count=len(batch),
        mean_sentiment=round(sum(scores) / len(scores), 2) if scores else None,
    )
