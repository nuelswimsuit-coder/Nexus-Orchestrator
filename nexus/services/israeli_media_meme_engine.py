"""
Israeli Telegram public-channel media → stripped files → local vision tags → Redis.

Swarm bots can sample paths via :func:`sample_meme_entries` using topic/sentiment
indexes written here.

Environment (high level)
--------------------------
NEXUS_MEME_INGEST_SESSION   — Telethon session base path (no ``.session`` suffix).
NEXUS_MEME_TG_CHANNELS    — Comma-separated ``@username`` or ``t.me/...`` entries.
NEXUS_MEME_STORE          — Download root (default ``var/meme_db``).
NEXUS_MEME_PER_CHANNEL    — Max new items per run per channel (default 60).

Respect Telegram ToS and channel rules; use only channels you may access.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from nexus.services.media_metadata_strip import strip_media_metadata_inplace
from nexus.services.meme_vision_local import classify_meme_visual

log = structlog.get_logger(__name__)

_SCHEMA = "nexus.swarm.meme_db.v1"
_CATALOG_Z = "nexus:swarm:meme_db:v1:catalog"
_LOCK_KEY = "nexus:swarm:meme_db:v1:ingest_lock"
_CURSOR_P = "nexus:swarm:meme_db:v1:cursor:{channel}"


def _channel_slug(channel: str) -> str:
    c = channel.strip().lstrip("@")
    return "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in c)[:80]


def _meme_item_key(meme_id: str) -> str:
    return f"nexus:swarm:meme_db:v1:item:{meme_id}"


def _topic_set(topic: str) -> str:
    return f"nexus:swarm:meme_db:v1:topic:{topic}"


def _sentiment_set(sentiment: str) -> str:
    return f"nexus:swarm:meme_db:v1:sentiment:{sentiment}"


def _parse_channels(parameters: dict[str, Any]) -> list[str]:
    if parameters.get("channels"):
        raw = parameters["channels"]
        if isinstance(raw, str):
            return [x.strip() for x in raw.split(",") if x.strip()]
        if isinstance(raw, list):
            return [str(x).strip() for x in raw if str(x).strip()]
    env = (os.getenv("NEXUS_MEME_TG_CHANNELS") or "").strip()
    return [x.strip() for x in env.split(",") if x.strip()]


def _media_plan(message: Any) -> tuple[str | None, str | None]:
    """
    Return ``(kind, uniq)`` where kind is photo | video | sticker | None.
    Skips tgs, voice, plain files we do not want in the meme DB.
    """
    try:
        if getattr(message, "voice", None) or getattr(message, "audio", None):
            return None, None
        if getattr(message, "sticker", None):
            doc = getattr(message, "document", None)
            if doc and getattr(doc, "mime_type", "") == "application/x-tgsticker":
                return None, None
            uq = getattr(doc, "file_unique_id", None) if doc else None
            return "sticker", str(uq or message.id)
        if getattr(message, "photo", None):
            ph = message.photo
            uq = getattr(ph, "file_unique_id", None)
            return "photo", str(uq or getattr(ph, "id", message.id))
        if getattr(message, "video", None):
            doc = message.video
            uq = getattr(doc, "file_unique_id", None)
            return "video", str(uq or message.id)
        if getattr(message, "document", None):
            doc = message.document
            mime = (getattr(doc, "mime_type", None) or "").lower()
            uq = getattr(doc, "file_unique_id", None)
            if mime == "application/x-tgsticker":
                return None, None
            if "video" in mime:
                return "video", str(uq or doc.id)
            if "image" in mime:
                return "photo", str(uq or doc.id)
        return None, None
    except Exception:
        return None, None


def _video_preview_path(video_path: Path) -> Path | None:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        return None
    out = video_path.with_suffix(".meme_preview.jpg")
    cmd = [
        ffmpeg,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-ss",
        "0.5",
        "-i",
        str(video_path),
        "-frames:v",
        "1",
        str(out),
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True, timeout=120)
        if out.is_file() and out.stat().st_size > 0:
            return out
    except Exception:
        pass
    try:
        out.unlink(missing_ok=True)
    except Exception:
        pass
    return None


async def _redis_purge_meme(redis: Any, meme_id: str) -> None:
    key = _meme_item_key(meme_id)
    blob = await redis.hgetall(key)
    if blob:
        topic = blob.get("topic")
        sentiment = blob.get("sentiment")
        if topic:
            await redis.srem(_topic_set(topic), meme_id)
        if sentiment:
            await redis.srem(_sentiment_set(sentiment), meme_id)
    await redis.delete(key)
    await redis.zrem(_CATALOG_Z, meme_id)


async def _redis_trim(redis: Any, cap: int) -> None:
    n = await redis.zcard(_CATALOG_Z)
    if n <= cap:
        return
    over = int(n - cap)
    oldest = await redis.zrange(_CATALOG_Z, 0, over - 1)
    for mid in oldest:
        await _redis_purge_meme(redis, mid)


async def sample_meme_entries(
    redis: Any,
    *,
    topic: str | None = None,
    sentiment: str | None = None,
    k: int = 5,
) -> list[dict[str, Any]]:
    """
    Return up to ``k`` meme metadata dicts (includes absolute ``path``) for bots.
    """
    if redis is None or k <= 0:
        return []
    meme_ids: list[str] = []
    if topic and sentiment:
        t_set, s_set = _topic_set(topic), _sentiment_set(sentiment)
        raw = await redis.sinter(t_set, s_set)
        meme_ids = list(raw) if raw else []
    elif topic:
        raw = await redis.srandmember(_topic_set(topic), k * 3)
        meme_ids = list(raw) if raw else []
    elif sentiment:
        raw = await redis.srandmember(_sentiment_set(sentiment), k * 3)
        meme_ids = list(raw) if raw else []
    else:
        raw = await redis.zrevrange(_CATALOG_Z, 0, max(k * 5, k) - 1)
        meme_ids = list(raw) if raw else []

    out: list[dict[str, Any]] = []
    for mid in meme_ids:
        if len(out) >= k:
            break
        h = await redis.hgetall(_meme_item_key(mid))
        if not h or not h.get("path"):
            continue
        try:
            out.append({**h, "meme_id": mid})
        except Exception:
            continue
    return out[:k]


def _redis_str_mapping(d: dict[str, Any]) -> dict[str, str]:
    return {str(k): "" if v is None else str(v) for k, v in d.items()}


async def ingest_israeli_telegram_media(
    client: Any,
    redis: Any,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    """
    Download new media from configured channels using an existing Telethon ``client``,
    strip metadata, classify, index Redis.
    """
    if redis is None:
        return {"status": "failed", "error": "redis_unavailable", "schema": _SCHEMA}

    lock_s = int(os.getenv("NEXUS_MEME_INGEST_LOCK_SEC", "7200") or "7200")
    got = await redis.set(_LOCK_KEY, "1", nx=True, ex=max(60, lock_s))
    if not got:
        return {"status": "skipped", "reason": "lock_held", "schema": _SCHEMA}

    channels = _parse_channels(parameters)
    store = Path(
        parameters.get("meme_store")
        or os.getenv("NEXUS_MEME_STORE")
        or "var/meme_db",
    ).resolve()
    try:
        per_ch = int(
            parameters.get("per_channel_limit")
            or os.getenv("NEXUS_MEME_PER_CHANNEL")
            or "60",
        )
    except ValueError:
        per_ch = 60
    per_ch = max(1, min(500, per_ch))
    try:
        cap = int(os.getenv("NEXUS_MEME_CATALOG_CAP", "8000") or "8000")
    except ValueError:
        cap = 8000
    cap = max(100, min(200_000, cap))

    summary: dict[str, Any] = {
        "schema": _SCHEMA,
        "status": "ok",
        "channels": channels,
        "ingested": 0,
        "skipped": 0,
        "errors": [],
    }

    try:
        if not channels:
            summary["status"] = "failed"
            summary["error"] = "missing NEXUS_MEME_TG_CHANNELS or parameters.channels"
            return summary

        store.mkdir(parents=True, exist_ok=True)

        for ch in channels:
            slug = _channel_slug(ch)
            cursor_key = _CURSOR_P.format(channel=slug)
            try:
                cur_raw = await redis.get(cursor_key)
                cursor_id = int(cur_raw) if cur_raw else 0
            except (TypeError, ValueError):
                cursor_id = 0

            entity = await client.get_entity(ch)
            ch_dir = store / slug
            ch_dir.mkdir(parents=True, exist_ok=True)

            collected: list[Any] = []
            if cursor_id == 0:
                async for m in client.iter_messages(entity, limit=per_ch):
                    collected.append(m)
            else:
                async for m in client.iter_messages(
                    entity,
                    min_id=cursor_id,
                    limit=per_ch,
                    reverse=True,
                ):
                    collected.append(m)

            max_seen = cursor_id
            for m in collected:
                try:
                    max_seen = max(max_seen, int(m.id))
                    kind, uniq = _media_plan(m)
                    if not kind:
                        summary["skipped"] += 1
                        continue
                    meme_id = hashlib.sha256(
                        f"{slug}:{m.id}:{uniq}".encode(),
                    ).hexdigest()[:32]
                    if await redis.exists(_meme_item_key(meme_id)):
                        continue

                    ext_hint = ".jpg" if kind == "photo" else ".webp"
                    if kind == "video":
                        ext_hint = ".mp4"
                    tmp_root = ch_dir / "_tmp"
                    tmp_root.mkdir(parents=True, exist_ok=True)
                    tmp_base = tmp_root / f"{meme_id}_dl"
                    saved = await m.download_media(file=str(tmp_base))
                    if not saved:
                        summary["skipped"] += 1
                        continue
                    src = Path(str(saved))
                    if not src.is_file():
                        summary["skipped"] += 1
                        continue
                    final = ch_dir / f"{meme_id}{src.suffix.lower() or ext_hint}"
                    if final.resolve() != src.resolve():
                        try:
                            final.unlink(missing_ok=True)
                        except Exception:
                            pass
                        src.replace(final)
                    else:
                        final = src

                    strip_media_metadata_inplace(final)

                    vision_path = final
                    preview_tmp: Path | None = None
                    if kind == "video":
                        preview_tmp = await asyncio.to_thread(_video_preview_path, final)
                        if preview_tmp is not None:
                            vision_path = preview_tmp

                    tags = await classify_meme_visual(vision_path, media_kind=kind)
                    if preview_tmp is not None:
                        try:
                            preview_tmp.unlink(missing_ok=True)
                        except Exception:
                            pass

                    now = datetime.now(timezone.utc)
                    ts_ms = int(now.timestamp() * 1000)
                    rec = {
                        "schema": _SCHEMA,
                        "path": str(final.resolve()),
                        "channel": ch,
                        "channel_slug": slug,
                        "message_id": str(m.id),
                        "media_kind": kind,
                        "topic": tags["topic"],
                        "sentiment": tags["sentiment"],
                        "vision_backend": tags.get("vision_backend", ""),
                        "ingested_at": now.isoformat(),
                    }
                    pipe = redis.pipeline(transaction=True)
                    pipe.hset(_meme_item_key(meme_id), mapping=_redis_str_mapping(rec))
                    pipe.zadd(_CATALOG_Z, {meme_id: ts_ms})
                    pipe.sadd(_topic_set(tags["topic"]), meme_id)
                    pipe.sadd(_sentiment_set(tags["sentiment"]), meme_id)
                    await pipe.execute()

                    summary["ingested"] += 1
                    await _redis_trim(redis, cap)

                    pub = {
                        "schema": _SCHEMA,
                        "event": "meme_ingested",
                        "meme_id": meme_id,
                        "topic": tags["topic"],
                        "sentiment": tags["sentiment"],
                        "path": rec["path"],
                    }
                    try:
                        await redis.publish(
                            "nexus:swarm:meme_db",
                            json.dumps(pub, ensure_ascii=False),
                        )
                    except Exception as exc:
                        log.debug("meme_db_pub_failed", error=str(exc))
                except Exception as exc:
                    summary["errors"].append(f"{ch} msg={getattr(m, 'id', '?')}: {exc}")
                    log.warning("meme_ingest_message_failed", channel=ch, error=str(exc))

            if max_seen > cursor_id:
                await redis.set(cursor_key, str(max_seen))

        return summary
    finally:
        try:
            await redis.delete(_LOCK_KEY)
        except Exception:
            pass
