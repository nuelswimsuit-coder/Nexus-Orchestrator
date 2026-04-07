"""
Bridge OpenClaw (Moltbot) JSON files on disk into Redis Pub/Sub for the swarm.

Uses ``watchdog`` to observe a single output directory. Each new or updated ``*.json``
file is read (with retries while the writer may still hold the file), normalized to::

    {"headline": "...", "content": "...", "timestamp": "..."}

and published on ``nexus:swarm:news_digest`` (see ``SWARM_NEWS_DIGEST_CHANNEL``).

**CLI / env**

- ``nexus-openclaw-bridge`` or ``python -m nexus.services.openclaw_bridge``
- ``--output-dir`` or ``OPENCLAW_OUTPUT_DIR`` — directory to watch (non-recursive).
- If neither is set and stdin is a TTY, you are prompted once for the path.
- ``REDIS_URL`` or ``redis://REDIS_HOST:REDIS_PORT/REDIS_DB`` (via ``nexus.shared.redis_util``).

**Batch LLM ingestion (token efficiency)**

Items are buffered until ``OPENCLAW_INGEST_BATCH_MAX`` (default ``10``) or
``OPENCLAW_INGEST_FLUSH_SEC`` seconds (default ``300``) since the first item in
the current window. One OpenAI chat call analyzes the whole batch; results are
written as separate Redis keys under ``nexus:openclaw:swarm:ingest:item:*``,
published on ``nexus:swarm:news_digest``, and keyword hits go through
``ingest_text_for_swarm_sync``. Requires ``OPENAI_API_KEY``; without it, local
fallback summaries/scores are used. Optional: ``OPENCLAW_BATCH_LLM_MODEL``.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any

import structlog
from redis import Redis
from redis.exceptions import RedisError
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from nexus.services.openclaw_ingest_batch import (
    OpenClawIngestBuffer,
    PendingOpenClawItem,
    flush_buffer_to_redis,
)
from nexus.services.recent_news_digest import SWARM_NEWS_DIGEST_CHANNEL
from nexus.shared.redis_util import coerce_redis_url_for_platform

log = structlog.get_logger(__name__)

_HEADLINE_KEYS = (
    "headline",
    "title",
    "subject",
    "head_line",
    "anchor_title",
    "name",
)
_CONTENT_KEYS = (
    "content",
    "body",
    "text",
    "summary",
    "article",
    "article_text",
    "digest_text",
    "excerpt",
    "description",
    "message",
)


def _coerce_redis_url(url: str) -> str:
    try:
        return coerce_redis_url_for_platform(url)
    except Exception:
        return url


def _redis_url_from_env() -> str:
    env_url = (os.getenv("REDIS_URL") or "").strip()
    host = (os.getenv("REDIS_HOST") or os.getenv("MASTER_IP") or "127.0.0.1").strip() or "127.0.0.1"
    port = (os.getenv("REDIS_PORT") or "6379").strip()
    db = (os.getenv("REDIS_DB") or "0").strip()
    raw = env_url or f"redis://{host}:{port}/{db}"
    return _coerce_redis_url(raw)


def _first_str(d: dict[str, Any], keys: tuple[str, ...]) -> str:
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def extract_headline_content(data: Any) -> tuple[str, str]:
    """Pull headline and body from OpenClaw JSON (flexible keys and light nesting)."""
    obj: Any = data
    if isinstance(obj, list) and obj:
        obj = obj[0]
    if not isinstance(obj, dict):
        return "", ""

    for nest_key in ("article", "item", "news", "data", "payload"):
        nested = obj.get(nest_key)
        if isinstance(nested, dict):
            h, c = extract_headline_content(nested)
            if h or c:
                fb_h = _first_str(obj, _HEADLINE_KEYS)
                fb_c = _first_str(obj, _CONTENT_KEYS)
                return (h or fb_h), (c or fb_c)

    return _first_str(obj, _HEADLINE_KEYS), _first_str(obj, _CONTENT_KEYS)


def _read_bytes_stable(path: Path, *, max_rounds: int = 22, delay: float = 0.05) -> bytes:
    """
    Read bytes only after two consecutive (st_size, len(data)) pairs match, so we
    avoid half-written JSON and many transient lock/share violations on Windows.
    """
    d = delay
    prev: tuple[int, int] | None = None
    last_err: Exception | None = None
    for _ in range(max_rounds):
        try:
            sz = path.stat().st_size
        except OSError as exc:
            last_err = exc
            time.sleep(d)
            d = min(d * 1.5, 0.55)
            continue
        try:
            data = path.read_bytes()
        except OSError as exc:
            last_err = exc
            time.sleep(d)
            d = min(d * 1.5, 0.55)
            continue
        cur = (sz, len(data))
        if prev == cur and sz == len(data):
            return data
        prev = cur
        time.sleep(d)
        d = min(d * 1.35, 0.5)
    if last_err:
        raise last_err
    raise OSError(f"could not read stable contents: {path}")


def read_openclaw_json(path: Path) -> Any:
    """Parse JSON with retries while OpenClaw is still writing or the file is locked."""
    d = 0.05
    last_exc: Exception | None = None
    for _ in range(18):
        try:
            raw = _read_bytes_stable(path)
            if not raw.strip():
                raise ValueError("empty file")
            return json.loads(raw.decode("utf-8"))
        except (OSError, ValueError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            last_exc = exc
            time.sleep(d)
            d = min(d * 1.45, 0.65)
    assert last_exc is not None
    raise last_exc


def build_swarm_message(
    headline: str,
    content: str,
    *,
    source_doc: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ts = datetime.now(timezone.utc).isoformat()
    out: dict[str, Any] = {"headline": headline, "content": content, "timestamp": ts}
    reserved = {
        "headline",
        "title",
        "subject",
        "head_line",
        "anchor_title",
        "name",
        "content",
        "body",
        "text",
        "summary",
        "article",
        "article_text",
        "digest_text",
        "excerpt",
        "description",
        "message",
        "timestamp",
        "ts",
    }
    if isinstance(source_doc, dict):
        for k, v in source_doc.items():
            ks = str(k)
            if ks in out or ks.lower() in reserved:
                continue
            if isinstance(v, (str, int, float, bool)) or v is None:
                out[ks] = v if not isinstance(v, str) else v[:8000]
    return out


def _payload_fingerprint(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()


class OpenClawJsonHandler(FileSystemEventHandler):
    """Watch ``*.json`` in one directory; dedupe identical payloads per path after publish."""

    def __init__(self, redis_client: Redis) -> None:
        self._redis = redis_client
        self._lock = RLock()
        self._last_hash_by_path: dict[str, str] = {}
        self._ingest_buffer = OpenClawIngestBuffer()

    def _maybe_handle(self, path: Path) -> None:
        if path.suffix.lower() != ".json":
            return
        if not path.is_file():
            return
        try:
            process_json_file(path, self._redis, self)
        except Exception as exc:
            log.warning("openclaw_bridge_process_failed", path=str(path), error=str(exc))

    def on_created(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self._maybe_handle(Path(event.src_path))

    def on_modified(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self._maybe_handle(Path(event.src_path))

    def on_moved(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self._maybe_handle(Path(event.dest_path))

    def is_same_as_last_publish(self, path: Path, payload: dict[str, Any]) -> bool:
        key = str(path.resolve())
        fp = _payload_fingerprint(payload)
        with self._lock:
            return self._last_hash_by_path.get(key) == fp

    def remember_publish(self, path: Path, payload: dict[str, Any]) -> None:
        key = str(path.resolve())
        fp = _payload_fingerprint(payload)
        with self._lock:
            self._last_hash_by_path[key] = fp


def process_json_file(
    path: Path,
    redis_client: Redis,
    handler: OpenClawJsonHandler | None = None,
) -> bool:
    """
    Read one JSON file, normalize, publish. Returns True if a message was published.

    Skips when headline and content are both empty, or when the payload matches the
    last successful publish for this path (common when both ``on_created`` and
    ``on_modified`` fire for the same write).
    """
    data = read_openclaw_json(path)
    headline, content = extract_headline_content(data)
    if not headline and not content:
        log.info("openclaw_bridge_skip_empty", path=str(path))
        return False

    src = data if isinstance(data, dict) else None
    payload = build_swarm_message(headline, content, source_doc=src)
    if handler is not None and handler.is_same_as_last_publish(path, payload):
        log.debug("openclaw_bridge_skip_duplicate", path=str(path))
        return False

    if handler is not None:
        batch_to_flush: list[PendingOpenClawItem] | None = None
        with handler._lock:
            handler._ingest_buffer.append(
                PendingOpenClawItem(
                    path=path,
                    headline=headline,
                    content=content,
                    payload=payload,
                )
            )
            if handler._ingest_buffer.should_flush_count():
                batch_to_flush = handler._ingest_buffer.drain()
        if batch_to_flush:
            try:
                flush_buffer_to_redis(redis_client, batch_to_flush)
                with handler._lock:
                    for it in batch_to_flush:
                        handler.remember_publish(it.path, it.payload)
            except Exception:
                with handler._lock:
                    handler._ingest_buffer.prepend(batch_to_flush)
                raise
        log.info(
            "openclaw_bridge_buffered",
            path=str(path),
            headline_preview=(headline or "")[:80],
            buffer_len=handler._ingest_buffer.peek_len(),
        )
        return True

    body = json.dumps(payload, ensure_ascii=False)
    try:
        redis_client.publish(SWARM_NEWS_DIGEST_CHANNEL, body)
    except RedisError as exc:
        log.error("openclaw_bridge_redis_publish_failed", error=str(exc))
        raise
    log.info(
        "openclaw_bridge_published",
        channel=SWARM_NEWS_DIGEST_CHANNEL,
        path=str(path),
        headline_preview=(headline or "")[:80],
    )
    return True


def resolve_watch_directory(cli_dir: str | None) -> Path:
    raw = (cli_dir or os.getenv("OPENCLAW_OUTPUT_DIR") or "").strip()
    if not raw:
        if sys.stdin.isatty():
            raw = input("OpenClaw output directory path (or set OPENCLAW_OUTPUT_DIR): ").strip()
        if not raw:
            raise SystemExit(
                "Missing OpenClaw output directory. Set OPENCLAW_OUTPUT_DIR or pass --output-dir."
            )
    p = Path(raw).expanduser().resolve()
    if not p.is_dir():
        raise SystemExit(f"OpenClaw output directory does not exist or is not a directory: {p}")
    return p


def run_bridge(watch_dir: Path, redis_url: str) -> None:
    """Run until interrupted: watch ``watch_dir`` (non-recursive) and publish to Redis."""
    client = Redis.from_url(redis_url, decode_responses=True)
    try:
        client.ping()
    except RedisError as exc:
        log.warning(
            "openclaw_bridge_redis_ping_failed",
            error=str(exc),
            hint="Publish attempts may fail until Redis is reachable.",
        )

    handler = OpenClawJsonHandler(client)
    observer = Observer()
    observer.schedule(handler, str(watch_dir), recursive=False)
    observer.start()
    safe_url = redis_url.split("@")[-1] if "@" in redis_url else redis_url
    log.info(
        "openclaw_bridge_started",
        watch_dir=str(watch_dir),
        channel=SWARM_NEWS_DIGEST_CHANNEL,
        redis=safe_url,
    )
    try:
        while True:
            time.sleep(1.0)
            try:
                batch: list[PendingOpenClawItem] | None = None
                with handler._lock:
                    buf = handler._ingest_buffer
                    if buf.peek_len() and buf.should_flush_time():
                        batch = buf.drain()
                if batch:
                    try:
                        flush_buffer_to_redis(client, batch)
                        with handler._lock:
                            for it in batch:
                                handler.remember_publish(it.path, it.payload)
                    except Exception as exc:
                        with handler._lock:
                            handler._ingest_buffer.prepend(batch)
                        log.warning("openclaw_bridge_time_flush_failed", error=str(exc))
            except Exception as exc:
                log.debug("openclaw_bridge_flush_tick_error", error=str(exc))
    except KeyboardInterrupt:
        log.info("openclaw_bridge_stopping")
    finally:
        observer.stop()
        observer.join(timeout=5)
        try:
            client.close()
        except Exception:
            pass


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Bridge OpenClaw JSON files into Redis Pub/Sub (nexus:swarm:news_digest).",
    )
    parser.add_argument(
        "--output-dir",
        default="",
        help="Directory to watch for .json files (default: OPENCLAW_OUTPUT_DIR)",
    )
    parser.add_argument(
        "--redis-url",
        default="",
        help="Redis URL (default: REDIS_URL or redis://REDIS_HOST:REDIS_PORT/REDIS_DB)",
    )
    args = parser.parse_args()
    watch_dir = resolve_watch_directory(args.output_dir or None)
    redis_url = (args.redis_url or "").strip() or _redis_url_from_env()
    run_bridge(watch_dir, redis_url)


if __name__ == "__main__":
    main()
