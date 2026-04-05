"""
Recent Telegram group messages via Telethon — used by swarm chat UI (short Redis cache).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Awaitable, Callable

from nexus.services.session_vault import discover_meta_paths_from_session_sqlite, vault_candidate_roots


def first_authorized_session_path_stem() -> str | None:
    """
    Telethon session path without ``.session`` suffix.

    Prefers ``<stem>.session`` + sibling ``<stem>.json`` containing ``api_id`` / ``api_hash``
    (vault indexing). If none, falls back to the first ``*.session`` under vault roots so callers
    that supply matching ``TELEGRAM_*`` / ``TELEFIX_*`` in the environment still work.
    """
    metas = list(discover_meta_paths_from_session_sqlite())
    if metas:
        meta = metas[0]
        return str((meta.parent / meta.stem).resolve())

    sessions: list[Path] = []
    for root in vault_candidate_roots():
        if not root.is_dir():
            continue
        for sess in root.rglob("*.session"):
            if sess.name.endswith("-journal"):
                continue
            sessions.append(sess)
    if not sessions:
        return None

    def _fallback_sort_key(p: Path) -> tuple[int, str]:
        # Prefer swarm-touched accounts (sibling .swarm_identity.json) over unrelated vault files.
        swarm_touch = p.with_name(f"{p.stem}.swarm_identity.json").is_file()
        return (0 if swarm_touch else 1, p.as_posix().lower())

    first = sorted(sessions, key=_fallback_sort_key)[0]
    return str(first.with_suffix("").resolve())


def _reply_to_id(m: Any) -> int | None:
    rid = getattr(m, "reply_to_msg_id", None)
    if rid is not None:
        try:
            return int(rid)
        except (TypeError, ValueError):
            pass
    rto = getattr(m, "reply_to", None)
    if rto is not None:
        inner = getattr(rto, "reply_to_msg_id", None)
        if inner is not None:
            try:
                return int(inner)
            except (TypeError, ValueError):
                pass
    return None


async def _sender_label(m: Any) -> str:
    name = "משתמש"
    try:
        sdr = await m.get_sender()
        if sdr is not None:
            parts = [
                str(getattr(sdr, "first_name", "") or "").strip(),
                str(getattr(sdr, "last_name", "") or "").strip(),
            ]
            un = str(getattr(sdr, "username", "") or "").strip()
            base = " ".join(p for p in parts if p).strip()
            if un:
                name = f"{base} (@{un})" if base else f"@{un}"
            elif base:
                name = base
    except Exception:
        pass
    return name


async def fetch_group_messages_telethon(
    session_path_no_ext: str,
    api_id: int,
    api_hash: str,
    entity_ref: str | int,
    *,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    Fetch recent messages (oldest first). ``entity_ref`` is invite/username string or numeric Telegram id.
    """
    try:
        from telethon import TelegramClient  # type: ignore[import-untyped]
    except ImportError:
        return []

    client = TelegramClient(session_path_no_ext, api_id, api_hash)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            return []

        if isinstance(entity_ref, int):
            entity = await client.get_entity(entity_ref)
        else:
            from src.nexus.services.israeli_swarm import _ensure_swarm_target_entity

            entity = await _ensure_swarm_target_entity(client, str(entity_ref).strip())

        msgs = await client.get_messages(entity, limit=limit)
        out: list[dict[str, Any]] = []
        for m in reversed([x for x in msgs if x]):
            mid = int(getattr(m, "id", 0) or 0)
            if not mid:
                continue
            raw = (getattr(m, "message", None) or getattr(m, "raw_text", None) or "") or ""
            text = str(raw).strip()
            if not text:
                text = "[מדיה / ללא טקסט]"
            dt = getattr(m, "date", None)
            ts_iso = dt.isoformat() if dt else ""
            sender = await _sender_label(m)
            out.append(
                {
                    "message_id": mid,
                    "date": ts_iso,
                    "text": text,
                    "sender_label": sender,
                    "out": bool(getattr(m, "out", False)),
                    "reply_to_msg_id": _reply_to_id(m),
                }
            )
        return out
    finally:
        await client.disconnect()


async def fetch_group_messages_cached(
    redis: Any,
    *,
    cache_key: str,
    ttl_seconds: int,
    producer: Callable[[], Awaitable[list[dict[str, Any]]]],
) -> tuple[list[dict[str, Any]], bool]:
    """Return (messages, from_cache)."""
    try:
        raw = await redis.get(cache_key)
        if raw:
            txt = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
            data = json.loads(txt)
            if isinstance(data, dict) and isinstance(data.get("messages"), list):
                return data["messages"], True
    except Exception:
        pass

    messages = await producer()
    try:
        await redis.set(
            cache_key,
            json.dumps({"messages": messages}, ensure_ascii=False),
            ex=max(5, min(ttl_seconds, 120)),
        )
    except Exception:
        pass
    return messages, False
