"""
ghostwriter.community_vibe — periodic Hebrew \"vibe\" in Israeli groups (message + reaction).

Uses :mod:`nexus.agents.ghostwriter.community_manager` for LLM lines and join limits.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from nexus.agents.ghostwriter.community_manager import (
    JoinHourLimiter,
    generate_israeli_ghost_message,
    join_group_auto,
)
from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

STATE_PREFIX = "nexus:ghostwriter:israeli:state:"
LOCK_PREFIX = "nexus:ghostwriter:israeli:lock:"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _redis_json_get(redis: Any, key: str) -> dict[str, Any]:
    if redis is None:
        return {}
    raw = await redis.get(key)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


async def _redis_json_set(redis: Any, key: str, data: dict[str, Any], ex: int | None = None) -> None:
    if redis is None:
        return
    payload = json.dumps(data, ensure_ascii=False)
    if ex:
        await redis.set(key, payload, ex=ex)
    else:
        await redis.set(key, payload)


def _resolve_gemini(parameters: dict[str, Any]) -> str:
    sec = parameters.get("__secrets__", {})
    return (
        str(parameters.get("gemini_api_key", "")).strip()
        or str(sec.get("GEMINI_API_KEY", "") or "")
        or os.getenv("GEMINI_API_KEY", "")
    )


def _resolve_telethon_creds(parameters: dict[str, Any]) -> tuple[int, str]:
    sec = parameters.get("__secrets__", {})
    api_id = int(sec.get("TELEFIX_API_ID") or os.getenv("TELEFIX_API_ID", "0") or "0")
    api_hash = str(sec.get("TELEFIX_API_HASH") or os.getenv("TELEFIX_API_HASH", "") or "")
    return api_id, api_hash


def _next_vibe_delay_seconds() -> int:
    lo = int(os.getenv("GHOSTWRITER_ISRAELI_VIBE_MIN_S", str(3 * 3600)))
    hi = int(os.getenv("GHOSTWRITER_ISRAELI_VIBE_MAX_S", str(6 * 3600)))
    hi = max(hi, lo + 60)
    return random.randint(lo, hi)


def _context_from_messages(messages: list[Any], limit: int = 24) -> list[str]:
    lines: list[str] = []
    for m in messages:
        if not m or not getattr(m, "id", None):
            continue
        text = (getattr(m, "message", None) or "").strip()
        if not text:
            continue
        lines.append(text)
    return lines[-limit:]


async def _send_reaction(client: Any, entity: Any, msg_id: int) -> None:
    try:
        from telethon.tl.functions.messages import SendReactionRequest  # type: ignore[import-untyped]
        from telethon.tl.types import ReactionEmoji  # type: ignore[import-untyped]

        emo = random.choice(["👍", "❤️", "🔥", "🙏"])
        await client(
            SendReactionRequest(
                peer=entity,
                msg_id=int(msg_id),
                reaction=[ReactionEmoji(emoticon=emo)],
            )
        )
    except Exception as exc:
        log.debug("ghostwriter_reaction_skipped", error=str(exc))


@registry.register("ghostwriter.community_vibe")
async def ghostwriter_community_vibe(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Send one informal Hebrew line and react to a recent peer message.

    Parameters
    ----------
    group_key : str
    group_id : int | None — Telegram entity id (negative supergroup id)
    username : str — optional, if ``group_id`` missing
    session_path : str — Telethon session file
    invite_link : str — optional, for ``join_if_needed``
    group_title : str — optional hint for the LLM
    join_if_needed : bool — try join (rate-limited) before resolving entity
    """
    redis = parameters.get("__redis__")
    group_key = str(parameters.get("group_key", "")).strip()
    group_id = parameters.get("group_id")
    username = str(parameters.get("username", "") or "").strip().lstrip("@")
    session_path = str(parameters.get("session_path", "") or "").strip()
    invite = str(parameters.get("invite_link", "") or "").strip()
    group_title = str(parameters.get("group_title", "") or "קבוצה")
    join_if_needed = bool(parameters.get("join_if_needed", True))
    session_key = str(parameters.get("session_key") or Path(session_path).stem or "session")

    lock_key = f"{LOCK_PREFIX}{group_key}" if group_key else ""

    async def release_lock() -> None:
        if redis and lock_key:
            try:
                await redis.delete(lock_key)
            except Exception:
                pass

    try:
        if not group_key or not session_path:
            return {"status": "failed", "error": "group_key and session_path required"}
        if group_id is None and not username:
            return {"status": "failed", "error": "group_id or username required"}

        api_key = _resolve_gemini(parameters)
        api_id, api_hash = _resolve_telethon_creds(parameters)
        if not api_key:
            return {"status": "failed", "error": "GEMINI_API_KEY missing"}
        if not api_id or not api_hash:
            return {"status": "failed", "error": "TELEFIX_API_ID / TELEFIX_API_HASH missing"}

        try:
            from telethon import TelegramClient  # type: ignore[import-untyped]
        except ImportError:
            return {"status": "failed", "error": "telethon not installed"}

        state_key = f"{STATE_PREFIX}{group_key}"
        state = await _redis_json_get(redis, state_key)
        limiter = JoinHourLimiter(max_per_hour=2)

        async with TelegramClient(session_path, api_id, api_hash) as client:
            entity = None
            try:
                if group_id is not None:
                    entity = await client.get_entity(int(group_id))
                else:
                    entity = await client.get_entity(username)
            except Exception:
                entity = None

            if entity is None and join_if_needed and invite:
                ok = await join_group_auto(
                    client,
                    invite,
                    session_key,
                    limiter,
                    redis=redis,
                )
                if ok:
                    try:
                        if group_id is not None:
                            entity = await client.get_entity(int(group_id))
                        else:
                            entity = await client.get_entity(username)
                    except Exception:
                        entity = None

            if entity is None:
                return {"status": "failed", "error": "could not resolve Telegram entity"}

            msgs = await client.get_messages(entity, limit=40)
            me = await client.get_me()
            my_id = getattr(me, "id", None)
            ctx = _context_from_messages([m for m in msgs if m])
            topic = random.choice(
                [
                    "מה הולך",
                    "מישהו עוקב אחרי הדברים פה?",
                    "וואלה איזה דיון",
                    "מטורף מה שקורה פה",
                ]
            )
            text = await generate_israeli_ghost_message(
                ctx,
                group_title=group_title,
                topic_hint=topic,
                provider="gemini",
                gemini_api_key=api_key,
            )
            text = (text or "").strip()
            if not text:
                return {"status": "failed", "error": "empty AI line"}

            await asyncio.sleep(random.uniform(2.0, 8.0))
            sent = await client.send_message(entity, text)
            sent_id = int(sent.id) if sent else None

            candidates: list[Any] = []
            for m in msgs:
                if not m or not getattr(m, "id", None):
                    continue
                if getattr(m, "out", False):
                    continue
                sid = getattr(m, "sender_id", None)
                if my_id is not None and sid == my_id:
                    continue
                if (getattr(m, "message", None) or "").strip():
                    candidates.append(m)
            if candidates:
                pick = random.choice(candidates[:12])
                await _send_reaction(client, entity, int(pick.id))

            delay_s = _next_vibe_delay_seconds()
            next_run = datetime.now(timezone.utc).timestamp() + delay_s
            state["next_run_at"] = datetime.fromtimestamp(next_run, tz=timezone.utc).isoformat()
            state["last_vibe_at"] = _iso_now()
            state["last_message_id"] = sent_id
            await _redis_json_set(redis, state_key, state, ex=86400 * 30)

            return {
                "status": "completed",
                "message_id": sent_id,
                "next_delay_s": delay_s,
            }
    finally:
        await release_lock()
