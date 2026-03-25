"""
swarm.group_warmer — AI-driven multi-session group chatter + community classification.

Uses Gemini 1.5 Flash (via ``nexus.agents.modules.community_vibe``) for personas,
topics, and lines. Telethon delivers messages. Variable delays and day/night
activity waves are applied when computing ``next_run_at`` (see scheduler).
"""

from __future__ import annotations

import json
import os
import random
from datetime import datetime, timezone
from typing import Any

import structlog

from nexus.agents.modules.community_vibe import (
    assign_personas,
    classify_community,
    compose_chatter_line,
    refresh_emerging_topic,
)
from nexus.agents.task_registry import registry

log = structlog.get_logger(__name__)

SWARM_STATE_PREFIX = "nexus:swarm:warmer:state:"
SWARM_COMMUNITY_PREFIX = "nexus:swarm:community:"
SWARM_LOCK_PREFIX = "nexus:swarm:warmer:lock:"

CLASSIFY_INTERVAL_S = 86400
DAY_START_H = 7
DAY_END_H = 22


def _next_interval_seconds(tz_name: str, engagement_mode: str = "") -> int:
    """
    Default: random 5–45 minutes, scaled by local day/night activity wave.

    ``high`` / ``high_engagement`` / ``high-engagement``: AI chatter every 10–20 minutes
    (fixed band, ignores day/night stretch).
    """
    em = (engagement_mode or "").strip().lower().replace("-", "_")
    if em in ("high", "high_engagement"):
        return random.randint(600, 1200)
    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo(tz_name.strip() or "UTC")
    except Exception:
        tz = timezone.utc
    now = datetime.now(tz)
    hour = now.hour
    base = random.randint(300, 2700)
    if DAY_START_H <= hour < DAY_END_H:
        mult = random.uniform(0.5, 1.0)
    else:
        mult = random.uniform(1.1, 2.5)
    return max(120, int(base * mult))


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


def _resolve_api_key(parameters: dict[str, Any]) -> str:
    secrets = parameters.get("__secrets__", {})
    return (
        str(parameters.get("gemini_api_key", "")).strip()
        or secrets.get("GEMINI_API_KEY", "")
        or os.getenv("GEMINI_API_KEY", "")
    )


def _resolve_telethon_creds(parameters: dict[str, Any]) -> tuple[int, str]:
    sec = parameters.get("__secrets__", {})
    api_id = int(sec.get("TELEFIX_API_ID") or os.getenv("TELEFIX_API_ID", "0") or "0")
    api_hash = str(sec.get("TELEFIX_API_HASH") or os.getenv("TELEFIX_API_HASH", "") or "")
    return api_id, api_hash


def _format_transcript_from_messages(
    messages: list[Any],
) -> tuple[str, list[dict[str, Any]], str]:
    """Transcript (oldest→newest), id map (newest first from Telethon), tail string."""
    lines: list[str] = []
    id_map: list[dict[str, Any]] = []
    for m in messages:
        if not m or not getattr(m, "id", None):
            continue
        uid = getattr(m, "sender_id", None)
        uname = ""
        if getattr(m, "sender", None):
            uname = getattr(m.sender, "username", "") or ""
        label = f"@{uname}" if uname else f"user:{uid}"
        text = (getattr(m, "message", None) or "").replace("\n", " ").strip()
        if not text:
            continue
        lines.append(f"{label}: {text}")
        id_map.append({"id": int(m.id), "sender": label})
    chronological = list(reversed(lines))
    transcript = "\n".join(chronological[-80:])
    tail = "\n".join(chronological[-40:])
    return transcript, id_map, tail


async def _try_edit_group_about(client: Any, entity: Any, about: str) -> bool:
    if not about or len(about) < 3:
        return False
    about = about[:255]
    try:
        from telethon.tl import functions  # type: ignore[import-untyped]

        if hasattr(entity, "megagroup") or getattr(entity, "broadcast", False):
            inp = await client.get_input_entity(entity)
            await client(functions.channels.EditAboutRequest(channel=inp, about=about))
            return True
        chat_id = int(getattr(entity, "id", 0))
        if chat_id:
            await client(functions.messages.EditChatAboutRequest(chat_id=chat_id, about=about))
            return True
    except Exception as exc:
        log.debug("edit_group_about_skipped", error=str(exc))
    return False


@registry.register("swarm.group_warmer")
async def group_warmer(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    One tick: optionally reclassify (24h), refresh topic, send one AI line from a
    rotating session, persist ``next_run_at`` for the master scheduler.

    Parameters
    ----------
    group_key    : str — Redis segment (e.g. str(telegram supergroup id))
    group_id     : int — Telegram entity id
    sessions     : list of {"session_path": str, "username": optional}
    timezone     : IANA tz for activity waves (default UTC)
    action       : "tick" | "classify_only"
    group_title  : optional hint when entity title missing
    """
    redis = parameters.get("__redis__")
    group_key = str(parameters.get("group_key", "")).strip()
    group_id = parameters.get("group_id")
    sessions: list[dict[str, Any]] = list(parameters.get("sessions") or [])
    tz_name = str(parameters.get("timezone", "UTC") or "UTC")
    action = str(parameters.get("action", "tick")).strip().lower()
    group_title = str(parameters.get("group_title", "") or "Telegram group")
    engagement_mode = str(parameters.get("engagement_mode", "") or "")

    lock_key = f"{SWARM_LOCK_PREFIX}{group_key}" if group_key else ""

    async def release_lock() -> None:
        if redis and lock_key:
            try:
                await redis.delete(lock_key)
            except Exception:
                pass

    try:
        if not group_key or group_id is None:
            return {"status": "failed", "error": "group_key and group_id required"}
        if not sessions:
            return {"status": "failed", "error": "sessions list required"}

        api_key = _resolve_api_key(parameters)
        api_id, api_hash = _resolve_telethon_creds(parameters)
        if not api_key:
            return {"status": "failed", "error": "GEMINI_API_KEY missing"}
        if not api_id or not api_hash:
            return {"status": "failed", "error": "TELEFIX_API_ID / TELEFIX_API_HASH missing"}

        state_key = f"{SWARM_STATE_PREFIX}{group_key}"
        community_key = f"{SWARM_COMMUNITY_PREFIX}{group_key}"
        state = await _redis_json_get(redis, state_key)

        try:
            from telethon import TelegramClient  # type: ignore[import-untyped]
        except ImportError:
            return {"status": "failed", "error": "telethon not installed"}

        reader_path = str(sessions[0].get("session_path", "")).strip()
        if not reader_path:
            return {"status": "failed", "error": "session_path empty"}

        async with TelegramClient(reader_path, api_id, api_hash) as client:
            entity = await client.get_entity(int(group_id))
            title = getattr(entity, "title", None) or group_title
            msgs = await client.get_messages(entity, limit=45)
            transcript, id_map, tail_text = _format_transcript_from_messages(
                [m for m in msgs if m],
            )

            now = datetime.now(timezone.utc)
            last_classify = state.get("last_classify_at")
            need_classify = action == "classify_only"
            if not need_classify and last_classify:
                try:
                    prev = datetime.fromisoformat(str(last_classify).replace("Z", "+00:00"))
                    if (now - prev).total_seconds() >= CLASSIFY_INTERVAL_S:
                        need_classify = True
                except Exception:
                    need_classify = True
            elif not last_classify:
                need_classify = True

            if need_classify:
                vibe = await classify_community(api_key, transcript or tail_text, title)
                community = {
                    "community_identity": str(vibe.get("community_identity", ""))[:120],
                    "group_description": str(vibe.get("group_description", ""))[:255],
                    "emerging_identity": str(vibe.get("emerging_identity", ""))[:2000],
                    "updated_at": _iso_now(),
                    "group_key": group_key,
                    "group_id": int(group_id),
                }
                await _redis_json_set(redis, community_key, community, ex=86400 * 14)
                state["last_classify_at"] = _iso_now()
                state["emerging_identity"] = community.get("emerging_identity") or state.get(
                    "emerging_identity", ""
                )
                desc = community.get("group_description", "")
                if desc:
                    await _try_edit_group_about(client, entity, desc)
                if action == "classify_only":
                    state["next_run_at"] = _iso_now()
                    await _redis_json_set(redis, state_key, state, ex=86400 * 30)
                    return {"status": "completed", "phase": "classify_only", "community": community}

            accounts = [
                {
                    "session_path": str(s.get("session_path", "")).strip(),
                    "username": str(s.get("username", "")),
                }
                for s in sessions
            ]
            personas: list[dict[str, Any]] = list(state.get("personas") or [])
            if len(personas) != len(accounts):
                personas = await assign_personas(api_key, accounts, transcript[-2000:] or title)
                state["personas"] = personas

            if not personas:
                return {"status": "failed", "error": "no personas"}

            prior_identity = str(state.get("emerging_identity", ""))
            topic_pack = await refresh_emerging_topic(api_key, transcript, prior_identity, title)
            state["emerging_identity"] = str(topic_pack.get("emerging_identity", prior_identity))
            topic = str(topic_pack.get("discussion_topic", "ongoing thread"))
            hooks = topic_pack.get("in_universe_hooks") or []
            if isinstance(hooks, str):
                hooks = [hooks]
            hooks = [str(h) for h in hooks][:5]

            rot = int(state.get("rotation_index", 0)) % len(personas)
            state["rotation_index"] = (rot + 1) % len(personas)
            speaker = personas[rot]
            session_path = str(speaker.get("session_path") or accounts[rot].get("session_path", "")).strip()
            if not session_path:
                session_path = reader_path

            handles = []
            for p in personas:
                u = str(p.get("username", "")).lstrip("@")
                if u:
                    handles.append(u)
            sp_u = str(speaker.get("username", "")).lstrip("@")
            other_handles = [h for h in handles if h != sp_u]

            line = await compose_chatter_line(
                api_key,
                emerging_identity=state["emerging_identity"],
                topic=topic,
                hooks=hooks,
                transcript=transcript,
                speaker=speaker,
                other_handles=[f"@{h}" for h in other_handles],
                message_index_map=id_map,
            )
            text = str(line.get("text", "")).strip()
            reply_id = line.get("reply_to_id")
            if reply_id is not None:
                try:
                    reply_id = int(reply_id)
                except Exception:
                    reply_id = None
            valid_ids = {m["id"] for m in id_map}
            if reply_id not in valid_ids:
                reply_id = None

            message_id = None
            if text and action == "tick":
                async with TelegramClient(session_path, api_id, api_hash) as poster:
                    post_entity = await poster.get_entity(int(group_id))
                    sent = await poster.send_message(
                        post_entity,
                        text,
                        reply_to=reply_id if reply_id else None,
                    )
                    message_id = int(sent.id) if sent else None

            delay_s = _next_interval_seconds(tz_name, engagement_mode)
            next_run = datetime.now(timezone.utc).timestamp() + delay_s
            state["next_run_at"] = datetime.fromtimestamp(next_run, tz=timezone.utc).isoformat()
            state["last_topic"] = topic
            state["transcript_tail"] = tail_text[-4000:]
            await _redis_json_set(redis, state_key, state, ex=86400 * 30)

            return {
                "status": "completed",
                "phase": "tick",
                "message_id": message_id,
                "speaker_archetype": speaker.get("archetype"),
                "next_delay_s": delay_s,
                "topic": topic,
            }
    finally:
        await release_lock()
