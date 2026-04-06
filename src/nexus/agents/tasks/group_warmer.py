"""
swarm.group_warmer — AI-driven multi-session group chatter + community classification.

Uses Gemini 1.5 Flash (via ``nexus.agents.modules.community_vibe``) for personas,
topics, and lines. Telethon delivers messages. Variable delays and day/night
activity waves are applied when computing ``next_run_at`` (see scheduler).
"""

from __future__ import annotations

import asyncio
import json
import os
import random
from datetime import datetime, timezone
from io import BytesIO
from typing import Any

import structlog
from pathlib import Path

from nexus.agents.modules.community_vibe import (
    assign_personas,
    classify_community,
    compose_chatter_line,
    refresh_emerging_topic,
)
from nexus.services.media_opsec import make_image_upload_salt_seed, prepare_jpeg_png_for_telegram_upload
from nexus.services.recent_news_digest import (
    TickNewsBundle,
    append_article_link_to_text,
    download_image_bytes,
    get_tick_news_bundle_for_consumer,
    telegram_image_filename_from_bytes,
)
from nexus.services.tg_message_text import llm_media_prefix_for_message, telethon_display_text
from nexus.services.tg_participant_privilege import sender_of_message_is_owner_or_admin
from nexus.agents.task_registry import registry

log = structlog.get_logger(__name__)

SWARM_STATE_PREFIX = "nexus:swarm:warmer:state:"
SWARM_COMMUNITY_PREFIX = "nexus:swarm:community:"
SWARM_LOCK_PREFIX = "nexus:swarm:warmer:lock:"

CLASSIFY_INTERVAL_S = 86400
DAY_START_H = 7
DAY_END_H = 22


def _norm_engagement_mode(mode: str) -> str:
    return (mode or "").strip().lower().replace("-", "_")


def _next_interval_seconds(tz_name: str, engagement_mode: str = "") -> int:
    """
    Time until the *next scheduler tick* for this group (after all turns in the
    current job complete).

    Defaults are shorter than legacy (roughly 1–20+ min, day/night scaled).
    ``slow`` / ``organic`` / ``legacy``: old ~5–45+ minute bands.

    ``max`` / ``maximum`` / ``aggressive``: ~1.5–4 min between bursts.
    ``conversation`` / ``burst`` / ``multi`` / ``simultaneous``: ~2–6 min.
    ``high`` / ``high_engagement``: ~4–9 min (fixed, no day/night stretch).
    """
    em = _norm_engagement_mode(engagement_mode)
    if em in ("max", "maximum", "aggressive"):
        return random.randint(90, 240)
    if em in ("conversation", "burst", "multi", "simultaneous"):
        return random.randint(120, 360)
    if em in ("high", "high_engagement"):
        return random.randint(240, 540)
    if em in ("slow", "organic", "legacy"):
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
    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo(tz_name.strip() or "UTC")
    except Exception:
        tz = timezone.utc
    now = datetime.now(tz)
    hour = now.hour
    base = random.randint(90, 1200)
    if DAY_START_H <= hour < DAY_END_H:
        mult = random.uniform(0.55, 1.0)
    else:
        mult = random.uniform(1.0, 1.75)
    return max(60, int(base * mult))


def _effective_turns_per_tick(parameters: dict[str, Any], engagement_mode: str, n_personas: int) -> int:
    raw = parameters.get("turns_per_tick")
    if raw is not None and str(raw).strip() != "":
        try:
            return max(1, min(8, int(raw)))
        except (TypeError, ValueError):
            pass
    em = _norm_engagement_mode(engagement_mode)
    if em in ("conversation", "burst", "multi", "simultaneous"):
        return max(2, min(5, n_personas)) if n_personas > 1 else 1
    return 1


def _intra_turn_delay_bounds(engagement_mode: str, parameters: dict[str, Any]) -> tuple[float, float]:
    lo_raw, hi_raw = parameters.get("intra_turn_min_s"), parameters.get("intra_turn_max_s")
    if lo_raw is not None and hi_raw is not None:
        try:
            lo, hi = float(lo_raw), float(hi_raw)
            lo = max(5.0, lo)
            hi = max(lo, hi)
            return (lo, hi)
        except (TypeError, ValueError):
            pass
    em = _norm_engagement_mode(engagement_mode)
    if em in ("conversation", "burst", "multi", "simultaneous", "max", "maximum", "aggressive"):
        return (12.0, 55.0)
    return (20.0, 85.0)


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
        raw_msg = (getattr(m, "message", None) or getattr(m, "raw_text", None) or "") or ""
        body = str(raw_msg).strip().replace("\n", " ")
        prefix = llm_media_prefix_for_message(m)
        if body:
            line_text = f"{prefix}{body}".strip() if prefix else body
        else:
            line_text = (prefix.strip() if prefix else telethon_display_text(m).replace("\n", " ").strip())
        if not line_text:
            continue
        lines.append(f"{label}: {line_text}")
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
    One tick: optionally reclassify (24h), refresh topic, then one or more AI lines
    from rotating sessions (multi-turn “conversation” when configured), persist
    ``next_run_at`` for the master scheduler.

    Parameters
    ----------
    group_key    : str — Redis segment (e.g. str(telegram supergroup id))
    group_id     : int — Telegram entity id
    sessions     : list of {"session_path": str, "username": optional}
    timezone     : IANA tz for activity waves (default UTC)
    action       : "tick" | "classify_only"
    group_title  : optional hint when entity title missing
    engagement_mode : e.g. ``conversation`` (multi-speaker default), ``max``, ``high``, ``slow``
    turns_per_tick : optional int 1–8; overrides auto turn count for conversation modes
    intra_turn_min_s / intra_turn_max_s : delay between lines in the same job (seconds)
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

            handles = []
            for p in personas:
                u = str(p.get("username", "")).lstrip("@")
                if u:
                    handles.append(u)

            turns = _effective_turns_per_tick(parameters, engagement_mode, len(personas))
            intra_lo, intra_hi = _intra_turn_delay_bounds(engagement_mode, parameters)
            try:
                news_bundle = await get_tick_news_bundle_for_consumer(redis)
            except Exception as exc:
                log.warning("warmer_news_bundle_failed", error=str(exc))
                news_bundle = TickNewsBundle(
                    digest_text="",
                    anchor_title="",
                    anchor_link="",
                    image_url=None,
                )
            message_ids: list[int | None] = []
            sent_media = False

            for turn_i in range(turns):
                if turn_i > 0:
                    await asyncio.sleep(random.uniform(intra_lo, intra_hi))
                    msgs = await client.get_messages(entity, limit=45)
                    transcript, id_map, tail_text = _format_transcript_from_messages(
                        [m for m in msgs if m],
                    )

                rot = int(state.get("rotation_index", 0)) % len(personas)
                state["rotation_index"] = (rot + 1) % len(personas)
                speaker = personas[rot]
                session_path = str(speaker.get("session_path") or accounts[rot].get("session_path", "")).strip()
                if not session_path:
                    session_path = reader_path

                sp_u = str(speaker.get("username", "")).lstrip("@")
                other_handles = [h for h in handles if h != sp_u]

                nd = news_bundle.digest_text if turn_i == 0 else ""
                ah = news_bundle.anchor_title if turn_i == 0 else ""
                line = await compose_chatter_line(
                    api_key,
                    emerging_identity=state["emerging_identity"],
                    topic=topic,
                    hooks=hooks,
                    transcript=transcript,
                    speaker=speaker,
                    other_handles=[f"@{h}" for h in other_handles],
                    message_index_map=id_map,
                    news_digest=nd,
                    anchor_headline=ah,
                )
                text = str(line.get("text", "")).strip()
                text, link_parse_mode = append_article_link_to_text(
                    text,
                    (news_bundle.anchor_link or "").strip(),
                    title=(ah or None),
                )
                reply_id = line.get("reply_to_id")
                if reply_id is not None:
                    try:
                        reply_id = int(reply_id)
                    except Exception:
                        reply_id = None
                valid_ids = {m["id"] for m in id_map}
                if reply_id not in valid_ids:
                    reply_id = None
                elif reply_id is not None:
                    try:
                        if await sender_of_message_is_owner_or_admin(client, entity, reply_id):
                            line = await compose_chatter_line(
                                api_key,
                                emerging_identity=state["emerging_identity"],
                                topic=topic,
                                hooks=hooks,
                                transcript=transcript,
                                speaker=speaker,
                                other_handles=[f"@{h}" for h in other_handles],
                                message_index_map=id_map,
                                news_digest=nd,
                                anchor_headline=ah,
                                privileged_reply_target=True,
                                forced_reply_to_id=reply_id,
                            )
                            text = str(line.get("text", "")).strip()
                            text, link_parse_mode = append_article_link_to_text(
                                text,
                                (news_bundle.anchor_link or "").strip(),
                                title=(ah or None),
                            )
                            new_r = line.get("reply_to_id")
                            try:
                                new_r = int(new_r) if new_r is not None else reply_id
                            except Exception:
                                new_r = reply_id
                            reply_id = new_r if new_r in valid_ids else reply_id
                    except Exception as exc:
                        log.debug("warmer_privileged_reply_regen_failed", error=str(exc))

                message_id: int | None = None
                if text and action == "tick":
                    photo_bytes: bytes | None = None
                    if turn_i == 0 and news_bundle.image_url:
                        photo_bytes = await download_image_bytes(news_bundle.image_url)
                        if photo_bytes is None:
                            log.debug(
                                "warmer_image_download_empty",
                                url=(news_bundle.image_url or "")[:160],
                            )
                    async with TelegramClient(session_path, api_id, api_hash) as poster:
                        post_entity = await poster.get_entity(int(group_id))
                        try:
                            if photo_bytes:
                                salt = make_image_upload_salt_seed(Path(session_path).stem)
                                photo_bytes, _ = prepare_jpeg_png_for_telegram_upload(
                                    photo_bytes, salt_seed=salt
                                )
                                fname = telegram_image_filename_from_bytes(photo_bytes)
                                bio = BytesIO(photo_bytes)
                                try:
                                    sent = await poster.send_file(
                                        post_entity,
                                        file=(fname, bio),
                                        caption=text[:1024],
                                        reply_to=reply_id if reply_id else None,
                                        force_document=False,
                                        parse_mode=link_parse_mode,
                                    )
                                    sent_media = True
                                except Exception as photo_exc:
                                    log.warning("warmer_send_file_failed", error=str(photo_exc))
                                    sent = await poster.send_message(
                                        post_entity,
                                        text,
                                        reply_to=reply_id if reply_id else None,
                                        parse_mode=link_parse_mode,
                                    )
                                    message_id = int(sent.id) if sent else None
                                else:
                                    message_id = int(sent.id) if sent else None
                            else:
                                sent = await poster.send_message(
                                    post_entity,
                                    text,
                                    reply_to=reply_id if reply_id else None,
                                    parse_mode=link_parse_mode,
                                )
                                message_id = int(sent.id) if sent else None
                        except Exception as exc:
                            log.warning("warmer_send_failed", error=str(exc))
                            try:
                                sent = await poster.send_message(
                                    post_entity,
                                    text,
                                    reply_to=reply_id if reply_id else None,
                                    parse_mode=link_parse_mode,
                                )
                                message_id = int(sent.id) if sent else None
                            except Exception as exc2:
                                log.warning("warmer_send_fallback_failed", error=str(exc2))
                message_ids.append(message_id)

            message_id = message_ids[0] if message_ids else None

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
                "message_ids": message_ids,
                "turns": turns,
                "speaker_archetype": speaker.get("archetype"),
                "next_delay_s": delay_s,
                "topic": topic,
                "sent_media": sent_media,
                "news_anchor": news_bundle.anchor_title[:200] if news_bundle.anchor_title else "",
            }
    finally:
        await release_lock()
