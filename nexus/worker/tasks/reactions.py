"""
Passive Telegram reactions for swarm sessions — native emoji reactions via Telethon.

Used on news-wake cycles so most dormant accounts only react (no LLM text), matching
real groups where reactions outnumber messages.
"""

from __future__ import annotations

import asyncio
import hashlib
import random
from typing import Any

import structlog

from nexus.services.tg_participant_privilege import sender_of_message_is_owner_or_admin
from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

# Must match `len(PERSONA_ARCHETYPES)` in swarm.community_factory (12 archetypes).
_N_ARCHETYPES = 12

# Archetype indices (0..11) aligned with PERSONA_ARCHETYPES in swarm.py
_BOOMERISH_ARCHETYPE_INDICES = frozenset({1, 2, 7, 8, 9, 11})
_CYNIC_ARCHETYPE_INDICES = frozenset({0, 3, 5, 6})
_ANGRY_ARCHETYPE_INDICES = frozenset({4, 10})

_BOOMER_EMOJIS = ["👍", "🙏", "🤝"]
_CYNIC_EMOJIS = ["🤡", "🤣", "💩"]
_ANGRY_EMOJIS = ["🤬", "👎", "🤮"]

_RECENT_MESSAGE_LIMIT = 15
_MAX_EXISTING_REACTIONS = 3


def _archetype_index_for_session(session_base: str) -> int:
    raw = (session_base or "default").encode("utf-8", errors="ignore")
    d = hashlib.md5(raw).digest()
    return int.from_bytes(d[0:2], "big") % _N_ARCHETYPES


def reaction_emoji_for_session(session_base: str) -> str:
    """Map stable session → emoji pool (Boomer / Cynic–troll / Angry–political)."""
    ai = _archetype_index_for_session(session_base)
    if ai in _BOOMERISH_ARCHETYPE_INDICES:
        pool = _BOOMER_EMOJIS
    elif ai in _CYNIC_ARCHETYPE_INDICES:
        pool = _CYNIC_EMOJIS
    elif ai in _ANGRY_ARCHETYPE_INDICES:
        pool = _ANGRY_EMOJIS
    else:
        pool = _BOOMER_EMOJIS
    return random.choice(pool)


def total_reaction_count_on_message(message: Any) -> int:
    r = getattr(message, "reactions", None)
    if r is None:
        return 0
    results = getattr(r, "results", None) or []
    return sum(int(getattr(x, "count", 0) or 0) for x in results)


async def send_passive_group_reaction(client: Any, entity: Any, session_base: str) -> bool:
    """
    Jitter, then react to one recent non-admin message with ≤3 existing reactions.
    ``get_messages(..., limit=15)`` is newest-first; candidates are shuffled for variety.
    """
    await asyncio.sleep(random.randint(10, 120))

    from telethon.tl.functions.messages import SendReactionRequest  # type: ignore[import-untyped]
    from telethon.tl.types import ReactionEmoji  # type: ignore[import-untyped]

    try:
        hist = await client.get_messages(entity, limit=_RECENT_MESSAGE_LIMIT)
    except Exception as exc:
        log.debug("passive_reaction_history_failed", error=str(exc))
        return False

    msgs = [m for m in (hist or []) if m is not None and getattr(m, "id", None)]
    if not msgs:
        return False

    me = None
    try:
        me = await client.get_me()
    except Exception:
        me = None
    my_id = int(getattr(me, "id", 0) or 0) if me is not None else 0

    random.shuffle(msgs)
    emo = reaction_emoji_for_session(session_base)

    for m in msgs:
        mid = int(getattr(m, "id", 0) or 0)
        if mid <= 0:
            continue
        if total_reaction_count_on_message(m) > _MAX_EXISTING_REACTIONS:
            continue
        sid = getattr(m, "sender_id", None)
        if my_id and sid is not None and int(sid) == my_id:
            continue
        try:
            if await sender_of_message_is_owner_or_admin(client, entity, mid):
                continue
        except Exception:
            continue
        try:
            await client(
                SendReactionRequest(
                    peer=entity,
                    msg_id=mid,
                    reaction=[ReactionEmoji(emoticon=emo)],
                )
            )
            log.info(
                "passive_reaction_sent",
                msg_id=mid,
                emoji=emo,
                session_hint=(session_base or "")[:32],
            )
            return True
        except Exception as exc:
            log.debug("passive_reaction_send_failed", msg_id=mid, error=str(exc))
            continue

    log.debug("passive_reaction_no_eligible_message")
    return False


@registry.register("swarm.passive_reaction")
async def swarm_passive_reaction_task(parameters: dict[str, Any]) -> dict[str, Any]:
    """Optional ARQ entry: one session reacts in one group (same rules as news-wake passive path)."""
    from nexus.worker.services.tg_session import async_telegram_client

    session_path = str(parameters.get("session_path", "") or parameters.get("session_base", "") or "").strip()
    group_id = parameters.get("group_id")
    if not session_path or group_id is None:
        return {"status": "failed", "error": "session_path and group_id required"}

    gid_int = int(group_id)
    try:
        async with async_telegram_client(session_path, parameters) as client:
            if not await client.is_user_authorized():
                return {"status": "failed", "error": "unauthorized"}
            ent = await client.get_entity(gid_int)
            ok = await send_passive_group_reaction(client, ent, session_path)
    except ValueError as exc:
        return {"status": "failed", "error": str(exc)}
    except Exception as exc:
        log.warning("swarm_passive_reaction_task_failed", error=str(exc))
        return {"status": "failed", "error": str(exc)}

    return {"status": "completed", "reaction_sent": ok, "group_id": gid_int}
