"""
swarm.community_factory — Israeli Community Factory: role split, group creation,
distributed joins with FloodWait / ban handling, and LLM-driven Hebrew chatter.

Redis namespace: nexus:swarm:factory:*
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import structlog

from nexus.worker.services.tg_session import (
    async_telegram_client,
    classify_telethon_account_error,
    flood_wait_seconds,
    resolve_telethon_creds,
)
from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

KEY_ROLES = "nexus:swarm:factory:roles"
KEY_GROUPS = "nexus:swarm:factory:groups"
KEY_STATE = "nexus:swarm:factory:state"
KEY_BANNED = "nexus:swarm:factory:banned"
KEY_COOLDOWNS = "nexus:swarm:factory:cooldowns"
KEY_METRICS = "nexus:swarm:factory:metrics"
KEY_PROFILE_GATE = "nexus:swarm:factory:profile_gate"
THREAD_KEY_PREFIX = "nexus:swarm:factory:thread:"
THREAD_ID_CAP = 5

GROUPS_TARGET_PER_OWNER = 20
REACTION_EMOJIS = ["🔥", "😂", "💀", "🤯", "👀", "😱", "💪", "🤦", "😅", "❤️", "🙏"]
REACTION_TL_EMOJIS = ["👍", "❤️", "🔥", "🙏", "😂", "👀"]
THREAD_REACTION_EMOJIS = ["👍", "🤦‍♂️", "🤬"]

ISRAELI_DISPLAY_NAMES = [
    "Yossi",
    "Avi C.",
    "Rotem",
    "Shir",
    "Kobi_88",
    "David",
    "Noam",
    "Tomer",
    "Lior_77",
    "Maya S.",
    "Guy",
    "Dana",
    "Itai",
    "Omer",
    "Nir",
]

ISRAELI_NEWS_SYSTEM_PROMPT = (
    "You are an Israeli Telegram user in a local news/politics group. \n"
    "RULES:\n"
    "1. Write in extremely casual, modern colloquial Hebrew slang (e.g., 'אמאלה', 'הזייה', 'אין מצב', 'אחי', 'וואלה', 'פיגוע פלילי'). \n"
    "2. NEVER use hashtags (#). Real people don't use them in chats.\n"
    "3. Keep it short (1-10 words). People type fast on phones.\n"
    "4. Occasional minor Hebrew typos are encouraged to simulate human typing.\n"
    "5. Do not sound poetic, formal, or like a translated article. Express cynical, stressed, or typical Israeli attitudes towards news."
)

FACTORY_TOPICS = [
    "קריפטו ומטבעות דיגיטליים — ביטקוין, אלטקוינים, בורסות",
    "ישראל–איראן — מתח אזורי וגיאופוליטיקה",
    "פוליטיקה ישראלית — קואליציה, משפט, מחאות",
    "כלכלה בישראל — דיור, מחירים, ריבית",
    "קניות אונליין — משלוחים, מבצעים, אתרים",
    "יד שנייה — יד2, מרקטפלייס, טיפים לקנייה",
]

def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve_sessions_dir(explicit: str | None) -> Path:
    if explicit and explicit.strip():
        p = Path(explicit).expanduser()
        return p.resolve()
    env = os.getenv("VAULT_SESSIONS_DIR", "").strip()
    if env:
        return Path(env).expanduser().resolve()
    return (_project_root() / "vault" / "sessions").resolve()


def _discover_session_bases(sessions_dir: Path) -> list[str]:
    if not sessions_dir.is_dir():
        return []
    files = sorted(sessions_dir.glob("*.session"), key=lambda p: p.as_posix().lower())
    return [str(p.with_suffix("").resolve()) for p in files]


def _split_roles(bases: list[str]) -> tuple[list[str], list[str]]:
    """
    ~3% owners, remainder members. Uses rounding so large pools track nominal 3%;
    with very few sessions, at least one owner is kept (may exceed 3% until n grows).
    """
    n = len(bases)
    if n == 0:
        return [], []
    owner_count = max(1, round(n * 0.03))
    owner_count = min(owner_count, n)
    owners = bases[:owner_count]
    members = bases[owner_count:]
    return owners, members


def _default_metrics() -> dict[str, Any]:
    return {
        "messages_sent": 0,
        "flood_waits": 0,
        "bans": 0,
        "joins_ok": 0,
        "joins_failed": 0,
        "join_attempts": 0,
        "groups_total": 0,
        "active_sessions": 0,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _default_state(sessions_dir: str) -> dict[str, Any]:
    return {
        "phase": "idle",
        "sessions_dir": sessions_dir,
        "creation_index": 0,
        "join_flat_idx": 0,
        "converse_idx": 0,
        "groups_per_owner_target": GROUPS_TARGET_PER_OWNER,
        "init_phases": [],
        "chat_enabled": False,
    }


async def _redis_json_get(redis: Any, key: str) -> Any:
    if redis is None:
        return None
    raw = await redis.get(key)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


async def _redis_json_set(redis: Any, key: str, data: Any) -> None:
    if redis is None:
        return
    await redis.set(key, json.dumps(data, ensure_ascii=False))


def _resolve_api_key(parameters: dict[str, Any]) -> str:
    secrets = parameters.get("__secrets__", {})
    return (
        str(parameters.get("gemini_api_key", "")).strip()
        or secrets.get("GEMINI_API_KEY", "")
        or os.getenv("GEMINI_API_KEY", "")
    )


def _resolve_openai_key(parameters: dict[str, Any]) -> str:
    secrets = parameters.get("__secrets__", {})
    return (
        str(parameters.get("openai_api_key", "")).strip()
        or secrets.get("OPENAI_API_KEY", "")
        or os.getenv("OPENAI_API_KEY", "")
    )


def _invite_hash(link_or_hash: str) -> str:
    s = (link_or_hash or "").strip()
    if "/+" in s:
        return s.split("/+")[-1].split("?")[0].strip()
    if "joinchat/" in s.lower():
        return s.split("joinchat/")[-1].split("?")[0].strip()
    return s.lstrip("+")


async def _try_peer_reaction(client: Any, entity: Any) -> bool:
    from telethon.tl.functions.messages import SendReactionRequest  # type: ignore[import-untyped]
    from telethon.tl.types import ReactionEmoji  # type: ignore[import-untyped]

    me = await client.get_me()
    my_id = getattr(me, "id", None)
    messages = await client.get_messages(entity, limit=40)
    candidates: list[Any] = []
    for m in messages:
        if not m or not getattr(m, "id", None):
            continue
        sid = getattr(m, "sender_id", None)
        if sid is None or sid == my_id:
            continue
        candidates.append(m)
    if not candidates:
        return False
    msg = random.choice(candidates)
    emo = random.choice(REACTION_TL_EMOJIS)
    await client(
        SendReactionRequest(
            peer=entity,
            msg_id=int(msg.id),
            reaction=[ReactionEmoji(emoticon=emo)],
        )
    )
    return True


async def _try_send_pack_sticker(client: Any, entity: Any) -> bool:
    from telethon.tl.functions.messages import GetStickerSetRequest  # type: ignore[import-untyped]
    from telethon.tl.types import InputStickerSetShortName  # type: ignore[import-untyped]

    short = (os.getenv("COMMUNITY_FACTORY_STICKER_SET", "AnimatedEmojies") or "").strip()
    if not short:
        return False
    try:
        res = await client(
            GetStickerSetRequest(
                stickerset=InputStickerSetShortName(short_name=short),
                hash=0,
            )
        )
        docs = [d for d in (getattr(res, "documents", None) or []) if d]
        if not docs:
            return False
        await client.send_file(entity, random.choice(docs))
        return True
    except Exception as exc:
        log.debug("factory_sticker_skipped", error=str(exc))
        return False


async def _mark_banned(redis: Any, session_base: str) -> None:
    if redis is None:
        return
    raw = await _redis_json_get(redis, KEY_BANNED)
    banned: list[str] = list(raw) if isinstance(raw, list) else []
    stem = session_base
    if stem not in banned:
        banned.append(stem)
    await _redis_json_set(redis, KEY_BANNED, banned)


async def _is_session_banned(redis: Any, session_base: str) -> bool:
    raw = await _redis_json_get(redis, KEY_BANNED)
    if not isinstance(raw, list):
        return False
    return session_base in raw


async def _cooldown_until(redis: Any, session_base: str) -> datetime | None:
    raw = await _redis_json_get(redis, KEY_COOLDOWNS)
    if not isinstance(raw, dict):
        return None
    iso = raw.get(session_base)
    if not iso:
        return None
    try:
        return datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
    except Exception:
        return None


async def _set_cooldown(redis: Any, session_base: str, seconds: int) -> None:
    if redis is None:
        return
    raw = await _redis_json_get(redis, KEY_COOLDOWNS)
    cd: dict[str, str] = dict(raw) if isinstance(raw, dict) else {}
    until = datetime.now(timezone.utc).timestamp() + seconds
    cd[session_base] = datetime.fromtimestamp(until, tz=timezone.utc).isoformat()
    await _redis_json_set(redis, KEY_COOLDOWNS, cd)


async def _bump_metric(redis: Any, field: str, delta: int = 1) -> None:
    if redis is None:
        return
    m = await _redis_json_get(redis, KEY_METRICS)
    if not isinstance(m, dict):
        m = _default_metrics()
    m[field] = int(m.get(field, 0)) + delta
    m["updated_at"] = datetime.now(timezone.utc).isoformat()
    await _redis_json_set(redis, KEY_METRICS, m)


async def _sync_active_sessions(redis: Any, all_bases: list[str]) -> None:
    banned_raw = await _redis_json_get(redis, KEY_BANNED)
    banned_n = len(banned_raw) if isinstance(banned_raw, list) else 0
    active = max(0, len(all_bases) - banned_n)
    m = await _redis_json_get(redis, KEY_METRICS)
    if not isinstance(m, dict):
        m = _default_metrics()
    m["active_sessions"] = active
    m["updated_at"] = datetime.now(timezone.utc).isoformat()
    await _redis_json_set(redis, KEY_METRICS, m)


async def _enqueue_task(task_type: str, parameters: dict[str, Any]) -> bool:
    try:
        import arq
        from arq.connections import RedisSettings

        from nexus.shared.config import settings
        from nexus.shared.schemas import TaskPayload

        task = TaskPayload(
            task_type=task_type,
            parameters=parameters,
            project_id="community-factory",
            priority=3,
            job_expires_seconds=600,
        )
        job_ttl = int(task.job_expires_seconds or int(os.getenv("TASK_DEFAULT_TIMEOUT", "300")))
        arq_pool = await arq.create_pool(
            RedisSettings.from_dsn(settings.redis_url),
            default_queue_name="nexus:tasks",
        )
        await arq_pool.enqueue_job(
            "execute_task",
            task_payload=task.model_dump_for_wire(),
            _job_id=str(uuid.uuid4()),
            _queue_name="nexus:tasks",
            _expires=job_ttl,
        )
        await arq_pool.aclose()
        return True
    except Exception as exc:
        log.error("community_factory_enqueue_failed", task_type=task_type, error=str(exc))
        return False


async def _generate_hebrew_line(api_key: str, topic: str, openai_key: str) -> str:
    if api_key:
        try:
            from nexus.modules.community_vibe import _gemini_json  # type: ignore[attr-defined]

            user = f'נושא לשיחה: "{topic}". כתוב משפט אחד או שניים בלבד.'
            out = await _gemini_json(api_key, _GEMINI_SYSTEM, user, temperature=0.9, max_tokens=256)
            text = str(out.get("text", "")).strip()
            if text:
                return text
        except Exception as exc:
            log.warning("factory_gemini_failed", error=str(exc))
    if openai_key:
        try:
            import httpx

            url = "https://api.openai.com/v1/chat/completions"
            headers = {"Authorization": f"Bearer {openai_key}"}
            payload = {
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": _GEMINI_SYSTEM},
                    {"role": "user", "content": f'נושא: "{topic}"'},
                ],
                "temperature": 0.9,
                "max_tokens": 200,
            }
            async with httpx.AsyncClient(timeout=60.0) as client:
                r = await client.post(url, json=payload, headers=headers)
                r.raise_for_status()
                data = r.json()
                choice = (data.get("choices") or [{}])[0]
                msg = (choice.get("message") or {}).get("content") or ""
                if msg.strip():
                    return msg.strip()
        except Exception as exc:
            log.warning("factory_openai_failed", error=str(exc))
    return f"וואלה {topic} זה מטורף אחי 😅"


def _roll_converse_mode() -> str:
    r = random.random()
    if r < 0.60:
        return "text"
    if r < 0.85:
        return "lurk"
    if r < 0.95:
        return "reaction"
    return "sticker"


@registry.register("swarm.community_factory.bootstrap")
async def community_factory_bootstrap(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Scan sessions_dir, compute 3% owners / 97% members, persist roles, init state/metrics,
    enqueue create/join/chat ticks per ``phases``.
    """
    redis = parameters.get("__redis__")
    sessions_dir = _resolve_sessions_dir(str(parameters.get("sessions_dir", "") or ""))
    phases = [str(p).lower() for p in (parameters.get("phases") or ["allocate", "create"])]
    dry_run = bool(parameters.get("dry_run", False))
    reset = bool(parameters.get("reset", False))

    bases = _discover_session_bases(sessions_dir)
    owners, members = _split_roles(bases)

    if reset and redis and not dry_run:
        await redis.delete(KEY_ROLES, KEY_GROUPS, KEY_STATE, KEY_BANNED, KEY_COOLDOWNS, KEY_METRICS)

    roles_payload = {"owners": owners, "members": members}

    if not dry_run and redis:
        await _redis_json_set(redis, KEY_ROLES, roles_payload)
        roles_path = os.getenv("COMMUNITY_FACTORY_ROLES_PATH", "").strip()
        if roles_path:
            try:
                Path(roles_path).parent.mkdir(parents=True, exist_ok=True)
                Path(roles_path).write_text(
                    json.dumps(roles_payload, ensure_ascii=False, indent=2), encoding="utf-8"
                )
            except OSError as exc:
                log.warning("factory_roles_file_write_failed", path=roles_path, error=str(exc))

        state = await _redis_json_get(redis, KEY_STATE)
        if not isinstance(state, dict):
            state = _default_state(str(sessions_dir))
        else:
            state["sessions_dir"] = str(sessions_dir)
        state["phase"] = "allocating"
        state["init_phases"] = phases
        state["chat_enabled"] = "chat" in phases
        state["creation_index"] = 0
        state["join_flat_idx"] = 0
        state["converse_idx"] = 0
        state["max_joins_per_tick"] = int(parameters.get("max_joins_per_tick") or 1)
        state["converse_chain_limit"] = int(
            parameters.get("converse_chain_limit")
            or os.getenv("COMMUNITY_FACTORY_CONVERSE_CHAIN", "5000")
        )
        await _redis_json_set(redis, KEY_STATE, state)

        m = await _redis_json_get(redis, KEY_METRICS)
        if not isinstance(m, dict):
            await _redis_json_set(redis, KEY_METRICS, _default_metrics())
        await _sync_active_sessions(redis, bases)

    carry = {
        "sessions_dir": str(sessions_dir),
        "phases": phases,
    }

    if dry_run:
        return {
            "status": "completed",
            "dry_run": True,
            "sessions_dir": str(sessions_dir),
            "total_sessions": len(bases),
            "owners": len(owners),
            "members": len(members),
            "roles": roles_payload,
        }

    if "create" in phases and owners:
        state = await _redis_json_get(redis, KEY_STATE)
        if isinstance(state, dict):
            state["phase"] = "creating"
            await _redis_json_set(redis, KEY_STATE, state)
        await _enqueue_task("swarm.community_factory.create_groups_tick", carry)
    elif "join" in phases and not ("create" in phases):
        await _enqueue_task("swarm.community_factory.join_tick", carry)
    if "chat" in phases and "create" not in phases and "join" not in phases:
        await _enqueue_task("swarm.community_factory.converse_tick", carry)

    return {
        "status": "completed",
        "total_sessions": len(bases),
        "owners": len(owners),
        "members": len(members),
        "phases": phases,
        "enqueued": True,
    }


@registry.register("swarm.community_factory.create_groups_tick")
async def community_factory_create_groups_tick(parameters: dict[str, Any]) -> dict[str, Any]:
    redis = parameters.get("__redis__")

    roles = await _redis_json_get(redis, KEY_ROLES)
    if not isinstance(roles, dict):
        return {"status": "failed", "error": "roles not allocated — run bootstrap"}
    owners: list[str] = list(roles.get("owners") or [])
    if not owners:
        return {"status": "failed", "error": "no owners"}

    aid, ahash = resolve_telethon_creds(owners[0], parameters)
    if not aid or not ahash:
        return {
            "status": "failed",
            "error": "Telethon api_id/api_hash missing: set TELEFIX_* or add .json next to the first owner session",
        }

    state = await _redis_json_get(redis, KEY_STATE)
    if not isinstance(state, dict):
        return {"status": "failed", "error": "state missing"}
    target = int(state.get("groups_per_owner_target") or GROUPS_TARGET_PER_OWNER)
    idx = int(state.get("creation_index", 0))
    max_idx = target * len(owners) - 1
    if idx > max_idx:
        iphases = list(state.get("init_phases") or [])
        if "join" in iphases:
            state["phase"] = "joining"
        elif "chat" in iphases:
            state["phase"] = "chatting"
        else:
            state["phase"] = "complete"
        await _redis_json_set(redis, KEY_STATE, state)
        if "join" in iphases:
            await _enqueue_task(
                "swarm.community_factory.join_tick",
                {"sessions_dir": state.get("sessions_dir", "")},
            )
        elif "chat" in iphases:
            await _enqueue_task(
                "swarm.community_factory.converse_tick",
                {"sessions_dir": state.get("sessions_dir", "")},
            )
        return {"status": "completed", "phase": "create_done", "groups_created_total": idx}

    try:
        from telethon.tl.functions.channels import CreateChannelRequest  # type: ignore[import-untyped]
        from telethon.tl.types import Channel  # type: ignore[import-untyped]
    except ImportError:
        return {"status": "failed", "error": "telethon not installed"}

    await asyncio.sleep(random.uniform(30.0, 120.0))

    owner_idx = idx % len(owners)
    owner_base = owners[owner_idx]

    title = f"CF {owner_idx}-{idx // len(owners)}-{random.randint(1000, 9999)}"
    group_id: int | None = None
    invite_link = ""

    try:
        async with async_telegram_client(owner_base, parameters) as client:
            if not await client.is_user_authorized():
                await _mark_banned(redis, owner_base)
                await _bump_metric(redis, "bans", 1)
                state["creation_index"] = idx + 1
                await _redis_json_set(redis, KEY_STATE, state)
                await _enqueue_task("swarm.community_factory.create_groups_tick", parameters)
                return {"status": "skipped", "reason": "owner_unauthorized"}

            created = await client(
                CreateChannelRequest(title=title[:128], about="", megagroup=True, broadcast=False)
            )
            chats = list(getattr(created, "chats", None) or [])
            ch = next((c for c in chats if isinstance(c, Channel)), None)
            if ch is None and chats:
                ch = chats[0]
            if ch is None:
                raise RuntimeError("CreateChannelRequest returned no channel")
            invite_link = await client.export_chat_invite_link(ch)
            group_id = int(ch.id)
    except ValueError as exc:
        log.warning("factory_create_creds_missing", owner=owner_base[:48], error=str(exc))
        state["creation_index"] = idx + 1
        await _redis_json_set(redis, KEY_STATE, state)
        await _enqueue_task("swarm.community_factory.create_groups_tick", parameters)
        return {"status": "failed", "error": str(exc), "continuing": True}
    except Exception as exc:
        kind = classify_telethon_account_error(exc)
        if kind == "ban":
            await _mark_banned(redis, owner_base)
            await _bump_metric(redis, "bans", 1)
        elif kind == "flood":
            sec = int(flood_wait_seconds(exc) * 1.1) + 1
            await _set_cooldown(redis, owner_base, sec)
            await _bump_metric(redis, "flood_waits", 1)
            await _enqueue_task("swarm.community_factory.create_groups_tick", parameters)
            return {"status": "deferred", "reason": "flood_wait", "seconds": sec}
        log.warning("factory_create_failed", error=str(exc))
        state["creation_index"] = idx + 1
        await _redis_json_set(redis, KEY_STATE, state)
        await _enqueue_task("swarm.community_factory.create_groups_tick", parameters)
        return {"status": "failed", "error": str(exc), "continuing": True}

    groups = await _redis_json_get(redis, KEY_GROUPS)
    glist: list[dict[str, Any]] = list(groups) if isinstance(groups, list) else []
    glist.append(
        {
            "group_id": group_id,
            "owner_session": owner_base,
            "invite_link": invite_link,
            "invite_hash": _invite_hash(invite_link),
            "title": title,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    await _redis_json_set(redis, KEY_GROUPS, glist)
    await _bump_metric(redis, "groups_total", 1)

    state["creation_index"] = idx + 1
    await _redis_json_set(redis, KEY_STATE, state)
    await _enqueue_task("swarm.community_factory.create_groups_tick", parameters)

    return {
        "status": "completed",
        "group_id": group_id,
        "invite_link": invite_link,
        "creation_index": idx + 1,
    }


@registry.register("swarm.community_factory.join_tick")
async def community_factory_join_tick(parameters: dict[str, Any]) -> dict[str, Any]:
    redis = parameters.get("__redis__")

    roles = await _redis_json_get(redis, KEY_ROLES)
    groups = await _redis_json_get(redis, KEY_GROUPS)
    state = await _redis_json_get(redis, KEY_STATE)
    if not isinstance(roles, dict) or not isinstance(state, dict):
        return {"status": "failed", "error": "roles or state missing"}
    if not isinstance(groups, list) or not groups:
        return {"status": "failed", "error": "no groups to join"}

    owners = list(roles.get("owners") or [])
    members = list(roles.get("members") or [])
    if not members:
        return {
            "status": "failed",
            "error": "no member sessions to join groups — need non-owner accounts in the pool",
        }

    aid, ahash = resolve_telethon_creds(members[0], parameters)
    if not aid or not ahash:
        return {
            "status": "failed",
            "error": "Telethon api_id/api_hash missing: set TELEFIX_* or add .json next to member sessions",
        }

    all_sessions = owners + members

    G = len(groups)
    S = len(members)
    flat_max = S * G
    j = int(state.get("join_flat_idx", 0))

    max_joins = int(
        state.get("max_joins_per_tick")
        or parameters.get("max_joins_per_tick")
        or os.getenv("COMMUNITY_FACTORY_MAX_JOINS_PER_TICK", "1")
    )

    try:
        from telethon.tl.functions.messages import ImportChatInviteRequest  # type: ignore[import-untyped]
    except ImportError:
        return {"status": "failed", "error": "telethon not installed"}

    attempts = 0
    while attempts < max(20, max_joins * 5) and j < flat_max:
        session_i = j % S
        group_i = j // S
        session_base = members[session_i]
        grp = groups[group_i] if group_i < len(groups) else {}
        link = str(grp.get("invite_link") or "")

        j += 1
        attempts += 1

        if await _is_session_banned(redis, session_base):
            continue
        until = await _cooldown_until(redis, session_base)
        if until and datetime.now(timezone.utc) < until:
            continue

        if not link:
            continue

        await _bump_metric(redis, "join_attempts", 1)
        h = _invite_hash(link)
        if not h:
            await _bump_metric(redis, "joins_failed", 1)
            continue

        try:
            async with async_telegram_client(session_base, parameters) as client:
                if not await client.is_user_authorized():
                    await _mark_banned(redis, session_base)
                    await _bump_metric(redis, "bans", 1)
                    continue
                await client(ImportChatInviteRequest(h))
            await _bump_metric(redis, "joins_ok", 1)
            state["join_flat_idx"] = j
            await _redis_json_set(redis, KEY_STATE, state)
            all_bases = _discover_session_bases(_resolve_sessions_dir(str(state.get("sessions_dir", ""))))
            await _sync_active_sessions(redis, all_bases or all_sessions)
            carry = dict(parameters)
            carry.pop("__redis__", None)
            if j < flat_max:
                await _enqueue_task("swarm.community_factory.join_tick", carry)
            else:
                state["phase"] = "chatting" if state.get("chat_enabled") else "complete"
                await _redis_json_set(redis, KEY_STATE, state)
                if state.get("chat_enabled"):
                    await _enqueue_task("swarm.community_factory.converse_tick", carry)
            return {"status": "completed", "joined": True, "join_flat_idx": j}

        except ValueError as exc:
            log.warning("factory_join_creds_missing", session=session_base[:32], error=str(exc))
            await _bump_metric(redis, "joins_failed", 1)
            continue
        except Exception as exc:
            kind = classify_telethon_account_error(exc)
            if kind == "ban":
                await _mark_banned(redis, session_base)
                await _bump_metric(redis, "bans", 1)
                continue
            if kind == "flood":
                sec = int(flood_wait_seconds(exc) * 1.1) + 1
                await _set_cooldown(redis, session_base, sec)
                await _bump_metric(redis, "flood_waits", 1)
                state["join_flat_idx"] = max(0, j - 1)
                await _redis_json_set(redis, KEY_STATE, state)
                carry = dict(parameters)
                carry.pop("__redis__", None)
                await _enqueue_task("swarm.community_factory.join_tick", carry)
                return {"status": "deferred", "reason": "flood_wait"}
            await _bump_metric(redis, "joins_failed", 1)
            log.debug("factory_join_failed", session=session_base[:32], error=str(exc))

    state["join_flat_idx"] = j
    await _redis_json_set(redis, KEY_STATE, state)
    carry = dict(parameters)
    carry.pop("__redis__", None)
    if j < flat_max:
        await _enqueue_task("swarm.community_factory.join_tick", carry)
    else:
        state["phase"] = "chatting" if state.get("chat_enabled") else "complete"
        await _redis_json_set(redis, KEY_STATE, state)
        if state.get("chat_enabled"):
            await _enqueue_task("swarm.community_factory.converse_tick", carry)

    return {"status": "completed", "joined": False, "join_flat_idx": j, "exhausted": j >= flat_max}


@registry.register("swarm.community_factory.converse_tick")
async def community_factory_converse_tick(parameters: dict[str, Any]) -> dict[str, Any]:
    redis = parameters.get("__redis__")
    api_key = _resolve_api_key(parameters)
    openai_key = _resolve_openai_key(parameters)

    roles = await _redis_json_get(redis, KEY_ROLES)
    groups = await _redis_json_get(redis, KEY_GROUPS)
    state = await _redis_json_get(redis, KEY_STATE)
    if not isinstance(roles, dict) or not isinstance(groups, list) or not groups:
        return {"status": "failed", "error": "missing roles or groups"}
    if not isinstance(state, dict):
        return {"status": "failed", "error": "state missing"}

    owners = list(roles.get("owners") or [])
    members = list(roles.get("members") or [])
    all_sessions = owners + members
    if not all_sessions:
        return {"status": "failed", "error": "no sessions"}

    aid, ahash = resolve_telethon_creds(all_sessions[0], parameters)
    if not aid or not ahash:
        return {
            "status": "failed",
            "error": "Telethon api_id/api_hash missing: set TELEFIX_* or add .json next to sessions",
        }

    stop_after = int(
        state.get("converse_chain_limit")
        or parameters.get("converse_ticks")
        or os.getenv("COMMUNITY_FACTORY_CONVERSE_CHAIN", "5000")
    )
    cidx = int(state.get("converse_idx", 0))
    if cidx >= stop_after:
        state["phase"] = "complete"
        await _redis_json_set(redis, KEY_STATE, state)
        return {"status": "completed", "phase": "chat_cap"}

    gi = cidx % len(groups)
    si = cidx % len(all_sessions)
    session_base = all_sessions[si]
    grp = groups[gi]
    group_id = grp.get("group_id")

    state["converse_idx"] = cidx + 1
    await _redis_json_set(redis, KEY_STATE, state)

    if await _is_session_banned(redis, session_base):
        carry = dict(parameters)
        carry.pop("__redis__", None)
        await _enqueue_task("swarm.community_factory.converse_tick", carry)
        return {"status": "skipped", "reason": "banned"}

    until = await _cooldown_until(redis, session_base)
    if until and datetime.now(timezone.utc) < until:
        carry = dict(parameters)
        carry.pop("__redis__", None)
        await _enqueue_task("swarm.community_factory.converse_tick", carry)
        return {"status": "deferred", "reason": "cooldown"}

    mode = _roll_converse_mode()
    if mode == "lurk":
        carry = dict(parameters)
        carry.pop("__redis__", None)
        await _enqueue_task("swarm.community_factory.converse_tick", carry)
        return {"status": "completed", "action": "lurk"}

    try:
        import telethon  # noqa: F401
    except ImportError:
        return {"status": "failed", "error": "telethon not installed"}

    topic = random.choice(FACTORY_TOPICS)
    carry = dict(parameters)
    carry.pop("__redis__", None)

    async def _enqueue_next() -> None:
        await _enqueue_task("swarm.community_factory.converse_tick", carry)

    if mode == "reaction":
        await asyncio.sleep(random.uniform(5.0, 300.0))
        try:
            async with async_telegram_client(session_base, parameters) as client:
                if not await client.is_user_authorized():
                    await _mark_banned(redis, session_base)
                    await _bump_metric(redis, "bans", 1)
                    await _enqueue_next()
                    return {"status": "skipped", "reason": "unauthorized"}
                ent = await client.get_entity(int(group_id))
                reacted = await _try_peer_reaction(client, ent)
                if not reacted:
                    await client.send_message(ent, random.choice(REACTION_EMOJIS))
            await _bump_metric(redis, "messages_sent", 1)
        except ValueError as exc:
            log.warning("factory_converse_creds_missing", error=str(exc))
        except Exception as exc:
            kind = classify_telethon_account_error(exc)
            if kind == "ban":
                await _mark_banned(redis, session_base)
                await _bump_metric(redis, "bans", 1)
            elif kind == "flood":
                sec = int(flood_wait_seconds(exc) * 1.1) + 1
                await _set_cooldown(redis, session_base, sec)
                await _bump_metric(redis, "flood_waits", 1)
            else:
                log.debug("factory_converse_reaction_failed", error=str(exc))
        await _enqueue_next()
        return {"status": "completed", "action": "reaction"}

    if mode == "sticker":
        await asyncio.sleep(random.uniform(5.0, 300.0))
        try:
            async with async_telegram_client(session_base, parameters) as client:
                if not await client.is_user_authorized():
                    await _mark_banned(redis, session_base)
                    await _bump_metric(redis, "bans", 1)
                    await _enqueue_next()
                    return {"status": "skipped", "reason": "unauthorized"}
                ent = await client.get_entity(int(group_id))
                sent = await _try_send_pack_sticker(client, ent)
                if not sent:
                    await client.send_message(ent, random.choice(REACTION_EMOJIS))
            await _bump_metric(redis, "messages_sent", 1)
        except ValueError as exc:
            log.warning("factory_converse_creds_missing", error=str(exc))
        except Exception as exc:
            kind = classify_telethon_account_error(exc)
            if kind == "ban":
                await _mark_banned(redis, session_base)
                await _bump_metric(redis, "bans", 1)
            elif kind == "flood":
                sec = int(flood_wait_seconds(exc) * 1.1) + 1
                await _set_cooldown(redis, session_base, sec)
                await _bump_metric(redis, "flood_waits", 1)
            else:
                log.debug("factory_converse_sticker_failed", error=str(exc))
        await _enqueue_next()
        return {"status": "completed", "action": "sticker"}

    await asyncio.sleep(random.uniform(5.0, 300.0))
    line = await _generate_hebrew_line(api_key, topic, openai_key)
    if random.random() < 0.12 and len(line) > 5:
        cut = random.randint(1, len(line) - 1)
        line = line[:cut] + line[cut + 1 :]
    try:
        async with async_telegram_client(session_base, parameters) as client:
            if not await client.is_user_authorized():
                await _mark_banned(redis, session_base)
                await _bump_metric(redis, "bans", 1)
                await _enqueue_next()
                return {"status": "skipped", "reason": "unauthorized"}
            ent = await client.get_entity(int(group_id))
            await client.send_message(ent, line[:4096])
        await _bump_metric(redis, "messages_sent", 1)
    except ValueError as exc:
        log.warning("factory_converse_creds_missing", error=str(exc))
    except Exception as exc:
        kind = classify_telethon_account_error(exc)
        if kind == "ban":
            await _mark_banned(redis, session_base)
            await _bump_metric(redis, "bans", 1)
        elif kind == "flood":
            sec = int(flood_wait_seconds(exc) * 1.1) + 1
            await _set_cooldown(redis, session_base, sec)
            await _bump_metric(redis, "flood_waits", 1)
        else:
            log.warning("factory_converse_send_failed", error=str(exc))

    await _enqueue_next()
    return {"status": "completed", "action": "text"}
