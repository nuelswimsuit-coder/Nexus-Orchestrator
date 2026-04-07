"""
swarm.onboarding.mass_join — isolated mass join + triage for Telethon vault sessions.

Separate from ``swarm.community_factory`` chat/join ticks: one-shot join of a target
group/link across all vault sessions that look active in Redis (``nexus:session_vault:meta:*``),
with low concurrency to reduce FloodWait risk.

Parameters
----------
target_link : str   — public t.me link / @username, or private ``joinchat`` / ``/+`` invite.
session_stems : list[str] | None — optional allow-list of vault stems; default = all eligible.

Task type
---------
swarm.onboarding.mass_join
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from nexus.services.session_vault import (
    SessionHealth,
    SessionStatus,
    discover_all_meta_json_files,
    merge_meta_row,
    meta_key,
)
from nexus.worker.services.tg_session import async_telegram_client, flood_wait_seconds
from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

_JOIN_SEM = asyncio.Semaphore(10)

_MASS_JOIN_LATEST_KEY = "nexus:swarm:mass_join:latest_task_id"
_MASS_JOIN_TTL_S = 86400 * 7


def _mass_join_meta_key(task_id: str) -> str:
    return f"nexus:swarm:mass_join:{task_id}:meta"


def _mass_join_sessions_key(task_id: str) -> str:
    return f"nexus:swarm:mass_join:{task_id}:sessions"


async def _mass_join_write_stem(redis: Any, task_id: str, stem: str, payload: dict[str, Any]) -> None:
    key = _mass_join_sessions_key(task_id)
    await redis.hset(key, stem, json.dumps(payload, ensure_ascii=False))


async def _join_one_session_tracked(
    meta_json: Path,
    target_link: str,
    parameters: dict[str, Any],
    redis: Any,
    task_id: str,
) -> dict[str, Any]:
    stem = meta_json.stem
    now_iso = datetime.now(timezone.utc).isoformat()
    await _mass_join_write_stem(
        redis,
        task_id,
        stem,
        {"status": "joining", "updated_at": now_iso},
    )
    res = await _join_one_session(meta_json, target_link, parameters, redis)
    ok = bool(res.get("ok"))
    done_iso = datetime.now(timezone.utc).isoformat()
    await _mass_join_write_stem(
        redis,
        task_id,
        stem,
        {
            "status": "success" if ok else "failed",
            "ok": ok,
            "reason": str(res.get("reason") or ""),
            "updated_at": done_iso,
        },
    )
    return res


def _invite_hash(link_or_hash: str) -> str:
    s = (link_or_hash or "").strip()
    if "/+" in s:
        return s.split("/+")[-1].split("?")[0].strip()
    if "joinchat/" in s.lower():
        return s.split("joinchat/")[-1].split("?")[0].strip()
    return s.lstrip("+")


def _is_invite_link(target: str) -> bool:
    t = (target or "").strip().lower()
    return "/+" in t or "joinchat/" in t


async def _redis_meta(redis: Any, stem: str) -> dict[str, Any]:
    raw = await redis.get(meta_key(stem))
    if not raw:
        return {}
    try:
        d = json.loads(raw)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _eligible_for_onboarding(meta: dict[str, Any], path: Path) -> bool:
    if meta.get("is_banned") is True:
        return False
    if meta.get("is_active") is False:
        return False
    st = str(meta.get("status") or "").strip().lower()
    if st in ("banned", "offline"):
        return False
    sess_file = path.with_suffix(".session")
    return sess_file.is_file()


async def _iter_onboarding_targets(
    redis: Any,
    allow_stems: set[str] | None,
) -> list[Path]:
    out: list[Path] = []
    for meta_json in discover_all_meta_json_files():
        stem = meta_json.stem
        if allow_stems is not None and stem not in allow_stems:
            continue
        row = await _redis_meta(redis, stem)
        if not row:
            row = {}
        if _eligible_for_onboarding(row, meta_json):
            out.append(meta_json)
    return out


async def _mark_dead_session(
    redis: Any,
    meta_json: Path,
    *,
    banned: bool,
    detail: str,
) -> None:
    row = {
        "is_active": False,
        "is_banned": banned,
        "status": SessionStatus.BANNED.value if banned else SessionStatus.OFFLINE.value,
        "health": SessionHealth.RED.value,
        "detail": detail,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
    await merge_meta_row(redis, meta_json, row)
    log.warning(
        "swarm_onboarding_session_deactivated",
        stem=meta_json.stem,
        is_banned=banned,
        detail=detail[:200],
    )


async def _do_join(client: Any, target: str) -> None:
    from telethon.tl.functions.channels import JoinChannelRequest  # type: ignore[import-untyped]
    from telethon.tl.functions.messages import ImportChatInviteRequest  # type: ignore[import-untyped]

    t = (target or "").strip()
    if _is_invite_link(t):
        h = _invite_hash(t)
        if not h:
            raise ValueError("could not parse invite hash from link")
        await client(ImportChatInviteRequest(h))
        return
    ent = await client.get_entity(t)
    await client(JoinChannelRequest(ent))


async def _join_one_session(
    meta_json: Path,
    target_link: str,
    parameters: dict[str, Any],
    redis: Any,
) -> dict[str, Any]:
    from telethon.errors import (  # type: ignore[import-untyped]
        AuthKeyUnregisteredError,
        FloodWaitError,
        UserDeactivatedBanError,
        UserDeactivatedError,
    )

    session_base = str(meta_json.parent / meta_json.stem)
    stem = meta_json.stem
    flood_sleep: int | None = None
    async with _JOIN_SEM:
        try:
            async with async_telegram_client(session_base, parameters) as client:
                if not await client.is_user_authorized():
                    await _mark_dead_session(
                        redis,
                        meta_json,
                        banned=False,
                        detail="swarm_onboarding: not authorized",
                    )
                    return {"stem": stem, "ok": False, "reason": "not_authorized"}

                await _do_join(client, target_link)

            return {"stem": stem, "ok": True}

        except (UserDeactivatedError, UserDeactivatedBanError) as exc:
            await _mark_dead_session(redis, meta_json, banned=True, detail=type(exc).__name__)
            return {"stem": stem, "ok": False, "reason": type(exc).__name__}

        except AuthKeyUnregisteredError as exc:
            await _mark_dead_session(redis, meta_json, banned=False, detail=type(exc).__name__)
            return {"stem": stem, "ok": False, "reason": type(exc).__name__}

        except FloodWaitError as exc:
            flood_sleep = min(int(flood_wait_seconds(exc)), 3600)
            log.warning("swarm_onboarding_flood_wait", stem=stem, seconds=flood_sleep)

        except Exception as exc:
            log.warning("swarm_onboarding_join_failed", stem=stem, error=str(exc))
            return {"stem": stem, "ok": False, "reason": str(exc)}

    if flood_sleep is None:
        log.error("swarm_onboarding_flood_wait_missing", stem=stem)
        return {"stem": stem, "ok": False, "reason": "internal_error"}
    await asyncio.sleep(flood_sleep)
    return {"stem": stem, "ok": False, "reason": "flood_wait", "seconds": flood_sleep}


@registry.register("swarm.onboarding.mass_join")
async def swarm_onboarding_mass_join(parameters: dict[str, Any]) -> dict[str, Any]:
    redis = parameters.get("__redis__")
    target = str(
        parameters.get("target_link")
        or parameters.get("group_link")
        or parameters.get("invite_link")
        or ""
    ).strip()
    if not target:
        return {"status": "failed", "error": "target_link (or group_link) is required"}
    if redis is None:
        return {"status": "failed", "error": "Redis not available (__redis__ missing)"}

    task_id = str(
        parameters.get("mass_join_task_id")
        or parameters.get("task_id")
        or ""
    ).strip()

    raw_stems = parameters.get("session_stems")
    allow: set[str] | None = None
    if isinstance(raw_stems, list) and raw_stems:
        allow = {str(s).strip() for s in raw_stems if str(s).strip()}

    paths = await _iter_onboarding_targets(redis, allow)
    if not paths:
        if task_id:
            meta_key = _mass_join_meta_key(task_id)
            sess_key = _mass_join_sessions_key(task_id)
            started = datetime.now(timezone.utc).isoformat()
            await redis.set(
                meta_key,
                json.dumps(
                    {
                        "task_id": task_id,
                        "target_link": target,
                        "started_at": started,
                        "finished_at": started,
                        "status": "completed",
                        "total": 0,
                        "joins_ok": 0,
                        "detail": "no eligible sessions",
                    },
                    ensure_ascii=False,
                ),
            )
            await redis.delete(sess_key)
            await redis.set(_MASS_JOIN_LATEST_KEY, task_id, ex=_MASS_JOIN_TTL_S)
            await redis.expire(meta_key, _MASS_JOIN_TTL_S)
        return {"status": "completed", "joined": 0, "detail": "no eligible sessions"}

    started_at = datetime.now(timezone.utc).isoformat()
    if task_id:
        meta_key = _mass_join_meta_key(task_id)
        sess_key = _mass_join_sessions_key(task_id)
        await redis.set(
            meta_key,
            json.dumps(
                {
                    "task_id": task_id,
                    "target_link": target,
                    "started_at": started_at,
                    "status": "running",
                    "total": len(paths),
                },
                ensure_ascii=False,
            ),
        )
        mapping = {
            p.stem: json.dumps({"status": "pending", "updated_at": started_at}, ensure_ascii=False)
            for p in paths
        }
        await redis.hset(sess_key, mapping=mapping)
        await redis.expire(sess_key, _MASS_JOIN_TTL_S)
        await redis.expire(meta_key, _MASS_JOIN_TTL_S)
        await redis.set(_MASS_JOIN_LATEST_KEY, task_id, ex=_MASS_JOIN_TTL_S)

    if task_id:
        tasks = [
            _join_one_session_tracked(p, target, parameters, redis, task_id) for p in paths
        ]
    else:
        tasks = [_join_one_session(p, target, parameters, redis) for p in paths]
    results = await asyncio.gather(*tasks)

    ok_n = sum(1 for r in results if r.get("ok"))
    finished_at = datetime.now(timezone.utc).isoformat()
    if task_id:
        await redis.set(
            _mass_join_meta_key(task_id),
            json.dumps(
                {
                    "task_id": task_id,
                    "target_link": target,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "status": "completed",
                    "total": len(paths),
                    "joins_ok": ok_n,
                },
                ensure_ascii=False,
            ),
        )
    return {
        "status": "completed",
        "target_link": target,
        "sessions_attempted": len(paths),
        "joins_ok": ok_n,
        "results": results,
    }
