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
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from nexus.services.session_vault import (
    SessionHealth,
    SessionStatus,
    discover_meta_paths_from_session_sqlite,
    merge_meta_row,
    meta_key,
    vault_candidate_roots,
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


def _vault_roots_for_diagnostics() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in vault_candidate_roots():
        try:
            rp = r.resolve()
            out.append({"path": str(rp), "is_dir": rp.is_dir()})
        except Exception:
            out.append({"path": str(r), "is_dir": False})
    return out


def _paired_session_file(meta_json: Path) -> Path | None:
    """
    Telethon sqlite next to ``*.json``: ``<stem>.session`` (case-insensitive on Windows).
    """
    direct = meta_json.with_suffix(".session")
    try:
        if direct.is_file():
            return direct.resolve()
    except OSError:
        pass
    if sys.platform == "win32":
        stem_l = meta_json.stem.lower()
        try:
            for p in meta_json.parent.iterdir():
                if not p.is_file():
                    continue
                if p.name.lower().endswith("-journal"):
                    continue
                if p.suffix.lower() != ".session":
                    continue
                if p.stem.lower() != stem_l:
                    continue
                try:
                    return p.resolve()
                except OSError:
                    return p
        except OSError:
            pass
    return None


def _session_base_str(meta_json: Path) -> str | None:
    paired = _paired_session_file(meta_json)
    if paired is None:
        return None
    return str(paired.parent / paired.stem)


async def _scan_onboarding_targets(
    redis: Any,
    allow_stems: set[str] | None,
) -> tuple[list[Path], dict[str, Any]]:
    """
    Returns eligible meta paths plus counts explaining skips (vault on worker disk vs Redis flags).

    Uses only ``*.session``-paired meta paths (not every ``*.json`` under the vault) so large
    export trees and orphan JSON files do not slow down or confuse mass join.
    """
    all_meta = discover_meta_paths_from_session_sqlite()
    missing_samples: list[dict[str, str]] = []
    diag: dict[str, Any] = {
        "discovered_meta_json_files": len(all_meta),
        "skipped_allow_list": 0,
        "skipped_missing_session_sqlite": 0,
        "skipped_redis_banned": 0,
        "skipped_redis_is_active_false": 0,
        "skipped_redis_status_offline_or_banned": 0,
        "eligible": 0,
        "vault_roots": _vault_roots_for_diagnostics(),
        "missing_session_stems": [],
        "missing_session_samples": [],
    }
    eligible: list[Path] = []
    for meta_json in all_meta:
        stem = meta_json.stem
        if allow_stems is not None and stem not in allow_stems:
            diag["skipped_allow_list"] += 1
            continue
        if _paired_session_file(meta_json) is None:
            diag["skipped_missing_session_sqlite"] += 1
            mss = diag["missing_session_stems"]
            if isinstance(mss, list) and len(mss) < 50:
                mss.append(stem)
            if len(missing_samples) < 12:
                try:
                    missing_samples.append(
                        {
                            "stem": stem,
                            "meta_json_path": str(meta_json.resolve()),
                        }
                    )
                except OSError:
                    missing_samples.append({"stem": stem, "meta_json_path": str(meta_json)})
            diag["missing_session_samples"] = missing_samples
            continue
        row = await _redis_meta(redis, stem)
        if not row:
            row = {}
        if row.get("is_banned") is True:
            diag["skipped_redis_banned"] += 1
            continue
        if row.get("is_active") is False:
            diag["skipped_redis_is_active_false"] += 1
            continue
        st = str(row.get("status") or "").strip().lower()
        if st in ("banned", "offline"):
            diag["skipped_redis_status_offline_or_banned"] += 1
            continue
        diag["eligible"] += 1
        eligible.append(meta_json)
    return eligible, diag


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
        AuthKeyDuplicatedError,
        AuthKeyUnregisteredError,
        FloodWaitError,
        UserDeactivatedBanError,
        UserDeactivatedError,
    )

    stem = meta_json.stem
    session_base = _session_base_str(meta_json)
    if not session_base:
        return {"stem": stem, "ok": False, "reason": "missing_session_file"}
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

        except AuthKeyDuplicatedError:
            # Same .session used from two IPs — Telegram invalidates the key until re-login.
            await _mark_dead_session(
                redis,
                meta_json,
                banned=False,
                detail="AuthKeyDuplicatedError: session used from two IPs",
            )
            return {"stem": stem, "ok": False, "reason": "AuthKeyDuplicatedError"}

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

    paths, join_diagnostics = await _scan_onboarding_targets(redis, allow)
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
                        "diagnostics": join_diagnostics,
                    },
                    ensure_ascii=False,
                ),
            )
            await redis.delete(sess_key)
            await redis.set(_MASS_JOIN_LATEST_KEY, task_id, ex=_MASS_JOIN_TTL_S)
            await redis.expire(meta_key, _MASS_JOIN_TTL_S)
        log.warning(
            "swarm_onboarding_no_eligible",
            target=target[:80],
            diagnostics=join_diagnostics,
        )
        return {
            "status": "completed",
            "joined": 0,
            "detail": "no eligible sessions",
            "diagnostics": join_diagnostics,
        }

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
