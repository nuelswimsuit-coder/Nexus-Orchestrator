"""
Rank-SEO Telethon group factory — vault sessions, megagroup creation, invite export,
Redis list storage, then swarm joins with jitter.

Task types
----------
seo.group_factory.bootstrap   — init state, enqueue create_tick
seo.group_factory.create_tick — one creation step (chained until cap)
seo.group_factory.join_tick   — one join step (chained until queue drained)
seo_group_factory             — legacy alias: bootstrap + SEO snapshot refresh

Redis
-----
nexus:seo_factory:generated_links  LIST of JSON
  {owner_id, owner_session, group_id, group_name, invite_link, invite_hash, created_at, run_id}
nexus:seo_factory:state            JSON run state
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from nexus.services.session_vault import SessionHealth, discover_meta_paths_from_session_sqlite, meta_key
from nexus.worker.services.tg_session import (
    async_telegram_client,
    classify_telethon_account_error,
    flood_wait_seconds,
)
from nexus.worker.task_registry import registry
from nexus.worker.tasks.swarm_onboarding import _invite_hash, _paired_session_file, _session_base_str

log = structlog.get_logger(__name__)

KEY_LINKS_LIST = "nexus:seo_factory:generated_links"
KEY_STATE = "nexus:seo_factory:state"

MAX_GROUPS_PER_SESSION = 3
CREATE_JITTER_S = (10.0, 30.0)
JOIN_JITTER_S = (15.0, 60.0)

PROJECT_ID = "seo-group-factory"


async def _redis_json_get(redis: Any, key: str) -> Any:
    raw = await redis.get(key)
    if not raw:
        return None
    try:
        return json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
    except Exception:
        return None


async def _redis_json_set(redis: Any, key: str, value: Any) -> None:
    await redis.set(key, json.dumps(value, ensure_ascii=False))


async def _redis_meta_row(redis: Any, stem: str) -> dict[str, Any]:
    raw = await redis.get(meta_key(stem))
    if not raw:
        return {}
    try:
        d = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _healthy_active_for_factory(row: dict[str, Any]) -> bool:
    if row.get("is_banned") is True:
        return False
    if row.get("is_active") is False:
        return False
    st = str(row.get("status") or "").strip().lower()
    if st in ("banned", "offline"):
        return False
    h = str(row.get("health") or "").strip().lower()
    if h == SessionHealth.RED.value:
        return False
    if h == SessionHealth.YELLOW.value:
        return False
    return True


async def _discover_healthy_session_bases(redis: Any) -> list[str]:
    bases: list[str] = []
    for meta_json in discover_meta_paths_from_session_sqlite():
        if _paired_session_file(meta_json) is None:
            continue
        row = await _redis_meta_row(redis, meta_json.stem)
        if not _healthy_active_for_factory(row):
            continue
        sb = _session_base_str(meta_json)
        if sb:
            bases.append(sb)
    return sorted(set(bases), key=lambda s: s.lower())


async def _enqueue_seo_task(task_type: str, parameters: dict[str, Any]) -> bool:
    try:
        import arq
        from arq.connections import RedisSettings

        from nexus.shared.config import settings
        from nexus.shared.schemas import TaskPayload

        carry = {k: v for k, v in parameters.items() if k != "__redis__"}
        task = TaskPayload(
            task_type=task_type,
            parameters=carry,
            project_id=PROJECT_ID,
            priority=3,
            job_expires_seconds=int(os.getenv("SEO_GROUP_FACTORY_JOB_TTL_S", "3600")),
        )
        job_ttl = int(task.job_expires_seconds or 3600)
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
        log.error("seo_group_factory_enqueue_failed", task_type=task_type, error=str(exc))
        return False


def _default_state() -> dict[str, Any]:
    return {
        "phase": "idle",
        "run_id": "",
        "sessions": [],
        "create_k": 0,
        "join_queue": [],
        "join_idx": 0,
        "updated_at": None,
    }


async def _append_link_record(redis: Any, record: dict[str, Any]) -> None:
    await redis.rpush(KEY_LINKS_LIST, json.dumps(record, ensure_ascii=False))


def _build_join_queue(sessions: list[str], stored_links: list[dict[str, Any]]) -> list[dict[str, Any]]:
    assignments: list[dict[str, Any]] = []
    owner_by_link: list[tuple[str, str]] = []
    for row in stored_links:
        owner = str(row.get("owner_session") or "")
        h = str(row.get("invite_hash") or "")
        if not h:
            h = _invite_hash(str(row.get("invite_link") or ""))
        if owner and h:
            owner_by_link.append((owner, h))
    for session_base in sessions:
        for owner_session, invite_hash in owner_by_link:
            if session_base == owner_session:
                continue
            assignments.append({"session_base": session_base, "invite_hash": invite_hash})
    random.shuffle(assignments)
    return assignments


@registry.register("seo.group_factory.bootstrap")
async def seo_group_factory_bootstrap(parameters: dict[str, Any]) -> dict[str, Any]:
    redis = parameters.get("__redis__")
    if redis is None:
        return {"status": "failed", "error": "Redis not available"}

    reset = bool(parameters.get("reset", False))
    if reset:
        await redis.delete(KEY_STATE, KEY_LINKS_LIST)

    sessions = await _discover_healthy_session_bases(redis)
    if not sessions:
        return {"status": "failed", "error": "no healthy active vault sessions"}

    run_id = str(parameters.get("run_id") or uuid.uuid4())
    state = _default_state()
    state["phase"] = "creating"
    state["run_id"] = run_id
    state["sessions"] = sessions
    state["create_k"] = 0
    state["join_queue"] = []
    state["join_idx"] = 0
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    await _redis_json_set(redis, KEY_STATE, state)

    ok = await _enqueue_seo_task("seo.group_factory.create_tick", parameters)
    if not ok:
        return {"status": "failed", "error": "enqueue create_tick failed", "sessions": len(sessions)}

    log.info(
        "seo_group_factory_bootstrap",
        run_id=run_id,
        sessions=len(sessions),
        max_total=MAX_GROUPS_PER_SESSION * len(sessions),
    )
    return {
        "status": "started",
        "run_id": run_id,
        "sessions": len(sessions),
        "max_groups": MAX_GROUPS_PER_SESSION * len(sessions),
    }


@registry.register("seo.group_factory.create_tick")
async def seo_group_factory_create_tick(parameters: dict[str, Any]) -> dict[str, Any]:
    redis = parameters.get("__redis__")
    if redis is None:
        return {"status": "failed", "error": "Redis not available"}

    state = await _redis_json_get(redis, KEY_STATE)
    if not isinstance(state, dict):
        return {"status": "failed", "error": "state missing — run bootstrap"}

    sessions: list[str] = [str(s) for s in (state.get("sessions") or []) if str(s).strip()]
    if not sessions:
        return {"status": "failed", "error": "no sessions in state"}

    n = len(sessions)
    cap = MAX_GROUPS_PER_SESSION * n
    k = int(state.get("create_k") or 0)

    if k >= cap:
        run_id = str(state.get("run_id") or "")
        raw_links = await redis.lrange(KEY_LINKS_LIST, 0, -1)
        stored: list[dict[str, Any]] = []
        for raw in raw_links or []:
            try:
                item = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                if isinstance(item, dict):
                    if run_id and str(item.get("run_id") or "") != run_id:
                        continue
                    stored.append(item)
            except Exception:
                continue
        jq = _build_join_queue(sessions, stored)
        state["phase"] = "joining"
        state["join_queue"] = jq
        state["join_idx"] = 0
        state["updated_at"] = datetime.now(timezone.utc).isoformat()
        await _redis_json_set(redis, KEY_STATE, state)
        if jq:
            await _enqueue_seo_task("seo.group_factory.join_tick", parameters)
        else:
            state["phase"] = "complete"
            state["updated_at"] = datetime.now(timezone.utc).isoformat()
            await _redis_json_set(redis, KEY_STATE, state)
        return {
            "status": "completed",
            "phase": "create_done",
            "groups_created": k,
            "join_assignments": len(jq),
        }

    await asyncio.sleep(random.uniform(CREATE_JITTER_S[0], CREATE_JITTER_S[1]))

    try:
        from telethon.tl.functions.channels import CreateChannelRequest  # type: ignore[import-untyped]
        from telethon.tl.functions.messages import ExportChatInviteRequest  # type: ignore[import-untyped]
        from telethon.tl.types import Channel  # type: ignore[import-untyped]
    except ImportError:
        return {"status": "failed", "error": "telethon not installed"}

    session_base = sessions[k % n]
    title = f"RSF {Path(session_base).name[:40]}-{k}-{random.randint(1000, 9999)}"

    group_id: int | None = None
    invite_link = ""
    invite_hash = ""
    owner_id: int | None = None

    try:
        async with async_telegram_client(session_base, parameters) as client:
            if not await client.is_user_authorized():
                log.warning("seo_factory_create_unauthorized", session=session_base[-40:])
                state["create_k"] = k + 1
                state["updated_at"] = datetime.now(timezone.utc).isoformat()
                await _redis_json_set(redis, KEY_STATE, state)
                await _enqueue_seo_task("seo.group_factory.create_tick", parameters)
                return {"status": "skipped", "reason": "unauthorized", "create_k": k + 1}

            me = await client.get_me()
            owner_id = int(getattr(me, "id", 0) or 0) or None

            created = await client(
                CreateChannelRequest(title=title[:128], about="", megagroup=True, broadcast=False)
            )
            chats = list(getattr(created, "chats", None) or [])
            ch = next((c for c in chats if isinstance(c, Channel)), None)
            if ch is None and chats:
                ch = chats[0]
            if ch is None:
                raise RuntimeError("CreateChannelRequest returned no channel")

            group_id = int(getattr(ch, "id", 0) or 0) or None
            export = await client(ExportChatInviteRequest(peer=ch))
            invite_link = str(getattr(export, "link", "") or "")
            invite_hash = _invite_hash(invite_link)

    except Exception as exc:
        kind = classify_telethon_account_error(exc)
        if kind == "flood":
            sec = int(flood_wait_seconds(exc) * 1.1) + 1
            log.warning("seo_factory_create_flood", seconds=sec, error=str(exc))
            await asyncio.sleep(min(sec, 300))
            await _enqueue_seo_task("seo.group_factory.create_tick", parameters)
            return {"status": "deferred", "reason": "flood_wait", "seconds": sec}
        log.warning("seo_factory_create_failed", session=session_base[-48:], error=str(exc))
        state["create_k"] = k + 1
        state["updated_at"] = datetime.now(timezone.utc).isoformat()
        await _redis_json_set(redis, KEY_STATE, state)
        await _enqueue_seo_task("seo.group_factory.create_tick", parameters)
        return {"status": "failed", "error": str(exc), "continuing": True}

    record = {
        "owner_id": owner_id,
        "owner_session": session_base,
        "group_id": group_id,
        "group_name": title[:128],
        "invite_link": invite_link,
        "invite_hash": invite_hash,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "run_id": str(state.get("run_id") or ""),
    }
    await _append_link_record(redis, record)

    state["create_k"] = k + 1
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    await _redis_json_set(redis, KEY_STATE, state)
    await _enqueue_seo_task("seo.group_factory.create_tick", parameters)

    return {
        "status": "completed",
        "group_id": group_id,
        "invite_link": invite_link,
        "create_k": k + 1,
    }


@registry.register("seo.group_factory.join_tick")
async def seo_group_factory_join_tick(parameters: dict[str, Any]) -> dict[str, Any]:
    redis = parameters.get("__redis__")
    if redis is None:
        return {"status": "failed", "error": "Redis not available"}

    state = await _redis_json_get(redis, KEY_STATE)
    if not isinstance(state, dict):
        return {"status": "failed", "error": "state missing"}

    queue = list(state.get("join_queue") or [])
    idx = int(state.get("join_idx") or 0)

    if idx >= len(queue):
        state["phase"] = "complete"
        state["updated_at"] = datetime.now(timezone.utc).isoformat()
        await _redis_json_set(redis, KEY_STATE, state)
        return {"status": "completed", "phase": "join_done", "joins_executed": idx}

    await asyncio.sleep(random.uniform(JOIN_JITTER_S[0], JOIN_JITTER_S[1]))

    try:
        from telethon.tl.functions.messages import ImportChatInviteRequest  # type: ignore[import-untyped]
    except ImportError:
        return {"status": "failed", "error": "telethon not installed"}

    job = queue[idx]
    session_base = str(job.get("session_base") or "")
    invite_hash = str(job.get("invite_hash") or "").strip()
    if not session_base or not invite_hash:
        state["join_idx"] = idx + 1
        state["updated_at"] = datetime.now(timezone.utc).isoformat()
        await _redis_json_set(redis, KEY_STATE, state)
        await _enqueue_seo_task("seo.group_factory.join_tick", parameters)
        return {"status": "skipped", "reason": "bad_assignment"}

    try:
        async with async_telegram_client(session_base, parameters) as client:
            if not await client.is_user_authorized():
                log.warning("seo_factory_join_unauthorized", session=session_base[-40:])
            else:
                await client(ImportChatInviteRequest(invite_hash))
    except Exception as exc:
        kind = classify_telethon_account_error(exc)
        if kind == "flood":
            sec = int(flood_wait_seconds(exc) * 1.1) + 1
            log.warning("seo_factory_join_flood", seconds=sec, error=str(exc))
            await asyncio.sleep(min(sec, 300))
            await _enqueue_seo_task("seo.group_factory.join_tick", parameters)
            return {"status": "deferred", "reason": "flood_wait"}
        log.debug("seo_factory_join_failed", session=session_base[-40:], error=str(exc))

    state["join_idx"] = idx + 1
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    await _redis_json_set(redis, KEY_STATE, state)
    await _enqueue_seo_task("seo.group_factory.join_tick", parameters)

    return {"status": "completed", "joined": True, "join_idx": idx + 1}


@registry.register("seo_group_factory")
async def seo_group_factory(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Legacy entry used by ``/api/factory/start-seo-groups``: starts the Telethon Rank-SEO
    pipeline (vault scan). ``sessions_dir`` / ``phases`` are ignored; ``reset`` clears
    ``nexus:seo_factory:*`` before the run.
    """
    redis = parameters.get("__redis__")
    carry = dict(parameters)
    carry["reset"] = bool(parameters.get("reset", False))
    out = await seo_group_factory_bootstrap(carry)
    if redis:
        try:
            from nexus.shared.seo_group_factory import persist_seo_factory_snapshot

            await persist_seo_factory_snapshot(redis)
        except Exception as exc:
            log.warning("seo_group_factory_snapshot_failed", error=str(exc))
    return {
        "status": "completed",
        "message": "RankSEO Telethon group factory bootstrap enqueued.",
        "bootstrap": out,
    }

