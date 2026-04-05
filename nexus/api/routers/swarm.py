"""
Swarm Social Synthesis — dashboard API for community identity + group registry.
"""

from __future__ import annotations

import json
import pathlib
import uuid
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import structlog
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from nexus.api.dependencies import RedisDep
from nexus.shared.config import settings

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/swarm", tags=["swarm"])

SWARM_GROUPS_KEY = "nexus:swarm:warmer:groups"
SWARM_STATE_PREFIX = "nexus:swarm:warmer:state:"
SWARM_COMMUNITY_PREFIX = "nexus:swarm:community:"


class SwarmSessionEntry(BaseModel):
    session_path: str = Field(..., description="Telethon session path without .session extension")
    username: str = Field(default="", description="Telegram @handle for mentions (no @)")


class SwarmGroupUpsert(BaseModel):
    group_id: int
    sessions: list[SwarmSessionEntry]
    timezone: str = "UTC"
    enabled: bool = True
    group_title: str = ""
    engagement_mode: str = Field(
        default="",
        description="Empty = default cadence; 'high' = chatter every 10–20 minutes",
    )


def _gk(group_id: int) -> str:
    return str(group_id)


@router.get("/dashboard", summary="Merged warmer state + community classification per group")
async def swarm_dashboard(redis: RedisDep) -> dict[str, Any]:
    raw = await redis.get(SWARM_GROUPS_KEY)
    groups: dict[str, Any] = {}
    if raw:
        try:
            groups = json.loads(raw)
        except Exception:
            groups = {}

    out: list[dict[str, Any]] = []
    for key, cfg in groups.items():
        if not isinstance(cfg, dict):
            continue
        comm_raw = await redis.get(f"{SWARM_COMMUNITY_PREFIX}{key}")
        st_raw = await redis.get(f"{SWARM_STATE_PREFIX}{key}")
        comm = json.loads(comm_raw) if comm_raw else {}
        st = json.loads(st_raw) if st_raw else {}
        row = {
            "group_key": key,
            "config": cfg,
            "community_identity": comm.get("community_identity", ""),
            "group_description": comm.get("group_description", ""),
            "emerging_identity": comm.get("emerging_identity", st.get("emerging_identity", "")),
            "updated_at": comm.get("updated_at"),
            "next_run_at": st.get("next_run_at"),
            "last_topic": st.get("last_topic"),
            "last_classify_at": st.get("last_classify_at"),
        }
        out.append(row)

    return {"groups": out, "count": len(out)}


@router.post("/groups/{group_key}", summary="Register or replace a warmed group")
async def upsert_swarm_group(
    group_key: str,
    body: SwarmGroupUpsert,
    redis: RedisDep,
) -> dict[str, Any]:
    raw = await redis.get(SWARM_GROUPS_KEY)
    all_g: dict[str, Any] = {}
    if raw:
        try:
            all_g = json.loads(raw)
        except Exception:
            all_g = {}

    all_g[group_key] = {
        "group_id": body.group_id,
        "sessions": [s.model_dump() for s in body.sessions],
        "timezone": body.timezone,
        "enabled": body.enabled,
        "group_title": body.group_title,
        "engagement_mode": body.engagement_mode,
    }
    await redis.set(SWARM_GROUPS_KEY, json.dumps(all_g, ensure_ascii=False))
    log.info("swarm_group_upserted", group_key=group_key)
    return {"ok": True, "group_key": group_key}


@router.delete("/groups/{group_key}", summary="Remove a group from the warmer registry")
async def delete_swarm_group(group_key: str, redis: RedisDep) -> dict[str, Any]:
    raw = await redis.get(SWARM_GROUPS_KEY)
    if not raw:
        return {"ok": True, "removed": False}
    try:
        all_g = json.loads(raw)
    except Exception:
        return {"ok": False, "error": "invalid registry"}
    if not isinstance(all_g, dict):
        return {"ok": False, "error": "invalid registry"}
    removed = all_g.pop(group_key, None) is not None
    await redis.set(SWARM_GROUPS_KEY, json.dumps(all_g, ensure_ascii=False))
    return {"ok": True, "removed": removed}


@router.post("/groups/by-id/{group_id}", summary="Upsert using group_id as registry key")
async def upsert_by_numeric_id(
    group_id: int,
    body: SwarmGroupUpsert,
    redis: RedisDep,
) -> dict[str, Any]:
    return await upsert_swarm_group(_gk(group_id), body, redis)


import socket as _socket

_SESSION_INVENTORY_KEY = "nexus:sessions:inventory"
_ALL_SCANNED_KEY = "nexus:sessions:all_scanned"


def _machine_id() -> str:
    return _socket.gethostname()


@router.get("/sessions/inventory", summary="Get global session inventory grouped by machine")
async def get_session_inventory(redis: RedisDep) -> dict[str, Any]:
    """
    Returns inventory in the shape the dashboard expects:
      {inventory_by_machine: {machine_id: [InventorySession]}, machines: [...], total: N}

    Data is aggregated from all per-node keys ``nexus:sessions:inventory:<machine_id>``
    plus the legacy single-node key ``nexus:sessions:inventory``.
    """
    inventory_by_machine: dict[str, list[dict[str, Any]]] = {}

    # Scan all per-node inventory keys
    cursor = 0
    while True:
        cursor, keys = await redis.scan(cursor=cursor, match="nexus:sessions:inventory:*", count=100)
        for key in keys:
            raw = await redis.get(key)
            if not raw:
                continue
            try:
                node_data = json.loads(raw)
                machine = key.replace("nexus:sessions:inventory:", "")
                sessions = node_data if isinstance(node_data, list) else node_data.get("sessions", [])
                if sessions:
                    inventory_by_machine[machine] = sessions
            except Exception:
                pass
        if cursor == 0:
            break

    # Also check the legacy single-node key
    raw_legacy = await redis.get(_SESSION_INVENTORY_KEY)
    if raw_legacy:
        try:
            legacy = json.loads(raw_legacy)
            if isinstance(legacy, dict) and "inventory_by_machine" in legacy:
                # Already in the new shape — merge
                for m, sessions in (legacy.get("inventory_by_machine") or {}).items():
                    if m not in inventory_by_machine:
                        inventory_by_machine[m] = sessions
            elif isinstance(legacy, list):
                mid = _machine_id()
                if mid not in inventory_by_machine:
                    inventory_by_machine[mid] = legacy
        except Exception:
            pass

    machines = list(inventory_by_machine.keys())
    total = sum(len(v) for v in inventory_by_machine.values())

    return {
        "inventory_by_machine": inventory_by_machine,
        "machines": machines,
        "total": total,
        "is_mock": False,
    }


@router.post("/sessions/inventory", summary="Update session inventory for this node")
async def set_session_inventory(body: dict, redis: RedisDep) -> dict[str, Any]:
    machine = body.get("machine_id") or _machine_id()
    # Store both per-node key and legacy key
    await redis.set(f"{_SESSION_INVENTORY_KEY}:{machine}", json.dumps(body, ensure_ascii=False))
    await redis.set(_SESSION_INVENTORY_KEY, json.dumps(body, ensure_ascii=False))
    return {"ok": True, "machine_id": machine}


@router.get("/sessions/all_scanned", summary="Get all scanned sessions across nodes")
async def get_all_scanned(redis: RedisDep) -> dict[str, Any]:
    """
    Bridge endpoint: aggregates sessions from all three Redis sources into a
    unified sessions_by_machine map.

    Sources (in order of priority, deduplicated by redis_key):
    1. ``session:*``                  — session_manager process heartbeats (TTL 120s)
    2. ``nexus:sessions:*``           — deployer worker sessions
    3. ``nexus:session_vault:meta:*`` — vault Telethon session metadata
    """
    sessions_by_machine: dict[str, list[dict[str, Any]]] = {}
    seen_keys: set[str] = set()

    def _add(machine: str, entry: dict[str, Any]) -> None:
        rk = entry.get("redis_key", "")
        if rk and rk in seen_keys:
            return
        if rk:
            seen_keys.add(rk)
        sessions_by_machine.setdefault(machine, []).append(entry)

    # ── Source 1: session:* (session_manager heartbeats) ──────────────────────
    try:
        cursor = 0
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match="session:*", count=200)
            for key in keys:
                ks = key.decode() if isinstance(key, bytes) else str(key)
                raw = await redis.get(ks)
                if not raw:
                    continue
                try:
                    d = json.loads(raw)
                except Exception:
                    continue
                if not isinstance(d, dict):
                    continue
                machine = str(d.get("computer_name") or _machine_id()).strip() or _machine_id()
                _add(machine, {
                    "redis_key": ks,
                    "phone_number": str(d.get("session_id") or ""),
                    "origin_machine": machine,
                    "status": str(d.get("status") or "active"),
                    "last_scanned_target": "session_manager",
                    "last_seen": d.get("last_seen"),
                    "session_id": str(d.get("session_id") or ""),
                    "source": "session_manager",
                })
            if cursor == 0:
                break
    except Exception:
        pass

    # ── Source 2: nexus:sessions:* (deployer worker sessions) ─────────────────
    _skip_prefixes = (
        "nexus:sessions:all_scanned",
        "nexus:sessions:inventory",
        "nexus:session_vault",
    )
    try:
        cursor = 0
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match="nexus:sessions:*", count=200)
            for key in keys:
                ks = key.decode() if isinstance(key, bytes) else str(key)
                if any(ks.startswith(p) for p in _skip_prefixes):
                    continue
                raw = await redis.get(ks)
                if not raw:
                    continue
                try:
                    d = json.loads(raw)
                except Exception:
                    continue
                if not isinstance(d, dict):
                    continue
                machine = str(d.get("machine_id") or d.get("computer_name") or _machine_id()).strip()
                phone = str(d.get("phone") or d.get("phone_number") or d.get("session_id") or "")
                _add(machine, {
                    "redis_key": ks,
                    "phone_number": phone,
                    "origin_machine": machine,
                    "status": str(d.get("status") or "active"),
                    "last_scanned_target": "deployer",
                    "last_seen": d.get("last_seen") or d.get("last_heartbeat"),
                    "session_id": str(d.get("session_id") or ""),
                    "source": "deployer",
                })
            if cursor == 0:
                break
    except Exception:
        pass

    # ── Source 3: nexus:session_vault:meta:* (vault metadata) ─────────────────
    try:
        cursor = 0
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match="nexus:session_vault:meta:*", count=200)
            for key in keys:
                ks = key.decode() if isinstance(key, bytes) else str(key)
                raw = await redis.get(ks)
                if not raw:
                    continue
                try:
                    d = json.loads(raw)
                except Exception:
                    continue
                if not isinstance(d, dict):
                    continue
                machine = _machine_id()
                stem = ks.replace("nexus:session_vault:meta:", "")
                phone = str(d.get("phone") or d.get("phone_number") or "")
                username = str(d.get("username") or d.get("first_name") or stem)
                _add(machine, {
                    "redis_key": ks,
                    "phone_number": phone or username,
                    "origin_machine": machine,
                    "status": str(d.get("status") or d.get("health") or "unknown"),
                    "last_scanned_target": "vault",
                    "last_seen": d.get("last_seen") or d.get("probed_at"),
                    "session_id": stem,
                    "source": "vault",
                })
            if cursor == 0:
                break
    except Exception:
        pass

    machines = list(sessions_by_machine.keys())
    total = sum(len(v) for v in sessions_by_machine.values())
    return {
        "sessions_by_machine": sessions_by_machine,
        "machines": machines,
        "total": total,
        "is_mock": False,
    }


@router.post("/sessions/all_scanned", summary="Update all scanned sessions")
async def set_all_scanned(body: dict, redis: RedisDep) -> dict[str, Any]:
    machine = body.get("machine_id") or _machine_id()
    await redis.set(f"{_ALL_SCANNED_KEY}:{machine}", json.dumps(body, ensure_ascii=False))
    await redis.set(_ALL_SCANNED_KEY, json.dumps(body, ensure_ascii=False))
    return {"ok": True, "machine_id": machine}


# ── Live AI Swarm — real-time feed endpoints ──────────────────────────────────

_ISRAELI_LAST_MSG_KEY = "nexus:swarm:israeli:last_message"
_ISRAELI_VERIFIED_KEY = "nexus:swarm:israeli:verified_count"
_ISRAELI_WRITTEN_KEY = "nexus:swarm:israeli:written_count"
_ISRAELI_EVENTS_KEY = "nexus:swarm:israeli:events"
_ISRAELI_STATUS_KEY = "nexus:swarm:israeli:status"
_ISRAELI_LAST_ENGINE_ERROR_KEY = "nexus:swarm:israeli:last_engine_error"
_ISRAELI_POKE_KEY = "nexus:swarm:israeli:poke"


def _redis_db_index_for_log() -> str:
    try:
        p = urlparse(settings.redis_url)
        seg = (p.path or "/0").strip("/").split("/")[0]
        return seg if seg.isdigit() else (seg or "0")
    except Exception:
        return "?"


async def _append_swarm_feed_line(
    redis: Any,
    message: str,
    *,
    topic: str = "api",
    engine: str = "nexus_api",
) -> None:
    """Visible in GET /live-feed recent_messages (phone empty → no fake bot row)."""
    payload = json.dumps(
        {
            "ts": datetime.now(timezone.utc).isoformat(),
            "phone": "",
            "message": message,
            "topic": topic,
            "engine": engine,
        },
        ensure_ascii=False,
    )
    await redis.rpush(_ISRAELI_EVENTS_KEY, payload)
    await redis.ltrim(_ISRAELI_EVENTS_KEY, -500, -1)
    try:
        await redis.publish("nexus:swarm:events", payload)
    except Exception:
        pass


# #region agent log
def _dbg_swarm43(location: str, message: str, data: dict[str, Any], hypothesis_id: str) -> None:
    import time as _t

    try:
        _p = pathlib.Path(__file__).resolve().parents[3] / "debug-43baa8.log"
        with open(_p, "a", encoding="utf-8") as _f:
            _f.write(
                json.dumps(
                    {
                        "sessionId": "43baa8",
                        "location": location,
                        "message": message,
                        "data": data,
                        "timestamp": int(_t.time() * 1000),
                        "hypothesisId": hypothesis_id,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
    except Exception:
        pass


# #endregion


class SwarmStartBody(BaseModel):
    target_group: str = Field(default="", description="Telegram group invite link or @username")


@router.get("/live-feed", summary="Live feed snapshot for the Live AI Swarm tab")
async def get_live_feed(redis: RedisDep) -> dict[str, Any]:
    """
    Reads live state written by IsraeliSwarmEngine from Redis and returns
    the SwarmFeedData shape expected by the frontend LiveSwarmView component.
    """
    last_msg_raw = await redis.get(_ISRAELI_LAST_MSG_KEY)
    verified_raw = await redis.get(_ISRAELI_VERIFIED_KEY)
    written_raw = await redis.get(_ISRAELI_WRITTEN_KEY)
    status_raw = await redis.get(_ISRAELI_STATUS_KEY)
    last_engine_err_raw = await redis.get(_ISRAELI_LAST_ENGINE_ERROR_KEY)
    events_raw: list[str] = await redis.lrange(_ISRAELI_EVENTS_KEY, -20, -1)

    last_message = ""
    last_message_ts: float = 0.0
    last_sender_phone = ""
    if last_msg_raw:
        try:
            lm = json.loads(last_msg_raw)
            last_message = lm.get("message", "")
            last_sender_phone = lm.get("phone", "")
            ts_raw = lm.get("ts", "")
            if ts_raw:
                from datetime import datetime, timezone as _tz
                try:
                    last_message_ts = datetime.fromisoformat(ts_raw).replace(
                        tzinfo=_tz.utc
                    ).timestamp()
                except Exception:
                    pass
        except Exception:
            pass

    verified_count = int(verified_raw or 0)
    written_count = int(written_raw or 0)
    is_running = (status_raw or "").strip().lower() == "running"

    # Build deduplicated bot list from recent events
    bots_by_phone: dict[str, dict[str, Any]] = {}
    for ev_raw in events_raw:
        try:
            ev = json.loads(ev_raw)
            phone = ev.get("phone", "")
            if not phone:
                continue
            if phone not in bots_by_phone:
                bots_by_phone[phone] = {
                    "phone": phone,
                    "machine_id": ev.get("engine", "israeli_swarm"),
                    "is_active": True,
                    "messages_sent": 0,
                    "last_message": "",
                    "is_king": False,
                }
            bots_by_phone[phone]["messages_sent"] += 1
            bots_by_phone[phone]["last_message"] = ev.get("message", "")
        except Exception:
            pass

    bots = list(bots_by_phone.values())
    active_talkers = len([b for b in bots if b["is_active"]])

    # total_sessions: count .session files in vault/sessions if accessible
    total_sessions = 0
    try:
        import pathlib as _pathlib
        import os as _os
        vault_sessions = _pathlib.Path(
            _os.getenv("VAULT_SESSIONS_DIR", "") or
            _pathlib.Path(__file__).resolve().parent.parent.parent.parent / "vault" / "sessions"
        )
        if vault_sessions.is_dir():
            total_sessions = sum(1 for _ in vault_sessions.glob("*.session"))
    except Exception:
        pass

    # recent_messages: last 10 events as a message feed
    recent_messages: list[dict[str, Any]] = []
    for ev_raw in events_raw[-10:]:
        try:
            ev = json.loads(ev_raw)
            if ev.get("message"):
                recent_messages.append({
                    "phone": ev.get("phone", ""),
                    "message": ev.get("message", ""),
                    "topic": ev.get("topic", ""),
                    "ts": ev.get("ts", ""),
                })
        except Exception:
            pass

    last_engine_error = ""
    if last_engine_err_raw:
        last_engine_error = (
            last_engine_err_raw.decode("utf-8", errors="replace")
            if isinstance(last_engine_err_raw, bytes)
            else str(last_engine_err_raw)
        )

    # #region agent log
    _sr = status_raw
    if _sr is None:
        _status_preview = ""
    elif isinstance(_sr, bytes):
        _status_preview = _sr.decode("utf-8", errors="replace")[:80]
    else:
        _status_preview = str(_sr)[:80]
    _dbg_swarm43(
        "swarm.py:get_live_feed",
        "live_feed_snapshot",
        {
            "is_running": is_running,
            "status_raw": _status_preview,
            "events_len": len(events_raw),
            "bots_len": len(bots),
            "total_sessions": total_sessions,
            "last_engine_error_len": len(last_engine_error),
            "last_engine_error_prefix": last_engine_error[:200],
        },
        "H1",
    )
    # #endregion

    return {
        "total_in_group": len(bots),
        "active_talkers": active_talkers,
        "last_message": last_message,
        "last_message_ts": last_message_ts,
        "last_sender_phone": last_sender_phone,
        "is_running": is_running,
        "bots": bots,
        "verified_count": verified_count,
        "written_count": written_count,
        "total_sessions": total_sessions,
        "recent_messages": recent_messages,
        "last_engine_error": last_engine_error,
    }


@router.post("/start", summary="Signal the Live AI Swarm to start")
async def start_swarm(body: SwarmStartBody, redis: RedisDep) -> dict[str, Any]:
    """
    Marks the swarm as running in Redis. The actual IsraeliSwarmEngine process
    is managed by the launcher via SWARM_GROUP_LINK env var; this endpoint
    updates the status flag so the UI reflects the correct state.
    """
    await redis.set(_ISRAELI_STATUS_KEY, "running")
    if body.target_group:
        await redis.set("nexus:swarm:israeli:target_group", body.target_group)
    log.info("swarm_start_requested", target_group=body.target_group or "(env)")

    redis_ping = "לא ידוע"
    try:
        if await redis.ping():
            redis_ping = "OK"
        else:
            redis_ping = "לא תקין"
    except Exception as exc:
        redis_ping = f"שגיאה: {type(exc).__name__}"

    tg = (body.target_group or "").strip()
    tg_preview = (tg[:48] + "…") if len(tg) > 48 else tg
    _dbx = _redis_db_index_for_log()
    if tg_preview:
        start_line = (
            f"[דשבורד] הנחיל הופעל — סטטוס Redis=running · PING={redis_ping} · DB={_dbx} · "
            f"יעד נשמר: {tg_preview}"
        )
    else:
        start_line = (
            f"[דשבורד] הנחיל הופעל — סטטוס Redis=running · PING={redis_ping} · DB={_dbx} · "
            "יעד קבוצה: (לא נשלח בבקשה — המנוע יקרא מ-SWARM_GROUP_LINK אם קיים)"
        )
    await _append_swarm_feed_line(redis, start_line, topic="api_start")
    await redis.set(_ISRAELI_POKE_KEY, "1", ex=120)
    await _append_swarm_feed_line(
        redis,
        "[דשבורד] נשלחה התרעה למנוע (poke) — מחזור פעילות אמור להתחיל תוך עד ~5 שניות",
        topic="api_start",
    )

    # #region agent log
    _dbg_swarm43(
        "swarm.py:start_swarm",
        "redis_marked_running",
        {"target_group_len": len((body.target_group or "").strip())},
        "H2",
    )
    # #endregion
    return {"ok": True, "status": "running"}


@router.post("/stop", summary="Signal the Live AI Swarm to stop")
async def stop_swarm(redis: RedisDep) -> dict[str, Any]:
    """
    Marks the swarm as stopped in Redis so the UI reflects the correct state.
    """
    await redis.set(_ISRAELI_STATUS_KEY, "stopped")
    await redis.delete(_ISRAELI_POKE_KEY)
    log.info("swarm_stop_requested")
    redis_ping = "לא ידוע"
    try:
        if await redis.ping():
            redis_ping = "OK"
        else:
            redis_ping = "לא תקין"
    except Exception as exc:
        redis_ping = f"שגיאה: {type(exc).__name__}"
    _dbx = _redis_db_index_for_log()
    await _append_swarm_feed_line(
        redis,
        f"[דשבורד] הנחיל הופסק — סטטוס Redis=stopped · PING={redis_ping} · DB={_dbx}",
        topic="api_stop",
    )
    return {"ok": True, "status": "stopped"}


# ── Community Factory (Israeli Swarm) — allocate / create / join / chat ───────

FACTORY_ROLES_KEY = "nexus:swarm:factory:roles"
FACTORY_GROUPS_KEY = "nexus:swarm:factory:groups"
FACTORY_STATE_KEY = "nexus:swarm:factory:state"
FACTORY_BANNED_KEY = "nexus:swarm:factory:banned"
FACTORY_COOLDOWNS_KEY = "nexus:swarm:factory:cooldowns"
FACTORY_METRICS_KEY = "nexus:swarm:factory:metrics"


class CommunityFactoryInitiateBody(BaseModel):
    sessions_dir: str = Field(default="", description="Directory of *.session files; default vault/sessions")
    phases: list[str] = Field(
        default_factory=lambda: ["allocate", "create", "join", "chat"],
        description="allocate | create | join | chat",
    )
    dry_run: bool = Field(default=False, description="Compute roles only; do not write Redis or enqueue")
    reset: bool = Field(default=False, description="Clear factory Redis keys before run")
    max_joins_per_tick: int = Field(default=1, ge=1, le=50)
    converse_chain_limit: int = Field(default=5000, ge=1, le=1_000_000)


@router.post("/initiate", summary="Start Community Factory pipeline (enqueue bootstrap task)")
async def community_factory_initiate(
    body: CommunityFactoryInitiateBody,
    redis: RedisDep,
) -> dict[str, Any]:
    """
    Enqueues ``swarm.community_factory.bootstrap`` on the ARQ worker queue.
    Requires a running worker with Telethon sessions and API credentials.
    """
    try:
        import arq
        from arq.connections import RedisSettings

        from nexus.shared.schemas import TaskPayload
    except ImportError as exc:
        raise HTTPException(status_code=500, detail=f"ARQ not available: {exc}") from exc

    params: dict[str, Any] = {
        "sessions_dir": body.sessions_dir.strip(),
        "phases": [str(p).lower() for p in body.phases],
        "dry_run": body.dry_run,
        "reset": body.reset,
        "max_joins_per_tick": body.max_joins_per_tick,
        "converse_chain_limit": body.converse_chain_limit,
    }

    if body.dry_run:
        # Run allocation logic synchronously via a lightweight inline import
        from nexus.worker.tasks.swarm import _discover_session_bases, _resolve_sessions_dir, _split_roles

        d = _resolve_sessions_dir(body.sessions_dir.strip() or None)
        bases = _discover_session_bases(d)
        owners, members = _split_roles(bases)
        return {
            "ok": True,
            "dry_run": True,
            "task_id": None,
            "sessions_dir": str(d),
            "total_sessions": len(bases),
            "owners": len(owners),
            "members": len(members),
            "roles": {"owners": owners, "members": members},
        }

    task_id = str(uuid.uuid4())
    task = TaskPayload(
        task_id=task_id,
        task_type="swarm.community_factory.bootstrap",
        parameters=params,
        project_id="community-factory",
        priority=3,
        job_expires_seconds=900,
    )

    try:
        arq_pool = await arq.create_pool(
            RedisSettings.from_dsn(settings.redis_url),
            default_queue_name="nexus:tasks",
        )
        job_ttl = int(task.job_expires_seconds or 900)
        job = await arq_pool.enqueue_job(
            "execute_task",
            task_payload=task.model_dump_for_wire(),
            _job_id=task_id,
            _queue_name="nexus:tasks",
            _expires=job_ttl,
        )
        await arq_pool.aclose()
    except Exception as exc:
        log.error("community_factory_initiate_enqueue_failed", error=str(exc))
        raise HTTPException(status_code=502, detail=f"Failed to enqueue task: {exc}") from exc

    log.info("community_factory_initiate_enqueued", task_id=task_id, job_id=getattr(job, "job_id", None))
    return {
        "ok": True,
        "task_id": task_id,
        "job_id": getattr(job, "job_id", None),
        "task_type": task.task_type,
        "parameters": {k: v for k, v in params.items() if k != "__redis__"},
    }


@router.get("/community-factory/status", summary="Community Factory metrics and state")
async def community_factory_status(redis: RedisDep) -> dict[str, Any]:
    roles_raw = await redis.get(FACTORY_ROLES_KEY)
    groups_raw = await redis.get(FACTORY_GROUPS_KEY)
    state_raw = await redis.get(FACTORY_STATE_KEY)
    metrics_raw = await redis.get(FACTORY_METRICS_KEY)
    banned_raw = await redis.get(FACTORY_BANNED_KEY)

    def _loads(raw: str | bytes | None) -> Any:
        if not raw:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            return json.loads(raw)
        except Exception:
            return None

    roles = _loads(roles_raw)
    groups = _loads(groups_raw)
    state = _loads(state_raw)
    metrics = _loads(metrics_raw)
    banned = _loads(banned_raw)

    if not isinstance(metrics, dict):
        metrics = {}

    owners_n = len(roles.get("owners", [])) if isinstance(roles, dict) else 0
    members_n = len(roles.get("members", [])) if isinstance(roles, dict) else 0
    total_sessions = owners_n + members_n
    groups_n = len(groups) if isinstance(groups, list) else 0

    join_attempts = int(metrics.get("join_attempts", 0) or 0)
    floods = int(metrics.get("flood_waits", 0) or 0)
    bans = int(metrics.get("bans", 0) or 0)
    err_denom = max(1, join_attempts)
    error_rate = round((floods + bans) / err_denom, 6)

    return {
        "phase": (state or {}).get("phase") if isinstance(state, dict) else None,
        "state": state if isinstance(state, dict) else {},
        "total_groups": groups_n,
        "total_sessions": total_sessions,
        "owners_count": owners_n,
        "members_count": members_n,
        "active_sessions": int(metrics.get("active_sessions", total_sessions)),
        "messages_sent": int(metrics.get("messages_sent", 0) or 0),
        "joins_ok": int(metrics.get("joins_ok", 0) or 0),
        "joins_failed": int(metrics.get("joins_failed", 0) or 0),
        "join_attempts": join_attempts,
        "flood_waits": floods,
        "bans": bans,
        "error_rate": error_rate,
        "banned_count": len(banned) if isinstance(banned, list) else 0,
        "metrics": metrics,
    }
