"""
Swarm Social Synthesis — dashboard API for community identity + group registry.
"""

from __future__ import annotations

import json
from typing import Any

import structlog
from fastapi import APIRouter
from pydantic import BaseModel, Field
from nexus.api.dependencies import RedisDep

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
    return {"ok": True, "status": "running"}


@router.post("/stop", summary="Signal the Live AI Swarm to stop")
async def stop_swarm(redis: RedisDep) -> dict[str, Any]:
    """
    Marks the swarm as stopped in Redis so the UI reflects the correct state.
    """
    await redis.set(_ISRAELI_STATUS_KEY, "stopped")
    log.info("swarm_stop_requested")
    return {"ok": True, "status": "stopped"}
