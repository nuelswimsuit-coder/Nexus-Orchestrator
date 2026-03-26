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
    Returns sessions_by_machine shape expected by the dashboard.
    """
    sessions_by_machine: dict[str, list[dict[str, Any]]] = {}

    cursor = 0
    while True:
        cursor, keys = await redis.scan(cursor=cursor, match="nexus:sessions:all_scanned:*", count=100)
        for key in keys:
            raw = await redis.get(key)
            if not raw:
                continue
            try:
                node_data = json.loads(raw)
                machine = key.replace("nexus:sessions:all_scanned:", "")
                sessions = node_data if isinstance(node_data, list) else node_data.get("sessions", [])
                if sessions:
                    sessions_by_machine[machine] = sessions
            except Exception:
                pass
        if cursor == 0:
            break

    raw_legacy = await redis.get(_ALL_SCANNED_KEY)
    if raw_legacy:
        try:
            legacy = json.loads(raw_legacy)
            if isinstance(legacy, dict) and "sessions_by_machine" in legacy:
                for m, sessions in (legacy.get("sessions_by_machine") or {}).items():
                    if m not in sessions_by_machine:
                        sessions_by_machine[m] = sessions
            elif isinstance(legacy, list):
                mid = _machine_id()
                if mid not in sessions_by_machine:
                    sessions_by_machine[mid] = legacy
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
