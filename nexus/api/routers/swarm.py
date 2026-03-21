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
