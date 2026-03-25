"""Async helpers for the active project (Redis ``nexus:active_project``)."""

from __future__ import annotations

import json
from typing import Any

from redis.asyncio import Redis

from nexus.shared.active_project_scope import (
    GLOBAL_MISSION_REDIS_KEY,
    KNOWN_PROJECT_PRESETS,
    REDIS_ACTIVE_PROJECT_KEY,
    build_active_project_meta,
    default_display_name,
    normalize_project_id,
    parse_active_project_json,
    resolve_project_type,
)


async def load_active_project(redis: Redis) -> dict[str, Any]:
    """
    Return active project metadata, preferring ``nexus:active_project`` JSON,
    then ``global_mission`` string, then defaults (Management Ahu / telefix ops).
    """
    raw = await redis.get(REDIS_ACTIVE_PROJECT_KEY)
    parsed = parse_active_project_json(raw)
    if parsed:
        pid = normalize_project_id(str(parsed.get("project_id", "default")))
        dn = str(parsed.get("display_name") or default_display_name(pid, resolve_project_type(pid)))
        ptype = str(parsed.get("project_type") or resolve_project_type(pid, dn))
        return {
            "project_id": pid,
            "display_name": dn,
            "project_type": ptype,
            "updated_at": str(parsed.get("updated_at") or ""),
        }

    gm = await redis.get(GLOBAL_MISSION_REDIS_KEY)
    pid = normalize_project_id(str(gm or "telefix"))
    ptype = resolve_project_type(pid)
    return build_active_project_meta(pid, default_display_name(pid, ptype))


async def persist_active_project(redis: Redis, project_id: str, display_name: str | None) -> dict[str, Any]:
    """Write ``nexus:active_project`` and mirror mission string for cluster tools."""
    meta = build_active_project_meta(project_id, display_name)
    await redis.set(REDIS_ACTIVE_PROJECT_KEY, json.dumps(meta))
    await redis.set(GLOBAL_MISSION_REDIS_KEY, meta["project_id"])
    return meta


def list_known_projects() -> list[dict[str, str]]:
    return [dict(p) for p in KNOWN_PROJECT_PRESETS]
