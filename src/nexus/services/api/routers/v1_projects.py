"""
API v1 — active project (cluster-wide dashboard scope).

GET  /api/v1/projects/active
PUT  /api/v1/projects/active
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from nexus.services.api.dependencies import RedisDep
from nexus.services.api.services.active_project import (
    list_known_projects,
    load_active_project,
    persist_active_project,
)
from nexus.shared.dashboard_context_store import ensure_project_dashboard_context

router = APIRouter(prefix="/v1/projects", tags=["projects-v1"])


class ActiveProjectResponse(BaseModel):
    project_id: str
    display_name: str
    project_type: str
    updated_at: str = ""
    available_projects: list[dict[str, str]] = Field(default_factory=list)
    dashboard_context: dict[str, Any] | None = None


class SetActiveProjectBody(BaseModel):
    project_id: str = Field(..., min_length=1, max_length=128)
    display_name: str | None = Field(default=None, max_length=256)


def _load_context_row(project_id: str) -> dict[str, Any] | None:
    import json
    import sqlite3

    from nexus.shared.dashboard_context_store import dashboard_db_path
    from nexus.shared.active_project_scope import normalize_project_id

    pid = normalize_project_id(project_id)
    path = dashboard_db_path()
    if not path.is_file():
        return None
    try:
        with sqlite3.connect(path, check_same_thread=False) as conn:
            row = conn.execute(
                "SELECT context_json FROM project_dashboard_context WHERE project_id = ?",
                (pid,),
            ).fetchone()
        if not row:
            return None
        return json.loads(row[0])
    except Exception:
        return None


@router.get("/active", response_model=ActiveProjectResponse, summary="Current active project (Redis)")
async def get_active_project(redis: RedisDep) -> ActiveProjectResponse:
    meta = await load_active_project(redis)
    ensure_project_dashboard_context(meta["project_id"], meta.get("display_name"))
    ctx = _load_context_row(meta["project_id"])
    return ActiveProjectResponse(
        project_id=meta["project_id"],
        display_name=meta["display_name"],
        project_type=meta["project_type"],
        updated_at=meta.get("updated_at", ""),
        available_projects=list_known_projects(),
        dashboard_context=ctx,
    )


@router.put("/active", response_model=ActiveProjectResponse, summary="Set active project cluster-wide")
async def put_active_project(body: SetActiveProjectBody, redis: RedisDep) -> ActiveProjectResponse:
    meta = await persist_active_project(redis, body.project_id, body.display_name)
    ensure_project_dashboard_context(meta["project_id"], meta.get("display_name"))
    ctx = _load_context_row(meta["project_id"])
    return ActiveProjectResponse(
        project_id=meta["project_id"],
        display_name=meta["display_name"],
        project_type=meta["project_type"],
        updated_at=meta["updated_at"],
        available_projects=list_known_projects(),
        dashboard_context=ctx,
    )
