"""
Incubator API — Evolution Engine endpoints.

Exposes the Scout (niche discovery) and Architect (project generation)
services via REST endpoints consumed by the /incubator dashboard page.

Endpoints
---------
GET  /api/incubator/niches          — Top 3 discovered niches (Scout output)
POST /api/incubator/niches/refresh  — Trigger a fresh Scout scan
GET  /api/incubator/projects        — All AI-born projects
POST /api/incubator/generate        — Generate a new project from a niche
POST /api/incubator/approve/{id}    — Approve a pending_review project
POST /api/incubator/kill/{id}       — Kill a live project
GET  /api/incubator/god-mode        — Get GOD MODE status
POST /api/incubator/god-mode        — Set GOD MODE on/off
POST /api/incubator/kill-switch     — Emergency: stop ALL autonomous projects
POST /api/incubator/kill-switch/clear — Clear kill switch
GET  /api/incubator/state           — Current Architect/Scout state
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/incubator", tags=["incubator"])


# ── Response models ───────────────────────────────────────────────────────────

class NicheItem(BaseModel):
    name: str
    source: str
    keywords: list[str]
    volume_score: float
    velocity_score: float
    monetisation_score: float
    composite: float
    confidence: int
    roi_estimate: str
    discovered_at: str
    raw_data: dict = {}


class NichesResponse(BaseModel):
    niches: list[NicheItem]
    total: int
    last_run: str | None
    state: str


class ProjectItem(BaseModel):
    project_id: str
    name: str
    slug: str
    niche: str
    niche_source: str
    generation: int
    status: str
    path: str
    born_at: str
    last_updated: str
    confidence_at_birth: int
    estimated_roi: str
    files_generated: list[str]
    stats: dict = {}
    god_mode_deployed: bool
    age_hours: float = 0.0


class ProjectsResponse(BaseModel):
    projects: list[ProjectItem]
    total: int


class GenerateRequest(BaseModel):
    niche_name: str
    keywords: list[str] = []
    roi_estimate: str = "Unknown"
    confidence: int = 70
    source: str = "manual"
    custom_brief: str = ""


class GenerateResponse(BaseModel):
    project_id: str
    name: str
    slug: str
    status: str
    path: str
    message: str


class GodModeRequest(BaseModel):
    enabled: bool


class GodModeResponse(BaseModel):
    enabled: bool
    message: str


class IncubatorStateResponse(BaseModel):
    architect_state: str
    scout_state: str
    god_mode: bool
    total_projects: int
    live_projects: int


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_architect(request: Request):
    if not hasattr(request.app.state, "architect"):
        from nexus.master.services.architect import ArchitectService
        request.app.state.architect = ArchitectService(request.app.state.redis)
    return request.app.state.architect


# ── Niche endpoints ───────────────────────────────────────────────────────────

@router.get(
    "/niches",
    response_model=NichesResponse,
    summary="Top 3 high-ROI Telegram niches discovered by the Scout",
)
async def get_niches(request: Request) -> NichesResponse:
    from nexus.master.services.scout import (
        SCOUT_LAST_RUN_KEY,
        SCOUT_STATE_KEY,
        get_current_niches,
    )
    redis = request.app.state.redis
    niches_data = await get_current_niches(redis)
    last_run = await redis.get(SCOUT_LAST_RUN_KEY)
    state = await redis.get(SCOUT_STATE_KEY) or "idle"
    return NichesResponse(
        niches=[NicheItem(**n) for n in niches_data],
        total=len(niches_data),
        last_run=last_run,
        state=state,
    )


@router.post(
    "/niches/refresh",
    summary="Trigger a fresh Scout scan (async)",
    status_code=status.HTTP_202_ACCEPTED,
)
async def refresh_niches(request: Request) -> dict:
    import asyncio
    from nexus.master.services.scout import _run_scout_cycle
    redis = request.app.state.redis
    asyncio.create_task(_run_scout_cycle(redis))
    return {"message": "Scout scan started — poll /api/incubator/niches for results."}


# ── Project endpoints ─────────────────────────────────────────────────────────

@router.get(
    "/projects",
    response_model=ProjectsResponse,
    summary="All AI-born incubator projects",
)
async def get_projects(request: Request) -> ProjectsResponse:
    architect = _get_architect(request)
    projects_data = await architect.get_all_projects()
    items: list[ProjectItem] = []
    for p in projects_data:
        try:
            born = datetime.fromisoformat(p.get("born_at", ""))
            age_h = (datetime.now(timezone.utc) - born).total_seconds() / 3600
        except Exception:
            age_h = 0.0
        items.append(ProjectItem(**{**p, "age_hours": age_h}))
    return ProjectsResponse(projects=items, total=len(items))


@router.post(
    "/generate",
    response_model=GenerateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Generate a new project from a niche (AI Architect)",
)
async def generate_project(body: GenerateRequest, request: Request) -> GenerateResponse:
    architect = _get_architect(request)
    niche_dict = {
        "name": body.niche_name,
        "keywords": body.keywords,
        "roi_estimate": body.roi_estimate,
        "confidence": body.confidence,
        "source": body.source,
    }
    try:
        project = await architect.generate_project(niche_dict, custom_brief=body.custom_brief)
    except Exception as exc:
        log.error("incubator_generate_error", error=str(exc))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Project generation failed: {exc}",
        ) from exc

    god_mode = await architect.is_god_mode()
    msg = (
        "Project deployed automatically (GOD MODE ON)."
        if god_mode
        else "Project created — awaiting review."
    )
    return GenerateResponse(
        project_id=project.project_id,
        name=project.name,
        slug=project.slug,
        status=project.status,
        path=project.path,
        message=msg,
    )


@router.post("/approve/{project_id}", summary="Approve a pending_review project")
async def approve_project(project_id: str, request: Request) -> dict:
    architect = _get_architect(request)
    ok = await architect.approve_project(project_id)
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project '{project_id}' not found or not in pending_review state.",
        )
    return {"project_id": project_id, "status": "live", "message": "Project approved."}


@router.post("/kill/{project_id}", summary="Kill a live project")
async def kill_project(project_id: str, request: Request) -> dict:
    architect = _get_architect(request)
    ok = await architect.kill_project(project_id)
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project '{project_id}' not found.",
        )
    return {"project_id": project_id, "status": "killed", "message": "Project killed."}


# ── GOD MODE endpoints ────────────────────────────────────────────────────────

@router.get("/god-mode", response_model=GodModeResponse, summary="Get GOD MODE status")
async def get_god_mode(request: Request) -> GodModeResponse:
    architect = _get_architect(request)
    enabled = await architect.is_god_mode()
    return GodModeResponse(
        enabled=enabled,
        message="GOD MODE is ON — projects deploy without approval." if enabled
        else "GOD MODE is OFF — projects require human approval.",
    )


@router.post("/god-mode", response_model=GodModeResponse, summary="Enable or disable GOD MODE")
async def set_god_mode(body: GodModeRequest, request: Request) -> GodModeResponse:
    architect = _get_architect(request)
    await architect.set_god_mode(body.enabled)
    log.info("god_mode_toggled", enabled=body.enabled)
    return GodModeResponse(
        enabled=body.enabled,
        message="GOD MODE ACTIVATED — autonomous deployment enabled." if body.enabled
        else "GOD MODE DEACTIVATED — human approval required.",
    )


# ── Kill Switch endpoints ─────────────────────────────────────────────────────

@router.post(
    "/kill-switch",
    summary="Emergency Kill Switch — stop ALL autonomous projects instantly",
)
async def activate_kill_switch(request: Request) -> dict:
    """
    Activate the global Kill Switch:
    1. Sets nexus:incubator:kill_all flag in Redis (blocks future spawns)
    2. Kills all projects in the incubator list
    3. Disables GOD MODE
    """
    from nexus.worker.tasks.incubator_spawn import KILL_ALL_KEY

    redis = request.app.state.redis
    architect = _get_architect(request)

    await redis.set(KILL_ALL_KEY, "1", ex=3600)

    projects = await architect.get_all_projects()
    killed = 0
    for p in projects:
        if p.get("status") in ("live", "pending_review"):
            await architect.kill_project(p["project_id"])
            killed += 1

    await architect.set_god_mode(False)

    log.warning("incubator_kill_switch_activated_api", killed=killed)
    return {
        "status": "kill_switch_active",
        "projects_killed": killed,
        "god_mode_disabled": True,
        "message": f"Kill switch activated. {killed} projects stopped. GOD MODE disabled.",
    }


@router.post(
    "/kill-switch/clear",
    summary="Clear the Kill Switch to re-enable autonomous spawning",
)
async def clear_kill_switch(request: Request) -> dict:
    from nexus.worker.tasks.incubator_spawn import KILL_ALL_KEY
    redis = request.app.state.redis
    await redis.delete(KILL_ALL_KEY)
    log.info("incubator_kill_switch_cleared_api")
    return {"status": "cleared", "message": "Kill switch cleared. Autonomous spawning re-enabled."}


# ── State endpoint ────────────────────────────────────────────────────────────

@router.get(
    "/state",
    response_model=IncubatorStateResponse,
    summary="Current state of the Evolution Engine",
)
async def get_incubator_state(request: Request) -> IncubatorStateResponse:
    from nexus.master.services.architect import INCUBATOR_STATE_KEY
    from nexus.master.services.scout import SCOUT_STATE_KEY

    redis = request.app.state.redis
    architect = _get_architect(request)

    architect_state = await redis.get(INCUBATOR_STATE_KEY) or "idle"
    scout_state     = await redis.get(SCOUT_STATE_KEY) or "idle"
    god_mode        = await architect.is_god_mode()
    projects        = await architect.get_all_projects()
    live_count      = sum(1 for p in projects if p.get("status") == "live")

    return IncubatorStateResponse(
        architect_state=architect_state,
        scout_state=scout_state,
        god_mode=god_mode,
        total_projects=len(projects),
        live_projects=live_count,
    )
