"""
Evolution API — First-Birth Protocol endpoints.

Routes
------
GET  /api/evolution/incubator          — List all incubator projects
GET  /api/evolution/state              — Current evolution engine state
POST /api/evolution/birth-resolve      — Approve or reject a birth proposal
POST /api/evolution/scout              — Manually trigger a scout cycle
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel

from nexus.master.services.evolution import (
    BIRTH_APPROVED_KEY,
    EVOLUTION_STATE_KEY,
    INCUBATOR_KEY,
    IncubatorProject,
    ProjectStatus,
)

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/evolution", tags=["evolution"])


# ── Response models ────────────────────────────────────────────────────────────

class IncubatorProjectOut(BaseModel):
    project_id: str
    name: str
    niche_id: str
    niche_description: str
    ai_logic: str
    file_path: str
    estimated_roi_pct: int
    confidence: int
    status: str
    created_at: str
    updated_at: str
    hitl_request_id: str = ""
    deployed_worker_id: str = ""
    rejection_reason: str = ""


class IncubatorResponse(BaseModel):
    projects: list[IncubatorProjectOut]
    total: int
    first_birth_approved: bool
    queried_at: str


class EvolutionStateResponse(BaseModel):
    state: str
    updated_at: str
    first_birth_approved: bool


class BirthResolveRequest(BaseModel):
    request_id: str
    approved: bool
    reviewer_id: str = "operator"
    reason: str = ""


class BirthResolveResponse(BaseModel):
    request_id: str
    project_id: str
    approved: bool
    reviewer_id: str
    responded_at: str
    message: str


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get(
    "/incubator",
    response_model=IncubatorResponse,
    summary="List all incubator projects with their birth status",
)
async def get_incubator(request: Request) -> IncubatorResponse:
    redis = request.app.state.redis
    raw   = await redis.get(INCUBATOR_KEY)
    projects: list[IncubatorProject] = []

    if raw:
        try:
            items = json.loads(raw)
            projects = [IncubatorProject.from_dict(d) for d in items]
        except Exception as exc:
            log.error("incubator_load_error", error=str(exc))

    approved_flag = await redis.get(BIRTH_APPROVED_KEY)
    first_approved = approved_flag == "true"

    return IncubatorResponse(
        projects=[IncubatorProjectOut(**p.to_dict()) for p in projects],
        total=len(projects),
        first_birth_approved=first_approved,
        queried_at=datetime.now(timezone.utc).isoformat(),
    )


@router.get(
    "/state",
    response_model=EvolutionStateResponse,
    summary="Current evolution engine state",
)
async def get_evolution_state(request: Request) -> EvolutionStateResponse:
    redis = request.app.state.redis
    raw   = await redis.get(EVOLUTION_STATE_KEY)

    approved_flag  = await redis.get(BIRTH_APPROVED_KEY)
    first_approved = approved_flag == "true"

    if raw:
        try:
            data = json.loads(raw)
            return EvolutionStateResponse(
                state=data.get("state", "idle"),
                updated_at=data.get("updated_at", ""),
                first_birth_approved=first_approved,
            )
        except Exception:
            pass

    return EvolutionStateResponse(
        state="idle",
        updated_at="",
        first_birth_approved=first_approved,
    )


@router.post(
    "/birth-resolve",
    response_model=BirthResolveResponse,
    summary="Approve or reject a Project Birth Proposal",
)
async def resolve_birth(body: BirthResolveRequest, request: Request) -> BirthResolveResponse:
    """
    Called by the Telegram bot callback handler or the dashboard Incubator page
    when the operator clicks APPROVE or REJECT on a birth proposal.

    On approval:
      - Sets nexus:birth:approved = "true" (persistent, no TTL)
      - Triggers project deployment via the EvolutionEngine
      - Returns success message

    On rejection:
      - Marks the project as rejected
      - Triggers a new scout cycle (different niche)
    """
    redis = request.app.state.redis

    # Look up the pending birth request
    birth_key = f"nexus:birth:pending:{body.request_id}"
    raw = await redis.get(birth_key)
    if raw is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Birth request {body.request_id} not found or already resolved.",
        )

    try:
        birth_data = json.loads(raw)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not parse birth request: {exc}",
        ) from exc

    project_id = birth_data.get("project_id", "")

    # Resolve via the EvolutionEngine attached to app state (if available)
    engine = getattr(request.app.state, "evolution_engine", None)
    if engine is not None:
        await engine.handle_birth_approval(
            project_id  = project_id,
            approved    = body.approved,
            reviewer_id = body.reviewer_id,
            reason      = body.reason,
        )
    else:
        # Fallback: manually update Redis flags when engine is not running
        if body.approved:
            await redis.set(BIRTH_APPROVED_KEY, "true")

        raw_projects = await redis.get(INCUBATOR_KEY)
        if raw_projects:
            try:
                items = json.loads(raw_projects)
                projects = [IncubatorProject.from_dict(d) for d in items]
                now = datetime.now(timezone.utc).isoformat()
                for p in projects:
                    if p.project_id == project_id:
                        p.status     = ProjectStatus.DEPLOYING if body.approved else ProjectStatus.REJECTED
                        p.updated_at = now
                        if not body.approved:
                            p.rejection_reason = body.reason or "Rejected by operator"
                await redis.set(INCUBATOR_KEY, json.dumps([p.to_dict() for p in projects]))
            except Exception as exc:
                log.error("birth_resolve_update_error", error=str(exc))

    # Delete the pending request key
    await redis.delete(birth_key)

    now = datetime.now(timezone.utc).isoformat()
    if body.approved:
        message = "🚀 GOD MODE ENABLED — Project is deploying to an available Worker."
    else:
        message = "❌ Project rejected — Scout will regenerate a new proposal."

    log.info(
        "birth_resolved",
        request_id=body.request_id,
        project_id=project_id,
        approved=body.approved,
        reviewer=body.reviewer_id,
    )

    return BirthResolveResponse(
        request_id   = body.request_id,
        project_id   = project_id,
        approved     = body.approved,
        reviewer_id  = body.reviewer_id,
        responded_at = now,
        message      = message,
    )


@router.post(
    "/scout",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Manually trigger a Scout cycle",
)
async def trigger_scout(request: Request) -> dict:
    """
    Manually kick off a Scout → Architect → Birth Gate cycle.
    Useful for testing or forcing a new project proposal.
    """
    engine = getattr(request.app.state, "evolution_engine", None)
    if engine is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Evolution Engine is not running. Start the master node first.",
        )

    import asyncio
    asyncio.create_task(engine.run_once())

    return {"message": "Scout cycle triggered — check /api/evolution/incubator for results."}
