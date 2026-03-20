"""
HITL (Human-in-the-Loop) endpoints.

GET  /api/hitl/pending  — list tasks waiting for human approval.
POST /api/hitl/resolve  — approve or reject a specific pending task.

Flow
----
1. The master's HitlGate publishes a HitlRequest to Redis when a task needs
   approval.  The API's HitlStore (running in a background task) receives it
   and stores it in memory.
2. The dashboard polls GET /api/hitl/pending and renders the "Action Required"
   panel when items are present.
3. The operator clicks Approve/Reject.  The dashboard POSTs to
   /api/hitl/resolve with the decision.
4. HitlStore publishes a HitlResponse to Redis.  The master's HitlGate
   receives it, resolves the asyncio.Event, and the suspended dispatch()
   call either enqueues the task or raises TaskRejectedError.
"""

from __future__ import annotations

import structlog
from fastapi import APIRouter, HTTPException, Request, status

from nexus.api.dependencies import HitlStoreDep
from nexus.api.schemas import (
    HitlPendingItem,
    HitlPendingResponse,
    HitlResolveRequest,
    HitlResolveResponse,
)

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/hitl", tags=["hitl"])


@router.get(
    "/pending",
    response_model=HitlPendingResponse,
    summary="List tasks awaiting human approval",
)
async def get_pending(hitl: HitlStoreDep) -> HitlPendingResponse:
    """
    Return all HITL requests that have not yet been resolved.

    Syncs from Redis durable keys on every call so requests published
    before the API started (or missed via pub/sub) are always included.
    """
    # Sync from durable Redis keys — recovers requests the pub/sub missed
    await hitl.refresh()

    items = [
        HitlPendingItem(
            request_id=req.request_id,
            task_id=req.task_id,
            task_type=req.task_type,
            context=req.context,
            requested_at=req.requested_at,
            expires_at=req.expires_at,
        )
        for req in hitl.pending()
    ]
    return HitlPendingResponse(items=items, total=len(items))


@router.post(
    "/resolve",
    response_model=HitlResolveResponse,
    status_code=status.HTTP_200_OK,
    summary="Approve or reject a pending HITL task",
)
async def resolve_hitl(
    body: HitlResolveRequest, hitl: HitlStoreDep, request: Request
) -> HitlResolveResponse:
    """
    Submit a human decision for a pending HITL request.

    - `approved: true`  → the master will enqueue the task for execution.
    - `approved: false` → the master will raise TaskRejectedError and skip it.

    Returns 404 if the request_id is not found (already resolved or expired).
    """
    try:
        response = await hitl.resolve(
            request_id=body.request_id,
            approved=body.approved,
            reviewer_id=body.reviewer_id,
            reason=body.reason,
        )
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc

    action = "approved" if body.approved else "rejected"
    log.info(
        "hitl_resolved_via_api",
        request_id=body.request_id,
        action=action,
        reviewer=body.reviewer_id,
    )

    # ── Auto-thresholding: record approval streak ──────────────────────────────
    # Infer the action_type from the task_type (e.g. "telegram.auto_scrape" → "scale_scrape")
    # We use the task_type from the resolved request to look up the action type.
    try:
        from nexus.master.services.decision_engine import (
            APPROVAL_STREAK_KEY,
            APPROVAL_STREAK_THRESHOLD,
            HITL_THRESHOLD,
            MIN_THRESHOLD,
            THRESHOLD_OVERRIDE_PREFIX,
            THRESHOLD_OVERRIDE_TTL,
            THRESHOLD_REDUCTION,
        )
        redis = request.app.state.redis
        # Map task_type to action_type for streak tracking
        task_type_to_action = {
            "telegram.auto_scrape": "scale_scrape",
            "telegram.auto_add":    "scale_add",
            "telegram.run_warmup":  "emergency_warmup",
            "telegram.super_scrape": "scale_scrape",
            "nexus.scale_worker":   "scale_workers",
        }
        action_type = task_type_to_action.get(response.task_id, "")
        # Fallback: use task_type directly if no mapping
        if not action_type:
            action_type = response.task_id.replace("telegram.", "").replace(".", "_")

        if action_type and redis:
            if body.approved:
                new_streak = await redis.hincrby(APPROVAL_STREAK_KEY, action_type, 1)
                if new_streak >= APPROVAL_STREAK_THRESHOLD:
                    override_key = f"{THRESHOLD_OVERRIDE_PREFIX}{action_type}"
                    current_raw = await redis.get(override_key)
                    current_t = int(current_raw) if current_raw else HITL_THRESHOLD
                    new_t = max(MIN_THRESHOLD, current_t - THRESHOLD_REDUCTION)
                    await redis.set(override_key, str(new_t), ex=THRESHOLD_OVERRIDE_TTL)
                    await redis.hset(APPROVAL_STREAK_KEY, action_type, 0)
                    log.info(
                        "auto_threshold_lowered_via_hitl",
                        action_type=action_type,
                        old_threshold=current_t,
                        new_threshold=new_t,
                    )
            else:
                await redis.hset(APPROVAL_STREAK_KEY, action_type, 0)
    except Exception as exc:
        log.warning("approval_streak_tracking_error", error=str(exc))

    return HitlResolveResponse(
        request_id=response.request_id,
        task_id=response.task_id,
        approved=response.approved,
        reviewer_id=response.reviewer_id,
        responded_at=response.responded_at,
        message=f"Task {response.task_id} has been {action}.",
    )
