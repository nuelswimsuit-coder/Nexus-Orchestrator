"""
Content Factory API endpoints.

GET  /api/content/previews        — List AI-generated content awaiting approval.
POST /api/content/resolve         — Approve (post) or reject a content preview.
POST /api/content/generate        — Trigger a new content_factory task.
GET  /api/content/factory-active  — Is a content factory job currently running?
"""

from __future__ import annotations

import json
import uuid

import structlog
from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel

from nexus.worker.tasks.content_factory import (
    CONTENT_ACTIVE_KEY,
    CONTENT_PREVIEWS_KEY,
    CONTENT_STATUS_KEY,
)

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/content", tags=["content"])


# ── Response models ────────────────────────────────────────────────────────────

class ContentPreviewItem(BaseModel):
    preview_id: str
    project_id: str
    target_group_id: str
    niche: str
    post_text: str
    image_path: str | None
    requires_hitl: bool
    hitl_reason: str
    status: str
    created_at: str


class ContentPreviewsResponse(BaseModel):
    previews: list[ContentPreviewItem]
    total: int


class ContentResolveRequest(BaseModel):
    preview_id: str
    action: str   # "approve" | "reject" | "regenerate"
    reviewer_id: str = "dashboard"


class ContentResolveResponse(BaseModel):
    preview_id: str
    action: str
    message: str


class ContentGenerateRequest(BaseModel):
    project_id: str = "telefix"
    target_group_id: str
    custom_text: str = ""
    force: bool = False


class ContentGenerateResponse(BaseModel):
    task_id: str
    message: str


class FactoryActiveResponse(BaseModel):
    active: bool
    status: str
    detail: str


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get(
    "/previews",
    response_model=ContentPreviewsResponse,
    summary="List AI-generated content awaiting approval",
)
async def get_previews(request: Request) -> ContentPreviewsResponse:
    """
    Return all content previews stored in Redis.
    Previews include both HITL-flagged posts (require approval) and
    successfully posted content (for the live feed).
    """
    redis = request.app.state.redis
    raw_items = await redis.lrange(CONTENT_PREVIEWS_KEY, 0, 49)

    previews: list[ContentPreviewItem] = []
    for raw in raw_items:
        try:
            data = json.loads(raw)
            previews.append(ContentPreviewItem(**data))
        except Exception:
            pass

    return ContentPreviewsResponse(previews=previews, total=len(previews))


@router.post(
    "/resolve",
    response_model=ContentResolveResponse,
    summary="Approve, reject, or regenerate a content preview",
)
async def resolve_preview(
    body: ContentResolveRequest,
    request: Request,
) -> ContentResolveResponse:
    """
    Act on a pending content preview.

    - approve    → dispatch the content_factory task with post_now=True
    - reject     → remove the preview from the queue
    - regenerate → dispatch a new content_factory task for the same group
    """
    redis = request.app.state.redis

    # Find and remove the preview from Redis
    raw_items = await redis.lrange(CONTENT_PREVIEWS_KEY, 0, 49)
    target_raw: str | None = None
    target_data: dict | None = None

    for raw in raw_items:
        try:
            data = json.loads(raw)
            if data.get("preview_id") == body.preview_id:
                target_raw = raw
                target_data = data
                break
        except Exception:
            pass

    if target_data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Preview {body.preview_id!r} not found",
        )

    if body.action == "reject":
        if target_raw:
            await redis.lrem(CONTENT_PREVIEWS_KEY, 1, target_raw)
        log.info("content_preview_rejected",
            preview_id=body.preview_id, reviewer=body.reviewer_id)
        return ContentResolveResponse(
            preview_id=body.preview_id,
            action="reject",
            message="Content preview rejected and removed.",
        )

    if body.action in ("approve", "regenerate"):
        from arq.connections import RedisSettings

        from nexus.shared.config import settings as nexus_settings
        from nexus.shared.schemas import TaskPayload

        task_id = str(uuid.uuid4())
        params: dict = {
            "project_id":      target_data["project_id"],
            "target_group_id": target_data["target_group_id"],
            "force": True,
        }
        if body.action == "approve":
            # Re-use the already-generated text; skip HITL gate
            params["custom_text"] = target_data["post_text"]
            params["post_now"] = True
            if target_raw:
                await redis.lrem(CONTENT_PREVIEWS_KEY, 1, target_raw)

        task = TaskPayload(
            task_id=task_id,
            task_type="telegram.content_factory",
            parameters=params,
            project_id=target_data["project_id"],
            priority=2,
        )

        try:
            import arq
            arq_pool = await arq.create_pool(
                RedisSettings.from_dsn(nexus_settings.redis_url),
                default_queue_name="nexus:tasks",
            )
            await arq_pool.enqueue_job(
                "execute_task",
                task_payload=task.model_dump_for_wire(),
                _job_id=task_id,
                _queue_name="nexus:tasks",
            )
            await arq_pool.aclose()
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Could not enqueue task: {exc}",
            ) from exc

        action_label = (
            "approved and queued for posting"
            if body.action == "approve"
            else "regeneration queued"
        )
        log.info("content_preview_resolved",
            preview_id=body.preview_id, action=body.action, task_id=task_id)
        return ContentResolveResponse(
            preview_id=body.preview_id,
            action=body.action,
            message=f"Content {action_label}. Task ID: {task_id}",
        )

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Unknown action: {body.action!r}. Use 'approve', 'reject', or 'regenerate'.",
    )


@router.post(
    "/generate",
    response_model=ContentGenerateResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger a new content generation task",
)
async def generate_content(
    body: ContentGenerateRequest,
    request: Request,
) -> ContentGenerateResponse:
    """
    Enqueue a telegram.content_factory task immediately.
    The task will generate text + image and store a preview in Redis.
    """
    from arq.connections import RedisSettings

    from nexus.shared.config import settings as nexus_settings
    from nexus.shared.schemas import TaskPayload

    task_id = str(uuid.uuid4())
    task = TaskPayload(
        task_id=task_id,
        task_type="telegram.content_factory",
        parameters={
            "project_id":      body.project_id,
            "target_group_id": body.target_group_id,
            "custom_text":     body.custom_text,
            "force":           body.force,
        },
        project_id=body.project_id,
        priority=2,
    )

    try:
        import arq
        arq_pool = await arq.create_pool(
            RedisSettings.from_dsn(nexus_settings.redis_url),
            default_queue_name="nexus:tasks",
        )
        await arq_pool.enqueue_job(
            "execute_task",
            task_payload=task.model_dump_for_wire(),
            _job_id=task_id,
            _queue_name="nexus:tasks",
        )
        await arq_pool.aclose()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Could not enqueue task: {exc}",
        ) from exc

    log.info("content_factory_triggered", task_id=task_id, group=body.target_group_id)
    return ContentGenerateResponse(
        task_id=task_id,
        message="Content generation task queued. Check /api/content/previews for results.",
    )


@router.get(
    "/factory-active",
    response_model=FactoryActiveResponse,
    summary="Check if a content factory job is currently running",
)
async def get_factory_active(request: Request) -> FactoryActiveResponse:
    """
    Returns whether a content_factory task is actively running.
    Used by the dashboard to show the indigo 'Thinking' glow on the monitor.
    """
    redis = request.app.state.redis
    active_flag = await redis.get(CONTENT_ACTIVE_KEY)
    status_raw = await redis.get(CONTENT_STATUS_KEY)

    status_str = "idle"
    detail = ""
    if status_raw:
        try:
            d = json.loads(status_raw)
            status_str = d.get("status", "idle")
            detail = d.get("detail", "")
        except Exception:
            pass

    return FactoryActiveResponse(
        active=active_flag is not None,
        status=status_str,
        detail=detail,
    )
