"""
RankSEO Group Factory — API router.

Enqueue ``seo_group_factory`` on the worker queue and expose Redis-backed
report + progress for the UI.
"""

from __future__ import annotations

import uuid
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from nexus.api.dependencies import RedisDep
from nexus.shared.config import settings
from nexus.shared.schemas import TaskPayload
from nexus.shared.seo_group_factory import persist_seo_factory_snapshot

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/factory", tags=["factory"])


class StartSeoGroupsBody(BaseModel):
    """Optional overrides for Community Factory bootstrap (passed to the worker)."""

    sessions_dir: str = Field(default="", description="Telethon sessions directory; default vault layout")
    phases: list[str] = Field(
        default_factory=lambda: ["allocate", "create", "join", "chat"],
        description="Factory phases: allocate | create | join | chat",
    )
    reset: bool = Field(default=False, description="Clear factory Redis keys before run")
    dry_run: bool = Field(default=False, description="Allocation only; no Redis writes / enqueue")
    max_joins_per_tick: int = Field(default=1, ge=1, le=50)
    converse_chain_limit: int = Field(default=5000, ge=1, le=1_000_000)


@router.post(
    "/start-seo-groups",
    summary="Start RankSEO group factory (async worker job)",
)
async def start_seo_groups(body: StartSeoGroupsBody) -> dict[str, Any]:
    """
    Enqueue ``seo_group_factory`` on ARQ. Returns immediately after the job is queued.
    """
    try:
        import arq  # type: ignore[import-untyped]
        from arq.connections import RedisSettings  # type: ignore[import-untyped]
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

    task_id = str(uuid.uuid4())
    task = TaskPayload(
        task_id=task_id,
        task_type="seo_group_factory",
        parameters=params,
        project_id="rankseo-factory",
        priority=3,
        job_expires_seconds=3600,
    )

    try:
        arq_pool = await arq.create_pool(
            RedisSettings.from_dsn(settings.redis_url),
            default_queue_name="nexus:tasks",
        )
        job_ttl = int(task.job_expires_seconds or 3600)
        job = await arq_pool.enqueue_job(
            "execute_task",
            task_payload=task.model_dump_for_wire(),
            _job_id=task_id,
            _queue_name="nexus:tasks",
            _expires=job_ttl,
        )
        await arq_pool.aclose()
    except Exception as exc:
        log.error("factory_start_seo_groups_enqueue_failed", error=str(exc))
        raise HTTPException(
            status_code=502,
            detail=f"Failed to enqueue seo_group_factory: {exc}",
        ) from exc

    log.info(
        "factory_start_seo_groups_enqueued",
        task_id=task_id,
        job_id=getattr(job, "job_id", None),
    )
    return {
        "ok": True,
        "message": "RankSEO group factory task queued successfully.",
        "task_id": task_id,
        "job_id": getattr(job, "job_id", None),
        "task_type": task.task_type,
    }


@router.get(
    "/seo-report",
    summary="Generated SEO groups report (invite links)",
    response_model=list[dict[str, str]],
)
async def get_seo_report(redis: RedisDep) -> list[dict[str, str]]:
    """
    Return all generated groups as ``[{group_name, invite_link, owner}, ...]``.

    Recomputes from live Community Factory Redis state so the UI stays current
    while workers advance the pipeline.
    """
    try:
        snap = await persist_seo_factory_snapshot(redis)
        return list(snap["report"])
    except Exception as exc:
        log.error("factory_seo_report_failed", error=str(exc))
        raise HTTPException(
            status_code=503,
            detail=f"Could not load SEO factory report: {exc}",
        ) from exc


@router.get(
    "/seo-status",
    summary="RankSEO factory progress",
)
async def get_seo_status(redis: RedisDep) -> dict[str, Any]:
    """
    Current progress: human-readable ``phase``, ``total_links_created``, plus ``raw_phase``.
    """
    try:
        snap = await persist_seo_factory_snapshot(redis)
        return dict(snap["status"])
    except Exception as exc:
        log.error("factory_seo_status_failed", error=str(exc))
        raise HTTPException(
            status_code=503,
            detail=f"Could not load SEO factory status: {exc}",
        ) from exc
