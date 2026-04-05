"""
Management dashboard — group metadata, member stats, rank tracker + ARQ scans.
"""

from __future__ import annotations

import uuid
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException

from nexus.api.schemas.management import (
    ManagementGroupRow,
    ManagementGroupsResponse,
    ManagementScanRequest,
    ManagementScanResponse,
    MemberStatsOut,
    RankTrackerOut,
)
from nexus.shared.config import settings
from nexus.shared.management_store import list_management_groups
from nexus.shared.schemas import TaskPayload

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/management", tags=["management"])


@router.get("/groups", response_model=ManagementGroupsResponse)
async def get_management_groups() -> ManagementGroupsResponse:
    raw = await list_management_groups()
    rows: list[ManagementGroupRow] = []
    for r in raw:
        ms = r.get("member_stats") or {}
        rows.append(
            ManagementGroupRow(
                id=r["id"],
                session_owner=r["session_owner"],
                group_id=r["group_id"],
                title=r.get("title"),
                username=r.get("username"),
                is_public=bool(r.get("is_public")),
                invite_link=r.get("invite_link"),
                creator_id=r.get("creator_id"),
                legacy_groups_id=r.get("legacy_groups_id"),
                updated_at=r.get("updated_at"),
                member_stats=MemberStatsOut(
                    total_members=int(ms.get("total_members") or 0),
                    premium_count=int(ms.get("premium_count") or 0),
                    deleted_count=int(ms.get("deleted_count") or 0),
                    active_real_count=int(ms.get("active_real_count") or 0),
                    updated_at=ms.get("updated_at"),
                ),
                rank_tracker=[
                    RankTrackerOut(
                        keyword_phrase=rt["keyword_phrase"],
                        current_rank=rt.get("current_rank"),
                        last_check=rt.get("last_check"),
                        is_shadowbanned=bool(rt.get("is_shadowbanned")),
                        updated_at=rt.get("updated_at"),
                    )
                    for rt in (r.get("rank_tracker") or [])
                ],
            )
        )
    return ManagementGroupsResponse(groups=rows)


@router.post("/scan", response_model=ManagementScanResponse)
async def post_management_scan(body: ManagementScanRequest) -> ManagementScanResponse:
    enqueued: list[dict[str, Any]] = []
    errors: list[str] = []

    try:
        import arq  # type: ignore[import-untyped]
        from arq.connections import RedisSettings  # type: ignore[import-untyped]
    except ImportError as exc:
        raise HTTPException(status_code=500, detail=f"ARQ not available: {exc}") from exc

    pool = await arq.create_pool(
        RedisSettings.from_dsn(settings.redis_url),
        default_queue_name="nexus:tasks",
    )
    try:
        if body.run_health_scan:
            task_id = str(uuid.uuid4())
            task = TaskPayload(
                task_id=task_id,
                task_type="management.group_health_scan",
                parameters={},
                project_id="management",
                priority=6,
            )
            job = await pool.enqueue_job(
                "execute_task",
                task_payload=task.model_dump_for_wire(),
                _job_id=task_id,
                _queue_name="nexus:tasks",
            )
            enqueued.append({
                "task_type": "management.group_health_scan",
                "task_id": task_id,
                "job_id": getattr(job, "job_id", None),
            })

        if body.run_sentinel_seo:
            task_id = str(uuid.uuid4())
            params: dict[str, Any] = {}
            if body.seo_keyword_phrases:
                params["seo_keyword_phrases"] = body.seo_keyword_phrases
            probe = (settings.nexus_seo_probe_session or "").strip()
            if probe:
                params["probe_session_stem"] = probe
            task = TaskPayload(
                task_id=task_id,
                task_type="management.sentinel_seo",
                parameters=params,
                project_id="management",
                priority=5,
            )
            job = await pool.enqueue_job(
                "execute_task",
                task_payload=task.model_dump_for_wire(),
                _job_id=task_id,
                _queue_name="nexus:tasks",
            )
            enqueued.append({
                "task_type": "management.sentinel_seo",
                "task_id": task_id,
                "job_id": getattr(job, "job_id", None),
            })

        if body.run_seo_watchdog:
            task_id = str(uuid.uuid4())
            task = TaskPayload(
                task_id=task_id,
                task_type="seo.watchdog.audit",
                parameters={"session_start_offset": -1},
                project_id="management",
                priority=4,
            )
            job = await pool.enqueue_job(
                "execute_task",
                task_payload=task.model_dump_for_wire(),
                _job_id=task_id,
                _queue_name="nexus:tasks",
            )
            enqueued.append({
                "task_type": "seo.watchdog.audit",
                "task_id": task_id,
                "job_id": getattr(job, "job_id", None),
            })
    except Exception as exc:
        errors.append(str(exc))
        log.warning("management_scan_enqueue_failed", error=str(exc))
    finally:
        await pool.aclose()

    return ManagementScanResponse(enqueued=enqueued, errors=errors)


@router.get("/config")
async def get_management_config() -> dict[str, Any]:
    """Non-secret flags for the Sentinel SEO UI."""
    return {
        "legacy_telefix_bot_enabled": settings.legacy_telefix_bot_enabled,
        "nexus_seo_probe_session_configured": bool(
            (settings.nexus_seo_probe_session or "").strip()
        ),
        "nexus_seo_auto_rename": settings.nexus_seo_auto_rename,
        "nexus_seo_target_title_set": bool((settings.nexus_seo_target_title or "").strip()),
    }
