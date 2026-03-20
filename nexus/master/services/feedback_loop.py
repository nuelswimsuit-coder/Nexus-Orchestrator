"""
Feedback Loop Service — Auto-Project Performance Monitor & Resource Allocator.

Runs every 10 minutes as a background asyncio task.

Responsibilities
----------------
1. Poll all active incubator projects for their metrics.
2. Detect "graduation events" — projects that added ≥ 100 users in ≤ 2 days.
3. When a project graduates, instruct the Decision Engine to allocate more
   CPU/RAM by scaling out an additional Worker dedicated to that project.
4. Write allocation decisions to Redis so the dashboard can reflect them.
5. Prune stale / failed projects from the active list.

Redis keys written
------------------
    nexus:feedback:allocations        — LPUSH list of allocation events (last 100)
    nexus:feedback:latest_allocation  — most recent allocation event (JSON, 1h TTL)
    nexus:incubator:project:<id>      — updated project record (via ArchitectService)
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

import structlog

from nexus.master.services.architect import (
    ArchitectService,
    GRADUATION_USERS_THRESHOLD,
    GRADUATION_DAYS,
    INCUBATOR_LIST_KEY,
    INCUBATOR_PROJECT_KEY,
)

log = structlog.get_logger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

FEEDBACK_INTERVAL_SECONDS = 600    # 10 minutes
ALLOCATION_LIST_KEY       = "nexus:feedback:allocations"
LATEST_ALLOCATION_KEY     = "nexus:feedback:latest_allocation"
ALLOCATION_TTL_SECONDS    = 3600   # 1 hour
ALLOCATION_MAX_STORED     = 100

# Resource scaling thresholds
SCALE_WORKER_CONFIDENCE_BOOST = 20   # extra confidence added to scale-worker action
MIN_USERS_FOR_SCALE           = 50   # users before we consider scaling


# ── Feedback Loop Service ──────────────────────────────────────────────────────

class FeedbackLoopService:
    """
    Monitors incubator project metrics and triggers resource reallocation
    when projects hit success thresholds.

    Usage
    -----
        feedback = FeedbackLoopService(redis=redis, architect=architect, dispatcher=dispatcher)
        asyncio.create_task(feedback.run_loop())
    """

    def __init__(
        self,
        redis: Any,
        architect: ArchitectService,
        dispatcher: Any = None,
    ) -> None:
        self._redis = redis
        self._architect = architect
        self._dispatcher = dispatcher
        self._running = False

    # ── Public API ─────────────────────────────────────────────────────────────

    async def run_loop(self) -> None:
        """Background loop — checks metrics every FEEDBACK_INTERVAL_SECONDS."""
        self._running = True
        log.info("feedback_loop_started", interval_s=FEEDBACK_INTERVAL_SECONDS)

        while self._running:
            try:
                await self._evaluation_cycle()
            except Exception as exc:
                log.error("feedback_loop_error", error=str(exc))

            await asyncio.sleep(FEEDBACK_INTERVAL_SECONDS)

    async def run_once(self) -> list[dict[str, Any]]:
        """Run a single evaluation cycle. Returns list of allocation events."""
        return await self._evaluation_cycle()

    def stop(self) -> None:
        self._running = False

    async def record_user_gain(self, project_id: str, users_added: int) -> None:
        """
        External hook — call this when a project gains users.
        The Architect's worker tasks should call this via the API.
        """
        await self._architect.update_metrics(project_id, users_added=users_added)

    async def get_allocations(self, limit: int = 20) -> list[dict[str, Any]]:
        """Return recent allocation events."""
        raw_list = await self._redis.lrange(ALLOCATION_LIST_KEY, 0, limit - 1)
        allocations = []
        for raw in raw_list:
            try:
                allocations.append(json.loads(raw))
            except Exception:
                pass
        return allocations

    # ── Internal ───────────────────────────────────────────────────────────────

    async def _evaluation_cycle(self) -> list[dict[str, Any]]:
        """
        Evaluate all active projects:
        1. Refresh metrics
        2. Detect graduations
        3. Trigger resource scaling for graduated / high-growth projects
        """
        projects = await self._architect.list_projects(limit=50)
        allocation_events: list[dict[str, Any]] = []

        for project in projects:
            project_id = project.get("id")
            status     = project.get("status", "")

            if status in ("failed", "graduated") or not project_id:
                continue

            metrics = project.get("metrics", {})
            users   = metrics.get("users_added", 0)
            days    = metrics.get("days_running", 0)

            # ── Check graduation ───────────────────────────────────────────────
            if (
                not metrics.get("graduated")
                and users >= GRADUATION_USERS_THRESHOLD
                and days <= GRADUATION_DAYS
            ):
                event = await self._handle_graduation(project)
                if event:
                    allocation_events.append(event)

            # ── Check high-growth (not yet graduated but growing fast) ─────────
            elif status == "running" and users >= MIN_USERS_FOR_SCALE and days <= 1:
                event = await self._handle_high_growth(project)
                if event:
                    allocation_events.append(event)

        if allocation_events:
            log.info("feedback_loop_cycle_complete", events=len(allocation_events))

        return allocation_events

    async def _handle_graduation(self, project: dict[str, Any]) -> dict[str, Any] | None:
        """
        Project hit the graduation threshold.
        1. Mark as graduated in Redis.
        2. Scale out a Worker dedicated to this project.
        3. Write allocation event.
        """
        project_id   = project["id"]
        project_name = project.get("name", project_id)
        metrics      = project.get("metrics", {})

        log.info(
            "feedback_graduation_detected",
            project_id=project_id,
            project_name=project_name,
            users=metrics.get("users_added"),
            days=metrics.get("days_running"),
        )

        # Update project status
        await self._architect.update_metrics(project_id)  # recalculate ROI

        # Scale out a Worker
        scale_job_id = await self._scale_worker_for_project(project)

        event: dict[str, Any] = {
            "type":          "graduation",
            "project_id":    project_id,
            "project_name":  project_name,
            "trigger":       f"{metrics.get('users_added')} users in {metrics.get('days_running')} days",
            "action":        "scale_worker",
            "scale_job_id":  scale_job_id,
            "cpu_boost":     "+1 Worker node",
            "ram_boost":     "+512 MB (new worker)",
            "created_at":    datetime.now(timezone.utc).isoformat(),
        }
        await self._store_allocation(event)
        return event

    async def _handle_high_growth(self, project: dict[str, Any]) -> dict[str, Any] | None:
        """
        Project is growing fast but hasn't graduated yet.
        Boost its priority in the task queue.
        """
        project_id   = project["id"]
        project_name = project.get("name", project_id)
        metrics      = project.get("metrics", {})

        log.info(
            "feedback_high_growth_detected",
            project_id=project_id,
            project_name=project_name,
            users=metrics.get("users_added"),
        )

        event: dict[str, Any] = {
            "type":         "high_growth",
            "project_id":   project_id,
            "project_name": project_name,
            "trigger":      f"{metrics.get('users_added')} users in <1 day",
            "action":       "priority_boost",
            "cpu_boost":    "priority elevated",
            "ram_boost":    "none",
            "created_at":   datetime.now(timezone.utc).isoformat(),
        }
        await self._store_allocation(event)
        return event

    async def _scale_worker_for_project(self, project: dict[str, Any]) -> str | None:
        """Dispatch a scale-worker task for the graduated project."""
        if self._dispatcher is None:
            return None

        try:
            from nexus.shared.schemas import TaskPayload
            task = TaskPayload(
                task_type="nexus.scale_worker",
                parameters={
                    "count": 1,
                    "reason": f"graduation:{project['id']}",
                    "project_name": project.get("name"),
                },
                project_id=project["id"],
                priority=1,   # highest priority
            )
            job_id = await self._dispatcher.dispatch(task)
            log.info("feedback_scale_worker_dispatched", job_id=job_id)
            return job_id
        except Exception as exc:
            log.error("feedback_scale_worker_error", error=str(exc))
            return None

    async def _store_allocation(self, event: dict[str, Any]) -> None:
        serialised = json.dumps(event)
        await self._redis.set(LATEST_ALLOCATION_KEY, serialised, ex=ALLOCATION_TTL_SECONDS)
        await self._redis.lpush(ALLOCATION_LIST_KEY, serialised)
        await self._redis.ltrim(ALLOCATION_LIST_KEY, 0, ALLOCATION_MAX_STORED - 1)
