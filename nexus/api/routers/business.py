"""
GET /api/business/stats

Operational Intelligence endpoint — bridges the Telefix Telegram bot database
into the Nexus Control Center dashboard.

Returns live counts from the Mangement Ahu project:
  - Total managed Telegram groups
  - Total scraped users (distinct)
  - Active / frozen / manager session file counts
  - Target group breakdown (source vs target)
  - Last run timestamps for each bot service
  - Forecast history dates
  - Whether the database is reachable

The database is opened in read-only mode so this endpoint never interferes
with the bot's own writes.  If the DB is unavailable, a safe zero-value
response is returned with db_available=False.
"""

from __future__ import annotations

import json
import uuid

import structlog
from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel

from nexus.api.services.telefix_bridge import get_operational_stats, get_windowed_stats
from nexus.master.services.decision_engine import run_decision_engine
from nexus.master.services.reporting import LAST_REPORT_KEY, REPORT_SENDING_KEY
from nexus.shared.schemas import NodeHeartbeat
from nexus.worker.tasks.auto_scrape import SCRAPE_STATUS_KEY, SCRAPE_STATUS_TTL

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/business", tags=["business"])


# ── Response model ─────────────────────────────────────────────────────────────

class BusinessStatsResponse(BaseModel):
    # Groups & targets
    total_managed_groups: int
    total_targets: int
    source_groups: int
    target_groups: int

    # Users
    total_scraped_users: int
    total_users_pipeline: int

    # Sessions (Telethon .json files on disk)
    active_sessions: int
    frozen_sessions: int
    manager_sessions: int

    # Last run timestamps (human-readable UTC strings or null)
    last_scraper_run: str | None
    last_adder_run: str | None
    last_forecast_run: str | None

    # Forecast history (list of date strings from settings)
    forecast_history: list[str]

    # Meta
    db_available: bool
    queried_at: str


# ── Endpoint ───────────────────────────────────────────────────────────────────

@router.get(
    "/stats",
    response_model=BusinessStatsResponse,
    summary="Operational intelligence from the Telefix project",
)
async def get_business_stats() -> BusinessStatsResponse:
    """
    Return a live snapshot of operational metrics from the Mangement Ahu
    Telegram bot project.

    Polls the SQLite database at:
        C:\\Users\\Yarin\\Desktop\\Mangement Ahu\\data\\telefix.db

    And counts session files at:
        C:\\Users\\Yarin\\Desktop\\Mangement Ahu\\sessions\\

    Safe to call frequently — the DB is opened read-only and the query
    is lightweight (COUNT queries with indexes).
    """
    stats = await get_operational_stats()
    d = stats.to_dict()
    return BusinessStatsResponse(**d)


# ── Scrape status ──────────────────────────────────────────────────────────────

class ScrapeStatusResponse(BaseModel):
    status: str        # "idle" | "running" | "completed" | "failed" | "low_resources"
    detail: str
    updated_at: str


class ForceScrapeRequest(BaseModel):
    sources: list[str] = []   # optional explicit group links; empty = use DB candidates
    force: bool = True         # skip MIN_RESCRAPE_HOURS guard


class ForceScrapeResponse(BaseModel):
    task_id: str
    message: str


@router.get(
    "/scrape-status",
    response_model=ScrapeStatusResponse,
    summary="Current auto-scrape job state",
)
async def get_scrape_status(request: Request) -> ScrapeStatusResponse:
    """
    Return the current state of the auto-scrape task.

    Reads the `nexus:scrape:status` Redis key written by the
    telegram.auto_scrape task handler.  Returns "idle" if the key
    does not exist (no scrape has run yet or TTL expired).
    """
    redis = request.app.state.redis
    raw = await redis.get(SCRAPE_STATUS_KEY)
    if raw is None:
        return ScrapeStatusResponse(
            status="idle",
            detail="No scrape has run yet",
            updated_at="",
        )
    try:
        data = json.loads(raw)
        return ScrapeStatusResponse(
            status=data.get("status", "idle"),
            detail=data.get("detail", ""),
            updated_at=data.get("updated_at", ""),
        )
    except Exception:
        return ScrapeStatusResponse(status="idle", detail="", updated_at="")


@router.post(
    "/force-scrape",
    response_model=ForceScrapeResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger an immediate scrape run",
)
async def force_scrape(body: ForceScrapeRequest, request: Request) -> ForceScrapeResponse:
    """
    Enqueue a `telegram.auto_scrape` task immediately, bypassing the
    nightly cron schedule and (optionally) the MIN_RESCRAPE_HOURS guard.

    The task is enqueued onto the ARQ queue and picked up by the next
    available worker.  Returns the task_id for status tracking.
    """
    from nexus.shared.schemas import TaskPayload

    redis = request.app.state.redis

    # Write "pending" status immediately so the dashboard updates right away.
    pending_payload = json.dumps({
        "status": "pending",
        "detail": "Force scrape requested via dashboard",
        "updated_at": __import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        ).isoformat(),
    })
    await redis.set(SCRAPE_STATUS_KEY, pending_payload, ex=SCRAPE_STATUS_TTL)

    task_id = str(uuid.uuid4())
    task = TaskPayload(
        task_id=task_id,
        task_type="telegram.auto_scrape",
        parameters={
            "force": body.force,
            "sources": body.sources,
        },
        project_id="telefix",
        priority=2,
    )

    # Enqueue directly onto the ARQ queue via the shared Redis connection.
    # (The API server does not hold a Dispatcher reference — it enqueues
    # directly using the ARQ wire format.)
    from arq.connections import RedisSettings

    from nexus.shared.config import settings as nexus_settings

    try:
        arq_pool = await __import__("arq").create_pool(
            RedisSettings.from_dsn(nexus_settings.redis_url),
            default_queue_name="nexus:tasks",
        )
        job = await arq_pool.enqueue_job(
            "execute_task",
            task_payload=task.model_dump_for_wire(),
            _job_id=task_id,
            _queue_name="nexus:tasks",
        )
        await arq_pool.aclose()

        if job is None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="A scrape task with this ID is already queued.",
            )

        log.info("force_scrape_enqueued", task_id=task_id)
        return ForceScrapeResponse(
            task_id=task_id,
            message="Scrape task enqueued — check /api/business/scrape-status for progress.",
        )

    except HTTPException:
        raise
    except Exception as exc:
        log.error("force_scrape_enqueue_error", error=str(exc))
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Could not enqueue task: {exc}",
        ) from exc


# ── Decision Engine ────────────────────────────────────────────────────────────

class DecisionItem(BaseModel):
    decision_type: str
    title: str
    reasoning: str
    confidence: int
    roi_impact: str
    action_task_type: str
    requires_approval: bool
    created_at: str


class DecisionsResponse(BaseModel):
    decisions: list[DecisionItem]
    total: int
    queried_at: str


@router.get(
    "/decisions",
    response_model=DecisionsResponse,
    summary="Autonomous profit-optimization decisions from the Decision Engine",
)
async def get_decisions(request: Request) -> DecisionsResponse:
    """
    Run the Decision Engine and return ranked scaling recommendations.

    The engine reads live data from telefix.db and the cluster heartbeats,
    applies the ForecastService financial model, and produces a list of
    concrete actions sorted by confidence score.

    Decisions with confidence < 70 are flagged as requires_approval=True
    and will appear as HITL tasks when dispatched.
    """
    from datetime import datetime, timezone

    redis = request.app.state.redis

    # Collect cluster stats for worker load calculation
    active_workers = 0
    total_jobs = 0
    cursor = 0
    pattern = b"nexus:heartbeat:*"
    while True:
        cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=100)
        for key in keys:
            raw = await redis.get(key)
            if raw:
                try:
                    hb = NodeHeartbeat.model_validate_json(raw)
                    if hb.role.value == "worker":
                        active_workers += 1
                        total_jobs += hb.active_jobs
                except Exception:
                    pass
        if cursor == 0:
            break

    decisions = await run_decision_engine(
        cluster_workers=active_workers,
        total_active_jobs=total_jobs,
    )

    now = datetime.now(timezone.utc).isoformat()
    return DecisionsResponse(
        decisions=[DecisionItem(**d.to_dict()) for d in decisions],
        total=len(decisions),
        queried_at=now,
    )


# ── Agent Thinking Log ─────────────────────────────────────────────────────────
# The agent log is stored in Redis as a list (LPUSH / LRANGE).
# The Decision Engine, scrape task, and adder task all write entries here.
# The dashboard polls this endpoint to show the "AI thinking" terminal.

AGENT_LOG_KEY = "nexus:agent:log"
AGENT_LOG_MAX = 200   # keep last 200 entries


class AgentLogEntry(BaseModel):
    ts: str
    level: str    # "info" | "decision" | "warning" | "action"
    message: str
    metadata: dict = {}


class AgentLogResponse(BaseModel):
    entries: list[AgentLogEntry]
    total: int


@router.get(
    "/agent-log",
    response_model=AgentLogResponse,
    summary="Live AI agent decision log",
)
async def get_agent_log(request: Request, limit: int = 50) -> AgentLogResponse:
    """
    Return the most recent entries from the agent thinking log.

    Entries are written by:
    - The Decision Engine (decisions made)
    - The auto_scrape task (scraping progress)
    - The auto_add task (adding progress)
    - The cron scheduler (scheduled fires)

    Poll this endpoint every 5 s to keep the dashboard terminal live.
    """
    redis = request.app.state.redis
    raw_entries = await redis.lrange(AGENT_LOG_KEY, 0, min(limit, AGENT_LOG_MAX) - 1)

    entries: list[AgentLogEntry] = []
    for raw in raw_entries:
        try:
            data = json.loads(raw)
            entries.append(AgentLogEntry(**data))
        except Exception:
            pass

    return AgentLogResponse(entries=entries, total=len(entries))


@router.post(
    "/agent-log",
    status_code=status.HTTP_201_CREATED,
    summary="Append an entry to the agent log (internal use)",
    include_in_schema=False,
)
async def append_agent_log(entry: AgentLogEntry, request: Request) -> dict:
    """Internal endpoint — called by master services to log decisions."""
    redis = request.app.state.redis
    await redis.lpush(AGENT_LOG_KEY, entry.model_dump_json())
    await redis.ltrim(AGENT_LOG_KEY, 0, AGENT_LOG_MAX - 1)
    return {"ok": True}


# ── Scale Worker ───────────────────────────────────────────────────────────────

class ScaleWorkerResponse(BaseModel):
    message: str
    command: str


@router.post(
    "/scale-worker",
    response_model=ScaleWorkerResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Deploy an additional worker container via Docker",
)
async def scale_worker() -> ScaleWorkerResponse:
    """
    Trigger a Docker scale-out to add one more worker container.

    Requires Docker to be installed and the nexus-worker image to be built.
    Run `python scripts/package_worker.py --build` first.
    """
    import subprocess
    import sys
    from pathlib import Path

    script = Path(__file__).resolve().parent.parent.parent.parent / "scripts" / "package_worker.py"
    cmd = [sys.executable, str(script), "--scale", "1"]

    try:
        subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return ScaleWorkerResponse(
            message="Scale-out initiated. New worker container starting...",
            command=" ".join(cmd),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Could not start Docker scale: {exc}",
        ) from exc


# ── Engine State ───────────────────────────────────────────────────────────────
# The AutonomousOrchestrator writes its current state here so the dashboard
# can sync the Master PC RGB colour in real time.

ENGINE_STATE_KEY = "nexus:engine:state"


class EngineStateResponse(BaseModel):
    state: str        # "idle" | "calculating" | "dispatching" | "warning"
    updated_at: str


@router.get(
    "/engine-state",
    response_model=EngineStateResponse,
    summary="Current state of the Autonomous Orchestrator (for RGB sync)",
)
async def get_engine_state(request: Request) -> EngineStateResponse:
    """
    Return the current state of the AutonomousOrchestrator.

    Used by the dashboard to sync the Master PC RGB colour:
      - "calculating" → Deep Indigo pulse
      - "dispatching" → Gold/Yellow flash
      - "warning"     → Red pulse
      - "idle"        → Normal green/red (online/offline)
    """
    redis = request.app.state.redis
    raw = await redis.get(ENGINE_STATE_KEY)
    if raw is None:
        return EngineStateResponse(state="idle", updated_at="")
    try:
        data = json.loads(raw)
        return EngineStateResponse(
            state=data.get("state", "idle"),
            updated_at=data.get("updated_at", ""),
        )
    except Exception:
        return EngineStateResponse(state="idle", updated_at="")


# ── Windowed stats (60 min / 24 h) ────────────────────────────────────────────

class WindowedStatsResponse(BaseModel):
    window_minutes: int
    new_scraped_users_window: int
    new_pipeline_users_window: int
    # Full snapshot fields (same as BusinessStatsResponse)
    total_managed_groups: int
    total_scraped_users: int
    total_users_pipeline: int
    active_sessions: int
    frozen_sessions: int
    manager_sessions: int
    total_targets: int
    source_groups: int
    target_groups: int
    last_scraper_run: str | None
    last_adder_run: str | None
    last_forecast_run: str | None
    forecast_history: list[str]
    db_available: bool
    queried_at: str


@router.get(
    "/stats/windowed",
    response_model=WindowedStatsResponse,
    summary="Operational stats for a specific time window (60 min or 24 h)",
)
async def get_windowed_business_stats(
    window: int = 1440,
) -> WindowedStatsResponse:
    """
    Return stats for the given time window.

    Query params:
      window=60    — last 60 minutes
      window=1440  — last 24 hours (default)

    The `new_scraped_users_window` and `new_pipeline_users_window` fields
    count only rows created within the window.  All other fields are totals.
    """
    data = await get_windowed_stats(window_minutes=window)
    return WindowedStatsResponse(**data)


# ── Profit report ──────────────────────────────────────────────────────────────

class ProfitReportResponse(BaseModel):
    db_available: bool
    window_hours: int
    new_scraped_users: int
    total_scraped_users: int
    total_pipeline: int
    target_groups: int
    source_groups: int
    estimated_roi: int
    active_sessions: int
    frozen_sessions: int
    manager_sessions: int
    health_ratio: float
    last_scraper_run: str | None
    last_adder_run: str | None
    forecast_history: list[str]
    generated_at: str


@router.get(
    "/report",
    response_model=ProfitReportResponse,
    summary="Latest profit report (generated daily at 20:00)",
)
async def get_profit_report(request: Request) -> ProfitReportResponse:
    """
    Return the most recently generated profit report.

    The report is generated daily at 20:00 by the ReportingService and
    stored in Redis under `nexus:report:last`.  If no report has been
    generated yet, triggers an immediate generation.
    """
    from nexus.master.services.reporting import _collect_report_data

    redis = request.app.state.redis
    raw = await redis.get(LAST_REPORT_KEY)

    if raw:
        try:
            data = json.loads(raw)
            return ProfitReportResponse(**data)
        except Exception:
            pass

    # No cached report — generate one now
    data = await _collect_report_data(window_hours=24)
    return ProfitReportResponse(**data)


# ── Report sending status (for RGB flash) ─────────────────────────────────────

class ReportStatusResponse(BaseModel):
    sending: bool
    started_at: str


@router.get(
    "/report-status",
    response_model=ReportStatusResponse,
    summary="Is the daily report currently being sent? (drives Neon Blue RGB flash)",
)
async def get_report_status(request: Request) -> ReportStatusResponse:
    """
    Returns whether the daily profit report is currently being sent.

    When sending=True, the dashboard flashes the Master PC RGB Neon Blue.
    The flag auto-expires after REPORT_SENDING_TTL seconds.
    """
    redis = request.app.state.redis
    raw = await redis.get(REPORT_SENDING_KEY)
    if raw:
        try:
            d = json.loads(raw)
            return ReportStatusResponse(
                sending=True,
                started_at=d.get("started_at", ""),
            )
        except Exception:
            pass
    return ReportStatusResponse(sending=False, started_at="")


# ── Stuck state + Force Run ────────────────────────────────────────────────────

from nexus.master.services.decision_engine import (  # noqa: E402
    APPROVAL_STREAK_KEY,
    HITL_THRESHOLD,
    MIN_THRESHOLD,
    STUCK_STATE_KEY,
    THRESHOLD_OVERRIDE_PREFIX,
)


class StuckStateResponse(BaseModel):
    stuck: bool
    action_type: str
    confidence: int
    threshold: int
    gap: int
    task_type: str
    task_params: dict
    detected_at: str


class ForceRunRequest(BaseModel):
    task_type: str
    task_params: dict = {}
    reviewer_id: str = "dashboard"


class ForceRunResponse(BaseModel):
    task_id: str
    message: str


class ThresholdInfoResponse(BaseModel):
    action_type: str
    effective_threshold: int
    default_threshold: int
    approval_streak: int
    streak_needed: int


@router.get(
    "/stuck-state",
    response_model=StuckStateResponse,
    summary="Current stuck-loop state (if any)",
)
async def get_stuck_state(request: Request) -> StuckStateResponse:
    """
    Returns the current stuck-loop state if the orchestrator has been
    blocked on the same action for ≥ 3 consecutive cycles (≈ 15 min).
    The dashboard uses this to show the Force Run button.
    """
    redis = request.app.state.redis
    raw = await redis.get(STUCK_STATE_KEY)
    if raw:
        try:
            d = json.loads(raw)
            return StuckStateResponse(
                stuck=True,
                action_type=d.get("action_type", ""),
                confidence=d.get("confidence", 0),
                threshold=d.get("threshold", HITL_THRESHOLD),
                gap=d.get("gap", 0),
                task_type=d.get("task_type", ""),
                task_params=d.get("task_params", {}),
                detected_at=d.get("detected_at", ""),
            )
        except Exception:
            pass
    return StuckStateResponse(
        stuck=False, action_type="", confidence=0, threshold=HITL_THRESHOLD,
        gap=0, task_type="", task_params={}, detected_at="",
    )


@router.post(
    "/force-run",
    response_model=ForceRunResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Force-run a task bypassing the confidence threshold",
)
async def force_run_task(body: ForceRunRequest, request: Request) -> ForceRunResponse:
    """
    Immediately enqueue a task bypassing the confidence check.
    Used when the orchestrator is stuck in a low-confidence loop.
    """
    from arq.connections import RedisSettings

    from nexus.shared.config import settings as nexus_settings
    from nexus.shared.schemas import TaskPayload

    task_id = str(uuid.uuid4())
    task = TaskPayload(
        task_id=task_id,
        task_type=body.task_type,
        parameters={**body.task_params, "force": True},
        project_id="telefix",
        priority=1,   # highest priority
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

        # Clear the stuck state
        redis = request.app.state.redis
        await redis.delete(STUCK_STATE_KEY)

        log.info(
            "force_run_enqueued",
            task_id=task_id,
            task_type=body.task_type,
            reviewer=body.reviewer_id,
        )
        return ForceRunResponse(
            task_id=task_id,
            message=f"Force-run enqueued: {body.task_type} (confidence check bypassed)",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Could not enqueue force-run: {exc}",
        ) from exc


@router.get(
    "/threshold-info/{action_type}",
    response_model=ThresholdInfoResponse,
    summary="Get effective threshold and approval streak for an action type",
)
async def get_threshold_info(action_type: str, request: Request) -> ThresholdInfoResponse:
    """
    Returns the current effective threshold and approval streak for an action type.
    Used by the dashboard to show the 'Need +N confidence' indicator.
    """
    from nexus.master.services.decision_engine import (
        APPROVAL_STREAK_THRESHOLD,
    )
    redis = request.app.state.redis

    # Get override threshold
    override_key = f"{THRESHOLD_OVERRIDE_PREFIX}{action_type}"
    override_val = await redis.get(override_key)
    effective = max(MIN_THRESHOLD, int(override_val)) if override_val else HITL_THRESHOLD

    # Get approval streak
    streak_val = await redis.hget(APPROVAL_STREAK_KEY, action_type)
    streak = int(streak_val) if streak_val else 0

    return ThresholdInfoResponse(
        action_type=action_type,
        effective_threshold=effective,
        default_threshold=HITL_THRESHOLD,
        approval_streak=streak,
        streak_needed=max(0, APPROVAL_STREAK_THRESHOLD - streak),
    )


# ── Supervisor Watchdog Status ─────────────────────────────────────────────────
# These endpoints expose the Supervisor's in-memory state to the dashboard.
# The Supervisor writes its state to Redis so the API can serve it even when
# the live Supervisor object is not directly reachable.

SUPERVISOR_STATUS_KEY = "nexus:supervisor:status"


class SupervisorWorkerStatus(BaseModel):
    name:            str
    node_id:         str
    status:          str    # "healthy" | "recovering" | "critical"
    strike_count:    int
    pid:             int | None
    last_restart_ts: float
    first_strike_ts: float


class SupervisorStatusResponse(BaseModel):
    workers:    list[SupervisorWorkerStatus]
    updated_at: str
    any_critical: bool


class SupervisorResetResponse(BaseModel):
    worker:  str
    success: bool
    message: str


@router.get(
    "/supervisor-status",
    response_model=SupervisorStatusResponse,
    summary="Current state of the Supervisor Watchdog (recovery / critical)",
)
async def get_supervisor_status(request: Request) -> SupervisorStatusResponse:
    """
    Returns the live status of all supervised worker processes.
    Polled by the dashboard to display recovery indicators and the Manual Reset button.

    Status values:
      healthy    — process is running normally
      recovering — in the middle of a recovery attempt (yellow)
      critical   — 3 strikes exhausted, requires manual intervention (red)
    """
    # Try the live Supervisor object first (fastest path)
    supervisor = getattr(request.app.state, "supervisor", None)
    if supervisor is not None:
        raw_workers = supervisor.get_all_statuses()
        workers = [SupervisorWorkerStatus(**w) for w in raw_workers.values()]
        return SupervisorStatusResponse(
            workers=workers,
            updated_at=__import__("datetime").datetime.now(
                __import__("datetime").timezone.utc
            ).isoformat(),
            any_critical=any(w.status == "critical" for w in workers),
        )

    # Fall back to Redis snapshot (API process started before master)
    redis = request.app.state.redis
    raw = await redis.get(SUPERVISOR_STATUS_KEY)
    if raw is None:
        return SupervisorStatusResponse(
            workers=[], updated_at="", any_critical=False
        )
    try:
        data       = json.loads(raw)
        workers_d  = data.get("workers", {})
        workers    = [SupervisorWorkerStatus(**v) for v in workers_d.values()]
        updated_at = data.get("updated_at", "")
        return SupervisorStatusResponse(
            workers=workers,
            updated_at=updated_at,
            any_critical=any(w.status == "critical" for w in workers),
        )
    except Exception as exc:
        log.warning("supervisor_status_parse_error", error=str(exc))
        return SupervisorStatusResponse(workers=[], updated_at="", any_critical=False)


@router.post(
    "/supervisor-reset/{worker_name}",
    response_model=SupervisorResetResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Manually reset a CRITICAL worker and attempt one clean restart",
)
async def supervisor_manual_reset(
    worker_name: str,
    request: Request,
) -> SupervisorResetResponse:
    """
    Reset a CRITICAL worker's strike counter and trigger one fresh restart.
    This is the 'Manual Reset' button in the dashboard — only appears when
    a worker is in CRITICAL state.
    """
    supervisor = getattr(request.app.state, "supervisor", None)
    if supervisor is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Supervisor is not running in this process.",
        )

    success = await supervisor.manual_reset(worker_name)
    if success:
        return SupervisorResetResponse(
            worker=worker_name,
            success=True,
            message=f"מעבד (Worker) '{worker_name}' אופס והופעל מחדש בהצלחה.",
        )
    return SupervisorResetResponse(
        worker=worker_name,
        success=False,
        message=f"מעבד (Worker) '{worker_name}' לא נמצא או אין פקודת הפעלה מחדש.",
    )
