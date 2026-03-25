"""
GET  /api/sentinel/status   — current Sentinel AI engine status
GET  /api/sentinel/events   — last N AI crash analysis events
GET  /api/sentinel/metrics  — rolling system metrics (latency, RAM)
POST /api/sentinel/report   — manually inject an error event (dev/test)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import structlog
from fastapi import APIRouter, Request
from pydantic import BaseModel

from nexus.api.dependencies import RedisDep

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/sentinel", tags=["sentinel"])

# ── Redis keys (mirror nexus/master/sentinel.py) ──────────────────────────────

STATUS_KEY    = "nexus:sentinel:ai:status"
EVENTS_KEY    = "nexus:sentinel:ai:events"
METRICS_KEY   = "nexus:sentinel:ai:metrics"
ERROR_CHANNEL = "nexus:sentinel:errors"


# ── Response models ───────────────────────────────────────────────────────────

class SentinelStatusResponse(BaseModel):
    state: str
    node_id: str
    latency_ms: float | None
    ram_pct: float | None
    latency_bad_cycles: int
    ram_bad_cycles: int
    windows_worker_online: bool | None
    rpc_url: str
    rpc_switched: bool
    updated_at: str


class SentinelEvent(BaseModel):
    ts: str
    event_type: str
    trigger: str
    metric_value: float
    action_taken: str
    reason_he: str
    ai_reason_en: str = ""


class SentinelEventsResponse(BaseModel):
    events: list[SentinelEvent]
    total: int


class SentinelMetric(BaseModel):
    ts: str
    latency_ms: float
    ram_pct: float


class SentinelMetricsResponse(BaseModel):
    metrics: list[SentinelMetric]
    latency_threshold_ms: int
    memory_threshold_pct: int


class ReportErrorRequest(BaseModel):
    node_id: str = "manual"
    task_type: str = "manual"
    error: str
    traceback: str = ""
    severity: str = "error"


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get(
    "/status",
    response_model=SentinelStatusResponse,
    summary="Sentinel AI engine status",
)
async def get_sentinel_status(request: Request, redis: RedisDep) -> SentinelStatusResponse:
    """
    Return the current Sentinel AI engine state.

    If the sentinel engine is running (registered on app.state.sentinel),
    its live snapshot is used.  Otherwise, Redis is queried for the last
    persisted status.
    """
    sentinel = getattr(request.app.state, "sentinel", None)
    if sentinel is not None:
        data = await sentinel.get_status_snapshot()
        return SentinelStatusResponse(**data)

    raw = await redis.get(STATUS_KEY)
    if raw:
        try:
            data = json.loads(raw)
            return SentinelStatusResponse(
                state=data.get("state", "unknown"),
                node_id=data.get("node_id", "master"),
                latency_ms=None,
                ram_pct=None,
                latency_bad_cycles=0,
                ram_bad_cycles=0,
                windows_worker_online=None,
                rpc_url=data.get("rpc_url", ""),
                rpc_switched=data.get("rpc_switched", False),
                updated_at=data.get("updated_at", datetime.now(timezone.utc).isoformat()),
            )
        except Exception:
            pass

    return SentinelStatusResponse(
        state="offline",
        node_id="master",
        latency_ms=None,
        ram_pct=None,
        latency_bad_cycles=0,
        ram_bad_cycles=0,
        windows_worker_online=None,
        rpc_url="",
        rpc_switched=False,
        updated_at=datetime.now(timezone.utc).isoformat(),
    )


@router.get(
    "/events",
    response_model=SentinelEventsResponse,
    summary="Last Sentinel AI crash analysis events",
)
async def get_sentinel_events(redis: RedisDep, limit: int = 20) -> SentinelEventsResponse:
    """
    Return the last `limit` AI diagnosis events from Redis.
    Each event contains the AI's analysis in both English and Hebrew.
    """
    limit = min(limit, 50)
    raw_entries: list[Any] = await redis.lrange(EVENTS_KEY, -limit, -1)
    events: list[SentinelEvent] = []

    for raw in reversed(raw_entries):
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
            events.append(SentinelEvent(
                ts=data.get("ts", ""),
                event_type=data.get("event_type", ""),
                trigger=data.get("trigger", ""),
                metric_value=float(data.get("metric_value", 0)),
                action_taken=data.get("action_taken", ""),
                reason_he=data.get("reason_he", ""),
                ai_reason_en=data.get("ai_reason_en", ""),
            ))
        except Exception as exc:
            log.warning("sentinel_event_parse_error", error=str(exc))

    return SentinelEventsResponse(events=events, total=len(events))


@router.get(
    "/metrics",
    response_model=SentinelMetricsResponse,
    summary="Rolling system metrics (latency + RAM)",
)
async def get_sentinel_metrics(redis: RedisDep, limit: int = 30) -> SentinelMetricsResponse:
    """Return the rolling window of system metrics recorded by the Sentinel."""
    limit = min(limit, 30)
    raw_entries: list[Any] = await redis.lrange(METRICS_KEY, -limit, -1)
    metrics: list[SentinelMetric] = []

    for raw in raw_entries:
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
            metrics.append(SentinelMetric(
                ts=data.get("ts", ""),
                latency_ms=float(data.get("latency_ms", 0)),
                ram_pct=float(data.get("ram_pct", 0)),
            ))
        except Exception:
            pass

    return SentinelMetricsResponse(
        metrics=metrics,
        latency_threshold_ms=2000,
        memory_threshold_pct=90,
    )


@router.post(
    "/report",
    summary="Manually inject an error event for Sentinel AI analysis",
)
async def report_error(body: ReportErrorRequest, redis: RedisDep) -> dict:
    """
    Inject a synthetic error event into the Sentinel error channel.
    Useful for testing the AI diagnostic pipeline without a real crash.
    """
    from nexus.master.sentinel import SentinelEngine

    await SentinelEngine.report_error(
        redis=redis,
        node_id=body.node_id,
        task_type=body.task_type,
        error=body.error,
        traceback=body.traceback,
        severity=body.severity,
    )
    log.info(
        "sentinel_manual_error_injected",
        node_id=body.node_id,
        task_type=body.task_type,
    )
    return {
        "status": "ok",
        "message": "Error event published to Sentinel channel",
        "node_id": body.node_id,
        "task_type": body.task_type,
    }


class RecoverWorkerRequest(BaseModel):
    node_id: str = "*"
    mode: str = "signal_only"


@router.post("/recover-worker", summary="Signal workers to restart via Redis panic channel")
async def recover_worker(body: RecoverWorkerRequest, redis: RedisDep) -> dict:
    """Publish a RESTART_WORKER signal to the panic channel so workers self-heal."""
    PANIC_CHANNEL = "nexus:kill_switch:panic"
    await redis.publish(PANIC_CHANNEL, f"RESTART_WORKER:{body.node_id}")
    log.info("recover_worker_signal_sent", node_id=body.node_id, mode=body.mode)
    return {"status": "published", "node_id": body.node_id, "mode": body.mode}
