"""
nexus/api/routers/scan.py — Full-System Scan & Automation Runner

Endpoints
---------
POST /api/scan/run     — Trigger a full scan: heartbeat check + enqueue all automation tasks
GET  /api/scan/status  — Current scan state (polling-friendly)
GET  /api/scan/stream  — SSE live log stream (real-time terminal output)
GET  /api/scan/history — Last N scan run summaries
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Any

import structlog
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from nexus.api.dependencies import RedisDep
from nexus.shared.config import settings

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/scan", tags=["scan"])

# ── Redis keys ────────────────────────────────────────────────────────────────
SCAN_STATUS_KEY    = "nexus:scan:status"          # JSON: current scan state
SCAN_LOG_KEY       = "nexus:scan:log"             # List of log line JSON
SCAN_CHANNEL       = "nexus:scan:events"          # Pub/Sub for live SSE
SCAN_HISTORY_KEY   = "nexus:scan:history"         # List of past run summaries
HEARTBEAT_PREFIX   = "nexus:heartbeat:"
ARQ_QUEUE_KEY      = "arq:queue:nexus:tasks"

SCAN_LOG_MAX       = 500
SCAN_HISTORY_MAX   = 50


# ── Pydantic models ───────────────────────────────────────────────────────────

class ScanRunRequest(BaseModel):
    tasks: list[str] | None = None
    """Subset of task_types to run. Omit to run all default automation tasks."""
    force: bool = False
    """Pass force=True to bypass cooldown guards in individual tasks."""


class ScanLogLine(BaseModel):
    ts: str
    level: str          # "info" | "ok" | "warn" | "error"
    msg: str
    detail: str = ""


class ScanStatusResponse(BaseModel):
    run_id: str | None
    phase: str          # "idle" | "running" | "done" | "error"
    started_at: str | None
    finished_at: str | None
    nodes_found: int
    nodes_online: int
    tasks_enqueued: int
    tasks_failed: int
    queue_depth: int
    errors: list[str]
    last_log: list[ScanLogLine]


class ScanRunResponse(BaseModel):
    run_id: str
    message: str
    tasks_requested: list[str]


class ScanHistoryEntry(BaseModel):
    run_id: str
    started_at: str
    finished_at: str | None
    phase: str
    nodes_found: int
    tasks_enqueued: int
    tasks_failed: int


class ScanHistoryResponse(BaseModel):
    runs: list[ScanHistoryEntry]
    total: int


# ── Default automation task suite ─────────────────────────────────────────────

DEFAULT_TASKS: list[dict[str, Any]] = [
    {
        "task_type": "account_mapper.map",
        "label": "Account Mapper",
        "project_id": "telefix",
        "priority": 2,
        "parameters": {},
    },
    {
        "task_type": "telegram.auto_scrape",
        "label": "Auto Scrape",
        "project_id": "telefix",
        "priority": 2,
        "parameters": {"force": True},
    },
    {
        "task_type": "retention.guardian.monitor",
        "label": "Retention Monitor",
        "project_id": "telefix",
        "priority": 1,
        "parameters": {},
    },
    {
        "task_type": "sentinel.status",
        "label": "Sentinel Status Check",
        "project_id": "telefix",
        "priority": 1,
        "parameters": {},
    },
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _publish_log(
    redis: Any,
    *,
    level: str,
    msg: str,
    detail: str = "",
) -> None:
    line = ScanLogLine(ts=_now_iso(), level=level, msg=msg, detail=detail)
    payload = line.model_dump_json()
    await redis.rpush(SCAN_LOG_KEY, payload)
    await redis.ltrim(SCAN_LOG_KEY, -SCAN_LOG_MAX, -1)
    try:
        await redis.publish(SCAN_CHANNEL, payload)
    except Exception:
        pass


async def _set_status(redis: Any, data: dict[str, Any]) -> None:
    await redis.set(SCAN_STATUS_KEY, json.dumps(data))


async def _get_status(redis: Any) -> dict[str, Any]:
    raw = await redis.get(SCAN_STATUS_KEY)
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    return {
        "run_id": None,
        "phase": "idle",
        "started_at": None,
        "finished_at": None,
        "nodes_found": 0,
        "nodes_online": 0,
        "tasks_enqueued": 0,
        "tasks_failed": 0,
        "queue_depth": 0,
        "errors": [],
    }


async def _run_scan(
    redis: Any,
    run_id: str,
    task_types: list[str] | None,
    force: bool,
) -> None:
    """
    Background coroutine: performs the full scan sequence and publishes live log lines.
    """
    state: dict[str, Any] = {
        "run_id": run_id,
        "phase": "running",
        "started_at": _now_iso(),
        "finished_at": None,
        "nodes_found": 0,
        "nodes_online": 0,
        "tasks_enqueued": 0,
        "tasks_failed": 0,
        "queue_depth": 0,
        "errors": [],
    }
    await _set_status(redis, state)

    try:
        # ── Phase 1: Heartbeat scan ────────────────────────────────────────────
        await _publish_log(redis, level="info", msg="🔍 סריקת Heartbeat — בודק נודים מחוברים…")
        await asyncio.sleep(0.1)

        nodes_found = 0
        nodes_online = 0
        node_details: list[str] = []

        cursor = 0
        pattern = f"{HEARTBEAT_PREFIX}*"
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=100)
            for key in keys:
                nodes_found += 1
                raw = await redis.get(key)
                if raw:
                    nodes_online += 1
                    try:
                        hb = json.loads(raw)
                        node_id = hb.get("node_id", key)
                        role = hb.get("role", "?")
                        cpu = hb.get("cpu_percent", "?")
                        ram = hb.get("ram_used_mb", "?")
                        ip = hb.get("local_ip", "")
                        node_details.append(
                            f"{node_id} [{role}] CPU:{cpu}% RAM:{ram}MB{' @ ' + ip if ip else ''}"
                        )
                    except Exception:
                        node_details.append(str(key))
            if cursor == 0:
                break

        state["nodes_found"] = nodes_found
        state["nodes_online"] = nodes_online
        await _set_status(redis, state)

        if nodes_online == 0:
            await _publish_log(
                redis,
                level="warn",
                msg="⚠️  אין נודים מחוברים — ודא שה-Worker רץ",
            )
        else:
            await _publish_log(
                redis,
                level="ok",
                msg=f"✅ נמצאו {nodes_online}/{nodes_found} נודים מחוברים",
                detail=" | ".join(node_details),
            )
            for nd in node_details:
                await _publish_log(redis, level="info", msg=f"   └ {nd}")

        # ── Phase 2: Queue depth ───────────────────────────────────────────────
        await asyncio.sleep(0.1)
        queue_depth = int(await redis.zcard(ARQ_QUEUE_KEY) or 0)
        state["queue_depth"] = queue_depth
        await _set_status(redis, state)
        await _publish_log(
            redis,
            level="info",
            msg=f"📋 תור ARQ: {queue_depth} משימות ממתינות",
        )

        # ── Phase 3: Enqueue automation tasks ─────────────────────────────────
        await asyncio.sleep(0.1)
        await _publish_log(redis, level="info", msg="🚀 מתחיל הרצת פעולות אוטומטיות…")

        # Determine which tasks to run
        tasks_to_run = DEFAULT_TASKS
        if task_types:
            tasks_to_run = [t for t in DEFAULT_TASKS if t["task_type"] in task_types]
            # Also allow arbitrary task_types not in DEFAULT_TASKS
            known = {t["task_type"] for t in DEFAULT_TASKS}
            for tt in task_types:
                if tt not in known:
                    tasks_to_run.append({
                        "task_type": tt,
                        "label": tt,
                        "project_id": "telefix",
                        "priority": 1,
                        "parameters": {},
                    })

        try:
            import arq
            from arq.connections import RedisSettings
            from nexus.shared.schemas import TaskPayload

            arq_pool = await arq.create_pool(
                RedisSettings.from_dsn(settings.redis_url),
                default_queue_name="nexus:tasks",
            )

            for task_def in tasks_to_run:
                task_id = str(uuid.uuid4())
                params = dict(task_def.get("parameters") or {})
                if force:
                    params["force"] = True

                try:
                    task = TaskPayload(
                        task_id=task_id,
                        task_type=task_def["task_type"],
                        parameters=params,
                        project_id=task_def.get("project_id", "telefix"),
                        priority=task_def.get("priority", 1),
                    )
                    job = await arq_pool.enqueue_job(
                        "execute_task",
                        task_payload=task.model_dump_for_wire(),
                        _job_id=task_id,
                        _queue_name="nexus:tasks",
                    )
                    if job is None:
                        await _publish_log(
                            redis,
                            level="warn",
                            msg=f"⚠️  {task_def['label']} — כבר בתור (דילוג)",
                            detail=f"task_id={task_id}",
                        )
                    else:
                        state["tasks_enqueued"] += 1
                        await _set_status(redis, state)
                        await _publish_log(
                            redis,
                            level="ok",
                            msg=f"✅ {task_def['label']} — נוסף לתור",
                            detail=f"task_id={task_id[:8]}… | type={task_def['task_type']}",
                        )
                except Exception as exc:
                    state["tasks_failed"] += 1
                    err_msg = f"{task_def['label']}: {exc}"
                    state["errors"].append(err_msg)
                    await _set_status(redis, state)
                    await _publish_log(
                        redis,
                        level="error",
                        msg=f"❌ {task_def['label']} — שגיאה",
                        detail=str(exc),
                    )

                await asyncio.sleep(0.05)

            await arq_pool.aclose()

        except Exception as exc:
            state["errors"].append(f"ARQ pool error: {exc}")
            await _set_status(redis, state)
            await _publish_log(
                redis,
                level="error",
                msg="❌ שגיאה בחיבור ל-ARQ",
                detail=str(exc),
            )

        # ── Phase 4: Final queue depth ─────────────────────────────────────────
        await asyncio.sleep(0.2)
        new_depth = int(await redis.zcard(ARQ_QUEUE_KEY) or 0)
        state["queue_depth"] = new_depth
        added = new_depth - queue_depth
        await _publish_log(
            redis,
            level="info",
            msg=f"📊 תור ARQ לאחר הרצה: {new_depth} משימות ({'+' if added >= 0 else ''}{added})",
        )

        # ── Done ───────────────────────────────────────────────────────────────
        state["phase"] = "done"
        state["finished_at"] = _now_iso()
        await _set_status(redis, state)

        summary = (
            f"✅ סריקה הושלמה — "
            f"{nodes_online} נודים | "
            f"{state['tasks_enqueued']} משימות הורצו | "
            f"{state['tasks_failed']} שגיאות"
        )
        await _publish_log(redis, level="ok", msg=summary)

        # Persist to history
        history_entry = {
            "run_id": run_id,
            "started_at": state["started_at"],
            "finished_at": state["finished_at"],
            "phase": "done",
            "nodes_found": nodes_found,
            "tasks_enqueued": state["tasks_enqueued"],
            "tasks_failed": state["tasks_failed"],
        }
        await redis.rpush(SCAN_HISTORY_KEY, json.dumps(history_entry))
        await redis.ltrim(SCAN_HISTORY_KEY, -SCAN_HISTORY_MAX, -1)

    except Exception as exc:
        log.exception("scan_run_crashed", run_id=run_id, error=str(exc))
        state["phase"] = "error"
        state["finished_at"] = _now_iso()
        state["errors"].append(str(exc))
        await _set_status(redis, state)
        await _publish_log(
            redis,
            level="error",
            msg=f"💥 סריקה נכשלה: {exc}",
        )


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post(
    "/run",
    response_model=ScanRunResponse,
    summary="Trigger a full-system scan + automation run",
)
async def run_scan(body: ScanRunRequest, request: Request, redis: RedisDep) -> ScanRunResponse:
    """
    1. Scans all Redis heartbeat keys to discover online nodes.
    2. Checks ARQ queue depth.
    3. Enqueues all default automation tasks (or a custom subset).
    4. Publishes live log events to the SSE stream.
    """
    # Guard: don't start a new scan if one is already running
    current = await _get_status(redis)
    if current.get("phase") == "running":
        return ScanRunResponse(
            run_id=current["run_id"] or "unknown",
            message="סריקה כבר רצה — המתן לסיומה",
            tasks_requested=[],
        )

    run_id = str(uuid.uuid4())
    task_types = body.tasks

    # Clear old log for this run
    await redis.delete(SCAN_LOG_KEY)

    # Fire and forget — don't block the HTTP response
    asyncio.create_task(
        _run_scan(redis, run_id, task_types, body.force),
        name=f"scan_run_{run_id[:8]}",
    )

    tasks_requested = task_types or [t["task_type"] for t in DEFAULT_TASKS]
    log.info("scan_run_started", run_id=run_id, tasks=tasks_requested)

    return ScanRunResponse(
        run_id=run_id,
        message="סריקה התחילה — עקוב אחרי /api/scan/stream לעדכונים חיים",
        tasks_requested=tasks_requested,
    )


@router.get(
    "/status",
    response_model=ScanStatusResponse,
    summary="Current scan state (polling-friendly)",
)
async def get_scan_status(redis: RedisDep) -> ScanStatusResponse:
    state = await _get_status(redis)

    # Load last 20 log lines
    raw_lines = await redis.lrange(SCAN_LOG_KEY, -20, -1)
    log_lines: list[ScanLogLine] = []
    for raw in raw_lines:
        try:
            d = json.loads(raw) if isinstance(raw, str) else raw
            log_lines.append(ScanLogLine(**d))
        except Exception:
            pass

    return ScanStatusResponse(
        run_id=state.get("run_id"),
        phase=state.get("phase", "idle"),
        started_at=state.get("started_at"),
        finished_at=state.get("finished_at"),
        nodes_found=int(state.get("nodes_found", 0)),
        nodes_online=int(state.get("nodes_online", 0)),
        tasks_enqueued=int(state.get("tasks_enqueued", 0)),
        tasks_failed=int(state.get("tasks_failed", 0)),
        queue_depth=int(state.get("queue_depth", 0)),
        errors=list(state.get("errors") or []),
        last_log=log_lines,
    )


@router.get(
    "/stream",
    summary="SSE: real-time scan log stream",
    response_class=StreamingResponse,
)
async def stream_scan_log(request: Request) -> StreamingResponse:
    """
    Subscribe to the scan log pub/sub channel and stream JSON log lines as SSE.
    Also replays the last 50 lines from Redis on connect (bootstrap).
    """
    from redis.asyncio import from_url

    redis_url = settings.redis_url

    async def _generator() -> AsyncGenerator[str, None]:
        client = from_url(redis_url, decode_responses=True)
        pubsub = None
        try:
            # Bootstrap: send existing log lines
            existing = await client.lrange(SCAN_LOG_KEY, -50, -1)
            for raw in existing:
                yield f"data: {raw}\n\n"

            pubsub = client.pubsub()
            await pubsub.subscribe(SCAN_CHANNEL)

            while True:
                if await request.is_disconnected():
                    break
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=25.0,
                )
                if message and message.get("type") == "message":
                    data = message.get("data", "")
                    if isinstance(data, bytes):
                        data = data.decode()
                    yield f"data: {data}\n\n"
                else:
                    yield ": keep-alive\n\n"
        finally:
            if pubsub is not None:
                try:
                    await pubsub.unsubscribe(SCAN_CHANNEL)
                except Exception:
                    pass
            await client.aclose()

    return StreamingResponse(
        _generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@router.get(
    "/history",
    response_model=ScanHistoryResponse,
    summary="Last N scan run summaries",
)
async def get_scan_history(redis: RedisDep, limit: int = 20) -> ScanHistoryResponse:
    limit = min(limit, 50)
    raw_entries = await redis.lrange(SCAN_HISTORY_KEY, -limit, -1)
    runs: list[ScanHistoryEntry] = []
    for raw in reversed(raw_entries):
        try:
            d = json.loads(raw) if isinstance(raw, str) else raw
            runs.append(ScanHistoryEntry(
                run_id=d.get("run_id", ""),
                started_at=d.get("started_at", ""),
                finished_at=d.get("finished_at"),
                phase=d.get("phase", "done"),
                nodes_found=int(d.get("nodes_found", 0)),
                tasks_enqueued=int(d.get("tasks_enqueued", 0)),
                tasks_failed=int(d.get("tasks_failed", 0)),
            ))
        except Exception:
            pass
    return ScanHistoryResponse(runs=runs, total=len(runs))
