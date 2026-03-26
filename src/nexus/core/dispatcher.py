"""
Master Dispatcher — the brain of the Nexus Orchestrator.

Responsibilities
----------------
1. Accept TaskPayload objects from any producer (API, CLI, scheduler, agent).
2. Inject secrets from the Vault into the payload before dispatch.
3. Validate worker capability requirements and route accordingly.
4. Route tasks through the HITL gate if they require human approval.
5. Enqueue approved tasks onto the ARQ Redis queue for worker consumption.
6. Track in-flight jobs and collect results.
7. Publish NodeHeartbeat so the cluster can observe master liveness.
8. Run cron-scheduled tasks (e.g. nightly auto-scrape at 02:00).

ARQ job model
-------------
ARQ stores each enqueued job in Redis as a hash under a key like:
    arq:job:<job_id>

Workers poll the queue, pick up a job, execute the registered async function,
and write the result back.  To retrieve the outcome, instantiate a Job object:
    job = Job(job_id, redis=arq_pool, _queue_name="nexus:tasks")
    info = await job.result_info()   # non-blocking: returns None if not done
    raw  = await job.result(timeout) # blocking: waits until done or timeout

Cron scheduler
--------------
`CronScheduler` runs as a background asyncio task.  It wakes up every minute,
checks whether any registered cron entry is due, and calls `dispatcher.dispatch()`
for matching tasks.  Entries are defined as (hour, minute, TaskPayload) tuples
in local time.  The scheduler is intentionally simple — for production use
replace with APScheduler or Celery Beat.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime

import structlog
from arq import ArqRedis, create_pool
from arq.connections import RedisSettings
from arq.jobs import Job, JobResult

from nexus.core.hitl_gate import HitlGate, TaskRejectedError
from nexus.core.resource_guard import ResourceGuard
from nexus.services.vault import Vault
from nexus.shared.constants import TASK_DEFAULT_TIMEOUT
from nexus.shared.notifications.service import NotificationService
from nexus.shared.fleet_redis import (
    FLEET_SCAN_TASK_TYPES,
    get_fleet_counter_snapshot,
    parse_fleet_audit_from_task_output,
    persist_fleet_audit_latest,
    persist_fleet_audit_sqlite,
    publish_fleet_scan_event,
    reset_fleet_member_counters,
)
from nexus.shared.schemas import (
    FleetAuditResults,
    FleetScanEvent,
    FleetScanPhase,
    NodeHeartbeat,
    NodeRole,
    TaskPayload,
    TaskResult,
    TaskStatus,
)

log = structlog.get_logger(__name__)

HEARTBEAT_KEY_PREFIX = "nexus:heartbeat:"


# ── Cron Scheduler ─────────────────────────────────────────────────────────────

@dataclass
class CronEntry:
    """A single scheduled task — fires at (hour, minute) local time daily."""
    hour: int
    minute: int
    task: TaskPayload
    name: str = ""
    _last_fired_date: str = field(default="", repr=False)


class CronScheduler:
    """
    Lightweight daily cron scheduler for the Master Dispatcher.

    Wakes up every 60 seconds, checks whether any registered entry is due
    (matching current local hour:minute), and dispatches it if it hasn't
    already fired today.

    Usage
    -----
        scheduler = CronScheduler(dispatcher)
        scheduler.add(hour=2, minute=0, task=my_task, name="nightly-scrape")
        asyncio.create_task(scheduler.run())
    """

    def __init__(self, dispatcher: Dispatcher) -> None:
        self._dispatcher = dispatcher
        self._entries: list[CronEntry] = []

    def add(self, hour: int, minute: int, task: TaskPayload, name: str = "") -> None:
        """Register a task to fire daily at (hour, minute) local time."""
        entry = CronEntry(hour=hour, minute=minute, task=task, name=name or task.task_type)
        self._entries.append(entry)
        log.info("cron_entry_registered", name=entry.name, at=f"{hour:02d}:{minute:02d}")

    async def run(self) -> None:
        """Background loop — checks every 60 s and fires due entries."""
        log.info("cron_scheduler_started", entries=len(self._entries))
        while True:
            await asyncio.sleep(60)
            now = datetime.now()
            today = now.strftime("%Y-%m-%d")
            for entry in self._entries:
                if now.hour == entry.hour and now.minute == entry.minute:
                    if entry._last_fired_date == today:
                        continue  # already fired today
                    entry._last_fired_date = today
                    log.info(
                        "cron_firing",
                        name=entry.name,
                        at=f"{entry.hour:02d}:{entry.minute:02d}",
                    )
                    asyncio.create_task(
                        self._safe_dispatch(entry),
                        name=f"cron-{entry.name}",
                    )

    async def _safe_dispatch(self, entry: CronEntry) -> None:
        try:
            job_id = await self._dispatcher.dispatch(entry.task)
            log.info("cron_dispatched", name=entry.name, job_id=job_id)
        except Exception as exc:
            log.error("cron_dispatch_error", name=entry.name, error=str(exc))


class CapabilityNotAvailableError(Exception):
    """Raised when no online worker satisfies the task's required_capabilities."""


class Dispatcher:
    """
    Core master-side orchestrator.

    Parameters
    ----------
    redis_settings       : ARQ connection details for the shared Redis broker.
    node_id              : Unique identifier for this master instance.
    resource_guard       : Pre-configured guard (monitor() should already run).
    vault                : Secrets vault for injecting credentials at dispatch.
    notification_service : Optional alert fanout for HITL and failure events.
    """

    def __init__(
        self,
        redis_settings: RedisSettings,
        node_id: str = "master",
        resource_guard: ResourceGuard | None = None,
        vault: Vault | None = None,
        notification_service: NotificationService | None = None,
    ) -> None:
        self._redis_settings = redis_settings
        self.node_id = node_id
        self._guard = resource_guard
        self._vault = vault or Vault()
        self._notifier = notification_service
        self._arq: ArqRedis | None = None
        self._hitl_gate: HitlGate | None = None
        self.cron: CronScheduler = CronScheduler(self)

        # Simple in-memory job tracker: task_id → TaskPayload
        # Replace with a persistent store (SQLite, Postgres) for production.
        self._in_flight: dict[str, TaskPayload] = {}

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Connect to Redis and launch background coroutines."""
        self._arq = await create_pool(
            self._redis_settings,
            default_queue_name="nexus:tasks",
        )
        self._hitl_gate = HitlGate(
            redis=self._arq,
            notification_service=self._notifier,
        )
        await self._hitl_gate.start()

        asyncio.create_task(self._heartbeat_loop(), name="master-heartbeat")
        asyncio.create_task(self.cron.run(), name="cron-scheduler")
        log.info(
            "dispatcher_started",
            node_id=self.node_id,
            vault_backend=type(self._vault._backend).__name__,
            notifications=self._notifier.provider_names if self._notifier else [],
        )

    async def stop(self) -> None:
        """Gracefully stop the HITL gate listener and close the Redis connection."""
        if self._hitl_gate:
            await self._hitl_gate.stop()
        if self._arq:
            await self._arq.aclose()
        log.info("dispatcher_stopped", node_id=self.node_id)

    # ── Public API ─────────────────────────────────────────────────────────────

    async def dispatch(self, task: TaskPayload) -> str:
        """
        Prepare, validate, gate, and enqueue a task.

        Steps
        -----
        1. Vault injection  — secrets are merged into the payload in-memory.
        2. Capability check — verify an online worker can handle this task.
        3. HITL gate        — suspend if human approval is required.
        4. ARQ enqueue      — push the job onto the Redis queue.

        Returns the ARQ job ID (== task_id) for result polling.

        Raises
        ------
        CapabilityNotAvailableError — no capable worker is online.
        TaskRejectedError           — operator rejected at the HITL gate.
        asyncio.TimeoutError        — HITL gate timed out.
        """
        assert self._arq is not None, "Dispatcher.start() must be called first"
        assert self._hitl_gate is not None

        log.info(
            "task_received",
            task_id=task.task_id,
            task_type=task.task_type,
            project_id=task.project_id,
            priority=task.priority,
            required_capabilities=task.required_capabilities,
        )

        # ── Step 1: Vault injection ────────────────────────────────────────────
        task = self._vault.inject(task)

        # ── Step 2: Capability routing ─────────────────────────────────────────
        if task.required_capabilities:
            await self._assert_capable_worker(task)

        # ── Step 3: HITL gate ──────────────────────────────────────────────────
        # This await may block for up to HITL_APPROVAL_TIMEOUT seconds.
        # All other dispatches continue concurrently — only THIS task is paused.
        try:
            await self._hitl_gate.request_approval(task)
        except TaskRejectedError:
            log.info("task_rejected_by_hitl", task_id=task.task_id)
            raise

        # ── Step 4: Enqueue onto ARQ ───────────────────────────────────────────
        # Use model_dump_for_wire() to include injected_secrets in the payload
        # (they are excluded from the default model_dump() to prevent logging).
        job_ttl = task.job_expires_seconds or TASK_DEFAULT_TIMEOUT
        job = await self._arq.enqueue_job(
            "execute_task",
            task_payload=task.model_dump_for_wire(),
            _job_id=task.task_id,
            _queue_name="nexus:tasks",
            _expires=job_ttl,
        )

        if job is None:
            log.warning("task_already_enqueued", task_id=task.task_id)
            return task.task_id

        self._in_flight[task.task_id] = task
        log.info(
            "task_enqueued",
            task_id=task.task_id,
            job_id=job.job_id,
            project_id=task.project_id,
        )

        # Fleet scan: reset global member counters and notify dashboards (SSE / Redis status).
        if task.task_type in FLEET_SCAN_TASK_TYPES:
            await reset_fleet_member_counters(self._arq)
            await publish_fleet_scan_event(
                self._arq,
                FleetScanEvent(
                    phase=FleetScanPhase.STARTED,
                    task_id=task.task_id,
                    task_type=task.task_type,
                    detail="Fleet scan dispatched to worker queue",
                ),
            )

        return job.job_id

    async def get_result(
        self,
        task_id: str,
        timeout: float = TASK_DEFAULT_TIMEOUT,
        poll_interval: float = 1.0,
    ) -> TaskResult:
        """
        Wait for the job with `task_id` to finish and return its TaskResult.

        Uses arq.jobs.Job for correct result retrieval:
          - result(timeout) blocks until the job finishes or timeout elapses.
          - result_info() fetches full metadata (start/finish times, etc.).
        """
        assert self._arq is not None

        job = Job(task_id, redis=self._arq, _queue_name="nexus:tasks")
        raw: dict = await job.result(timeout=timeout, poll_delay=poll_interval)
        info: JobResult | None = await job.result_info()

        original_task = self._in_flight.pop(task_id, None)

        worker_id = "unknown"
        started_at = None
        finished_at = None

        if info is not None:
            if isinstance(raw, dict):
                worker_id = raw.get("worker_id", worker_id)
            started_at = info.start_time
            finished_at = info.finish_time

        error: str | None = raw.get("error") if isinstance(raw, dict) else None
        output = raw.get("output") if isinstance(raw, dict) else raw

        # Fire failure notification if the task failed and we have a notifier.
        if error and self._notifier:
            asyncio.create_task(
                self._notifier.notify_task_failed(
                    task_id=task_id,
                    task_type=original_task.task_type if original_task else "unknown",
                    error=error,
                    attempt=1,
                    max_tries=3,
                ),
                name=f"notify-fail-{task_id}",
            )

        # Persist fleet audit + publish scan-ended when this task was a fleet scan.
        if (
            self._arq
            and original_task
            and original_task.task_type in FLEET_SCAN_TASK_TYPES
        ):
            snap = await get_fleet_counter_snapshot(self._arq)
            audit = parse_fleet_audit_from_task_output(output)
            if audit is None:
                audit = FleetAuditResults(
                    task_id=task_id,
                    worker_id=worker_id,
                    source="dispatcher",
                    groups=[],
                    total_managed_members=snap["total_managed_members"],
                    total_premium_members=snap["total_premium_members"],
                )
            else:
                audit = audit.model_copy(
                    update={
                        "task_id": task_id,
                        "worker_id": worker_id,
                        "total_managed_members": snap["total_managed_members"],
                        "total_premium_members": snap["total_premium_members"],
                    }
                )
            await persist_fleet_audit_latest(self._arq, audit)
            await persist_fleet_audit_sqlite(audit)
            await publish_fleet_scan_event(
                self._arq,
                FleetScanEvent(
                    phase=FleetScanPhase.ENDED,
                    task_id=task_id,
                    task_type=original_task.task_type,
                    detail="completed" if not error else (error or "failed"),
                    managed_members_total=snap["total_managed_members"],
                    premium_members_total=snap["total_premium_members"],
                ),
            )

        result = TaskResult(
            task_id=task_id,
            worker_id=worker_id,
            status=TaskStatus.FAILED if error else TaskStatus.COMPLETED,
            output=output,
            error=error,
            started_at=started_at,
            finished_at=finished_at,
        )
        if self._arq is not None:
            try:
                from nexus.shared.cc_events import publish_cc_event

                asyncio.create_task(
                    publish_cc_event(
                        self._arq,
                        "task_finished",
                        {
                            "task_id": task_id,
                            "worker_id": worker_id,
                            "status": result.status.value,
                            "project_id": (
                                original_task.project_id
                                if original_task
                                else "default"
                            ),
                            "task_type": (
                                original_task.task_type if original_task else None
                            ),
                            "error": bool(error),
                        },
                    )
                )
            except Exception:
                pass

        return result

    async def dispatch_and_wait(self, task: TaskPayload) -> TaskResult:
        """Convenience: dispatch a task and block until the result is ready."""
        job_id = await self.dispatch(task)
        return await self.get_result(job_id)

    # ── Capability routing ─────────────────────────────────────────────────────

    async def _assert_capable_worker(self, task: TaskPayload) -> None:
        """
        Scan live heartbeat keys and verify at least one online worker
        declares all of the task's required_capabilities.

        Raises CapabilityNotAvailableError if no capable worker is found.
        """
        assert self._arq is not None
        required = set(task.required_capabilities)

        cursor = 0
        pattern = f"{HEARTBEAT_KEY_PREFIX}*".encode()
        while True:
            cursor, keys = await self._arq.scan(
                cursor=cursor, match=pattern, count=100
            )
            for key in keys:
                raw = await self._arq.get(key)
                if raw is None:
                    continue
                try:
                    hb = NodeHeartbeat.model_validate_json(raw)
                    if hb.role == NodeRole.WORKER and required.issubset(
                        set(hb.capabilities)
                    ):
                        log.debug(
                            "capable_worker_found",
                            worker=hb.node_id,
                            capabilities=hb.capabilities,
                        )
                        return
                except Exception:
                    pass
            if cursor == 0:
                break

        raise CapabilityNotAvailableError(
            f"No online worker satisfies required_capabilities={list(required)} "
            f"for task_type='{task.task_type}'"
        )

    # ── Internal helpers ───────────────────────────────────────────────────────

    async def _heartbeat_loop(self, interval: float = 30.0) -> None:
        """
        Publish a NodeHeartbeat every `interval` seconds.

        Two delivery mechanisms:
        - Redis key "nexus:heartbeat:<node_id>" with TTL — for the API's
          cluster/status endpoint (SCAN-based, no subscription needed).
        - Redis pub/sub channel "nexus:heartbeats" — for real-time subscribers.
        """
        import os as _os
        from nexus.agents.hardware import get_hardware_info
        hw = get_hardware_info()

        heartbeat_key = f"{HEARTBEAT_KEY_PREFIX}{self.node_id}"
        key_ttl = int(interval * 2)
        display_name = _os.getenv("NODE_DISPLAY_NAME", "")

        while True:
            await asyncio.sleep(interval)
            stats = (
                self._guard.current_stats()
                if self._guard
                else {"cpu_percent": 0.0, "ram_mb": 0.0}
            )
            from nexus.shared.system_stats import get_cpu_temp_celsius  # noqa: PLC0415
            raw_temp = get_cpu_temp_celsius()
            cpu_temp_c = raw_temp if raw_temp is not None else -1.0

            heartbeat = NodeHeartbeat(
                node_id=self.node_id,
                role=NodeRole.MASTER,
                cpu_percent=stats["cpu_percent"],
                ram_used_mb=stats["ram_mb"],
                active_jobs=len(self._in_flight),
                capabilities=[],
                # Phase 3 hardware fields
                local_ip=hw["local_ip"],
                cpu_model=hw["cpu_model"],
                gpu_model=hw["gpu_model"],
                ram_total_mb=hw["ram_total_mb"],
                active_tasks_count=len(self._in_flight),
                os_info=hw["os_info"],
                # Phase 4 extended hardware
                motherboard=hw.get("motherboard", "N/A"),
                cpu_temp_c=cpu_temp_c,
                display_name=display_name,
            )
            if self._arq:
                payload = heartbeat.model_dump_json()
                await self._arq.set(heartbeat_key, payload, ex=key_ttl)
                await self._arq.publish("nexus:heartbeats", payload)  # type: ignore[attr-defined]
                await self._arq.publish("nexus:heartbeat", payload)  # type: ignore[attr-defined]
            log.debug("heartbeat_published", **heartbeat.model_dump())
