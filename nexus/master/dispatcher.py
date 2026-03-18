"""
Master Dispatcher — the brain of the Nexus Orchestrator.

Responsibilities
----------------
1. Accept TaskPayload objects from any producer (API, CLI, scheduler, agent).
2. Route tasks through the HITL gate if they require human approval.
3. Enqueue approved tasks onto the ARQ Redis queue for worker consumption.
4. Track in-flight jobs and collect results.
5. Publish NodeHeartbeat so the cluster can observe master liveness.

ARQ job model
-------------
ARQ stores each enqueued job in Redis as a hash under a key like:
    arq:job:<job_id>

Workers poll the queue, pick up a job, execute the registered async function,
and write the result back.  To retrieve the outcome, instantiate a Job object:
    job = Job(job_id, redis=arq_pool, _queue_name="nexus:tasks")
    info = await job.result_info()   # non-blocking: returns None if not done
    raw  = await job.result(timeout) # blocking: waits until done or timeout

Extending this dispatcher
-------------------------
- Add a `schedule_task()` method for cron-style deferred dispatch.
- Add a `broadcast_task()` method to fan out to all workers simultaneously.
- Replace the simple result-polling loop with a Redis Streams consumer group
  for guaranteed-delivery result collection at scale.
"""

from __future__ import annotations

import asyncio

import structlog
from arq import ArqRedis, create_pool
from arq.connections import RedisSettings
from arq.jobs import Job, JobResult

from nexus.master.hitl_gate import HitlGate, TaskRejectedError
from nexus.master.resource_guard import ResourceGuard
from nexus.shared.constants import TASK_DEFAULT_TIMEOUT
from nexus.shared.schemas import NodeHeartbeat, NodeRole, TaskPayload, TaskResult, TaskStatus

log = structlog.get_logger(__name__)


class Dispatcher:
    """
    Core master-side orchestrator.

    Parameters
    ----------
    redis_settings : arq.connections.RedisSettings
        Connection details for the shared Redis broker.
    node_id : str
        Unique identifier for this master instance (used in heartbeats).
    resource_guard : ResourceGuard
        Pre-configured guard; its monitor() coroutine should already be
        running as a background task before Dispatcher is created.
    """

    def __init__(
        self,
        redis_settings: RedisSettings,
        node_id: str = "master",
        resource_guard: ResourceGuard | None = None,
    ) -> None:
        self._redis_settings = redis_settings
        self.node_id = node_id
        self._guard = resource_guard
        self._arq: ArqRedis | None = None
        self._hitl_gate: HitlGate | None = None

        # Simple in-memory job tracker: job_id → TaskPayload
        # Replace with a persistent store (SQLite, Postgres) for production.
        self._in_flight: dict[str, TaskPayload] = {}

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Connect to Redis and launch background coroutines."""
        self._arq = await create_pool(
            self._redis_settings,
            default_queue_name="nexus:tasks",
        )
        self._hitl_gate = HitlGate(self._arq)
        await self._hitl_gate.start()

        asyncio.create_task(self._heartbeat_loop(), name="master-heartbeat")
        log.info("dispatcher_started", node_id=self.node_id)

    async def stop(self) -> None:
        """Gracefully close the Redis connection."""
        if self._arq:
            await self._arq.aclose()
        log.info("dispatcher_stopped", node_id=self.node_id)

    # ── Public API ─────────────────────────────────────────────────────────────

    async def dispatch(self, task: TaskPayload) -> str:
        """
        Route a task through the HITL gate (if needed) and enqueue it.

        Returns the ARQ job ID which can be used to poll for the result.

        HITL hook
        ---------
        If `task.requires_approval` is True, this coroutine suspends here
        until a human approves or rejects the task via the HITL gate.
        The worker queue is not touched until approval is granted, so no
        worker capacity is consumed during the wait.
        """
        assert self._arq is not None, "Dispatcher.start() must be called first"
        assert self._hitl_gate is not None

        log.info("task_received", task_id=task.task_id, task_type=task.task_type)

        # ── HITL gate ──────────────────────────────────────────────────────────
        # This await may block for up to HITL_APPROVAL_TIMEOUT seconds while
        # a human reviews the task.  All other dispatches continue concurrently
        # because this is async — only THIS task is paused.
        try:
            await self._hitl_gate.request_approval(task)
        except TaskRejectedError:
            log.info("task_rejected_by_hitl", task_id=task.task_id)
            raise

        # ── Enqueue onto ARQ ───────────────────────────────────────────────────
        # `execute_task` is the function name registered on the worker side
        # (see nexus/worker/listener.py).  ARQ serialises the kwargs to JSON
        # and stores them in Redis; the worker deserialises and calls the fn.
        job = await self._arq.enqueue_job(
            "execute_task",
            task_payload=task.model_dump(),
            _job_id=task.task_id,
            _queue_name="nexus:tasks",
            _expires=TASK_DEFAULT_TIMEOUT,
        )

        if job is None:
            # ARQ returns None if a job with the same ID already exists.
            log.warning("task_already_enqueued", task_id=task.task_id)
            return task.task_id

        self._in_flight[task.task_id] = task
        log.info("task_enqueued", task_id=task.task_id, job_id=job.job_id)
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
          - result_info() is a non-blocking probe (returns None while running).
          - result(timeout) is the blocking form used here to avoid a busy loop.

        For fire-and-forget workflows, skip this and let workers publish results
        to a results stream independently.
        """
        assert self._arq is not None

        job = Job(task_id, redis=self._arq, _queue_name="nexus:tasks")

        # Block until the job finishes or the timeout elapses.
        # job.result() raises asyncio.TimeoutError on expiry and
        # arq.jobs.JobExecutionFailed if the worker raised an unhandled exception.
        raw: dict = await job.result(timeout=timeout, poll_delay=poll_interval)

        # Fetch the full metadata (start/finish times, success flag, kwargs).
        info: JobResult | None = await job.result_info()

        self._in_flight.pop(task_id, None)

        worker_id: str = "unknown"
        started_at = None
        finished_at = None

        if info is not None:
            # kwargs holds the arguments passed to execute_task on the worker.
            worker_id = info.kwargs.get("task_payload", {}).get("task_id", "unknown")
            # Prefer the worker_id embedded in the result dict itself.
            if isinstance(raw, dict):
                worker_id = raw.get("worker_id", worker_id)
            started_at = info.start_time
            finished_at = info.finish_time

        error: str | None = raw.get("error") if isinstance(raw, dict) else None
        output = raw.get("output") if isinstance(raw, dict) else raw

        return TaskResult(
            task_id=task_id,
            worker_id=worker_id,
            status=TaskStatus.FAILED if error else TaskStatus.COMPLETED,
            output=output,
            error=error,
            started_at=started_at,
            finished_at=finished_at,
        )

    async def dispatch_and_wait(self, task: TaskPayload) -> TaskResult:
        """Convenience: dispatch a task and block until the result is ready."""
        job_id = await self.dispatch(task)
        return await self.get_result(job_id)

    # ── Internal helpers ───────────────────────────────────────────────────────

    async def _heartbeat_loop(self, interval: float = 30.0) -> None:
        """
        Publish a NodeHeartbeat every `interval` seconds.

        Workers and a future dashboard can subscribe to the heartbeat channel
        to detect master failures and trigger failover logic.
        """
        while True:
            await asyncio.sleep(interval)
            stats = (
                self._guard.current_stats()
                if self._guard
                else {"cpu_percent": 0.0, "ram_mb": 0.0}
            )
            heartbeat = NodeHeartbeat(
                node_id=self.node_id,
                role=NodeRole.MASTER,
                cpu_percent=stats["cpu_percent"],
                ram_used_mb=stats["ram_mb"],
                active_jobs=len(self._in_flight),
            )
            if self._arq:
                await self._arq.publish(  # type: ignore[attr-defined]
                    "nexus:heartbeats",
                    heartbeat.model_dump_json(),
                )
            log.debug("heartbeat_published", **heartbeat.model_dump())
