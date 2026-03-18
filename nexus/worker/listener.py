"""
Worker Listener — the execution engine running on each Worker Node.

How it works
------------
ARQ uses a WorkerSettings class (not a running server) to configure the
worker process.  When `arq nexus.worker.listener.WorkerSettings` is invoked
(or `scripts/start_worker.py` is run), ARQ:

1. Connects to Redis using `redis_settings`.
2. Polls the `nexus:tasks` queue for jobs.
3. Calls `execute_task(**job_kwargs)` for each job it picks up.
4. Stores the return value (or exception) back in Redis.

The `execute_task` function is the single entry point for ALL task types.
It delegates to the TaskRegistry which routes by `task_type`.

HITL hook (worker side)
-----------------------
If a task's execution reaches a point where it needs a human decision
mid-execution (not just pre-approval), the handler can raise a
`HitlPauseRequested` exception (TODO: define in shared/schemas.py).
The listener catches this, publishes a HitlRequest, and re-queues the
task with a delay.  This pattern is sketched in the `execute_task` docstring.

Deploying to remote nodes
-------------------------
Copy the entire `nexus/` package and `scripts/start_worker.py` to each
Worker Node.  Install dependencies with `pip install -e .` (or from
requirements.txt).  Set the same REDIS_URL in .env so all nodes share the
same broker.  The Linux and Windows workers are otherwise identical — ARQ
is cross-platform.
"""

from __future__ import annotations

import os
import socket
from datetime import datetime, timezone
from typing import Any

import structlog
from arq.connections import RedisSettings

from nexus.worker.task_registry import registry  # noqa: F401 — side-effect: registers built-ins

log = structlog.get_logger(__name__)

# Unique identifier for this worker process.
# Override with NODE_ID env var when deploying multiple workers.
WORKER_ID = os.getenv("NODE_ID", f"worker-{socket.gethostname()}")


async def startup(ctx: dict[str, Any]) -> None:
    """
    Called once by ARQ when the worker process starts.

    Use this to initialise expensive shared resources (DB connections,
    ML model loading, HTTP client sessions) that should be reused across
    many task executions rather than recreated per-task.
    """
    ctx["worker_id"] = WORKER_ID
    ctx["started_at"] = datetime.now(timezone.utc)
    log.info("worker_started", worker_id=WORKER_ID, registered_tasks=registry.registered_types)


async def shutdown(ctx: dict[str, Any]) -> None:
    """
    Called once by ARQ when the worker process shuts down cleanly.

    Close any resources opened in `startup` here.
    """
    log.info("worker_shutdown", worker_id=ctx.get("worker_id", WORKER_ID))


async def execute_task(
    ctx: dict[str, Any], task_payload: dict[str, Any], **_: Any
) -> dict[str, Any]:
    """
    Universal task handler — the single ARQ function registered on every worker.

    Parameters
    ----------
    ctx          : ARQ context dict (populated by startup(); holds shared resources).
    task_payload : Serialised TaskPayload dict sent by the master's Dispatcher.

    Returns
    -------
    A dict with keys `output` and optionally `error`, `worker_id`.
    ARQ stores this in Redis; the master's Dispatcher.get_result() retrieves it.

    HITL mid-execution hook (future)
    ---------------------------------
    If a handler needs a human decision partway through (e.g., an LLM agent
    reaches an ambiguous branch), it can raise HitlPauseRequested(context=...).
    The except block below would:
        1. Publish a HitlRequest to HITL_REQUEST_CHANNEL.
        2. Re-enqueue this task with a delay (arq deferred job).
        3. Return a sentinel result so the master marks it AWAITING_APPROVAL.
    This keeps the worker free to process other tasks during the human wait.
    """
    from nexus.shared.schemas import TaskPayload  # local import avoids circular at module level

    worker_id: str = ctx.get("worker_id", WORKER_ID)
    started_at = datetime.now(timezone.utc)

    # Deserialise and validate the payload using the shared Pydantic schema.
    task = TaskPayload.model_validate(task_payload)

    log.info(
        "task_started",
        task_id=task.task_id,
        task_type=task.task_type,
        worker_id=worker_id,
    )

    try:
        output = await registry.execute(task.task_type, task.parameters)
        finished_at = datetime.now(timezone.utc)
        duration = (finished_at - started_at).total_seconds()

        log.info(
            "task_completed",
            task_id=task.task_id,
            task_type=task.task_type,
            worker_id=worker_id,
            duration_s=round(duration, 3),
        )
        return {
            "output": output,
            "error": None,
            "worker_id": worker_id,
            "duration_seconds": duration,
        }

    except KeyError as exc:
        # Unknown task_type — configuration error, not a transient failure.
        log.error("task_unknown_type", task_id=task.task_id, error=str(exc))
        return {"output": None, "error": str(exc), "worker_id": worker_id}

    except Exception as exc:
        # Handler raised an unexpected error.  Log it and return a failure
        # result rather than letting ARQ retry indefinitely.
        log.exception("task_failed", task_id=task.task_id, task_type=task.task_type, error=str(exc))
        return {"output": None, "error": str(exc), "worker_id": worker_id}


# ── ARQ WorkerSettings ─────────────────────────────────────────────────────────
# ARQ discovers configuration by importing this class.
# Run the worker with:
#   arq nexus.worker.listener.WorkerSettings
# or via scripts/start_worker.py which calls arq programmatically.

class WorkerSettings:
    # The async functions ARQ is allowed to execute.
    functions = [execute_task]

    # Lifecycle hooks.
    on_startup = startup
    on_shutdown = shutdown

    # Redis connection — reads REDIS_URL from environment.
    redis_settings = RedisSettings.from_dsn(
        os.getenv("REDIS_URL", "redis://localhost:6379/0")
    )

    # Queue to consume from — must match the queue the master enqueues onto.
    queue_name = "nexus:tasks"

    # Maximum concurrent jobs per worker process.
    # Set via WORKER_MAX_JOBS env var; default 4.
    max_jobs: int = int(os.getenv("WORKER_MAX_JOBS", "4"))

    # How long (seconds) a job may run before ARQ hard-cancels it.
    job_timeout: int = int(os.getenv("TASK_DEFAULT_TIMEOUT", "300"))

    # How long (seconds) to keep job results in Redis after completion.
    keep_result: int = 86400  # 24 hours

    # Retry policy: attempt each job once by default.
    # Increase max_tries for tasks that are safe to retry on transient errors.
    max_tries: int = 1
