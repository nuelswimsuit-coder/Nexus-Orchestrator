"""
Master Node entrypoint.

Usage
-----
    python scripts/start_master.py

Or via the installed CLI entrypoint (after `pip install -e .`):
    nexus-master

What this script does
---------------------
1. Loads settings from .env.
2. Configures structured logging.
3. Applies OS-level low-priority scheduling so the master runs quietly
   in the background without competing with your foreground applications.
4. Starts the ResourceGuard background monitor.
5. Connects the Dispatcher to Redis.
6. Runs a simple demo loop that dispatches two smoke-test tasks and prints
   the results.  Replace this loop with your real orchestration logic.
"""

from __future__ import annotations

import asyncio

import structlog
from arq.connections import RedisSettings

from nexus.master.dispatcher import Dispatcher
from nexus.master.resource_guard import ResourceGuard, apply_low_priority
from nexus.shared.config import settings
from nexus.shared.logging_config import configure_logging
from nexus.shared.schemas import TaskPayload

log = structlog.get_logger(__name__)


async def run() -> None:
    # ── 1. Logging ─────────────────────────────────────────────────────────────
    configure_logging(level=settings.log_level, node_id=settings.node_id)
    log.info("nexus_master_starting", node_id=settings.node_id)

    # ── 2. Resource management ─────────────────────────────────────────────────
    # Lower OS scheduling priority so the master yields to foreground apps.
    apply_low_priority()

    guard = ResourceGuard(
        cpu_cap_percent=settings.master_cpu_cap_percent,
        ram_cap_mb=settings.master_ram_cap_mb,
    )
    # Fire-and-forget: runs concurrently with everything else.
    asyncio.create_task(guard.monitor(), name="resource-guard")

    # ── 3. Dispatcher ──────────────────────────────────────────────────────────
    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    dispatcher = Dispatcher(
        redis_settings=redis_settings,
        node_id=settings.node_id,
        resource_guard=guard,
    )
    await dispatcher.start()

    # ── 4. Demo orchestration loop ─────────────────────────────────────────────
    # Replace everything below with your real workflow logic.
    # This section is intentionally minimal — just enough to verify the
    # full stack is wired correctly end-to-end.

    log.info("dispatching_smoke_tests")

    echo_task = TaskPayload(
        task_type="system.echo",
        parameters={"message": "Hello from the Master Node!"},
    )
    sleep_task = TaskPayload(
        task_type="system.sleep",
        parameters={"seconds": 2},
    )

    # Dispatch both tasks concurrently and wait for both results.
    job_id_echo, job_id_sleep = await asyncio.gather(
        dispatcher.dispatch(echo_task),
        dispatcher.dispatch(sleep_task),
    )
    log.info("tasks_dispatched", echo_job=job_id_echo, sleep_job=job_id_sleep)

    # Poll for results (blocking — swap for fire-and-forget in production).
    echo_result, sleep_result = await asyncio.gather(
        dispatcher.get_result(job_id_echo),
        dispatcher.get_result(job_id_sleep),
    )

    log.info("echo_result", result=echo_result.model_dump())
    log.info("sleep_result", result=sleep_result.model_dump())

    # ── 5. Keep master alive (replace with your event loop / scheduler) ────────
    log.info("master_ready", hint="Replace the demo loop with your orchestration logic.")
    try:
        await asyncio.Event().wait()  # block forever until Ctrl-C
    except asyncio.CancelledError:
        pass
    finally:
        await dispatcher.stop()
        log.info("nexus_master_stopped")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
