"""
Background loop on the Master: enqueue ``retention.guardian.monitor`` every
``RETENTION_MONITOR_INTERVAL_S`` (default 4 hours).

Disabled when ``RETENTION_MONITOR_ENABLED`` is 0/false/off or when
``RETENTION_GROUPS_JSON`` is unset/empty (nothing to monitor).
"""

from __future__ import annotations

import asyncio
import json
import os

import structlog

from nexus.core.dispatcher import Dispatcher
from nexus.shared.schemas import TaskPayload

log = structlog.get_logger(__name__)


def _retention_enabled() -> bool:
    v = (os.getenv("RETENTION_MONITOR_ENABLED") or "1").strip().lower()
    return v not in {"0", "false", "no", "off"}


def _has_groups_configured() -> bool:
    raw = (os.getenv("RETENTION_GROUPS_JSON") or "").strip()
    if not raw:
        return False
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return False
    return isinstance(data, list) and len(data) > 0


async def run_retention_guardian_loop(dispatcher: Dispatcher) -> None:
    if not _retention_enabled():
        log.info("retention_guardian_loop_disabled", reason="RETENTION_MONITOR_ENABLED off")
        return
    if not _has_groups_configured():
        log.info(
            "retention_guardian_loop_idle",
            hint="Set RETENTION_GROUPS_JSON to enable 4-hour retention checks",
        )
        return

    interval_s = int(os.getenv("RETENTION_MONITOR_INTERVAL_S", str(4 * 3600)))
    warmup_s = float(os.getenv("RETENTION_MONITOR_WARMUP_S", "120"))

    log.info(
        "retention_guardian_loop_started",
        interval_s=interval_s,
        warmup_s=warmup_s,
    )

    await asyncio.sleep(warmup_s)

    while True:
        task = TaskPayload(
            task_type="retention.guardian.monitor",
            parameters={},
            project_id="retention-guardian",
            priority=4,
            job_expires_seconds=max(3600, interval_s + 900),
        )
        try:
            job_id = await dispatcher.dispatch(task)
            log.info("retention_guardian_dispatched", job_id=job_id)
        except Exception as exc:
            log.error("retention_guardian_dispatch_failed", error=str(exc))
        await asyncio.sleep(interval_s)
