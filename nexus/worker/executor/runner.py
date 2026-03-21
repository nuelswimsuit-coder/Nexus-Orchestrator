"""
Task Runner — the execution core inside each Worker Node.

This module is the bridge between the ARQ entry point (`execute_task` in
listener.py) and the TaskRegistry.

Responsibilities
----------------
- Validate the incoming TaskPayload (Pydantic).
- Check that the worker declares all required capabilities.
- Delegate to the TaskRegistry for the actual handler call.
- Capture timing, format the result dict, and handle errors uniformly.
- Exponential backoff retry on transient failures.
- Global exception handler ensures no task ever silently disappears.

Exponential Backoff
-------------------
Transient failures (network errors, temporary resource exhaustion) are
retried with exponential backoff:

    attempt 1 → wait 2s
    attempt 2 → wait 4s
    attempt 3 → wait 8s  (then ARQ marks permanently failed)

Non-transient errors (KeyError for unknown task type, capability mismatch)
are returned immediately without retry.

Capability declaration
----------------------
Set WORKER_CAPABILITIES in .env:

    WORKER_CAPABILITIES=linux-only,high-ram
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone
from typing import Any

import structlog
import psutil

from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

_RAW_CAPS = os.getenv("WORKER_CAPABILITIES", "any")
WORKER_CAPABILITIES: set[str] = {c.strip() for c in _RAW_CAPS.split(",") if c.strip()}

# Auto-augment capabilities so routing works even when env vars are minimal.
if sys.platform.startswith("win"):
    WORKER_CAPABILITIES.add("windows-only")
else:
    WORKER_CAPABILITIES.add("linux-only")

if (psutil.virtual_memory().total / (1024 * 1024 * 1024)) >= 12:
    WORKER_CAPABILITIES.add("high-ram")

# Errors that should NOT be retried (configuration / logic errors)
_NON_RETRYABLE = (KeyError, ValueError, TypeError, NotImplementedError)

# Backoff delays per attempt index (seconds)
_BACKOFF_DELAYS = [2.0, 4.0, 8.0]
MAX_RETRIES = int(os.getenv("TASK_MAX_RETRIES", "3"))


async def run_task(
    task_payload: dict[str, Any],
    worker_id: str,
    redis: Any = None,
) -> dict[str, Any]:
    """
    Execute a task payload with exponential backoff retry.

    Parameters
    ----------
    task_payload : Deserialised TaskPayload dict (from ARQ job kwargs).
    worker_id    : Identity of this worker process.
    redis        : Optional Redis client for status writes.

    Returns
    -------
    dict with keys: output, error, worker_id, duration_seconds, project_id,
                    attempts (int)
    """
    from nexus.shared.schemas import TaskPayload

    started_at = datetime.now(timezone.utc)

    # ── Validate payload ───────────────────────────────────────────────────────
    try:
        task = TaskPayload.model_validate(task_payload)
    except Exception as exc:
        log.error(
            "task_payload_invalid",
            worker_id=worker_id,
            error=str(exc),
        )
        return {
            "output": None,
            "error": f"Invalid payload: {exc}",
            "worker_id": worker_id,
            "duration_seconds": 0.0,
            "project_id": task_payload.get("project_id", "unknown"),
            "attempts": 1,
        }

    # ── Capability check ───────────────────────────────────────────────────────
    required = set(task.required_capabilities)
    if required and not required.issubset(WORKER_CAPABILITIES | {"any"}):
        missing = required - WORKER_CAPABILITIES
        error_msg = (
            f"Worker '{worker_id}' lacks capabilities {missing}. "
            f"Declared: {WORKER_CAPABILITIES}. Required: {required}."
        )
        log.warning(
            "worker_capability_mismatch",
            task_id=task.task_id,
            project_id=task.project_id,
            required=list(required),
            declared=list(WORKER_CAPABILITIES),
        )
        return {
            "output": None,
            "error": error_msg,
            "worker_id": worker_id,
            "duration_seconds": 0.0,
            "project_id": task.project_id,
            "attempts": 1,
        }

    log.info(
        "task_started",
        task_id=task.task_id,
        task_type=task.task_type,
        project_id=task.project_id,
        worker_id=worker_id,
        node_id=worker_id,
    )

    # ── Build effective parameters ─────────────────────────────────────────────
    effective_params = {**task.parameters}
    if task.injected_secrets:
        effective_params["__secrets__"] = task.injected_secrets
    if redis is not None:
        effective_params["__redis__"] = redis

    # ── Execute with exponential backoff ───────────────────────────────────────
    last_error: str = ""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            output = await registry.execute(task.task_type, effective_params)
            finished_at = datetime.now(timezone.utc)
            duration = (finished_at - started_at).total_seconds()

            if attempt > 1:
                log.info(
                    "task_succeeded_after_retry",
                    task_id=task.task_id,
                    task_type=task.task_type,
                    project_id=task.project_id,
                    worker_id=worker_id,
                    attempt=attempt,
                    duration_s=round(duration, 3),
                )
            else:
                log.info(
                    "task_completed",
                    task_id=task.task_id,
                    task_type=task.task_type,
                    project_id=task.project_id,
                    worker_id=worker_id,
                    duration_s=round(duration, 3),
                )

            return {
                "output": output,
                "error": None,
                "worker_id": worker_id,
                "duration_seconds": duration,
                "project_id": task.project_id,
                "attempts": attempt,
            }

        except _NON_RETRYABLE as exc:
            # Configuration / logic error — do not retry
            log.error(
                "task_non_retryable_error",
                task_id=task.task_id,
                task_type=task.task_type,
                project_id=task.project_id,
                worker_id=worker_id,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return {
                "output": None,
                "error": str(exc),
                "worker_id": worker_id,
                "duration_seconds": (
                    datetime.now(timezone.utc) - started_at
                ).total_seconds(),
                "project_id": task.project_id,
                "attempts": attempt,
            }

        except Exception as exc:
            last_error = str(exc)
            remaining = MAX_RETRIES - attempt

            if remaining > 0:
                delay = _BACKOFF_DELAYS[min(attempt - 1, len(_BACKOFF_DELAYS) - 1)]
                log.warning(
                    "task_transient_error_retrying",
                    task_id=task.task_id,
                    task_type=task.task_type,
                    project_id=task.project_id,
                    worker_id=worker_id,
                    attempt=attempt,
                    max_retries=MAX_RETRIES,
                    retry_in_s=delay,
                    error=last_error,
                )
                await asyncio.sleep(delay)
            else:
                log.exception(
                    "task_permanently_failed",
                    task_id=task.task_id,
                    task_type=task.task_type,
                    project_id=task.project_id,
                    worker_id=worker_id,
                    attempts=attempt,
                    error=last_error,
                )

    # All retries exhausted
    return {
        "output": None,
        "error": f"Failed after {MAX_RETRIES} attempts: {last_error}",
        "worker_id": worker_id,
        "duration_seconds": (
            datetime.now(timezone.utc) - started_at
        ).total_seconds(),
        "project_id": task.project_id,
        "attempts": MAX_RETRIES,
    }
