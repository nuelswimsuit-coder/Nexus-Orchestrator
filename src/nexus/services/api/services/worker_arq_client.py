"""
Enqueue ARQ ``execute_task`` jobs from the API process and wait for results.

Used when the Master (API + bot) offloads CPU-heavy work to laptop workers
while keeping a synchronous HTTP response. Falls back to local execution when
no workers are connected (callers check ``count_online_workers`` first).
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import structlog
from arq import create_pool
from arq.connections import RedisSettings
from arq.jobs import Job

from nexus.services.vault import Vault
from nexus.shared.config import settings
from nexus.shared.schemas import NodeRole, TaskPayload

log = structlog.get_logger(__name__)


async def count_online_workers(redis: Any) -> int:
    """Workers with a live heartbeat key (same semantics as ``/api/cluster/status``)."""
    from nexus.shared.heartbeat_scan import load_live_node_heartbeats

    try:
        beats = await load_live_node_heartbeats(redis)
    except Exception as exc:
        log.warning("worker_count_heartbeat_failed", error=str(exc))
        return 0
    return sum(1 for h in beats if h.role == NodeRole.WORKER)


async def enqueue_execute_task_and_wait(
    task: TaskPayload,
    *,
    timeout_s: float,
    poll_delay_s: float = 0.35,
) -> dict[str, Any] | None:
    """
    Inject secrets, enqueue onto ``nexus:tasks``, block until the job finishes.

    Returns the raw dict from ``execute_task`` (keys ``output``, ``error``, …),
    or ``None`` if enqueue/result retrieval failed.
    """
    vault = Vault()
    enriched = vault.inject(task)
    redis_url = (os.environ.get("REDIS_URL") or settings.redis_url or "").strip()
    if not redis_url:
        log.warning("worker_arq_no_redis_url")
        return None

    pool = None
    tid = enriched.task_id
    try:
        pool = await create_pool(
            RedisSettings.from_dsn(redis_url),
            default_queue_name="nexus:tasks",
        )
        job = await pool.enqueue_job(
            "execute_task",
            task_payload=enriched.model_dump_for_wire(),
            _job_id=tid,
            _queue_name="nexus:tasks",
        )
        if job is None:
            log.warning("worker_arq_enqueue_duplicate", task_id=tid)
            return None
        aj = Job(tid, redis=pool, _queue_name="nexus:tasks")
        return await aj.result(timeout=timeout_s, poll_delay=poll_delay_s)
    except Exception as exc:
        log.warning("worker_arq_job_failed", task_id=tid, error=str(exc))
        return None
    finally:
        if pool is not None:
            await pool.aclose()


async def enqueue_auto_scrape_fire_and_forget(
    *,
    sources: list[str],
    project_id: str,
    force: bool = True,
) -> str | None:
    """
    Queue ``telegram.auto_scrape`` without waiting (monitor / registration hooks).

    Returns task_id on success, or None if Redis/enqueue failed.
    """
    redis_url = (os.environ.get("REDIS_URL") or settings.redis_url or "").strip()
    if not redis_url or not sources:
        return None
    task = TaskPayload(
        task_id=str(uuid.uuid4()),
        task_type="telegram.auto_scrape",
        project_id=project_id,
        parameters={"force": force, "sources": sources},
        priority=2,
    )
    pool = None
    try:
        pool = await create_pool(
            RedisSettings.from_dsn(redis_url),
            default_queue_name="nexus:tasks",
        )
        job = await pool.enqueue_job(
            "execute_task",
            task_payload=task.model_dump_for_wire(),
            _job_id=task.task_id,
            _queue_name="nexus:tasks",
        )
        if job is None:
            return None
        return task.task_id
    except Exception as exc:
        log.warning("enqueue_auto_scrape_failed", error=str(exc))
        return None
    finally:
        if pool is not None:
            await pool.aclose()


def build_llm_task_payload(
    *,
    message: str,
    analysis_mode: str = "chat",
    context_messages: list[dict[str, str]] | None = None,
    project_id: str = "nexus-llm",
) -> TaskPayload:
    return TaskPayload(
        task_id=str(uuid.uuid4()),
        task_type="nexus.llm.gemini_terminal",
        project_id=project_id,
        parameters={
            "message": message,
            "analysis_mode": analysis_mode,
            "context_messages": context_messages or [],
        },
        priority=3,
    )


async def enqueue_execute_tasks_parallel(
    tasks: list[TaskPayload],
    *,
    timeout_s: float,
    poll_delay_s: float = 0.35,
) -> list[dict[str, Any] | None]:
    """
    Inject secrets, enqueue each job onto ``nexus:tasks``, wait for all in parallel.

    Returns one entry per input task (same order): raw ``execute_task`` dict or
    ``None`` if enqueue/result failed for that slot.
    """
    if not tasks:
        return []
    vault = Vault()
    redis_url = (os.environ.get("REDIS_URL") or settings.redis_url or "").strip()
    if not redis_url:
        log.warning("worker_arq_parallel_no_redis_url")
        return [None] * len(tasks)

    enriched = [vault.inject(t) for t in tasks]
    pool = None
    try:
        pool = await create_pool(
            RedisSettings.from_dsn(redis_url),
            default_queue_name="nexus:tasks",
        )
        jobs: list[Job | None] = []
        for t in enriched:
            tid = t.task_id
            job = await pool.enqueue_job(
                "execute_task",
                task_payload=t.model_dump_for_wire(),
                _job_id=tid,
                _queue_name="nexus:tasks",
            )
            jobs.append(job)
        out: list[dict[str, Any] | None] = []
        for t, job in zip(enriched, jobs, strict=True):
            if job is None:
                log.warning("worker_arq_parallel_enqueue_duplicate", task_id=t.task_id)
                out.append(None)
                continue
            aj = Job(t.task_id, redis=pool, _queue_name="nexus:tasks")
            try:
                raw = await aj.result(timeout=timeout_s, poll_delay=poll_delay_s)
                out.append(raw if isinstance(raw, dict) else None)
            except Exception as exc:
                log.warning(
                    "worker_arq_parallel_job_failed",
                    task_id=t.task_id,
                    error=str(exc),
                )
                out.append(None)
        return out
    except Exception as exc:
        log.warning("worker_arq_parallel_pool_failed", error=str(exc))
        return [None] * len(tasks)
    finally:
        if pool is not None:
            await pool.aclose()
