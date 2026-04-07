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

The `execute_task` function delegates to `nexus.worker.executor.runner.run_task`
which handles validation, capability checks, secrets merging, and handler dispatch.

Result serialization
--------------------
ARQ serialises job results with msgpack.  msgpack cannot handle:
  - datetime objects            → converted to ISO-format strings
  - set objects                 → converted to sorted lists
  - Pydantic models             → converted via model_dump()
  - bytes                       → converted to base64 strings
  - any other non-primitive     → converted to str()

`_sanitize_result()` recursively walks the result dict and converts every
non-serializable value before ARQ attempts to store it.  This prevents
`arq.jobs.SerializationError` from crashing the worker.

Resilience / Failover
---------------------
- `max_tries = 3`       — ARQ re-queues a failed job up to 3 times.
- `job_timeout`         — Configurable via TASK_DEFAULT_TIMEOUT env var.
- `keep_result = 86400` — Results retained for 24 h.
"""

from __future__ import annotations

import asyncio
import base64
import os
import socket
from datetime import date, datetime, timezone
from typing import Any

import structlog
from arq.connections import RedisSettings

import nexus.worker.tasks.account_mapper  # noqa: F401 — registers account_mapper.map
import nexus.worker.tasks.auditor  # noqa: F401 — registers seo.watchdog.audit
import nexus.worker.tasks.auto_scrape  # noqa: F401 — registers telegram.auto_scrape
import nexus.worker.tasks.content_factory  # noqa: F401 — registers telegram.content_factory
import nexus.worker.tasks.group_warmer  # noqa: F401 — registers swarm.group_warmer
import nexus.worker.tasks.health_check  # noqa: F401 — registers management.group_health_scan
import nexus.worker.tasks.incubator_spawn  # noqa: F401 — registers nexus.incubator.*
import nexus.worker.tasks.israeli_media_ingest  # noqa: F401 — swarm.israeli_media.ingest
import nexus.worker.tasks.lurkers  # noqa: F401 — registers swarm.lurkers.tick
import nexus.worker.tasks.moltbot  # noqa: F401 — registers bot.moltbot
import nexus.worker.tasks.news_digest_refresh  # noqa: F401 — swarm.news_digest.refresh
import nexus.worker.tasks.openclaw  # noqa: F401 — registers scraper.openclaw/openclaw.browser_scrape
import nexus.worker.tasks.poll_generator  # noqa: F401 — swarm.poll_generator / swarm.poll.cast_vote
import nexus.worker.tasks.polymarket_bot  # noqa: F401 — trading.polymarket_bot_tick / trading.polymarket_bot_session / polymarket_bot
import nexus.worker.tasks.prediction  # noqa: F401 — registers prediction.cross_exchange
import nexus.worker.tasks.reactions  # noqa: F401 — registers swarm.passive_reaction
import nexus.worker.tasks.retention_monitor  # noqa: F401 — retention.guardian.monitor
import nexus.worker.tasks.scale  # noqa: F401 — registers nexus.scale_worker
import nexus.worker.tasks.sentinel_seo  # noqa: F401 — registers management.sentinel_seo
import nexus.worker.tasks.seo_group_factory  # noqa: F401 — seo.group_factory.* + seo_group_factory
import nexus.worker.tasks.spambot_weekly  # noqa: F401 — registers management.vault_spambot_weekly
import nexus.worker.tasks.staged_session_warmup  # noqa: F401 — registers telegram.run_warmup
import nexus.worker.tasks.super_scraper  # noqa: F401 — registers telegram.super_scrape
import nexus.worker.tasks.swarm  # noqa: F401 — registers swarm.community_factory.*
import nexus.worker.tasks.swarm_onboarding  # noqa: F401 — swarm.onboarding mass join
import nexus.worker.tasks.telegram_adder  # noqa: F401 — registers telegram.auto_add
from nexus.worker.executor.runner import WORKER_CAPABILITIES, run_task
from nexus.worker.task_registry import registry  # noqa: F401 — registers built-ins
from nexus.worker.tasks.poly5m_velocity import (
    attach_velocity_feed_to_worker_ctx,
    detach_velocity_feed_from_worker_ctx,
)
from nexus.worker.tasks.swarm import run_swarm_news_digest_subscriber

log = structlog.get_logger(__name__)

WORKER_ID = os.getenv("NODE_ID", f"worker-{socket.gethostname()}")


# ── Result sanitization ────────────────────────────────────────────────────────

def _sanitize_value(value: Any) -> Any:
    """
    Recursively convert a value to a msgpack-serializable primitive.

    ARQ uses msgpack to store job results in Redis.  msgpack only supports:
      None, bool, int, float, str, bytes, list, dict

    Everything else must be converted before returning from execute_task.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    if isinstance(value, bytes):
        # Encode bytes as base64 string so they survive the round-trip
        return base64.b64encode(value).decode("ascii")

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, set):
        return sorted(_sanitize_value(v) for v in value)

    if isinstance(value, (list, tuple)):
        return [_sanitize_value(v) for v in value]

    if isinstance(value, dict):
        return {str(k): _sanitize_value(v) for k, v in value.items()}

    # Pydantic models
    if hasattr(value, "model_dump"):
        return _sanitize_value(value.model_dump())

    # dataclasses
    if hasattr(value, "__dataclass_fields__"):
        import dataclasses
        return _sanitize_value(dataclasses.asdict(value))

    # Enum
    if hasattr(value, "value"):
        return _sanitize_value(value.value)

    # Last resort: stringify
    return str(value)


def _sanitize_result(result: dict[str, Any]) -> dict[str, Any]:
    """
    Walk the entire result dict and convert every non-serializable value.

    Called on the return value of run_task() before ARQ stores it in Redis.
    Guarantees no SerializationError regardless of what task handlers return.
    """
    return {str(k): _sanitize_value(v) for k, v in result.items()}


# ── ARQ lifecycle hooks ────────────────────────────────────────────────────────

_PANIC_KEY     = "SYSTEM_STATE:PANIC"
_PANIC_CHANNEL = "nexus:system:control"


async def startup(ctx: dict[str, Any]) -> None:
    """
    Called once by ARQ when the worker process starts.

    Initialise expensive shared resources here (DB connections, ML models,
    HTTP client sessions) — they will be reused across many task executions.
    """
    ctx["worker_id"] = WORKER_ID
    ctx["started_at"] = datetime.now(timezone.utc)
    ctx["panic"] = False
    log.info(
        "worker_started",
        worker_id=WORKER_ID,
        capabilities=list(WORKER_CAPABILITIES),
        registered_tasks=registry.registered_types,
    )

    # Publish initial heartbeat so the master's cluster status shows this
    # worker immediately on startup rather than waiting for the first interval.
    await _publish_heartbeat(ctx)

    # Periodic heartbeat loop — refreshes the Redis key every 30s so the
    # master always sees this worker as online (TTL is 120s).
    async def _heartbeat_loop() -> None:
        while True:
            try:
                await asyncio.sleep(30)
                await _publish_heartbeat(ctx)
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    ctx["_heartbeat_task"] = asyncio.create_task(
        _heartbeat_loop(),
        name="worker_heartbeat_loop",
    )

    # OpenClaw ↔ Nexus sync: periodic Redis "Test Heartbeat" (default 30 min).
    if os.getenv("NEXUS_OPENCLAW_SYNC_HEARTBEAT", "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }:
        _redis_oc = ctx.get("redis")
        if _redis_oc is not None:
            from nexus.shared.health_monitor import run_openclaw_test_heartbeat_loop  # noqa: PLC0415

            ctx["_openclaw_sync_hb_task"] = asyncio.create_task(
                run_openclaw_test_heartbeat_loop(_redis_oc),
                name="openclaw_sync_test_heartbeat",
            )

    # Subscribe to the system control channel so we receive TERMINATE/RESUME
    # signals from the panic endpoint immediately (without waiting for the
    # next execute_task call to discover the Redis flag).
    ctx["_panic_task"] = asyncio.create_task(
        _panic_subscriber(ctx),
        name="worker_panic_subscriber",
    )

    ctx["_swarm_news_digest_task"] = asyncio.create_task(
        run_swarm_news_digest_subscriber(ctx),
        name="worker_swarm_news_digest_subscriber",
    )

    attach_velocity_feed_to_worker_ctx(ctx)


async def shutdown(ctx: dict[str, Any]) -> None:
    """Called once by ARQ when the worker process shuts down cleanly."""
    await detach_velocity_feed_from_worker_ctx(ctx)
    for task_key in (
        "_panic_task",
        "_heartbeat_task",
        "_swarm_news_digest_task",
        "_openclaw_sync_hb_task",
    ):
        if task := ctx.get(task_key):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    log.info("worker_shutdown", worker_id=ctx.get("worker_id", WORKER_ID))


async def execute_task(
    ctx: dict[str, Any], task_payload: dict[str, Any], **_: Any
) -> dict[str, Any]:
    """
    Universal ARQ entry point — called for every job on this worker.

    Delegates to runner.run_task(), then sanitizes the result dict so
    ARQ can serialize it with msgpack without raising SerializationError.

    The `**_` absorbs any extra kwargs ARQ might inject.
    """
    worker_id: str = ctx.get("worker_id", WORKER_ID)
    redis = ctx.get("redis")

    # ── System Panic guard ─────────────────────────────────────────────────────
    # Check both the in-memory flag (set by pub/sub subscriber) and the Redis
    # key directly so tasks are blocked even if the subscriber missed a beat.
    is_panic = ctx.get("panic", False)
    if not is_panic and redis is not None:
        try:
            is_panic = (await redis.get(_PANIC_KEY)) == "true"
        except Exception:
            pass

    if is_panic:
        log.critical(
            "task_blocked_system_panic",
            task_type=task_payload.get("task_type"),
            worker_id=worker_id,
        )
        return {
            "output":           None,
            "error":            "SYSTEM_PANIC: Task blocked by emergency kill-switch",
            "worker_id":        worker_id,
            "duration_seconds": 0.0,
            "project_id":       str(task_payload.get("project_id", "unknown")),
            "attempts":         1,
        }

    raw_result = await run_task(
        task_payload=task_payload,
        worker_id=worker_id,
        redis=redis,
    )

    # ── Sanitize before ARQ serializes ────────────────────────────────────────
    # This is the critical step that prevents SerializationError.
    # run_task() may return datetime objects, sets, Pydantic models, or any
    # value returned by a task handler.  _sanitize_result() converts them all
    # to msgpack-safe primitives (str, int, float, list, dict, None, bool).
    try:
        return _sanitize_result(raw_result)
    except Exception as exc:
        log.error(
            "result_sanitization_failed",
            worker_id=worker_id,
            error=str(exc),
            result_keys=list(raw_result.keys()) if isinstance(raw_result, dict) else "?",
        )
        # Return a minimal safe result so ARQ doesn't crash
        return {
            "output": None,
            "error": f"Result sanitization failed: {exc}",
            "worker_id": worker_id,
            "duration_seconds": 0.0,
            "project_id": str(task_payload.get("project_id", "unknown")),
            "attempts": 1,
        }


async def _panic_subscriber(ctx: dict[str, Any]) -> None:
    """
    Background coroutine that subscribes to ``nexus:system:control`` and
    updates ``ctx['panic']`` when TERMINATE / RESUME signals arrive.

    Uses a dedicated Redis connection (not the shared ARQ one) because
    Pub/Sub requires a stateful subscribe mode that cannot be mixed with
    normal commands on the same connection.
    """
    from redis.asyncio import from_url as _redis_from_url

    from nexus.shared import redis_util

    raw_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")
    redis_url = redis_util.coerce_redis_url_for_platform(raw_url)
    retry_s = 1.0
    attempt = 0
    while True:
        pubsub_client = None
        try:
            attempt += 1
            pubsub_client = _redis_from_url(redis_url, decode_responses=True)
            pubsub = pubsub_client.pubsub()
            await pubsub.subscribe(_PANIC_CHANNEL)
            if attempt > 1:
                log.info("worker_panic_subscriber_reconnected", attempts=attempt)
            else:
                log.info("worker_panic_subscriber_started", channel=_PANIC_CHANNEL)
            retry_s = 1.0

            async for message in pubsub.listen():
                if message.get("type") != "message":
                    continue
                data = message.get("data", "")
                if data in ("TERMINATE", "FORCE_STOP"):
                    ctx["panic"] = True
                    log.critical(
                        "worker_terminate_signal_received",
                        worker_id=ctx.get("worker_id"),
                        signal=data,
                    )
                elif data == "RESUME":
                    ctx["panic"] = False
                    log.info("worker_resume_signal_received", worker_id=ctx.get("worker_id"))
        except asyncio.CancelledError:
            break
        except Exception as exc:
            if attempt <= 2 or attempt % 5 == 0:
                log.warning(
                    "worker_panic_subscriber_retry",
                    attempt=attempt,
                    retry_in_s=round(retry_s, 2),
                    error=str(exc),
                )
            await asyncio.sleep(retry_s)
            retry_s = min(retry_s * 1.7, 10.0)
        finally:
            if pubsub_client is not None:
                try:
                    await pubsub_client.aclose()
                except Exception:
                    pass


async def _publish_heartbeat(ctx: dict[str, Any]) -> None:
    """
    Write a NodeHeartbeat key to Redis so the API cluster endpoint
    shows this worker as online immediately after startup.
    """
    from nexus.shared.schemas import NodeHeartbeat, NodeRole
    from nexus.worker.hardware import (
        get_cpu_temperature,
        get_gpu_memory_used_percent,
        get_hardware_info,
    )

    redis = ctx.get("redis")
    if redis is None:
        return

    import psutil

    hw = get_hardware_info()
    mem = psutil.virtual_memory()
    ram_used_mb = round(mem.used / (1024 * 1024), 1)
    cpu_percent = psutil.cpu_percent(interval=None)
    cpu_temp = get_cpu_temperature()
    gpu_mem_pct = get_gpu_memory_used_percent()

    display_name = os.getenv("NODE_DISPLAY_NAME", "")

    heartbeat = NodeHeartbeat(
        node_id=WORKER_ID,
        role=NodeRole.WORKER,
        cpu_percent=cpu_percent,
        ram_used_mb=ram_used_mb,
        active_jobs=0,
        capabilities=list(WORKER_CAPABILITIES),
        local_ip=hw["local_ip"],
        cpu_model=hw["cpu_model"],
        gpu_model=hw["gpu_model"],
        gpu_mem_used_pct=gpu_mem_pct,
        ram_total_mb=hw["ram_total_mb"],
        active_tasks_count=0,
        os_info=hw["os_info"],
        motherboard=hw["motherboard"],
        cpu_temp_c=cpu_temp,
        display_name=display_name,
    )
    key = f"nexus:heartbeat:{WORKER_ID}"
    await redis.set(key, heartbeat.model_dump_json(), ex=120)
    log.debug("worker_heartbeat_published", node_id=WORKER_ID, ip=hw["local_ip"])


# ── Redis settings factory ─────────────────────────────────────────────────────

def _build_redis_settings() -> RedisSettings:
    """
    Smart Redis connection resolver.

    Priority order:
      1. ``REDIS_URL``  — full DSN string, explicit override (e.g. from .env)
      2. ``REDIS_HOST`` — explicit hostname, port defaults to REDIS_PORT / 6379
      3. Auto-detect environment:
           - Inside Docker  → ``host.docker.internal``  (Windows / Mac host loopback)
           - Direct Python  → ``127.0.0.1``

    Detection heuristic: Linux Docker containers always have ``/.dockerenv``.
    The ``DOCKER_CONTAINER=1`` env var can be set to force Docker mode on any OS.
    """
    if redis_url := os.getenv("REDIS_URL"):
        log.debug("redis_url_from_env", url=redis_url)
        return RedisSettings.from_dsn(redis_url)

    redis_host = os.getenv("REDIS_HOST")
    if not redis_host:
        in_docker = os.path.exists("/.dockerenv") or bool(os.getenv("DOCKER_CONTAINER"))
        redis_host = "host.docker.internal" if in_docker else "127.0.0.1"
        log.debug("redis_host_autodetected", host=redis_host, in_docker=in_docker)
    else:
        log.debug("redis_host_from_env", host=redis_host)

    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_db   = int(os.getenv("REDIS_DB", "0"))
    return RedisSettings(host=redis_host, port=redis_port, database=redis_db)


# ── ARQ WorkerSettings ─────────────────────────────────────────────────────────

class WorkerSettings:
    functions = [execute_task]
    on_startup = startup
    on_shutdown = shutdown

    redis_settings = _build_redis_settings()

    queue_name = "nexus:tasks"

    max_jobs: int = int(os.getenv("WORKER_MAX_JOBS", "4"))
    job_timeout: int = int(os.getenv("TASK_DEFAULT_TIMEOUT", "300"))
    max_tries: int = 3
    keep_result: int = 86400  # 24 hours
