"""
Worker Listener — the execution engine running on each Worker Node.

How it works
------------
ARQ uses a WorkerSettings class (not a running server) to configure the
worker process.  When `arq nexus.agents.listener.WorkerSettings` is invoked
(or `scripts/start_worker.py` is run), ARQ:

1. Connects to Redis using `redis_settings`.
2. Polls the `nexus:tasks` queue for jobs.
3. Calls `execute_task(**job_kwargs)` for each job it picks up.
4. Stores the return value (or exception) back in Redis.

Turbo Mode (Architect) fans out multiple ``nexus.llm.gemini_terminal`` jobs with
``analysis_mode=turbo_shard`` in parallel; the Telegram bot gathers ARQ results
and runs a final aggregation prompt on the API — workers do not coordinate
with each other beyond sharing the queue.

Telethon **session data** for vault-backed tasks is not read from worker disk by
default: :func:`nexus.agents.session_transfer.attach_vault_sessions_to_task`
POSTs to ``{NEXUS_MASTER_HUB_URL}/api/sessions/vault/lease-batch`` on the Master
(``NEXUS_MASTER_HUB_URL`` defaults to ``http://10.100.102.8:8001`` via
:func:`nexus.agents.session_transfer.get_session_vault_api_base`).

The `execute_task` function may lease Telethon StringSessions from the master API
(via :mod:`nexus.agents.session_transfer`) before delegating to
`nexus.agents.executor.runner.run_task`, which handles validation, capability
checks, secrets merging, and handler dispatch.

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

import ctypes
import os

# Before any ``nexus.*`` import: pydantic-settings reads env when ``config`` loads.
# Workers lease credentials from the Master Command Hub at ``/api/sessions/vault/*``
# (base ``http://10.100.102.8:8001``), not from local ``.session`` files.
os.environ.setdefault("NEXUS_MASTER_HUB_URL", "http://10.100.102.8:8001")

import asyncio
import base64
import socket
from datetime import date, datetime, timezone
from typing import Any

import structlog
from arq.connections import RedisSettings

import nexus.agents.tasks.auto_scrape  # noqa: F401 — registers telegram.auto_scrape
import nexus.agents.tasks.content_factory  # noqa: F401 — registers telegram.content_factory
import nexus.agents.tasks.group_warmer  # noqa: F401 — registers swarm.group_warmer
import nexus.agents.tasks.llm_gemini  # noqa: F401 — registers nexus.llm.gemini_terminal
import nexus.agents.tasks.incubator_spawn  # noqa: F401 — registers nexus.incubator.*
import nexus.agents.tasks.account_mapper  # noqa: F401 — registers account_mapper.map
import nexus.agents.tasks.moltbot  # noqa: F401 — registers bot.moltbot
import nexus.agents.tasks.openclaw  # noqa: F401 — registers scraper.openclaw/openclaw.browser_scrape
import nexus.agents.tasks.polymarket_bot  # noqa: F401 — trading.polymarket_bot_session
import nexus.agents.tasks.prediction  # noqa: F401 — registers prediction.cross_exchange
import nexus.agents.tasks.retention_monitor  # noqa: F401 — retention.guardian.monitor
import nexus.agents.tasks.staged_session_warmup  # noqa: F401 — registers telegram.run_warmup
import nexus.agents.tasks.super_scraper  # noqa: F401 — registers telegram.super_scrape
import nexus.agents.tasks.telegram_adder  # noqa: F401 — registers telegram.auto_add
from nexus.shared.config import settings
from nexus.agents.executor.runner import WORKER_CAPABILITIES, run_task
from nexus.agents.session_transfer import (
    attach_vault_sessions_to_task,
    get_session_vault_api_base,
    release_vault_leases,
)
from nexus.agents.task_registry import registry  # noqa: F401 — registers built-ins
from nexus.agents.tasks.poly5m_velocity import (
    attach_velocity_feed_to_worker_ctx,
    detach_velocity_feed_from_worker_ctx,
)
from nexus.agents.task_activity import (
    clear_worker_activity_if_matches,
    describe_task,
    set_worker_activity,
)

log = structlog.get_logger(__name__)

WORKER_ID = os.getenv("NODE_ID", f"worker-{socket.gethostname()}")

# Master sets this key when ``position_manager`` runs with ``--turbo`` (see ``POSITION_TURBO_ACTIVE_KEY``).
_POSITION_TURBO_REDIS_KEY = "nexus:live_trading:turbo_active"


def _boost_worker_thread_priority_for_turbo() -> None:
    """Raise ARQ main thread priority when turbo is active (Windows)."""
    if os.name != "nt":
        return
    try:
        k32 = ctypes.windll.kernel32
        THREAD_PRIORITY_ABOVE_NORMAL = 1
        k32.SetThreadPriority(k32.GetCurrentThread(), THREAD_PRIORITY_ABOVE_NORMAL)
    except Exception:
        pass


async def _turbo_scheduling_active(redis: Any) -> bool:
    if os.getenv("NEXUS_POSITION_TURBO", "").strip().lower() in ("1", "true", "yes", "on"):
        return True
    if redis is None:
        return False
    try:
        raw = await redis.get(_POSITION_TURBO_REDIS_KEY)
    except Exception:
        return False
    return raw is not None and str(raw).strip().lower() in ("1", "true", "yes", "on")


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
        session_vault_hub=get_session_vault_api_base(),
        nexus_master_hub_url=(settings.nexus_master_hub_url or "").strip() or None,
    )

    # Publish initial heartbeat so the master's cluster status shows this
    # worker immediately on startup rather than waiting for the first interval.
    await _publish_heartbeat(ctx)

    # Subscribe to the system control channel so we receive TERMINATE/RESUME
    # signals from the panic endpoint immediately (without waiting for the
    # next execute_task call to discover the Redis flag).
    ctx["_panic_task"] = asyncio.create_task(
        _panic_subscriber(ctx),
        name="worker_panic_subscriber",
    )

    attach_velocity_feed_to_worker_ctx(ctx)


async def shutdown(ctx: dict[str, Any]) -> None:
    """Called once by ARQ when the worker process shuts down cleanly."""
    await detach_velocity_feed_from_worker_ctx(ctx)
    if task := ctx.get("_panic_task"):
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

    # ── Master session vault: lease StringSessions before task execution ───────
    params = dict(task_payload.get("parameters") or {})
    vault_release_stems: list[str] | None = None
    if redis is not None:
        vault = await attach_vault_sessions_to_task(
            str(task_payload.get("task_type", "")),
            params,
            worker_id=worker_id,
            task_id=str(task_payload.get("task_id", "unknown")),
            redis=redis,
        )
        if vault.block_error:
            log.error(
                "session_vault_attach_blocked",
                worker_id=worker_id,
                task_type=task_payload.get("task_type"),
                error=vault.block_error,
            )
            return _sanitize_result(
                {
                    "output": None,
                    "error": vault.block_error,
                    "worker_id": worker_id,
                    "duration_seconds": 0.0,
                    "project_id": str(task_payload.get("project_id", "unknown")),
                    "attempts": 1,
                }
            )
        params.update(vault.param_patch)
        vault_release_stems = vault.release_stems

    effective_payload = {**task_payload, "parameters": params}
    run_tid = str(effective_payload.get("task_id", "unknown"))

    # ── Per-task CPU priority boost for high-throughput tasks ─────────────────
    # Scraper and CLOB-bot tasks are I/O + CPU intensive; elevate their thread
    # priority to ensure they reach the 40 %+ CPU utilisation target.
    _HIGH_PRIO_TASK_PREFIXES = (
        "telegram.super_scrape",
        "telegram.auto_scrape",
        "scraper.",
        "openclaw.",
        "trading.polymarket",
        "prediction.",
        "poly5m.",
    )
    _task_type_str = str(effective_payload.get("task_type", ""))
    if any(_task_type_str.startswith(pfx) for pfx in _HIGH_PRIO_TASK_PREFIXES):
        try:
            import psutil as _psutil  # type: ignore[import]
            _proc = _psutil.Process()
            if os.name == "nt":
                _proc.nice(_psutil.ABOVE_NORMAL_PRIORITY_CLASS)
            else:
                try:
                    _proc.nice(-10)
                except (PermissionError, _psutil.AccessDenied):
                    pass
        except Exception:
            pass

    activity_armed = False
    if redis is not None:
        try:
            await set_worker_activity(
                redis,
                worker_id=worker_id,
                task_id=run_tid,
                label=describe_task(effective_payload),
            )
            activity_armed = True
        except Exception:
            pass

    try:
        raw_result = await run_task(
            task_payload=effective_payload,
            worker_id=worker_id,
            redis=redis,
        )
    finally:
        if activity_armed and redis is not None:
            try:
                await clear_worker_activity_if_matches(
                    redis, worker_id=worker_id, task_id=run_tid
                )
            except Exception:
                pass
        if vault_release_stems is not None:
            await release_vault_leases(vault_release_stems, worker_id)

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

    redis_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")
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
                elif isinstance(data, str) and data.startswith("RESTART_WORKER:"):
                    target = data.split(":", 1)[1].strip() if ":" in data else "*"
                    wid = str(ctx.get("worker_id") or WORKER_ID)
                    if target in ("*", "all") or target == wid:
                        log.warning(
                            "worker_restart_signal_received",
                            target=target,
                            worker_id=wid,
                        )

                        async def _exit_for_supervisor() -> None:
                            await asyncio.sleep(1.5)
                            import os

                            os._exit(0)

                        asyncio.create_task(
                            _exit_for_supervisor(),
                            name="worker_self_restart",
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
    import os as _os

    from nexus.shared.schemas import NodeHeartbeat, NodeRole
    from nexus.agents.hardware import get_hardware_info
    from nexus.worker.hardware import get_gpu_memory_used_percent

    redis = ctx.get("redis")
    if redis is None:
        return

    hw = get_hardware_info()
    mem = __import__("psutil").virtual_memory()
    ram_used_mb = round(mem.used / (1024 * 1024), 1)
    cpu_percent = __import__("psutil").cpu_percent(interval=None)

    from nexus.shared.system_stats import get_cpu_temp_celsius  # noqa: PLC0415
    raw_temp = get_cpu_temp_celsius()
    cpu_temp_c = raw_temp if raw_temp is not None else -1.0

    display_name = _os.getenv("NODE_DISPLAY_NAME", "")
    gpu_mem_pct = get_gpu_memory_used_percent()

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
        motherboard=hw.get("motherboard", "N/A"),
        cpu_temp_c=cpu_temp_c,
        display_name=display_name,
    )
    key = f"nexus:heartbeat:{WORKER_ID}"
    payload = heartbeat.model_dump_json()
    await redis.set(key, payload, ex=120)
    try:
        # String mirror for GET nexus:heartbeat (position manager / ops dashboards).
        await redis.set("nexus:heartbeat", payload, ex=120)
    except Exception:
        pass
    try:
        await redis.publish("nexus:heartbeat", payload)
    except Exception:
        pass
    if await _turbo_scheduling_active(redis):
        _boost_worker_thread_priority_for_turbo()
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
    poll_delay: float = float(os.getenv("WORKER_POLL_DELAY", "5.0"))
