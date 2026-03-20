"""
Worker Node entrypoint.

Usage
-----
    python scripts/start_worker.py

Or via the installed CLI entrypoint (after `pip install -e .`):
    nexus-worker

Deploy this script (along with the full `nexus/` package) to each Worker Node.
Set REDIS_URL and NODE_ID in the .env file on each machine.

What this script does
---------------------
1. Loads settings from .env.
2. Configures structured logging.
3. Starts the ARQ worker process which polls Redis for tasks and executes them
   by calling `execute_task` in nexus/worker/listener.py.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

# Windows / Python 3.10+ fix: the default ProactorEventLoop does not support
# all asyncio features used by ARQ.  Switch to SelectorEventLoop and ensure a
# loop exists in the main thread before anything else runs.
if sys.platform == "win32":
    # Required for Windows + Python 3.8+ compatibility with aiohttp/arq
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

try:
    asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

import structlog
from arq import run_worker

from nexus.shared.config import settings
from nexus.shared.logging_config import configure_logging
from nexus.shared.system_settings import read_system_settings
from nexus.worker.listener import WorkerSettings

log = structlog.get_logger(__name__)

# Force-load .env before reading Telegram credentials so that values are
# available regardless of the working directory from which this script runs.
_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"
if _ENV_FILE.exists():
    for _line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if not _line or _line.startswith("#") or "=" not in _line:
            continue
        _key, _, _val = _line.partition("=")
        _key = _key.strip()
        _val = _val.strip().split("#")[0].strip()
        if _key and _key not in os.environ:
            os.environ[_key] = _val


def main() -> None:
    system_runtime = read_system_settings()
    # Keep worker concurrency in a strict low-power envelope (2-3 jobs).
    bounded_jobs = max(2, min(int(system_runtime["max_workers"]), 3))
    WorkerSettings.max_jobs = bounded_jobs
    # Prediction loops read this env var to avoid CPU spikes.
    os.environ["NEXUS_PREDICTION_THROTTLE_DELAY"] = "1.0"

    # Force production-friendly logging to reduce console I/O overhead.
    configure_logging(level=str(system_runtime["log_level"]), node_id=settings.node_id)
    # A slightly higher poll delay lowers idle CPU usage on worker nodes.
    WorkerSettings.poll_delay = float(os.getenv("WORKER_POLL_DELAY", "1.0"))

    # WorkerSettings.redis_settings is built by listener._build_redis_settings()
    # which auto-detects Docker vs direct run.  Log the resolved host here so
    # any connection failures are immediately obvious in the startup output.
    rs = WorkerSettings.redis_settings
    resolved = f"redis://{rs.host}:{rs.port}/{rs.database}"
    log.info(
        "nexus_worker_starting",
        node_id=settings.node_id,
        redis_resolved=resolved,
        max_jobs=WorkerSettings.max_jobs,
        throttle_delay_s=1.0,
    )

    # ── Boot notification ─────────────────────────────────────────────────────
    # If the system was rebooted less than 5 minutes ago, send a Telegram
    # message so the operator knows the Worker came back online automatically.
    # This runs in a short-lived event loop before ARQ takes over.
    tg_token   = os.environ.get("TELEGRAM_BOT_TOKEN", "") or settings.telegram_bot_token
    tg_chat_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "") or settings.telegram_admin_chat_id

    async def _notify() -> None:
        from nexus.shared.boot_notifier import check_and_notify_boot  # noqa: PLC0415
        await check_and_notify_boot(
            bot_token=tg_token,
            admin_chat_id=tg_chat_id,
            node_id=settings.node_id,
        )

    asyncio.run(_notify())

    # `run_worker` is ARQ's blocking worker loop.  It handles:
    #   - Connecting to Redis
    #   - Polling the queue
    #   - Calling execute_task for each job
    #   - Graceful shutdown on SIGTERM / Ctrl-C
    run_worker(WorkerSettings)


if __name__ == "__main__":
    main()
