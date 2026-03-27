"""
Worker Node entrypoint.

Usage
-----
    python scripts/start_worker.py
    python scripts/start_worker.py --master-host 192.168.1.10
    python scripts/start_worker.py --master-ip 192.168.1.10   # alias

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

import argparse
import asyncio
import os
import sys
from pathlib import Path

# Windows: force SelectorEventLoop — ProactorEventLoop is unstable with
# long-lived Redis connections and causes WinError 121 / WinError 64.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Linux production: optional uvloop.
if sys.platform != "win32" and os.environ.get("ENVIRONMENT", "PRODUCTION").upper() == "PRODUCTION":
    try:
        import uvloop  # type: ignore[import-not-found]

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except Exception:
        pass

import psutil
import structlog
from arq import run_worker

from nexus.shared.config import settings
from nexus.shared.logging_config import configure_logging
from nexus.shared.redis_util import default_redis_host
from nexus.shared.system_settings import read_system_settings

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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start Nexus ARQ worker node")
    _default_host = (
        os.getenv("MASTER_IP")
        or os.getenv("REDIS_HOST")
        or default_redis_host()
    ).strip() or default_redis_host()
    parser.add_argument(
        "--master-host",
        "--master-ip",
        dest="master_host",
        default=_default_host,
        help=(
            "Master Redis host/IP (env: MASTER_IP or REDIS_HOST; "
            "default: 127.0.0.1 on Windows, 10.100.102.8 on Linux)"
        ),
    )
    # Use parse_known_args so that unknown flags inherited from nexus_core's
    # sys.argv (--worker, --turbo-boost, --skip-sync-check) are silently ignored
    # instead of causing argparse to exit with code 2.
    args, _ = parser.parse_known_args()
    return args


def _apply_master_redis(master_host: str) -> None:
    from nexus.shared.redis_util import coerce_redis_url_for_platform  # noqa: PLC0415
    host = (master_host or "127.0.0.1").strip() or "127.0.0.1"
    port = os.getenv("REDIS_PORT", "6379")
    db = os.getenv("REDIS_DB", "0")
    url = coerce_redis_url_for_platform(f"redis://{host}:{port}/{db}")
    os.environ["MASTER_IP"] = host
    os.environ["REDIS_HOST"] = host
    os.environ["REDIS_URL"] = url


def main() -> None:
    args = _parse_args()
    master_host = (args.master_host or "127.0.0.1").strip() or "127.0.0.1"
    _apply_master_redis(master_host)

    # WorkerSettings reads env at import time, so import it only after
    # --master-host / env overrides have been applied.
    from nexus.worker.listener import WorkerSettings  # noqa: PLC0415

    # CLI wins over any stale class-level redis_settings built from prior imports
    # or DSN edge cases: ARQ uses this object when the worker starts.
    # Derive the arq host from the already-coerced REDIS_URL so we always
    # connect via [::1] on Windows. arq expects a bare hostname (no brackets).
    from urllib.parse import urlparse as _urlparse  # noqa: PLC0415
    _coerced_url = os.environ.get("REDIS_URL", "")
    _parsed_host = (_urlparse(_coerced_url).hostname or master_host).strip("[]")
    WorkerSettings.redis_settings.host = _parsed_host

    system_runtime = read_system_settings()
    # Muscle mode (default): target ~90% CPU on worker laptops via high ARQ
    # concurrency — HFT / scraper throughput. Set NEXUS_WORKER_LOW_POWER=1
    # to fall back to the old 2–3 job cap.
    low_power = os.getenv("NEXUS_WORKER_LOW_POWER", "").lower() in {"1", "true", "yes", "on"}
    if low_power:
        bounded_jobs = max(2, min(int(system_runtime["max_workers"]), 3))
        throttle = os.getenv("NEXUS_PREDICTION_THROTTLE_DELAY", "1.0")
    else:
        cpu_target_pct = float(os.getenv("NEXUS_WORKER_CPU_UTIL_TARGET", "90"))
        n_cpu = int(psutil.cpu_count(logical=True) or 4)
        cap = int(os.getenv("NEXUS_WORKER_MAX_JOBS_CAP", "32"))
        from_cfg = max(2, int(system_runtime["max_workers"]))
        muscle = max(from_cfg, min(cap, max(2, int(n_cpu * (cpu_target_pct / 100.0)))))
        explicit = os.getenv("NEXUS_WORKER_MAX_JOBS", "").strip()
        bounded_jobs = max(2, int(explicit)) if explicit.isdigit() else muscle
        throttle = os.getenv("NEXUS_PREDICTION_THROTTLE_DELAY", "0.35")
    WorkerSettings.max_jobs = bounded_jobs
    os.environ["NEXUS_PREDICTION_THROTTLE_DELAY"] = throttle

    # Production workers keep logs minimal.
    configure_logging(level="ERROR", node_id=settings.node_id)
    # A slightly higher poll delay lowers idle CPU usage on worker nodes.
    WorkerSettings.poll_delay = float(os.getenv("WORKER_POLL_DELAY", "1.0"))

    # Log the resolved broker (WARNING so it is visible when log level is ERROR).
    rs = WorkerSettings.redis_settings
    resolved = f"redis://{rs.host}:{rs.port}/{rs.database}"
    log.warning(
        "nexus_worker_starting",
        node_id=settings.node_id,
        redis_host=rs.host,
        redis_resolved=resolved,
        max_jobs=WorkerSettings.max_jobs,
        throttle_delay_s=float(os.environ.get("NEXUS_PREDICTION_THROTTLE_DELAY", "1.0")),
        low_power_mode=low_power,
    )

    # ── Boot notification ─────────────────────────────────────────────────────
    tg_token   = os.environ.get("TELEGRAM_BOT_TOKEN", "") or settings.telegram_bot_token
    tg_chat_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "") or settings.telegram_admin_chat_id

    async def _notify() -> None:
        from nexus.shared.boot_notifier import check_and_notify_boot  # noqa: PLC0415
        await check_and_notify_boot(
            bot_token=tg_token,
            admin_chat_id=tg_chat_id,
            node_id=settings.node_id,
        )

    try:
        asyncio.run(asyncio.wait_for(_notify(), timeout=10))
    except (asyncio.TimeoutError, Exception):
        pass

    # WinError 121 (semaphore timeout) / WinError 64 (network name deleted) recovery.
    # On Windows, ProactorEventLoop (now replaced by SelectorEventLoop above) and
    # long-lived Redis sockets can still trigger these OS errors transiently.
    # Catch them, wait 5 s, rebuild the event loop, and restart the ARQ worker.
    _WIN_TRANSIENT_ERRORS = {121, 64}
    _MAX_RESTART_ATTEMPTS = 10

    for _attempt in range(1, _MAX_RESTART_ATTEMPTS + 1):
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError("event loop is closed")
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        try:
            run_worker(WorkerSettings)
            break  # clean exit — do not restart
        except OSError as exc:
            winerror = getattr(exc, "winerror", None)
            if winerror in _WIN_TRANSIENT_ERRORS:
                log.warning(
                    "nexus_worker_win_transient_error",
                    winerror=winerror,
                    attempt=_attempt,
                    max_attempts=_MAX_RESTART_ATTEMPTS,
                    action="reinitialising Redis pool in 5 s",
                )
                import time as _time  # noqa: PLC0415
                _time.sleep(5)
                # Force a fresh event loop for the next attempt.
                try:
                    loop = asyncio.get_event_loop()
                    if not loop.is_closed():
                        loop.close()
                except Exception:
                    pass
                asyncio.set_event_loop(asyncio.new_event_loop())
                if _attempt >= _MAX_RESTART_ATTEMPTS:
                    log.error(
                        "nexus_worker_restart_limit_reached",
                        max_attempts=_MAX_RESTART_ATTEMPTS,
                    )
                    raise
            else:
                raise


if __name__ == "__main__":
    main()
