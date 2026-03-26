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

# Linux production: optional uvloop (Windows uses the default asyncio policy).
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

    # #region agent log
    import json as _j, time as _t
    _log_path = Path(__file__).resolve().parent.parent / "debug-d379fc.log"
    try:
        with open(_log_path, "a") as _f:
            _f.write(_j.dumps({"sessionId": "d379fc", "location": "start_worker.py:main_start", "message": "worker starting", "data": {"platform": sys.platform, "master_host": master_host, "redis_url": os.environ.get("REDIS_URL", ""), "node_id": os.environ.get("NODE_ID", "")}, "timestamp": int(_t.time() * 1000), "hypothesisId": "H1"}) + "\n")
    except Exception:
        pass
    # #endregion

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

    # #region agent log
    import json as _j, time as _t
    _log_path = Path(__file__).resolve().parent.parent / "debug-d379fc.log"
    try:
        with open(_log_path, "a") as _f:
            _f.write(_j.dumps({"sessionId": "d379fc", "location": "start_worker.py:arq_host", "message": "ARQ redis_settings resolved", "data": {"arq_host": rs.host, "arq_port": rs.port, "arq_db": rs.database, "resolved": resolved}, "timestamp": int(_t.time() * 1000), "hypothesisId": "H1"}) + "\n")
    except Exception:
        pass
    # #endregion

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

    asyncio.run(_notify())

    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("event loop is closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    run_worker(WorkerSettings)


if __name__ == "__main__":
    main()
