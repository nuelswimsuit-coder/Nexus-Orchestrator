"""
Unified launcher for lightweight Nexus core services.

Starts:
  - scripts/start_api.py
  - scripts/start_telegram_bot.py
  - scripts/start_worker.py

All services are launched via multiprocessing and can be stopped together
with Ctrl-C (SIGINT).

Task dispatch (CLI)
-------------------
When ``--task`` is set, enqueues a single ARQ job (same wire format as the
master Dispatcher) and sets Redis key ``global_mission`` for node monitors::

    python scripts/nexus_core.py --task telegram.auto_scrape --project telefix \\
        --priority 3 --params '{}'

``--dry-run`` checks worker heartbeats only (no enqueue / no Redis mission write).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import runpy
import socket
import signal
import sys
from multiprocessing import Process
from pathlib import Path
from time import sleep
from typing import Any

from nexus.utils.resources import GlobalResourceManager, load_node_config

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent  # repo root — used for optional .env bootstrap
API_SCRIPT = BASE_DIR / "start_api.py"
BOT_SCRIPT = BASE_DIR / "start_telegram_bot.py"
WORKER_SCRIPT = BASE_DIR / "start_worker.py"
MASTER_NODE_ID = "master-hybrid-node"

GLOBAL_MISSION_KEY = "global_mission"
HEARTBEAT_KEY_PREFIX = "nexus:heartbeat:"
ARQ_QUEUE_NAME = "nexus:tasks"

# Friendly aliases → registry task_type (worker handler is ``telegram.auto_scrape``).
TASK_TYPE_ALIASES: dict[str, str] = {
    "auto_scrape": "telegram.auto_scrape",
    "telegram.autoscrape": "telegram.auto_scrape",
}


def _resolve_task_type(raw: str) -> str:
    key = raw.strip()
    return TASK_TYPE_ALIASES.get(key, key)


def _bootstrap_env_from_dotenv() -> None:
    """Fill missing os.environ from repo ``.env`` (same pattern as start_worker)."""
    env_path = PROJECT_ROOT / ".env"
    if not env_path.is_file():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().split("#")[0].strip()
        if key and key not in os.environ:
            os.environ[key] = val


def _redis_dsn_for_dispatch(master_ip: str) -> str:
    env_url = (os.getenv("REDIS_URL") or "").strip()
    if env_url:
        return env_url
    host = (master_ip or "127.0.0.1").strip() or "127.0.0.1"
    port = os.getenv("REDIS_PORT", "6379")
    db = os.getenv("REDIS_DB", "0")
    return f"redis://{host}:{port}/{db}"


async def _count_online_workers(redis: Any) -> tuple[int, list[str]]:
    """Workers = heartbeat keys whose payload has role ``worker``."""
    from nexus.shared.schemas import NodeHeartbeat, NodeRole

    cursor = 0
    pattern = f"{HEARTBEAT_KEY_PREFIX}*".encode()
    ids: list[str] = []
    while True:
        cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=200)
        for key in keys:
            raw = await redis.get(key)
            if raw is None:
                continue
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", errors="replace")
            try:
                hb = NodeHeartbeat.model_validate_json(raw)
            except Exception:
                continue
            if hb.role == NodeRole.WORKER:
                ids.append(hb.node_id)
        if cursor == 0:
            break
    return len(ids), sorted(set(ids))


async def _cli_dispatch_async(args: argparse.Namespace) -> int:
    from arq import create_pool
    from arq.connections import RedisSettings

    from nexus.shared.constants import TASK_DEFAULT_TIMEOUT
    from nexus.shared.schemas import TaskPayload

    _bootstrap_env_from_dotenv()
    master_ip = (args.master_ip or "127.0.0.1").strip() or "127.0.0.1"
    dsn = _redis_dsn_for_dispatch(master_ip)
    task_type = _resolve_task_type(args.task or "")
    try:
        params_obj = json.loads(args.params or "{}")
    except json.JSONDecodeError as exc:
        print(f"[nexus_core] Invalid --params JSON: {exc}", file=sys.stderr)
        return 1
    if not isinstance(params_obj, dict):
        print("[nexus_core] --params must be a JSON object (dict).", file=sys.stderr)
        return 1

    project = (args.project or "default").strip() or "default"
    priority = int(args.priority)

    pool: Any = None
    try:
        pool = await create_pool(
            RedisSettings.from_dsn(dsn),
            default_queue_name=ARQ_QUEUE_NAME,
        )
        n_workers, worker_ids = await _count_online_workers(pool)
        print(f"[nexus_core] Redis: {dsn.split('@')[-1] if '@' in dsn else dsn}")
        print(f"[nexus_core] Workers online (heartbeat): {n_workers} {worker_ids}")

        if args.dry_run:
            print(
                f"[nexus_core] DRY RUN — would set {GLOBAL_MISSION_KEY}={project!r} "
                f"and enqueue task_type={task_type!r} priority={priority} "
                f"project_id={project!r}"
            )
            if n_workers == 0:
                print("[nexus_core] DRY RUN: no workers online — real run would still enqueue.", file=sys.stderr)
            return 0

        if args.require_workers and n_workers == 0:
            print(
                "[nexus_core] No workers online (--require-workers); aborting.",
                file=sys.stderr,
            )
            return 1
        if n_workers == 0:
            print(
                "[nexus_core] Warning: enqueueing with no worker heartbeats (job will wait in queue).",
                file=sys.stderr,
            )

        payload = TaskPayload(
            task_type=task_type,
            parameters=params_obj,
            priority=priority,
            project_id=project,
        )
        await pool.set(GLOBAL_MISSION_KEY, project)

        job = await pool.enqueue_job(
            "execute_task",
            task_payload=payload.model_dump_for_wire(),
            _job_id=payload.task_id,
            _queue_name=ARQ_QUEUE_NAME,
            _expires=TASK_DEFAULT_TIMEOUT,
        )
        jid = job.job_id if job else payload.task_id
        print(
            f"[nexus_core] Dispatched task_id={payload.task_id} job_id={jid} "
            f"type={task_type} project={project} {GLOBAL_MISSION_KEY}={project!r}"
        )
        return 0
    except OSError as exc:
        print(f"[nexus_core] Redis connection failed: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"[nexus_core] Dispatch failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if pool is not None:
            await pool.aclose()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Start Nexus master-hybrid core services, or dispatch a single ARQ task",
    )
    parser.add_argument(
        "--master-ip",
        default=os.getenv("MASTER_IP", "127.0.0.1"),
        help="Redis host/IP for local and remote workers (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--task",
        default=None,
        metavar="TYPE",
        help="Task type (e.g. telegram.auto_scrape). If set, enqueue and exit (no core services).",
    )
    parser.add_argument(
        "--project",
        default="default",
        help="project_id for TaskPayload and value stored in global_mission (default: default)",
    )
    parser.add_argument(
        "--priority",
        type=int,
        default=5,
        choices=range(1, 11),
        metavar="1-10",
        help="Task priority 1–10 (default: 5)",
    )
    parser.add_argument(
        "--params",
        default="{}",
        help='JSON object string for task parameters (default: "{}")',
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only check Redis + worker heartbeats; do not enqueue or set global_mission",
    )
    parser.add_argument(
        "--require-workers",
        action="store_true",
        help="Abort if no worker heartbeats (ignored with --dry-run)",
    )
    return parser.parse_args()


def _run_script(script_path: str, env_overrides: dict[str, str] | None = None) -> None:
    """Run a Python script in an isolated child process."""
    if env_overrides:
        for key, value in env_overrides.items():
            os.environ[str(key)] = str(value)
    runpy.run_path(script_path, run_name="__main__")


def _graceful_stop(processes: list[Process]) -> None:
    """Request clean stop for all child processes, then force-stop if needed."""
    for proc in processes:
        if proc.is_alive():
            try:
                # Give each child process a chance to handle SIGINT cleanly.
                if hasattr(signal, "SIGINT"):
                    signal_name = signal.SIGINT
                    # os.kill works across platforms for Python child processes.
                    import os

                    os.kill(proc.pid, signal_name)
            except Exception:
                pass

    deadline_s = 8
    for _ in range(deadline_s * 10):
        alive = [proc for proc in processes if proc.is_alive()]
        if not alive:
            return
        sleep(0.1)

    for proc in processes:
        if proc.is_alive():
            proc.terminate()
    for proc in processes:
        proc.join(timeout=3)


def _check_redis_socket(host: str = "127.0.0.1", port: int = 6379) -> bool:
    """Quick TCP probe to validate Redis reachability before launching services."""
    try:
        with socket.create_connection((host, port), timeout=1.5):
            return True
    except OSError:
        return False


def main() -> None:
    args = _parse_args()
    if args.task is not None:
        code = asyncio.run(_cli_dispatch_async(args))
        raise SystemExit(code)

    master_ip = (args.master_ip or "127.0.0.1").strip()
    missing = [p for p in (API_SCRIPT, BOT_SCRIPT, WORKER_SCRIPT) if not p.exists()]
    if missing:
        for path in missing:
            print(f"[nexus_core] Missing required script: {path}")
        sys.exit(1)
    if not _check_redis_socket(host=master_ip):
        print(
            f"\033[1m[!] Redis is unreachable at {master_ip}:6379. "
            "Run 'wsl service redis-server start' (or verify host).\033[0m"
        )

    # Master-hybrid identity is inherited by API/Bot and enforced explicitly
    # for the colocated worker process below.
    os.environ["NODE_ID"] = MASTER_NODE_ID
    os.environ["MASTER_IP"] = master_ip

    # Apply node-level resource controls from node_config.json.
    node_cfg = load_node_config()
    limiter = GlobalResourceManager(
        cpu_limit=node_cfg.cpu_limit,
        ram_limit=node_cfg.ram_limit,
        gpu_limit=node_cfg.gpu_limit,
    )
    limiter.start()

    processes = [
        Process(target=_run_script, args=(str(API_SCRIPT),), name="nexus-api"),
        Process(target=_run_script, args=(str(BOT_SCRIPT),), name="nexus-telegram-bot"),
        Process(
            target=_run_script,
            args=(
                str(WORKER_SCRIPT),
                {
                    "NODE_ID": MASTER_NODE_ID,
                    "MASTER_IP": master_ip,
                    "REDIS_URL": f"redis://{master_ip}:6379/0",
                    "REDIS_HOST": master_ip,
                },
            ),
            name="nexus-local-worker",
        ),
    ]

    for proc in processes:
        proc.start()
        print(f"[nexus_core] Started {proc.name} (pid={proc.pid})")

    stop_requested = False

    def _on_signal(signum: int, _frame: object) -> None:
        nonlocal stop_requested
        if not stop_requested:
            stop_requested = True
            print(f"[nexus_core] Signal {signum} received, shutting down services...")

    signal.signal(signal.SIGINT, _on_signal)
    try:
        signal.signal(signal.SIGTERM, _on_signal)
    except (AttributeError, OSError, ValueError):
        # SIGTERM handling may not be available depending on platform/runtime.
        pass

    try:
        while True:
            if stop_requested:
                break
            if any(not proc.is_alive() for proc in processes):
                print("[nexus_core] A child process exited. Stopping all services.")
                break
            sleep(0.5)
    finally:
        _graceful_stop(processes)
        limiter.stop()
        print("[nexus_core] Shutdown complete.")


if __name__ == "__main__":
    main()
