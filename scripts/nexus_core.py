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

PowerShell: do **not** use backslash-escaped JSON; use single quotes around the whole JSON::

    python scripts/nexus_core.py --task telegram.group_message_purge --params '{"targets":["mygroup"]}'

Or use ``--params-file path.json`` (UTF-8) to avoid quoting issues entirely.

Long tasks (e.g. ``telegram.owner_groups_lockdown``): the CLI only enqueues by default.
Add ``--wait-result`` to block until the worker finishes and print ``audit_csv_path`` / summary.
Very large vaults (thousands of sessions) can run for hours — use ``max_sessions`` in the params JSON to test; ``--wait-result`` prints progress on stderr (~10s, then every 30s).

If Redis times out on ``::1``, set ``REDIS_URL=redis://127.0.0.1:6379/0`` in ``.env`` and ensure Redis is running.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import runpy
import signal
import socket
import sys
from urllib.parse import urlparse, urlunparse
from multiprocessing import Process
from pathlib import Path
from time import sleep
from typing import Any

# Windows: force SelectorEventLoop — ProactorEventLoop is unstable with
# long-lived Redis connections and causes WinError 121 / WinError 64.
if sys.platform == "win32":
    from asyncio.windows_events import WindowsSelectorEventLoopPolicy

    asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())

from nexus.utils.resources import GlobalResourceManager, load_node_config

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent  # repo root — used for optional .env bootstrap
API_SCRIPT = BASE_DIR / "start_api.py"
BOT_SCRIPT = BASE_DIR / "start_telegram_bot.py"
WORKER_SCRIPT = BASE_DIR / "start_worker.py"
MASTER_NODE_ID = "master-hybrid-node"


def _coerce_redis_url(url: str) -> str:
    """Rewrite 127.0.0.1 -> [::1] on Windows to avoid WSL2/Hyper-V port-proxy interception."""
    try:
        from nexus.shared.redis_util import coerce_redis_url_for_platform
        return coerce_redis_url_for_platform(url)
    except Exception:
        return url


GLOBAL_MISSION_KEY = "global_mission"
HEARTBEAT_KEY_PREFIX = "nexus:heartbeat:"
ARQ_QUEUE_NAME = "nexus:tasks"

# Friendly aliases -> registry task_type (worker handler is ``telegram.auto_scrape``).
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


def _ensure_windows_redis_ipv4_loopback() -> None:
    """Avoid ::1 / localhost → IPv6 timeouts on Windows when Redis listens on IPv4 only."""
    if sys.platform != "win32":
        return
    raw = (os.getenv("REDIS_URL") or "").strip()
    if not raw:
        os.environ["REDIS_URL"] = "redis://127.0.0.1:6379/0"
        return
    try:
        u = urlparse(raw)
        h = (u.hostname or "").lower().strip("[]")
        if h not in ("localhost", "::1"):
            return
        port = u.port or 6379
        auth = ""
        if u.username is not None:
            auth = u.username
            if u.password is not None:
                auth = f"{auth}:{u.password}"
            auth = f"{auth}@"
        new_netloc = f"{auth}127.0.0.1:{port}"
        fixed = urlunparse((u.scheme, new_netloc, u.path or "", u.params, u.query, u.fragment))
        os.environ["REDIS_URL"] = fixed
    except Exception:
        os.environ["REDIS_URL"] = "redis://127.0.0.1:6379/0"


def _normalize_cli_json_text(raw: str) -> str:
    s = raw.strip().lstrip("\ufeff")
    for a, b in (
        ("\u201c", '"'),
        ("\u201d", '"'),
        ("\u2018", "'"),
        ("\u2019", "'"),
    ):
        s = s.replace(a, b)
    return s


def _redis_dsn_for_dispatch(master_ip: str) -> str:
    env_url = (os.getenv("REDIS_URL") or "").strip()
    host = (master_ip or "127.0.0.1").strip() or "127.0.0.1"
    port = os.getenv("REDIS_PORT", "6379")
    db = os.getenv("REDIS_DB", "0")
    raw = env_url or f"redis://{host}:{port}/{db}"
    return _coerce_redis_url(raw)


async def _wait_job_result_with_progress(
    jref: Any,
    job_id: str,
    *,
    timeout: float,
    poll_delay: float,
) -> Any:
    """Wrap ``Job.result`` with periodic stderr lines so long runs do not look hung."""
    loop = asyncio.get_running_loop()
    start = loop.time()

    async def _heartbeat() -> None:
        try:
            first = True
            while True:
                await asyncio.sleep(10.0 if first else 30.0)
                first = False
                elapsed = int(loop.time() - start)
                try:
                    st = await jref.status()
                    stv = st.value
                except Exception:
                    stv = "?"
                print(
                    f"[nexus_core] --wait-result: still running ({elapsed}s elapsed, "
                    f"status={stv}) job_id={job_id}",
                    file=sys.stderr,
                    flush=True,
                )
        except asyncio.CancelledError:
            raise

    hb = asyncio.create_task(_heartbeat())
    try:
        return await jref.result(timeout=timeout, poll_delay=poll_delay)
    finally:
        hb.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await hb


def _print_redis_broker_hint() -> None:
    """Explain that the queue broker must be running (not a Nexus code bug)."""
    print(
        "\n[nexus_core] Redis לא זמין ב־127.0.0.1:6379 — צריך להפעיל שרת Redis לפני enqueue.\n"
        "  Docker (מומלץ):\n"
        "    docker run -d --name redis-nexus -p 6379:6379 redis:7-alpine\n"
        "  בדיקה ב־PowerShell:\n"
        "    Test-NetConnection -ComputerName 127.0.0.1 -Port 6379\n",
        file=sys.stderr,
    )


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
    _ensure_windows_redis_ipv4_loopback()
    master_ip = (args.master_ip or "127.0.0.1").strip() or "127.0.0.1"
    dsn = _redis_dsn_for_dispatch(master_ip)
    task_type = _resolve_task_type(args.task or "")
    params_raw: str
    if getattr(args, "params_file", None):
        p = Path(args.params_file)
        if not p.is_file():
            print(f"[nexus_core] --params-file not found: {p}", file=sys.stderr)
            return 1
        params_raw = p.read_text(encoding="utf-8")
    else:
        params_raw = args.params or "{}"
    params_raw = _normalize_cli_json_text(params_raw)
    try:
        params_obj = json.loads(params_raw)
    except json.JSONDecodeError as exc:
        src = "params-file" if getattr(args, "params_file", None) else "--params"
        print(f"[nexus_core] Invalid JSON ({src}): {exc}", file=sys.stderr)
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
        # #region agent log
        try:
            import json as _json
            import time as _time
            _lf = PROJECT_ROOT / "debug-6bcb28.log"
            _lf.open("a", encoding="utf-8").write(
                _json.dumps(
                    {
                        "sessionId": "6bcb28",
                        "hypothesisId": "H5",
                        "location": "nexus_core.py:_cli_dispatch_async",
                        "message": "enqueue_ok_cli_exits_zero",
                        "data": {
                            "task_type": task_type,
                            "job_id": str(jid),
                            "project": project,
                        },
                        "timestamp": int(_time.time() * 1000),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
        except Exception:
            pass
        # #endregion

        if task_type == "telegram.owner_groups_lockdown" and not args.wait_result:
            print(
                "[nexus_core] המשימה רצה אצל ה-worker (לא בטרמינל הזה). "
                "להמתין לסיום ולקבל נתיב לדוח CSV: הוסף --wait-result",
                file=sys.stderr,
            )

        if args.wait_result and job is not None:
            from arq.jobs import Job

            jref = Job(str(jid), redis=pool, _queue_name=ARQ_QUEUE_NAME)
            print(
                f"[nexus_core] --wait-result: waiting up to {float(args.wait_timeout):.0f}s for job {jid}. "
                "Large vaults (many *.session) can take hours; add \"max_sessions\" to params to limit scope. "
                "Progress on stderr: first ping ~10s, then every 30s.",
                file=sys.stderr,
                flush=True,
            )
            try:
                raw = await _wait_job_result_with_progress(
                    jref,
                    str(jid),
                    timeout=float(args.wait_timeout),
                    poll_delay=1.0,
                )
            except Exception as exc:
                print(f"[nexus_core] --wait-result: timeout or error waiting for job: {exc}", file=sys.stderr)
                return 1
            if isinstance(raw, dict):
                err = raw.get("error")
                out = raw.get("output")
                print(
                    f"[nexus_core] Job finished worker_id={raw.get('worker_id')!r} "
                    f"error={err!r} duration_s={raw.get('duration_seconds')!r}",
                )
                if isinstance(out, dict):
                    for key in (
                        "status",
                        "audit_csv_path",
                        "groups_touched",
                        "sessions_considered",
                        "sidecar_json_created",
                    ):
                        if key in out:
                            print(f"[nexus_core]   {key}: {out[key]!r}")
                elif out is not None:
                    print(f"[nexus_core]   output: {out!r}")
            else:
                print(f"[nexus_core] Job raw result: {raw!r}")
            # #region agent log
            try:
                import json as _json
                import time as _time

                _lf = PROJECT_ROOT / "debug-6bcb28.log"
                _wait_data: dict[str, Any] = {"job_id": str(jid)}
                if isinstance(raw, dict):
                    o = raw.get("output")
                    if isinstance(o, dict):
                        _wait_data["audit_csv_path"] = o.get("audit_csv_path")
                        _wait_data["groups_touched"] = o.get("groups_touched")
                    _wait_data["error"] = raw.get("error")
                _lf.open("a", encoding="utf-8").write(
                    _json.dumps(
                        {
                            "sessionId": "6bcb28",
                            "hypothesisId": "H5",
                            "location": "nexus_core.py:wait_result",
                            "message": "job_wait_completed",
                            "data": _wait_data,
                            "timestamp": int(_time.time() * 1000),
                            "runId": "post-fix",
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
            except Exception:
                pass
            # #endregion

        return 0
    except OSError as exc:
        print(f"[nexus_core] Redis connection failed: {exc}", file=sys.stderr)
        _print_redis_broker_hint()
        return 1
    except Exception as exc:
        print(f"[nexus_core] Dispatch failed: {exc}", file=sys.stderr)
        el = str(exc).lower()
        if "timeout" in el or "6379" in el or "redis" in el or "connection" in el:
            _print_redis_broker_hint()
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
        help="Task priority 1-10 (default: 5)",
    )
    parser.add_argument(
        "--params",
        default="{}",
        help='JSON object string for task parameters (default: "{}")',
    )
    parser.add_argument(
        "--params-file",
        default=None,
        metavar="PATH",
        help="Read parameters from a UTF-8 JSON file (overrides --params). Safer on PowerShell than inline JSON.",
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
    parser.add_argument(
        "--wait-result",
        action="store_true",
        help="After enqueue, block until the job completes and print output summary (use for lockdown / CSV path).",
    )
    parser.add_argument(
        "--wait-timeout",
        type=float,
        default=7200.0,
        metavar="SEC",
        help="Max seconds to wait with --wait-result (default: 7200)",
    )
    parser.add_argument(
        "--worker",
        action="store_true",
        help="Worker only: start start_worker.py (no start_api / no telegram bot). "
        "Use with nexus_launcher, which spawns API and bots separately.",
    )
    parser.add_argument('--turbo-boost', action='store_true', help='Enable high process priority')
    parser.add_argument('--skip-sync-check', action='store_true', help='Skip Redis sync check on startup')
    return parser.parse_args()


def _run_script(script_path: str, env_overrides: dict[str, str] | None = None) -> None:
    """Run a Python script in an isolated child process."""
    if env_overrides:
        for key, value in env_overrides.items():
            os.environ[str(key)] = str(value)
    # Reset sys.argv so child scripts don't inherit nexus_core's CLI flags
    # (e.g. --worker --turbo-boost --skip-sync-check) and fail their own argparse.
    sys.argv = [script_path]
    runpy.run_path(script_path, run_name="__main__")


def _graceful_stop(processes: list[Process]) -> None:
    """Request clean stop for all child processes, then force-stop if needed."""
    for proc in processes:
        if proc.is_alive():
            try:
                if hasattr(signal, "SIGINT"):
                    signal_name = signal.SIGINT
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

    if args.turbo_boost:
        try:
            import psutil
            proc = psutil.Process()
            if sys.platform == "win32":
                proc.nice(psutil.HIGH_PRIORITY_CLASS)
                print("[nexus_core] Turbo-boost: process priority set to HIGH (Windows).")
            else:
                proc.nice(-10)
                print("[nexus_core] Turbo-boost: process nice value set to -10 (Linux).")
        except Exception as exc:
            print(f"[nexus_core] Turbo-boost: could not set priority — {exc}", file=sys.stderr)

    missing = [p for p in (API_SCRIPT, BOT_SCRIPT, WORKER_SCRIPT) if not p.exists()]
    if missing:
        for path in missing:
            print(f"[nexus_core] Missing required script: {path}")
        sys.exit(1)
    if not args.skip_sync_check and not _check_redis_socket(host=master_ip):
        print(
            f"\033[1m[!] Redis is unreachable at {master_ip}:6379. "
            "Run 'wsl service redis-server start' (or verify host).\033[0m"
        )

    os.environ["NODE_ID"] = MASTER_NODE_ID
    os.environ["MASTER_IP"] = master_ip

    node_cfg = load_node_config()
    limiter = GlobalResourceManager(
        cpu_limit=node_cfg.cpu_limit,
        ram_limit=node_cfg.ram_limit,
        gpu_limit=node_cfg.gpu_limit,
    )
    limiter.start()

    worker_proc = Process(
        target=_run_script,
        args=(
            str(WORKER_SCRIPT),
            {
                "NODE_ID": MASTER_NODE_ID,
                "MASTER_IP": master_ip,
                "REDIS_URL": _coerce_redis_url(f"redis://{master_ip}:6379/0"),
                "REDIS_HOST": master_ip,
            },
        ),
        name="nexus-local-worker",
    )

    # ``nexus_launcher`` passes ``--worker`` and also spawns ``start_api.py`` / ``start_telegram_bot.py``
    # as separate services. Without this branch, two ``start_api`` processes fight for port 8001
    # (Windows Errno 10048) and neither binds — browser gets ERR_CONNECTION_REFUSED.
    if args.worker:
        processes = [worker_proc]
        print(
            "[nexus_core] --worker: only the ARQ worker is started here; "
            "run API / Telegram via the launcher or start_api.py / start_telegram_bot.py."
        )
    else:
        processes = [
            Process(target=_run_script, args=(str(API_SCRIPT),), name="nexus-api"),
            Process(target=_run_script, args=(str(BOT_SCRIPT),), name="nexus-telegram-bot"),
            worker_proc,
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
