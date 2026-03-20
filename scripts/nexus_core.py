"""
Unified launcher for lightweight Nexus core services.

Starts:
  - scripts/start_api.py
  - scripts/start_telegram_bot.py

Both services are launched via multiprocessing and can be stopped together
with Ctrl-C (SIGINT).
"""

from __future__ import annotations

import os
import runpy
import socket
import signal
import sys
from multiprocessing import Process
from pathlib import Path
from time import sleep

from nexus.utils.resources import GlobalResourceManager, load_node_config

BASE_DIR = Path(__file__).resolve().parent
API_SCRIPT = BASE_DIR / "start_api.py"
BOT_SCRIPT = BASE_DIR / "start_telegram_bot.py"
WORKER_SCRIPT = BASE_DIR / "start_worker.py"


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
    missing = [p for p in (API_SCRIPT, BOT_SCRIPT, WORKER_SCRIPT) if not p.exists()]
    if missing:
        for path in missing:
            print(f"[nexus_core] Missing required script: {path}")
        sys.exit(1)
    if not _check_redis_socket():
        print("\033[1m[!] Redis is unreachable. Run 'wsl service redis-server start'\033[0m")

    # Master-hybrid identity is inherited by API/Bot and enforced explicitly
    # for the colocated worker process below.
    os.environ["NODE_ID"] = "master-hybrid-node"

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
                    "NODE_ID": "master-hybrid-node",
                    "REDIS_URL": "redis://127.0.0.1:6379/0",
                    "REDIS_HOST": "127.0.0.1",
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
