"""
Nexus Supreme — Hot Reload Watcher
===================================
Watches the project source tree for .py file changes and automatically
restarts the configured service processes (Telegram bot, API server, workers).

Usage:
    python scripts/hot_reload_watcher.py                  # restart all
    python scripts/hot_reload_watcher.py --service bot    # restart bot only
    python scripts/hot_reload_watcher.py --service api    # restart API only

Requires:
    pip install watchdog

Environment variables honoured:
    NEXUS_BOT_TOKEN   — skip bot restart if absent
    NEXUS_RELOAD_DELAY — seconds to wait between detecting a change and
                         restarting (default 1.5, avoids partial-save thrash)
    NEXUS_WATCH_DIRS  — comma-separated list of dirs to watch
                        (default: nexus, nexus_supreme, scripts)
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# ---------------------------------------------------------------------------
# Service definitions — each entry maps a short name to its start command.
# Modify freely; relative paths are resolved from ROOT.
# ---------------------------------------------------------------------------

SERVICES: dict[str, list[str]] = {
    "bot":    [sys.executable, "scripts/start_telegram_bot.py"],
    "api":    [sys.executable, "-m", "uvicorn", "nexus.api.main:app",
               "--host", "0.0.0.0", "--port", "8000", "--reload"],
    "worker": [sys.executable, "scripts/start_worker.py"],
    "master": [sys.executable, "scripts/start_master.py"],
}

WATCH_DIRS_DEFAULT = ["nexus", "nexus_supreme", "scripts"]
RELOAD_DELAY       = float(os.environ.get("NEXUS_RELOAD_DELAY", "1.5"))

# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Nexus hot-reload watcher")
    p.add_argument(
        "--service", "-s",
        nargs="+",
        choices=list(SERVICES.keys()) + ["all"],
        default=["all"],
        help="Which service(s) to restart on change (default: all)",
    )
    p.add_argument(
        "--once", action="store_true",
        help="Restart once immediately then exit (useful in CI)",
    )
    return p.parse_args()


class ProcessManager:
    """Manage a set of service subprocesses with restart capability."""

    def __init__(self, names: list[str]) -> None:
        if "all" in names:
            names = list(SERVICES.keys())
        self._names = names
        self._procs: dict[str, subprocess.Popen | None] = {n: None for n in names}

    def start_all(self) -> None:
        for name in self._names:
            self.start(name)

    def start(self, name: str) -> None:
        self.stop(name)
        cmd = SERVICES[name]
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=str(ROOT),
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
            )
            self._procs[name] = proc
            print(f"[reload] ▶  started  {name}  (pid {proc.pid})")
        except FileNotFoundError as exc:
            print(f"[reload] ❌  {name}: {exc}")

    def stop(self, name: str) -> None:
        proc = self._procs.get(name)
        if proc and proc.poll() is None:
            try:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                print(f"[reload] ⏹  stopped  {name}  (pid {proc.pid})")
            except Exception as exc:
                print(f"[reload] ⚠  stop {name}: {exc}")
        self._procs[name] = None

    def restart_all(self) -> None:
        print(f"[reload] 🔄  restarting: {', '.join(self._names)}")
        for name in self._names:
            self.start(name)

    def stop_all(self) -> None:
        for name in self._names:
            self.stop(name)


def _watch_with_watchdog(manager: ProcessManager, watch_dirs: list[Path]) -> None:
    """Use the watchdog library for efficient inotify/kqueue/FSEvents watching."""
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler

    last_restart = [0.0]

    class _Handler(FileSystemEventHandler):
        def on_any_event(self, event):
            # Only care about Python source changes
            if not str(event.src_path).endswith(".py"):
                return
            now = time.monotonic()
            if now - last_restart[0] < RELOAD_DELAY:
                return
            last_restart[0] = now
            print(f"\n[reload] 📄  changed: {event.src_path}")
            time.sleep(RELOAD_DELAY)   # wait for IDE to finish writing
            manager.restart_all()

    handler  = _Handler()
    observer = Observer()
    for d in watch_dirs:
        if d.exists():
            observer.schedule(handler, str(d), recursive=True)
            print(f"[reload] 👁  watching {d}")
        else:
            print(f"[reload] ⚠  directory not found: {d}")

    observer.start()
    print("[reload] ✅  hot-reload watcher running  (Ctrl+C to stop)")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        observer.stop()
        observer.join()


def _watch_polling(manager: ProcessManager, watch_dirs: list[Path]) -> None:
    """Fallback polling watcher — no extra deps needed."""
    import glob as _glob
    print("[reload] ℹ  watchdog not installed — using polling (5s interval)")

    def _mtimes() -> dict[str, float]:
        result = {}
        for d in watch_dirs:
            for f in d.rglob("*.py"):
                result[str(f)] = f.stat().st_mtime
        return result

    prev = _mtimes()
    print("[reload] ✅  polling watcher running  (Ctrl+C to stop)")
    try:
        while True:
            time.sleep(5)
            curr = _mtimes()
            changed = [p for p, m in curr.items() if prev.get(p) != m]
            if changed:
                print(f"\n[reload] 📄  changed: {changed[0]}" +
                      (f" (+{len(changed)-1} more)" if len(changed) > 1 else ""))
                manager.restart_all()
                prev = _mtimes()
    except KeyboardInterrupt:
        pass


def main() -> None:
    args    = _parse_args()
    names   = args.service
    manager = ProcessManager(names)

    watch_env  = os.environ.get("NEXUS_WATCH_DIRS", "")
    watch_dirs = [
        ROOT / d for d in (watch_env.split(",") if watch_env else WATCH_DIRS_DEFAULT)
    ]

    manager.start_all()

    if args.once:
        print("[reload] --once flag: started, exiting immediately.")
        return

    try:
        import watchdog  # noqa: F401
        _watch_with_watchdog(manager, watch_dirs)
    except ImportError:
        _watch_polling(manager, watch_dirs)
    finally:
        print("\n[reload] 🛑  shutting down all services…")
        manager.stop_all()


if __name__ == "__main__":
    main()
