"""
QThread workers — keep the GUI responsive by running I/O off the main thread.

Workers emit pyqtSignals only — never touch QWidget objects directly.
All UI mutations must happen in the main thread via connected slots.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Callable

import requests
from PyQt6.QtCore import QThread, pyqtSignal


class ApiPoller(QThread):
    """Polls one or more Nexus FastAPI endpoints every N seconds."""
    data_ready = pyqtSignal(str, object)   # (endpoint, parsed_json)
    error      = pyqtSignal(str, str)       # (endpoint, error_message)

    def __init__(self, base_url: str, endpoints: list[str], interval: float = 5.0,
                 api_key: str = "") -> None:
        super().__init__()
        self._base     = base_url.rstrip("/")
        self._eps      = endpoints
        self._interval = interval
        self._api_key  = api_key
        self._stop     = False

    def run(self) -> None:
        import time
        headers = {"X-Nexus-Api-Key": self._api_key} if self._api_key else {}
        while not self._stop:
            for ep in self._eps:
                url = f"{self._base}{ep}"
                try:
                    resp = requests.get(url, headers=headers, timeout=5)
                    resp.raise_for_status()
                    self.data_ready.emit(ep, resp.json())
                except Exception as exc:
                    self.error.emit(ep, str(exc))
            time.sleep(self._interval)

    def stop(self) -> None:
        self._stop = True
        self.quit()


class MigrationWorker(QThread):
    """Runs the AHU → Nexus migration in a background thread."""
    progress = pyqtSignal(str)
    finished = pyqtSignal(dict)

    def __init__(self, force: bool = False) -> None:
        super().__init__()
        self._force = force

    def run(self) -> None:
        try:
            from nexus_supreme.core.db.migration import run_migration
            result = run_migration(force=self._force, progress=self.progress.emit)
            self.finished.emit(result)
        except Exception as exc:
            self.progress.emit(f"ERROR: {exc}")
            self.finished.emit({"ok": False, "error": str(exc)})


class ScraperWorker(QThread):
    """Runs ChatArchiver.archive_chat in a background thread."""
    progress = pyqtSignal(str)
    finished = pyqtSignal(dict)

    def __init__(self, session_path: str, chat_id: int, api_id: int, api_hash: str,
                 download_media: bool = True) -> None:
        super().__init__()
        self._session   = session_path
        self._chat_id   = chat_id
        self._api_id    = api_id
        self._api_hash  = api_hash
        self._dl_media  = download_media

    def run(self) -> None:
        import asyncio
        try:
            from nexus_supreme.core.scraper import ChatArchiver
            archiver = ChatArchiver(
                session_path   = self._session,
                api_id         = self._api_id,
                api_hash       = self._api_hash,
                download_media = self._dl_media,
                progress_cb    = self.progress.emit,
            )
            loop   = asyncio.new_event_loop()
            result = loop.run_until_complete(archiver.archive_chat(self._chat_id))
            loop.run_until_complete(archiver.close())
            loop.close()
            self.finished.emit(result)
        except Exception as exc:
            self.progress.emit(f"ERROR: {exc}")
            self.finished.emit({"ok": False, "error": str(exc)})


class ServiceLauncher(QThread):
    """Launches a subprocess (service) and streams its stdout to the log panel."""
    line_ready = pyqtSignal(str)
    finished   = pyqtSignal(int)   # exit code

    def __init__(self, cmd: list[str] | str, cwd: str, shell: bool = False,
                 env: dict | None = None) -> None:
        super().__init__()
        self._cmd  = cmd
        self._cwd  = cwd
        self._shell= shell
        self._env  = env
        self._proc: subprocess.Popen | None = None

    def run(self) -> None:
        import os
        env = {**os.environ, **(self._env or {})}
        try:
            self._proc = subprocess.Popen(
                self._cmd,
                cwd   = self._cwd,
                shell = self._shell,
                stdout= subprocess.PIPE,
                stderr= subprocess.STDOUT,
                env   = env,
            )
            for raw in iter(self._proc.stdout.readline, b""):  # type: ignore[union-attr]
                line = raw.decode("utf-8", errors="replace").rstrip()
                if line:
                    self.line_ready.emit(line)
            self._proc.wait()
            self.finished.emit(self._proc.returncode or 0)
        except Exception as exc:
            self.line_ready.emit(f"[ERROR] {exc}")
            self.finished.emit(-1)

    def stop(self) -> None:
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()


# ══════════════════════════════════════════════════════════════════════════════
# Redis Live Poller — reads heartbeat keys and queue depth every 2 s
# ══════════════════════════════════════════════════════════════════════════════

class RedisLivePoller(QThread):
    """
    Reads Nexus telemetry directly from Redis every `interval` seconds.

    Keys consumed:
      nexus:heartbeat:<node_id>   — JSON NodeHeartbeat (per worker/master)
      arq:queue:nexus:tasks       — sorted set; ZCARD = pending jobs
      nexus:war_room:intel        — optional war-room cache

    Emits:
      telemetry(list[dict])       — list of parsed NodeHeartbeat dicts
      queue_depth(int)            — number of pending ARQ jobs
      redis_latency_ms(float)     — PING round-trip in ms
      redis_error(str)            — connection / parse error message
    """
    telemetry        = pyqtSignal(list)    # list[dict] — one per node
    queue_depth      = pyqtSignal(int)
    redis_latency_ms = pyqtSignal(float)
    redis_error      = pyqtSignal(str)

    HEARTBEAT_PREFIX = "nexus:heartbeat:"
    ARQ_QUEUE_KEY    = "arq:queue:nexus:tasks"

    def __init__(self, redis_url: str = "", interval: float = 2.0) -> None:
        super().__init__()
        self._url      = redis_url or os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        self._interval = interval
        self._stop     = False

    def run(self) -> None:
        try:
            import redis as redis_lib
        except ImportError:
            self.redis_error.emit("redis-py not installed — run: pip install redis")
            return

        r: redis_lib.Redis | None = None

        while not self._stop:
            try:
                if r is None:
                    r = redis_lib.from_url(
                        self._url,
                        decode_responses=True,
                        socket_connect_timeout=3,
                        socket_timeout=3,
                    )

                # ── PING latency ──────────────────────────────────────────────
                t0 = time.perf_counter()
                r.ping()
                latency_ms = (time.perf_counter() - t0) * 1000
                self.redis_latency_ms.emit(round(latency_ms, 1))

                # ── Heartbeats ────────────────────────────────────────────────
                nodes: list[dict] = []
                cursor = 0
                while True:
                    cursor, keys = r.scan(
                        cursor, match=f"{self.HEARTBEAT_PREFIX}*", count=50
                    )
                    for key in keys:
                        raw = r.get(key)
                        if not raw:
                            continue
                        try:
                            hb = json.loads(raw)
                            # Normalise field names (handle both snake_case variants)
                            hb.setdefault("status", "online")
                            hb.setdefault("cpu_percent",  hb.pop("cpu_pct", 0))
                            hb.setdefault("ram_used_mb",  hb.pop("ram_mb",  0))
                            hb.setdefault("active_jobs",  0)
                            hb.setdefault("node_id",
                                          key[len(self.HEARTBEAT_PREFIX):])
                            nodes.append(hb)
                        except (json.JSONDecodeError, KeyError):
                            pass
                    if cursor == 0:
                        break

                self.telemetry.emit(nodes)

                # ── ARQ queue depth ───────────────────────────────────────────
                try:
                    depth = r.zcard(self.ARQ_QUEUE_KEY) or 0
                    self.queue_depth.emit(int(depth))
                except Exception:
                    pass

            except Exception as exc:
                self.redis_error.emit(str(exc)[:120])
                r = None   # force reconnect next cycle

            time.sleep(self._interval)

    def stop(self) -> None:
        self._stop = True
        self.quit()
