"""
Nexus Diagnostic Engine — NexusDiagnostics

Collects a structured JSON health report for the current node and sends it to
the "Management Ahu (Ops Sync)" Telegram chat.

Report fields
-------------
  node_name       — socket.gethostname()
  cpu_temp        — CPU temperature via psutil (°C), or "N/A" if unavailable
  redis_status    — "OK" | "UNREACHABLE"
  telefix_db_rows — row count from nexus_dashboard.sqlite3 (all tables combined)
  git_last_sync   — ISO timestamp of the last local git commit
  active_pids     — list of Nexus-related process PIDs currently running

Sending schedule
----------------
  • Every 60 minutes (background daemon thread).
  • On-demand: call ``NexusDiagnostics.send_now()`` or import ``report_and_send()``.
  • Triggered externally by nexus_core.py on every task dispatch.

Environment variables
---------------------
  TELEGRAM_BOT_TOKEN         — bot token
  TELEGRAM_ADMIN_CHAT_ID     — primary admin chat (fallback)
  TELEGRAM_OPS_CHAT_ID       — "Management Ahu (Ops Sync)" chat ID (preferred)
  REDIS_URL                  — Redis DSN (default: redis://127.0.0.1:6379/0)
  NEXUS_TELEFIX_DB           — path to telefix DB (default: data/nexus_dashboard.sqlite3)
  NEXUS_DIAG_INTERVAL        — report interval in seconds (default: 3600)

Usage
-----
    # Standalone (blocking):
    python scripts/nexus_diagnostics.py

    # As a background service from another script:
    from scripts.nexus_diagnostics import NexusDiagnostics
    diag = NexusDiagnostics()
    diag.start()          # non-blocking daemon thread
    diag.send_now()       # immediate one-shot report
    diag.stop()           # graceful shutdown
"""

from __future__ import annotations

import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.getcwd())

import json
import socket
import sqlite3
import subprocess
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ── Config ────────────────────────────────────────────────────────────────────

_HERE = Path(os.path.dirname(os.path.abspath(__file__)))
ROOT: Path = _HERE.parent if _HERE.name == "scripts" else _HERE

# Load .env if present so this script works standalone
_ENV_FILE = ROOT / ".env"
if _ENV_FILE.exists():
    for _line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if not _line or _line.startswith("#") or "=" not in _line:
            continue
        _k, _, _v = _line.partition("=")
        _k = _k.strip()
        _v = _v.strip().split("#")[0].strip()
        if _k and _k not in os.environ:
            os.environ[_k] = _v

BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
# Prefer dedicated ops chat; fall back to admin chat
OPS_CHAT_ID: str = (
    os.getenv("TELEGRAM_OPS_CHAT_ID", "")
    or os.getenv("TELEGRAM_ADMIN_CHAT_ID", "")
)
REDIS_URL: str = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
TELEFIX_DB: Path = ROOT / os.getenv("NEXUS_TELEFIX_DB", "data/nexus_dashboard.sqlite3")
DIAG_INTERVAL: int = int(os.getenv("NEXUS_DIAG_INTERVAL", "3600"))

NEXUS_PROCESS_KEYWORDS = [
    "nexus_core", "start_worker", "start_api", "start_telegram_bot",
    "nexus_launcher", "arq", "uvicorn",
]


# ── Collectors ────────────────────────────────────────────────────────────────

def _collect_node_name() -> str:
    return socket.gethostname()


def _collect_cpu_temp() -> str:
    """Return CPU temperature string or 'N/A'."""
    try:
        import psutil  # type: ignore[import]
        temps = psutil.sensors_temperatures()
        if not temps:
            return "N/A"
        for key in ("coretemp", "cpu_thermal", "k10temp", "acpitz"):
            if key in temps and temps[key]:
                return f"{temps[key][0].current:.1f}°C"
        # Fallback: first available sensor
        for entries in temps.values():
            if entries:
                return f"{entries[0].current:.1f}°C"
    except Exception:
        pass
    return "N/A"


def _collect_redis_latency() -> dict:
    """Ping Redis and measure round-trip latency in ms. Returns dict with status and latency_ms."""
    try:
        import redis as _redis  # type: ignore[import]
        import time as _time
        client = _redis.from_url(REDIS_URL, socket_connect_timeout=2, socket_timeout=2)
        t0 = _time.perf_counter()
        client.ping()
        latency_ms = round((_time.perf_counter() - t0) * 1000, 2)
        client.close()
        return {"status": "OK", "latency_ms": latency_ms}
    except Exception:
        return {"status": "UNREACHABLE", "latency_ms": None}


def _collect_redis_status() -> str:
    """Ping Redis; return 'OK' or 'UNREACHABLE'."""
    return _collect_redis_latency()["status"]


def _collect_telefix_db_rows() -> dict:
    """Count rows per table in the telefix SQLite DB. Returns dict with total and per-table breakdown."""
    if not TELEFIX_DB.exists():
        return {"total": -1, "tables": {}}
    try:
        conn = sqlite3.connect(str(TELEFIX_DB), timeout=5)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        breakdown: dict = {}
        total = 0
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM [{table}]")  # noqa: S608
                row = cursor.fetchone()
                count = int(row[0]) if row else 0
                breakdown[table] = count
                total += count
            except Exception:
                breakdown[table] = -1
        conn.close()
        return {"total": total, "tables": breakdown}
    except Exception:
        return {"total": -1, "tables": {}}


def _collect_git_last_sync() -> str:
    """Return ISO timestamp of the last git commit, or 'unknown'."""
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%ci"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return "unknown"


def _collect_active_pids() -> list[int]:
    """Return PIDs of processes whose cmdline contains Nexus-related keywords."""
    pids: list[int] = []
    try:
        import psutil  # type: ignore[import]
        for proc in psutil.process_iter(["pid", "cmdline"]):
            try:
                cmdline = " ".join(proc.info.get("cmdline") or []).lower()
                if any(kw in cmdline for kw in NEXUS_PROCESS_KEYWORDS):
                    pids.append(proc.info["pid"])
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    except Exception:
        pass
    return sorted(pids)


# ── Report builder ────────────────────────────────────────────────────────────

def build_report() -> dict[str, Any]:
    """Collect all diagnostic fields and return a JSON-serialisable dict."""
    redis_info = _collect_redis_latency()
    db_info = _collect_telefix_db_rows()
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "node_name": _collect_node_name(),
        "cpu_temp": _collect_cpu_temp(),
        "redis_status": redis_info["status"],
        "redis_latency_ms": redis_info["latency_ms"],
        "telefix_db_rows": db_info["total"],
        "telefix_db_tables": db_info["tables"],
        "git_last_sync": _collect_git_last_sync(),
        "active_pids": _collect_active_pids(),
    }


# ── Telegram sender ───────────────────────────────────────────────────────────

def _format_telegram_message(report: dict[str, Any]) -> str:
    pids = report.get("active_pids", [])
    pids_str = ", ".join(str(p) for p in pids) if pids else "none"
    redis_ok = report.get("redis_status") == "OK"
    redis_icon = "✅" if redis_ok else "🔴"
    latency = report.get("redis_latency_ms")
    latency_str = f"{latency}ms" if latency is not None else "—"
    rows = report.get("telefix_db_rows", -1)
    rows_str = str(rows) if rows >= 0 else "DB not found"

    # Per-table breakdown (compact)
    tables: dict = report.get("telefix_db_tables", {})
    table_lines = ""
    if tables:
        table_lines = "\n".join(
            f"  • `{t}`: {c}" for t, c in sorted(tables.items(), key=lambda x: -x[1])[:8]
        )
        table_lines = f"\n{table_lines}"

    return (
        "🖥 *Nexus System Health Report*\n"
        f"🕐 `{report.get('timestamp', '?')}`\n\n"
        f"📡 *Node:* `{report.get('node_name', '?')}`\n"
        f"🌡 *CPU Temp:* `{report.get('cpu_temp', 'N/A')}`\n"
        f"{redis_icon} *Redis:* `{report.get('redis_status', '?')}` — latency `{latency_str}`\n"
        f"🗄 *Telefix DB Rows:* `{rows_str}`{table_lines}\n"
        f"🔀 *Git Last Sync:* `{report.get('git_last_sync', 'unknown')}`\n"
        f"⚙️ *Active PIDs:* `{pids_str}`"
    )


def send_telegram(message: str, chat_id: str = OPS_CHAT_ID) -> bool:
    """Send a Markdown message to the given Telegram chat. Returns True on success."""
    if not BOT_TOKEN or not chat_id:
        print("[nexus_diagnostics] Telegram not configured (missing token or chat_id).", flush=True)
        return False
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = json.dumps({
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
        }).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status == 200
    except Exception as exc:
        print(f"[nexus_diagnostics] Telegram send failed: {exc}", flush=True)
        return False


def report_and_send(chat_id: str = OPS_CHAT_ID) -> dict[str, Any]:
    """
    Build a diagnostic report, print it, and send it to Telegram.
    Returns the report dict (useful for callers that need the data).
    """
    report = build_report()
    print(
        f"[nexus_diagnostics] Report: {json.dumps(report, ensure_ascii=False)}",
        flush=True,
    )
    message = _format_telegram_message(report)
    send_telegram(message, chat_id=chat_id)
    return report


# ── Background service class ──────────────────────────────────────────────────

class NexusDiagnostics:
    """
    Background daemon that sends a diagnostic report every ``interval`` seconds
    and exposes ``send_now()`` for on-demand reports (e.g. on task dispatch).

    Usage
    -----
        diag = NexusDiagnostics()
        diag.start()
        diag.send_now()   # triggered by nexus_core on each job dispatch
        diag.stop()
    """

    SERVICE_NAME = "NexusDiagnostics"

    def __init__(
        self,
        interval: int = DIAG_INTERVAL,
        chat_id: str = OPS_CHAT_ID,
    ) -> None:
        self.interval = interval
        self.chat_id = chat_id
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name=self.SERVICE_NAME,
            daemon=True,
        )
        self._thread.start()
        print(
            f"[{self.SERVICE_NAME}] Started — interval={self.interval}s "
            f"chat_id={self.chat_id or '(not set)'}",
            flush=True,
        )

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)
        print(f"[{self.SERVICE_NAME}] Stopped.", flush=True)

    def send_now(self) -> dict[str, Any]:
        """Send an immediate report (non-blocking call — runs in caller thread)."""
        return report_and_send(chat_id=self.chat_id)

    def _loop(self) -> None:
        # Send once immediately on startup
        try:
            report_and_send(chat_id=self.chat_id)
        except Exception as exc:
            print(f"[{self.SERVICE_NAME}] Initial report failed: {exc}", flush=True)

        # Then repeat every ``interval`` seconds
        elapsed = 0
        while not self._stop_event.is_set():
            time.sleep(1)
            elapsed += 1
            if elapsed >= self.interval:
                elapsed = 0
                try:
                    report_and_send(chat_id=self.chat_id)
                except Exception as exc:
                    print(f"[{self.SERVICE_NAME}] Periodic report failed: {exc}", flush=True)


# ── Standalone entry point ────────────────────────────────────────────────────

def main() -> None:
    """Run NexusDiagnostics in the foreground (blocking)."""
    svc = NexusDiagnostics()
    svc.start()
    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        svc.stop()


if __name__ == "__main__":
    main()
