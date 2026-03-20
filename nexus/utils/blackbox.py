"""
nexus/utils/blackbox.py — Flight Data Recorder (Black Box)

Thread-safe circular buffer that continuously records critical telemetry.
On catastrophic failure the buffer is flushed to a timestamped JSON file
structured for AI-assisted root-cause analysis (e.g. paste into Gemini).

Buffer capacity : 500 events (BLACKBOX_MAXLEN env var)
Dump location   : logs/blackbox/crash_dump_YYYYMMDD_HHMMSS.json

Design goals
------------
- Zero overhead when idle — deque appends are O(1) and lock-contention is
  minimal because event recording is a single short critical section.
- Async-safe dump — `dump_to_file()` offloads disk I/O to the thread-pool
  executor so it never stalls the FastAPI / asyncio event loop.
- AI-friendly JSON — the "Crash_Reason_Summary" key is at the top of the
  document so a language model sees context before reading the event log.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psutil
import structlog

log = structlog.get_logger(__name__)

BLACKBOX_MAXLEN: int = int(os.getenv("BLACKBOX_MAXLEN", "500"))

# Resolve dump directory relative to the project root (two levels up from here:
# nexus/utils/blackbox.py → nexus/ → project root).
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BLACKBOX_DIR  = _PROJECT_ROOT / os.getenv("BLACKBOX_DIR", "logs/blackbox")

_MAX_ORDERS = 5
_MAX_TICKS  = 5


class BlackBox:
    """
    In-memory ring buffer that records a continuous telemetry stream.

    The three recording categories mirror the three subsystems most likely to
    cause a production incident:

    * **Resource snapshots** — CPU % and RAM usage sampled periodically.
    * **API latency events** — measured round-trip times for Polymarket /
      Binance / internal calls.
    * **Order payloads** — last N orders sent to Polymarket (secrets masked).
    * **Binance price ticks** — last N raw ticker messages received.

    All category data is also pushed into the unified ``_events`` deque so the
    chronological event log in the dump is complete.
    """

    def __init__(self) -> None:
        self._lock   = threading.Lock()
        self._events: deque[dict[str, Any]] = deque(maxlen=BLACKBOX_MAXLEN)

        # Structured windows — kept separately for easy top-level dump access.
        self._orders: deque[dict[str, Any]] = deque(maxlen=_MAX_ORDERS)
        self._ticks:  deque[dict[str, Any]] = deque(maxlen=_MAX_TICKS)

        self._last_dump_path: str | None = None

    # ── Recording API ──────────────────────────────────────────────────────────

    def record_resource_snapshot(self) -> None:
        """Sample current CPU % and RAM and push to the ring buffer."""
        try:
            cpu = psutil.cpu_percent(interval=None)
            vm  = psutil.virtual_memory()
            self._push({
                "type":        "resource",
                "cpu_pct":     round(cpu, 1),
                "ram_used_mb": round(vm.used / 1_048_576),
                "ram_pct":     round(vm.percent, 1),
            })
        except Exception as exc:
            log.warning("blackbox_resource_error", error=str(exc))

    def record_api_latency(self, service: str, latency_ms: float) -> None:
        """Record a measured API round-trip latency in milliseconds."""
        self._push({
            "type":       "api_latency",
            "service":    service,
            "latency_ms": round(latency_ms, 2),
        })

    def record_order_payload(self, payload: dict[str, Any]) -> None:
        """
        Record an order payload sent to Polymarket.

        Sensitive keys (api_key, secret, token, signature) are masked.
        Only the last 5 orders are retained in the structured window.
        """
        safe = _mask_secrets(payload)
        with self._lock:
            self._orders.append({**safe, "_ts": _now_iso()})
        self._push({"type": "order_sent", "summary": _truncate(safe, max_keys=6)})

    def record_binance_tick(self, tick: dict[str, Any]) -> None:
        """
        Record a raw price tick from Binance.

        Only the last 5 ticks are retained in the structured window.
        """
        with self._lock:
            self._ticks.append({**tick, "_ts": _now_iso()})
        self._push({
            "type":   "binance_tick",
            "symbol": tick.get("symbol"),
            "price":  tick.get("price") or tick.get("c"),
        })

    # ── Dump ──────────────────────────────────────────────────────────────────

    async def dump_to_file(self, reason: str = "Unknown") -> str:
        """
        Flush the in-memory buffer to a timestamped JSON file.

        Disk I/O runs in the default thread-pool executor so the call is
        non-blocking for the asyncio event loop.  Safe to fire-and-forget
        with ``asyncio.create_task()``.

        Returns the absolute path of the written file.
        """
        loop = asyncio.get_event_loop()
        path = await loop.run_in_executor(None, self._write_dump, reason)
        self._last_dump_path = path
        return path

    def dump_to_file_sync(self, reason: str = "Unknown") -> str:
        """Synchronous variant for non-async call sites (e.g. signal handlers)."""
        path = self._write_dump(reason)
        self._last_dump_path = path
        return path

    @property
    def last_dump_path(self) -> str | None:
        """Path of the most recently written dump, or None if none exists yet."""
        return self._last_dump_path

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _push(self, event: dict[str, Any]) -> None:
        event["_ts"] = _now_iso()
        with self._lock:
            self._events.append(event)

    def _write_dump(self, reason: str) -> str:
        """Serialise buffer to disk. Runs inside thread-pool executor."""
        BLACKBOX_DIR.mkdir(parents=True, exist_ok=True)
        ts   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = BLACKBOX_DIR / f"crash_dump_{ts}.json"

        with self._lock:
            events = list(self._events)
            orders = list(self._orders)
            ticks  = list(self._ticks)

        # Live resource stats at dump time (best-effort).
        try:
            cpu = psutil.cpu_percent(interval=None)
            vm  = psutil.virtual_memory()
            live_stats: dict[str, Any] = {
                "cpu_pct":      round(cpu, 1),
                "ram_used_mb":  round(vm.used  / 1_048_576),
                "ram_total_mb": round(vm.total / 1_048_576),
                "ram_pct":      round(vm.percent, 1),
            }
        except Exception:
            live_stats = {}

        doc = {
            # ── AI analysis header ──────────────────────────────────────────
            "Crash_Reason_Summary":   reason,
            "dump_timestamp_utc":     _now_iso(),
            "buffer_capacity":        BLACKBOX_MAXLEN,
            "captured_events":        len(events),
            # ── Snapshot at dump time ───────────────────────────────────────
            "live_resource_stats":    live_stats,
            # ── Structured windows ──────────────────────────────────────────
            "last_5_order_payloads":  orders,
            "last_5_binance_ticks":   ticks,
            # ── Chronological event log ─────────────────────────────────────
            "event_log":              events,
        }

        path.write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")
        log.info(
            "blackbox_dump_written",
            path=str(path),
            events=len(events),
            reason=reason,
            status="[SUCCESS] Black Box dump complete",
        )
        return str(path)


# ── Module-level singleton ─────────────────────────────────────────────────────

blackbox: BlackBox = BlackBox()


# ── Utility helpers ────────────────────────────────────────────────────────────

_SENSITIVE_KEYS = frozenset({"api_key", "secret", "token", "password", "signature", "key"})


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _mask_secrets(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        k: ("***" if k.lower() in _SENSITIVE_KEYS else v)
        for k, v in payload.items()
    }


def _truncate(d: dict[str, Any], max_keys: int = 8) -> dict[str, Any]:
    return dict(list(d.items())[:max_keys])
