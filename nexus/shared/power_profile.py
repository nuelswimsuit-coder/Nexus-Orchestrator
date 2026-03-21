"""
Dynamic Master power profile — time-based CPU affinity/cap and Poly5M cadence.

Redis
-----
nexus:power:override_mode   — auto | force_night | force_active
nexus:power:snapshot        — JSON for API / dashboard (written by Master)
nexus:power:poly5m_cycle_seconds — optional override for scalper loop (seconds)
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import psutil
import structlog

log = structlog.get_logger(__name__)

REDIS_OVERRIDE_KEY = "nexus:power:override_mode"
REDIS_SNAPSHOT_KEY = "nexus:power:snapshot"
REDIS_POLY_CYCLE_KEY = "nexus:power:poly5m_cycle_seconds"

NIGHT_CPU_CAP = float(os.getenv("NEXUS_POWER_NIGHT_CPU_PCT", "90"))
ACTIVE_CPU_CAP = float(os.getenv("NEXUS_POWER_ACTIVE_CPU_PCT", "50"))
NIGHT_CORE_FRACTION = float(os.getenv("NEXUS_POWER_NIGHT_CORE_FRACTION", "0.9"))
ACTIVE_CORE_FRACTION = float(os.getenv("NEXUS_POWER_ACTIVE_CORE_FRACTION", "0.5"))
NIGHT_POLY_CYCLE_S = int(os.getenv("NEXUS_POWER_NIGHT_POLY5M_CYCLE_S", "90"))
ACTIVE_POLY_CYCLE_S = int(os.getenv("NEXUS_POWER_ACTIVE_POLY5M_CYCLE_S", os.getenv("POLY5M_CYCLE_SECONDS", "300")))
IDLE_TO_ACTIVE_S = int(os.getenv("NEXUS_POWER_IDLE_ACTIVE_AFTER_S", "300"))

VALID_OVERRIDES = frozenset({"auto", "force_night", "force_active"})


def _logical_cpu_count() -> int:
    return max(1, int(psutil.cpu_count(logical=True) or 1))


def _core_list(fraction: float) -> list[int]:
    n = _logical_cpu_count()
    k = max(1, int(round(n * max(0.05, min(1.0, fraction)))))
    k = min(k, n)
    return list(range(k))


def seconds_since_last_input() -> float | None:
    """Windows: seconds since last keyboard/mouse input. Other OS: None."""
    if sys.platform != "win32":
        return None
    try:
        import ctypes

        class LASTINPUTINFO(ctypes.Structure):
            _fields_ = [("cbSize", ctypes.c_uint32), ("dwTime", ctypes.c_uint32)]

        li = LASTINPUTINFO()
        li.cbSize = ctypes.sizeof(LASTINPUTINFO)
        if not ctypes.windll.user32.GetLastInputInfo(ctypes.byref(li)):  # type: ignore[attr-defined]
            return None
        tick64 = ctypes.windll.kernel32.GetTickCount64()  # type: ignore[attr-defined]
        elapsed_ms = int(tick64) - int(li.dwTime)
        if elapsed_ms < 0:
            elapsed_ms = 0
        return elapsed_ms / 1000.0
    except Exception:
        return None


def is_scheduled_night_local(now: datetime | None = None) -> bool:
    t = now or datetime.now()
    return 0 <= t.hour < 8


def _next_schedule_boundary_local(now: datetime) -> datetime:
    """Next instant the *scheduled* mode flips (local time)."""
    if is_scheduled_night_local(now):
        return now.replace(hour=8, minute=0, second=0, microsecond=0)
    # Active (08:00–24:00): next scheduled change is midnight → night window.
    return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)


@dataclass
class PowerDecision:
    effective: str  # "night" | "active"
    cpu_cap_percent: float
    core_fraction: float
    affinity_cores: list[int]
    poly5m_cycle_seconds: int
    override: str
    scheduled_night: bool
    idle_dropped_to_active: bool
    seconds_idle: float | None
    next_shift_local_iso: str
    seconds_until_shift: int
    display_line: str


def _normalize_override(raw: str | None) -> str:
    s = (raw or "auto").strip().lower()
    return s if s in VALID_OVERRIDES else "auto"


def decide_power_profile(
    *,
    now: datetime | None = None,
    override_raw: str | None = None,
) -> PowerDecision:
    now = now or datetime.now()
    override = _normalize_override(override_raw)
    scheduled_night = is_scheduled_night_local(now)
    idle_s = seconds_since_last_input()
    idle_drop = False

    if override == "force_night":
        effective = "night"
    elif override == "force_active":
        effective = "active"
    else:
        if scheduled_night and idle_s is not None and idle_s >= float(IDLE_TO_ACTIVE_S):
            effective = "active"
            idle_drop = True
        elif scheduled_night:
            effective = "night"
        else:
            effective = "active"

    if effective == "night":
        cap = NIGHT_CPU_CAP
        frac = NIGHT_CORE_FRACTION
        poly = NIGHT_POLY_CYCLE_S
        label = "NIGHT-MODE"
    else:
        cap = ACTIVE_CPU_CAP
        frac = ACTIVE_CORE_FRACTION
        poly = ACTIVE_POLY_CYCLE_S
        label = "ACTIVE-MODE"

    cores = _core_list(frac)
    next_boundary = _next_schedule_boundary_local(now)
    sec_until = max(0, int((next_boundary - now).total_seconds()))

    disp = f"MASTER: [{label} {int(cap)}%]"
    return PowerDecision(
        effective=effective,
        cpu_cap_percent=cap,
        core_fraction=frac,
        affinity_cores=cores,
        poly5m_cycle_seconds=poly,
        override=override,
        scheduled_night=scheduled_night,
        idle_dropped_to_active=idle_drop,
        seconds_idle=idle_s,
        next_shift_local_iso=next_boundary.isoformat(timespec="seconds"),
        seconds_until_shift=sec_until,
        display_line=disp,
    )


def apply_power_to_process(
    pid: int,
    decision: PowerDecision,
    *,
    set_affinity: bool = True,
) -> dict[str, Any]:
    """Apply CPU affinity to an existing process (no restart). Returns status dict."""
    out: dict[str, Any] = {"pid": pid, "affinity_ok": False, "cores": decision.affinity_cores}
    if not set_affinity:
        return out
    try:
        proc = psutil.Process(pid)
        proc.cpu_affinity(decision.affinity_cores)
        out["affinity_ok"] = True
    except Exception as exc:
        log.warning("power_affinity_failed", pid=pid, error=str(exc))
    return out


def snapshot_dict(
    pid: int,
    decision: PowerDecision,
    affinity_ok: bool,
) -> dict[str, Any]:
    return {
        "effective_mode": decision.effective,
        "display_label": decision.display_line,
        "cpu_cap_percent": decision.cpu_cap_percent,
        "affinity_cores": decision.affinity_cores,
        "affinity_applied": affinity_ok,
        "logical_cores": _logical_cpu_count(),
        "override": decision.override,
        "scheduled_night": decision.scheduled_night,
        "idle_dropped_to_active": decision.idle_dropped_to_active,
        "seconds_since_input": decision.seconds_idle,
        "poly5m_cycle_seconds": decision.poly5m_cycle_seconds,
        "master_pid": pid,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "next_shift_local": decision.next_shift_local_iso,
        "seconds_until_shift": decision.seconds_until_shift,
    }


def snapshot_json(pid: int, decision: PowerDecision, affinity_ok: bool) -> str:
    return json.dumps(snapshot_dict(pid, decision, affinity_ok), ensure_ascii=True)


def cycle_override_mode(current: str) -> str:
    order = ("auto", "force_night", "force_active")
    try:
        i = order.index(current)
    except ValueError:
        i = 0
    return order[(i + 1) % len(order)]


def parse_snapshot(raw: str | None) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        return None
