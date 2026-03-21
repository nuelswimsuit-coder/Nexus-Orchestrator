from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import structlog

from nexus.shared.config import settings

log = structlog.get_logger(__name__)

SYSTEM_SETTINGS_PATH = Path(__file__).resolve().parents[2] / "config" / "system_settings.json"

DEFAULT_SYSTEM_SETTINGS: dict[str, Any] = {
    "power_limit": 50,
    "max_workers": 8,
    "log_level": "INFO",
}


def _clamp_int(value: Any, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(min_value, min(max_value, parsed))


def _normalize_log_level(value: Any, default: str = "INFO") -> str:
    level = str(value or default).upper()
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        return default
    return level


def read_system_settings() -> dict[str, Any]:
    """
    Read runtime system settings from config/system_settings.json.
    Falls back to sane defaults if the file is missing or invalid.
    """
    data: dict[str, Any] = {}
    try:
        if SYSTEM_SETTINGS_PATH.exists():
            raw = json.loads(SYSTEM_SETTINGS_PATH.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                data = raw
    except Exception as exc:
        log.warning("system_settings_read_failed", error=str(exc))

    merged = {**DEFAULT_SYSTEM_SETTINGS, **data}
    return {
        "power_limit": _clamp_int(merged.get("power_limit"), 30, 0, 100),
        "max_workers": _clamp_int(merged.get("max_workers"), 3, 1, 64),
        "log_level": _normalize_log_level(merged.get("log_level"), "INFO"),
    }


def write_system_settings(updates: dict[str, Any]) -> dict[str, Any]:
    """
    Update config/system_settings.json and return the normalized saved payload.
    """
    current = read_system_settings()
    merged = {**current, **updates}
    normalized = {
        "power_limit": _clamp_int(merged.get("power_limit"), 30, 0, 100),
        "max_workers": _clamp_int(merged.get("max_workers"), 3, 1, 64),
        "log_level": _normalize_log_level(merged.get("log_level"), "INFO"),
    }
    SYSTEM_SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    SYSTEM_SETTINGS_PATH.write_text(
        json.dumps(normalized, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )
    return normalized


def sync_runtime_from_system_settings(*, apply_power_limit: bool = True) -> dict[str, Any]:
    """
    Apply system_settings.json values to the in-process settings singleton.

    When ``apply_power_limit`` is False, ``master_cpu_cap_percent`` is left unchanged
    (used while NEXUS dynamic power owns the Master CPU cap).
    """
    dynamic = read_system_settings()
    if apply_power_limit:
        object.__setattr__(settings, "master_cpu_cap_percent", float(dynamic["power_limit"]))
    object.__setattr__(settings, "worker_max_jobs", int(dynamic["max_workers"]))
    object.__setattr__(settings, "log_level", str(dynamic["log_level"]))
    return dynamic
