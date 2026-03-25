"""
Live system statistics — CPU temperature, usage, memory, and per-node heat map.

Temperature detection strategy
-------------------------------
Linux / macOS : psutil.sensors_temperatures() — reads coretemp / k10temp / acpitz.
                Also reads /sys/class/thermal/thermal_zone*/temp (millidegrees).
Windows       : WMI query against MSAcpi_ThermalZoneTemperature.
                Raw value is in tenths of Kelvin; converted to Celsius via:
                    °C = (raw / 10) - 273.15

Heat Map & Auto-Throttle
------------------------
``publish_node_heat_map(redis_client, node_id)`` writes per-node thermal data to
Redis under ``nexus:thermal:<node_id>``.  Any node exceeding ``THROTTLE_TEMP_C``
(default 85°C) has a ``throttle=True`` flag set, which the task dispatcher reads
to reduce its queue depth.

The result is published as ``cpu_temp`` (float, Celsius) in the heartbeat payload.
Returns None when no sensor data is available so callers can omit the field
gracefully rather than sending a bogus 0.0.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import structlog

log = structlog.get_logger(__name__)

# Temperature threshold above which a node is auto-throttled (°C)
THROTTLE_TEMP_C: float = float(os.getenv("NEXUS_THROTTLE_TEMP_C", "85"))
# Redis key prefix for per-node thermal data
THERMAL_KEY_PREFIX = "nexus:thermal:"
THERMAL_KEY_TTL = 120  # seconds


def get_cpu_temp_celsius() -> float | None:
    """
    Return the current CPU temperature in Celsius, or None if unavailable.

    Tries platform-appropriate methods in order:
    1. psutil.sensors_temperatures() — Linux / macOS
    2. /sys/class/thermal/thermal_zone* — Linux fallback
    3. WMI MSAcpi_ThermalZoneTemperature — Windows
    """
    if sys.platform == "win32":
        return _get_cpu_temp_windows()
    temp = _get_cpu_temp_psutil()
    if temp is None:
        temp = _get_cpu_temp_sysfs()
    return temp


# ── Linux / macOS ──────────────────────────────────────────────────────────────

def _get_cpu_temp_psutil() -> float | None:
    """Read CPU temperature via psutil.sensors_temperatures()."""
    try:
        import psutil  # noqa: PLC0415

        all_sensors: dict[str, list[Any]] | None = getattr(
            psutil, "sensors_temperatures", None
        )
        if all_sensors is None:
            return None

        sensors = psutil.sensors_temperatures()
        if not sensors:
            return None

        # Prefer coretemp (Intel) → k10temp (AMD) → acpitz → first available
        priority_keys = ["coretemp", "k10temp", "acpitz"]
        for key in priority_keys:
            entries = sensors.get(key)
            if entries:
                # Use the first "Package" or "Tdie" entry; fall back to first entry
                for entry in entries:
                    label = (entry.label or "").lower()
                    if "package" in label or "tdie" in label or "tctl" in label:
                        return round(float(entry.current), 1)
                return round(float(entries[0].current), 1)

        # Fallback: first available sensor
        for entries in sensors.values():
            if entries:
                return round(float(entries[0].current), 1)

    except Exception as exc:
        log.debug("cpu_temp_psutil_failed", error=str(exc))

    return None


def _get_cpu_temp_sysfs() -> float | None:
    """
    Read CPU temperature from /sys/class/thermal/thermal_zone*/temp (Linux).
    Values are in millidegrees Celsius.
    """
    try:
        thermal_base = Path("/sys/class/thermal")
        if not thermal_base.exists():
            return None
        readings: list[float] = []
        for zone in sorted(thermal_base.glob("thermal_zone*")):
            temp_file = zone / "temp"
            type_file = zone / "type"
            if not temp_file.exists():
                continue
            zone_type = ""
            if type_file.exists():
                try:
                    zone_type = type_file.read_text().strip().lower()
                except Exception:
                    pass
            try:
                raw = int(temp_file.read_text().strip())
                celsius = raw / 1000.0
                if 0.0 < celsius < 150.0:
                    # Prefer CPU-related zones
                    if any(k in zone_type for k in ("cpu", "core", "x86_pkg", "acpitz")):
                        return round(celsius, 1)
                    readings.append(celsius)
            except Exception:
                continue
        if readings:
            return round(max(readings), 1)
    except Exception as exc:
        log.debug("cpu_temp_sysfs_failed", error=str(exc))
    return None


# ── Windows (WMI) ──────────────────────────────────────────────────────────────

def _get_cpu_temp_windows() -> float | None:
    """
    Query MSAcpi_ThermalZoneTemperature via WMI.

    The raw value is in tenths of Kelvin:
        °C = (raw / 10) - 273.15
    """
    try:
        import wmi  # type: ignore[import-untyped]  # noqa: PLC0415

        w = wmi.WMI(namespace=r"root\wmi")
        temps = w.MSAcpi_ThermalZoneTemperature()
        if not temps:
            return None

        readings: list[float] = []
        for zone in temps:
            raw = getattr(zone, "CurrentTemperature", None)
            if raw is not None:
                celsius = (float(raw) / 10.0) - 273.15
                if 0.0 < celsius < 150.0:
                    readings.append(celsius)

        if readings:
            return round(max(readings), 1)

    except ImportError:
        log.debug("cpu_temp_wmi_unavailable", reason="wmi package not installed")
    except Exception as exc:
        log.debug("cpu_temp_wmi_failed", error=str(exc))

    # Fallback: try psutil even on Windows (some builds expose sensors)
    return _get_cpu_temp_psutil()


# ── Heartbeat payload helper ───────────────────────────────────────────────────

def build_stats_payload() -> dict[str, Any]:
    """
    Return a dict of live system stats suitable for merging into a heartbeat.

    Keys:
        cpu_temp  : float | None — CPU temperature in Celsius (omitted if None)
        throttle  : bool — True if cpu_temp > THROTTLE_TEMP_C
    """
    payload: dict[str, Any] = {}
    temp = get_cpu_temp_celsius()
    if temp is not None:
        payload["cpu_temp"] = temp
        payload["throttle"] = temp > THROTTLE_TEMP_C
    else:
        payload["throttle"] = False
    return payload


# ── Per-node heat map (Redis) ─────────────────────────────────────────────────

def publish_node_heat_map(redis_client: Any, node_id: str) -> dict[str, Any]:
    """
    Collect thermal + CPU stats for this node and publish to Redis.

    Writes to ``nexus:thermal:<node_id>`` with a TTL of ``THERMAL_KEY_TTL`` seconds.
    If ``cpu_temp > THROTTLE_TEMP_C``, sets ``throttle=True`` so the dispatcher
    can reduce this node's task queue depth.

    Returns the published payload dict.
    """
    import asyncio

    payload = build_stats_payload()
    try:
        import psutil  # noqa: PLC0415
        payload["cpu_percent"] = psutil.cpu_percent(interval=0.1)
        mem = psutil.virtual_memory()
        payload["mem_percent"] = mem.percent
        payload["mem_available_mb"] = round(mem.available / 1024 / 1024, 1)
    except Exception:
        pass

    payload["node_id"] = node_id
    payload["ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    key = f"{THERMAL_KEY_PREFIX}{node_id}"
    serialized = json.dumps(payload)

    try:
        if asyncio.iscoroutinefunction(getattr(redis_client, "set", None)):
            # Async client — caller must await; return payload and let caller handle
            pass
        else:
            redis_client.set(key, serialized, ex=THERMAL_KEY_TTL)
            if payload.get("throttle"):
                log.warning(
                    "node_thermal_throttle_triggered",
                    node_id=node_id,
                    cpu_temp=payload.get("cpu_temp"),
                    threshold=THROTTLE_TEMP_C,
                )
    except Exception as exc:
        log.debug("publish_node_heat_map_redis_failed", error=str(exc))

    return payload


async def async_publish_node_heat_map(redis_client: Any, node_id: str) -> dict[str, Any]:
    """Async version of ``publish_node_heat_map`` for use inside asyncio contexts."""
    import asyncio

    payload = build_stats_payload()
    try:
        import psutil  # noqa: PLC0415
        loop = asyncio.get_event_loop()
        cpu_pct = await loop.run_in_executor(None, lambda: psutil.cpu_percent(interval=0.1))
        payload["cpu_percent"] = cpu_pct
        mem = psutil.virtual_memory()
        payload["mem_percent"] = mem.percent
        payload["mem_available_mb"] = round(mem.available / 1024 / 1024, 1)
    except Exception:
        pass

    payload["node_id"] = node_id
    payload["ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    key = f"{THERMAL_KEY_PREFIX}{node_id}"
    serialized = json.dumps(payload)

    try:
        await redis_client.set(key, serialized, ex=THERMAL_KEY_TTL)
        if payload.get("throttle"):
            log.warning(
                "node_thermal_throttle_triggered",
                node_id=node_id,
                cpu_temp=payload.get("cpu_temp"),
                threshold=THROTTLE_TEMP_C,
            )
    except Exception as exc:
        log.debug("async_publish_node_heat_map_redis_failed", error=str(exc))

    return payload


async def get_cluster_heat_map(redis_client: Any) -> list[dict[str, Any]]:
    """
    Fetch thermal data for all nodes from Redis.

    Scans ``nexus:thermal:*`` keys and returns a list of node thermal payloads,
    sorted by ``cpu_temp`` descending (hottest first).
    """
    results: list[dict[str, Any]] = []
    try:
        cursor = 0
        while True:
            cursor, keys = await redis_client.scan(
                cursor=cursor, match=f"{THERMAL_KEY_PREFIX}*", count=100
            )
            for key in keys:
                raw = await redis_client.get(key)
                if raw:
                    try:
                        data = json.loads(raw)
                        results.append(data)
                    except Exception:
                        pass
            if cursor == 0:
                break
    except Exception as exc:
        log.debug("get_cluster_heat_map_failed", error=str(exc))

    results.sort(key=lambda x: float(x.get("cpu_temp") or 0), reverse=True)
    return results
