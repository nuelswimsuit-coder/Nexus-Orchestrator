"""
Redis helpers for fleet mapper metrics, audit snapshots, and scan events.

Used by the Master :class:`~nexus.master.dispatcher.Dispatcher`, worker tasks
(``telegram.super_scrape``, ``telegram.auto_scrape``), and the API SSE stream.
"""

from __future__ import annotations

from typing import Any

import structlog

from nexus.shared.memory_cache import TTLMemoryCache
from nexus.shared.schemas import (
    FleetAuditResults,
    FleetScanEvent,
    FleetScanPhase,
)

log = structlog.get_logger(__name__)

_FLEET_COUNTER_MEM = TTLMemoryCache[dict[str, int]](max_entries=8)
_FLEET_COUNTER_MEM_TTL_S = 2.0
_FLEET_COUNTER_MEM_KEY = "nexus:fleet:counter_snapshot:mem"

_FLEET_AUDIT_MEM = TTLMemoryCache[FleetAuditResults](max_entries=4)
_FLEET_AUDIT_MEM_TTL_S = 12.0
_FLEET_AUDIT_MEM_KEY = "nexus:fleet:audit:latest:mem"

# Redis keys (global counters — reset when a fleet scan task is dispatched from Master).
# Unprefixed aliases exist for external tooling that expects the literal names from the spec.
REDIS_TOTAL_MANAGED_MEMBERS = "nexus:fleet:total_managed_members"
REDIS_TOTAL_PREMIUM_MEMBERS = "nexus:fleet:total_premium_members"
REDIS_ALIAS_TOTAL_MANAGED_MEMBERS = "total_managed_members"
REDIS_ALIAS_TOTAL_PREMIUM_MEMBERS = "total_premium_members"
REDIS_FLEET_AUDIT_LATEST = "nexus:fleet:audit:latest"
FLEET_SCAN_CHANNEL = "nexus:fleet:scan"
FLEET_SCAN_STATUS_KEY = "nexus:fleet:scan:status"
FLEET_AUDIT_LATEST_TTL_S = 86400 * 7
FLEET_SCAN_STATUS_TTL_S = 7200

# Task types that represent a fleet-wide scan / mapper run (master publishes start)
FLEET_SCAN_TASK_TYPES: frozenset[str] = frozenset(
    {
        "telegram.auto_scrape",
        "telegram.super_scrape",
        "openclaw.browser_scrape",
        "scraper.openclaw",
    }
)


async def reset_fleet_member_counters(redis: Any) -> None:
    """Zero global member counters at the start of a master-dispatched fleet scan."""
    if redis is None:
        return
    _FLEET_COUNTER_MEM.delete(_FLEET_COUNTER_MEM_KEY)
    await redis.set(REDIS_TOTAL_MANAGED_MEMBERS, "0")
    await redis.set(REDIS_TOTAL_PREMIUM_MEMBERS, "0")
    await redis.set(REDIS_ALIAS_TOTAL_MANAGED_MEMBERS, "0")
    await redis.set(REDIS_ALIAS_TOTAL_PREMIUM_MEMBERS, "0")


async def fleet_mapper_record_group(
    redis: Any,
    *,
    managed_members: int,
    premium_members: int = 0,
) -> tuple[int, int]:
    """
    Called when the Mapper discovers a group with known member counts.

    Returns ``(new_managed_total, new_premium_total)`` from Redis after increment.
    """
    if redis is None:
        return (0, 0)
    m = max(0, int(managed_members))
    p = max(0, int(premium_members))
    managed = int(await redis.incrby(REDIS_TOTAL_MANAGED_MEMBERS, m))
    premium = int(await redis.incrby(REDIS_TOTAL_PREMIUM_MEMBERS, p))
    await redis.set(REDIS_ALIAS_TOTAL_MANAGED_MEMBERS, str(managed))
    await redis.set(REDIS_ALIAS_TOTAL_PREMIUM_MEMBERS, str(premium))
    _FLEET_COUNTER_MEM.delete(_FLEET_COUNTER_MEM_KEY)
    return (managed, premium)


async def publish_fleet_scan_event(redis: Any, event: FleetScanEvent) -> None:
    """Fan out to pub/sub and persist the latest event for polling / SSE bootstrap."""
    if redis is None:
        return
    payload = event.model_dump_json()
    try:
        await redis.publish(FLEET_SCAN_CHANNEL, payload)
    except Exception as exc:
        log.warning("fleet_scan_publish_failed", error=str(exc))
    try:
        await redis.set(FLEET_SCAN_STATUS_KEY, payload, ex=FLEET_SCAN_STATUS_TTL_S)
    except Exception as exc:
        log.warning("fleet_scan_status_set_failed", error=str(exc))


async def persist_fleet_audit_latest(redis: Any, results: FleetAuditResults) -> None:
    """Store the latest structured audit snapshot in Redis (JSON)."""
    if redis is None:
        return
    await redis.set(
        REDIS_FLEET_AUDIT_LATEST,
        results.model_dump_json(),
        ex=FLEET_AUDIT_LATEST_TTL_S,
    )
    _FLEET_AUDIT_MEM.set(_FLEET_AUDIT_MEM_KEY, results, _FLEET_AUDIT_MEM_TTL_S)


def parse_fleet_audit_from_task_output(output: Any) -> FleetAuditResults | None:
    """Extract ``FleetAuditResults`` from a worker result dict, if present."""
    if not isinstance(output, dict):
        return None
    raw = output.get("fleet_audit_results") or output.get("fleet_audit")
    if raw is None:
        return None
    try:
        if isinstance(raw, str):
            return FleetAuditResults.model_validate_json(raw)
        return FleetAuditResults.model_validate(raw)
    except Exception:
        log.debug("fleet_audit_parse_skipped")
        return None


async def persist_fleet_audit_sqlite(results: FleetAuditResults) -> None:
    """Append one row to telefix-side ``nexus_fleet_audit`` (best-effort)."""
    try:
        from nexus.api.services import telefix_bridge

        await telefix_bridge.append_fleet_audit_run(results.model_dump(mode="json"))
    except Exception as exc:
        log.warning("fleet_audit_sqlite_append_failed", error=str(exc))


async def load_latest_fleet_audit(redis: Any) -> FleetAuditResults | None:
    if redis is None:
        return None
    hit = _FLEET_AUDIT_MEM.get(_FLEET_AUDIT_MEM_KEY)
    if hit is not None:
        return hit
    raw = await redis.get(REDIS_FLEET_AUDIT_LATEST)
    if not raw:
        return None
    try:
        if isinstance(raw, bytes):
            raw = raw.decode()
        parsed = FleetAuditResults.model_validate_json(raw)
        _FLEET_AUDIT_MEM.set(_FLEET_AUDIT_MEM_KEY, parsed, _FLEET_AUDIT_MEM_TTL_S)
        return parsed
    except Exception:
        return None


async def get_fleet_counter_snapshot(redis: Any) -> dict[str, int]:
    """Return current Redis totals for managed / premium member counters."""
    if redis is None:
        return {"total_managed_members": 0, "total_premium_members": 0}
    snap_hit = _FLEET_COUNTER_MEM.get(_FLEET_COUNTER_MEM_KEY)
    if snap_hit is not None:
        return snap_hit
    m = await redis.get(REDIS_TOTAL_MANAGED_MEMBERS)
    if m is None:
        m = await redis.get(REDIS_ALIAS_TOTAL_MANAGED_MEMBERS)
    p = await redis.get(REDIS_TOTAL_PREMIUM_MEMBERS)
    if p is None:
        p = await redis.get(REDIS_ALIAS_TOTAL_PREMIUM_MEMBERS)
    out = {
        "total_managed_members": int(m or 0),
        "total_premium_members": int(p or 0),
    }
    _FLEET_COUNTER_MEM.set(_FLEET_COUNTER_MEM_KEY, out, _FLEET_COUNTER_MEM_TTL_S)
    return out
