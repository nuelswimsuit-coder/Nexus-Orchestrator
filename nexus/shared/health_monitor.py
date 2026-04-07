"""
OpenClaw ↔ Nexus sync health — Redis test heartbeat and master-side watchdog.

Worker (OpenClaw path): publishes a ``Test Heartbeat`` payload to Redis on a
fixed interval (default 30 minutes).

Master: evaluates freshness; if the last heartbeat is older than the stale
threshold (default 60 minutes), surfaces a critical alert in War Room intel
and notifies the admin channel via :class:`NotificationService`.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import structlog

from nexus.shared.notifications.base import Alert, AlertLevel

if TYPE_CHECKING:
    from nexus.shared.notifications.service import NotificationService

log = structlog.get_logger(__name__)

# Redis — single source of truth for last successful OpenClaw→Nexus ping
OPENCLAW_TEST_HEARTBEAT_KEY = "nexus:openclaw:test_heartbeat"
OPENCLAW_TEST_HEARTBEAT_CHANNEL = "nexus:openclaw:test_heartbeat"

# Latch: present while the unhealthy state is active (Telegram dedupe per incident)
OPENCLAW_SYNC_ALERT_LATCH_KEY = "nexus:openclaw:sync_alert_latch"

DEFAULT_HEARTBEAT_INTERVAL_S = 1800  # 30 minutes
DEFAULT_STALE_AFTER_S = 3600  # 60 minutes
DEFAULT_MONITOR_TICK_S = 60
DEFAULT_KEY_TTL_S = 172800  # 48h — value holds ISO ts; TTL avoids orphan keys forever


def _heartbeat_interval_s() -> int:
    return max(60, int(os.getenv("NEXUS_OPENCLAW_HEARTBEAT_INTERVAL_S", str(DEFAULT_HEARTBEAT_INTERVAL_S))))


def _stale_after_s() -> int:
    return max(120, int(os.getenv("NEXUS_OPENCLAW_HEARTBEAT_STALE_S", str(DEFAULT_STALE_AFTER_S))))


def _monitor_tick_s() -> int:
    return max(15, int(os.getenv("NEXUS_OPENCLAW_SYNC_MONITOR_TICK_S", str(DEFAULT_MONITOR_TICK_S))))


def _key_ttl_s() -> int:
    return max(3600, int(os.getenv("NEXUS_OPENCLAW_HEARTBEAT_KEY_TTL_S", str(DEFAULT_KEY_TTL_S))))


def _parse_iso_ts(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None


async def publish_openclaw_test_heartbeat(redis: Any, *, node_id: str | None = None) -> None:
    """
    Write ``Test Heartbeat`` to Redis (and optional pub/sub mirror for subscribers).
    """
    now = datetime.now(timezone.utc)
    nid = (node_id or os.getenv("NODE_ID") or f"worker-{socket.gethostname()}").strip()
    payload = {
        "message": "Test Heartbeat",
        "issued_by": "openclaw-sync",
        "ts": now.isoformat(),
        "node_id": nid,
    }
    body = json.dumps(payload, separators=(",", ":"))
    ex = _key_ttl_s()
    await redis.set(OPENCLAW_TEST_HEARTBEAT_KEY, body, ex=ex)
    try:
        await redis.publish(OPENCLAW_TEST_HEARTBEAT_CHANNEL, body)
    except Exception:
        pass
    log.info("openclaw_test_heartbeat_published", node_id=nid)


async def run_openclaw_test_heartbeat_loop(redis: Any) -> None:
    """Background loop for worker nodes — publish immediately, then every interval."""
    interval = _heartbeat_interval_s()
    while True:
        try:
            await publish_openclaw_test_heartbeat(redis)
        except Exception as exc:
            log.warning("openclaw_test_heartbeat_loop_publish_failed", error=str(exc))
        await asyncio.sleep(interval)


def evaluate_openclaw_sync_from_raw(raw: str | bytes | None, *, now: datetime | None = None) -> dict[str, Any]:
    """
    Build War Room / API slice from stored Redis value (sync helper for tests and API).
    """
    now_utc = now or datetime.now(timezone.utc)
    stale_s = _stale_after_s()
    if raw is None:
        return {
            "healthy": False,
            "last_heartbeat_at": None,
            "seconds_since_heartbeat": None,
            "alert_level": "critical",
            "message": "No OpenClaw test heartbeat in Redis — worker sync path may be down.",
            "stale_threshold_s": stale_s,
        }
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {
            "healthy": False,
            "last_heartbeat_at": None,
            "seconds_since_heartbeat": None,
            "alert_level": "critical",
            "message": "OpenClaw heartbeat key is present but invalid JSON.",
            "stale_threshold_s": stale_s,
        }
    ts = _parse_iso_ts(data.get("ts"))
    if ts is None:
        return {
            "healthy": False,
            "last_heartbeat_at": None,
            "seconds_since_heartbeat": None,
            "alert_level": "critical",
            "message": "OpenClaw heartbeat missing a valid timestamp.",
            "stale_threshold_s": stale_s,
        }
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    age = (now_utc - ts).total_seconds()
    healthy = age <= stale_s
    return {
        "healthy": healthy,
        "last_heartbeat_at": ts.isoformat(),
        "seconds_since_heartbeat": round(age, 1),
        "alert_level": "ok" if healthy else "critical",
        "message": (
            "OpenClaw ↔ Nexus sync OK (test heartbeat fresh)."
            if healthy
            else (
                f"RED ALERT: OpenClaw test heartbeat stale ({int(age)}s > {stale_s}s). "
                "Verify worker / Redis / OpenClaw path."
            )
        ),
        "stale_threshold_s": stale_s,
    }


async def load_openclaw_nexus_sync_status(redis: Any) -> dict[str, Any]:
    raw = await redis.get(OPENCLAW_TEST_HEARTBEAT_KEY)
    return evaluate_openclaw_sync_from_raw(raw)


async def _notify_openclaw_sync_critical(
    notifier: NotificationService,
    detail: dict[str, Any],
) -> None:
    body_lines = [
        detail.get("message", "OpenClaw ↔ Nexus sync failure."),
        f"Last heartbeat: {detail.get('last_heartbeat_at') or 'never'}",
        f"Stale threshold: {detail.get('stale_threshold_s')}s",
    ]
    await notifier.notify(
        Alert(
            title="🚨 OpenClaw ↔ Nexus sync — RED ALERT",
            body="\n".join(body_lines),
            level=AlertLevel.CRITICAL,
            metadata={"component": "openclaw_sync", "war_room": "true"},
        )
    )


async def run_openclaw_health_monitor_loop(
    redis: Any,
    notifier: NotificationService | None,
) -> None:
    """
    Master-side loop: detect stale/missing heartbeat, set latch + Telegram once per incident.
    """
    tick = _monitor_tick_s()
    log.info(
        "openclaw_health_monitor_started",
        tick_s=tick,
        stale_after_s=_stale_after_s(),
    )
    while True:
        try:
            status = await load_openclaw_nexus_sync_status(redis)
            healthy = bool(status.get("healthy"))
            if healthy:
                try:
                    await redis.delete(OPENCLAW_SYNC_ALERT_LATCH_KEY)
                except Exception:
                    pass
            else:
                latched = False
                try:
                    raw_latch = await redis.get(OPENCLAW_SYNC_ALERT_LATCH_KEY)
                    latched = raw_latch is not None and str(raw_latch).strip() != ""
                except Exception:
                    pass
                if not latched:
                    try:
                        await redis.set(OPENCLAW_SYNC_ALERT_LATCH_KEY, "1", ex=_stale_after_s() * 4)
                    except Exception:
                        pass
                    if notifier is not None:
                        await _notify_openclaw_sync_critical(notifier, status)
                    log.warning(
                        "openclaw_sync_unhealthy",
                        last_heartbeat=status.get("last_heartbeat_at"),
                        seconds_since=status.get("seconds_since_heartbeat"),
                    )
        except Exception as exc:
            log.warning("openclaw_health_monitor_tick_error", error=str(exc))
        await asyncio.sleep(tick)
