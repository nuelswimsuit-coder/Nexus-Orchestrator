"""
Command & Control — Redis pub/sub fan-out for WebSocket clients.

Workers and the master publish JSON lines to ``nexus:cc:events``; the API
WebSocket handler subscribes and forwards them to connected dashboards.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import structlog

log = structlog.get_logger(__name__)

CC_EVENTS_CHANNEL = "nexus:cc:events"


async def publish_cc_event(
    redis: Any,
    event_type: str,
    payload: dict[str, Any] | None = None,
) -> None:
    """Best-effort publish; never raises to callers."""
    if redis is None:
        return
    try:
        msg = json.dumps(
            {
                "type": event_type,
                "payload": payload or {},
                "ts": datetime.now(timezone.utc).isoformat(),
            },
            default=str,
        )
        await redis.publish(CC_EVENTS_CHANNEL, msg)
    except Exception as exc:
        log.debug("cc_event_publish_failed", error=str(exc), event_type=event_type)
