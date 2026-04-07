"""
Swarm diagnostic logs — Redis pub/sub ``nexus:swarm:logs``.

OpenClaw self-improvement and operators subscribe here; payloads are JSON objects.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import structlog

log = structlog.get_logger(__name__)

SWARM_LOGS_CHANNEL = "nexus:swarm:logs"

ISSUE_PARROT_BUG = "Parrot Bug"
ISSUE_HALLUCINATION = "Hallucination"
ISSUE_OPENCLAW_VERIFY = "OpenClawVerify"


async def publish_swarm_log_event(redis: Any, payload: dict[str, Any]) -> None:
    """Best-effort publish; never raises."""
    if redis is None:
        return
    try:
        body = {
            **payload,
            "ts": payload.get("ts") or datetime.now(timezone.utc).isoformat(),
        }
        await redis.publish(SWARM_LOGS_CHANNEL, json.dumps(body, ensure_ascii=False))
    except Exception as exc:
        log.debug("swarm_logs_publish_failed", error=str(exc))
