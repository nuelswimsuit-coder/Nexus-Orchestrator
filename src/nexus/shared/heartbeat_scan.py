"""
Load live NodeHeartbeat payloads from Redis (SCAN ``nexus:heartbeat:*``).

Used by the cluster API, swarm snapshot ``nexus:nodes:all``, and Sentinel.
"""

from __future__ import annotations

import structlog
from typing import Any

from nexus.shared.schemas import NodeHeartbeat

log = structlog.get_logger(__name__)

HEARTBEAT_KEY_PREFIX = "nexus:heartbeat:"


async def load_live_node_heartbeats(redis: Any) -> list[NodeHeartbeat]:
    """Return all non-expired heartbeats (order undefined)."""
    out: list[NodeHeartbeat] = []
    cursor = 0
    pattern = f"{HEARTBEAT_KEY_PREFIX}*".encode()

    while True:
        cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=100)
        for key in keys:
            raw = await redis.get(key)
            if raw is None:
                continue
            try:
                out.append(NodeHeartbeat.model_validate_json(raw))
            except Exception as exc:
                log.warning("heartbeat_scan_parse_error", key=key, error=str(exc))
        if cursor == 0:
            break

    return out
