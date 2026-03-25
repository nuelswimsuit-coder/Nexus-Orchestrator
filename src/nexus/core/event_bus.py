"""
Global event bus — Redis Pub/Sub channels for Master ↔ Workers ↔ UI decoupling.

UI (e.g. ``node_monitor``) may subscribe to :data:`MONITOR_CHANNEL` for push hints;
core services publish JSON envelopes ``{"event": str, "payload": dict}``.
"""

from __future__ import annotations

import json
from typing import Any

MONITOR_CHANNEL = "nexus:bus:monitor"
WORKER_EVENTS_CHANNEL = "nexus:bus:workers"


def mission_updated_message(*, project: str, task_type: str) -> str:
    return json.dumps(
        {
            "event": "mission_updated",
            "payload": {"mission": project, "task_type": task_type},
        },
        default=str,
    )


async def publish_mission_updated(arq_redis: Any, *, project: str, task_type: str) -> None:
    """Publish on :data:`MONITOR_CHANNEL` using an ARQ/redis asyncio client."""
    await arq_redis.publish(MONITOR_CHANNEL, mission_updated_message(project=project, task_type=task_type))
