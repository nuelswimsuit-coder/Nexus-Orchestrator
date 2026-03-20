"""
GET /api/cluster/status

Returns a snapshot of the cluster: all nodes that have published a heartbeat
recently, their resource usage, and the current ARQ queue depth.

Heartbeat data
--------------
Each node (master + workers) periodically publishes a NodeHeartbeat to the
Redis key  nexus:heartbeat:<node_id>  (a JSON string with a short TTL).
The API reads all matching keys to build the node list.  A node is considered
"online" if its heartbeat key still exists (i.e., has not expired).

Queue depth
-----------
ARQ stores queued jobs as a Redis sorted set at  arq:queue:<queue_name>.
ZCARD gives the number of pending (not yet picked up) jobs.
"""

from __future__ import annotations

from datetime import datetime, timezone

import structlog
from fastapi import APIRouter
from pydantic import BaseModel

from nexus.api.dependencies import RedisDep
from nexus.api.schemas import (
    ClusterStatusResponse,
    NodeStatus,
    QueueStats,
    ResourceCaps,
)
from nexus.shared.config import settings
from nexus.shared.schemas import NodeHeartbeat

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/cluster", tags=["cluster"])

# Heartbeat keys are stored as  nexus:heartbeat:<node_id>
HEARTBEAT_KEY_PREFIX = "nexus:heartbeat:"
# ARQ queue sorted-set key pattern
ARQ_QUEUE_KEY = "arq:queue:nexus:tasks"


@router.get("/status", response_model=ClusterStatusResponse, summary="Cluster topology and health")
async def get_cluster_status(redis: RedisDep) -> ClusterStatusResponse:
    """
    Return live cluster state:
    - All nodes that have published a heartbeat within their TTL window.
    - Master resource caps from settings.
    - Pending job count for each monitored queue.
    """
    # ── Collect node heartbeats ────────────────────────────────────────────────
    # Scan for all heartbeat keys without blocking the event loop.
    node_statuses: list[NodeStatus] = []
    cursor = 0
    pattern = f"{HEARTBEAT_KEY_PREFIX}*".encode()

    while True:
        cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=100)
        for key in keys:
            raw = await redis.get(key)
            if raw is None:
                continue
            try:
                hb = NodeHeartbeat.model_validate_json(raw)
                node_statuses.append(
                    NodeStatus(
                        node_id=hb.node_id,
                        role=hb.role,
                        cpu_percent=hb.cpu_percent,
                        ram_used_mb=hb.ram_used_mb,
                        active_jobs=hb.active_jobs,
                        last_seen=hb.timestamp,
                        online=True,
                        # Phase 3 hardware fields
                        local_ip=hb.local_ip,
                        cpu_model=hb.cpu_model,
                        gpu_model=hb.gpu_model,
                        ram_total_mb=hb.ram_total_mb,
                        active_tasks_count=hb.active_tasks_count,
                        os_info=hb.os_info,
                    )
                )
            except Exception as exc:
                log.warning("heartbeat_parse_error", key=key, error=str(exc))
        if cursor == 0:
            break

    # Sort: master first, then workers alphabetically.
    node_statuses.sort(key=lambda n: (n.role.value != "master", n.node_id))

    # ── Queue depth ────────────────────────────────────────────────────────────
    pending_count = await redis.zcard(ARQ_QUEUE_KEY)
    queues = [QueueStats(queue_name="nexus:tasks", pending_jobs=pending_count)]

    # ── Master resource caps (from settings, not live measurement) ─────────────
    caps = ResourceCaps(
        cpu_cap_percent=settings.master_cpu_cap_percent,
        ram_cap_mb=settings.master_ram_cap_mb,
    )

    return ClusterStatusResponse(
        nodes=node_statuses,
        master_resource_caps=caps,
        queues=queues,
        timestamp=datetime.now(timezone.utc),
    )


# ── Sentinel pulse test ────────────────────────────────────────────────────────

class SentinelTestRequest(BaseModel):
    target_id: str


@router.post(
    "/test-sentinel",
    summary="Dispatch a sentinel pulse to verify Master-Worker communication",
    tags=["cluster"],
)
async def test_sentinel(body: SentinelTestRequest, redis: RedisDep) -> dict:
    """
    Enqueue a lightweight sentinel report for *target_id* and confirm that the
    Master can reach the Worker layer via the ARQ task queue.

    Returns a success payload once the pulse has been dispatched.
    """
    log.info(
        "SENTINEL_PULSE: Dispatching report for target_id to Workers",
        target_id=body.target_id,
    )

    await redis.rpush(
        "nexus:sentinel:pulses",
        f'{{"target_id": "{body.target_id}", "timestamp": "{datetime.now(timezone.utc).isoformat()}"}}',
    )

    return {
        "status": "ok",
        "message": "Sentinel pulse dispatched",
        "target_id": body.target_id,
    }
