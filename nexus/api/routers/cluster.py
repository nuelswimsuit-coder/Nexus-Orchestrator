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

import asyncio
import json
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from time import perf_counter

import structlog
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from redis.asyncio import Redis

from nexus.api.dependencies import RedisDep
from nexus.api.schemas import (
    ClusterHealthNode,
    ClusterHealthResponse,
    ClusterStatusResponse,
    FleetAssetRow,
    FleetAssetsResponse,
    NodeStatus,
    QueueStats,
    ResourceCaps,
    TargetHeatCell,
)
from nexus.api.services.telefix_bridge import get_fleet_group_assets
from nexus.shared.config import settings
from nexus.shared.fleet_redis import (
    FLEET_SCAN_CHANNEL,
    FLEET_SCAN_STATUS_KEY,
    get_fleet_counter_snapshot,
    load_latest_fleet_audit,
)
from nexus.shared.schemas import FleetScanEvent, NodeHeartbeat, NodeRole
from nexus.shared.swarm_signals import SWARM_SIGNAL_KEY

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/cluster", tags=["cluster"])

# Heartbeat keys are stored as  nexus:heartbeat:<node_id>
HEARTBEAT_KEY_PREFIX = "nexus:heartbeat:"
WAR_ROOM_CACHE_KEY = "nexus:war_room:intel"
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
                        # Phase 4 extended hardware
                        motherboard=hb.motherboard,
                        cpu_temp_c=hb.cpu_temp_c,
                        display_name=hb.display_name,
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


def _redis_key_str(key: bytes | str) -> str:
    return key.decode() if isinstance(key, (bytes, bytearray)) else str(key)


async def _timed_heartbeat_get(redis: Redis, key: bytes | str) -> tuple[str, bytes | str | None, float]:
    k = _redis_key_str(key)
    t0 = perf_counter()
    raw = await redis.get(key)
    ms = (perf_counter() - t0) * 1000.0
    return k, raw, ms


def _node_health_status(online: bool, probe_ms: float) -> str:
    if not online:
        return "offline"
    if probe_ms > 250.0:
        return "degraded"
    return "ok"


async def _load_target_heatmap(redis: Redis) -> list[TargetHeatCell]:
    cells = [
        TargetHeatCell(id="btc_regulation", label="BTC Regulation", intensity=12.0),
        TargetHeatCell(id="whale_alerts", label="Whale Alerts", intensity=8.0),
    ]
    try:
        raw = await redis.get(WAR_ROOM_CACHE_KEY)
        if not raw:
            return cells
        if isinstance(raw, bytes):
            raw = raw.decode()
        data = json.loads(raw)
        conf = float(data.get("master_confidence_pct", 0.0) or 0.0)
        whale_hits = int(data.get("swarm_whale_hits", 0) or 0)
        cells[0].intensity = round(max(0.0, min(100.0, conf * 0.9)), 1)
        cells[1].intensity = round(max(0.0, min(100.0, min(whale_hits * 6.5, 100.0))), 1)
    except Exception:
        pass
    return cells


@router.get("/health", response_model=ClusterHealthResponse, summary="Redis + per-node probe latencies for fleet grid")
async def get_cluster_health(redis: RedisDep) -> ClusterHealthResponse:
    """
    Measures Redis PING RTT and parallel GET latency for each ``nexus:heartbeat:*`` key.
    Also returns recent swarm signal lines and a two-cell target heatmap derived from
    cached war-room intel when available.
    """
    redis_ok = False
    redis_ping_ms: float | None = None
    try:
        t0 = perf_counter()
        await redis.ping()
        redis_ping_ms = round((perf_counter() - t0) * 1000.0, 3)
        redis_ok = True
    except Exception as exc:
        log.warning("cluster_health_redis_ping_failed", error=str(exc))

    hb_keys: list[bytes | str] = []
    if redis_ok:
        try:
            cursor = 0
            pattern = f"{HEARTBEAT_KEY_PREFIX}*".encode()
            while True:
                cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=128)
                hb_keys.extend(keys)
                if cursor == 0:
                    break
        except Exception as exc:
            log.warning("cluster_health_scan_failed", error=str(exc))

    triples: list[tuple[str, bytes | str | None, float]] = []
    if hb_keys and redis_ok:
        try:
            triples = await asyncio.gather(*[_timed_heartbeat_get(redis, k) for k in hb_keys])
        except Exception as exc:
            log.warning("cluster_health_gather_failed", error=str(exc))

    parsed: list[tuple[NodeHeartbeat, str, float]] = []
    for _k, raw, probe_ms in triples:
        if raw is None:
            continue
        try:
            if isinstance(raw, bytes):
                raw = raw.decode()
            hb = NodeHeartbeat.model_validate_json(raw)
            parsed.append((hb, _node_health_status(True, probe_ms), probe_ms))
        except Exception as exc:
            log.warning("cluster_health_heartbeat_parse_error", error=str(exc))

    parsed.sort(key=lambda t: (t[0].role != NodeRole.MASTER, t[0].node_id))

    nodes_out: list[ClusterHealthNode] = []
    for hb, st, probe_ms in parsed:
        is_master = hb.role == NodeRole.MASTER
        os_lower = (hb.os_info or "").lower()
        is_windows = "windows" in os_lower
        if is_master:
            label = "מחשב מאסטר עובד ומנהל בהתאמה"
        else:
            label = "לפטופ ווינדוס עובד" if is_windows else "לפטופ לינוקס עובד"
        nodes_out.append(
            ClusterHealthNode(
                node_id=hb.node_id,
                role=hb.role,
                online=True,
                status=st,
                probe_latency_ms=round(probe_ms, 3),
                cpu_percent=float(hb.cpu_percent),
                ram_used_mb=float(hb.ram_used_mb),
                active_jobs=int(hb.active_jobs),
                last_seen=hb.timestamp,
                local_ip=hb.local_ip or "unknown",
                cpu_model=hb.cpu_model or "unknown",
                gpu_model=hb.gpu_model or "N/A",
                ram_total_mb=float(hb.ram_total_mb or 0.0),
                os_info=hb.os_info or "unknown",
                display_label=label,
                # Phase 4 extended hardware
                motherboard=hb.motherboard or "N/A",
                cpu_temp_c=float(hb.cpu_temp_c if hb.cpu_temp_c is not None else -1.0),
                display_name=hb.display_name or "",
            )
        )

    workers_online = sum(1 for n in nodes_out if n.role == NodeRole.WORKER and n.online)

    swarm_activity: list[str] = []
    if redis_ok:
        try:
            lines = await redis.lrange(SWARM_SIGNAL_KEY, 0, 39)
            for line in lines or []:
                swarm_activity.append(line.decode() if isinstance(line, bytes) else str(line))
        except Exception as exc:
            log.debug("cluster_health_swarm_read_failed", error=str(exc))

    targets = await _load_target_heatmap(redis) if redis_ok else [
        TargetHeatCell(id="btc_regulation", label="BTC Regulation", intensity=0.0),
        TargetHeatCell(id="whale_alerts", label="Whale Alerts", intensity=0.0),
    ]

    return ClusterHealthResponse(
        redis_ok=redis_ok,
        redis_ping_ms=redis_ping_ms,
        nodes=nodes_out,
        workers_online=workers_online,
        swarm_activity=swarm_activity,
        targets=targets,
        timestamp=datetime.now(timezone.utc),
    )


@router.get(
    "/fleet/assets",
    response_model=FleetAssetsResponse,
    summary="Fleet groups, member counts, owning session, and Redis mapper totals",
)
async def get_fleet_assets(redis: RedisDep) -> FleetAssetsResponse:
    """
    Sorted list of managed groups from telefix.db (via telefix_bridge), merged with
    live ``total_managed_members`` / ``total_premium_members`` counters in Redis.
    """
    raw = await get_fleet_group_assets()
    counters = await get_fleet_counter_snapshot(redis)
    audit = await load_latest_fleet_audit(redis)

    groups = [
        FleetAssetRow(
            group_id=g["group_id"],
            title=(g.get("group_name") or g["group_id"] or "").strip() or str(g["group_id"]),
            member_count=int(g.get("member_count") or 0),
            premium_members=int(g.get("premium_count") or 0),
            session_owner=str(g.get("owner_session") or ""),
            status=str(g.get("status") or "MONITORING"),
            last_automation=g.get("last_automation"),
        )
        for g in raw.get("groups") or []
    ]

    queried = raw.get("queried_at")
    try:
        queried_at = (
            datetime.fromisoformat(queried.replace("Z", "+00:00"))
            if isinstance(queried, str)
            else datetime.now(timezone.utc)
        )
    except Exception:
        queried_at = datetime.now(timezone.utc)

    return FleetAssetsResponse(
        groups=groups,
        total_managed_members=counters["total_managed_members"],
        total_premium_members=counters["total_premium_members"],
        latest_audit=audit,
        db_available=bool(raw.get("db_available")),
        queried_at=queried_at,
    )


@router.get(
    "/fleet/scan/status",
    summary="Latest fleet scan event (for polling without SSE)",
)
async def get_fleet_scan_status(redis: RedisDep) -> dict:
    raw = await redis.get(FLEET_SCAN_STATUS_KEY)
    if not raw:
        return {"event": None}
    if isinstance(raw, bytes):
        raw = raw.decode()
    try:
        ev = FleetScanEvent.model_validate_json(raw)
        return {"event": ev.model_dump(mode="json")}
    except Exception:
        return {"event": None, "raw": raw}


@router.get(
    "/fleet/scan/stream",
    summary="SSE: real-time fleet mapper / scraper scan phase updates",
    response_class=StreamingResponse,
)
async def stream_fleet_scan(request: Request) -> StreamingResponse:
    """
    Subscribes to Redis ``nexus:fleet:scan`` and streams JSON events (started /
    progress / ended) for dashboard progress UI.

    Uses a dedicated Redis connection so pub/sub mode does not interfere with
    the API pool used for request-scoped commands.
    """
    from redis.asyncio import from_url

    redis_url = settings.redis_url

    async def _generator() -> AsyncGenerator[str, None]:
        client = from_url(redis_url, decode_responses=True)
        pubsub = None
        try:
            snap = await client.get(FLEET_SCAN_STATUS_KEY)
            if snap:
                if isinstance(snap, bytes):
                    snap = snap.decode()
                yield f"data: {snap}\n\n"
            pubsub = client.pubsub()
            await pubsub.subscribe(FLEET_SCAN_CHANNEL)
            while True:
                if await request.is_disconnected():
                    break
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=30.0,
                )
                if message and message.get("type") == "message":
                    data = message.get("data")
                    if isinstance(data, bytes):
                        data = data.decode()
                    yield f"data: {data}\n\n"
                    try:
                        ev = json.loads(data)
                        if ev.get("phase") == "ended":
                            break
                    except Exception:
                        pass
                else:
                    yield ": keep-alive\n\n"
        finally:
            if pubsub is not None:
                try:
                    await pubsub.unsubscribe(FLEET_SCAN_CHANNEL)
                except Exception:
                    pass
            await client.aclose()

    return StreamingResponse(
        _generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
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
