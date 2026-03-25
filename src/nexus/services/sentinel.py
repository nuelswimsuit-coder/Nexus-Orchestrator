"""
Sentinel heartbeat helpers — LAN mesh visibility (master, fixed Linux worker IP,
Windows gaming worker, and any other laptop publishing ``nexus:heartbeat:*``).
"""

from __future__ import annotations

import os
from typing import Any

from nexus.shared.heartbeat_scan import HEARTBEAT_KEY_PREFIX, load_live_node_heartbeats
from nexus.shared.schemas import NodeHeartbeat, NodeRole

# Default anchors: Windows master loopback + Linux worker on LAN (override via env).
DEFAULT_EXPECTED_IPS = ("127.0.0.1", "10.100.102.20")

WINDOWS_HEARTBEAT_KEY_DEFAULT = "nexus:heartbeat:worker-windows"


def expected_swarm_ips() -> frozenset[str]:
    """IPs Sentinel treats as required anchors (comma-separated env extends defaults)."""
    ips = {ip.strip() for ip in DEFAULT_EXPECTED_IPS if ip.strip()}
    extra = os.getenv("SENTINEL_EXPECTED_NODE_IPS", "")
    for part in extra.split(","):
        p = part.strip()
        if p:
            ips.add(p)
    return frozenset(ips)


def _normalise_ip(ip: str) -> str:
    return (ip or "").strip().lower()


def is_master_slot(hb: NodeHeartbeat) -> bool:
    """Master process (Windows host) — role or canonical node id."""
    if hb.role == NodeRole.MASTER:
        return True
    return hb.node_id.lower() in ("master", "nexus-master")


def is_linux_worker_anchor(hb: NodeHeartbeat, anchor_ip: str | None = None) -> bool:
    """Linux worker at the configured LAN address."""
    target = (anchor_ip or os.getenv("SENTINEL_LINUX_WORKER_IP", "10.100.102.20")).strip()
    return _normalise_ip(hb.local_ip) == _normalise_ip(target)


def is_loopback_master_ip(hb: NodeHeartbeat) -> bool:
    ip = _normalise_ip(hb.local_ip)
    return ip in ("127.0.0.1", "::1", "localhost")


async def scan_cluster_heartbeats(redis: Any) -> list[NodeHeartbeat]:
    """All live heartbeats (any laptop on the LAN that publishes to Redis)."""
    return await load_live_node_heartbeats(redis)


def windows_worker_heartbeat_online(
    heartbeats: list[NodeHeartbeat],
    *,
    explicit_redis_key_hit: bool,
) -> bool:
    """
    True if a Windows-leaning worker is alive: explicit legacy Redis key, or any
    worker heartbeat that looks like Windows and is not the Linux anchor IP.
    """
    if explicit_redis_key_hit:
        return True
    for hb in heartbeats:
        if hb.role != NodeRole.WORKER:
            continue
        if is_linux_worker_anchor(hb):
            continue
        nid = hb.node_id.lower()
        if "windows" in nid:
            return True
        osi = (hb.os_info or "").lower()
        if "windows" in osi:
            return True
    return False


async def redis_has_windows_worker_key(redis: Any) -> bool:
    k = os.getenv("SENTINEL_WINDOWS_HEARTBEAT_KEY", WINDOWS_HEARTBEAT_KEY_DEFAULT).strip() or WINDOWS_HEARTBEAT_KEY_DEFAULT
    if not k.startswith(HEARTBEAT_KEY_PREFIX):
        k = f"{HEARTBEAT_KEY_PREFIX}{k}"
    try:
        return (await redis.get(k)) is not None
    except Exception:
        return False


def summarise_swarm_peers_from_list(hbs: list[NodeHeartbeat]) -> dict[str, Any]:
    """In-memory summary after a single SCAN (avoids duplicate Redis round-trips)."""
    ips = {_normalise_ip(h.local_ip) for h in hbs if _normalise_ip(h.local_ip)}
    expected = expected_swarm_ips()
    masters = [h for h in hbs if is_master_slot(h)]
    workers = [h for h in hbs if h.role == NodeRole.WORKER]
    return {
        "heartbeat_count": len(hbs),
        "master_count": len(masters),
        "worker_count": len(workers),
        "local_ips": sorted(ips),
        "expected_ips": sorted(expected),
        "expected_ips_seen": sorted(expected & ips),
        "expected_ips_missing": sorted(expected - ips),
        "linux_anchor_present": any(is_linux_worker_anchor(h) for h in hbs),
        "loopback_master_ip_present": any(is_loopback_master_ip(h) for h in hbs),
    }


async def summarise_swarm_peers(redis: Any) -> dict[str, Any]:
    """Snapshot for ad-hoc callers (performs one SCAN)."""
    hbs = await scan_cluster_heartbeats(redis)
    return summarise_swarm_peers_from_list(hbs)
