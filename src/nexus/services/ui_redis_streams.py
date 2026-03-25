"""
Push dashboard-friendly snapshots to Redis for the Ultra-Data pipeline.

Keys (string JSON, TTL refreshed each tick)
------------------------------------------
nexus:ui:scrapes   — file counts under ``vault/data/scrapes`` (and optional env override).
nexus:ui:swarm     — CPU / RAM / optional temp for the master + every ``nexus:heartbeat:*`` node.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

log = structlog.get_logger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
REDIS_KEY_SCRAPES = "nexus:ui:scrapes"
REDIS_KEY_SWARM = "nexus:ui:swarm"
TTL_S = 120
TICK_S = 5.0


def _scrapes_root() -> Path:
    raw = (os.getenv("NEXUS_UI_SCRAPES_DIR") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return (REPO_ROOT / "vault" / "data" / "scrapes").resolve()


def _count_scrape_files(root: Path) -> dict[str, Any]:
    if not root.is_dir():
        return {
            "root": str(root),
            "exists": False,
            "total_files": 0,
            "by_subdir": {},
        }
    by_sub: dict[str, int] = {}
    total = 0
    for dirpath, _dirnames, filenames in os.walk(root):
        sub = str(Path(dirpath).relative_to(root))
        if sub == ".":
            key = "_root"
        else:
            key = sub.replace("\\", "/")
        n = len([f for f in filenames if f and not f.startswith(".")])
        if n:
            by_sub[key] = by_sub.get(key, 0) + n
        total += n
    return {
        "root": str(root),
        "exists": True,
        "total_files": total,
        "by_subdir": by_sub,
    }


def _master_temp_c() -> float | None:
    try:
        import psutil

        t = psutil.sensors_temperatures()
        if not t:
            return None
        for _label, entries in t.items():
            for e in entries:
                if e.current and e.current > 0:
                    return round(float(e.current), 2)
    except Exception:
        pass
    return None


def _master_metrics() -> dict[str, Any]:
    import psutil

    mem = psutil.virtual_memory()
    return {
        "node_id": os.getenv("NEXUS_NODE_ID", "master"),
        "role": "master",
        "cpu_percent": round(float(psutil.cpu_percent(interval=None)), 2),
        "ram_used_mb": round(mem.used / (1024 * 1024), 1),
        "ram_total_mb": round(mem.total / (1024 * 1024), 1),
        "cpu_temp_c": _master_temp_c(),
        "os_info": f"{sys.platform}",
    }


async def ui_data_tick(redis: Any) -> None:
    """Single publish cycle (scrapes + swarm)."""
    now = datetime.now(timezone.utc).isoformat()
    root = _scrapes_root()
    scrape_payload = {
        "updated_at": now,
        **_count_scrape_files(root),
    }
    await redis.set(REDIS_KEY_SCRAPES, json.dumps(scrape_payload, ensure_ascii=False), ex=TTL_S)

    from nexus.shared.heartbeat_scan import load_live_node_heartbeats

    nodes: list[dict[str, Any]] = []
    try:
        beats = await load_live_node_heartbeats(redis)
        for hb in beats:
            nodes.append(
                {
                    "node_id": hb.node_id,
                    "role": str(hb.role.value),
                    "cpu_percent": hb.cpu_percent,
                    "ram_used_mb": hb.ram_used_mb,
                    "ram_total_mb": hb.ram_total_mb,
                    "local_ip": hb.local_ip,
                    "os_info": hb.os_info,
                    "cpu_temp_c": None,
                }
            )
    except Exception as exc:
        log.warning("ui_swarm_heartbeat_scan_failed", error=str(exc))

    swarm_payload = {
        "updated_at": now,
        "master": _master_metrics(),
        "nodes": nodes,
    }
    await redis.set(REDIS_KEY_SWARM, json.dumps(swarm_payload, ensure_ascii=False), ex=TTL_S)


async def run_ui_streams_loop(redis: Any, interval_s: float = TICK_S) -> None:
    log.info("ui_redis_streams_started", scrapes_key=REDIS_KEY_SCRAPES, swarm_key=REDIS_KEY_SWARM)
    import asyncio

    while True:
        try:
            await ui_data_tick(redis)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("ui_redis_streams_tick_failed", error=str(exc))
        await asyncio.sleep(interval_s)
