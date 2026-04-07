"""
OpenClaw hardware orchestrator — fleet health polling, GPU VRAM pressure scaling, auto nexus-push.

Rules (operator policy)
-----------------------
1. Poll Redis node heartbeats (``nexus:heartbeat:*``) for Master, Mac Mini, and Laptop roles.
2. If a node whose GPU model matches RTX 3080 (configurable substring) reports
   GPU VRAM usage ≥ threshold (default 90%%), rewrite ``configs/workers.json`` so the
   Mac Mini worker is first and set routing metadata + Redis hint.
3. When pressure clears (default < 75%% VRAM), restore the saved ``workers.json`` snapshot.
4. After any architectural change to ``workers.json``, run ``nexus-push`` (sync script).

Environment
-----------
OPENCLAW_ORCH_POLL_SEC           Poll interval (default 20).
OPENCLAW_ORCH_REPO_ROOT          Override repo root (default: parent of ``nexus/``).
OPENCLAW_ORCH_GPU_SUBSTR         GPU model substring, case-insensitive (default ``3080``).
OPENCLAW_ORCH_GPU_VRAM_PCT_HIGH  Enter drain state at this VRAM %% (default 90).
OPENCLAW_ORCH_GPU_VRAM_PCT_LOW   Leave drain state below this %% (default 75).
OPENCLAW_ORCH_MAC_NODE_ID        Preferred worker when draining (default ``worker_mac_mini``).
OPENCLAW_ORCH_MASTER_NODE_IDS    Comma-separated node_id list (default ``master``).
OPENCLAW_ORCH_LAPTOP_NODE_IDS    Comma-separated laptop worker node_ids (see env doc in code).
OPENCLAW_ORCH_AUTO_PUSH          Set ``0`` to skip ``nexus-push`` (default ``1``).
OPENCLAW_ORCH_DRY_RUN            Set ``1`` to log actions without writing files or pushing.
OPENCLAW_ORCH_USE_SYSTEM_RAM_FALLBACK  If ``1`` and VRAM unknown, use system RAM %% on GPU node.

Uses the same Redis resolution as ``scripts/nexus_push.py`` (``.env`` / ``configs/.env``).
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog
from dotenv import load_dotenv
from redis.asyncio import Redis

from nexus.shared import redis_util
from nexus.shared.config import settings
from nexus.shared.schemas import NodeHeartbeat, NodeRole
from nexus.shared.workers_config import resolve_workers_config_path

log = structlog.get_logger(__name__)

HEARTBEAT_PREFIX = "nexus:heartbeat:"
REDIS_SNAPSHOT_KEY = "nexus:openclaw:hardware:pre_drain_workers_json"
REDIS_DRAIN_ACTIVE_KEY = "nexus:openclaw:hardware:drain_active"
REDIS_LAST_SCAN_KEY = "nexus:openclaw:hardware:last_scan"
REDIS_PREFERRED_WORKER_KEY = "nexus:openclaw:preferred_session_worker"


def _repo_root() -> Path:
    explicit = (os.getenv("OPENCLAW_ORCH_REPO_ROOT") or "").strip()
    if explicit:
        return Path(explicit).expanduser().resolve()
    # nexus/services/this_file.py → repo root is three levels up
    return Path(__file__).resolve().parent.parent.parent


def _load_env(repo_root: Path) -> None:
    load_dotenv(repo_root / ".env", override=False)
    cfg = repo_root / "configs" / ".env"
    if cfg.is_file():
        load_dotenv(cfg, override=False)


def _split_ids(raw: str) -> set[str]:
    return {x.strip() for x in raw.split(",") if x.strip()}


def _workers_path(repo_root: Path) -> Path | None:
    env_path = (
        os.environ.get("NEXUS_WORKERS_CONFIG") or os.environ.get("WORKERS_JSON") or ""
    ).strip()
    p = resolve_workers_config_path(repo_root, env_path or None)
    return p


def _read_workers_file(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_workers_file(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _worker_entry_id(entry: Any) -> str:
    if isinstance(entry, dict):
        return str(entry.get("node_id") or entry.get("id") or "").strip()
    return ""


def _reorder_workers_first(workers: list[Any], node_id: str) -> list[Any]:
    first: list[Any] = []
    rest: list[Any] = []
    for w in workers:
        if _worker_entry_id(w) == node_id:
            first.append(w)
        else:
            rest.append(w)
    return first + rest if first else list(workers)


def _run_nexus_push(repo_root: Path) -> int:
    exe = shutil.which("nexus-push")
    cmd = [exe] if exe else [sys.executable, "-m", "scripts.nexus_push"]
    proc = subprocess.run(
        cmd,
        cwd=str(repo_root),
        check=False,
    )
    return int(proc.returncode)


async def _scan_heartbeats(redis: Redis) -> list[NodeHeartbeat]:
    out: list[NodeHeartbeat] = []
    cursor = 0
    pattern = f"{HEARTBEAT_PREFIX}*".encode()
    while True:
        cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=128)
        for key in keys:
            raw = await redis.get(key)
            if raw is None:
                continue
            try:
                if isinstance(raw, bytes):
                    raw = raw.decode()
                out.append(NodeHeartbeat.model_validate_json(raw))
            except Exception as exc:
                log.warning("openclaw_orch_heartbeat_parse_error", key=key, error=str(exc))
        if cursor == 0:
            break
    return out


def _node_vram_pct(hb: NodeHeartbeat, use_sys_ram_fallback: bool) -> float:
    g = float(hb.gpu_mem_used_pct)
    if g >= 0:
        return g
    if not use_sys_ram_fallback or not hb.ram_total_mb or hb.ram_total_mb <= 0:
        return -1.0
    return round(100.0 * float(hb.ram_used_mb) / float(hb.ram_total_mb), 2)


def _pick_gpu_pressure_node(
    heartbeats: list[NodeHeartbeat],
    gpu_substr: str,
    use_sys_ram_fallback: bool,
) -> tuple[NodeHeartbeat | None, float]:
    sub = gpu_substr.lower()
    best: tuple[NodeHeartbeat | None, float] = (None, -1.0)
    for hb in heartbeats:
        gm = (hb.gpu_model or "").lower()
        if sub not in gm:
            continue
        pct = _node_vram_pct(hb, use_sys_ram_fallback)
        if pct < 0:
            continue
        if pct > best[1]:
            best = (hb, pct)
    return best


def _monitor_labels(
    hb: NodeHeartbeat,
    master_ids: set[str],
    laptop_ids: set[str],
    mac_id: str,
) -> list[str]:
    labels: list[str] = []
    nid = hb.node_id
    if hb.role == NodeRole.MASTER or nid in master_ids:
        labels.append("master")
    if nid == mac_id:
        labels.append("mac_mini")
    if nid in laptop_ids:
        labels.append("laptop")
    return labels


async def _orchestrator_cycle(
    redis: Redis,
    *,
    repo_root: Path,
    workers_path: Path,
    gpu_substr: str,
    high_pct: float,
    low_pct: float,
    mac_node_id: str,
    master_ids: set[str],
    laptop_ids: set[str],
    auto_push: bool,
    dry_run: bool,
    use_sys_ram_fallback: bool,
) -> None:
    heartbeats = await _scan_heartbeats(redis)
    gpu_node, vram_pct = _pick_gpu_pressure_node(heartbeats, gpu_substr, use_sys_ram_fallback)

    nodes_report: list[dict[str, Any]] = []
    for hb in heartbeats:
        nodes_report.append(
            {
                "node_id": hb.node_id,
                "role": hb.role.value,
                "online": True,
                "labels": _monitor_labels(hb, master_ids, laptop_ids, mac_node_id),
                "cpu_percent": hb.cpu_percent,
                "ram_used_mb": hb.ram_used_mb,
                "gpu_model": hb.gpu_model,
                "gpu_mem_used_pct": hb.gpu_mem_used_pct,
                "local_ip": hb.local_ip,
            }
        )

    await redis.set(
        REDIS_LAST_SCAN_KEY,
        json.dumps(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "nodes": nodes_report,
                "gpu_pressure_node_id": gpu_node.node_id if gpu_node else None,
                "gpu_pressure_vram_pct": vram_pct if gpu_node else None,
            },
            ensure_ascii=False,
        ),
        ex=86400,
    )

    draining = (await redis.get(REDIS_DRAIN_ACTIVE_KEY) or "").strip() == "1"
    pressure = gpu_node is not None and vram_pct >= high_pct
    clear = gpu_node is not None and vram_pct < low_pct

    if dry_run:
        log.info(
            "openclaw_orch_dry_run_tick",
            draining=draining,
            pressure=pressure,
            vram_pct=vram_pct,
            gpu_node=gpu_node.node_id if gpu_node else None,
            nodes=len(nodes_report),
        )
        return

    if pressure and not draining:
        try:
            data = _read_workers_file(workers_path)
        except Exception as exc:
            log.error("openclaw_orch_workers_read_failed", path=str(workers_path), error=str(exc))
            return

        workers = data.get("workers")
        if not isinstance(workers, list):
            log.warning("openclaw_orch_no_workers_array", path=str(workers_path))
            return

        await redis.set(REDIS_SNAPSHOT_KEY, json.dumps(data, ensure_ascii=False))
        new_workers = _reorder_workers_first(workers, mac_node_id)
        data["workers"] = new_workers
        data["openclaw_hardware_orchestrator"] = {
            "preferred_session_worker_node_id": mac_node_id,
            "draining_gpu_node_id": gpu_node.node_id,
            "gpu_model_substr": gpu_substr,
            "reason": "gpu_vram_pressure",
            "vram_percent": vram_pct,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        _write_workers_file(workers_path, data)
        await redis.set(REDIS_DRAIN_ACTIVE_KEY, "1")
        await redis.set(REDIS_PREFERRED_WORKER_KEY, mac_node_id)
        log.warning(
            "openclaw_orch_drain_applied",
            mac_first=mac_node_id,
            gpu_node=gpu_node.node_id,
            vram_pct=vram_pct,
        )
        if auto_push:
            rc = _run_nexus_push(repo_root)
            if rc != 0:
                log.error("openclaw_orch_nexus_push_failed", returncode=rc)
            else:
                log.info("openclaw_orch_nexus_push_ok")

    elif draining and clear:
        raw_snap = await redis.get(REDIS_SNAPSHOT_KEY)
        if not raw_snap:
            await redis.delete(REDIS_DRAIN_ACTIVE_KEY)
            await redis.delete(REDIS_PREFERRED_WORKER_KEY)
            log.warning("openclaw_orch_clear_no_snapshot")
            return
        try:
            if isinstance(raw_snap, bytes):
                raw_snap = raw_snap.decode()
            restored = json.loads(raw_snap)
            _write_workers_file(workers_path, restored)
        except Exception as exc:
            log.error("openclaw_orch_restore_failed", error=str(exc))
            return

        await redis.delete(REDIS_DRAIN_ACTIVE_KEY)
        await redis.delete(REDIS_PREFERRED_WORKER_KEY)
        await redis.delete(REDIS_SNAPSHOT_KEY)
        log.info("openclaw_orch_drain_cleared", vram_pct=vram_pct)
        if auto_push:
            rc = _run_nexus_push(repo_root)
            if rc != 0:
                log.error("openclaw_orch_nexus_push_failed", returncode=rc)
            else:
                log.info("openclaw_orch_nexus_push_ok")


def _build_redis() -> Redis:
    url = redis_util.coerce_redis_url_for_platform(settings.redis_url)
    return Redis.from_url(url, decode_responses=True)


async def run_loop() -> None:
    repo_root = _repo_root()
    _load_env(repo_root)

    poll = float(os.getenv("OPENCLAW_ORCH_POLL_SEC") or "20")
    gpu_substr = (os.getenv("OPENCLAW_ORCH_GPU_SUBSTR") or "3080").strip() or "3080"
    high_pct = float(os.getenv("OPENCLAW_ORCH_GPU_VRAM_PCT_HIGH") or "90")
    low_pct = float(os.getenv("OPENCLAW_ORCH_GPU_VRAM_PCT_LOW") or "75")
    mac_node_id = (os.getenv("OPENCLAW_ORCH_MAC_NODE_ID") or "worker_mac_mini").strip()
    master_ids = _split_ids(os.getenv("OPENCLAW_ORCH_MASTER_NODE_IDS") or "master")
    laptop_ids = _split_ids(
        os.getenv("OPENCLAW_ORCH_LAPTOP_NODE_IDS")
        or "worker_laptop_linux,worker_laptop_windows"
    )
    auto_push = (os.getenv("OPENCLAW_ORCH_AUTO_PUSH") or "1").strip() not in ("0", "false", "no")
    dry_run = (os.getenv("OPENCLAW_ORCH_DRY_RUN") or "").strip() in ("1", "true", "yes")
    use_sys_ram_fallback = (os.getenv("OPENCLAW_ORCH_USE_SYSTEM_RAM_FALLBACK") or "").strip() in (
        "1",
        "true",
        "yes",
    )

    wp = _workers_path(repo_root)
    if wp is None:
        log.error(
            "openclaw_orch_no_workers_json",
            hint="Create configs/workers.json or set NEXUS_WORKERS_CONFIG.",
        )
        raise SystemExit(2)

    redis = _build_redis()
    try:
        await redis.ping()
    except Exception as exc:
        log.error("openclaw_orch_redis_unreachable", error=str(exc))
        raise SystemExit(3) from exc

    log.info(
        "openclaw_orch_started",
        poll_sec=poll,
        workers_json=str(wp),
        gpu_substr=gpu_substr,
        high_pct=high_pct,
        low_pct=low_pct,
        mac_node_id=mac_node_id,
        dry_run=dry_run,
    )

    try:
        while True:
            try:
                await _orchestrator_cycle(
                    redis,
                    repo_root=repo_root,
                    workers_path=wp,
                    gpu_substr=gpu_substr,
                    high_pct=high_pct,
                    low_pct=low_pct,
                    mac_node_id=mac_node_id,
                    master_ids=master_ids,
                    laptop_ids=laptop_ids,
                    auto_push=auto_push,
                    dry_run=dry_run,
                    use_sys_ram_fallback=use_sys_ram_fallback,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("openclaw_orch_cycle_error", error=str(exc))
            await asyncio.sleep(poll)
    finally:
        await redis.aclose()


def main() -> None:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run_loop())


if __name__ == "__main__":
    main()
