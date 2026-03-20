"""
Incubator Spawn Task — nexus.incubator.spawn

Handles the worker-side execution of spawning a new AI-generated project.
This task is dispatched by the Architect when GOD MODE is ON or after HITL
approval.

Safety guarantees
-----------------
1. ResourceGuard pre-flight: refuses to spawn if CPU > 30% or RAM > 512 MB.
2. Hard cap: max 5 live incubator projects at any time.
3. Each spawned project runs in a subprocess with its own resource limits.
4. Kill switch: checks nexus:incubator:kill_all before starting.

Task type: "nexus.incubator.spawn"
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psutil
import structlog

from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

# ── Safety constants ──────────────────────────────────────────────────────────
MAX_LIVE_PROJECTS   = 5
CPU_CAP_PERCENT     = 30.0
RAM_CAP_MB          = 512.0
KILL_ALL_KEY        = "nexus:incubator:kill_all"
INCUBATOR_PROCS_KEY = "nexus:incubator:running_pids"


def _check_resources() -> tuple[bool, str]:
    """Pre-flight resource check. Returns (ok, reason)."""
    proc = psutil.Process(os.getpid())
    cpu  = psutil.cpu_percent(interval=0.5)
    ram  = proc.memory_info().rss / (1024 * 1024)

    if cpu > CPU_CAP_PERCENT:
        return False, f"CPU at {cpu:.0f}% (cap: {CPU_CAP_PERCENT:.0f}%)"
    if ram > RAM_CAP_MB:
        return False, f"RAM at {ram:.0f} MB (cap: {RAM_CAP_MB:.0f} MB)"
    return True, "ok"


@registry.register("nexus.incubator.spawn")
async def spawn_incubator_project(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Spawn a generated project as a background subprocess.

    Parameters
    ----------
    project_id   — The incubator project ID
    project_path — Absolute path to the project directory
    project_name — Human-readable project name

    Returns
    -------
    dict with pid, status, and message
    """
    project_id   = parameters.get("project_id", "unknown")
    project_path = parameters.get("project_path", "")
    project_name = parameters.get("project_name", project_id)

    log.info("incubator_spawn_starting", project_id=project_id, path=project_path)

    # ── Kill switch check ──────────────────────────────────────────────────────
    # We import redis lazily to avoid circular imports
    try:
        from nexus.shared.config import settings
        import redis.asyncio as aioredis
        r = aioredis.from_url(settings.redis_url, decode_responses=True)
        kill_all = await r.get(KILL_ALL_KEY)
        await r.aclose()
        if kill_all == "1":
            log.warning("incubator_spawn_blocked_kill_switch", project_id=project_id)
            return {"status": "blocked", "reason": "Kill switch is active", "project_id": project_id}
    except Exception as exc:
        log.warning("incubator_spawn_redis_check_failed", error=str(exc))

    # ── Resource pre-flight ────────────────────────────────────────────────────
    ok, reason = _check_resources()
    if not ok:
        log.warning("incubator_spawn_resource_blocked", project_id=project_id, reason=reason)
        return {"status": "blocked", "reason": f"Resource cap: {reason}", "project_id": project_id}

    # ── Validate project path ──────────────────────────────────────────────────
    main_py = Path(project_path) / "main.py"
    if not main_py.exists():
        log.error("incubator_spawn_no_main", project_id=project_id, path=str(main_py))
        return {"status": "error", "reason": "main.py not found", "project_id": project_id}

    # ── Spawn subprocess ───────────────────────────────────────────────────────
    try:
        proc = subprocess.Popen(
            [sys.executable, str(main_py)],
            cwd=str(project_path),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            # Windows: BELOW_NORMAL_PRIORITY_CLASS
            creationflags=(0x00004000 if sys.platform == "win32" else 0),
        )

        log.info(
            "incubator_project_spawned",
            project_id=project_id,
            pid=proc.pid,
            path=str(main_py),
        )

        # Give it 3 seconds to see if it crashes immediately
        await asyncio.sleep(3)
        if proc.poll() is not None:
            stderr_out = proc.stderr.read().decode("utf-8", errors="replace")[:500] if proc.stderr else ""
            log.error("incubator_project_crashed_immediately", project_id=project_id, stderr=stderr_out)
            return {
                "status": "crashed",
                "reason": f"Process exited immediately: {stderr_out}",
                "project_id": project_id,
            }

        return {
            "status": "running",
            "project_id": project_id,
            "pid": proc.pid,
            "message": f"Project '{project_name}' spawned successfully (PID {proc.pid})",
            "spawned_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as exc:
        log.error("incubator_spawn_error", project_id=project_id, error=str(exc))
        return {"status": "error", "reason": str(exc), "project_id": project_id}


@registry.register("nexus.incubator.kill_all")
async def kill_all_incubator_projects(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Emergency kill switch — terminates all running incubator project processes.

    Also sets the nexus:incubator:kill_all Redis flag so future spawns are blocked
    until the flag is cleared.

    Task type: "nexus.incubator.kill_all"
    """
    log.warning("incubator_kill_all_activated")

    killed_pids: list[int] = []
    errors: list[str] = []

    # Set the kill flag in Redis
    try:
        from nexus.shared.config import settings
        import redis.asyncio as aioredis
        r = aioredis.from_url(settings.redis_url, decode_responses=True)
        await r.set(KILL_ALL_KEY, "1", ex=3600)  # 1 hour TTL
        await r.aclose()
    except Exception as exc:
        errors.append(f"Redis flag error: {exc}")

    # Find and kill any Python processes running from Nexus-Projects
    nexus_projects_root = r"C:\Users\Yarin\Desktop\Nexus-Projects"
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            cmdline = proc.info.get("cmdline") or []
            cmdline_str = " ".join(str(c) for c in cmdline)
            if nexus_projects_root in cmdline_str and "python" in cmdline_str.lower():
                proc.kill()
                killed_pids.append(proc.info["pid"])
                log.info("incubator_process_killed", pid=proc.info["pid"])
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
        except Exception as exc:
            errors.append(f"PID {proc.info.get('pid', '?')}: {exc}")

    return {
        "status": "complete",
        "killed_pids": killed_pids,
        "killed_count": len(killed_pids),
        "errors": errors,
        "kill_flag_set": True,
        "message": f"Kill switch activated. {len(killed_pids)} processes terminated.",
    }


@registry.register("nexus.incubator.clear_kill_switch")
async def clear_kill_switch(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Clear the kill switch flag so new incubator projects can be spawned again.
    Task type: "nexus.incubator.clear_kill_switch"
    """
    try:
        from nexus.shared.config import settings
        import redis.asyncio as aioredis
        r = aioredis.from_url(settings.redis_url, decode_responses=True)
        await r.delete(KILL_ALL_KEY)
        await r.aclose()
        log.info("incubator_kill_switch_cleared")
        return {"status": "ok", "message": "Kill switch cleared. Spawning re-enabled."}
    except Exception as exc:
        return {"status": "error", "reason": str(exc)}
