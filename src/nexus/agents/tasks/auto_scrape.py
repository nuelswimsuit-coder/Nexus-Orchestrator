"""
telegram.auto_scrape — Autonomous Telegram group scraper task.

Architecture
------------
This task bridges the Nexus Worker with the Mangement Ahu Telegram bot project.
It runs as a standard ARQ job on any worker node that has access to the
Mangement Ahu project directory.

Execution pipeline
------------------
1. Pre-flight: ResourceGuard check.
   - If CPU > CPU_THRESHOLD (default 30%), reschedule for RESCHEDULE_DELAY_S
     seconds and return a "low_resources" status.  The task does NOT block
     the worker slot during the wait — it returns immediately and the master
     re-dispatches after the delay.

2. Candidate selection: query telefix.db for source groups that have not
   been scraped recently (last_run:scraper metric older than MIN_RESCRAPE_HOURS).

3. Status broadcast: write a "running" status to Redis key
   nexus:scrape:status so the dashboard can show "Scanning...".

4. Scraper invocation: run the Mangement Ahu scraper as a subprocess.
   This keeps the two projects' dependencies (Telethon, etc.) fully isolated.
   The subprocess runs `python run_bot.py --scrape-only --headless` if that
   flag exists, otherwise falls back to a direct Python call that imports
   the scraper engine from the Mangement Ahu project path.

5. Result: update the metrics table (last_run:scraper), write a "completed"
   or "failed" status to Redis, and return structured output.

Stealth Mode integration
------------------------
The task writes its running state to Redis key `nexus:scrape:status`.
The dashboard reads this key and shows a "Scanning..." pulse on the
machine HUD — even in Stealth Mode where RGB is suppressed.
The pulse icon is rendered in the HUD regardless of stealth state.

Resource thresholds
-------------------
CPU_THRESHOLD      = 30 %   — abort and reschedule if exceeded
RESCHEDULE_DELAY_S = 900    — 15 minutes
MIN_RESCRAPE_HOURS = 6      — skip if scraped within this window
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any

import aiosqlite
import psutil
import structlog

from nexus.shared.active_project_scope import (
    LEGACY_SCRAPE_STATUS_KEY,
    resolve_project_type,
    scrape_status_redis_key,
)
from nexus.shared.fleet_redis import (
    get_fleet_counter_snapshot,
    publish_fleet_scan_event,
)
from nexus.shared.schemas import FleetScanEvent, FleetScanPhase
from nexus.shared.checkpoint_store import CheckpointStore
from nexus.agents.task_registry import registry

log = structlog.get_logger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

TELEFIX_PROJECT = r"C:\Users\Yarin\Desktop\Mangement Ahu"
TELEFIX_DB      = os.path.join(TELEFIX_PROJECT, "data", "telefix.db")

CPU_THRESHOLD      = float(os.getenv("SCRAPE_CPU_THRESHOLD", "30"))
RESCHEDULE_DELAY_S = int(os.getenv("SCRAPE_RESCHEDULE_DELAY", "900"))   # 15 min
MIN_RESCRAPE_HOURS = float(os.getenv("SCRAPE_MIN_INTERVAL_HOURS", "6"))

# Legacy alias (per-project keys + mirror for operations_legal — see _write_scrape_status).
SCRAPE_STATUS_KEY = LEGACY_SCRAPE_STATUS_KEY
SCRAPE_STATUS_TTL = 3600  # 1 hour


# ── Helpers ────────────────────────────────────────────────────────────────────

async def _write_scrape_status(
    redis: Any,
    status: str,
    detail: str = "",
    *,
    project_id: str = "telefix",
) -> None:
    """Write the current scrape state to Redis for the dashboard to read."""
    if redis is None:
        return
    import json

    pid = str(project_id or "telefix")
    payload = {
        "status": status,          # "idle" | "running" | "completed" | "failed" | "low_resources"
        "detail": detail,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "project_id": pid,
    }
    blob = json.dumps(payload)
    await redis.set(scrape_status_redis_key(pid), blob, ex=SCRAPE_STATUS_TTL)
    if resolve_project_type(pid) == "operations_legal":
        await redis.set(LEGACY_SCRAPE_STATUS_KEY, blob, ex=SCRAPE_STATUS_TTL)


async def _fleet_auto_emit(redis: Any, phase: FleetScanPhase, detail: str) -> None:
    """Mirror scrape lifecycle to ``nexus:fleet:scan`` for the dashboard progress bar."""
    if redis is None:
        return
    snap = await get_fleet_counter_snapshot(redis)
    await publish_fleet_scan_event(
        redis,
        FleetScanEvent(
            phase=phase,
            task_type="telegram.auto_scrape",
            detail=detail,
            managed_members_total=snap["total_managed_members"],
            premium_members_total=snap["total_premium_members"],
        ),
    )


async def _get_source_groups() -> list[dict[str, Any]]:
    """
    Return source groups that are candidates for scraping.

    A group is a candidate if:
    - It exists in the targets table with role='source'.
    - The last scraper run was more than MIN_RESCRAPE_HOURS ago
      (checked via the metrics table).
    """
    if not os.path.exists(TELEFIX_DB):
        log.warning("auto_scrape_db_missing", path=TELEFIX_DB)
        return []

    candidates: list[dict[str, Any]] = []
    try:
        uri = f"file:{TELEFIX_DB.replace(chr(92), '/')}?mode=ro"
        async with aiosqlite.connect(uri, uri=True, timeout=5) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA busy_timeout = 5000")

            # Get last scraper run timestamp
            async with db.execute(
                "SELECT value FROM metrics WHERE key = 'last_run:scraper'"
            ) as cur:
                row = await cur.fetchone()
                last_run_ts = float(row["value"]) if row else 0.0

            hours_since = (time.time() - last_run_ts) / 3600
            if hours_since < MIN_RESCRAPE_HOURS:
                log.info(
                    "auto_scrape_too_recent",
                    hours_since=round(hours_since, 1),
                    min_hours=MIN_RESCRAPE_HOURS,
                )
                return []

            # Fetch source groups
            async with db.execute(
                "SELECT id, link, title FROM targets WHERE role = 'source'"
            ) as cur:
                rows = await cur.fetchall()
                candidates = [dict(r) for r in rows]

    except Exception as exc:
        log.error("auto_scrape_candidate_query_error", error=str(exc))

    return candidates


def _run_scraper_subprocess(sources: list[str], *, task_id: str = "") -> dict[str, Any]:
    """
    Run the Mangement Ahu scraper in a subprocess.

    Strategy: add the project to sys.path and call the scraper engine
    directly via a helper script.  This keeps Telethon and the bot's
    dependencies fully isolated from the Nexus worker's venv.

    When ``task_id`` is provided it is forwarded to the helper so that
    per-source checkpoints are written and the run can be resumed after a
    crash, power outage, or restart.

    Returns a dict with keys: success, users_saved, error.
    """
    helper_script = os.path.join(
        os.path.dirname(__file__), "_scraper_subprocess_helper.py"
    )

    cmd = [
        sys.executable,
        helper_script,
        "--project", TELEFIX_PROJECT,
        "--sources", ",".join(sources),
    ]
    if task_id:
        cmd += ["--task-id", task_id]

    log.info("auto_scrape_subprocess_start", sources=sources, cmd=" ".join(cmd))

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=1800,  # 30 min hard limit
            cwd=TELEFIX_PROJECT,
        )
        if result.returncode == 0:
            import json
            try:
                output = json.loads(result.stdout.strip().splitlines()[-1])
                return {"success": True, "users_saved": output.get("users_saved", 0), "error": None}
            except Exception:
                return {"success": True, "users_saved": 0, "error": None}
        else:
            return {
                "success": False,
                "users_saved": 0,
                "error": result.stderr[-500:] if result.stderr else "subprocess failed",
            }
    except subprocess.TimeoutExpired:
        return {"success": False, "users_saved": 0, "error": "scraper subprocess timed out"}
    except Exception as exc:
        return {"success": False, "users_saved": 0, "error": str(exc)}


# ── Task handler ───────────────────────────────────────────────────────────────

@registry.register("telegram.auto_scrape")
async def auto_scrape(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Autonomous Telegram group scraper.

    Parameters (all optional)
    -------------------------
    force         : bool  — skip the MIN_RESCRAPE_HOURS guard (default False)
    cpu_threshold : float — override CPU threshold (default CPU_THRESHOLD)
    sources       : list  — explicit list of group links to scrape (default: from DB)
    task_id       : str   — ARQ job id; used for crash-safe checkpointing

    Returns
    -------
    dict with keys:
        status        : "completed" | "failed" | "low_resources" | "no_candidates"
        users_saved   : int
        sources_count : int
        cpu_at_start  : float
        duration_s    : float
        resumed       : bool — True if this run continued from a previous checkpoint
        error         : str | None
    """
    started_at = time.monotonic()
    force = bool(parameters.get("force", False))
    cpu_threshold = float(parameters.get("cpu_threshold", CPU_THRESHOLD))

    # Grab Redis from ARQ context if available (injected by listener startup)
    redis = parameters.get("__redis__")
    proj = str(parameters.get("project_id", "telefix"))

    # task_id is injected by the runner via parameters or ARQ job context
    task_id: str = str(parameters.get("task_id") or parameters.get("__task_id__") or "")

    # ── Checkpoint store ───────────────────────────────────────────────────────
    store: CheckpointStore | None = None
    resumed = False
    if task_id:
        try:
            store = CheckpointStore(task_id)
            stale = store.reset_stale_running()
            if stale:
                log.warning("auto_scrape_checkpoint_stale_reset", task_id=task_id, count=stale)
            if store.has_any_progress():
                resumed = True
                log.info("auto_scrape_resuming_from_checkpoint", task_id=task_id,
                         summary=store.summary())
        except Exception as cp_err:
            log.warning("auto_scrape_checkpoint_init_failed", error=str(cp_err))
            store = None

    # ── 1. Pre-flight: ResourceGuard CPU check ─────────────────────────────────
    cpu_now = psutil.cpu_percent(interval=1.0)
    log.info("auto_scrape_preflight", cpu_percent=cpu_now, threshold=cpu_threshold)

    if cpu_now > cpu_threshold and not force:
        log.warning(
            "auto_scrape_low_resources",
            cpu=cpu_now,
            threshold=cpu_threshold,
            reschedule_s=RESCHEDULE_DELAY_S,
        )
        await _write_scrape_status(
            redis,
            "low_resources",
            f"CPU {cpu_now:.0f}% > {cpu_threshold:.0f}%"
            f" — rescheduled in {RESCHEDULE_DELAY_S//60} min",
            project_id=proj,
        )
        await _fleet_auto_emit(
            redis,
            FleetScanPhase.ENDED,
            f"low_resources: CPU {cpu_now:.0f}%",
        )
        return {
            "status": "low_resources",
            "users_saved": 0,
            "sources_count": 0,
            "cpu_at_start": cpu_now,
            "duration_s": round(time.monotonic() - started_at, 2),
            "reschedule_in_s": RESCHEDULE_DELAY_S,
            "resumed": resumed,
            "error": None,
        }

    # ── 2. Candidate selection ─────────────────────────────────────────────────
    explicit_sources: list[str] = parameters.get("sources", [])

    if explicit_sources:
        candidates = [{"link": s, "title": s} for s in explicit_sources]
    else:
        candidates = await _get_source_groups()

    if not candidates:
        log.info("auto_scrape_no_candidates")
        await _write_scrape_status(
            redis, "idle", "No candidate groups to scrape", project_id=proj
        )
        await _fleet_auto_emit(redis, FleetScanPhase.ENDED, "No candidate groups to scrape")
        return {
            "status": "no_candidates",
            "users_saved": 0,
            "sources_count": 0,
            "cpu_at_start": cpu_now,
            "duration_s": round(time.monotonic() - started_at, 2),
            "resumed": resumed,
            "error": None,
        }

    source_links = [c["link"] for c in candidates]
    log.info("auto_scrape_candidates_found", count=len(candidates), sources=source_links)

    # ── 3. Broadcast "running" status ──────────────────────────────────────────
    resume_note = " (resuming)" if resumed else ""
    await _write_scrape_status(
        redis,
        "running",
        f"Scraping {len(candidates)} group(s): {', '.join(source_links[:3])}"
        + ("..." if len(candidates) > 3 else "") + resume_note,
        project_id=proj,
    )
    await _fleet_auto_emit(
        redis,
        FleetScanPhase.PROGRESS,
        f"Scraping {len(candidates)} group(s){resume_note}",
    )

    # ── 4. Run scraper in subprocess (with checkpoint task_id) ─────────────────
    # Run in a thread executor so we don't block the asyncio event loop
    # during the subprocess.run() call (which is synchronous).
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        lambda: _run_scraper_subprocess(source_links, task_id=task_id),
    )

    duration = round(time.monotonic() - started_at, 2)

    # ── 5. Write final status ──────────────────────────────────────────────────
    if result["success"]:
        # Clear checkpoints on clean completion
        if store is not None:
            try:
                store.clear()
            except Exception:
                pass

        await _write_scrape_status(
            redis,
            "completed",
            f"Saved {result['users_saved']} users"
            f" from {len(candidates)} group(s) in {duration:.0f}s",
            project_id=proj,
        )
        await _fleet_auto_emit(
            redis,
            FleetScanPhase.ENDED,
            f"Scrape completed — {result['users_saved']} users saved",
        )
        log.info(
            "auto_scrape_completed",
            users_saved=result["users_saved"],
            sources=len(candidates),
            duration_s=duration,
            resumed=resumed,
        )
        return {
            "status": "completed",
            "users_saved": result["users_saved"],
            "sources_count": len(candidates),
            "cpu_at_start": cpu_now,
            "duration_s": duration,
            "resumed": resumed,
            "error": None,
        }
    else:
        await _write_scrape_status(
            redis,
            "failed",
            result["error"] or "unknown error",
            project_id=proj,
        )
        await _fleet_auto_emit(
            redis,
            FleetScanPhase.ENDED,
            f"failed: {(result['error'] or 'unknown')[:200]}",
        )
        log.error("auto_scrape_failed", error=result["error"], duration_s=duration)
        return {
            "status": "failed",
            "users_saved": 0,
            "sources_count": len(candidates),
            "cpu_at_start": cpu_now,
            "duration_s": duration,
            "resumed": resumed,
            "error": result["error"],
        }
