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

from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

TELEFIX_PROJECT = r"C:\Users\Yarin\Desktop\Mangement Ahu"
TELEFIX_DB      = os.path.join(TELEFIX_PROJECT, "data", "telefix.db")

CPU_THRESHOLD      = float(os.getenv("SCRAPE_CPU_THRESHOLD", "30"))
RESCHEDULE_DELAY_S = int(os.getenv("SCRAPE_RESCHEDULE_DELAY", "900"))   # 15 min
MIN_RESCRAPE_HOURS = float(os.getenv("SCRAPE_MIN_INTERVAL_HOURS", "6"))

# Redis key written by this task so the dashboard can read scrape state.
SCRAPE_STATUS_KEY = "nexus:scrape:status"
SCRAPE_STATUS_TTL = 3600  # 1 hour


# ── Helpers ────────────────────────────────────────────────────────────────────

async def _write_scrape_status(redis: Any, status: str, detail: str = "") -> None:
    """Write the current scrape state to Redis for the dashboard to read."""
    if redis is None:
        return
    payload = {
        "status": status,          # "idle" | "running" | "completed" | "failed" | "low_resources"
        "detail": detail,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    import json
    await redis.set(SCRAPE_STATUS_KEY, json.dumps(payload), ex=SCRAPE_STATUS_TTL)


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


def _run_scraper_subprocess(sources: list[str]) -> dict[str, Any]:
    """
    Run the Mangement Ahu scraper in a subprocess.

    Strategy: add the project to sys.path and call the scraper engine
    directly via a helper script.  This keeps Telethon and the bot's
    dependencies fully isolated from the Nexus worker's venv.

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

    Returns
    -------
    dict with keys:
        status        : "completed" | "failed" | "low_resources" | "no_candidates"
        users_saved   : int
        sources_count : int
        cpu_at_start  : float
        duration_s    : float
        error         : str | None
    """
    started_at = time.monotonic()
    force = bool(parameters.get("force", False))
    cpu_threshold = float(parameters.get("cpu_threshold", CPU_THRESHOLD))

    # Grab Redis from ARQ context if available (injected by listener startup)
    redis = parameters.get("__redis__")

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
        )
        return {
            "status": "low_resources",
            "users_saved": 0,
            "sources_count": 0,
            "cpu_at_start": cpu_now,
            "duration_s": round(time.monotonic() - started_at, 2),
            "reschedule_in_s": RESCHEDULE_DELAY_S,
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
        await _write_scrape_status(redis, "idle", "No candidate groups to scrape")
        return {
            "status": "no_candidates",
            "users_saved": 0,
            "sources_count": 0,
            "cpu_at_start": cpu_now,
            "duration_s": round(time.monotonic() - started_at, 2),
            "error": None,
        }

    source_links = [c["link"] for c in candidates]
    log.info("auto_scrape_candidates_found", count=len(candidates), sources=source_links)

    # ── 3. Broadcast "running" status ──────────────────────────────────────────
    await _write_scrape_status(
        redis,
        "running",
        f"Scraping {len(candidates)} group(s): {', '.join(source_links[:3])}"
        + ("..." if len(candidates) > 3 else ""),
    )

    # ── 4. Run scraper in subprocess ───────────────────────────────────────────
    # Run in a thread executor so we don't block the asyncio event loop
    # during the subprocess.run() call (which is synchronous).
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None, _run_scraper_subprocess, source_links
    )

    duration = round(time.monotonic() - started_at, 2)

    # ── 5. Write final status ──────────────────────────────────────────────────
    if result["success"]:
        await _write_scrape_status(
            redis,
            "completed",
            f"Saved {result['users_saved']} users"
            f" from {len(candidates)} group(s) in {duration:.0f}s",
        )
        log.info(
            "auto_scrape_completed",
            users_saved=result["users_saved"],
            sources=len(candidates),
            duration_s=duration,
        )
        return {
            "status": "completed",
            "users_saved": result["users_saved"],
            "sources_count": len(candidates),
            "cpu_at_start": cpu_now,
            "duration_s": duration,
            "error": None,
        }
    else:
        await _write_scrape_status(redis, "failed", result["error"] or "unknown error")
        log.error("auto_scrape_failed", error=result["error"], duration_s=duration)
        return {
            "status": "failed",
            "users_saved": 0,
            "sources_count": len(candidates),
            "cpu_at_start": cpu_now,
            "duration_s": duration,
            "error": result["error"],
        }
