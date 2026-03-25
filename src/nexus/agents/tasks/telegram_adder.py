"""
telegram.auto_add — Autonomous Telegram group adder task.

Mirrors the structure of telegram.auto_scrape but invokes the
SmartAdderEngine from the Mangement Ahu project via subprocess isolation.

Pipeline
--------
1. Pre-flight: CPU check (same threshold as auto_scrape).
2. Candidate selection: fetch target groups and pending users from telefix.db.
3. Status broadcast: write "running" to Redis nexus:add:status.
4. Adder invocation: subprocess call to _adder_subprocess_helper.py.
5. Result: write final status to Redis, return structured output.

Telethon session handling (fixed encoding/path logic)
------------------------------------------------------
The adder subprocess helper uses the same session loading logic as the
Mangement Ahu project, which:
- Loads .json session files from sessions/adders/ (UTF-8, with latin-1 fallback)
- Resolves absolute paths using the project's paths.py SESSIONS_DIR constant
- Handles Windows path separators correctly via pathlib.Path

HITL integration
----------------
If the adder encounters a FloodWait or account ban, it writes a
HitlRequest to the Redis HITL channel so the operator can decide
whether to continue, pause, or freeze the affected session.
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

from nexus.agents.task_registry import registry
from nexus.agents.tasks.auto_scrape import CPU_THRESHOLD, RESCHEDULE_DELAY_S, TELEFIX_DB

log = structlog.get_logger(__name__)

ADD_STATUS_KEY = "nexus:add:status"
ADD_STATUS_TTL = 3600
MIN_USERS_TO_ADD = int(os.getenv("ADD_MIN_USERS", "10"))


async def _write_add_status(redis: Any, status: str, detail: str = "") -> None:
    if redis is None:
        return
    import json
    payload = json.dumps({
        "status": status,
        "detail": detail,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    await redis.set(ADD_STATUS_KEY, payload, ex=ADD_STATUS_TTL)


async def _get_add_candidates() -> dict[str, Any]:
    """Return target groups and pending users from telefix.db."""
    if not os.path.exists(TELEFIX_DB):
        return {"targets": [], "user_count": 0}

    try:
        uri = f"file:{TELEFIX_DB.replace(chr(92), '/')}?mode=ro"
        async with aiosqlite.connect(uri, uri=True, timeout=5) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA busy_timeout = 5000")

            async with db.execute(
                "SELECT link, title FROM targets WHERE role='target'"
            ) as c:
                targets = [dict(r) for r in await c.fetchall()]

            async with db.execute(
                "SELECT COUNT(*) AS n FROM users WHERE status='PENDING'"
            ) as c:
                r = await c.fetchone()
                user_count = r["n"] if r else 0

        return {"targets": targets, "user_count": user_count}
    except Exception as exc:
        log.error("adder_candidate_query_error", error=str(exc))
        return {"targets": [], "user_count": 0}


def _run_adder_subprocess(target_links: list[str]) -> dict[str, Any]:
    """Run the Mangement Ahu adder engine in a subprocess."""
    helper = os.path.join(os.path.dirname(__file__), "_adder_subprocess_helper.py")

    cmd = [
        sys.executable, helper,
        "--project", r"C:\Users\Yarin\Desktop\Mangement Ahu",
        "--targets", ",".join(target_links),
    ]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=3600, cwd=r"C:\Users\Yarin\Desktop\Mangement Ahu",
        )
        if result.returncode == 0:
            import json
            try:
                output = json.loads(result.stdout.strip().splitlines()[-1])
                return {"success": True, "added": output.get("added", 0), "error": None}
            except Exception:
                return {"success": True, "added": 0, "error": None}
        return {"success": False, "added": 0, "error": result.stderr[-500:] or "subprocess failed"}
    except subprocess.TimeoutExpired:
        return {"success": False, "added": 0, "error": "adder subprocess timed out"}
    except Exception as exc:
        return {"success": False, "added": 0, "error": str(exc)}


@registry.register("telegram.auto_add")
async def auto_add(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Autonomous Telegram group adder.

    Parameters (all optional)
    -------------------------
    force         : bool  — skip CPU check
    targets       : list  — explicit target group links
    """
    started_at = time.monotonic()
    force = bool(parameters.get("force", False))
    redis = parameters.get("__redis__")

    # ── Pre-flight ─────────────────────────────────────────────────────────────
    cpu_now = psutil.cpu_percent(interval=1.0)
    if cpu_now > CPU_THRESHOLD and not force:
        await _write_add_status(redis, "low_resources",
            f"CPU {cpu_now:.0f}% > {CPU_THRESHOLD:.0f}%")
        return {
            "status": "low_resources",
            "added": 0,
            "cpu_at_start": cpu_now,
            "duration_s": round(time.monotonic() - started_at, 2),
            "reschedule_in_s": RESCHEDULE_DELAY_S,
            "error": None,
        }

    # ── Candidates ─────────────────────────────────────────────────────────────
    explicit_targets: list[str] = parameters.get("targets", [])
    if explicit_targets:
        targets = [{"link": t, "title": t} for t in explicit_targets]
        user_count = MIN_USERS_TO_ADD
    else:
        candidates = await _get_add_candidates()
        targets = candidates["targets"]
        user_count = candidates["user_count"]

    if not targets or user_count < MIN_USERS_TO_ADD:
        await _write_add_status(redis, "idle",
            f"No candidates ({user_count} users, {len(targets)} targets)")
        return {
            "status": "no_candidates",
            "added": 0,
            "cpu_at_start": cpu_now,
            "duration_s": round(time.monotonic() - started_at, 2),
            "error": None,
        }

    target_links = [t["link"] for t in targets]
    await _write_add_status(redis, "running",
        f"Adding to {len(targets)} group(s), {user_count} users pending")

    # ── Run adder ──────────────────────────────────────────────────────────────
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _run_adder_subprocess, target_links)
    duration = round(time.monotonic() - started_at, 2)

    if result["success"]:
        await _write_add_status(redis, "completed",
            f"Added {result['added']} users in {duration:.0f}s")
        return {
            "status": "completed",
            "added": result["added"],
            "cpu_at_start": cpu_now,
            "duration_s": duration,
            "error": None,
        }
    else:
        await _write_add_status(redis, "failed", result["error"] or "unknown")
        return {
            "status": "failed",
            "added": 0,
            "cpu_at_start": cpu_now,
            "duration_s": duration,
            "error": result["error"],
        }
