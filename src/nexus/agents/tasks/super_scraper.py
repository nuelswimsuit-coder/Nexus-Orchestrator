"""
telegram.super_scrape — Strategic Intelligence Hunter.

Unlike `telegram.auto_scrape` which scrapes known source groups, the
Super-Scraper *hunts* for new, high-value Telegram groups based on niche
keywords derived from the most profitable projects in telefix.db.

Pipeline
--------
1. Pre-flight: CPU ≤ 40% AND RAM ≤ 80%.  If resources are tight, the task
   silently postpones itself for POSTPONE_DELAY_S (30 min) and returns.
   Stealth Override (parameter `stealth_override=True`) bypasses this check.

2. Niche intelligence: query telefix.db to find the top-performing niches
   (groups with the most scraped users → highest ROI proxy).

3. Keyword generation: derive search keywords from the top niches.

4. Group discovery: search Telegram for groups matching each keyword using
   the Telethon search API (via subprocess to keep deps isolated).
   Filter: only groups with member_count > MIN_MEMBER_COUNT (default 500).

5. New niche detection: compare discovered groups against existing targets.
   If new groups are found in a niche not yet in the DB:
   → HITL gate: publish a HitlRequest via Redis.
   → WhatsApp + Telegram notifications fire in parallel.
   → Task returns with status="awaiting_approval" and stores candidates.

6. Mass-scrape: if approved (or no new niches), dispatch auto_scrape for
   each approved group.

7. Status: writes to Redis nexus:super_scraper:status and agent log.

Resource thresholds
-------------------
SUPER_CPU_THRESHOLD  = 40 %   — abort if exceeded (vs 30% for auto_scrape)
SUPER_RAM_THRESHOLD  = 80 %   — abort if RAM usage exceeds this fraction
POSTPONE_DELAY_S     = 1800   — 30 minutes
MIN_MEMBER_COUNT     = 500    — only scrape groups with ≥ this many members
MAX_GROUPS_PER_NICHE = 5      — cap discovery per keyword to avoid spam
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import aiosqlite
import psutil
import structlog

from nexus.shared.fleet_redis import (
    fleet_mapper_record_group,
    get_fleet_counter_snapshot,
    publish_fleet_scan_event,
)
from nexus.shared.schemas import FleetScanEvent, FleetScanPhase
from nexus.shared.checkpoint_store import CheckpointStore
from nexus.agents.task_registry import registry
from nexus.agents.tasks.auto_scrape import TELEFIX_DB, TELEFIX_PROJECT

log = structlog.get_logger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

SUPER_CPU_THRESHOLD  = float(os.getenv("SUPER_SCRAPE_CPU_THRESHOLD", "40"))
SUPER_RAM_THRESHOLD  = float(os.getenv("SUPER_SCRAPE_RAM_THRESHOLD", "80"))
POSTPONE_DELAY_S     = int(os.getenv("SUPER_SCRAPE_POSTPONE_DELAY", "1800"))  # 30 min
MIN_MEMBER_COUNT     = int(os.getenv("SUPER_SCRAPE_MIN_MEMBERS", "500"))
MAX_GROUPS_PER_NICHE = int(os.getenv("SUPER_SCRAPE_MAX_PER_NICHE", "5"))

SUPER_STATUS_KEY      = "nexus:super_scraper:status"
SUPER_CANDIDATES_KEY  = "nexus:super_scraper:candidates"
SUPER_STATUS_TTL      = 7200   # 2 hours

# Keyword templates per niche category
NICHE_KEYWORDS: dict[str, list[str]] = {
    "crypto":    ["crypto alpha", "defi signals", "bitcoin trading", "altcoin gems"],
    "finance":   ["investment tips", "stock signals", "forex trading", "passive income"],
    "tech":      ["ai tools", "tech news", "startup ideas", "programming tips"],
    "fitness":   ["fitness motivation", "workout tips", "diet advice", "gym community"],
    "fashion":   ["fashion trends", "style tips", "luxury brands", "streetwear"],
    "gaming":    ["gaming community", "esports news", "game tips", "mobile gaming"],
    "marketing": ["digital marketing", "social media growth", "seo tips", "dropshipping"],
    "general":   ["viral content", "trending news", "community chat", "daily motivation"],
}


# ── Redis helpers ──────────────────────────────────────────────────────────────

async def _write_status(redis: Any, status: str, detail: str = "") -> None:
    if redis is None:
        return
    payload = json.dumps({
        "status": status,
        "detail": detail,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    await redis.set(SUPER_STATUS_KEY, payload, ex=SUPER_STATUS_TTL)


async def _store_candidates(redis: Any, candidates: list[dict]) -> None:
    if redis is None or not candidates:
        return
    await redis.set(
        SUPER_CANDIDATES_KEY,
        json.dumps(candidates),
        ex=SUPER_STATUS_TTL,
    )


async def _fleet_super_emit(redis: Any, phase: FleetScanPhase, detail: str) -> None:
    """Push fleet scan phase to Redis for the dashboard SSE stream."""
    if redis is None:
        return
    snap = await get_fleet_counter_snapshot(redis)
    await publish_fleet_scan_event(
        redis,
        FleetScanEvent(
            phase=phase,
            task_type="telegram.super_scrape",
            detail=detail,
            managed_members_total=snap["total_managed_members"],
            premium_members_total=snap["total_premium_members"],
        ),
    )


# ── Niche intelligence ─────────────────────────────────────────────────────────

async def _get_top_niches(limit: int = 5) -> list[str]:
    """
    Query telefix.db to find the most productive source groups (by scraped
    user count) and extract their niche keywords.

    Returns a list of niche category strings (keys of NICHE_KEYWORDS).
    """
    if not os.path.exists(TELEFIX_DB):
        return ["crypto", "general"]

    niches: list[str] = []
    try:
        uri = f"file:{TELEFIX_DB.replace(chr(92), '/')}?mode=ro"
        async with aiosqlite.connect(uri, uri=True, timeout=5) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA busy_timeout = 5000")

            # Top source groups by scraped user count
            async with db.execute("""
                SELECT source_group, COUNT(*) AS cnt
                FROM scraped_users
                GROUP BY source_group
                ORDER BY cnt DESC
                LIMIT ?
            """, (limit,)) as c:
                rows = await c.fetchall()

            for row in rows:
                group_name = (row["source_group"] or "").lower()
                # Map group name to a niche category
                for niche, keywords in NICHE_KEYWORDS.items():
                    if any(kw.split()[0] in group_name for kw in keywords):
                        if niche not in niches:
                            niches.append(niche)
                        break

            # Also check managed_groups titles
            async with db.execute(
                "SELECT title FROM managed_groups LIMIT 10"
            ) as c:
                for row in await c.fetchall():
                    title = (row["title"] or "").lower()
                    for niche in NICHE_KEYWORDS:
                        if niche in title and niche not in niches:
                            niches.append(niche)

    except Exception as exc:
        log.warning("super_scraper_niche_query_error", error=str(exc))

    # Always include at least one niche
    if not niches:
        niches = ["crypto", "general"]

    return niches[:limit]


async def _get_existing_group_links() -> set[str]:
    """Return all group links already in the targets table."""
    if not os.path.exists(TELEFIX_DB):
        return set()
    try:
        uri = f"file:{TELEFIX_DB.replace(chr(92), '/')}?mode=ro"
        async with aiosqlite.connect(uri, uri=True, timeout=5) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA busy_timeout = 5000")
            async with db.execute("SELECT link FROM targets") as c:
                return {row["link"] for row in await c.fetchall()}
    except Exception:
        return set()


# ── Group discovery subprocess ─────────────────────────────────────────────────

def _run_discovery_subprocess(
    keywords: list[str],
    min_members: int,
    max_per_keyword: int,
    secrets: dict[str, str],
) -> list[dict[str, Any]]:
    """
    Search Telegram for groups matching `keywords` via a subprocess.
    Returns a list of {title, link, member_count, niche} dicts.
    """
    helper = os.path.join(os.path.dirname(__file__), "_discovery_helper.py")
    cmd = [
        sys.executable, helper,
        "--project",     TELEFIX_PROJECT,
        "--keywords",    ",".join(keywords),
        "--min-members", str(min_members),
        "--max-per-kw",  str(max_per_keyword),
        "--api-id",      secrets.get("TELEFIX_API_ID", ""),
        "--api-hash",    secrets.get("TELEFIX_API_HASH", ""),
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=300, cwd=TELEFIX_PROJECT,
        )
        if result.returncode == 0:
            lines = result.stdout.strip().splitlines()
            if lines:
                return json.loads(lines[-1])
        log.warning("super_scraper_discovery_failed",
            error=result.stderr[-300:] if result.stderr else "no output")
    except subprocess.TimeoutExpired:
        log.warning("super_scraper_discovery_timeout")
    except Exception as exc:
        log.error("super_scraper_discovery_error", error=str(exc))
    return []


# ── Main task handler ──────────────────────────────────────────────────────────

@registry.register("telegram.super_scrape")
async def super_scrape(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Strategic Intelligence Hunter — finds new high-value Telegram groups.

    Parameters
    ----------
    stealth_override : bool  — bypass CPU/RAM check (default False)
    force_niches     : list  — explicit niche list (default: from DB)
    approved_groups  : list  — pre-approved group links (skip HITL gate)
    max_per_niche    : int   — override MAX_GROUPS_PER_NICHE
    task_id          : str   — ARQ job id for crash-safe checkpointing

    Returns
    -------
    dict with keys: status, niches_scanned, new_groups_found,
                    awaiting_approval, dispatched_scrapes, duration_s,
                    resumed, error
    """
    started_at = time.monotonic()
    stealth_override = bool(parameters.get("stealth_override", False))
    force_niches: list[str] = parameters.get("force_niches", [])
    approved_groups: list[str] = parameters.get("approved_groups", [])
    max_per_niche = int(parameters.get("max_per_niche", MAX_GROUPS_PER_NICHE))
    redis   = parameters.get("__redis__")
    secrets = parameters.get("__secrets__", {})
    task_id: str = str(parameters.get("task_id") or parameters.get("__task_id__") or "")

    # ── Checkpoint store ───────────────────────────────────────────────────────
    store: CheckpointStore | None = None
    resumed = False
    if task_id:
        try:
            store = CheckpointStore(task_id)
            store.reset_stale_running()
            if store.has_any_progress():
                resumed = True
                log.info("super_scraper_resuming_from_checkpoint",
                         task_id=task_id, summary=store.summary())
        except Exception as cp_err:
            log.warning("super_scraper_checkpoint_init_failed", error=str(cp_err))
            store = None

    # ── 1. Pre-flight: CPU + RAM check ────────────────────────────────────────
    cpu_now = psutil.cpu_percent(interval=1.0)
    mem     = psutil.virtual_memory()
    ram_pct = mem.percent

    log.info("super_scraper_preflight",
        cpu=cpu_now, ram=ram_pct,
        cpu_threshold=SUPER_CPU_THRESHOLD, ram_threshold=SUPER_RAM_THRESHOLD,
        stealth_override=stealth_override)

    if not stealth_override and (cpu_now > SUPER_CPU_THRESHOLD or ram_pct > SUPER_RAM_THRESHOLD):
        reason = (
            f"CPU {cpu_now:.0f}% > {SUPER_CPU_THRESHOLD:.0f}%"
            if cpu_now > SUPER_CPU_THRESHOLD
            else f"RAM {ram_pct:.0f}% > {SUPER_RAM_THRESHOLD:.0f}%"
        )
        await _write_status(redis, "postponed",
            f"{reason} — postponed {POSTPONE_DELAY_S//60} min")
        await _fleet_super_emit(redis, FleetScanPhase.ENDED, f"postponed: {reason}")
        log.info("super_scraper_postponed", reason=reason)
        return {
            "status": "postponed",
            "niches_scanned": 0,
            "new_groups_found": 0,
            "awaiting_approval": False,
            "dispatched_scrapes": 0,
            "duration_s": round(time.monotonic() - started_at, 2),
            "postpone_s": POSTPONE_DELAY_S,
            "resumed": resumed,
            "error": None,
        }

    # ── 2. Niche intelligence (resume: restore from checkpoint if available) ───
    niches: list[str] = []
    if store is not None:
        saved_niches = store.get_task_meta("niches")
        if saved_niches and isinstance(saved_niches, list):
            niches = saved_niches
            log.info("super_scraper_niches_from_checkpoint", niches=niches)

    if not niches:
        niches = force_niches if force_niches else await _get_top_niches()
        if store is not None:
            store.save_task_meta("niches", niches)

    await _write_status(redis, "hunting",
        f"Scanning niches: {', '.join(niches)}" + (" (resuming)" if resumed else ""))
    log.info("super_scraper_niches", niches=niches)

    # ── 3. Build keyword list ─────────────────────────────────────────────────
    keywords: list[str] = []
    for niche in niches:
        keywords.extend(NICHE_KEYWORDS.get(niche, [niche])[:2])

    # ── 4. Group discovery (resume: skip if already done) ─────────────────────
    discovered: list[dict] = []
    discovery_done = store is not None and store.get_step("phase:discovery") is not None \
        and (store.get_step("phase:discovery") or {}).get("status") == "done"

    if discovery_done and store is not None:
        saved_discovered = store.get_task_meta("discovered_groups")
        if saved_discovered and isinstance(saved_discovered, list):
            discovered = saved_discovered
            log.info("super_scraper_discovery_from_checkpoint", count=len(discovered))
    else:
        if store is not None:
            store.mark_running("phase:discovery")

        await _write_status(redis, "discovering",
            f"Searching {len(keywords)} keyword(s) for groups with ≥{MIN_MEMBER_COUNT} members")

        loop = asyncio.get_event_loop()
        discovered = await loop.run_in_executor(
            None, _run_discovery_subprocess,
            keywords, MIN_MEMBER_COUNT, max_per_niche, secrets,
        )

        if store is not None:
            store.mark_done("phase:discovery", {"count": len(discovered)})
            store.save_task_meta("discovered_groups", discovered)

    log.info("super_scraper_discovered", count=len(discovered))

    mtot, ptot = 0, 0
    if redis is not None:
        for g in discovered:
            mc = int(g.get("member_count") or 0)
            pc = int(g.get("premium_count") or g.get("premium_members") or 0)
            mtot, ptot = await fleet_mapper_record_group(
                redis,
                managed_members=mc,
                premium_members=pc,
            )
        if discovered:
            await publish_fleet_scan_event(
                redis,
                FleetScanEvent(
                    phase=FleetScanPhase.PROGRESS,
                    task_type="telegram.super_scrape",
                    detail=f"Mapper indexed {len(discovered)} group(s)",
                    groups_found_delta=len(discovered),
                    managed_members_total=mtot,
                    premium_members_total=ptot,
                ),
            )

    # ── 5. New niche detection ────────────────────────────────────────────────
    existing_links = await _get_existing_group_links()
    new_groups = [
        g for g in discovered
        if g.get("link") and g["link"] not in existing_links
    ]

    if not new_groups:
        await _write_status(redis, "idle",
            f"No new groups found across {len(niches)} niche(s)")
        await _fleet_super_emit(
            redis,
            FleetScanPhase.ENDED,
            f"No new groups across {len(niches)} niche(s)",
        )
        if store is not None:
            store.clear()
        return {
            "status": "no_new_groups",
            "niches_scanned": len(niches),
            "new_groups_found": 0,
            "awaiting_approval": False,
            "dispatched_scrapes": 0,
            "duration_s": round(time.monotonic() - started_at, 2),
            "resumed": resumed,
            "error": None,
        }

    # ── 6. HITL gate for new niche discoveries ────────────────────────────────
    # Pre-approved groups (from a previous approval) skip the gate.
    pre_approved_set = set(approved_groups)
    needs_approval = [g for g in new_groups if g.get("link") not in pre_approved_set]
    already_approved = [g for g in new_groups if g.get("link") in pre_approved_set]

    if needs_approval:
        # Restore candidate_id from checkpoint if available (avoids duplicate HITL requests)
        candidate_id: str = ""
        if store is not None:
            candidate_id = store.get_task_meta("candidate_id") or ""

        if not candidate_id:
            candidate_id = str(uuid.uuid4())
            candidates_payload = {
                "candidate_id": candidate_id,
                "groups": needs_approval,
                "niches": niches,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            await _store_candidates(redis, [candidates_payload])

            if store is not None:
                store.save_task_meta("candidate_id", candidate_id)
                store.mark_running("phase:hitl_gate")

            # Publish HITL request to Redis so the master's HitlGate picks it up
            if redis is not None:
                from nexus.shared.constants import HITL_REQUEST_CHANNEL
                from nexus.shared.schemas import HitlRequest
                hitl_req = HitlRequest(
                    task_id=candidate_id,
                    task_type="telegram.super_scrape",
                    project_id="telefix",
                    context=(
                        f"Super-Scraper found {len(needs_approval)} new group(s) "
                        f"across niches: {', '.join(niches)}. "
                        f"Approve to start mass-scrape, reject to skip. "
                        f"Top group: {needs_approval[0].get('title', '?')} "
                        f"({needs_approval[0].get('member_count', '?')} members)"
                    ),
                )
                await redis.publish(HITL_REQUEST_CHANNEL, hitl_req.model_dump_json())
                log.info("super_scraper_hitl_published",
                    groups=len(needs_approval), candidate_id=candidate_id)
        else:
            log.info("super_scraper_hitl_already_pending",
                     candidate_id=candidate_id, groups=len(needs_approval))

        await _write_status(redis, "awaiting_approval",
            f"{len(needs_approval)} new group(s) pending approval")
        await _fleet_super_emit(
            redis,
            FleetScanPhase.ENDED,
            f"Awaiting HITL approval for {len(needs_approval)} group(s)",
        )

        # Dispatch scrapes for already-approved groups while waiting
        dispatched = len(already_approved)
        if already_approved:
            await _dispatch_scrapes(redis, already_approved, secrets)

        return {
            "status": "awaiting_approval",
            "niches_scanned": len(niches),
            "new_groups_found": len(new_groups),
            "awaiting_approval": True,
            "candidate_id": candidate_id,
            "dispatched_scrapes": dispatched,
            "duration_s": round(time.monotonic() - started_at, 2),
            "resumed": resumed,
            "error": None,
        }

    # ── 7. All groups pre-approved — dispatch scrapes ─────────────────────────
    dispatched = await _dispatch_scrapes(redis, new_groups, secrets)
    duration = round(time.monotonic() - started_at, 2)

    if store is not None:
        store.clear()

    await _write_status(redis, "completed",
        f"Dispatched {dispatched} scrape(s) from {len(niches)} niche(s) in {duration:.0f}s")
    await _fleet_super_emit(
        redis,
        FleetScanPhase.ENDED,
        f"Completed: dispatched {dispatched} scrape(s) in {duration:.0f}s",
    )

    return {
        "status": "completed",
        "niches_scanned": len(niches),
        "new_groups_found": len(new_groups),
        "awaiting_approval": False,
        "dispatched_scrapes": dispatched,
        "duration_s": duration,
        "resumed": resumed,
        "error": None,
    }


async def _dispatch_scrapes(
    redis: Any,
    groups: list[dict],
    secrets: dict[str, str],
) -> int:
    """Enqueue auto_scrape tasks for each approved group."""
    if not groups:
        return 0

    links = [g["link"] for g in groups if g.get("link")]
    if not links or redis is None:
        return 0

    try:
        import arq
        from arq.connections import RedisSettings

        from nexus.shared.config import settings as nexus_settings
        from nexus.shared.schemas import TaskPayload

        task = TaskPayload(
            task_type="telegram.auto_scrape",
            parameters={"sources": links, "force": True},
            project_id="telefix",
            priority=2,
        )
        arq_pool = await arq.create_pool(
            RedisSettings.from_dsn(nexus_settings.redis_url),
            default_queue_name="nexus:tasks",
        )
        await arq_pool.enqueue_job(
            "execute_task",
            task_payload=task.model_dump_for_wire(),
            _job_id=str(uuid.uuid4()),
            _queue_name="nexus:tasks",
        )
        await arq_pool.aclose()
        log.info("super_scraper_dispatched_scrapes", count=len(links))
        return len(links)
    except Exception as exc:
        log.error("super_scraper_dispatch_error", error=str(exc))
        return 0
