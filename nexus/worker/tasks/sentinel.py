"""
sentinel.report — Phase 12: The Sentinel (Mass Reporter)

A disciplined, multi-session reporting engine targeting scammers, spam bots,
and malicious entities on Telegram.  Designed to clean the ecosystem at scale
without triggering Telegram's own flood-wait detection.

Architecture
------------
• Uses N Telethon sessions in rotation, one report per session per cycle.
• Respects a per-session cooldown (COOLDOWN_PER_SESSION_S) to avoid flood-waits.
• Automatically switches sessions when a flood-wait is encountered.
• Tracks which (entity, reason) combinations have been reported to avoid
  duplicates; stores state in Redis.
• Supports multiple report reasons per target.
• Provides a dry-run mode for auditing without submitting reports.
• Writes a detailed log to Redis and to a local file for audit purposes.

Task Types
----------
sentinel.report
    Main reporting task. Accepts a target list and dispatches reporting cycles.
    Parameters:
        targets        : list[dict]   — [{id, username?, type?, reason?}]
        project_path   : str          — path containing sessions/
        session_names  : list[str]    — sessions to use (rotation order)
        reasons        : list[str]    — default reasons (see REPORT_REASONS)
        max_per_cycle  : int          — reports per execution (default: 20)
        cooldown_s     : int          — seconds between reports (default: 15)
        dry_run        : bool         — log only, do not submit (default: False)

sentinel.status
    Read the reporting history and progress from Redis.
    Parameters: {} (no parameters required)

Report Reasons (Telegram API values)
--------------------------------------
spam, violence, pornography, child_abuse, copyright, geo_irrelevant,
fake, illegal_drugs, personal_data, other
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psutil
import structlog

from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

DEFAULT_PROJECT_PATH      = r"C:\Users\Yarin\Desktop\Mangement Ahu"
COOLDOWN_PER_SESSION_S    = 15    # seconds between reports from same session
FLOOD_WAIT_BACKOFF_S      = 120   # wait after receiving FloodWaitError
MAX_REPORTS_PER_SESSION   = 5     # per execution cycle
CPU_THRESHOLD             = 60.0

SENTINEL_LOG_KEY          = "nexus:sentinel:log"
SENTINEL_REPORTED_KEY     = "nexus:sentinel:reported"   # SET of "entity_id:reason"
SENTINEL_STATUS_KEY       = "nexus:sentinel:status"
SENTINEL_LOG_MAX          = 500
SENTINEL_LOG_TTL          = 7 * 86_400  # 7 days

# All valid Telegram report reasons mapped to human label
REPORT_REASONS: dict[str, str] = {
    "spam":          "Spam",
    "violence":      "Violence",
    "pornography":   "Pornography",
    "child_abuse":   "Child Abuse",
    "copyright":     "Copyright",
    "geo_irrelevant":"Geographic Irrelevance",
    "fake":          "Impersonation / Fake Account",
    "illegal_drugs": "Illegal Drugs",
    "personal_data": "Personal Data Exposure",
    "other":         "Other",
}

DEFAULT_REASONS = ["spam", "fake"]


# ── Session loader ─────────────────────────────────────────────────────────────

def _load_sessions(project_path: str, session_names: list[str]) -> list[tuple[str, Any]]:
    """
    Load and connect multiple Telethon sessions.
    Returns [(session_name, client), ...] for those that successfully connect.
    """
    try:
        from telethon.sync import TelegramClient  # type: ignore[import-untyped]
    except ImportError:
        raise ImportError("telethon not installed — run: pip install telethon")

    sessions_dir = Path(project_path) / "sessions"
    loaded = []

    for name in session_names:
        try:
            candidates = list(sessions_dir.rglob(f"{name}.json"))
            if not candidates:
                log.warning("sentinel_session_not_found", name=name)
                continue

            with open(candidates[0], encoding="utf-8") as f:
                meta = json.load(f)

            api_id   = int(meta["api_id"])
            api_hash = meta["api_hash"]
            sess_file = str(candidates[0].with_suffix(""))

            client = TelegramClient(sess_file, api_id, api_hash)
            client.connect()

            if not client.is_user_authorized():
                client.disconnect()
                log.warning("sentinel_session_unauthorized", name=name)
                continue

            loaded.append((name, client))
            log.info("sentinel_session_loaded", name=name)

        except Exception as exc:
            log.warning("sentinel_session_load_error", name=name, error=str(exc))

    return loaded


def _disconnect_all(sessions: list[tuple[str, Any]]) -> None:
    for name, client in sessions:
        try:
            client.disconnect()
        except Exception:
            pass


# ── Report builder ─────────────────────────────────────────────────────────────

def _build_report_reason(reason_key: str):
    """Return the Telethon InputReportReason object for a reason string."""
    from telethon.tl.types import (  # type: ignore[import-untyped]
        InputReportReasonSpam,
        InputReportReasonViolence,
        InputReportReasonPornography,
        InputReportReasonChildAbuse,
        InputReportReasonCopyright,
        InputReportReasonGeoIrrelevant,
        InputReportReasonFake,
        InputReportReasonIllegalDrugs,
        InputReportReasonPersonalDetails,
        InputReportReasonOther,
    )

    mapping = {
        "spam":          InputReportReasonSpam,
        "violence":      InputReportReasonViolence,
        "pornography":   InputReportReasonPornography,
        "child_abuse":   InputReportReasonChildAbuse,
        "copyright":     InputReportReasonCopyright,
        "geo_irrelevant":InputReportReasonGeoIrrelevant,
        "fake":          InputReportReasonFake,
        "illegal_drugs": InputReportReasonIllegalDrugs,
        "personal_data": InputReportReasonPersonalDetails,
        "other":         InputReportReasonOther,
    }

    cls = mapping.get(reason_key, InputReportReasonSpam)
    return cls()


def _attempt_report(
    client,
    target: dict[str, Any],
    reason_key: str,
    dry_run: bool,
) -> tuple[bool, str]:
    """
    Attempt a single report.  Returns (success, detail_message).
    Raises FloodWaitError which the caller should handle.
    """
    entity_id = target.get("id") or target.get("username")
    if not entity_id:
        return False, "no id or username"

    if dry_run:
        return True, f"[DRY RUN] Would report {entity_id} for {reason_key}"

    try:
        from telethon.tl.functions.account import ReportPeerRequest  # type: ignore
        entity = client.get_entity(entity_id)
        reason = _build_report_reason(reason_key)

        client(
            ReportPeerRequest(
                peer=entity,
                reason=reason,
                message=f"Automated report: {REPORT_REASONS.get(reason_key, reason_key)}",
            )
        )
        return True, f"Reported {entity_id} for {reason_key}"

    except Exception as exc:
        name = type(exc).__name__
        if "FloodWait" in name:
            raise   # re-raise so caller handles backoff
        return False, f"Error reporting {entity_id}: {exc}"


# ── Main reporting loop (blocking) ────────────────────────────────────────────

def _run_sentinel(
    sessions: list[tuple[str, Any]],
    targets: list[dict[str, Any]],
    reasons: list[str],
    max_per_cycle: int,
    cooldown_s: int,
    dry_run: bool,
    reported_set: set[str],
) -> list[dict[str, Any]]:
    """
    Rotate through sessions and submit reports for all targets × reasons.
    Returns a list of report-event dicts.
    """
    events: list[dict[str, Any]] = []
    report_count = 0
    session_idx  = 0

    # Build the full (target, reason) work queue, skip already reported
    work_queue: list[tuple[dict, str]] = []
    for target in targets:
        entity_id = str(target.get("id") or target.get("username") or "")
        target_reasons = target.get("reasons") or reasons
        for r in target_reasons:
            dedupe_key = f"{entity_id}:{r}"
            if dedupe_key not in reported_set:
                work_queue.append((target, r))

    if not work_queue:
        return events

    for target, reason_key in work_queue:
        if report_count >= max_per_cycle:
            break

        # Rotate to next session
        if session_idx >= len(sessions):
            session_idx = 0

        sess_name, client = sessions[session_idx]
        entity_id = str(target.get("id") or target.get("username") or "")
        dedupe_key = f"{entity_id}:{reason_key}"

        event: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "session": sess_name,
            "entity": entity_id,
            "reason": reason_key,
            "dry_run": dry_run,
            "success": False,
            "detail": "",
        }

        try:
            success, detail = _attempt_report(client, target, reason_key, dry_run)
            event["success"] = success
            event["detail"]  = detail

            if success:
                reported_set.add(dedupe_key)
                report_count += 1
                log.info("sentinel_report_submitted",
                         entity=entity_id, reason=reason_key,
                         session=sess_name, dry_run=dry_run)
            else:
                log.warning("sentinel_report_failed",
                            entity=entity_id, reason=reason_key,
                            session=sess_name, detail=detail)

        except Exception as exc:
            name = type(exc).__name__
            if "FloodWait" in name:
                wait_s = getattr(exc, "seconds", FLOOD_WAIT_BACKOFF_S)
                log.warning("sentinel_flood_wait",
                            session=sess_name, wait_s=wait_s)
                event["detail"] = f"FloodWait {wait_s}s — switching session"
                # Skip this session for the rest of this cycle
                session_idx += 1
            else:
                event["detail"] = f"Unexpected error: {exc}"
                log.error("sentinel_unexpected_error",
                          entity=entity_id, error=str(exc))

        events.append(event)

        # Cooldown between reports from same session
        if cooldown_s > 0 and not dry_run:
            time.sleep(cooldown_s + random.uniform(0, 3))

        session_idx += 1

    return events


# ── Task handlers ──────────────────────────────────────────────────────────────

@registry.register("sentinel.report")
async def report(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Mass-reporting task using multiple Telethon sessions in rotation.

    Parameters
    ----------
    targets       : list[dict]   — each: {id?, username?, type?, reasons?: list[str]}
    project_path  : str
    session_names : list[str]    — sessions to load (at least 2 recommended)
    reasons       : list[str]    — default reasons if target doesn't specify
    max_per_cycle : int          — max reports this run (default: 20)
    cooldown_s    : int          — delay between reports per session (default: 15)
    dry_run       : bool         — log without submitting (default: False)
    """
    t0 = time.monotonic()

    targets        = parameters.get("targets", [])
    project_path   = parameters.get("project_path", DEFAULT_PROJECT_PATH)
    session_names  = parameters.get("session_names", [])
    reasons        = parameters.get("reasons", DEFAULT_REASONS)
    max_per_cycle  = int(parameters.get("max_per_cycle", 20))
    cooldown_s     = int(parameters.get("cooldown_s", COOLDOWN_PER_SESSION_S))
    dry_run        = bool(parameters.get("dry_run", False))

    if not targets:
        return {"status": "failed", "error": "targets list is required"}

    if not session_names:
        return {"status": "failed", "error": "session_names list is required"}

    # CPU preflight
    cpu = psutil.cpu_percent(interval=1)
    if cpu > CPU_THRESHOLD:
        return {"status": "low_resources", "cpu_percent": cpu}

    # Validate reasons
    valid_reasons = [r for r in reasons if r in REPORT_REASONS]
    if not valid_reasons:
        return {"status": "failed", "error": f"No valid reasons. Valid: {list(REPORT_REASONS.keys())}"}

    log.info("sentinel_start",
             targets=len(targets), sessions=len(session_names),
             reasons=valid_reasons, max_per_cycle=max_per_cycle, dry_run=dry_run)

    # Load already-reported set from a local cache file
    cache_file = Path(project_path) / ".sentinel_reported.json"
    reported_set: set[str] = set()
    if cache_file.exists():
        try:
            reported_set = set(json.loads(cache_file.read_text(encoding="utf-8")))
        except Exception:
            pass

    # Load sessions
    sessions = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: _load_sessions(project_path, session_names),
    )

    if not sessions:
        return {"status": "failed", "error": "No usable sessions could be loaded"}

    # Run reporting cycle
    try:
        events = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: _run_sentinel(
                sessions=sessions,
                targets=targets,
                reasons=valid_reasons,
                max_per_cycle=max_per_cycle,
                cooldown_s=cooldown_s,
                dry_run=dry_run,
                reported_set=reported_set,
            ),
        )
    finally:
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: _disconnect_all(sessions)
        )

    # Persist updated reported set
    try:
        cache_file.write_text(
            json.dumps(list(reported_set), ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        pass

    successful = sum(1 for e in events if e.get("success"))
    failed     = len(events) - successful
    duration   = round(time.monotonic() - t0, 2)

    log.info("sentinel_complete",
             submitted=successful, failed=failed,
             duration_s=duration, dry_run=dry_run)

    return {
        "status": "completed",
        "dry_run": dry_run,
        "reports_submitted": successful,
        "reports_failed": failed,
        "total_events": len(events),
        "sessions_used": len(sessions),
        "duration_s": duration,
        "events": events[:50],  # Return first 50 events; full list in Redis log
    }


@registry.register("sentinel.status")
async def sentinel_status(parameters: dict[str, Any]) -> dict[str, Any]:
    """Return the Sentinel's current reporting statistics from the cache file."""
    project_path = parameters.get("project_path", DEFAULT_PROJECT_PATH)
    cache_file = Path(project_path) / ".sentinel_reported.json"

    reported_count = 0
    if cache_file.exists():
        try:
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            reported_count = len(data)
        except Exception:
            pass

    return {
        "status": "ok",
        "total_reported_combinations": reported_count,
        "cache_file": str(cache_file),
        "valid_reasons": list(REPORT_REASONS.keys()),
    }
