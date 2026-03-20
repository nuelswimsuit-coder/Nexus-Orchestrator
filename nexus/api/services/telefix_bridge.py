"""
Telefix Bridge Service — read-only operational intelligence from the
Mangement Ahu Telegram bot database.

Database location
-----------------
C:\\Users\\Yarin\\Desktop\\Mangement Ahu\\data\\telefix.db

This is a SQLite database created and owned by the Telefix bot project.
The bridge opens it in read-only mode (uri=True with mode=ro) so it never
interferes with the bot's own writes.

Schema (confirmed from live database, 2026-03-18)
--------------------------------------------------
users           — telegram_id, access_hash, username, is_premium,
                  source_session, origin_group, target_group, status, created_at
scraped_users   — user_id, access_hash, username, source_group, is_premium,
                  last_active, added_at, scraped_by_session
managed_groups  — group_id, title, username, owner_session, last_automation
targets         — id, link, title, role, created_at
enrollments     — id, user_id, target_link, status, timestamp
settings        — key, value, updated_at
metrics         — key, value, updated_at

Session files (on-disk, not in DB)
-----------------------------------
The bot stores Telethon session files as JSON under:
  sessions/adders/   — active adder accounts
  sessions/frozen/   — temporarily frozen accounts
  sessions/managers/ — manager/owner accounts

Active session count = number of .json files in sessions/adders/.

Metrics keys (confirmed live)
------------------------------
  last_run:forecast, last_run:scraper, last_run:adder,
  last_run:warmup, last_run:admin

ROI / financial data is derived from:
  - Total users added (users table, status='ADDED' or any)
  - Total scraped users (scraped_users table)
  - Forecast history stored in settings key 'forecast:history'
"""

from __future__ import annotations

import glob
import os
from datetime import datetime, timezone
from typing import Any

import aiosqlite
import structlog

log = structlog.get_logger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────────

_PROJECT_ROOT = r"C:\Users\Yarin\Desktop\Mangement Ahu"
DB_PATH = os.path.join(_PROJECT_ROOT, "data", "telefix.db")
SESSIONS_DIR = os.path.join(_PROJECT_ROOT, "sessions")


# ── Stats model ────────────────────────────────────────────────────────────────

class OperationalStats:
    """Snapshot of all operational metrics from the Telefix project."""

    def __init__(
        self,
        total_managed_groups: int,
        total_scraped_users: int,
        total_users_pipeline: int,
        active_sessions: int,
        frozen_sessions: int,
        manager_sessions: int,
        total_targets: int,
        source_groups: int,
        target_groups: int,
        last_scraper_run: str | None,
        last_adder_run: str | None,
        last_forecast_run: str | None,
        forecast_history: list[str],
        db_available: bool,
        queried_at: str,
    ) -> None:
        self.total_managed_groups = total_managed_groups
        self.total_scraped_users = total_scraped_users
        self.total_users_pipeline = total_users_pipeline
        self.active_sessions = active_sessions
        self.frozen_sessions = frozen_sessions
        self.manager_sessions = manager_sessions
        self.total_targets = total_targets
        self.source_groups = source_groups
        self.target_groups = target_groups
        self.last_scraper_run = last_scraper_run
        self.last_adder_run = last_adder_run
        self.last_forecast_run = last_forecast_run
        self.forecast_history = forecast_history
        self.db_available = db_available
        self.queried_at = queried_at

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_managed_groups": self.total_managed_groups,
            "total_scraped_users": self.total_scraped_users,
            "total_users_pipeline": self.total_users_pipeline,
            "active_sessions": self.active_sessions,
            "frozen_sessions": self.frozen_sessions,
            "manager_sessions": self.manager_sessions,
            "total_targets": self.total_targets,
            "source_groups": self.source_groups,
            "target_groups": self.target_groups,
            "last_scraper_run": self.last_scraper_run,
            "last_adder_run": self.last_adder_run,
            "last_forecast_run": self.last_forecast_run,
            "forecast_history": self.forecast_history,
            "db_available": self.db_available,
            "queried_at": self.queried_at,
        }


# ── Session file counting (sync, fast) ────────────────────────────────────────

def _count_session_files() -> dict[str, int]:
    """Count .json session files in each sessions sub-directory."""
    counts: dict[str, int] = {"adders": 0, "frozen": 0, "managers": 0}
    for category in counts:
        pattern = os.path.join(SESSIONS_DIR, category, "*.json")
        counts[category] = len(glob.glob(pattern))
    return counts


def _format_unix_ts(ts: float | None) -> str | None:
    """Convert a Unix timestamp float to a human-readable UTC string."""
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return None


# ── Main query function ────────────────────────────────────────────────────────

async def get_operational_stats() -> OperationalStats:
    """
    Query the Telefix SQLite database and return a full OperationalStats snapshot.

    Opens the database in read-only mode (uri=True, mode=ro) so this service
    never interferes with the bot's own writes.  If the database file does not
    exist or is locked, returns a safe zero-value snapshot with db_available=False.
    """
    now = datetime.now(timezone.utc).isoformat()
    sessions = _count_session_files()

    if not os.path.exists(DB_PATH):
        log.warning("telefix_db_not_found", path=DB_PATH)
        return OperationalStats(
            total_managed_groups=0,
            total_scraped_users=0,
            total_users_pipeline=0,
            active_sessions=sessions["adders"],
            frozen_sessions=sessions["frozen"],
            manager_sessions=sessions["managers"],
            total_targets=0,
            source_groups=0,
            target_groups=0,
            last_scraper_run=None,
            last_adder_run=None,
            last_forecast_run=None,
            forecast_history=[],
            db_available=False,
            queried_at=now,
        )

    try:
        # Open in read-only mode via SQLite URI.
        # busy_timeout=5000ms prevents "database is locked" errors when the
        # Telefix bot is writing concurrently.  WAL mode (set by the writer)
        # allows multiple concurrent readers even while a write is in progress.
        uri = f"file:{DB_PATH.replace(chr(92), '/')}?mode=ro"
        async with aiosqlite.connect(uri, uri=True, timeout=5) as db:
            db.row_factory = aiosqlite.Row
            # Tell SQLite to wait up to 5 s before returning SQLITE_BUSY
            await db.execute("PRAGMA busy_timeout = 5000")

            # ── Managed groups ─────────────────────────────────────────────
            async with db.execute("SELECT COUNT(*) AS n FROM managed_groups") as cur:
                row = await cur.fetchone()
                total_managed_groups = row["n"] if row else 0

            # ── Scraped users (distinct user_id) ───────────────────────────
            async with db.execute(
                "SELECT COUNT(DISTINCT user_id) AS n FROM scraped_users"
            ) as cur:
                row = await cur.fetchone()
                total_scraped_users = row["n"] if row else 0

            # ── Users pipeline (all rows in users table) ───────────────────
            async with db.execute("SELECT COUNT(*) AS n FROM users") as cur:
                row = await cur.fetchone()
                total_users_pipeline = row["n"] if row else 0

            # ── Targets breakdown ──────────────────────────────────────────
            async with db.execute("SELECT COUNT(*) AS n FROM targets") as cur:
                row = await cur.fetchone()
                total_targets = row["n"] if row else 0

            async with db.execute(
                "SELECT COUNT(*) AS n FROM targets WHERE role = 'source'"
            ) as cur:
                row = await cur.fetchone()
                source_groups = row["n"] if row else 0

            async with db.execute(
                "SELECT COUNT(*) AS n FROM targets WHERE role = 'target'"
            ) as cur:
                row = await cur.fetchone()
                target_groups = row["n"] if row else 0

            # ── Metrics (last run timestamps) ──────────────────────────────
            async with db.execute(
                "SELECT key, value FROM metrics WHERE key LIKE 'last_run:%'"
            ) as cur:
                metric_rows = await cur.fetchall()

            metrics: dict[str, float] = {r["key"]: r["value"] for r in metric_rows}

            last_scraper_run = _format_unix_ts(metrics.get("last_run:scraper"))
            last_adder_run   = _format_unix_ts(metrics.get("last_run:adder"))
            last_forecast_run = _format_unix_ts(metrics.get("last_run:forecast"))

            # ── Forecast history (settings table) ─────────────────────────
            async with db.execute(
                "SELECT value FROM settings WHERE key = 'forecast:history'"
            ) as cur:
                row = await cur.fetchone()
                forecast_raw = row["value"] if row else ""

            forecast_history = (
                [d.strip() for d in forecast_raw.split(",") if d.strip()]
                if forecast_raw
                else []
            )

        log.info(
            "telefix_stats_queried",
            groups=total_managed_groups,
            scraped=total_scraped_users,
            sessions=sessions["adders"],
        )

        return OperationalStats(
            total_managed_groups=total_managed_groups,
            total_scraped_users=total_scraped_users,
            total_users_pipeline=total_users_pipeline,
            active_sessions=sessions["adders"],
            frozen_sessions=sessions["frozen"],
            manager_sessions=sessions["managers"],
            total_targets=total_targets,
            source_groups=source_groups,
            target_groups=target_groups,
            last_scraper_run=last_scraper_run,
            last_adder_run=last_adder_run,
            last_forecast_run=last_forecast_run,
            forecast_history=forecast_history,
            db_available=True,
            queried_at=now,
        )

    except Exception as exc:
        log.error("telefix_stats_error", error=str(exc))
        return OperationalStats(
            total_managed_groups=0,
            total_scraped_users=0,
            total_users_pipeline=0,
            active_sessions=sessions["adders"],
            frozen_sessions=sessions["frozen"],
            manager_sessions=sessions["managers"],
            total_targets=0,
            source_groups=0,
            target_groups=0,
            last_scraper_run=None,
            last_adder_run=None,
            last_forecast_run=None,
            forecast_history=[],
            db_available=False,
            queried_at=now,
        )


# ── Time-windowed stats ────────────────────────────────────────────────────────

async def get_windowed_stats(window_minutes: int = 1440) -> dict[str, Any]:
    """
    Return stats for the given time window (default 1440 min = 24 h).

    Supports two windows:
      60   — last 60 minutes (live operational view)
      1440 — last 24 hours   (daily summary)

    Returns a dict with new_scraped_users, new_pipeline_users, and
    the full OperationalStats snapshot for context.
    """
    import time as _time
    cutoff_ts = _time.time() - (window_minutes * 60)
    cutoff_str = datetime.fromtimestamp(cutoff_ts, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    base = await get_operational_stats()
    result = base.to_dict()
    result["window_minutes"] = window_minutes

    if not os.path.exists(DB_PATH):
        result["new_scraped_users_window"] = 0
        result["new_pipeline_users_window"] = 0
        return result

    try:
        uri = f"file:{DB_PATH.replace(chr(92), '/')}?mode=ro"
        async with aiosqlite.connect(uri, uri=True, timeout=5) as db:
            db.row_factory = aiosqlite.Row

            async with db.execute(
                "SELECT COUNT(*) AS n FROM scraped_users WHERE added_at >= ?",
                (cutoff_str,),
            ) as c:
                row = await c.fetchone()
                result["new_scraped_users_window"] = row["n"] if row else 0

            async with db.execute(
                "SELECT COUNT(*) AS n FROM users WHERE created_at >= ?",
                (cutoff_str,),
            ) as c:
                row = await c.fetchone()
                result["new_pipeline_users_window"] = row["n"] if row else 0

    except Exception as exc:
        log.error("telefix_windowed_stats_error", error=str(exc))
        result["new_scraped_users_window"] = 0
        result["new_pipeline_users_window"] = 0

    return result
