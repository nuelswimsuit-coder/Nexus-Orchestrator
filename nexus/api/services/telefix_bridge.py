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

Nexus extension (written by Nexus, not the Telefix bot)
--------------------------------------------------------
nexus_fleet_audit — id, run_id, created_at, payload_json
                    (append-only snapshots of ``FleetAuditResults``)

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
import json
import os
import time as time_module
from datetime import datetime, timezone
from typing import Any

import aiosqlite
import structlog

log = structlog.get_logger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────────

_PROJECT_ROOT = r"C:\Users\Yarin\Desktop\Mangement Ahu"
DB_PATH = os.path.join(_PROJECT_ROOT, "data", "telefix.db")
SESSIONS_DIR = os.path.join(_PROJECT_ROOT, "sessions")


def _telefix_db_path() -> str:
    """Resolve telefix.db (Desktop layout when available, else legacy Windows path)."""
    try:
        from nexus.shared.paths import get_telefix_path

        return str(get_telefix_path("Mangement Ahu") / "data" / "telefix.db")
    except Exception:
        return DB_PATH


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


# ── Fleet intelligence (managed groups × scraped_users) ────────────────────────


def _parse_last_automation_ts(val: Any) -> float | None:
    """Best-effort parse of managed_groups.last_automation to Unix seconds."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        t = float(val)
        return t / 1000.0 if t > 1e12 else t
    s = str(val).strip()
    if not s:
        return None
    try:
        iso = s.replace("Z", "+00:00") if s.endswith("Z") else s
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        try:
            return float(s)
        except Exception:
            return None


def _fleet_status_from_automation(last_automation: Any) -> str:
    ts = _parse_last_automation_ts(last_automation)
    if ts is None:
        return "MONITORING"
    age_s = time_module.time() - ts
    if age_s < 3 * 86400:
        return "ACTIVE"
    if age_s < 14 * 86400:
        return "STALE"
    return "DORMANT"


async def get_fleet_group_assets() -> dict[str, Any]:
    """
    Per managed group: display name, member/premium counts from scraped_users,
    owner session, and derived status from last_automation.

    Joins scraped_users on title (exact, case-insensitive), username substring,
    or Telegram group id appearing in source_group.
    """
    now = datetime.now(timezone.utc).isoformat()
    db_file = _telefix_db_path()
    if not os.path.exists(db_file):
        return {"groups": [], "db_available": False, "queried_at": now}

    sql = """
        SELECT
          mg.group_id,
          COALESCE(
            NULLIF(TRIM(mg.title), ''),
            NULLIF(TRIM(mg.username), ''),
            CAST(mg.group_id AS TEXT)
          ) AS group_name,
          mg.title AS raw_title,
          mg.username AS raw_username,
          mg.owner_session,
          mg.last_automation,
          COUNT(su.user_id) AS member_count,
          SUM(
            CASE
              WHEN su.user_id IS NULL THEN 0
              WHEN CAST(COALESCE(su.is_premium, 0) AS INTEGER) != 0 THEN 1
              ELSE 0
            END
          ) AS premium_count
        FROM managed_groups mg
        LEFT JOIN scraped_users su ON (
          (
            LENGTH(TRIM(COALESCE(mg.title, ''))) > 0
            AND LOWER(TRIM(COALESCE(su.source_group, ''))) = LOWER(TRIM(COALESCE(mg.title, '')))
          )
          OR (
            mg.username IS NOT NULL
            AND TRIM(mg.username) != ''
            AND LENGTH(TRIM(COALESCE(su.source_group, ''))) > 0
            AND INSTR(
              LOWER(REPLACE(COALESCE(su.source_group, ''), 'https://t.me/', '')),
              LOWER(REPLACE(TRIM(COALESCE(mg.username, '')), '@', ''))
            ) > 0
          )
          OR (
            INSTR(COALESCE(su.source_group, ''), CAST(mg.group_id AS TEXT)) > 0
          )
        )
        GROUP BY mg.group_id, mg.title, mg.username, mg.owner_session, mg.last_automation
    """

    try:
        uri = f"file:{db_file.replace(chr(92), '/')}?mode=ro"
        async with aiosqlite.connect(uri, uri=True, timeout=5) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA busy_timeout = 5000")
            async with db.execute(sql) as cur:
                rows = await cur.fetchall()

        groups: list[dict[str, Any]] = []
        for r in rows:
            gid = r["group_id"]
            gid_s = str(int(gid)) if isinstance(gid, (int, float)) and float(gid).is_integer() else str(gid)
            mcount = int(r["member_count"] or 0)
            pcount = int(r["premium_count"] or 0)
            last_auto = r["last_automation"]
            groups.append(
                {
                    "group_id": gid_s,
                    "group_name": (r["group_name"] or gid_s).strip() or gid_s,
                    "member_count": mcount,
                    "premium_count": pcount,
                    "owner_session": r["owner_session"],
                    "status": _fleet_status_from_automation(last_auto),
                    "last_automation": None if last_auto is None else str(last_auto),
                }
            )

        groups.sort(key=lambda x: x["member_count"], reverse=True)

        log.info("fleet_assets_queried", rows=len(groups))
        return {"groups": groups, "db_available": True, "queried_at": now}

    except Exception as exc:
        log.error("fleet_assets_error", error=str(exc))
        return {"groups": [], "db_available": False, "queried_at": now}


async def append_fleet_audit_run(payload: dict[str, Any]) -> bool:
    """
    Append a ``FleetAuditResults`` snapshot to Nexus-owned table ``nexus_fleet_audit``.

    Uses a normal SQLite connection (not read-only) so the Telefix bot and Nexus
    can coexist; WAL mode allows concurrent reads from the bot while we INSERT.
    """
    path = _telefix_db_path()
    if not os.path.exists(path):
        log.warning("fleet_audit_db_missing", path=path)
        return False

    run_id = str(payload.get("run_id", ""))
    scanned = payload.get("scanned_at")
    if isinstance(scanned, dict):
        scanned = json.dumps(scanned)
    created_at = str(scanned or datetime.now(timezone.utc).isoformat())
    blob = json.dumps(payload, default=str)

    try:
        async with aiosqlite.connect(path, timeout=10) as db:
            await db.execute("PRAGMA busy_timeout = 8000")
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS nexus_fleet_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            await db.execute(
                """
                INSERT INTO nexus_fleet_audit (run_id, created_at, payload_json)
                VALUES (?, ?, ?)
                """,
                (run_id, created_at, blob),
            )
            await db.commit()
        log.info("fleet_audit_row_saved", run_id=run_id)
        return True
    except Exception as exc:
        log.error("fleet_audit_save_error", error=str(exc))
        return False
