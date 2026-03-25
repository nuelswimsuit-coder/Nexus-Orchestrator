"""
nexus/shared/db_util.py — Global Singleton DB Connector with Self-Healing
=========================================================================

Provides a thread-safe SQLite connector with ``check_same_thread=False`` so
any node (Jacob-PC, Worker-Linux, etc.) can share the same connection object.

Self-Healing behaviour
----------------------
If ``telefix.db`` is not found locally:
  1. Try to download it from the Master Node over HTTP.
  2. If that fails, call ``create_default_db()`` to create a fresh empty DB
     with the correct schema (sqlite3 + migration script).
  3. If resource creation itself fails, immediately dispatch a
     ``CRITICAL_SYSTEM_REPORT`` via the Telegram bot with the full stack trace.
  4. Enqueue an ``Auto-Scrape`` task via ARQ so the DB is populated from live
     Telegram data.

Usage
-----
    from nexus.shared.db_util import get_db, get_telefix_db

    conn = get_db("nexus_architect.sqlite3")
    telefix = get_telefix_db()
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import sqlite3
import threading
import traceback
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────────

_REPO_ROOT = Path(__file__).resolve().parents[4]
_DATA_DIR = _REPO_ROOT / "data"
_DATA_DIR.mkdir(parents=True, exist_ok=True)

TELEFIX_DB_NAME = "telefix.db"

_TELEFIX_DB_HARDWIRED = Path("C:/Users/Yarin/Desktop/Nexus-Orchestrator/telefix.db")

# ── Extended search paths: Project Root, Desktop, OneDrive/Desktop ────────────
_HOME = Path.home()
_DESKTOP_PATHS: list[Path] = [
    _HOME / "Desktop",
    _HOME / "OneDrive" / "Desktop",
    _HOME / "OneDrive - Personal" / "Desktop",
    Path("C:/Users/Yarin/Desktop"),
    Path("C:/Users/Yarin/OneDrive/Desktop"),
]

_TELEFIX_SEARCH_PATHS: list[Path] = [
    _REPO_ROOT / "telefix.db",
    _REPO_ROOT / "data" / "telefix.db",
    _TELEFIX_DB_HARDWIRED,
    Path.home() / "telefix.db",
    # Desktop variants
    *[p / "telefix.db" for p in _DESKTOP_PATHS],
    # OneDrive project root variants
    *[p / "Nexus-Orchestrator" / "telefix.db" for p in _DESKTOP_PATHS],
    *[p / "Nexus-Orchestrator" / "data" / "telefix.db" for p in _DESKTOP_PATHS],
]


def _master_copy_telefix_to_project_root() -> Path | None:
    """
    Master-node helper: if telefix.db is NOT in the project root but IS found
    on the Desktop (or any known location), copy it into the project root so
    all nodes resolve to the same canonical path.

    Returns the destination path on success, None if nothing was copied.
    """
    dest = _REPO_ROOT / "telefix.db"
    if dest.exists() and dest.stat().st_size > 0:
        return dest  # already in project root — nothing to do

    for candidate in _TELEFIX_SEARCH_PATHS:
        if candidate == dest:
            continue
        if candidate.exists() and candidate.stat().st_size > 0:
            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(str(candidate), str(dest))
                log.warning(
                    "db_util.master_copy_telefix — copied %s → %s",
                    candidate,
                    dest,
                )
                print(
                    f"📦 [DB-RESOLVER] telefix.db copied from {candidate} → {dest}",
                    flush=True,
                )
                return dest
            except Exception as exc:
                log.warning("db_util.master_copy_failed — %s", str(exc))
    return None


def _resolve_telefix_db_path() -> Path:
    env_path = os.getenv("TELEFIX_DB_PATH", "").strip()
    if env_path:
        return Path(env_path)
    root = os.getenv("TELEFIX_PROJECT_ROOT", "").strip()
    if root:
        candidate = Path(root) / "data" / "telefix.db"
        if candidate.exists():
            return candidate

    # Project root is the canonical location — try to ensure it exists there
    project_root_db = _REPO_ROOT / "telefix.db"
    if not (project_root_db.exists() and project_root_db.stat().st_size > 0):
        # Attempt to auto-copy from Desktop / any known location (Master behaviour)
        copied = _master_copy_telefix_to_project_root()
        if copied:
            return copied

    for candidate in _TELEFIX_SEARCH_PATHS:
        if candidate.exists():
            return candidate
    # Default: project root (will be created by self-heal if missing)
    return _REPO_ROOT / "telefix.db"

TELEFIX_DB_PATH = _resolve_telefix_db_path()

_MASTER_API_BASE = (
    os.getenv("NEXUS_MASTER_API_BASE") or os.getenv("NEXUS_API_BASE_URL") or ""
).rstrip("/")

# ── Thread-safe singleton registry ────────────────────────────────────────────

_lock = threading.Lock()
_connections: dict[str, sqlite3.Connection] = {}


# ── Telefix DB schema (minimal bootstrap) ─────────────────────────────────────

_TELEFIX_SCHEMA = """
CREATE TABLE IF NOT EXISTS groups (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    title       TEXT,
    username    TEXT,
    invite_link TEXT,
    member_count INTEGER DEFAULT 0,
    warmup_days  INTEGER DEFAULT 0,
    in_search    INTEGER DEFAULT 0,
    verified     INTEGER DEFAULT 0,
    is_israeli   INTEGER DEFAULT 0,
    created_at   TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sessions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    phone        TEXT UNIQUE,
    machine_id   TEXT,
    status       TEXT DEFAULT 'idle',
    last_active  TEXT,
    current_task TEXT,
    created_at   TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS tasks (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id      TEXT UNIQUE,
    task_type    TEXT,
    status       TEXT DEFAULT 'pending',
    fail_count   INTEGER DEFAULT 0,
    last_error   TEXT,
    node_id      TEXT,
    created_at   TEXT DEFAULT (datetime('now')),
    updated_at   TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS scrape_files (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    file       TEXT,
    scraped_at TEXT DEFAULT (datetime('now')),
    row_count  INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS system_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    level      TEXT,
    source     TEXT,
    message    TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
"""


def create_default_db(db_path: Path | None = None) -> sqlite3.Connection:
    """
    Create a fresh telefix.db at *db_path* (defaults to ``TELEFIX_DB_PATH``)
    with the baseline schema applied.

    This is the self-healing entry point — called automatically when the DB
    file is missing and all remote recovery strategies have failed.

    Raises ``RuntimeError`` if the schema migration itself fails; the caller
    is responsible for dispatching a ``CRITICAL_SYSTEM_REPORT`` in that case.
    """
    target = db_path or TELEFIX_DB_PATH
    target.parent.mkdir(parents=True, exist_ok=True)

    msg = f"🛠️ [SELF-HEAL] telefix.db was missing and has been created at: {target}"
    print(msg, flush=True)
    log.warning("db_util.create_default_db — %s", str(target))

    conn = sqlite3.connect(str(target), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(_TELEFIX_SCHEMA)
        conn.commit()
        log.info("db_util.schema_applied — %s", str(target))
    except Exception as exc:
        conn.close()
        raise RuntimeError(
            f"Schema migration failed for {target}: {exc}"
        ) from exc

    # Register in singleton cache
    key = str(target.resolve())
    with _lock:
        _connections[key] = conn

    return conn


# ── Telegram critical reporter ────────────────────────────────────────────────

def _dispatch_critical_report(title: str, body: str) -> None:
    """
    Best-effort: send a CRITICAL_SYSTEM_REPORT to the Telegram admin chat.
    Uses urllib only (no aiogram dependency) so it works even when the full
    stack is not initialised.
    """
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_ADMIN_CHAT_ID", "").strip()
    if not bot_token or not chat_id:
        log.error(
            "db_util.critical_report_skipped",
            reason="TELEGRAM_BOT_TOKEN or TELEGRAM_ADMIN_CHAT_ID not set",
            title=title,
        )
        return

    import urllib.request

    # Truncate to stay under Telegram's 4096-char limit
    safe_body = body[:3500] if len(body) > 3500 else body
    text = (
        f"🚨 *CRITICAL\\_SYSTEM\\_REPORT*\n\n"
        f"*{title.replace('_', chr(92) + '_')}*\n\n"
        f"```\n{safe_body}\n```"
    )
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = json.dumps(
        {"chat_id": chat_id, "text": text, "parse_mode": "MarkdownV2"}
    ).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10):
            log.info("db_util.critical_report_sent", title=title)
    except Exception as exc:
        log.error("db_util.critical_report_failed", error=str(exc), title=title)


# ── Generic DB helpers ────────────────────────────────────────────────────────

def get_db(db_name: str, *, db_dir: Path | None = None) -> sqlite3.Connection:
    """Return (or create) a singleton SQLite connection for *db_name*."""
    db_path = (db_dir or _DATA_DIR) / db_name
    key = str(db_path.resolve())

    with _lock:
        conn = _connections.get(key)
        if conn is None:
            db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(db_path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            _connections[key] = conn
            log.debug("db_util.opened — %s", key)
        return conn


def close_db(db_name: str, *, db_dir: Path | None = None) -> None:
    """Close and remove the singleton connection for *db_name*."""
    db_path = (db_dir or _DATA_DIR) / db_name
    key = str(db_path.resolve())
    with _lock:
        conn = _connections.pop(key, None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ── telefix.db helpers ────────────────────────────────────────────────────────

async def _download_telefix_from_master() -> bool:
    """Try to pull telefix.db from the Master Node REST API."""
    if not _MASTER_API_BASE:
        log.warning("db_util.telefix_sync_skipped — NEXUS_MASTER_API_BASE not set")
        return False
    url = f"{_MASTER_API_BASE}/api/telefix/db/download"
    try:
        import httpx  # lazy import

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            TELEFIX_DB_PATH.write_bytes(resp.content)
            log.info("db_util.telefix_downloaded — %d bytes from %s", len(resp.content), url)
            return True
    except Exception as exc:
        log.warning("db_util.telefix_download_failed — %s — %s", url, str(exc))
        return False


async def _trigger_auto_scrape() -> None:
    """Enqueue a super_scrape task via ARQ so a fresh telefix.db is generated."""
    redis_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    try:
        from arq import create_pool  # type: ignore[import-untyped]
        from arq.connections import RedisSettings  # type: ignore[import-untyped]

        from nexus.shared.schemas import TaskPayload  # noqa: PLC0415

        task = TaskPayload(
            task_type="telegram.auto_scrape",
            parameters={"source": "auto_recovery", "reason": "telefix_db_missing"},
            project_id="telefix",
            priority=5,
        )
        pool = await create_pool(
            RedisSettings.from_dsn(redis_url),
            default_queue_name="nexus:tasks",
        )
        try:
            await pool.enqueue_job(
                "execute_task",
                task_payload=task.model_dump_for_wire(),
                _job_id=task.task_id,
                _queue_name="nexus:tasks",
            )
            log.warning(
                "db_util.auto_scrape_triggered — telefix_db_missing — task_id=%s",
                task.task_id,
            )
        finally:
            await pool.aclose()
    except Exception as exc:
        log.error("db_util.auto_scrape_enqueue_failed — %s", str(exc))


async def ensure_telefix_db() -> Path:
    """Ensure telefix.db exists.

    Resolution order:
    1. Hard-wired absolute path (``TELEFIX_DB_PATH``).
    2. Download from Master Node via ``NEXUS_MASTER_API_BASE`` if set.
    3. ``create_default_db()`` — bootstrap a fresh empty DB with schema.
    4. Enqueue ``telegram.auto_scrape`` via ARQ to populate it.

    If step 3 fails, a ``CRITICAL_SYSTEM_REPORT`` is dispatched via Telegram
    with the full stack trace before re-raising.
    """
    if TELEFIX_DB_PATH.exists() and TELEFIX_DB_PATH.stat().st_size > 0:
        return TELEFIX_DB_PATH

    log.warning("db_util.telefix_missing — %s — not found or empty, attempting master download", str(TELEFIX_DB_PATH))

    # Strategy 2: pull from master
    if await _download_telefix_from_master():
        if TELEFIX_DB_PATH.exists() and TELEFIX_DB_PATH.stat().st_size > 0:
            log.info("db_util.telefix_synced_from_master — %s", str(TELEFIX_DB_PATH))
            return TELEFIX_DB_PATH

    # Strategy 3: self-heal — create a fresh empty DB with schema
    log.warning("db_util.self_heal_triggered — %s — master download failed, bootstrapping fresh DB", str(TELEFIX_DB_PATH))
    try:
        create_default_db(TELEFIX_DB_PATH)
        log.info("db_util.self_heal_success — %s", str(TELEFIX_DB_PATH))
    except Exception:
        tb = traceback.format_exc()
        log.critical("db_util.self_heal_failed — %s", tb)
        _dispatch_critical_report(
            title="TELEFIX_DB_CREATION_FAILED",
            body=(
                f"Path: {TELEFIX_DB_PATH}\n\n"
                f"All recovery strategies exhausted.\n\n"
                f"{tb}"
            ),
        )
        raise FileNotFoundError(
            f"\n\n{'='*60}\n"
            f"FATAL: telefix.db NOT FOUND and self-heal FAILED.\n"
            f"  {TELEFIX_DB_PATH}\n"
            f"A CRITICAL_SYSTEM_REPORT has been dispatched via Telegram.\n"
            f"{'='*60}\n"
        )

    # Strategy 4: trigger auto-scrape so the DB is populated asynchronously
    await _trigger_auto_scrape()

    return TELEFIX_DB_PATH


def get_telefix_db() -> sqlite3.Connection:
    """Return the singleton telefix.db connection.

    If the DB file is missing, ``create_default_db()`` is called automatically
    (self-healing). If that also fails, a ``CRITICAL_SYSTEM_REPORT`` is
    dispatched via Telegram and a ``FileNotFoundError`` is raised.
    """
    if not TELEFIX_DB_PATH.exists() or TELEFIX_DB_PATH.stat().st_size == 0:
        log.warning("db_util.get_telefix_db_missing — %s — triggering create_default_db()", str(TELEFIX_DB_PATH))
        try:
            create_default_db(TELEFIX_DB_PATH)
        except Exception:
            tb = traceback.format_exc()
            log.critical("db_util.get_telefix_db_create_failed — %s", tb)
            _dispatch_critical_report(
                title="TELEFIX_DB_NOT_FOUND",
                body=(
                    f"Path: {TELEFIX_DB_PATH}\n\n"
                    f"get_telefix_db() called but file missing and create_default_db() failed.\n\n"
                    f"{tb}"
                ),
            )
            raise FileNotFoundError(
                f"\n\n{'='*60}\n"
                f"FATAL: telefix.db NOT FOUND at hard-wired path:\n"
                f"  {TELEFIX_DB_PATH}\n"
                f"Self-heal failed. CRITICAL_SYSTEM_REPORT dispatched via Telegram.\n"
                f"{'='*60}\n"
            )

    key = str(TELEFIX_DB_PATH.resolve())
    with _lock:
        conn = _connections.get(key)
        if conn is None:
            conn = sqlite3.connect(str(TELEFIX_DB_PATH), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            _connections[key] = conn
            log.debug("db_util.telefix_opened — %s", key)
        return conn


async def sync_telefix_path_from_master(redis_url: str = "") -> bool:
    """
    Pull the telefix.db path broadcast by the Master Node from Redis and
    update the module-level ``TELEFIX_DB_PATH`` + env var.
    """
    global TELEFIX_DB_PATH  # noqa: PLW0603

    _redis_url = redis_url or os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    try:
        try:
            import redis.asyncio as aioredis  # type: ignore[import-not-found]

            r = aioredis.from_url(_redis_url, decode_responses=True)
            raw = await r.get("nexus:master:telefix_db_path")
            await r.aclose()
            if raw:
                data = json.loads(raw)
                remote_path = data.get("telefix_db_path", "").strip()
                db_exists_on_master = bool(data.get("db_exists", False))
                if remote_path:
                    os.environ["TELEFIX_DB_PATH"] = remote_path
                    TELEFIX_DB_PATH = Path(remote_path)
                    log.info(
                        "db_util.telefix_path_synced_from_master — %s (exists_on_master=%s)",
                        remote_path,
                        db_exists_on_master,
                    )
                    return True
        except ImportError:
            pass
    except Exception as exc:
        log.warning("db_util.telefix_path_sync_failed — %s", str(exc))
    return False


def get_all_telefix_targets() -> list[str]:
    """Return all group/channel usernames/links stored in telefix.db."""
    try:
        conn = get_telefix_db()
        cur = conn.execute(
            "SELECT username FROM groups WHERE username IS NOT NULL "
            "UNION SELECT invite_link FROM groups WHERE invite_link IS NOT NULL"
        )
        return [str(row[0]) for row in cur.fetchall() if row[0]]
    except Exception as exc:
        log.warning("db_util.get_all_targets_failed — %s", str(exc))
        return []


def get_telefix_row_counts() -> dict[str, int]:
    """Return row counts for all tables in telefix.db.

    Used by the UI to determine VERIFIED vs WRITTEN status — if a table has
    rows, the data is considered VERIFIED.
    """
    tables = ["groups", "sessions", "tasks", "scrape_files", "system_events"]
    counts: dict[str, int] = {}
    try:
        conn = get_telefix_db()
        for table in tables:
            try:
                row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                counts[table] = int(row[0]) if row else 0
            except Exception:
                counts[table] = 0
    except Exception as exc:
        log.warning("db_util.row_count_failed — %s", str(exc))
        for table in tables:
            counts[table] = 0
    return counts


def get_verified_status() -> str:
    """Return 'VERIFIED' if the groups table has at least one row, else 'UNVERIFIED'.

    This drives the VERIFIED badge in the UI dashboard.
    """
    try:
        conn = get_telefix_db()
        row = conn.execute("SELECT COUNT(*) FROM groups").fetchone()
        count = int(row[0]) if row else 0
        return "VERIFIED" if count > 0 else "UNVERIFIED"
    except Exception as exc:
        log.warning("db_util.get_verified_status_failed — %s", str(exc))
        return "UNVERIFIED"


def record_task_failure(
    task_id: str,
    task_type: str,
    error: str,
    node_id: str = "",
    *,
    notify_threshold: int = 3,
) -> int:
    """Record a task failure in the tasks table and return the updated fail_count.

    When ``fail_count`` reaches ``notify_threshold`` (default 3), dispatches an
    ERROR_REPORT to the Telegram admin chat via ``_dispatch_critical_report``.
    """
    try:
        conn = get_telefix_db()
        conn.execute(
            """
            INSERT INTO tasks (task_id, task_type, status, fail_count, last_error, node_id, updated_at)
            VALUES (?, ?, 'failed', 1, ?, ?, datetime('now'))
            ON CONFLICT(task_id) DO UPDATE SET
                fail_count = fail_count + 1,
                last_error = excluded.last_error,
                status     = 'failed',
                updated_at = datetime('now')
            """,
            (task_id, task_type, error[:2000], node_id),
        )
        conn.commit()
        row = conn.execute(
            "SELECT fail_count FROM tasks WHERE task_id = ?", (task_id,)
        ).fetchone()
        fail_count = int(row[0]) if row else 1
    except Exception as exc:
        log.warning("db_util.record_task_failure_db_error — %s", str(exc))
        fail_count = 1

    if fail_count >= notify_threshold:
        _dispatch_critical_report(
            title="ERROR_REPORT",
            body=(
                f"Task '{task_type}' (id={task_id}) has failed {fail_count} times.\n"
                f"Node: {node_id or 'unknown'}\n\n"
                f"Last error:\n{error[:1500]}"
            ),
        )
        log.error(
            "db_util.task_failure_threshold_reached",
            task_id=task_id,
            task_type=task_type,
            fail_count=fail_count,
            node_id=node_id,
        )

    return fail_count


def is_db_verified() -> bool:
    """
    Return True ONLY if ``SELECT COUNT(*) FROM groups`` > 0.

    This is the authoritative check for the UI 'VERIFIED' badge.
    A freshly bootstrapped (empty) DB returns False even though the schema exists.
    """
    try:
        conn = get_telefix_db()
        row = conn.execute("SELECT COUNT(*) FROM groups").fetchone()
        return bool(row and int(row[0]) > 0)
    except Exception as exc:
        log.warning("db_util.is_db_verified_failed — %s", str(exc))
        return False


def get_db_health() -> dict[str, object]:
    """
    Full health snapshot for the UI dashboard.

    Returns:
        db_found    : bool — DB file exists and is non-empty
        db_path     : str  — resolved path
        tables      : dict[str, int] — row counts per table
        verified    : bool — groups table has at least 1 row (REAL data check)
        written     : bool — any table has at least 1 row
        total_rows  : int  — sum of all row counts
    """
    found = TELEFIX_DB_PATH.exists() and TELEFIX_DB_PATH.stat().st_size > 0
    counts = get_telefix_row_counts() if found else {}
    total = sum(counts.values())
    verified = counts.get("groups", 0) > 0
    written = total > 0
    return {
        "db_found": found,
        "db_path": str(TELEFIX_DB_PATH),
        "tables": counts,
        "verified": verified,
        "written": written,
        "total_rows": total,
    }
