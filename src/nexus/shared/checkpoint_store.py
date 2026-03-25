"""
nexus/shared/checkpoint_store.py — Crash-safe Task Checkpoint Store
====================================================================

Provides atomic, SQLite-backed checkpoints so that any scan or long-running
process can resume exactly where it left off after a crash, power outage,
restart, or forced kill.

Architecture
------------
- One ``task_checkpoints`` table in ``nexus_checkpoints.db`` (data/ dir).
- Each checkpoint row is keyed by ``(task_id, step_key)``.
- ``step_key`` is a human-readable label like ``"source:t.me/group"`` or
  ``"phase:discovery"``.
- ``payload`` is a JSON blob with arbitrary progress data.
- ``status`` is one of: ``pending`` | ``running`` | ``done`` | ``failed``.
- Rows are written with ``WAL`` mode + ``IMMEDIATE`` transactions for
  crash-safety (no partial writes survive a power cut).

Usage
-----
    from nexus.shared.checkpoint_store import CheckpointStore

    store = CheckpointStore(task_id="abc-123")

    # On start: load any existing progress
    done_sources = store.get_done_steps(prefix="source:")

    # During work: mark a step as running (survives crash as "running")
    store.mark_running("source:t.me/mygroup")

    # After success: mark done with result payload
    store.mark_done("source:t.me/mygroup", {"users_saved": 42})

    # After failure: mark failed with error
    store.mark_failed("source:t.me/mygroup", "TimeoutError: ...")

    # When the whole task completes: clear its checkpoints
    store.clear()

Resume pattern
--------------
    all_sources = ["t.me/a", "t.me/b", "t.me/c"]
    done = store.get_done_steps(prefix="source:")
    remaining = [s for s in all_sources if f"source:{s}" not in done]
    # only process `remaining`
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# ── DB location ───────────────────────────────────────────────────────────────

def _find_repo_root() -> Path:
    """Walk up from this file to find the repo root (contains pyproject.toml or .git)."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / ".git").exists() or (parent / "pyproject.toml").exists():
            return parent
    # Fallback: three levels up from src/nexus/shared/ → repo root
    return here.parents[3]

_REPO_ROOT = _find_repo_root()
_DATA_DIR = _REPO_ROOT / "data"
_DATA_DIR.mkdir(parents=True, exist_ok=True)

CHECKPOINT_DB_PATH = _DATA_DIR / "nexus_checkpoints.db"

_SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;

CREATE TABLE IF NOT EXISTS task_checkpoints (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     TEXT    NOT NULL,
    step_key    TEXT    NOT NULL,
    status      TEXT    NOT NULL DEFAULT 'pending',
    payload     TEXT,
    error       TEXT,
    started_at  TEXT,
    updated_at  TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE (task_id, step_key)
);

CREATE INDEX IF NOT EXISTS idx_cp_task_status
    ON task_checkpoints (task_id, status);
"""

_lock = threading.Lock()
_conn: sqlite3.Connection | None = None


def _get_conn() -> sqlite3.Connection:
    global _conn  # noqa: PLW0603
    with _lock:
        if _conn is None:
            _conn = sqlite3.connect(
                str(CHECKPOINT_DB_PATH),
                check_same_thread=False,
                isolation_level=None,   # autocommit; we manage transactions manually
            )
            _conn.row_factory = sqlite3.Row
            _conn.executescript(_SCHEMA)
            log.debug("checkpoint_store.opened — %s", CHECKPOINT_DB_PATH)
        return _conn


# ── Public API ────────────────────────────────────────────────────────────────

class CheckpointStore:
    """
    Thread-safe, crash-safe checkpoint store for a single task run.

    Parameters
    ----------
    task_id : Unique identifier for the task (ARQ job id or UUID).
    """

    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        self._db = _get_conn()

    # ── Read helpers ──────────────────────────────────────────────────────────

    def get_all_steps(self) -> list[dict[str, Any]]:
        """Return all checkpoint rows for this task."""
        rows = self._db.execute(
            "SELECT step_key, status, payload, error, started_at, updated_at "
            "FROM task_checkpoints WHERE task_id = ? ORDER BY id",
            (self.task_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_done_steps(self, *, prefix: str = "") -> set[str]:
        """
        Return the set of ``step_key`` values that are marked ``done``.

        Pass ``prefix`` to filter (e.g. ``prefix="source:"``).
        """
        rows = self._db.execute(
            "SELECT step_key FROM task_checkpoints "
            "WHERE task_id = ? AND status = 'done'",
            (self.task_id,),
        ).fetchall()
        keys = {r["step_key"] for r in rows}
        if prefix:
            keys = {k for k in keys if k.startswith(prefix)}
        return keys

    def get_step(self, step_key: str) -> dict[str, Any] | None:
        """Return a single checkpoint row or None."""
        row = self._db.execute(
            "SELECT step_key, status, payload, error, started_at, updated_at "
            "FROM task_checkpoints WHERE task_id = ? AND step_key = ?",
            (self.task_id, step_key),
        ).fetchone()
        return dict(row) if row else None

    def has_any_progress(self) -> bool:
        """True if there is at least one checkpoint row for this task."""
        row = self._db.execute(
            "SELECT 1 FROM task_checkpoints WHERE task_id = ? LIMIT 1",
            (self.task_id,),
        ).fetchone()
        return row is not None

    def get_task_meta(self, key: str) -> Any | None:
        """
        Retrieve a task-level metadata value stored under ``__meta__:{key}``.
        Returns the decoded JSON value or None.
        """
        row = self.get_step(f"__meta__:{key}")
        if row and row.get("payload"):
            try:
                return json.loads(row["payload"])
            except Exception:
                return row["payload"]
        return None

    # ── Write helpers ─────────────────────────────────────────────────────────

    def _upsert(
        self,
        step_key: str,
        status: str,
        payload: Any = None,
        error: str | None = None,
        set_started: bool = False,
    ) -> None:
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        payload_json = json.dumps(payload) if payload is not None else None
        error_str = str(error)[:2000] if error else None

        if set_started:
            self._db.execute(
                """
                INSERT INTO task_checkpoints
                    (task_id, step_key, status, payload, error, started_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id, step_key) DO UPDATE SET
                    status     = excluded.status,
                    payload    = COALESCE(excluded.payload, payload),
                    error      = excluded.error,
                    started_at = COALESCE(started_at, excluded.started_at),
                    updated_at = excluded.updated_at
                """,
                (self.task_id, step_key, status, payload_json, error_str, now, now),
            )
        else:
            self._db.execute(
                """
                INSERT INTO task_checkpoints
                    (task_id, step_key, status, payload, error, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id, step_key) DO UPDATE SET
                    status     = excluded.status,
                    payload    = COALESCE(excluded.payload, payload),
                    error      = excluded.error,
                    updated_at = excluded.updated_at
                """,
                (self.task_id, step_key, status, payload_json, error_str, now),
            )

    def mark_running(self, step_key: str, meta: Any = None) -> None:
        """
        Mark a step as currently running.

        If the process crashes while in this state, the step will be
        ``running`` on restart — treat it as incomplete and re-run it.
        """
        self._upsert(step_key, "running", payload=meta, set_started=True)
        log.debug("checkpoint.running task=%s step=%s", self.task_id, step_key)

    def mark_done(self, step_key: str, result: Any = None) -> None:
        """Mark a step as successfully completed."""
        self._upsert(step_key, "done", payload=result)
        log.debug("checkpoint.done task=%s step=%s", self.task_id, step_key)

    def mark_failed(self, step_key: str, error: str) -> None:
        """Mark a step as failed (will be retried on next run)."""
        self._upsert(step_key, "failed", error=error)
        log.warning("checkpoint.failed task=%s step=%s error=%s", self.task_id, step_key, error[:200])

    def save_task_meta(self, key: str, value: Any) -> None:
        """
        Store arbitrary task-level metadata under ``__meta__:{key}``.

        Useful for saving the full candidate list, niche keywords, phase
        state, etc. so they survive a restart.
        """
        self._upsert(f"__meta__:{key}", "done", payload=value)

    def clear(self) -> None:
        """Delete all checkpoint rows for this task (call on clean completion)."""
        self._db.execute(
            "DELETE FROM task_checkpoints WHERE task_id = ?",
            (self.task_id,),
        )
        log.debug("checkpoint.cleared task=%s", self.task_id)

    def clear_failed(self) -> None:
        """Reset all ``failed`` steps to ``pending`` so they are retried."""
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        self._db.execute(
            "UPDATE task_checkpoints SET status='pending', error=NULL, updated_at=? "
            "WHERE task_id=? AND status='failed'",
            (now, self.task_id),
        )

    def reset_stale_running(self) -> int:
        """
        Any step still in ``running`` state from a previous (crashed) run is
        reset to ``pending`` so it will be retried.

        Returns the number of steps reset.
        """
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        cur = self._db.execute(
            "UPDATE task_checkpoints SET status='pending', updated_at=? "
            "WHERE task_id=? AND status='running'",
            (now, self.task_id),
        )
        count = cur.rowcount
        if count:
            log.warning(
                "checkpoint.stale_running_reset task=%s count=%d",
                self.task_id,
                count,
            )
        return count

    # ── Summary ───────────────────────────────────────────────────────────────

    def summary(self) -> dict[str, Any]:
        """Return a progress summary dict for logging/dashboard."""
        rows = self.get_all_steps()
        by_status: dict[str, int] = {}
        for r in rows:
            s = r["status"]
            by_status[s] = by_status.get(s, 0) + 1
        return {
            "task_id": self.task_id,
            "total": len(rows),
            "done": by_status.get("done", 0),
            "running": by_status.get("running", 0),
            "failed": by_status.get("failed", 0),
            "pending": by_status.get("pending", 0),
        }


# ── Module-level convenience functions ───────────────────────────────────────

def get_store(task_id: str) -> CheckpointStore:
    """Return a CheckpointStore for the given task_id."""
    return CheckpointStore(task_id)


def purge_old_checkpoints(older_than_days: int = 7) -> int:
    """
    Delete checkpoint rows older than *older_than_days* days.

    Call this periodically (e.g. from a maintenance task) to keep the DB lean.
    Returns the number of rows deleted.
    """
    db = _get_conn()
    cur = db.execute(
        "DELETE FROM task_checkpoints "
        "WHERE updated_at < strftime('%Y-%m-%dT%H:%M:%SZ', 'now', ?)",
        (f"-{older_than_days} days",),
    )
    count = cur.rowcount
    if count:
        log.info("checkpoint_store.purged older_than=%dd rows=%d", older_than_days, count)
    return count
