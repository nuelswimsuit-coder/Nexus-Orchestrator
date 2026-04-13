"""
nexus/shared/repository.py — Repository Pattern (Sessions, Tasks, Workers)
===========================================================================

Provides a clean data-access layer on top of Redis (L1/L2 caches) and
SQLite (durable storage for sessions and tasks).

Architecture
------------
- ``BaseRepository[T]``     Abstract generic base with common helpers.
- ``SessionRepository``     Read-through L1 (process TTLMemoryCache) → L2 (Redis)
                            → L3 (SQLite).  Write-through on save.
- ``TaskRepository``        Redis-backed task state + DLQ inspection.
- ``WorkerRepository``      Heartbeat and load tracking via Redis with TTL keys.
- ``DataStore``             Unified facade: ``store.sessions``, ``store.tasks``,
                            ``store.workers``.

Module-level singleton
    ``store = DataStore()`` — import and use directly.
"""

from __future__ import annotations

import json
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Generic, TypeVar

import aiosqlite
import structlog

from nexus.shared.memory_cache import TTLMemoryCache

log = structlog.get_logger(__name__)

T = TypeVar("T")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_redis() -> Any:
    """Lazy-load the process-wide Redis client."""
    import nexus.shared._redis_pool as _pool  # noqa: PLC0415
    return _pool.get_client()


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / ".git").exists() or (parent / "pyproject.toml").exists():
            return parent
    return here.parents[2]


_DATA_DIR = _repo_root() / "data"
_DATA_DIR.mkdir(parents=True, exist_ok=True)

_SESSIONS_DB  = _DATA_DIR / "nexus_sessions.db"
_TASKS_DB     = _DATA_DIR / "nexus_tasks.db"

_SCHEMA_SESSIONS = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    data_json  TEXT NOT NULL,
    updated_at REAL NOT NULL
);
"""

_SCHEMA_TASKS = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;
CREATE TABLE IF NOT EXISTS tasks (
    task_id    TEXT PRIMARY KEY,
    status     TEXT NOT NULL DEFAULT 'pending',
    data_json  TEXT NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks (status);
"""


async def _ensure_sessions_schema(db: aiosqlite.Connection) -> None:
    await db.executescript(_SCHEMA_SESSIONS)
    await db.commit()


async def _ensure_tasks_schema(db: aiosqlite.Connection) -> None:
    await db.executescript(_SCHEMA_TASKS)
    await db.commit()


# ---------------------------------------------------------------------------
# BaseRepository
# ---------------------------------------------------------------------------

class BaseRepository(ABC, Generic[T]):
    """
    Abstract generic repository.

    Concrete subclasses implement ``get``, ``save``, and ``delete``.
    Common helpers (JSON encode/decode, Redis key builders) live here.
    """

    # Subclasses set this to namespace Redis keys.
    _prefix: str = "nexus:repo"

    def _redis_key(self, *parts: str) -> str:
        return ":".join([self._prefix, *parts])

    @staticmethod
    def _encode(data: dict[str, Any]) -> str:
        return json.dumps(data, default=str)

    @staticmethod
    def _decode(raw: str | bytes) -> dict[str, Any]:
        if isinstance(raw, bytes):
            raw = raw.decode()
        return json.loads(raw)

    @abstractmethod
    async def get(self, key: str) -> T | None: ...

    @abstractmethod
    async def save(self, key: str, data: T) -> None: ...

    @abstractmethod
    async def delete(self, key: str) -> None: ...


# ---------------------------------------------------------------------------
# SessionRepository
# ---------------------------------------------------------------------------

# L1: process-local TTL cache (5 s)  — avoids hammering Redis on hot paths.
_SESSION_L1: TTLMemoryCache[dict[str, Any]] = TTLMemoryCache(max_entries=4096)
_SESSION_L1_TTL = 5.0  # seconds
_SESSION_REDIS_TTL = 3600  # 1 hour


class SessionRepository(BaseRepository[dict[str, Any]]):
    """
    Three-tier read-through cache for Telegram sessions.

    L1  — Process-local :class:`~nexus.shared.memory_cache.TTLMemoryCache`
    L2  — Redis hash (``nexus:session:<session_id>``) with 1-hour TTL
    L3  — SQLite ``nexus_sessions.db`` for durability across restarts
    """

    _prefix = "nexus:session"

    # ------------------------------------------------------------------
    # Read path: L1 → L2 → L3
    # ------------------------------------------------------------------

    async def get(self, session_id: str) -> dict[str, Any] | None:
        """Return session data or None if not found."""
        # L1
        cached = _SESSION_L1.get(session_id)
        if cached is not None:
            return cached

        # L2
        r = _get_redis()
        raw = await r.get(self._redis_key(session_id))
        if raw is not None:
            data = self._decode(raw)
            _SESSION_L1.set(session_id, data, ttl=_SESSION_L1_TTL)
            return data

        # L3
        data = await self._sqlite_get(session_id)
        if data is not None:
            # Warm L2
            await r.setex(
                self._redis_key(session_id),
                _SESSION_REDIS_TTL,
                self._encode(data),
            )
            _SESSION_L1.set(session_id, data, ttl=_SESSION_L1_TTL)
        return data

    async def _sqlite_get(self, session_id: str) -> dict[str, Any] | None:
        try:
            async with aiosqlite.connect(str(_SESSIONS_DB)) as db:
                await _ensure_sessions_schema(db)
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    "SELECT data_json FROM sessions WHERE session_id = ?",
                    (session_id,),
                ) as cur:
                    row = await cur.fetchone()
                    if row is None:
                        return None
                    return json.loads(row["data_json"])
        except Exception as exc:  # noqa: BLE001
            log.error("session_repo.sqlite_get_failed", session_id=session_id, exc=str(exc))
            return None

    # ------------------------------------------------------------------
    # Write path: write-through
    # ------------------------------------------------------------------

    async def save(self, session_id: str, data: dict[str, Any]) -> None:
        """Persist session data to L1, L2, and L3."""
        # L1
        _SESSION_L1.set(session_id, data, ttl=_SESSION_L1_TTL)

        # L2
        r = _get_redis()
        await r.setex(
            self._redis_key(session_id),
            _SESSION_REDIS_TTL,
            self._encode(data),
        )

        # L3
        await self._sqlite_save(session_id, data)

    async def _sqlite_save(self, session_id: str, data: dict[str, Any]) -> None:
        try:
            async with aiosqlite.connect(str(_SESSIONS_DB)) as db:
                await _ensure_sessions_schema(db)
                await db.execute(
                    """
                    INSERT INTO sessions (session_id, data_json, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(session_id) DO UPDATE SET
                        data_json  = excluded.data_json,
                        updated_at = excluded.updated_at
                    """,
                    (session_id, self._encode(data), time.time()),
                )
                await db.commit()
        except Exception as exc:  # noqa: BLE001
            log.error("session_repo.sqlite_save_failed", session_id=session_id, exc=str(exc))

    async def delete(self, session_id: str) -> None:
        """Remove session from all tiers."""
        _SESSION_L1.delete(session_id)
        r = _get_redis()
        await r.delete(self._redis_key(session_id))
        try:
            async with aiosqlite.connect(str(_SESSIONS_DB)) as db:
                await _ensure_sessions_schema(db)
                await db.execute(
                    "DELETE FROM sessions WHERE session_id = ?", (session_id,)
                )
                await db.commit()
        except Exception as exc:  # noqa: BLE001
            log.error("session_repo.delete_failed", session_id=session_id, exc=str(exc))

    async def list_active(self) -> list[dict[str, Any]]:
        """Return all sessions from SQLite (source of truth for active sessions)."""
        try:
            async with aiosqlite.connect(str(_SESSIONS_DB)) as db:
                await _ensure_sessions_schema(db)
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    "SELECT data_json FROM sessions ORDER BY updated_at DESC"
                ) as cur:
                    rows = await cur.fetchall()
                    return [json.loads(r["data_json"]) for r in rows]
        except Exception as exc:  # noqa: BLE001
            log.error("session_repo.list_active_failed", exc=str(exc))
            return []


# ---------------------------------------------------------------------------
# TaskRepository
# ---------------------------------------------------------------------------

_TASK_REDIS_TTL = 86400  # 24 hours


class TaskRepository(BaseRepository[dict[str, Any]]):
    """
    Redis-primary task state with SQLite durability.

    Keys
    ----
    ``nexus:task:<task_id>``         — individual task JSON
    ``nexus:task:by_status:<status>``— sorted set (score = created_at timestamp)
    """

    _prefix = "nexus:task"

    def _status_key(self, status: str) -> str:
        return f"nexus:task:by_status:{status}"

    async def get(self, task_id: str) -> dict[str, Any] | None:
        r = _get_redis()
        raw = await r.get(self._redis_key(task_id))
        if raw is not None:
            return self._decode(raw)
        # Fallback to SQLite
        return await self._sqlite_get(task_id)

    async def _sqlite_get(self, task_id: str) -> dict[str, Any] | None:
        try:
            async with aiosqlite.connect(str(_TASKS_DB)) as db:
                await _ensure_tasks_schema(db)
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    "SELECT data_json FROM tasks WHERE task_id = ?", (task_id,)
                ) as cur:
                    row = await cur.fetchone()
                    if row is None:
                        return None
                    return json.loads(row["data_json"])
        except Exception as exc:  # noqa: BLE001
            log.error("task_repo.sqlite_get_failed", task_id=task_id, exc=str(exc))
            return None

    async def save(self, task_id: str, data: dict[str, Any]) -> None:
        """Write task to Redis + SQLite and maintain status index."""
        r = _get_redis()

        # Remove from old status bucket if status changed
        old_raw = await r.get(self._redis_key(task_id))
        if old_raw is not None:
            old_data = self._decode(old_raw)
            old_status = old_data.get("status")
            new_status = data.get("status")
            if old_status and old_status != new_status:
                await r.zrem(self._status_key(old_status), task_id)

        encoded = self._encode(data)
        await r.setex(self._redis_key(task_id), _TASK_REDIS_TTL, encoded)

        # Maintain status sorted set (score = created_at or now)
        status = data.get("status", "pending")
        score = data.get("created_at_ts") or time.time()
        await r.zadd(self._status_key(status), {task_id: score})

        # SQLite durability
        await self._sqlite_save(task_id, data)

    async def _sqlite_save(self, task_id: str, data: dict[str, Any]) -> None:
        try:
            async with aiosqlite.connect(str(_TASKS_DB)) as db:
                await _ensure_tasks_schema(db)
                await db.execute(
                    """
                    INSERT INTO tasks (task_id, status, data_json, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(task_id) DO UPDATE SET
                        status     = excluded.status,
                        data_json  = excluded.data_json,
                        updated_at = excluded.updated_at
                    """,
                    (
                        task_id,
                        data.get("status", "pending"),
                        self._encode(data),
                        time.time(),
                    ),
                )
                await db.commit()
        except Exception as exc:  # noqa: BLE001
            log.error("task_repo.sqlite_save_failed", task_id=task_id, exc=str(exc))

    async def delete(self, task_id: str) -> None:
        r = _get_redis()
        raw = await r.get(self._redis_key(task_id))
        if raw is not None:
            data = self._decode(raw)
            status = data.get("status", "pending")
            await r.zrem(self._status_key(status), task_id)
        await r.delete(self._redis_key(task_id))
        try:
            async with aiosqlite.connect(str(_TASKS_DB)) as db:
                await _ensure_tasks_schema(db)
                await db.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
                await db.commit()
        except Exception as exc:  # noqa: BLE001
            log.error("task_repo.delete_failed", task_id=task_id, exc=str(exc))

    async def get_by_status(self, status: str) -> list[dict[str, Any]]:
        """Return all task dicts with the given *status*, ordered oldest-first."""
        r = _get_redis()
        task_ids = await r.zrange(self._status_key(status), 0, -1)
        if not task_ids:
            # Fallback to SQLite
            return await self._sqlite_get_by_status(status)
        results: list[dict[str, Any]] = []
        for tid in task_ids:
            if isinstance(tid, bytes):
                tid = tid.decode()
            raw = await r.get(self._redis_key(tid))
            if raw is not None:
                results.append(self._decode(raw))
        return results

    async def _sqlite_get_by_status(self, status: str) -> list[dict[str, Any]]:
        try:
            async with aiosqlite.connect(str(_TASKS_DB)) as db:
                await _ensure_tasks_schema(db)
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    "SELECT data_json FROM tasks WHERE status = ? ORDER BY updated_at ASC",
                    (status,),
                ) as cur:
                    rows = await cur.fetchall()
                    return [json.loads(r["data_json"]) for r in rows]
        except Exception as exc:  # noqa: BLE001
            log.error("task_repo.sqlite_by_status_failed", status=status, exc=str(exc))
            return []

    async def get_dlq_items(self, limit: int = 50) -> list[dict[str, Any]]:
        """Return up to *limit* items from the Dead-Letter Queue."""
        from nexus.shared.circuit_breaker import dlq  # noqa: PLC0415
        return await dlq.drain(limit=limit)


# ---------------------------------------------------------------------------
# WorkerRepository
# ---------------------------------------------------------------------------

class WorkerRepository(BaseRepository[dict[str, Any]]):
    """
    Heartbeat and load tracking for worker nodes.

    All data is Redis-only — heartbeats are short-lived and not worth
    persisting to SQLite.

    Keys
    ----
    ``nexus:worker:hb:<worker_id>``   — heartbeat JSON (TTL = 60 s)
    ``nexus:worker:load:<worker_id>`` — float string (TTL = 10 s)
    ``nexus:worker:registry``         — sorted set of worker_ids, score = last seen
    """

    _prefix = "nexus:worker"

    def _hb_key(self, worker_id: str) -> str:
        return f"nexus:worker:hb:{worker_id}"

    def _load_key(self, worker_id: str) -> str:
        return f"nexus:worker:load:{worker_id}"

    _REGISTRY_KEY = "nexus:worker:registry"

    async def get(self, worker_id: str) -> dict[str, Any] | None:
        return await self.get_heartbeat(worker_id)

    async def save(self, worker_id: str, data: dict[str, Any]) -> None:
        await self.save_heartbeat(worker_id, data)

    async def delete(self, worker_id: str) -> None:
        r = _get_redis()
        await r.delete(self._hb_key(worker_id), self._load_key(worker_id))
        await r.zrem(self._REGISTRY_KEY, worker_id)

    async def get_heartbeat(self, worker_id: str) -> dict[str, Any] | None:
        r = _get_redis()
        raw = await r.get(self._hb_key(worker_id))
        if raw is None:
            return None
        return json.loads(raw if isinstance(raw, str) else raw.decode())

    async def save_heartbeat(
        self,
        worker_id: str,
        data: dict[str, Any],
        ttl: int = 60,
    ) -> None:
        r = _get_redis()
        encoded = json.dumps(data, default=str)
        await r.setex(self._hb_key(worker_id), ttl, encoded)
        # Keep registry entry alive (score = unix timestamp for ordering)
        await r.zadd(self._REGISTRY_KEY, {worker_id: time.time()})

    async def list_alive(self) -> list[dict[str, Any]]:
        """
        Return heartbeat dicts for workers whose Redis key is still alive.

        Workers whose heartbeat TTL has expired are automatically excluded —
        no explicit TTL comparison needed because Redis already deleted the key.
        """
        r = _get_redis()
        # All worker_ids registered in the last 24 h
        cutoff = time.time() - 86400
        worker_ids = await r.zrangebyscore(self._REGISTRY_KEY, cutoff, "+inf")
        results: list[dict[str, Any]] = []
        for wid in worker_ids:
            if isinstance(wid, bytes):
                wid = wid.decode()
            hb = await self.get_heartbeat(wid)
            if hb is not None:  # Key alive → worker alive
                results.append(hb)
            else:
                # Clean up stale registry entry
                await r.zrem(self._REGISTRY_KEY, wid)
        return results

    async def get_load(self, worker_id: str) -> float:
        r = _get_redis()
        raw = await r.get(self._load_key(worker_id))
        if raw is None:
            return 0.0
        try:
            return float(raw.decode() if isinstance(raw, bytes) else raw)
        except ValueError:
            return 0.0

    async def set_load(self, worker_id: str, load: float, ttl: int = 10) -> None:
        r = _get_redis()
        await r.setex(self._load_key(worker_id), ttl, str(load))


# ---------------------------------------------------------------------------
# DataStore — unified facade
# ---------------------------------------------------------------------------

class DataStore:
    """
    Single entry point for all repository access.

    Usage::

        from nexus.shared.repository import store

        session = await store.sessions.get("abc-123")
        task    = await store.tasks.get("task-uuid")
        workers = await store.workers.list_alive()
    """

    def __init__(self) -> None:
        self.sessions = SessionRepository()
        self.tasks    = TaskRepository()
        self.workers  = WorkerRepository()

    async def health_check(self) -> dict[str, Any]:
        """
        Return a dict with liveness status for Redis and SQLite.

        Suitable for use in a ``/health`` or ``/readyz`` endpoint.
        """
        result: dict[str, Any] = {
            "redis": False,
            "sqlite_sessions": False,
            "sqlite_tasks": False,
            "timestamp": time.time(),
        }

        # Redis ping
        try:
            r = _get_redis()
            pong = await r.ping()
            result["redis"] = pong is True or pong == b"PONG" or str(pong).upper() == "PONG"
        except Exception as exc:  # noqa: BLE001
            result["redis_error"] = str(exc)

        # SQLite sessions
        try:
            async with aiosqlite.connect(str(_SESSIONS_DB)) as db:
                await db.execute("SELECT 1")
            result["sqlite_sessions"] = True
        except Exception as exc:  # noqa: BLE001
            result["sqlite_sessions_error"] = str(exc)

        # SQLite tasks
        try:
            async with aiosqlite.connect(str(_TASKS_DB)) as db:
                await db.execute("SELECT 1")
            result["sqlite_tasks"] = True
        except Exception as exc:  # noqa: BLE001
            result["sqlite_tasks_error"] = str(exc)

        result["healthy"] = all(
            [result["redis"], result["sqlite_sessions"], result["sqlite_tasks"]]
        )
        log.debug("datastore.health_check", **{k: v for k, v in result.items()})
        return result


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

store = DataStore()

__all__ = [
    "BaseRepository",
    "SessionRepository",
    "TaskRepository",
    "WorkerRepository",
    "DataStore",
    "store",
]
