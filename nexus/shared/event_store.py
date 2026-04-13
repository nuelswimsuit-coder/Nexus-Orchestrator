"""
nexus/shared/event_store.py — Event Sourcing Store
====================================================

Implements an append-only event log with dual-write:

Redis Stream (``nexus:events``)
    Fast recent-event access via XADD/XRANGE/XREVRANGE.  A background task
    prunes the stream to the last 10,000 entries to bound memory usage.

SQLite (``nexus_events.db``)
    Durable storage for replay and long-lived queries.  Uses WAL + NORMAL
    synchronous mode for crash-safety without write latency.

Public surface
--------------
``EventType``     — enum of all domain event types
``Event``         — Pydantic v2 model (validated, serialisable)
``EventStore``    — async store with append / query / replay methods
``emit(...)``     — convenience helper that creates + appends an Event
``event_store``   — module-level singleton

Usage::

    from nexus.shared.event_store import emit, EventType

    ev = await emit(
        EventType.TASK_DISPATCHED,
        trace_id="abc",
        task_id="xyz",
        payload={"worker": "w1"},
    )
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

import aiosqlite
import structlog
from pydantic import BaseModel, Field

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Redis helper
# ---------------------------------------------------------------------------

def _get_redis() -> Any:
    import nexus.shared._redis_pool as _pool  # noqa: PLC0415
    return _pool.get_client()


# ---------------------------------------------------------------------------
# DB path
# ---------------------------------------------------------------------------

def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / ".git").exists() or (parent / "pyproject.toml").exists():
            return parent
    return here.parents[2]


_DATA_DIR = _repo_root() / "data"
_DATA_DIR.mkdir(parents=True, exist_ok=True)
_EVENTS_DB = _DATA_DIR / "nexus_events.db"

_SCHEMA_EVENTS = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;
CREATE TABLE IF NOT EXISTS events (
    event_id    TEXT PRIMARY KEY,
    event_type  TEXT NOT NULL,
    trace_id    TEXT NOT NULL,
    session_id  TEXT,
    task_id     TEXT,
    worker_id   TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    timestamp   REAL NOT NULL,
    version     INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_events_trace    ON events (trace_id);
CREATE INDEX IF NOT EXISTS idx_events_session  ON events (session_id);
CREATE INDEX IF NOT EXISTS idx_events_task     ON events (task_id);
CREATE INDEX IF NOT EXISTS idx_events_ts       ON events (timestamp);
CREATE INDEX IF NOT EXISTS idx_events_type     ON events (event_type);
"""

_REDIS_STREAM_KEY  = "nexus:events"
_STREAM_MAX_LEN    = 10_000  # entries kept in Redis stream


# ---------------------------------------------------------------------------
# EventType
# ---------------------------------------------------------------------------

class EventType(str, Enum):
    TASK_DISPATCHED      = "TASK_DISPATCHED"
    TASK_STARTED         = "TASK_STARTED"
    TASK_COMPLETED       = "TASK_COMPLETED"
    TASK_FAILED          = "TASK_FAILED"
    TASK_REJECTED        = "TASK_REJECTED"
    HITL_REQUESTED       = "HITL_REQUESTED"
    HITL_APPROVED        = "HITL_APPROVED"
    HITL_REJECTED        = "HITL_REJECTED"
    WORKER_CONNECTED     = "WORKER_CONNECTED"
    WORKER_DISCONNECTED  = "WORKER_DISCONNECTED"
    CIRCUIT_OPENED       = "CIRCUIT_OPENED"
    CIRCUIT_CLOSED       = "CIRCUIT_CLOSED"
    SESSION_CREATED      = "SESSION_CREATED"
    SESSION_UPDATED      = "SESSION_UPDATED"
    SYSTEM_ALERT         = "SYSTEM_ALERT"


# ---------------------------------------------------------------------------
# Event model
# ---------------------------------------------------------------------------

class Event(BaseModel):
    """
    Immutable domain event.  All fields except ``payload`` are indexed for
    fast retrieval; ``payload`` carries event-specific data.
    """

    event_id:   str        = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    trace_id:   str
    session_id: str | None = None
    task_id:    str | None = None
    worker_id:  str | None = None
    payload:    dict[str, Any] = Field(default_factory=dict)
    timestamp:  datetime   = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    version:    int        = 1

    model_config = {"frozen": True}

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    def to_redis_fields(self) -> dict[str, str]:
        """Flat string dict for XADD field list."""
        return {
            "event_id":    self.event_id,
            "event_type":  self.event_type.value,
            "trace_id":    self.trace_id,
            "session_id":  self.session_id or "",
            "task_id":     self.task_id    or "",
            "worker_id":   self.worker_id  or "",
            "payload_json": json.dumps(self.payload, default=str),
            "timestamp":   self.timestamp.isoformat(),
            "version":     str(self.version),
        }

    @classmethod
    def from_redis_fields(cls, fields: dict[bytes | str, bytes | str]) -> "Event":
        """Reconstruct an Event from XRANGE/XREVRANGE entry fields."""
        def _s(v: bytes | str) -> str:
            return v.decode() if isinstance(v, bytes) else v

        def _key(k: bytes | str) -> str:
            return k.decode() if isinstance(k, bytes) else k

        f = {_key(k): _s(v) for k, v in fields.items()}
        ts_raw = f.get("timestamp", "")
        try:
            ts = datetime.fromisoformat(ts_raw)
        except (ValueError, TypeError):
            ts = datetime.now(timezone.utc)

        return cls(
            event_id   = f.get("event_id",   str(uuid.uuid4())),
            event_type = EventType(f.get("event_type", "SYSTEM_ALERT")),
            trace_id   = f.get("trace_id",   ""),
            session_id = f.get("session_id") or None,
            task_id    = f.get("task_id")    or None,
            worker_id  = f.get("worker_id")  or None,
            payload    = json.loads(f.get("payload_json", "{}")),
            timestamp  = ts,
            version    = int(f.get("version", 1)),
        )

    @classmethod
    def from_sqlite_row(cls, row: aiosqlite.Row) -> "Event":
        """Reconstruct an Event from a SQLite row."""
        ts_raw = row["timestamp"]
        try:
            ts = datetime.fromtimestamp(float(ts_raw), tz=timezone.utc)
        except (ValueError, TypeError, OSError):
            ts = datetime.now(timezone.utc)
        return cls(
            event_id   = row["event_id"],
            event_type = EventType(row["event_type"]),
            trace_id   = row["trace_id"],
            session_id = row["session_id"] or None,
            task_id    = row["task_id"]    or None,
            worker_id  = row["worker_id"]  or None,
            payload    = json.loads(row["payload_json"] or "{}"),
            timestamp  = ts,
            version    = int(row["version"]),
        )


# ---------------------------------------------------------------------------
# EventStore
# ---------------------------------------------------------------------------

class EventStore:
    """
    Dual-write event store: Redis Stream (recent) + SQLite (durable).

    Redis stream entries are automatically pruned to the last 10,000 via
    XADD MAXLEN.  A background pruning task also trims periodically.
    """

    def __init__(self) -> None:
        self._prune_task: asyncio.Task[None] | None = None
        self._initialized = False

    # ------------------------------------------------------------------
    # Schema bootstrap
    # ------------------------------------------------------------------

    async def _ensure_schema(self) -> None:
        if self._initialized:
            return
        async with aiosqlite.connect(str(_EVENTS_DB)) as db:
            await db.executescript(_SCHEMA_EVENTS)
            await db.commit()
        self._initialized = True

    # ------------------------------------------------------------------
    # Append
    # ------------------------------------------------------------------

    async def append(self, event: Event) -> str:
        """
        Write *event* to Redis stream and SQLite.

        Returns the Redis stream entry ID (e.g. ``"1712345678900-0"``).
        """
        await self._ensure_schema()

        # --- Redis Stream ---
        stream_id: str = ""
        try:
            r = _get_redis()
            stream_id = await r.xadd(
                _REDIS_STREAM_KEY,
                event.to_redis_fields(),
                maxlen=_STREAM_MAX_LEN,
                approximate=True,
            )
            if isinstance(stream_id, bytes):
                stream_id = stream_id.decode()
        except Exception as exc:  # noqa: BLE001
            log.error(
                "event_store.redis_write_failed",
                event_id=event.event_id,
                exc=str(exc),
            )

        # --- SQLite ---
        try:
            async with aiosqlite.connect(str(_EVENTS_DB)) as db:
                await _ensure_sqlite_schema(db)
                await db.execute(
                    """
                    INSERT OR IGNORE INTO events
                        (event_id, event_type, trace_id, session_id, task_id,
                         worker_id, payload_json, timestamp, version)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.event_id,
                        event.event_type.value,
                        event.trace_id,
                        event.session_id,
                        event.task_id,
                        event.worker_id,
                        json.dumps(event.payload, default=str),
                        event.timestamp.timestamp(),
                        event.version,
                    ),
                )
                await db.commit()
        except Exception as exc:  # noqa: BLE001
            log.error(
                "event_store.sqlite_write_failed",
                event_id=event.event_id,
                exc=str(exc),
            )

        log.debug(
            "event_store.appended",
            event_id=event.event_id,
            event_type=event.event_type.value,
            stream_id=stream_id,
        )
        return stream_id

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    async def get_by_trace(self, trace_id: str) -> list[Event]:
        """Return all events for *trace_id* from SQLite, oldest-first."""
        return await self._sqlite_query(
            "SELECT * FROM events WHERE trace_id = ? ORDER BY timestamp ASC",
            (trace_id,),
        )

    async def get_by_session(self, session_id: str) -> list[Event]:
        """Return all events for *session_id* from SQLite, oldest-first."""
        return await self._sqlite_query(
            "SELECT * FROM events WHERE session_id = ? ORDER BY timestamp ASC",
            (session_id,),
        )

    async def get_by_task(self, task_id: str) -> list[Event]:
        """Return all events for *task_id* from SQLite, oldest-first."""
        return await self._sqlite_query(
            "SELECT * FROM events WHERE task_id = ? ORDER BY timestamp ASC",
            (task_id,),
        )

    async def get_recent(self, limit: int = 100) -> list[Event]:
        """
        Return the most recent *limit* events from the Redis stream.

        Falls back to SQLite if Redis stream is unavailable.
        """
        try:
            r = _get_redis()
            # XREVRANGE gives newest-first; we reverse for chronological order.
            entries = await r.xrevrange(_REDIS_STREAM_KEY, "+", "-", count=limit)
            events = []
            for _stream_id, fields in entries:
                try:
                    events.append(Event.from_redis_fields(fields))
                except Exception as exc:  # noqa: BLE001
                    log.warning("event_store.parse_stream_entry_failed", exc=str(exc))
            events.reverse()  # chronological order
            return events
        except Exception as exc:  # noqa: BLE001
            log.warning("event_store.redis_recent_failed", exc=str(exc))
            # Fallback to SQLite
            return await self._sqlite_query(
                "SELECT * FROM events ORDER BY timestamp DESC LIMIT ?",
                (limit,),
            )

    async def replay(self, from_timestamp: datetime) -> list[Event]:
        """
        Return all events from SQLite at or after *from_timestamp*.

        Useful for rebuilding aggregate state or catching up a consumer.
        """
        ts = from_timestamp.timestamp()
        return await self._sqlite_query(
            "SELECT * FROM events WHERE timestamp >= ? ORDER BY timestamp ASC",
            (ts,),
        )

    # ------------------------------------------------------------------
    # Internal SQLite query helper
    # ------------------------------------------------------------------

    async def _sqlite_query(
        self, sql: str, params: tuple[Any, ...] = ()
    ) -> list[Event]:
        await self._ensure_schema()
        try:
            async with aiosqlite.connect(str(_EVENTS_DB)) as db:
                await _ensure_sqlite_schema(db)
                db.row_factory = aiosqlite.Row
                async with db.execute(sql, params) as cur:
                    rows = await cur.fetchall()
                    return [Event.from_sqlite_row(r) for r in rows]
        except Exception as exc:  # noqa: BLE001
            log.error("event_store.sqlite_query_failed", sql=sql[:80], exc=str(exc))
            return []

    # ------------------------------------------------------------------
    # Background pruning
    # ------------------------------------------------------------------

    def start_pruner(self) -> None:
        """
        Start the background Redis stream pruner.

        Call once at application startup (e.g. FastAPI lifespan).
        """
        if self._prune_task is None or self._prune_task.done():
            self._prune_task = asyncio.create_task(
                self._prune_loop(), name="event_store_pruner"
            )

    def stop_pruner(self) -> None:
        """Cancel the background pruner (call at shutdown)."""
        if self._prune_task and not self._prune_task.done():
            self._prune_task.cancel()

    async def _prune_loop(self) -> None:
        """Periodically trim the Redis stream to MAXLEN."""
        while True:
            try:
                await asyncio.sleep(300)  # every 5 minutes
                r = _get_redis()
                trimmed = await r.xtrim(
                    _REDIS_STREAM_KEY,
                    maxlen=_STREAM_MAX_LEN,
                    approximate=True,
                )
                if trimmed:
                    log.debug("event_store.stream_pruned", trimmed=trimmed)
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001
                log.warning("event_store.prune_error", exc=str(exc))


# ---------------------------------------------------------------------------
# Schema helper (called inside open aiosqlite connection)
# ---------------------------------------------------------------------------

async def _ensure_sqlite_schema(db: aiosqlite.Connection) -> None:
    """Idempotently create tables/indexes if they don't exist."""
    await db.executescript(_SCHEMA_EVENTS)
    await db.commit()


# ---------------------------------------------------------------------------
# Convenience emit function
# ---------------------------------------------------------------------------

async def emit(
    event_type: EventType,
    *,
    trace_id: str = "",
    session_id: str | None = None,
    task_id: str | None = None,
    worker_id: str | None = None,
    payload: dict[str, Any] | None = None,
    **extra_payload: Any,
) -> Event:
    """
    Create and append a new :class:`Event` in one call.

    Extra keyword arguments are merged into *payload*::

        ev = await emit(
            EventType.CIRCUIT_OPENED,
            trace_id="t1",
            worker_id="worker-3",
            failures=3,
        )
    """
    merged_payload = {**(payload or {}), **extra_payload}
    event = Event(
        event_type = event_type,
        trace_id   = trace_id or str(uuid.uuid4()),
        session_id = session_id,
        task_id    = task_id,
        worker_id  = worker_id,
        payload    = merged_payload,
    )
    await event_store.append(event)
    return event


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

event_store = EventStore()

__all__ = [
    "EventType",
    "Event",
    "EventStore",
    "emit",
    "event_store",
]
