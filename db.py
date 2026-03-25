"""
SQLite persistence for the Telegram \"Architect\" operator flow.

Stores per-operator locked target (Telegram user_id) and append-only context text
chunks for behavioral analysis when forwarded messages lack visible text (privacy).

All public I/O uses aiosqlite so the Telegram bot event loop is not blocked.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite

from utils.paths import repository_root


def _db_path() -> Path:
    raw = (os.environ.get("NEXUS_ARCHITECT_DB_PATH") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    data_dir = repository_root() / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / "nexus_architect.sqlite3"


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# SQLite row size / UX: split pasted corpus into multiple rows when over this size.
ARCHITECT_CONTEXT_CHUNK_MAX = 5000


async def _connect() -> aiosqlite.Connection:
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    db = await aiosqlite.connect(str(path), timeout=30)
    db.row_factory = aiosqlite.Row
    try:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.execute("PRAGMA busy_timeout=30000")
    except aiosqlite.Error:
        pass
    return db


async def init_schema() -> None:
    db = await _connect()
    try:
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS architect_lock (
                operator_chat_id INTEGER PRIMARY KEY,
                target_user_id   INTEGER NOT NULL,
                target_name      TEXT NOT NULL DEFAULT '',
                updated_at       TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS architect_context (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                operator_chat_id  INTEGER NOT NULL,
                target_user_id    INTEGER NOT NULL,
                body              TEXT NOT NULL,
                source            TEXT NOT NULL DEFAULT 'manual',
                created_at        TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_architect_ctx_op_target
                ON architect_context (operator_chat_id, target_user_id);

            CREATE TABLE IF NOT EXISTS architect_turbo_blob (
                operator_chat_id INTEGER NOT NULL,
                blob_id          TEXT NOT NULL,
                body             TEXT NOT NULL,
                created_at       TEXT NOT NULL,
                PRIMARY KEY (operator_chat_id, blob_id)
            );
            """
        )
        await db.commit()
    finally:
        await db.close()


async def set_lock(operator_chat_id: int, target_user_id: int, target_name: str = "") -> None:
    await init_schema()
    name = (target_name or "").strip() or str(target_user_id)
    db = await _connect()
    try:
        await db.execute(
            """
            INSERT INTO architect_lock (operator_chat_id, target_user_id, target_name, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(operator_chat_id) DO UPDATE SET
                target_user_id = excluded.target_user_id,
                target_name = excluded.target_name,
                updated_at = excluded.updated_at
            """,
            (int(operator_chat_id), int(target_user_id), name, _utc_now()),
        )
        await db.commit()
    finally:
        await db.close()


@dataclass(frozen=True)
class ArchitectLock:
    target_user_id: int
    target_name: str


async def get_lock(operator_chat_id: int) -> ArchitectLock | None:
    await init_schema()
    db = await _connect()
    try:
        async with db.execute(
            "SELECT target_user_id, target_name FROM architect_lock WHERE operator_chat_id = ?",
            (int(operator_chat_id),),
        ) as cur:
            row = await cur.fetchone()
    finally:
        await db.close()
    if not row:
        return None
    return ArchitectLock(
        target_user_id=int(row["target_user_id"]),
        target_name=str(row["target_name"]),
    )


async def clear_lock(operator_chat_id: int) -> None:
    await init_schema()
    oid = int(operator_chat_id)
    db = await _connect()
    try:
        await db.execute("DELETE FROM architect_lock WHERE operator_chat_id = ?", (oid,))
        await db.commit()
    finally:
        await db.close()


async def add_context_chunk(
    operator_chat_id: int,
    target_user_id: int,
    body: str,
    *,
    source: str = "manual",
) -> None:
    text = (body or "").strip()
    if not text:
        return
    await init_schema()
    base_src = (source or "manual")[:32]
    pieces: list[str]
    if len(text) <= ARCHITECT_CONTEXT_CHUNK_MAX:
        pieces = [text]
    else:
        pieces = []
        i = 0
        while i < len(text):
            pieces.append(text[i : i + ARCHITECT_CONTEXT_CHUNK_MAX])
            i += ARCHITECT_CONTEXT_CHUNK_MAX
    db = await _connect()
    try:
        for idx, piece in enumerate(pieces):
            src = base_src if idx == 0 else f"{base_src[:27]}#{idx + 1}"[:32]
            await db.execute(
                """
                INSERT INTO architect_context
                    (operator_chat_id, target_user_id, body, source, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    int(operator_chat_id),
                    int(target_user_id),
                    piece,
                    src,
                    _utc_now(),
                ),
            )
        await db.commit()
    finally:
        await db.close()


async def count_context_chunks(operator_chat_id: int, target_user_id: int) -> int:
    await init_schema()
    db = await _connect()
    try:
        async with db.execute(
            """
            SELECT COUNT(*) AS n FROM architect_context
            WHERE operator_chat_id = ? AND target_user_id = ?
            """,
            (int(operator_chat_id), int(target_user_id)),
        ) as cur:
            row = await cur.fetchone()
    finally:
        await db.close()
    return int(row["n"]) if row else 0


async def get_all_context_bodies(operator_chat_id: int, target_user_id: int) -> list[str]:
    await init_schema()
    db = await _connect()
    try:
        async with db.execute(
            """
            SELECT body FROM architect_context
            WHERE operator_chat_id = ? AND target_user_id = ?
            ORDER BY id ASC
            """,
            (int(operator_chat_id), int(target_user_id)),
        ) as cur:
            rows = await cur.fetchall()
    finally:
        await db.close()
    return [str(r["body"]) for r in rows]


async def clear_context_chunks(operator_chat_id: int, target_user_id: int) -> int:
    await init_schema()
    oid = int(operator_chat_id)
    tid = int(target_user_id)
    db = await _connect()
    try:
        cur = await db.execute(
            "DELETE FROM architect_context WHERE operator_chat_id = ? AND target_user_id = ?",
            (oid, tid),
        )
        await db.commit()
        return int(cur.rowcount or 0)
    finally:
        await db.close()


async def save_turbo_blob(operator_chat_id: int, blob_id: str, body: str) -> None:
    """Persist a large turbo / master-profile payload for Telegram callback retrieval."""
    bid = (blob_id or "").strip()
    if not bid:
        return
    text = body or ""
    await init_schema()
    oid = int(operator_chat_id)
    db = await _connect()
    try:
        await db.execute(
            """
            INSERT INTO architect_turbo_blob (operator_chat_id, blob_id, body, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(operator_chat_id, blob_id) DO UPDATE SET
                body = excluded.body,
                created_at = excluded.created_at
            """,
            (oid, bid[:32], text, _utc_now()),
        )
        await db.commit()
    finally:
        await db.close()


async def load_turbo_blob(operator_chat_id: int, blob_id: str) -> str | None:
    bid = (blob_id or "").strip()
    if not bid:
        return None
    await init_schema()
    oid = int(operator_chat_id)
    db = await _connect()
    try:
        async with db.execute(
            "SELECT body FROM architect_turbo_blob WHERE operator_chat_id = ? AND blob_id = ?",
            (oid, bid[:32]),
        ) as cur:
            row = await cur.fetchone()
    finally:
        await db.close()
    if not row:
        return None
    return str(row["body"])
