"""
Async SQLite access for management dashboard tables (telefix.db).

Uses the same DB path resolution as the rest of Nexus (TELEFIX_DB_PATH env,
then repo-root telefix.db).
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite
from nexus.shared.management_schema import MANAGEMENT_DDL


def management_db_path() -> Path:
    raw = (os.getenv("TELEFIX_DB_PATH") or "").strip()
    if raw:
        return Path(raw)
    try:
        from nexus.shared.config import settings

        if (settings.telefix_db or "").strip():
            return Path(settings.telefix_db)
    except Exception:
        pass
    # Repo root: nexus/shared/management_store.py -> parents[2]
    return Path(__file__).resolve().parents[2] / "telefix.db"


async def _connect() -> aiosqlite.Connection:
    path = management_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    db = await aiosqlite.connect(str(path))
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA foreign_keys=ON")
    await db.executescript(MANAGEMENT_DDL)
    await db.commit()
    return db


async def upsert_group_metadata(
    *,
    session_owner: str,
    group_id: int,
    title: str | None,
    username: str | None,
    is_public: bool,
    invite_link: str | None,
    creator_id: int | None,
    legacy_groups_id: int | None = None,
) -> int:
    """Insert or update group_metadata; return row id."""
    now = datetime.now(timezone.utc).isoformat()
    async with await _connect() as db:
        await db.execute(
            """
            INSERT INTO group_metadata (
                session_owner, group_id, title, username, is_public, invite_link,
                creator_id, legacy_groups_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_owner, group_id) DO UPDATE SET
                title = excluded.title,
                username = excluded.username,
                is_public = excluded.is_public,
                invite_link = excluded.invite_link,
                creator_id = excluded.creator_id,
                legacy_groups_id = COALESCE(excluded.legacy_groups_id, group_metadata.legacy_groups_id),
                updated_at = excluded.updated_at
            """,
            (
                session_owner,
                group_id,
                title,
                username,
                1 if is_public else 0,
                invite_link,
                creator_id,
                legacy_groups_id,
                now,
            ),
        )
        await db.commit()
        cur = await db.execute(
            "SELECT id FROM group_metadata WHERE session_owner = ? AND group_id = ?",
            (session_owner, group_id),
        )
        row = await cur.fetchone()
        if not row:
            raise RuntimeError("upsert group_metadata failed to read id")
        return int(row[0])


async def upsert_member_stats(
    *,
    group_metadata_id: int,
    total_members: int,
    premium_count: int,
    deleted_count: int,
    active_real_count: int,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    async with await _connect() as db:
        await db.execute(
            """
            INSERT INTO member_stats (
                group_metadata_id, total_members, premium_count, deleted_count,
                active_real_count, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(group_metadata_id) DO UPDATE SET
                total_members = excluded.total_members,
                premium_count = excluded.premium_count,
                deleted_count = excluded.deleted_count,
                active_real_count = excluded.active_real_count,
                updated_at = excluded.updated_at
            """,
            (
                group_metadata_id,
                total_members,
                premium_count,
                deleted_count,
                active_real_count,
                now,
            ),
        )
        await db.commit()


async def upsert_rank_tracker(
    *,
    group_metadata_id: int,
    keyword_phrase: str,
    current_rank: int | None,
    is_shadowbanned: bool,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    async with await _connect() as db:
        await db.execute(
            """
            INSERT INTO rank_tracker (
                group_metadata_id, keyword_phrase, current_rank, last_check,
                is_shadowbanned, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(group_metadata_id, keyword_phrase) DO UPDATE SET
                current_rank = excluded.current_rank,
                last_check = excluded.last_check,
                is_shadowbanned = excluded.is_shadowbanned,
                updated_at = excluded.updated_at
            """,
            (
                group_metadata_id,
                keyword_phrase.strip(),
                current_rank,
                now,
                1 if is_shadowbanned else 0,
                now,
            ),
        )
        await db.commit()


async def list_management_groups() -> list[dict[str, Any]]:
    """Join group_metadata + member_stats; attach rank_tracker rows."""
    async with await _connect() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT
                gm.id AS gm_id,
                gm.session_owner,
                gm.group_id,
                gm.title,
                gm.username,
                gm.is_public,
                gm.invite_link,
                gm.creator_id,
                gm.legacy_groups_id,
                gm.updated_at AS gm_updated_at,
                ms.total_members,
                ms.premium_count,
                ms.deleted_count,
                ms.active_real_count,
                ms.updated_at AS ms_updated_at
            FROM group_metadata gm
            LEFT JOIN member_stats ms ON ms.group_metadata_id = gm.id
            ORDER BY gm.updated_at DESC
            """
        )
        rows = await cur.fetchall()
        gm_ids = [int(r["gm_id"]) for r in rows]
        ranks_map: dict[int, list[dict[str, Any]]] = {gid: [] for gid in gm_ids}
        if gm_ids:
            placeholders = ",".join("?" * len(gm_ids))
            rc = await db.execute(
                f"""
                SELECT group_metadata_id, keyword_phrase, current_rank, last_check,
                       is_shadowbanned, updated_at
                FROM rank_tracker
                WHERE group_metadata_id IN ({placeholders})
                ORDER BY keyword_phrase
                """,
                gm_ids,
            )
            rank_rows = await rc.fetchall()
            for rt in rank_rows:
                gid = int(rt[0])
                ranks_map.setdefault(gid, []).append({
                    "keyword_phrase": rt[1],
                    "current_rank": rt[2],
                    "last_check": rt[3],
                    "is_shadowbanned": bool(rt[4]),
                    "updated_at": rt[5],
                })

        out: list[dict[str, Any]] = []
        for r in rows:
            gid = int(r["gm_id"])
            out.append({
                "id": gid,
                "session_owner": r["session_owner"],
                "group_id": r["group_id"],
                "title": r["title"],
                "username": r["username"],
                "is_public": bool(r["is_public"]),
                "invite_link": r["invite_link"],
                "creator_id": r["creator_id"],
                "legacy_groups_id": r["legacy_groups_id"],
                "updated_at": r["gm_updated_at"],
                "member_stats": {
                    "total_members": r["total_members"] or 0,
                    "premium_count": r["premium_count"] or 0,
                    "deleted_count": r["deleted_count"] or 0,
                    "active_real_count": r["active_real_count"] or 0,
                    "updated_at": r["ms_updated_at"],
                },
                "rank_tracker": ranks_map.get(gid, []),
            })
        return out


def sync_upsert_group_bundle(
    *,
    session_owner: str,
    group_id: int,
    title: str | None,
    username: str | None,
    is_public: bool,
    invite_link: str | None,
    creator_id: int | None,
    total_members: int,
    premium_count: int,
    deleted_count: int,
    active_real_count: int,
) -> int:
    """
    Synchronous upsert for worker thread (Telethon sync client).
    Returns group_metadata id.
    """
    import sqlite3

    path = management_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(str(path), check_same_thread=False)
    try:
        conn.executescript(MANAGEMENT_DDL)
        conn.execute(
            """
            INSERT INTO group_metadata (
                session_owner, group_id, title, username, is_public, invite_link,
                creator_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_owner, group_id) DO UPDATE SET
                title = excluded.title,
                username = excluded.username,
                is_public = excluded.is_public,
                invite_link = excluded.invite_link,
                creator_id = excluded.creator_id,
                updated_at = excluded.updated_at
            """,
            (
                session_owner,
                group_id,
                title,
                username,
                1 if is_public else 0,
                invite_link,
                creator_id,
                now,
            ),
        )
        conn.commit()
        cur = conn.execute(
            "SELECT id FROM group_metadata WHERE session_owner = ? AND group_id = ?",
            (session_owner, group_id),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("sync_upsert_group_bundle: missing id")
        gm_id = int(row[0])
        conn.execute(
            """
            INSERT INTO member_stats (
                group_metadata_id, total_members, premium_count, deleted_count,
                active_real_count, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(group_metadata_id) DO UPDATE SET
                total_members = excluded.total_members,
                premium_count = excluded.premium_count,
                deleted_count = excluded.deleted_count,
                active_real_count = excluded.active_real_count,
                updated_at = excluded.updated_at
            """,
            (
                gm_id,
                total_members,
                premium_count,
                deleted_count,
                active_real_count,
                now,
            ),
        )
        conn.commit()
        return gm_id
    finally:
        conn.close()


def sync_list_groups_minimal() -> list[dict[str, Any]]:
    """Sync read id, session_owner, group_id, title, username, invite_link."""
    import sqlite3

    path = management_db_path()
    if not path.exists():
        return []
    conn = sqlite3.connect(str(path), check_same_thread=False)
    try:
        conn.executescript(MANAGEMENT_DDL)
        cur = conn.execute(
            """
            SELECT id, session_owner, group_id, title, username, invite_link
            FROM group_metadata
            ORDER BY id
            """
        )
        rows = cur.fetchall()
        return [
            {
                "id": int(r[0]),
                "session_owner": r[1],
                "group_id": int(r[2]),
                "title": r[3],
                "username": r[4],
                "invite_link": r[5],
            }
            for r in rows
        ]
    finally:
        conn.close()


def sync_upsert_rank_tracker_row(
    *,
    group_metadata_id: int,
    keyword_phrase: str,
    current_rank: int | None,
    is_shadowbanned: bool,
) -> None:
    import sqlite3

    phrase = keyword_phrase.strip()
    if len(phrase.split()) < 2:
        # Allow single-token phrases for username-based SEO checks (product rule: 2+ for real keywords).
        pass

    now = datetime.now(timezone.utc).isoformat()
    path = management_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    try:
        conn.executescript(MANAGEMENT_DDL)
        conn.execute(
            """
            INSERT INTO rank_tracker (
                group_metadata_id, keyword_phrase, current_rank, last_check,
                is_shadowbanned, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(group_metadata_id, keyword_phrase) DO UPDATE SET
                current_rank = excluded.current_rank,
                last_check = excluded.last_check,
                is_shadowbanned = excluded.is_shadowbanned,
                updated_at = excluded.updated_at
            """,
            (
                group_metadata_id,
                phrase,
                current_rank,
                now,
                1 if is_shadowbanned else 0,
                now,
            ),
        )
        conn.commit()
    finally:
        conn.close()
