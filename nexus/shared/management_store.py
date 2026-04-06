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


async def upsert_vault_session_spambot_health(
    *,
    session_stem: str,
    spambot_checked_at: str,
    shadowban_suspected: bool,
    spambot_reply_snippet: str | None,
) -> None:
    """Persist @SpamBot weekly scan outcome for Commander / dashboards (telefix.db)."""
    now = datetime.now(timezone.utc).isoformat()
    snippet = (spambot_reply_snippet or "")[:2000]
    async with await _connect() as db:
        await db.execute(
            """
            INSERT INTO vault_session_telegram_health (
                session_stem, spambot_checked_at, shadowban_suspected,
                spambot_reply_snippet, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(session_stem) DO UPDATE SET
                spambot_checked_at = excluded.spambot_checked_at,
                shadowban_suspected = excluded.shadowban_suspected,
                spambot_reply_snippet = excluded.spambot_reply_snippet,
                updated_at = excluded.updated_at
            """,
            (
                session_stem.strip(),
                spambot_checked_at,
                1 if shadowban_suspected else 0,
                snippet or None,
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


def sync_get_member_audit_map(group_id: int) -> dict[int, dict[str, Any]]:
    """user_id -> {status, join_date, subscription_duration_days, is_premium}."""
    import sqlite3

    path = management_db_path()
    if not path.exists():
        return {}
    conn = sqlite3.connect(str(path), check_same_thread=False)
    try:
        conn.executescript(MANAGEMENT_DDL)
        cur = conn.execute(
            """
            SELECT user_id, status, join_date, subscription_duration_days, is_premium
            FROM member_audit WHERE group_id = ?
            """,
            (group_id,),
        )
        out: dict[int, dict[str, Any]] = {}
        for r in cur.fetchall():
            out[int(r[0])] = {
                "status": r[1],
                "join_date": r[2],
                "subscription_duration_days": int(r[3]),
                "is_premium": bool(r[4]),
            }
        return out
    finally:
        conn.close()


def sync_upsert_seo_invite_snapshot(
    *,
    group_id: int,
    invite_link: str | None,
    usage_count: int,
    participant_count: int,
    ghost_delta: int,
    audited_at: str | None = None,
) -> None:
    import sqlite3

    now = audited_at or datetime.now(timezone.utc).isoformat()
    path = management_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    try:
        conn.executescript(MANAGEMENT_DDL)
        conn.execute(
            """
            INSERT INTO seo_invite_snapshot (
                group_id, invite_link, usage_count, participant_count, ghost_delta, audited_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(group_id) DO UPDATE SET
                invite_link = excluded.invite_link,
                usage_count = excluded.usage_count,
                participant_count = excluded.participant_count,
                ghost_delta = excluded.ghost_delta,
                audited_at = excluded.audited_at
            """,
            (
                group_id,
                invite_link,
                usage_count,
                participant_count,
                ghost_delta,
                now,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def sync_get_seo_invite_snapshot(group_id: int) -> dict[str, Any] | None:
    import sqlite3

    path = management_db_path()
    if not path.exists():
        return None
    conn = sqlite3.connect(str(path), check_same_thread=False)
    try:
        conn.executescript(MANAGEMENT_DDL)
        cur = conn.execute(
            """
            SELECT group_id, invite_link, usage_count, participant_count, ghost_delta, audited_at
            FROM seo_invite_snapshot WHERE group_id = ?
            """,
            (group_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "group_id": int(row[0]),
            "invite_link": row[1],
            "usage_count": int(row[2] or 0),
            "participant_count": int(row[3] or 0),
            "ghost_delta": int(row[4] or 0),
            "audited_at": row[5],
        }
    finally:
        conn.close()


def sync_apply_member_participants(
    *,
    group_id: int,
    rows: list[dict[str, Any]],
    default_subscription_days: int,
) -> None:
    """
    Upsert current participants, mark absent users as Left.
    Each row: user_id, is_premium (bool), status ('Active'|'Deleted'|'Banned'),
    invite_slug (optional), join_date (optional ISO string; else first-seen time).
    Preserves join_date on conflict; sets join_date on first insert from row or now.
    """
    import sqlite3

    if default_subscription_days not in (30, 60):
        default_subscription_days = 30
    now = datetime.now(timezone.utc).isoformat()
    path = management_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    try:
        conn.executescript(MANAGEMENT_DDL)
        present: set[int] = set()
        for r in rows:
            uid = int(r["user_id"])
            present.add(uid)
            st = str(r["status"])
            prem = 1 if r.get("is_premium") else 0
            slug = r.get("invite_slug")
            row_join = r.get("join_date")
            join_val = row_join if isinstance(row_join, str) and row_join.strip() else now
            conn.execute(
                """
                INSERT INTO member_audit (
                    group_id, user_id, join_date, subscription_duration_days,
                    is_premium, status, invite_slug, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(group_id, user_id) DO UPDATE SET
                    is_premium = excluded.is_premium,
                    status = excluded.status,
                    invite_slug = COALESCE(excluded.invite_slug, member_audit.invite_slug),
                    subscription_duration_days = COALESCE(
                        member_audit.subscription_duration_days, excluded.subscription_duration_days
                    ),
                    join_date = COALESCE(member_audit.join_date, excluded.join_date),
                    updated_at = excluded.updated_at
                """,
                (
                    group_id,
                    uid,
                    join_val,
                    default_subscription_days,
                    prem,
                    st,
                    slug,
                    now,
                ),
            )
        if present:
            placeholders = ",".join("?" * len(present))
            conn.execute(
                f"""
                UPDATE member_audit SET status = 'Left', updated_at = ?
                WHERE group_id = ? AND user_id NOT IN ({placeholders})
                  AND status IN ('Active', 'Deleted', 'Banned')
                """,
                (now, group_id, *sorted(present)),
            )
        else:
            conn.execute(
                """
                UPDATE member_audit SET status = 'Left', updated_at = ?
                WHERE group_id = ? AND status IN ('Active', 'Deleted', 'Banned')
                """,
                (now, group_id),
            )
        conn.commit()
    finally:
        conn.close()


async def get_group_health_bundle(
    *,
    group_metadata_id: int | None = None,
    telegram_group_id: int | None = None,
) -> dict[str, Any] | None:
    """Resolve by gm.id or Telegram group_id; join member_stats + latest invite snapshot."""
    if group_metadata_id is None and telegram_group_id is None:
        return None
    async with await _connect() as db:
        if group_metadata_id is not None:
            cur = await db.execute(
                """
                SELECT gm.id, gm.group_id, gm.title, gm.username, gm.invite_link, gm.updated_at,
                       ms.total_members, ms.premium_count, ms.deleted_count, ms.active_real_count
                FROM group_metadata gm
                LEFT JOIN member_stats ms ON ms.group_metadata_id = gm.id
                WHERE gm.id = ?
                """,
                (group_metadata_id,),
            )
        else:
            cur = await db.execute(
                """
                SELECT gm.id, gm.group_id, gm.title, gm.username, gm.invite_link, gm.updated_at,
                       ms.total_members, ms.premium_count, ms.deleted_count, ms.active_real_count
                FROM group_metadata gm
                LEFT JOIN member_stats ms ON ms.group_metadata_id = gm.id
                WHERE gm.group_id = ?
                ORDER BY gm.updated_at DESC LIMIT 1
                """,
                (telegram_group_id,),
            )
        row = await cur.fetchone()
        if not row:
            return None
        gid = int(row["group_id"])
        snap_cur = await db.execute(
            """
            SELECT invite_link, usage_count, participant_count, ghost_delta, audited_at
            FROM seo_invite_snapshot WHERE group_id = ?
            """,
            (gid,),
        )
        snap = await snap_cur.fetchone()
        return {
            "group_metadata_id": int(row["id"]),
            "group_id": gid,
            "title": row["title"],
            "username": row["username"],
            "invite_link": row["invite_link"],
            "gm_updated_at": row["updated_at"],
            "total_members": int(row["total_members"] or 0),
            "premium_count": int(row["premium_count"] or 0),
            "deleted_count": int(row["deleted_count"] or 0),
            "active_real_count": int(row["active_real_count"] or 0),
            "invite_snapshot": (
                {
                    "invite_link": snap["invite_link"],
                    "usage_count": int(snap["usage_count"] or 0),
                    "participant_count": int(snap["participant_count"] or 0),
                    "ghost_delta": int(snap["ghost_delta"] or 0),
                    "audited_at": snap["audited_at"],
                }
                if snap
                else None
            ),
        }


async def aggregate_rank_projection_inputs() -> dict[str, Any]:
    """Cross-group aggregates for heuristic rank projection."""
    async with await _connect() as db:
        cur = await db.execute(
            """
            SELECT AVG(CAST(ms.premium_count AS REAL) / NULLIF(ms.total_members, 0)) AS avg_prem,
                   AVG(ms.total_members) AS avg_members,
                   COUNT(*) AS n_groups
            FROM group_metadata gm
            JOIN member_stats ms ON ms.group_metadata_id = gm.id
            WHERE ms.total_members > 0
            """
        )
        row = await cur.fetchone()
        avg_prem = float(row["avg_prem"] or 0.0)
        avg_members = float(row["avg_members"] or 0.0)
        n_groups = int(row["n_groups"] or 0)

        cur2 = await db.execute(
            """
            SELECT AVG(
                (julianday('now') - julianday(ma.join_date)) / 30.0
            ) AS avg_tenure_mo
            FROM member_audit ma
            WHERE ma.join_date IS NOT NULL AND ma.status = 'Active'
            """
        )
        r2 = await cur2.fetchone()
        avg_tenure_mo = float(r2["avg_tenure_mo"] or 0.0)

        cur3 = await db.execute(
            """
            SELECT AVG(julianday('now') - julianday(gm.updated_at)) AS avg_gm_age_days
            FROM group_metadata gm
            """
        )
        r3 = await cur3.fetchone()
        avg_gm_age_days = float(r3["avg_gm_age_days"] or 0.0)

        return {
            "avg_premium_ratio": avg_prem,
            "avg_members": avg_members,
            "n_groups": n_groups,
            "avg_member_tenure_months": avg_tenure_mo,
            "avg_group_row_age_days": avg_gm_age_days,
        }


def sync_insert_seo_churn_event(
    *,
    group_id: int,
    user_id: int,
    detected_at: str,
    join_date: str | None,
    left_at: str,
    subscription_days: int,
    reason: str,
) -> None:
    import sqlite3

    if subscription_days not in (30, 60):
        subscription_days = 30
    path = management_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    try:
        conn.executescript(MANAGEMENT_DDL)
        conn.execute(
            """
            INSERT OR IGNORE INTO seo_churn_event (
                group_id, user_id, detected_at, join_date, left_at, subscription_days, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (group_id, user_id, detected_at, join_date, left_at, subscription_days, reason),
        )
        conn.commit()
    finally:
        conn.close()


SEO_INVITE_REDIS_KEY_FMT = "nexus:seo:invite:{group_id}"
SEO_INVITE_REDIS_TTL_S = 604800


async def aggregate_seo_fleet_stats(
    *,
    alerts_limit: int = 100,
    top_ghost_limit: int = 50,
) -> dict[str, Any]:
    """
    Dedupe by Telegram group_id (latest member_stats row per group), join invite snapshots,
    and list recent seo_churn_event rows for 1XPANEL.
    """
    alerts_limit = max(0, min(500, int(alerts_limit)))
    top_ghost_limit = max(0, min(200, int(top_ghost_limit)))
    path = management_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(str(path)) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA foreign_keys=ON")
        await db.executescript(MANAGEMENT_DDL)
        await db.commit()
        cur = await db.execute(
            """
            WITH ranked AS (
                SELECT
                    gm.id AS gm_id,
                    gm.group_id,
                    gm.title,
                    gm.username,
                    COALESCE(ms.total_members, 0) AS total_members,
                    COALESCE(ms.premium_count, 0) AS premium_count,
                    COALESCE(ms.deleted_count, 0) AS deleted_count,
                    COALESCE(ms.active_real_count, 0) AS active_real_count,
                    ROW_NUMBER() OVER (
                        PARTITION BY gm.group_id
                        ORDER BY datetime(COALESCE(ms.updated_at, gm.updated_at, '1970-01-01')) DESC
                    ) AS rn
                FROM group_metadata gm
                LEFT JOIN member_stats ms ON ms.group_metadata_id = gm.id
            )
            SELECT gm_id, group_id, title, username, total_members, premium_count,
                   deleted_count, active_real_count
            FROM ranked
            WHERE rn = 1
            """
        )
        g_rows = await cur.fetchall()

        n_groups = len(g_rows)
        total_members = sum(int(r["total_members"] or 0) for r in g_rows)
        total_premium = sum(int(r["premium_count"] or 0) for r in g_rows)
        fleet_premium_ratio = (
            round(float(total_premium) / float(total_members), 6) if total_members else 0.0
        )

        group_ids = [int(r["group_id"]) for r in g_rows]
        snap_map: dict[int, dict[str, Any]] = {}
        if group_ids:
            placeholders = ",".join("?" * len(group_ids))
            sc = await db.execute(
                f"""
                SELECT group_id, invite_link, usage_count, participant_count, ghost_delta, audited_at
                FROM seo_invite_snapshot
                WHERE group_id IN ({placeholders})
                """,
                group_ids,
            )
            for s in await sc.fetchall():
                snap_map[int(s["group_id"])] = dict(s)

        groups_out: list[dict[str, Any]] = []
        for r in g_rows:
            gid = int(r["group_id"])
            tm = int(r["total_members"] or 0)
            pc = int(r["premium_count"] or 0)
            snap = snap_map.get(gid) or {}
            gh = int(snap.get("ghost_delta") or 0)
            groups_out.append({
                "group_id": gid,
                "group_metadata_id": int(r["gm_id"]),
                "title": r["title"],
                "username": r["username"],
                "active_members": tm,
                "premium_count": pc,
                "premium_ratio": round(pc / tm, 6) if tm else 0.0,
                "invite_usage": int(snap.get("usage_count") or 0),
                "ghost_delta": gh,
                "invite_audited_at": snap.get("audited_at"),
            })

        groups_by_ghost = sorted(groups_out, key=lambda x: (-x["ghost_delta"], -x["active_members"]))
        top_ghosts = groups_by_ghost[:top_ghost_limit] if top_ghost_limit else []

        churn_7d = 0
        if alerts_limit:
            c7 = await db.execute(
                """
                SELECT COUNT(*) AS c FROM seo_churn_event
                WHERE datetime(detected_at) >= datetime('now', '-7 days')
                """
            )
            row7 = await c7.fetchone()
            churn_7d = int(row7["c"] or 0) if row7 else 0

        alerts: list[dict[str, Any]] = []
        if alerts_limit:
            ac = await db.execute(
                """
                SELECT group_id, user_id, detected_at, join_date, left_at, subscription_days, reason
                FROM seo_churn_event
                ORDER BY datetime(detected_at) DESC
                LIMIT ?
                """,
                (alerts_limit,),
            )
            for a in await ac.fetchall():
                alerts.append({
                    "group_id": int(a["group_id"]),
                    "user_id": int(a["user_id"]),
                    "detected_at": a["detected_at"],
                    "join_date": a["join_date"],
                    "left_at": a["left_at"],
                    "subscription_days": int(a["subscription_days"] or 30),
                    "reason": a["reason"],
                })

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "n_groups": n_groups,
            "total_active_members": total_members,
            "fleet_premium_ratio": fleet_premium_ratio,
            "total_premium_members": total_premium,
            "replacement_alerts_count_7d": churn_7d,
            "groups": groups_out,
            "top_ghost_groups": top_ghosts,
            "replacement_alerts": alerts,
        }
