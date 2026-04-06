"""
SQLite DDL for the 3-screen management dashboard (group metadata, stats, rank tracker).

Applied idempotently via CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS.
"""

from __future__ import annotations

MANAGEMENT_DDL = """
CREATE TABLE IF NOT EXISTS group_metadata (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    session_owner     TEXT NOT NULL,
    group_id          INTEGER NOT NULL,
    title             TEXT,
    username          TEXT,
    is_public         INTEGER DEFAULT 0,
    invite_link       TEXT,
    creator_id        INTEGER,
    legacy_groups_id  INTEGER,
    updated_at        TEXT DEFAULT (datetime('now')),
    UNIQUE(session_owner, group_id)
);

CREATE INDEX IF NOT EXISTS idx_group_metadata_session ON group_metadata(session_owner);
CREATE INDEX IF NOT EXISTS idx_group_metadata_username ON group_metadata(username);

CREATE TABLE IF NOT EXISTS member_stats (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    group_metadata_id  INTEGER NOT NULL UNIQUE,
    total_members      INTEGER DEFAULT 0,
    premium_count      INTEGER DEFAULT 0,
    deleted_count      INTEGER DEFAULT 0,
    active_real_count  INTEGER DEFAULT 0,
    updated_at         TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (group_metadata_id) REFERENCES group_metadata(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_member_stats_gm ON member_stats(group_metadata_id);

CREATE TABLE IF NOT EXISTS rank_tracker (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    group_metadata_id  INTEGER NOT NULL,
    keyword_phrase     TEXT NOT NULL,
    current_rank       INTEGER,
    last_check         TEXT,
    is_shadowbanned    INTEGER DEFAULT 0,
    updated_at         TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (group_metadata_id) REFERENCES group_metadata(id) ON DELETE CASCADE,
    UNIQUE(group_metadata_id, keyword_phrase)
);

CREATE INDEX IF NOT EXISTS idx_rank_tracker_gm ON rank_tracker(group_metadata_id);

CREATE TABLE IF NOT EXISTS member_audit (
    id                         INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id                   INTEGER NOT NULL,
    user_id                    INTEGER NOT NULL,
    join_date                  TEXT,
    subscription_duration_days INTEGER NOT NULL DEFAULT 30
        CHECK (subscription_duration_days IN (30, 60)),
    is_premium                 INTEGER NOT NULL DEFAULT 0,
    status                     TEXT NOT NULL
        CHECK (status IN ('Active', 'Banned', 'Deleted', 'Left')),
    invite_slug                TEXT,
    updated_at                 TEXT DEFAULT (datetime('now')),
    UNIQUE (group_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_member_audit_group ON member_audit(group_id);
CREATE INDEX IF NOT EXISTS idx_member_audit_status ON member_audit(group_id, status);

CREATE TABLE IF NOT EXISTS seo_invite_snapshot (
    group_id           INTEGER NOT NULL PRIMARY KEY,
    invite_link        TEXT,
    usage_count        INTEGER NOT NULL DEFAULT 0,
    participant_count  INTEGER NOT NULL DEFAULT 0,
    ghost_delta        INTEGER NOT NULL DEFAULT 0,
    audited_at         TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS seo_churn_event (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id           INTEGER NOT NULL,
    user_id            INTEGER NOT NULL,
    detected_at        TEXT NOT NULL,
    join_date          TEXT,
    left_at            TEXT,
    subscription_days  INTEGER NOT NULL DEFAULT 30,
    reason             TEXT,
    UNIQUE (group_id, user_id, detected_at)
);

CREATE INDEX IF NOT EXISTS idx_seo_churn_group ON seo_churn_event(group_id);
CREATE INDEX IF NOT EXISTS idx_seo_churn_detected ON seo_churn_event(detected_at DESC);

CREATE TABLE IF NOT EXISTS vault_session_telegram_health (
    session_stem           TEXT PRIMARY KEY,
    spambot_checked_at     TEXT,
    shadowban_suspected    INTEGER NOT NULL DEFAULT 0,
    spambot_reply_snippet  TEXT,
    updated_at             TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_vault_session_spambot_checked
    ON vault_session_telegram_health(spambot_checked_at DESC);
"""


def apply_management_ddl_sync(conn) -> None:
    """Apply management dashboard tables to an open sqlite3 connection."""
    conn.executescript(MANAGEMENT_DDL)
    conn.commit()
