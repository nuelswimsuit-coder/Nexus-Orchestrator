"""Tests for SEO fleet aggregate stats and churn persistence."""

from __future__ import annotations

import sqlite3

import pytest

from nexus.shared.management_schema import MANAGEMENT_DDL
from nexus.shared.management_store import aggregate_seo_fleet_stats, sync_insert_seo_churn_event


@pytest.fixture
def _seo_db(tmp_path, monkeypatch):
    p = tmp_path / "seo_mgmt.db"
    monkeypatch.setenv("TELEFIX_DB_PATH", str(p))
    conn = sqlite3.connect(str(p))
    conn.executescript(MANAGEMENT_DDL)
    conn.execute(
        """
        INSERT INTO group_metadata (
            session_owner, group_id, title, username, is_public, invite_link, updated_at
        ) VALUES ('o1', 1001, 'G1', 'g1un', 0, 'https://t.me/+abc', '2026-01-01')
        """
    )
    gm_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    conn.execute(
        """
        INSERT INTO member_stats (
            group_metadata_id, total_members, premium_count, deleted_count,
            active_real_count, updated_at
        ) VALUES (?, 100, 40, 2, 98, '2026-01-02')
        """,
        (gm_id,),
    )
    conn.execute(
        """
        INSERT INTO seo_invite_snapshot (
            group_id, invite_link, usage_count, participant_count, ghost_delta, audited_at
        ) VALUES (1001, 'https://t.me/+abc', 50, 100, 0, '2026-01-03')
        """
    )
    conn.commit()
    conn.close()
    return p


@pytest.mark.asyncio
async def test_aggregate_seo_fleet_stats_dedupe_and_ratio(_seo_db) -> None:
    out = await aggregate_seo_fleet_stats(alerts_limit=5, top_ghost_limit=10)
    assert out["n_groups"] == 1
    assert out["total_active_members"] == 100
    assert abs(float(out["fleet_premium_ratio"]) - 0.4) < 1e-6
    assert abs(float(out["groups"][0]["premium_ratio"]) - 0.4) < 1e-6
    assert out["top_ghost_groups"][0]["ghost_delta"] == 0


@pytest.mark.asyncio
async def test_aggregate_seo_fleet_stats_dedupes_duplicate_telegram_group(_seo_db) -> None:
    conn = sqlite3.connect(str(_seo_db))
    conn.execute(
        """
        INSERT INTO group_metadata (
            session_owner, group_id, title, username, is_public, invite_link, updated_at
        ) VALUES ('o2', 1001, 'G1b', 'g1un2', 0, NULL, '2026-01-04')
        """
    )
    gm2 = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    conn.execute(
        """
        INSERT INTO member_stats (
            group_metadata_id, total_members, premium_count, deleted_count,
            active_real_count, updated_at
        ) VALUES (?, 50, 10, 0, 50, '2026-01-05')
        """,
        (gm2,),
    )
    conn.commit()
    conn.close()

    out = await aggregate_seo_fleet_stats(alerts_limit=0, top_ghost_limit=5)
    assert out["n_groups"] == 1
    assert out["total_active_members"] == 50
    assert out["total_premium_members"] == 10


@pytest.mark.asyncio
async def test_aggregate_replacement_alerts(_seo_db) -> None:
    sync_insert_seo_churn_event(
        group_id=1001,
        user_id=42,
        detected_at="2026-01-05T10:00:00+00:00",
        join_date="2026-01-01T00:00:00+00:00",
        left_at="2026-01-05T10:00:00+00:00",
        subscription_days=30,
        reason="early_churn_before_subscription_window",
    )
    out = await aggregate_seo_fleet_stats(alerts_limit=10, top_ghost_limit=5)
    assert len(out["replacement_alerts"]) >= 1
    assert out["replacement_alerts"][0]["user_id"] == 42
    assert "early_churn" in (out["replacement_alerts"][0].get("reason") or "")
