"""Pydantic models for SEO watchdog / 1XPANEL dashboard API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class InviteSnapshotOut(BaseModel):
    invite_link: str | None = None
    usage_count: int = 0
    participant_count: int = 0
    ghost_delta: int = 0
    audited_at: str | None = None


class GroupHealthResponse(BaseModel):
    group_metadata_id: int
    group_id: int
    title: str | None = None
    username: str | None = None
    premium_pct: float = Field(description="Share of members with Telegram Premium (from member_stats).")
    alive_ratio: float = Field(description="active_real_count / total_members when total > 0.")
    dead_ratio: float = Field(description="deleted_count / total_members when total > 0.")
    total_members: int = 0
    premium_count: int = 0
    deleted_count: int = 0
    active_real_count: int = 0
    invite_snapshot: InviteSnapshotOut | None = None
    gm_updated_at: str | None = None


class RankProjectionResponse(BaseModel):
    score: float = Field(description="Heuristic 0–100 SEO rank proxy.")
    tier: str
    inputs: dict[str, Any] = Field(default_factory=dict)


class ReplacementAlertOut(BaseModel):
    group_id: int
    user_id: int
    detected_at: str | None = None
    join_date: str | None = None
    left_at: str | None = None
    subscription_days: int = 30
    reason: str | None = None


class GroupSeoStatRow(BaseModel):
    group_id: int
    group_metadata_id: int
    title: str | None = None
    username: str | None = None
    active_members: int = 0
    premium_count: int = 0
    premium_ratio: float = 0.0
    invite_usage: int = 0
    ghost_delta: int = 0
    invite_audited_at: str | None = None
    redis_invite_usage: int | None = None
    redis_invite_audited_at: str | None = None


class SeoStatsResponse(BaseModel):
    """Aggregate SEO / invite health for 1XPANEL polling."""

    generated_at: str
    n_groups: int = 0
    total_active_members: int = 0
    fleet_premium_ratio: float = Field(
        0.0, description="SUM(premium) / SUM(members) across deduped Telegram groups."
    )
    total_premium_members: int = 0
    replacement_alerts_count_7d: int = 0
    groups: list[GroupSeoStatRow] = Field(default_factory=list)
    top_ghost_groups: list[GroupSeoStatRow] = Field(
        default_factory=list,
        description="Groups with highest invite ghost_delta (replacement pressure).",
    )
    replacement_alerts: list[ReplacementAlertOut] = Field(default_factory=list)
