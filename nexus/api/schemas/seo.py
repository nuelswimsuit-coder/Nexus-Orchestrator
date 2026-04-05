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
