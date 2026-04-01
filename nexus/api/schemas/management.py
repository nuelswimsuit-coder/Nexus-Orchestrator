"""Pydantic models for the 3-screen management dashboard API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


class MemberStatsOut(BaseModel):
    total_members: int = 0
    premium_count: int = 0
    deleted_count: int = 0
    active_real_count: int = 0
    updated_at: str | None = None


class RankTrackerOut(BaseModel):
    keyword_phrase: str
    current_rank: int | None = None
    last_check: str | None = None
    is_shadowbanned: bool = False
    updated_at: str | None = None


class ManagementGroupRow(BaseModel):
    id: int
    session_owner: str
    group_id: int
    title: str | None = None
    username: str | None = None
    is_public: bool = False
    invite_link: str | None = None
    creator_id: int | None = None
    legacy_groups_id: int | None = None
    updated_at: str | None = None
    member_stats: MemberStatsOut
    rank_tracker: list[RankTrackerOut] = Field(default_factory=list)


class ManagementGroupsResponse(BaseModel):
    groups: list[ManagementGroupRow]


class ManagementScanRequest(BaseModel):
    run_health_scan: bool = True
    run_sentinel_seo: bool = False
    seo_keyword_phrases: list[str] | None = None

    @field_validator("seo_keyword_phrases")
    @classmethod
    def _phrases_two_words(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return v
        for p in v:
            if len(str(p).split()) < 2:
                raise ValueError("Each seo_keyword_phrases entry must contain at least 2 words")
        return v


class ManagementScanResponse(BaseModel):
    enqueued: list[dict[str, Any]]
    errors: list[str] = Field(default_factory=list)
