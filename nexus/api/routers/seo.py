"""
SEO dashboard API for 1XPANEL — health metrics from telefix.db (member_stats, snapshots).
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from nexus.api.schemas.seo import (
    GroupHealthResponse,
    InviteSnapshotOut,
    RankProjectionResponse,
)
from nexus.shared.management_store import aggregate_rank_projection_inputs, get_group_health_bundle

router = APIRouter(prefix="/seo", tags=["seo"])

# Heuristic weights for rank_projection (documented; not ML).
_W_PREMIUM = 45.0
_W_TENURE = 30.0
_W_GROUP_AGE = 15.0
_W_SCALE = 10.0


@router.get("/group/{group_ref}/health", response_model=GroupHealthResponse)
async def get_seo_group_health(group_ref: int) -> GroupHealthResponse:
    bundle = await get_group_health_bundle(group_metadata_id=group_ref)
    if not bundle:
        bundle = await get_group_health_bundle(telegram_group_id=group_ref)
    if not bundle:
        raise HTTPException(status_code=404, detail="Group not found")

    total = max(0, int(bundle["total_members"] or 0))
    prem_c = int(bundle["premium_count"] or 0)
    del_c = int(bundle["deleted_count"] or 0)
    alive_c = int(bundle["active_real_count"] or 0)

    premium_pct = round(100.0 * prem_c / total, 2) if total else 0.0
    alive_ratio = round(alive_c / total, 4) if total else 0.0
    dead_ratio = round(del_c / total, 4) if total else 0.0

    snap_raw = bundle.get("invite_snapshot")
    invite_snapshot = None
    if snap_raw:
        invite_snapshot = InviteSnapshotOut(
            invite_link=snap_raw.get("invite_link"),
            usage_count=int(snap_raw.get("usage_count") or 0),
            participant_count=int(snap_raw.get("participant_count") or 0),
            ghost_delta=int(snap_raw.get("ghost_delta") or 0),
            audited_at=snap_raw.get("audited_at"),
        )

    return GroupHealthResponse(
        group_metadata_id=int(bundle["group_metadata_id"]),
        group_id=int(bundle["group_id"]),
        title=bundle.get("title"),
        username=bundle.get("username"),
        premium_pct=premium_pct,
        alive_ratio=alive_ratio,
        dead_ratio=dead_ratio,
        total_members=total,
        premium_count=prem_c,
        deleted_count=del_c,
        active_real_count=alive_c,
        invite_snapshot=invite_snapshot,
        gm_updated_at=bundle.get("gm_updated_at"),
    )


@router.get("/rank-projection", response_model=RankProjectionResponse)
async def get_rank_projection() -> RankProjectionResponse:
    inp = await aggregate_rank_projection_inputs()
    prem = float(inp.get("avg_premium_ratio") or 0.0)
    tenure_mo = float(inp.get("avg_member_tenure_months") or 0.0)
    age_days = float(inp.get("avg_group_row_age_days") or 0.0)
    n_groups = int(inp.get("n_groups") or 0)
    avg_members = float(inp.get("avg_members") or 0.0)

    tenure_u = min(1.0, tenure_mo / 12.0)
    age_u = min(1.0, age_days / 730.0)
    scale_u = min(1.0, n_groups / 20.0) * 0.5 + min(1.0, avg_members / 3000.0) * 0.5

    score = (
        _W_PREMIUM * prem
        + _W_TENURE * tenure_u
        + _W_GROUP_AGE * (1.0 - age_u * 0.5)
        + _W_SCALE * scale_u
    )
    score = round(max(0.0, min(100.0, score)), 2)

    if score >= 80:
        tier = "A"
    elif score >= 60:
        tier = "B"
    elif score >= 40:
        tier = "C"
    else:
        tier = "D"

    return RankProjectionResponse(
        score=score,
        tier=tier,
        inputs={
            "avg_premium_ratio": prem,
            "avg_member_tenure_months": tenure_mo,
            "avg_group_row_age_days": age_days,
            "n_groups": n_groups,
            "avg_members": avg_members,
            "weights": {
                "premium": _W_PREMIUM,
                "tenure": _W_TENURE,
                "group_age": _W_GROUP_AGE,
                "scale": _W_SCALE,
            },
        },
    )
