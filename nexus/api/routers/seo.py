"""
SEO dashboard API for 1XPANEL — health metrics from telefix.db (member_stats, snapshots).

Env (worker / ops): SEO_WATCHDOG_SHARDS, SEO_GROUP_IDS_JSON, SEO_DEFAULT_SUBSCRIPTION_DAYS,
NEXUS_HEALTH_PARTICIPANT_LIMIT — see seo.watchdog.audit task.
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from nexus.api.schemas.seo import (
    GroupHealthResponse,
    GroupSeoStatRow,
    InviteSnapshotOut,
    RankProjectionResponse,
    ReplacementAlertOut,
    SeoStatsResponse,
)
from nexus.shared.management_store import (
    SEO_INVITE_REDIS_KEY_FMT,
    aggregate_rank_projection_inputs,
    aggregate_seo_fleet_stats,
    get_group_health_bundle,
)

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


def _decode_redis_raw(raw: Any) -> str:
    if raw is None:
        return ""
    if isinstance(raw, (bytes, bytearray)):
        return raw.decode("utf-8", errors="replace")
    return str(raw)


async def _overlay_redis_invite_fields(redis: Any, rows: list[dict[str, Any]]) -> None:
    for g in rows:
        key = SEO_INVITE_REDIS_KEY_FMT.format(group_id=int(g["group_id"]))
        try:
            raw = await redis.get(key)
        except Exception:
            continue
        if not raw:
            continue
        try:
            data = json.loads(_decode_redis_raw(raw))
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(data, dict):
            if data.get("usage_count") is not None:
                g["redis_invite_usage"] = int(data["usage_count"])
            if data.get("audited_at"):
                g["redis_invite_audited_at"] = str(data["audited_at"])


@router.get("/stats", response_model=SeoStatsResponse)
async def get_seo_stats(
    request: Request,
    alerts_limit: int = Query(100, ge=0, le=500),
    top_ghost_limit: int = Query(50, ge=0, le=200),
    merge_redis_invites: bool = Query(
        False,
        description="Merge latest nexus:seo:invite:{group_id} keys when Redis is available.",
    ),
) -> SeoStatsResponse:
    raw = await aggregate_seo_fleet_stats(
        alerts_limit=alerts_limit,
        top_ghost_limit=top_ghost_limit,
    )
    groups = list(raw.get("groups") or [])
    top_ghost = list(raw.get("top_ghost_groups") or [])

    r = getattr(request.app.state, "redis", None)
    if merge_redis_invites and r is not None:
        try:
            await r.ping()
        except Exception:
            r = None
        if r is not None:
            await _overlay_redis_invite_fields(r, groups)
            await _overlay_redis_invite_fields(r, top_ghost)

    def _row(m: dict[str, Any]) -> GroupSeoStatRow:
        return GroupSeoStatRow(**{k: v for k, v in m.items() if k in GroupSeoStatRow.model_fields})

    alerts_raw = raw.get("replacement_alerts") or []
    return SeoStatsResponse(
        generated_at=str(raw.get("generated_at") or ""),
        n_groups=int(raw.get("n_groups") or 0),
        total_active_members=int(raw.get("total_active_members") or 0),
        fleet_premium_ratio=float(raw.get("fleet_premium_ratio") or 0.0),
        total_premium_members=int(raw.get("total_premium_members") or 0),
        replacement_alerts_count_7d=int(raw.get("replacement_alerts_count_7d") or 0),
        groups=[_row(x) for x in groups],
        top_ghost_groups=[_row(x) for x in top_ghost],
        replacement_alerts=[ReplacementAlertOut(**a) for a in alerts_raw],
    )
