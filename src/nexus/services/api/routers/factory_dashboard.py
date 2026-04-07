"""
SEO / private-group factory API for Nexus OS dashboard.

Paths (mounted at ``/api``): ``POST /factory/start-seo-groups``,
``GET /factory/seo-status``, ``GET /factory/seo-report``.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from nexus.api.dependencies import RedisDep
from nexus.shared.config import settings

from src.nexus.services.api.routers import telefix_dashboard as tf

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/factory", tags=["factory-seo"])

_SEO_FACTORY_RUN = tf._VAULT_DATA / "seo_factory_run.json"


def _seo_run_default() -> dict[str, Any]:
    return {
        "active": False,
        "run_id": None,
        "started_at": None,
        "target_total": 50,
        "updated_at": None,
    }


def _load_seo_run() -> dict[str, Any]:
    raw = tf._read_json(_SEO_FACTORY_RUN, {})
    return {**_seo_run_default(), **raw} if isinstance(raw, dict) else _seo_run_default()


def _write_seo_run(data: dict[str, Any]) -> None:
    tf._write_json(_SEO_FACTORY_RUN, data)


async def _require_legacy_telefix_writes() -> None:
    if not settings.legacy_telefix_bot_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Legacy TeleFix bot controls are disabled. Use /api/management/*.",
        )


def _is_private_tme_link(url: str) -> bool:
    u = url.strip().lower()
    return "t.me/+" in u or "telegram.me/+" in u


def _normalize_invite_href(s: str) -> str:
    t = s.strip()
    if not t:
        return t
    if t.startswith("http://") or t.startswith("https://"):
        return t
    if t.startswith("t.me/") or t.startswith("telegram.me/"):
        return f"https://{t}"
    return t


def _private_invite_links_from_db() -> list[str]:
    db_path = tf._find_telefix_db_path()
    if db_path is None or not db_path.is_file():
        return []
    out: list[str] = []
    try:
        conn = sqlite3.connect(str(db_path), timeout=5, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.cursor()
            if not tf._sqlite_table_exists(cur, "managed_groups"):
                return []
            has_groups_tbl = tf._sqlite_table_exists(cur, "groups")
            if has_groups_tbl:
                sql = """
                    SELECT g.invite_link AS joined_invite, mg.username
                    FROM managed_groups mg
                    LEFT JOIN groups g ON CAST(mg.group_id AS TEXT) = CAST(g.id AS TEXT)
                """
            else:
                sql = """
                    SELECT NULL AS joined_invite, mg.username
                    FROM managed_groups mg
                """
            cur.execute(sql)
            for row in cur.fetchall():
                inv = tf._invite_for_managed_row(row["joined_invite"], row["username"])
                if inv and _is_private_tme_link(inv):
                    out.append(_normalize_invite_href(inv))
        finally:
            conn.close()
    except Exception as exc:
        log.warning("seo_factory_db_private_links_failed", error=str(exc))
    return out


def _private_invite_links_from_vault() -> list[str]:
    raw = tf._read_json(tf._GROUP_STATE, {})
    groups = raw.get("groups") if isinstance(raw.get("groups"), list) else []
    out: list[str] = []
    for g in groups:
        if not isinstance(g, dict):
            continue
        if not bool(g.get("is_private", False)):
            continue
        link = (g.get("telegram_link") or g.get("invite_link") or "").strip()
        if link and _is_private_tme_link(link):
            out.append(_normalize_invite_href(link))
    return out


def _dedupe_preserve_order(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for u in urls:
        key = u.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(key)
    return result


def _factory_state_group_count() -> int:
    state = tf._load_factory_state()
    raw = state.get("groups") or {}
    if isinstance(raw, dict):
        return len(raw)
    if isinstance(raw, list):
        return len(raw)
    return 0


async def _session_matched_factory_count(redis: Any) -> int:
    try:
        resp = await tf._telefix_groups_factory_scope(redis)
        return int(resp.count or len(resp.groups or []))
    except Exception as exc:
        log.debug("seo_factory_factory_scope_failed", error=str(exc))
        return 0


class SeoFactoryStatusOut(BaseModel):
    active: bool
    phase: str = Field(description="idle | creating_groups | attaching_sessions | winding_down")
    message_he: str
    current: int = 0
    total: int = 0
    groups_in_factory_state: int = 0
    groups_with_matched_session: int = 0


class SeoFactoryReportOut(BaseModel):
    links: list[str]
    total_private_groups_created: int


class StartSeoFactoryOut(BaseModel):
    ok: bool
    detail: str
    run_id: str
    target_total: int


@router.post(
    "/start-seo-groups",
    response_model=StartSeoFactoryOut,
    dependencies=[Depends(_require_legacy_telefix_writes)],
)
async def post_start_seo_groups() -> StartSeoFactoryOut:
    """
    Arm the group factory for SEO private batches and record an SEO run for UI polling.
    """
    fac_settings = tf._load_factory_settings()
    gpd = int(fac_settings.get("groups_per_day") or 2)
    env_target = int(os.getenv("SEO_FACTORY_TARGET_GROUPS", "50"))
    target_total = max(env_target, min(200, gpd * 10))

    run_id = uuid.uuid4().hex[:12]
    now = tf._utc_now_iso()
    _write_seo_run({
        "active": True,
        "run_id": run_id,
        "started_at": now,
        "target_total": target_total,
        "updated_at": now,
    })

    current = {**fac_settings}
    current["automation_armed"] = True
    current["armed_at"] = now
    current["updated_at"] = now
    tf._write_json(tf._GROUP_FACTORY_SETTINGS, current)

    detail = (
        "מפעל SEO הופעל: נרשמה ריצה חדשה ו־automation_armed הופעל. "
        f"יעד קבוצות לסטטוס: {target_total}."
    )
    tf.append_group_factory_activity("info", f"[SEO Factory] {detail}")
    log.info("seo_factory_started", run_id=run_id, target_total=target_total)

    return StartSeoFactoryOut(
        ok=True,
        detail=detail,
        run_id=run_id,
        target_total=target_total,
    )


@router.get("/seo-status", response_model=SeoFactoryStatusOut)
async def get_seo_status(redis: RedisDep) -> SeoFactoryStatusOut:
    """
    Real-time style status derived from vault run flags + group factory state + fleet match counts.
    """
    run = _load_seo_run()
    settings_f = tf._load_factory_settings()
    armed = bool(settings_f.get("automation_armed"))
    run_flag = bool(run.get("active"))
    active = run_flag and armed

    target = int(run.get("target_total") or 50)
    in_state = _factory_state_group_count()
    matched = await _session_matched_factory_count(redis)

    if not active:
        return SeoFactoryStatusOut(
            active=False,
            phase="idle",
            message_he="המפעל לא פעיל — לחץ «התחל מפעל» להפעלה.",
            current=0,
            total=target,
            groups_in_factory_state=in_state,
            groups_with_matched_session=matched,
        )

    if in_state == 0:
        msg = f"יוצר קבוצות… 0/{target}"
        phase = "creating_groups"
    elif matched < in_state:
        msg = f"מצרף סשנים… {matched}/{in_state}"
        phase = "attaching_sessions"
    elif in_state < target:
        msg = f"יוצר קבוצות… {in_state}/{target}"
        phase = "creating_groups"
    else:
        msg = f"מסיים סבב… {in_state}/{target}"
        phase = "winding_down"

    run["updated_at"] = tf._utc_now_iso()
    _write_seo_run(run)

    return SeoFactoryStatusOut(
        active=True,
        phase=phase,
        message_he=msg,
        current=min(in_state, target) if phase == "creating_groups" else matched,
        total=target,
        groups_in_factory_state=in_state,
        groups_with_matched_session=matched,
    )


@router.get("/seo-report", response_model=SeoFactoryReportOut)
async def get_seo_report() -> SeoFactoryReportOut:
    """All known ``https://t.me/+…`` links from DB + private vault rows."""
    merged = _private_invite_links_from_db() + _private_invite_links_from_vault()
    links = _dedupe_preserve_order(merged)
    return SeoFactoryReportOut(
        links=links,
        total_private_groups_created=len(links),
    )
