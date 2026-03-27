"""
TeleFix dashboard helpers — group warmup / search visibility, bot factory snapshot,
and scrape vault browser (``vault/data/scrapes/*.json``).
"""

from __future__ import annotations

import csv
import io
import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_OPERATIONS_CHAT_LINK = os.environ.get(
    "OPERATIONS_CHAT_LINK", "https://t.me/Ahu_Management_Private"
)

import structlog
from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/telefix", tags=["telefix-dashboard"])

_REPO_ROOT = Path(__file__).resolve().parents[5]
_VAULT_DATA = _REPO_ROOT / "vault" / "data"
_GROUP_STATE = _VAULT_DATA / "group_infiltration.json"
_BOT_STATE = _VAULT_DATA / "bot_factory.json"
_SCRAPES_DIR = _VAULT_DATA / "scrapes"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_dirs() -> None:
    _VAULT_DATA.mkdir(parents=True, exist_ok=True)
    _SCRAPES_DIR.mkdir(parents=True, exist_ok=True)


def _read_json(path: Path, default: Any) -> Any:
    if not path.is_file():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        log.warning("telefix_json_corrupt", path=str(path))
        return default


def _write_json(path: Path, data: Any) -> None:
    _ensure_dirs()
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _default_groups() -> list[dict[str, Any]]:
    return [
        {
            "id": "g1",
            "name_he": "קהילת משקיעים תל אביב",
            "warmup_days": 3,
            "is_private": True,
            "in_search": False,
        },
        {
            "id": "g2",
            "name_he": "ערוץ הזדמנויות נדל״ן",
            "warmup_days": 11,
            "is_private": False,
            "in_search": True,
        },
        {
            "id": "g3",
            "name_he": "מועדון קריפטו שקט",
            "warmup_days": 7,
            "is_private": True,
            "in_search": False,
        },
    ]


def _default_bot_state() -> dict[str, Any]:
    return {
        "bots_created_total": 12,
        "sessions_bound": 8,
        "warmup_active": 3,
        "bulk_job": None,
        "tokens": [
            {
                "bot_id": "b_demo_1",
                "username": "telefix_scout_alpha_bot",
                "token_suffix": "x7Qk",
                "session_stem": "session_tl_01",
                "warmup_status": "חימום — יום 4/14",
            },
            {
                "bot_id": "b_demo_2",
                "username": "telefix_listener_beta_bot",
                "token_suffix": "m2Zp",
                "session_stem": "session_ha_02",
                "warmup_status": "מוכן לשימוש",
            },
        ],
    }


def _load_group_state() -> dict[str, Any]:
    raw = _read_json(_GROUP_STATE, {})
    groups = raw.get("groups")
    if not isinstance(groups, list) or not groups:
        return {"groups": _default_groups(), "updated_at": _utc_now_iso()}
    return raw


def _load_bot_state() -> dict[str, Any]:
    raw = _read_json(_BOT_STATE, {})
    if not raw.get("tokens"):
        return {**_default_bot_state(), "updated_at": _utc_now_iso()}
    return raw


# ── Group infiltration ──────────────────────────────────────────────────────────


class GroupInfiltrationRow(BaseModel):
    id: str
    name_he: str
    warmup_days: int = Field(ge=1, le=14)
    is_private: bool
    in_search: bool


class GroupInfiltrationResponse(BaseModel):
    groups: list[GroupInfiltrationRow]
    updated_at: str


@router.get("/group-infiltration", response_model=GroupInfiltrationResponse)
async def get_group_infiltration() -> GroupInfiltrationResponse:
    st = _load_group_state()
    groups = st.get("groups") or []
    rows: list[GroupInfiltrationRow] = []
    for g in groups:
        if not isinstance(g, dict):
            continue
        try:
            rows.append(
                GroupInfiltrationRow(
                    id=str(g["id"]),
                    name_he=str(g.get("name_he") or g.get("name") or ""),
                    warmup_days=max(1, min(14, int(g.get("warmup_days", 1)))),
                    is_private=bool(g.get("is_private", False)),
                    in_search=bool(g.get("in_search", False)),
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    if not rows:
        rows = [GroupInfiltrationRow(**x) for x in _default_groups()]
    return GroupInfiltrationResponse(
        groups=rows,
        updated_at=str(st.get("updated_at") or _utc_now_iso()),
    )


class CreateGroupRequest(BaseModel):
    name_he: str = Field(min_length=1, max_length=120)
    invite_link: str | None = None
    is_private: bool = True
    warmup_days: int = Field(default=1, ge=1, le=14)


class CreateGroupResponse(BaseModel):
    ok: bool
    group: GroupInfiltrationRow


@router.post("/group-infiltration", response_model=CreateGroupResponse, status_code=201)
async def create_group(body: CreateGroupRequest) -> CreateGroupResponse:
    st = _load_group_state()
    groups: list[dict[str, Any]] = list(st.get("groups") or [])
    new_id = f"g{uuid.uuid4().hex[:8]}"
    new_group: dict[str, Any] = {
        "id": new_id,
        "name_he": body.name_he,
        "invite_link": body.invite_link,
        "is_private": body.is_private,
        "warmup_days": body.warmup_days,
        "in_search": False,
    }
    groups.append(new_group)
    st["groups"] = groups
    st["updated_at"] = _utc_now_iso()
    _write_json(_GROUP_STATE, st)
    log.info("group_created", id=new_id, name=body.name_he)
    return CreateGroupResponse(
        ok=True,
        group=GroupInfiltrationRow(
            id=new_id,
            name_he=body.name_he,
            warmup_days=body.warmup_days,
            is_private=body.is_private,
            in_search=False,
        ),
    )


class DeleteGroupResponse(BaseModel):
    ok: bool
    group_id: str


@router.delete("/group-infiltration/{group_id}", response_model=DeleteGroupResponse)
async def delete_group(group_id: str) -> DeleteGroupResponse:
    st = _load_group_state()
    groups: list[dict[str, Any]] = list(st.get("groups") or [])
    new_groups = [g for g in groups if isinstance(g, dict) and str(g.get("id")) != group_id]
    if len(new_groups) == len(groups):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown group")
    st["groups"] = new_groups
    st["updated_at"] = _utc_now_iso()
    _write_json(_GROUP_STATE, st)
    log.info("group_deleted", id=group_id)
    return DeleteGroupResponse(ok=True, group_id=group_id)


class ForceSearchResponse(BaseModel):
    ok: bool
    group_id: str
    in_search: bool
    detail: str


@router.post(
    "/group-infiltration/{group_id}/force-search",
    response_model=ForceSearchResponse,
)
async def force_group_search(group_id: str) -> ForceSearchResponse:
    st = _load_group_state()
    groups = list(st.get("groups") or [])
    found = False
    for i, g in enumerate(groups):
        if isinstance(g, dict) and str(g.get("id")) == group_id:
            g["in_search"] = True
            groups[i] = g
            found = True
            break
    if not found:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown group")
    st["groups"] = groups
    st["updated_at"] = _utc_now_iso()
    _write_json(_GROUP_STATE, st)
    return ForceSearchResponse(
        ok=True,
        group_id=group_id,
        in_search=True,
        detail="ניסיון העלאה לחיפוש נרשם — סטטוס עודכן למופיע בחיפוש.",
    )


# ── Bot factory ────────────────────────────────────────────────────────────────


class BotTokenPublic(BaseModel):
    bot_id: str
    username: str | None
    token_masked: str
    session_stem: str | None
    warmup_status: str


class BotFactoryResponse(BaseModel):
    bots_created_total: int
    sessions_bound: int
    warmup_active: int
    bulk_job: dict[str, Any] | None
    tokens: list[BotTokenPublic]
    updated_at: str


def _mask_token(suffix: str) -> str:
    s = (suffix or "").strip()
    if len(s) <= 2:
        return "••••••••"
    return f"••••••••{s}"


@router.get("/bot-factory", response_model=BotFactoryResponse)
async def get_bot_factory() -> BotFactoryResponse:
    st = _load_bot_state()
    tokens_in = st.get("tokens") or []
    out: list[BotTokenPublic] = []
    for t in tokens_in:
        if not isinstance(t, dict):
            continue
        suf = str(t.get("token_suffix") or "")[-6:] or "????"
        out.append(
            BotTokenPublic(
                bot_id=str(t.get("bot_id") or uuid.uuid4().hex[:8]),
                username=t.get("username"),
                token_masked=_mask_token(suf),
                session_stem=t.get("session_stem"),
                warmup_status=str(t.get("warmup_status") or "—"),
            )
        )
    return BotFactoryResponse(
        bots_created_total=int(st.get("bots_created_total") or 0),
        sessions_bound=int(st.get("sessions_bound") or 0),
        warmup_active=int(st.get("warmup_active") or 0),
        bulk_job=st.get("bulk_job") if isinstance(st.get("bulk_job"), dict) else None,
        tokens=out,
        updated_at=str(st.get("updated_at") or _utc_now_iso()),
    )


class BulkBotsBody(BaseModel):
    count: int = Field(ge=1, le=500)


class BulkBotsResponse(BaseModel):
    ok: bool
    requested: int
    job_id: str
    message: str


@router.post("/bot-factory/bulk", response_model=BulkBotsResponse)
async def bot_factory_bulk(body: BulkBotsBody) -> BulkBotsResponse:
    st = _load_bot_state()
    job_id = uuid.uuid4().hex[:12]
    st["bulk_job"] = {
        "job_id": job_id,
        "requested": body.count,
        "status": "queued",
        "started_at": _utc_now_iso(),
    }
    st["bots_created_total"] = int(st.get("bots_created_total") or 0) + body.count
    st["warmup_active"] = int(st.get("warmup_active") or 0) + min(body.count, 50)
    st["updated_at"] = _utc_now_iso()
    _write_json(_BOT_STATE, st)
    log.info("bot_factory_bulk_enqueued", job_id=job_id, count=body.count)
    return BulkBotsResponse(
        ok=True,
        requested=body.count,
        job_id=job_id,
        message=f"ייצור המוני נרשם ({body.count}) — מזהה משימה: {job_id}",
    )


# ── Real groups from telefix.db ─────────────────────────────────────────────────

_TELEFIX_DB_SEARCH_PATHS = [
    Path(os.environ.get("TELEFIX_DB_PATH", "")) if os.environ.get("TELEFIX_DB_PATH") else None,
    _REPO_ROOT / "telefix.db",
    Path.home() / "Desktop" / "telefix.db",
    Path("C:/Users/Yarin/Desktop/telefix.db"),
    Path.home() / "Desktop" / "Nexus-Orchestrator" / "telefix.db",
]


def _find_telefix_db_path() -> Path | None:
    for p in _TELEFIX_DB_SEARCH_PATHS:
        if p is not None and p.is_file():
            return p
    # Recursive fallback on Desktop
    for desk in (Path.home() / "Desktop", Path("C:/Users/Yarin/Desktop")):
        if desk.is_dir():
            for found in desk.rglob("telefix.db"):
                return found
    return None


class TelefixGroupRecord(BaseModel):
    id: int | str
    title: str
    invite_link: str | None = None
    username: str | None = None
    member_count: int | None = None


class TelefixGroupsResponse(BaseModel):
    groups: list[TelefixGroupRecord]
    count: int
    source: str


@router.get("/groups", response_model=TelefixGroupsResponse)
async def get_telefix_groups() -> TelefixGroupsResponse:
    """
    Return actual group records from telefix.db — title and invite link (t.me/…).
    Falls back to the JSON vault if the DB is unavailable.
    """
    db_path = _find_telefix_db_path()
    if db_path is not None:
        try:
            conn = sqlite3.connect(str(db_path), timeout=5, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            try:
                cur = conn.cursor()
                # Try common table/column names used by telefix scrapers
                groups: list[TelefixGroupRecord] = []
                for table, id_col, title_col, invite_col, username_col, members_col in [
                    ("groups", "id", "title", "invite_link", "username", "member_count"),
                    ("telefix_groups", "id", "title", "invite_link", "username", "members"),
                    ("telegram_groups", "group_id", "name", "invite_link", "username", "members_count"),
                    ("telefix", "id", "title", "invite_link", "username", "user_count"),
                ]:
                    try:
                        cur.execute(f"SELECT * FROM {table} LIMIT 1")
                        cols = [d[0] for d in cur.description]
                        _id = id_col if id_col in cols else cols[0]
                        _title = title_col if title_col in cols else (cols[1] if len(cols) > 1 else cols[0])
                        _invite = invite_col if invite_col in cols else None
                        _uname = username_col if username_col in cols else None
                        _members = members_col if members_col in cols else None

                        select_cols = [f"{_id}", f"{_title}"]
                        if _invite:
                            select_cols.append(_invite)
                        if _uname:
                            select_cols.append(_uname)
                        if _members:
                            select_cols.append(_members)

                        cur.execute(f"SELECT {', '.join(select_cols)} FROM {table}")
                        rows = cur.fetchall()
                        for row in rows:
                            row_dict = dict(zip(select_cols, row))
                            raw_invite = row_dict.get(_invite) if _invite else None
                            raw_uname = row_dict.get(_uname) if _uname else None
                            # Build invite link: prefer stored link, else construct from username
                            invite = None
                            if raw_invite:
                                invite = str(raw_invite)
                            elif raw_uname:
                                uname = str(raw_uname).lstrip("@")
                                invite = f"https://t.me/{uname}"
                            groups.append(
                                TelefixGroupRecord(
                                    id=row_dict[_id],
                                    title=str(row_dict[_title] or ""),
                                    invite_link=invite,
                                    username=str(raw_uname) if raw_uname else None,
                                    member_count=int(row_dict[_members]) if _members and row_dict.get(_members) else None,
                                )
                            )
                        if groups:
                            return TelefixGroupsResponse(
                                groups=groups,
                                count=len(groups),
                                source=f"telefix.db:{table}",
                            )
                    except sqlite3.OperationalError:
                        continue
            finally:
                conn.close()
        except Exception as exc:
            log.warning("telefix_groups_db_error", error=str(exc))

    # Fallback: return vault JSON groups with placeholder invite links
    st = _load_group_state()
    vault_groups = st.get("groups") or []
    fallback: list[TelefixGroupRecord] = []
    for g in vault_groups:
        if not isinstance(g, dict):
            continue
        name = str(g.get("name_he") or g.get("name") or "")
        fallback.append(
            TelefixGroupRecord(
                id=str(g.get("id") or uuid.uuid4().hex[:8]),
                title=name,
                invite_link=None,
            )
        )
    return TelefixGroupsResponse(
        groups=fallback,
        count=len(fallback),
        source="vault/group_infiltration.json",
    )


def _ids_match_row(want: str, raw_id: Any) -> bool:
    if raw_id is None or want is None:
        return False
    a = str(raw_id).strip()
    b = str(want).strip()
    if a == b:
        return True
    try:
        return int(a) == int(float(b))
    except (ValueError, TypeError):
        return False


def lookup_telefix_group_by_id(group_id: str) -> tuple[str, str | None] | None:
    """
    Resolve (title, invite_link) from telefix.db for a group row id.
    Used when force-search targets a DB id not yet present in vault JSON.
    """
    want = str(group_id).strip()
    db_path = _find_telefix_db_path()
    if db_path is None:
        return None
    try:
        conn = sqlite3.connect(str(db_path), timeout=5, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.cursor()
            for table, id_col, title_col, invite_col, username_col, members_col in [
                ("groups", "id", "title", "invite_link", "username", "member_count"),
                ("telefix_groups", "id", "title", "invite_link", "username", "members"),
                ("telegram_groups", "group_id", "name", "invite_link", "username", "members_count"),
                ("telefix", "id", "title", "invite_link", "username", "user_count"),
            ]:
                try:
                    cur.execute(f"SELECT * FROM {table} LIMIT 1")
                    cols = [d[0] for d in cur.description]
                    _id = id_col if id_col in cols else cols[0]
                    _title = title_col if title_col in cols else (cols[1] if len(cols) > 1 else cols[0])
                    _invite = invite_col if invite_col in cols else None
                    _uname = username_col if username_col in cols else None
                    select_cols = [f"{_id}", f"{_title}"]
                    if _invite:
                        select_cols.append(_invite)
                    if _uname:
                        select_cols.append(_uname)
                    cur.execute(f"SELECT {', '.join(select_cols)} FROM {table}")
                    for row in cur.fetchall():
                        row_dict = dict(zip(select_cols, row))
                        rid = row_dict.get(_id)
                        if not _ids_match_row(want, rid):
                            continue
                        title = str(row_dict.get(_title) or "")
                        raw_invite = row_dict.get(_invite) if _invite else None
                        raw_uname = row_dict.get(_uname) if _uname else None
                        invite: str | None = None
                        if raw_invite:
                            invite = str(raw_invite)
                        elif raw_uname:
                            invite = f"https://t.me/{str(raw_uname).lstrip('@')}"
                        return (title, invite)
                except sqlite3.OperationalError:
                    continue
        finally:
            conn.close()
    except Exception as exc:
        log.warning("lookup_telefix_group_by_id_failed", error=str(exc))
    return None


_GROUP_FACTORY_ACTIVITY = _VAULT_DATA / "group_factory_activity.json"
_MAX_FACTORY_ACTIVITY = 200


def append_group_factory_activity(level: str, message: str) -> None:
    """Append one line to durable UI activity log (trimmed)."""
    _ensure_dirs()
    raw = _read_json(_GROUP_FACTORY_ACTIVITY, {})
    entries = raw.get("entries") if isinstance(raw.get("entries"), list) else []
    entries.append({"ts": _utc_now_iso(), "level": level, "message": message})
    if len(entries) > _MAX_FACTORY_ACTIVITY:
        entries = entries[-_MAX_FACTORY_ACTIVITY:]
    _write_json(
        _GROUP_FACTORY_ACTIVITY,
        {"entries": entries, "updated_at": _utc_now_iso()},
    )


# ── DB status — real row counts (drives Verified / Written UI badges) ──────────


class TelefixDbStatus(BaseModel):
    db_found: bool
    db_path: str
    tables: dict[str, int]
    verified: bool  # True when groups table has at least 1 row
    written: bool   # True when scrape_files table has at least 1 row
    total_rows: int


@router.get("/db-status", response_model=TelefixDbStatus, summary="Real telefix.db row counts")
async def get_telefix_db_status() -> TelefixDbStatus:
    """
    Return row counts for every table in telefix.db.

    The UI uses ``verified`` (groups rows > 0) and ``written`` (scrape_files rows > 0)
    to replace placeholder badges with real status indicators.
    """
    db_path = _find_telefix_db_path()
    if db_path is None:
        return TelefixDbStatus(
            db_found=False,
            db_path="",
            tables={},
            verified=False,
            written=False,
            total_rows=0,
        )

    tables_to_check = ["groups", "sessions", "scrape_files", "system_events"]
    counts: dict[str, int] = {}
    try:
        conn = sqlite3.connect(str(db_path), timeout=5, check_same_thread=False)
        try:
            for table in tables_to_check:
                try:
                    row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                    counts[table] = int(row[0]) if row else 0
                except sqlite3.OperationalError:
                    counts[table] = 0
        finally:
            conn.close()
    except Exception as exc:
        log.warning("telefix_db_status_error", error=str(exc))
        for table in tables_to_check:
            counts[table] = 0

    total = sum(counts.values())
    return TelefixDbStatus(
        db_found=True,
        db_path=str(db_path),
        tables=counts,
        verified=counts.get("groups", 0) > 0,
        written=counts.get("scrape_files", 0) > 0,
        total_rows=total,
    )


# ── Group Factory Schedule ─────────────────────────────────────────────────────

_GROUP_FACTORY_STATE = _REPO_ROOT / "vault" / "data" / "group_factory_state.json"
_GROUP_FACTORY_SETTINGS = _VAULT_DATA / "group_factory_settings.json"

_DEFAULT_FACTORY_SETTINGS = {
    "warmup_days": 14,
    "cooldown_hours": 24,
    "groups_per_day": 2,
    "automation_armed": False,
}


def _load_factory_settings() -> dict[str, Any]:
    raw = _read_json(_GROUP_FACTORY_SETTINGS, {})
    return {**_DEFAULT_FACTORY_SETTINGS, **raw}


def _load_factory_state() -> dict[str, Any]:
    return _read_json(_GROUP_FACTORY_STATE, {"groups": {}, "updated_at": None})


class GroupFactoryScheduleResponse(BaseModel):
    settings: dict[str, Any]
    groups_total: int
    groups_in_warmup: int
    groups_in_public_trial: int
    groups_in_search: int
    groups: list[dict[str, Any]]
    updated_at: str | None


class GroupFactorySettingsPatch(BaseModel):
    warmup_days: int | None = Field(default=None, ge=1, le=30)
    cooldown_hours: int | None = Field(default=None, ge=1, le=168)
    groups_per_day: int | None = Field(default=None, ge=1, le=50)


@router.get("/group-factory/schedule", response_model=GroupFactoryScheduleResponse)
async def get_group_factory_schedule() -> GroupFactoryScheduleResponse:
    settings = _load_factory_settings()
    state = _load_factory_state()
    raw_groups = state.get("groups") or {}

    groups_list: list[dict[str, Any]] = []
    in_warmup = 0
    in_public_trial = 0
    in_search = 0

    if isinstance(raw_groups, dict):
        for key, g in raw_groups.items():
            if not isinstance(g, dict):
                continue
            phase = str(g.get("phase") or "warmup")
            if phase == "warmup":
                in_warmup += 1
            elif phase == "public_trial":
                in_public_trial += 1
            elif phase in ("in_search", "search_indexed"):
                in_search += 1
            groups_list.append({
                "key": key,
                "phase": phase,
                "birth_ts": g.get("birth_ts"),
                "display_title_hint": g.get("display_title_hint"),
                "search_indexed": bool(g.get("search_indexed", False)),
                "cooldown_until": g.get("cooldown_until"),
                "last_index_probe_at": g.get("last_index_probe_at"),
            })
    elif isinstance(raw_groups, list):
        for g in raw_groups:
            if not isinstance(g, dict):
                continue
            phase = str(g.get("phase") or "warmup")
            if phase == "warmup":
                in_warmup += 1
            elif phase == "public_trial":
                in_public_trial += 1
            elif phase in ("in_search", "search_indexed"):
                in_search += 1
            groups_list.append(g)

    return GroupFactoryScheduleResponse(
        settings=settings,
        groups_total=len(groups_list),
        groups_in_warmup=in_warmup,
        groups_in_public_trial=in_public_trial,
        groups_in_search=in_search,
        groups=groups_list,
        updated_at=state.get("updated_at"),
    )


@router.patch("/group-factory/schedule", summary="Update group factory settings")
async def patch_group_factory_schedule(body: GroupFactorySettingsPatch) -> dict[str, Any]:
    current = _load_factory_settings()
    if body.warmup_days is not None:
        current["warmup_days"] = body.warmup_days
    if body.cooldown_hours is not None:
        current["cooldown_hours"] = body.cooldown_hours
    if body.groups_per_day is not None:
        current["groups_per_day"] = body.groups_per_day
    current["updated_at"] = _utc_now_iso()
    _write_json(_GROUP_FACTORY_SETTINGS, current)
    log.info("group_factory_settings_updated", settings=current)
    append_group_factory_activity("info", "הגדרות מפעל קבוצות עודכנו.")
    return {"ok": True, "settings": current}


class GroupFactoryActivityResponse(BaseModel):
    entries: list[dict[str, Any]]
    updated_at: str | None


@router.get("/group-factory/activity", response_model=GroupFactoryActivityResponse)
async def get_group_factory_activity() -> GroupFactoryActivityResponse:
    raw = _read_json(_GROUP_FACTORY_ACTIVITY, {})
    entries = raw.get("entries") if isinstance(raw.get("entries"), list) else []
    return GroupFactoryActivityResponse(
        entries=[e for e in entries if isinstance(e, dict)],
        updated_at=raw.get("updated_at"),
    )


@router.post("/group-factory/start", summary="Arm group factory automation (UI + settings flag)")
async def post_group_factory_start() -> dict[str, Any]:
    current = _load_factory_settings()
    current["automation_armed"] = True
    current["armed_at"] = _utc_now_iso()
    current["updated_at"] = _utc_now_iso()
    _write_json(_GROUP_FACTORY_SETTINGS, current)
    msg = (
        "מפעל הקבוצות הופעל: דגל automation_armed=true נשמר. "
        "לולאת GroupFactory ברקע רצה בתהליך המאסטר (אם פעיל)."
    )
    append_group_factory_activity("info", msg)
    log.info("group_factory_armed")
    return {"ok": True, "settings": current, "detail": msg}


# ── Operations config ──────────────────────────────────────────────────────────


@router.get("/ops-config", summary="Return operations config values from environment")
async def get_ops_config() -> dict[str, str]:
    """Returns env-driven config values consumed by the dashboard UI."""
    return {
        "operations_chat_link": os.environ.get(
            "OPERATIONS_CHAT_LINK", _OPERATIONS_CHAT_LINK
        ),
    }


# ── DB download / sync ─────────────────────────────────────────────────────────


def _ensure_telefix_db() -> Path:
    """Return a valid telefix.db path, creating an empty SQLite file if absent."""
    db_path = _find_telefix_db_path()
    if db_path is not None:
        return db_path

    # Fallback: create an empty valid SQLite file at the repo root so the
    # frontend never receives a 404 on /api/telefix/sync.
    fallback = _REPO_ROOT / "telefix.db"
    try:
        conn = sqlite3.connect(str(fallback))
        conn.execute(
            "CREATE TABLE IF NOT EXISTS groups "
            "(id INTEGER PRIMARY KEY, title TEXT, invite_link TEXT, "
            "username TEXT, member_count INTEGER)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sessions "
            "(id INTEGER PRIMARY KEY, phone TEXT, status TEXT)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS scrape_files "
            "(id INTEGER PRIMARY KEY, filename TEXT, scraped_at TEXT)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS system_events "
            "(id INTEGER PRIMARY KEY, event TEXT, ts TEXT)"
        )
        conn.commit()
        conn.close()
        log.info("telefix_db_created_empty", path=str(fallback))
    except Exception as exc:
        log.warning("telefix_db_create_failed", error=str(exc))
    return fallback


def _trigger_initial_scrape() -> None:
    """Fire-and-forget: enqueue an InitialScrape task via ARQ if Redis is reachable."""
    try:
        import asyncio
        import redis as _redis_sync  # type: ignore[import-untyped]

        r = _redis_sync.Redis.from_url(
            os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0"),
            socket_connect_timeout=2,
        )
        import json as _json
        import uuid as _uuid

        payload = _json.dumps({
            "task_type": "telegram.auto_scrape",
            "project_id": os.environ.get("DEFAULT_PROJECT_ID", "telefix"),
            "priority": 5,
            "params": {"trigger": "initial_scrape"},
            "job_id": _uuid.uuid4().hex,
        })
        r.lpush("nexus:tasks", payload)
        r.close()
        log.info("telefix_initial_scrape_enqueued")
    except Exception as exc:
        log.warning("telefix_initial_scrape_enqueue_failed", error=str(exc))


@router.get("/sync", summary="Download telefix.db for frontend sync")
async def download_telefix_db() -> FileResponse:
    """
    Serve ``telefix.db`` as a binary download.

    If the database file does not exist yet, an empty but structurally valid
    SQLite file is created at the repo root and an ``InitialScrape`` task is
    enqueued so the frontend always gets a 200 instead of a 404.
    """
    db_existed = _find_telefix_db_path() is not None
    db_path = _ensure_telefix_db()
    if not db_path.is_file():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="telefix.db could not be located or created",
        )
    if not db_existed:
        # DB was just created — trigger a background scrape to populate it
        _trigger_initial_scrape()
    return FileResponse(
        path=str(db_path),
        media_type="application/octet-stream",
        filename="telefix.db",
    )


# ── Scrapes vault ───────────────────────────────────────────────────────────────


def _normalize_scrape_record(obj: Any, filename: str) -> dict[str, Any]:
    """Map arbitrary JSON files into a flat row for UI + CSV."""
    if isinstance(obj, list):
        obj = {
            "users": obj,
            "source_group": "",
            "selected_messages": [],
            "scraped_at": _utc_now_iso(),
            "ai_relevance": 0.5,
            "keywords": [],
        }
    if not isinstance(obj, dict):
        obj = {}
    users = obj.get("users") or obj.get("scraped_users") or []
    if not isinstance(users, list):
        users = []
    msgs = obj.get("selected_messages") or obj.get("messages") or []
    if not isinstance(msgs, list):
        msgs = []
    kw = obj.get("keywords") or []
    if not isinstance(kw, list):
        kw = []
    rel = obj.get("ai_relevance")
    try:
        rel_f = float(rel) if rel is not None else 0.5
    except (TypeError, ValueError):
        rel_f = 0.5
    return {
        "file": filename,
        "scraped_at": str(obj.get("scraped_at") or obj.get("created_at") or ""),
        "source_group": str(obj.get("source_group") or obj.get("group") or ""),
        "users": users,
        "selected_messages": msgs,
        "ai_relevance": max(0.0, min(1.0, rel_f)),
        "keywords": [str(x) for x in kw],
    }


def _iter_scrape_files() -> list[dict[str, Any]]:
    _ensure_dirs()
    rows: list[dict[str, Any]] = []
    if not _SCRAPES_DIR.is_dir():
        return rows
    for p in sorted(_SCRAPES_DIR.glob("*.json")):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        rows.append(_normalize_scrape_record(data, p.name))
    return rows


class ScrapeVaultResponse(BaseModel):
    files: list[dict[str, Any]]
    count: int


@router.get("/scrapes", response_model=ScrapeVaultResponse)
async def list_scrapes() -> ScrapeVaultResponse:
    rows = _iter_scrape_files()
    return ScrapeVaultResponse(files=rows, count=len(rows))


@router.get("/scrapes/export")
async def export_scrapes_csv(
    date_from: str | None = Query(None, description="ISO date substring filter"),
    keyword: str | None = Query(None),
    min_relevance: float | None = Query(None, ge=0.0, le=1.0),
) -> Response:
    rows = _iter_scrape_files()
    kw_l = (keyword or "").strip().lower()

    def row_ok(r: dict[str, Any]) -> bool:
        if date_from and date_from not in str(r.get("scraped_at") or ""):
            return False
        if min_relevance is not None and float(r.get("ai_relevance") or 0) < min_relevance:
            return False
        if kw_l:
            blob = json.dumps(r, ensure_ascii=False).lower()
            if kw_l not in blob:
                return False
        return True

    filtered = [r for r in rows if row_ok(r)]

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "file",
            "scraped_at",
            "source_group",
            "ai_relevance",
            "keywords",
            "users_json",
            "messages_json",
        ]
    )
    for r in filtered:
        w.writerow(
            [
                r.get("file"),
                r.get("scraped_at"),
                r.get("source_group"),
                r.get("ai_relevance"),
                ";".join(r.get("keywords") or []),
                json.dumps(r.get("users"), ensure_ascii=False),
                json.dumps(r.get("selected_messages"), ensure_ascii=False),
            ]
        )

    body = "\ufeff" + buf.getvalue()
    return Response(
        content=body.encode("utf-8"),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="scrapes_export_{uuid.uuid4().hex[:8]}.csv"'
        },
    )
