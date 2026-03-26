"""
Group Infiltration router — manages vault/group_infiltration.json.

Endpoints
---------
GET  /api/telefix/group-infiltration
    Returns all tracked groups with auto-computed warmup_days and in_search status.

POST /api/telefix/group-infiltration
    Add a group manually (by ID + name) or trigger Telegram creation.

POST /api/telefix/group-infiltration/{group_id}/force-search
    Mark a group as in_search=True and record the forced timestamp.

DELETE /api/telefix/group-infiltration/{group_id}
    Remove a group from tracking.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/telefix", tags=["telefix"])

_REPO_ROOT = Path(__file__).resolve().parents[3]
_VAULT_FILE = _REPO_ROOT / "vault" / "group_infiltration.json"
_WARMUP_TARGET = 14  # days until a group is considered fully warmed up


# ── Helpers ────────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _load() -> dict[str, Any]:
    _VAULT_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not _VAULT_FILE.exists():
        _VAULT_FILE.write_text(
            json.dumps({"updated_at": _now_iso(), "groups": []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    try:
        data = json.loads(_VAULT_FILE.read_text(encoding="utf-8"))
        if "groups" not in data:
            data["groups"] = []
        return data
    except (OSError, json.JSONDecodeError) as exc:
        log.error("group_infiltration_load_error", error=str(exc))
        return {"updated_at": _now_iso(), "groups": []}


def _save(data: dict[str, Any]) -> None:
    data["updated_at"] = _now_iso()
    _VAULT_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _compute_warmup_days(joined_at: str | None) -> int:
    """Return number of full days since joined_at (capped at _WARMUP_TARGET)."""
    if not joined_at:
        return 0
    try:
        dt = datetime.fromisoformat(joined_at.replace("Z", "+00:00"))
        delta = datetime.now(timezone.utc) - dt
        return min(int(delta.total_seconds() // 86400), _WARMUP_TARGET)
    except (ValueError, TypeError):
        return 0


def _enrich(g: dict[str, Any]) -> dict[str, Any]:
    """Add computed fields to a group record before returning to client."""
    g = dict(g)
    g["warmup_days"] = _compute_warmup_days(g.get("joined_at"))
    # in_search is True if explicitly set OR warmup is complete
    if not g.get("in_search", False):
        g["in_search"] = g["warmup_days"] >= _WARMUP_TARGET
    return g


# ── Schemas ────────────────────────────────────────────────────────────────────

class GroupRecord(BaseModel):
    id: str
    name_he: str
    is_private: bool = False
    in_search: bool = False
    joined_at: str | None = None
    warmup_days: int = 0
    telegram_link: str | None = None
    notes: str | None = None


class GroupListResponse(BaseModel):
    groups: list[GroupRecord]
    total: int
    in_search_count: int
    warming_count: int
    updated_at: str


class CreateGroupRequest(BaseModel):
    name_he: str = Field(..., min_length=1, max_length=120, description="שם הקבוצה בעברית")
    group_id: str = Field(..., min_length=1, description="מזהה ייחודי (Telegram ID או שם)")
    is_private: bool = False
    telegram_link: str | None = Field(None, description="קישור הזמנה (t.me/...)")
    notes: str | None = None
    create_on_telegram: bool = Field(
        False,
        description="אם True — ינסה ליצור קבוצה חדשה ב-Telegram דרך הסשן הראשון הזמין",
    )


class ForceSearchResponse(BaseModel):
    detail: str
    group_id: str
    in_search: bool


# ── Routes ─────────────────────────────────────────────────────────────────────

@router.get(
    "/group-infiltration",
    response_model=GroupListResponse,
    summary="רשימת קבוצות — מפעל חדירה לחיפוש",
)
async def list_groups() -> GroupListResponse:
    data = _load()
    groups = [_enrich(g) for g in data.get("groups", [])]
    in_search_count = sum(1 for g in groups if g.get("in_search"))
    warming_count = sum(
        1 for g in groups
        if not g.get("in_search") and g.get("warmup_days", 0) < _WARMUP_TARGET
    )
    return GroupListResponse(
        groups=[GroupRecord(**g) for g in groups],
        total=len(groups),
        in_search_count=in_search_count,
        warming_count=warming_count,
        updated_at=data.get("updated_at", _now_iso()),
    )


@router.post(
    "/group-infiltration",
    response_model=GroupRecord,
    status_code=status.HTTP_201_CREATED,
    summary="הוסף קבוצה לרשימת המעקב",
)
async def create_group(req: CreateGroupRequest) -> GroupRecord:
    data = _load()
    groups: list[dict[str, Any]] = data.get("groups", [])

    # Prevent duplicates
    if any(g["id"] == req.group_id for g in groups):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"קבוצה עם ID '{req.group_id}' כבר קיימת ברשימה",
        )

    telegram_link = req.telegram_link
    created_on_telegram = False

    if req.create_on_telegram:
        # Attempt to create the group via Telethon using the first available session
        try:
            telegram_link = await _create_telegram_group(req.name_he)
            created_on_telegram = True
            log.info("group_infiltration_telegram_created", name=req.name_he, link=telegram_link)
        except Exception as exc:
            log.warning("group_infiltration_telegram_create_failed", error=str(exc))
            # Continue without Telegram — add manually

    new_group: dict[str, Any] = {
        "id": req.group_id,
        "name_he": req.name_he,
        "is_private": req.is_private,
        "in_search": False,
        "joined_at": _now_iso(),
        "telegram_link": telegram_link,
        "notes": req.notes,
        "created_on_telegram": created_on_telegram,
    }
    groups.append(new_group)
    data["groups"] = groups
    _save(data)

    log.info("group_infiltration_added", id=req.group_id, name=req.name_he)
    return GroupRecord(**_enrich(new_group))


@router.post(
    "/group-infiltration/{group_id}/force-search",
    response_model=ForceSearchResponse,
    summary="כפה חיפוש — סמן קבוצה כ-in_search",
)
async def force_search(group_id: str) -> ForceSearchResponse:
    data = _load()
    groups: list[dict[str, Any]] = data.get("groups", [])

    target = next((g for g in groups if g["id"] == group_id), None)
    if target is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"קבוצה '{group_id}' לא נמצאה",
        )

    target["in_search"] = True
    target["force_searched_at"] = _now_iso()
    data["groups"] = groups
    _save(data)

    log.info("group_infiltration_force_search", group_id=group_id)
    return ForceSearchResponse(
        detail=f"קבוצה '{target.get('name_he', group_id)}' סומנה כ-in_search ✓",
        group_id=group_id,
        in_search=True,
    )


@router.delete(
    "/group-infiltration/{group_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="הסר קבוצה מרשימת המעקב",
)
async def delete_group(group_id: str) -> None:
    data = _load()
    groups: list[dict[str, Any]] = data.get("groups", [])
    new_groups = [g for g in groups if g["id"] != group_id]
    if len(new_groups) == len(groups):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"קבוצה '{group_id}' לא נמצאה",
        )
    data["groups"] = new_groups
    _save(data)
    log.info("group_infiltration_deleted", group_id=group_id)


# ── Telegram group creation helper ────────────────────────────────────────────

async def _create_telegram_group(name: str) -> str | None:
    """
    Try to create a new Telegram group using the first available Telethon session.
    Returns the invite link or None on failure.
    """
    sessions_dir = _REPO_ROOT / "vault" / "sessions"
    if not sessions_dir.is_dir():
        raise RuntimeError("vault/sessions לא קיים")

    session_files = sorted(sessions_dir.glob("*.json"))
    if not session_files:
        raise RuntimeError("אין סשנים זמינים ב-vault/sessions")

    # Find a session with a valid .session file
    session_path: Path | None = None
    for sf in session_files:
        try:
            meta = json.loads(sf.read_text(encoding="utf-8"))
            sp = meta.get("session_path") or str(sf.with_suffix(""))
            if Path(sp).exists() or Path(sp + ".session").exists():
                session_path = Path(sp)
                break
        except Exception:
            continue

    if session_path is None:
        raise RuntimeError("לא נמצא קובץ .session תקין")

    try:
        from telethon import TelegramClient  # type: ignore[import-untyped]
        from telethon.tl.functions.messages import CreateChatRequest  # type: ignore[import-untyped]
        from telethon.tl.functions.messages import ExportChatInviteRequest  # type: ignore[import-untyped]

        api_id = int(os.environ.get("TELEGRAM_API_ID", "0"))
        api_hash = os.environ.get("TELEGRAM_API_HASH", "")
        if not api_id or not api_hash:
            raise RuntimeError("TELEGRAM_API_ID / TELEGRAM_API_HASH לא מוגדרים ב-.env")

        client = TelegramClient(str(session_path), api_id, api_hash)
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            raise RuntimeError("הסשן לא מאושר")

        result = await client(CreateChatRequest(users=[], title=name))
        chat = result.chats[0]
        invite = await client(ExportChatInviteRequest(peer=chat))
        await client.disconnect()
        return getattr(invite, "link", None)
    except ImportError:
        raise RuntimeError("telethon לא מותקן")
