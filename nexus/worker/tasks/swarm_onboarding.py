"""
swarm.onboarding_mass_join — Mass join + triage for Telethon sessions (isolated from
the community_factory chat loop).

Uses telefix.db ``sessions`` rows (with optional ``session_stem`` / ``is_active`` /
``is_banned`` columns added on first run) plus the vault session files under
``vault/sessions``. Dead sessions are flagged in SQLite and in Redis
``nexus:swarm:factory:banned`` so ``swarm.community_factory`` skips them.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from nexus.shared.db_util import get_telefix_db
from nexus.worker.services.tg_session import (
    async_telegram_client,
    flood_wait_seconds,
    resolve_telethon_creds,
)
from nexus.worker.task_registry import registry
from nexus.worker.tasks.swarm import (
    _discover_session_bases,
    _is_session_banned,
    _mark_banned,
    _resolve_sessions_dir,
    _set_cooldown,
)

log = structlog.get_logger(__name__)

_JOIN_SEM = asyncio.Semaphore(10)


def _ensure_session_triage_columns(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(sessions)").fetchall()
    colnames = {str(r[1]) for r in rows}
    for stmt in (
        "ALTER TABLE sessions ADD COLUMN session_stem TEXT",
        "ALTER TABLE sessions ADD COLUMN is_active INTEGER DEFAULT 1",
        "ALTER TABLE sessions ADD COLUMN is_banned INTEGER DEFAULT 0",
    ):
        col = stmt.split("COLUMN ")[1].split(" ")[0].lower()
        if col not in colnames:
            try:
                conn.execute(stmt)
                conn.commit()
            except sqlite3.OperationalError:
                pass


def _read_meta_phone(session_base: str) -> str | None:
    meta = Path(session_base).with_suffix(".json")
    if not meta.is_file():
        return None
    try:
        data = json.loads(meta.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    raw = data.get("phone")
    if raw is None:
        return None
    s = str(raw).strip()
    return s or None


def _db_allows_session(conn: sqlite3.Connection, session_base: str) -> bool:
    stem = Path(session_base).name
    phone = _read_meta_phone(session_base)
    try:
        _ensure_session_triage_columns(conn)
        row = conn.execute(
            """
            SELECT is_active, is_banned FROM sessions
            WHERE (session_stem IS NOT NULL AND session_stem = ?)
               OR (? IS NOT NULL AND phone IS NOT NULL AND phone = ?)
            ORDER BY id DESC LIMIT 1
            """,
            (stem, phone, phone),
        ).fetchone()
    except sqlite3.Error as exc:
        log.warning("swarm_onboarding_db_query_failed", error=str(exc))
        return True
    if row is None:
        return True
    ia, ib = row[0], row[1]
    if ib is not None and int(ib) == 1:
        return False
    if ia is not None and int(ia) == 0:
        return False
    return True


def _persist_session_flags(
    conn: sqlite3.Connection,
    session_base: str,
    *,
    is_active: bool,
    is_banned: bool,
) -> None:
    stem = Path(session_base).name
    phone = _read_meta_phone(session_base)
    now = datetime.now(timezone.utc).isoformat()
    _ensure_session_triage_columns(conn)
    try:
        row = conn.execute(
            """
            SELECT id FROM sessions
            WHERE (session_stem IS NOT NULL AND session_stem = ?)
               OR (? IS NOT NULL AND phone IS NOT NULL AND phone = ?)
            ORDER BY id DESC LIMIT 1
            """,
            (stem, phone, phone),
        ).fetchone()
        if row:
            conn.execute(
                """
                UPDATE sessions SET
                    is_active = ?, is_banned = ?, last_active = ?,
                    session_stem = COALESCE(session_stem, ?)
                WHERE id = ?
                """,
                (1 if is_active else 0, 1 if is_banned else 0, now, stem, row[0]),
            )
        else:
            try:
                conn.execute(
                    """
                    INSERT INTO sessions (
                        phone, machine_id, status, last_active,
                        session_stem, is_active, is_banned
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        phone,
                        None,
                        "banned" if is_banned else "disabled",
                        now,
                        stem,
                        1 if is_active else 0,
                        1 if is_banned else 0,
                    ),
                )
            except sqlite3.IntegrityError:
                conn.execute(
                    """
                    UPDATE sessions SET
                        is_active = ?, is_banned = ?, last_active = ?,
                        session_stem = COALESCE(session_stem, ?), status = ?
                    WHERE phone IS NOT NULL AND phone = ?
                    """,
                    (
                        1 if is_active else 0,
                        1 if is_banned else 0,
                        now,
                        stem,
                        "banned" if is_banned else "disabled",
                        phone,
                    ),
                )
        conn.commit()
    except sqlite3.Error as exc:
        log.error("swarm_onboarding_persist_failed", stem=stem, error=str(exc))


def _private_invite_hash(link: str) -> str | None:
    """Hash for ``t.me/+…`` / ``joinchat/…`` / leading ``+`` only (not public @usernames)."""
    s = (link or "").strip()
    if "/+" in s:
        h = s.split("/+")[-1].split("?")[0].strip()
        return h or None
    low = s.lower()
    if "joinchat/" in low:
        h = s.split("joinchat/")[-1].split("?")[0].strip()
        return h or None
    if s.startswith("+"):
        h = s[1:].strip()
        return h or None
    return None


def _public_username_from_link(link: str) -> str | None:
    s = link.strip()
    for prefix in (
        "https://t.me/",
        "http://t.me/",
        "https://telegram.me/",
        "http://telegram.me/",
    ):
        if len(s) >= len(prefix) and s[: len(prefix)].lower() == prefix.lower():
            rest = s[len(prefix) :].split("/")[0].split("?")[0].strip()
            if not rest or rest.startswith("+"):
                return None
            return rest.lstrip("@") or None
    if s.startswith("@"):
        tail = s[1:].split("/")[0].split("?")[0].strip()
        if tail.startswith("+") or not tail:
            return None
        return tail
    if s and "/" not in s and not s.lower().startswith("http"):
        return s.lstrip("@") or None
    return None


async def _join_target(client: Any, group_link: str) -> None:
    from telethon import errors  # type: ignore[import-untyped]
    from telethon.tl.functions.channels import JoinChannelRequest  # type: ignore
    from telethon.tl.functions.messages import ImportChatInviteRequest  # type: ignore
    from telethon.tl.types import Channel  # type: ignore

    t = (group_link or "").strip()
    if not t:
        raise ValueError("empty group_link")

    priv = _private_invite_hash(t)
    if priv:
        try:
            await client(ImportChatInviteRequest(priv))
            return
        except errors.UserAlreadyParticipantError:
            return

    uname = _public_username_from_link(t)
    if uname:
        ent = await client.get_entity(uname)
        if isinstance(ent, Channel):
            try:
                await client(JoinChannelRequest(await client.get_input_entity(ent)))
            except errors.UserAlreadyParticipantError:
                pass
        return

    ent = await client.get_entity(t)
    if isinstance(ent, Channel) and (
        getattr(ent, "megagroup", False) or not getattr(ent, "broadcast", False)
    ):
        try:
            await client(JoinChannelRequest(await client.get_input_entity(ent)))
        except errors.UserAlreadyParticipantError:
            pass


async def _one_session_join(
    *,
    session_base: str,
    parameters: dict[str, Any],
    group_link: str,
    redis: Any,
) -> dict[str, Any]:
    from telethon.errors import (  # type: ignore[import-untyped]
        AuthKeyUnregisteredError,
        FloodWaitError,
        PhoneNumberBannedError,
        UserDeactivatedBanError,
        UserDeactivatedError,
    )

    stem = Path(session_base).name
    async with _JOIN_SEM:
        try:
            api_id, api_hash = resolve_telethon_creds(session_base, parameters)
            if not api_id or not api_hash:
                return {"session": stem, "ok": False, "error": "missing api_id/api_hash"}

            async with async_telegram_client(session_base, parameters) as client:
                if not await client.is_user_authorized():
                    conn = get_telefix_db()
                    _persist_session_flags(conn, session_base, is_active=False, is_banned=True)
                    await _mark_banned(redis, session_base)
                    return {"session": stem, "ok": False, "error": "not_authorized"}

                await _join_target(client, group_link)

            return {"session": stem, "ok": True}

        except FloodWaitError as exc:
            sec = flood_wait_seconds(exc)
            await _set_cooldown(redis, session_base, sec)
            log.warning("swarm_onboarding_flood_wait", session=stem, seconds=sec)
            return {"session": stem, "ok": False, "error": f"flood_wait:{sec}"}

        except (UserDeactivatedError, UserDeactivatedBanError, PhoneNumberBannedError) as exc:
            conn = get_telefix_db()
            _persist_session_flags(conn, session_base, is_active=False, is_banned=True)
            await _mark_banned(redis, session_base)
            log.warning("swarm_onboarding_user_banned_or_deactivated", session=stem, kind=type(exc).__name__)
            return {"session": stem, "ok": False, "error": type(exc).__name__}

        except AuthKeyUnregisteredError:
            conn = get_telefix_db()
            _persist_session_flags(conn, session_base, is_active=False, is_banned=False)
            await _mark_banned(redis, session_base)
            log.warning("swarm_onboarding_auth_key_dead", session=stem)
            return {"session": stem, "ok": False, "error": "auth_key_unregistered"}

        except Exception as exc:
            log.warning("swarm_onboarding_join_failed", session=stem, error=str(exc))
            return {"session": stem, "ok": False, "error": str(exc)[:200]}


@registry.register("swarm.onboarding_mass_join")
async def swarm_onboarding_mass_join(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Join ``target_group`` (username, t.me link, or invite) with all active vault
    sessions, triaging Telethon errors into SQLite + Redis.
    """
    redis = parameters.get("__redis__")
    group_link = (
        str(parameters.get("target_group") or parameters.get("group_link") or "").strip()
        or str(parameters.get("invite_link") or "").strip()
    )
    if not group_link:
        return {"status": "failed", "error": "target_group / group_link / invite_link required"}

    sessions_dir = _resolve_sessions_dir(str(parameters.get("sessions_dir", "") or ""))
    conn = get_telefix_db()
    bases_all = _discover_session_bases(sessions_dir)
    bases: list[str] = []
    for b in bases_all:
        if redis and await _is_session_banned(redis, b):
            continue
        if not _db_allows_session(conn, b):
            continue
        bases.append(b)

    if not bases:
        return {"status": "failed", "error": "no active sessions after DB/redis filter"}

    tasks = [
        _one_session_join(
            session_base=b,
            parameters=parameters,
            group_link=group_link,
            redis=redis,
        )
        for b in bases
    ]
    rows = await asyncio.gather(*tasks)
    ok_n = sum(1 for r in rows if r.get("ok"))
    return {
        "status": "completed",
        "target": group_link,
        "sessions_attempted": len(bases),
        "sessions_joined": ok_n,
        "results": rows,
    }
