"""
telegram.group_message_purge — מחיקת המונית של הודעות בקבוצות (יעדים + אופציונלי כל הקבוצות המנוהלות)

סדר ברירת מחדל (חשוב):
  1) לכל יעד ב־targets — עוברים על **כל** הסשנים (מחיקה ממוקדת קודם).
  2) רק אחר כך, אם purge_all_managed_groups=True — סריקת דיאלוגים מנוהלים.

שימושים: ניקוי ספאם/תוכן פוגעני אחרי חשיפת סשנים, או ניקוי יעד ממוקד.

פרמטרים
--------
targets                  רשימת @username, או קישורי https://t.me/...
purge_all_managed_groups אם True — אחרי היעדים, גם מוחק בכל דיאלוג שבו הסשן יוצר או אדמין
max_messages_per_chat    תקרה להודעות למחיקה לכל צ׳אט (ברירת מחדל 20000)
only_own_messages        אם True — רק הודעות שנשלחו מהסשן הנוכחי
lockdown_owned_after     אם True — אחרי ניקוי, נעילת הרשאות לבעלים (כמו owner_groups_lockdown)
session_stems / max_sessions
dry_run / skip_notify
db_locked_retries        מספר ניסיונות כש־SQLite session נעול (ברירת מחדל 5)
"""

from __future__ import annotations

import asyncio
import re
from collections import defaultdict
from typing import Any

import structlog

from nexus.services.session_vault import discover_all_meta_json_files
from nexus.worker.services.tg_session import async_telegram_client
from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

_TME_RE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/([a-zA-Z][a-zA-Z0-9_]{3,})",
    re.IGNORECASE,
)


def _parse_targets(raw: list[str] | None) -> list[str]:
    if not raw:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for line in raw:
        s = str(line).strip()
        if not s:
            continue
        m = _TME_RE.search(s)
        if m:
            un = m.group(1).lower()
        elif s.startswith("@"):
            un = s[1:].strip().lower()
        else:
            un = s[1:].strip().lower() if s.startswith("+") else s.lower()
        if not re.match(r"^[a-zA-Z0-9_]+$", un):
            continue
        if un in {"joinchat", "addstickers", "share", "socks", "proxy"}:
            continue
        if un not in seen:
            seen.add(un)
            out.append(un)
    return out


def _can_moderate(entity: Any) -> bool:
    from telethon.tl.types import Channel, Chat  # type: ignore

    if isinstance(entity, Channel):
        return bool(entity.creator or entity.admin_rights)
    if isinstance(entity, Chat):
        return bool(entity.creator)
    return False


async def _purge_entity(
    client: Any,
    entity: Any,
    *,
    max_messages: int,
    only_own_messages: bool,
    me_id: int | None,
) -> tuple[int, list[str]]:
    from telethon.tl.types import Channel, Chat  # type: ignore

    if not isinstance(entity, (Channel, Chat)):
        return 0, ["skip: not a group/channel"]

    deleted = 0
    errs: list[str] = []
    buffer: list[int] = []
    async for msg in client.iter_messages(entity, limit=max_messages):
        if not msg or not msg.id:
            continue
        if only_own_messages and me_id is not None:
            sid = getattr(msg, "sender_id", None)
            if sid is not None and sid != me_id:
                continue
        buffer.append(msg.id)
        if len(buffer) >= 100:
            d, e = await _delete_batch(client, entity, buffer)
            deleted += d
            errs.extend(e)
            buffer = []
            await asyncio.sleep(0.25)
    if buffer:
        d, e = await _delete_batch(client, entity, buffer)
        deleted += d
        errs.extend(e)
    return deleted, errs


async def _delete_batch(client: Any, entity: Any, batch: list[int]) -> tuple[int, list[str]]:
    if not batch:
        return 0, []
    try:
        await client.delete_messages(entity, batch)
        return len(batch), []
    except Exception as exc:
        return 0, [f"delete_messages:{exc!s}"]


def _is_db_locked(exc: BaseException) -> bool:
    return "database is locked" in str(exc).lower()


@registry.register("telegram.group_message_purge")
async def group_message_purge(parameters: dict[str, Any]) -> dict[str, Any]:
    from telethon.tl.types import Channel, Chat  # type: ignore

    dry_run = bool(parameters.get("dry_run"))
    skip_notify = bool(parameters.get("skip_notify"))
    purge_all = bool(parameters.get("purge_all_managed_groups"))
    lockdown_after = bool(parameters.get("lockdown_owned_after", True))
    only_own = bool(parameters.get("only_own_messages"))
    max_per_chat = int(parameters.get("max_messages_per_chat") or 20000)
    if max_per_chat < 1:
        max_per_chat = 20000

    try:
        db_retries = int(parameters.get("db_locked_retries") or 5)
    except (TypeError, ValueError):
        db_retries = 5
    db_retries = max(1, min(db_retries, 15))

    raw_targets = parameters.get("targets")
    if isinstance(raw_targets, str):
        raw_targets = [raw_targets]
    targets = _parse_targets(list(raw_targets or []))

    if not targets and not purge_all:
        return {
            "status": "failed",
            "error": "Provide targets and/or purge_all_managed_groups=true",
        }

    stem_filter = parameters.get("session_stems")
    if isinstance(stem_filter, str):
        stem_filter = [stem_filter]
    allow_stems: set[str] | None = (
        {str(s).strip() for s in stem_filter if str(s).strip()}
        if isinstance(stem_filter, list)
        else None
    )

    meta_paths = discover_all_meta_json_files()
    if allow_stems is not None:
        meta_paths = [p for p in meta_paths if p.stem in allow_stems]

    raw_max = parameters.get("max_sessions")
    if raw_max is not None:
        try:
            cap = int(raw_max)
            if cap > 0:
                meta_paths = meta_paths[:cap]
        except (TypeError, ValueError):
            pass

    from nexus.worker.tasks.owner_groups_lockdown import (
        _lockdown_basic_chat,
        _lockdown_megagroup_or_channel,
    )

    report: list[dict[str, Any]] = []
    session_errors: list[dict[str, Any]] = []
    # peer_ids שכבר טופלו בפאזת יעדים (לפי סשן) — כדי לא לשכפל עבודה ב־purge_all
    phase1_peers_by_session: dict[str, set[int]] = defaultdict(set)
    targets_deleted_totals: dict[str, int] = defaultdict(int)

    async def _run_purge_on(
        client: Any,
        stem: str,
        entity: Any,
        label: str,
        *,
        processed_peers: set[int],
        me_id: int | None,
    ) -> None:
        from telethon import utils  # type: ignore

        pid = utils.get_peer_id(entity)
        if pid in processed_peers:
            return
        processed_peers.add(pid)
        title = getattr(entity, "title", None) or label
        if dry_run:
            report.append(
                {
                    "session": stem,
                    "peer_id": pid,
                    "title": str(title),
                    "dry_run": True,
                    "label": label,
                },
            )
            return
        deleted, errs = await _purge_entity(
            client,
            entity,
            max_messages=max_per_chat,
            only_own_messages=only_own,
            me_id=me_id,
        )
        entry: dict[str, Any] = {
            "session": stem,
            "peer_id": pid,
            "title": str(title),
            "label": label,
            "messages_deleted": deleted,
            "errors": errs,
        }
        if label.startswith("target:"):
            un = label.split(":", 1)[1]
            targets_deleted_totals[un] += deleted
            log.info(
                "group_message_purge_target_session",
                target=un,
                session=stem,
                messages_deleted=deleted,
                title=str(title)[:80],
            )
        if lockdown_after and isinstance(entity, Channel) and getattr(entity, "creator", None):
            if entity.megagroup or entity.broadcast:
                lk = await _lockdown_megagroup_or_channel(client, entity, dry_run=False)
                entry["lockdown_steps"] = lk.get("steps", [])
                entry["errors"] = entry["errors"] + lk.get("errors", [])
        elif lockdown_after and isinstance(entity, Chat) and getattr(entity, "creator", None):
            lk = await _lockdown_basic_chat(client, entity, dry_run=False)
            entry["lockdown_steps"] = lk.get("steps", [])
            entry["errors"] = entry["errors"] + lk.get("errors", [])
        report.append(entry)

    # ── פאזה 1: לכל יעד — כל הסשנים (קודם הקבוצות שציינת) ───────────────────
    for un in targets:
        for meta_json in meta_paths:
            stem = meta_json.stem
            session_base = str(meta_json.with_suffix(""))
            params: dict[str, Any] = {
                "session_stem": stem,
                **{k: v for k, v in parameters.items() if k in ("__secrets__", "string_session")},
            }
            try:
                for attempt in range(db_retries):
                    try:
                        async with async_telegram_client(session_base, params) as client:
                            if not await client.is_user_authorized():
                                session_errors.append(
                                    {"session": stem, "error": "not_authorized", "phase": "targets"},
                                )
                                break
                            me = await client.get_me()
                            me_id = getattr(me, "id", None) if me else None
                            processed: set[int] = phase1_peers_by_session[stem]
                            try:
                                ent = await client.get_entity(un)
                                await _run_purge_on(
                                    client,
                                    stem,
                                    ent,
                                    f"target:{un}",
                                    processed_peers=processed,
                                    me_id=me_id,
                                )
                            except Exception as exc:
                                report.append(
                                    {
                                        "session": stem,
                                        "target": un,
                                        "error": str(exc),
                                        "label": "resolve_target",
                                    },
                                )
                            await asyncio.sleep(0.12)
                        break
                    except Exception as exc:
                        if _is_db_locked(exc) and attempt < db_retries - 1:
                            log.info(
                                "group_message_purge_db_locked_retry",
                                session=stem,
                                target=un,
                                attempt=attempt + 1,
                            )
                            await asyncio.sleep(0.5 * (2**attempt))
                            continue
                        log.warning(
                            "group_message_purge_session_failed",
                            session=stem,
                            phase="targets",
                            error=str(exc),
                        )
                        session_errors.append(
                            {"session": stem, "error": str(exc), "phase": "targets"},
                        )
                        break
            except Exception as exc:
                log.warning("group_message_purge_session_failed", session=stem, error=str(exc))
                session_errors.append({"session": stem, "error": str(exc), "phase": "targets"})

    # ── פאזה 2: דיאלוגים מנוהלים (אופציונלי) ─────────────────────────────────
    if purge_all:
        for meta_json in meta_paths:
            stem = meta_json.stem
            session_base = str(meta_json.with_suffix(""))
            params: dict[str, Any] = {
                "session_stem": stem,
                **{k: v for k, v in parameters.items() if k in ("__secrets__", "string_session")},
            }
            try:
                for attempt in range(db_retries):
                    try:
                        async with async_telegram_client(session_base, params) as client:
                            if not await client.is_user_authorized():
                                session_errors.append(
                                    {"session": stem, "error": "not_authorized", "phase": "managed"},
                                )
                                break
                            me = await client.get_me()
                            me_id = getattr(me, "id", None) if me else None
                            processed = phase1_peers_by_session[stem].copy()
                            async for dialog in client.iter_dialogs():
                                ent = dialog.entity
                                if not isinstance(ent, (Channel, Chat)):
                                    continue
                                if not _can_moderate(ent):
                                    continue
                                await _run_purge_on(
                                    client,
                                    stem,
                                    ent,
                                    "managed_dialog",
                                    processed_peers=processed,
                                    me_id=me_id,
                                )
                            await asyncio.sleep(0.12)
                        break
                    except Exception as exc:
                        if _is_db_locked(exc) and attempt < db_retries - 1:
                            log.info(
                                "group_message_purge_db_locked_retry",
                                session=stem,
                                phase="managed",
                                attempt=attempt + 1,
                            )
                            await asyncio.sleep(0.5 * (2**attempt))
                            continue
                        log.warning(
                            "group_message_purge_session_failed",
                            session=stem,
                            phase="managed",
                            error=str(exc),
                        )
                        session_errors.append(
                            {"session": stem, "error": str(exc), "phase": "managed"},
                        )
                        break
            except Exception as exc:
                log.warning("group_message_purge_session_failed", session=stem, error=str(exc))
                session_errors.append({"session": stem, "error": str(exc), "phase": "managed"})

    targets_summary = {k: int(targets_deleted_totals[k]) for k in targets}

    out: dict[str, Any] = {
        "status": "ok",
        "targets_parsed": targets,
        "targets_deleted_totals": targets_summary,
        "purge_all_managed_groups": purge_all,
        "sessions_considered": len(meta_paths),
        "max_sessions_applied": raw_max,
        "phase_order": "targets_first_all_sessions_then_managed",
        "operations": report,
        "session_errors": session_errors,
    }

    if not skip_notify:
        try:
            from nexus.shared.notifications.providers.telegram import TelegramProvider

            _MD = re.compile(r"([_\*\[\]\(\)~`>#+\-=|{}.!\\])")

            def _esc(t: str) -> str:
                return _MD.sub(r"\\\1", str(t))

            prov = TelegramProvider()
            lines = [
                _esc("🧹 *Group message purge*"),
                _esc(f"targets_deleted_totals={targets_summary}"),
                _esc(f"purge_all={purge_all}"),
                _esc(f"פעולות: {len(report)}"),
            ]
            body = "\n".join(lines)[:3900]
            await prov.send_message(body)
        except Exception as exc:
            out["notify_error"] = str(exc)

    return out
