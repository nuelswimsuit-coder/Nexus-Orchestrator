"""
telegram.owner_groups_lockdown — נעילת קבוצות שהסשן הוא יוצר/בעלים שלהן

עבור כל סשן Telethon פעיל ב־vault: מאתר דיאלוגים שבהם המשתמש הוא creator,
ומחיל:
  • ברירת מחדל: ללא שליחת תוכן (כולל מדיה, סטיקרים וכו׳) לחברים
  • תוכן מוגן (ללא שמירה/העברה במובן Telegram — ToggleNoForwards)
  • הסתרת רשימת משתתפים (מגה־קבוצות/ערוצים)
  • בעלים: מצב אנונימי כאדמין (כשנתמך)
  • מחיקת הודעות: ברירת מחדל **רק מהיום** (purge_all_messages=false); מחיקה מלאה עד max_messages_per_chat אם purge_all_messages=true
  • דוח והתקדמות לבוט (עברית) — progress_notify / notify_chat_id

פרמטרים (אופציונליים)
----------------------
session_stems          רשימת stems לסינון; ריק = כל הסשנים ב־vault
dry_run                אם True — רק דוח ללא שינוי בפועל
skip_notify            אם True — לא לשלוח דוח בוט
progress_notify        אם True (ברירת מחדל) — הודעת בוט אחרי כל קבוצה + תחילת סשן
notify_chat_id / notify_bot_token — יעד וטוקן (ראה TelegramProvider.from_task_parameters)
timezone               ברירת מחדל Asia/Jerusalem (חישוב ״היום״ כש־purge_all_messages=false)
max_sessions           מספר מקסימלי של קבצי סשן לעיבוד (ברירת מחדל: כולם)
purge_all_messages     אם True — מחיקת היסטוריה עד max_messages_per_chat; אם False (ברירת מחדל) — רק הודעות מהיום
include_admin_groups   אם True (ברירת מחדל) — גם קבוצות שבהן אתה אדמין עם delete_messages (מחיקה בלבד; נעילת הגדרות רק לבעלים)
auto_create_meta_json  אם True (ברירת מחדל) — יוצר ליד כל *.session קובץ *.json מ־TELEFIX_API_ID / TELEFIX_API_HASH (אם חסר)
write_audit_csv        אם True (ברירת מחדל) — כותב דוח CSV בשורש הפרויקט בתיקייה «דוחות -סריקות סשנים + בעלי/אדמיני קבוצות»
audit_csv_dir          נתיב מותאם לדוח CSV (אופציונלי; יחסי לפרויקט או מוחלט)
max_messages_per_chat  תקרה למחיקה מלאה (ברירת מחדל 20000)
only_own_messages      אם True — במצב מחיקה מלאה רק הודעות של הסשן (בדרך כלל False לבעלים)
"""

from __future__ import annotations

import asyncio
import csv
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import structlog

from nexus.services.session_vault import (
    discover_all_meta_json_files,
    ensure_sidecar_json_for_vault,
    repo_root,
)
from nexus.worker.services.tg_session import async_telegram_client, global_telethon_credentials
from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

# דוח CSV — תואם ל־nexus_group_audit.csv (עמודות)
_SESSION_AUDIT_REPORT_DIR = "דוחות -סריקות סשנים + בעלי/אדמיני קבוצות"
_AUDIT_CSV_COLUMNS = [
    "Session_Name",
    "Original_Session_Path",
    "Final_Session_Path",
    "Group_Name",
    "Group_ID",
    "Entity_Type",
    "Member_Count",
    "Premium_Count",
    "Boost_Premiums",
    "Role",
    "JSON_File_Path",
    "TData_Folder_Path",
    "Archive_File_Path",
]

# MarkdownV2 escape — תואם ל־TelegramProvider
_MD_ESCAPE_RE = re.compile(r"([_\*\[\]\(\)~`>#+\-=|{}.!\\])")


def _esc(text: str) -> str:
    return _MD_ESCAPE_RE.sub(r"\\\1", str(text))


def _esc_lines(lines: list[str]) -> str:
    return "\n".join(_esc(x) for x in lines)[:3900]


def _chunks(ids: list[int], size: int) -> list[list[int]]:
    return [ids[i : i + size] for i in range(0, len(ids), size)]


def _entity_member_count(entity: Any) -> int:
    try:
        c = getattr(entity, "participants_count", None)
        if c is not None:
            return int(c)
    except (TypeError, ValueError):
        pass
    return 0


def _resolve_audit_report_dir(parameters: dict[str, Any]) -> Path:
    raw = parameters.get("audit_csv_dir")
    root = repo_root()
    if raw:
        p = Path(str(raw)).expanduser()
        if p.is_absolute():
            return p.resolve()
        return (root / p).resolve()
    return (root / _SESSION_AUDIT_REPORT_DIR).resolve()


def _write_session_audit_csv(
    rows: list[dict[str, Any]],
    report_dir: Path,
) -> Path | None:
    if not rows:
        return None
    report_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = report_dir / f"session_group_audit_{ts}.csv"
    with out.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=_AUDIT_CSV_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)
    return out


async def _collect_today_message_ids(
    client: Any,
    entity: Any,
    tz: ZoneInfo,
) -> list[int]:
    """הודעות שהזמן המקומי שלהן הוא ״היום״ (לפי tz)."""
    now_local = datetime.now(tz)
    today = now_local.date()
    out: list[int] = []
    async for msg in client.iter_messages(entity):
        if not msg or msg.date is None:
            continue
        msg_dt = msg.date
        if msg_dt.tzinfo is None:
            msg_dt = msg_dt.replace(tzinfo=timezone.utc)
        local = msg_dt.astimezone(tz)
        if local.date() < today:
            break
        if local.date() == today:
            out.append(msg.id)
    return out


async def _delete_ids(client: Any, entity: Any, ids: list[int]) -> tuple[int, list[str]]:
    deleted = 0
    errs: list[str] = []
    for batch in _chunks(ids, 100):
        try:
            await client.delete_messages(entity, batch)
            deleted += len(batch)
        except Exception as exc:
            errs.append(f"delete_messages:{exc!s}")
    return deleted, errs


async def _lockdown_megagroup_or_channel(
    client: Any,
    entity: Any,
    *,
    dry_run: bool,
) -> dict[str, Any]:
    from telethon.tl import functions  # type: ignore[import-untyped]

    steps: list[str] = []
    errors: list[str] = []

    if dry_run:
        return {"steps": ["dry_run_skip"], "errors": []}

    try:
        await client.edit_permissions(
            entity,
            user=None,
            send_messages=False,
            send_media=False,
            send_stickers=False,
            send_gifs=False,
            send_games=False,
            send_inline=False,
            embed_link_previews=False,
            send_polls=False,
            change_info=False,
            invite_users=False,
            pin_messages=False,
        )
        steps.append("default_banned_rights")
    except Exception as exc:
        errors.append(f"default_banned_rights:{exc!s}")

    try:
        await client(
            functions.channels.ToggleParticipantsHiddenRequest(
                channel=entity,
                enabled=True,
            )
        )
        steps.append("participants_hidden")
    except Exception as exc:
        errors.append(f"participants_hidden:{exc!s}")

    try:
        await client(
            functions.messages.ToggleNoForwardsRequest(
                peer=await client.get_input_entity(entity),
                enabled=True,
            )
        )
        steps.append("protected_content_noforwards")
    except Exception as exc:
        errors.append(f"toggle_noforwards:{exc!s}")

    try:
        await client.edit_admin(
            entity,
            "me",
            is_admin=True,
            anonymous=True,
        )
        steps.append("owner_anonymous_admin")
    except Exception as exc:
        errors.append(f"owner_anonymous:{exc!s}")

    try:
        from telethon.tl.types import ChannelParticipantsAdmins  # type: ignore

        me = await client.get_me()
        restricted = 0
        async for part in client.iter_participants(
            entity,
            filter=ChannelParticipantsAdmins(),
        ):
            uid = getattr(part, "id", None)
            if uid is None or me is None or uid == me.id:
                continue
            try:
                await client.edit_permissions(
                    entity,
                    user=part,
                    send_messages=False,
                    send_media=False,
                    send_stickers=False,
                    send_gifs=False,
                    send_games=False,
                    send_inline=False,
                    embed_link_previews=False,
                    send_polls=False,
                    change_info=False,
                    invite_users=False,
                    pin_messages=False,
                )
                restricted += 1
                await asyncio.sleep(0.2)
            except Exception as exc:
                errors.append(f"restrict_admin:{uid}:{exc!s}")
        if restricted:
            steps.append(f"restrict_other_admins:{restricted}")
    except Exception as exc:
        errors.append(f"iter_admins:{exc!s}")

    return {"steps": steps, "errors": errors}


async def _lockdown_basic_chat(
    client: Any,
    entity: Any,
    *,
    dry_run: bool,
) -> dict[str, Any]:
    from telethon.tl import functions, types  # type: ignore[import-untyped]

    steps: list[str] = []
    errors: list[str] = []
    if dry_run:
        return {"steps": ["dry_run_skip"], "errors": []}

    peer = await client.get_input_entity(entity)
    rights = types.ChatBannedRights(
        until_date=None,
        send_messages=True,
        send_media=True,
        send_stickers=True,
        send_gifs=True,
        send_games=True,
        send_inline=True,
        embed_links=True,
        send_polls=True,
        change_info=True,
        invite_users=True,
        pin_messages=True,
        manage_topics=True,
        send_photos=True,
        send_videos=True,
        send_roundvideos=True,
        send_audios=True,
        send_voices=True,
        send_docs=True,
        send_plain=True,
    )
    try:
        await client(
            functions.messages.EditChatDefaultBannedRightsRequest(
                peer=peer,
                banned_rights=rights,
            )
        )
        steps.append("default_banned_rights_basic_chat")
    except Exception as exc:
        errors.append(f"default_banned_basic:{exc!s}")

    try:
        await client(functions.messages.ToggleNoForwardsRequest(peer=peer, enabled=True))
        steps.append("protected_content_noforwards")
    except Exception as exc:
        errors.append(f"toggle_noforwards:{exc!s}")

    return {"steps": steps, "errors": errors}


def _entity_label(entity: Any) -> str:
    from telethon.tl.types import Channel  # type: ignore

    if isinstance(entity, Channel):
        un = getattr(entity, "username", None) or ""
        if un:
            return f"@{un}"
    return getattr(entity, "title", None) or str(getattr(entity, "id", "?"))


async def _is_group_owner(client: Any, entity: Any, me_id: int | None) -> bool:
    """
    האם הסשן הוא יוצר הבקשה. ב־iter_dialogs השדה entity.creator לעיתים לא ממולא —
    משלימים עם get_permissions ו־GetFullChannelRequest (creator_id).
    """
    from telethon.tl.functions.channels import GetFullChannelRequest  # type: ignore
    from telethon.tl.types import Channel, Chat  # type: ignore

    if isinstance(entity, Channel):
        if getattr(entity, "creator", None) is True:
            return True
        try:
            perm = await client.get_permissions(entity)
            if perm is not None and getattr(perm, "is_creator", False):
                return True
        except Exception as exc:
            log.debug("owner_groups_lockdown_perm_channel", error=str(exc))
        if me_id:
            try:
                r = await client(GetFullChannelRequest(channel=entity))
                fc = getattr(r, "full_chat", None)
                cid = int(getattr(fc, "creator_id", 0) or 0) if fc else 0
                if cid and cid == me_id:
                    return True
            except Exception as exc:
                log.debug("owner_groups_lockdown_full_channel", error=str(exc))
        return False

    if isinstance(entity, Chat):
        if getattr(entity, "creator", None) is True:
            return True
        try:
            perm = await client.get_permissions(entity)
            if perm is not None and getattr(perm, "is_creator", False):
                return True
        except Exception as exc:
            log.debug("owner_groups_lockdown_perm_chat", error=str(exc))
        return False

    return False


async def _dialog_owner_and_bulk_delete(
    client: Any,
    entity: Any,
    me_id: int | None,
) -> tuple[bool, bool]:
    """
    (is_owner, can_delete_others_messages) — נעילת הגדרות רק לבעלים;
    מחיקת הודעות (כולל של אחרים) גם לאדמין עם delete_messages.
    """
    is_owner = await _is_group_owner(client, entity, me_id)
    if is_owner:
        return True, True
    try:
        perm = await client.get_permissions(entity)
    except Exception as exc:
        log.debug("owner_groups_lockdown_get_permissions", error=str(exc))
        return False, False
    if perm is None:
        return False, False
    if getattr(perm, "is_creator", False):
        return True, True
    bulk = bool(
        getattr(perm, "is_admin", False)
        and getattr(perm, "delete_messages", False)
    )
    return False, bulk


@registry.register("telegram.owner_groups_lockdown")
async def owner_groups_lockdown(parameters: dict[str, Any]) -> dict[str, Any]:
    from telethon import utils  # type: ignore[import-untyped]
    from telethon.tl.types import Channel, Chat  # type: ignore[import-untyped]

    dry_run = bool(parameters.get("dry_run"))
    skip_notify = bool(parameters.get("skip_notify"))
    progress_notify = bool(parameters.get("progress_notify", True))
    purge_all_messages = bool(parameters.get("purge_all_messages", False))
    include_admin_groups = bool(parameters.get("include_admin_groups", True))
    only_own = bool(parameters.get("only_own_messages", False))
    try:
        max_per_chat = int(parameters.get("max_messages_per_chat") or 20000)
    except (TypeError, ValueError):
        max_per_chat = 20000
    if max_per_chat < 1:
        max_per_chat = 20000

    tz_name = str(parameters.get("timezone") or "Asia/Jerusalem").strip() or "Asia/Jerusalem"
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")

    stem_filter = parameters.get("session_stems")
    if isinstance(stem_filter, str):
        stem_filter = [stem_filter]
    allow_stems: set[str] | None = (
        {str(s).strip() for s in stem_filter if str(s).strip()}
        if isinstance(stem_filter, list)
        else None
    )

    auto_create_meta_json = bool(parameters.get("auto_create_meta_json", True))
    sidecar_created = 0
    if auto_create_meta_json:
        gid, ghash = global_telethon_credentials(parameters)
        if gid and ghash:
            sidecar_created = ensure_sidecar_json_for_vault(gid, ghash)

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

    prov_notify: Any = None
    if not skip_notify:
        from nexus.shared.notifications.providers.telegram import TelegramProvider

        prov_notify = TelegramProvider.from_task_parameters(parameters)

    notify_gave_up = False

    async def _notify_he(lines: list[str]) -> None:
        nonlocal notify_gave_up
        if skip_notify or not progress_notify or notify_gave_up:
            return
        if prov_notify is None or not prov_notify._is_configured():
            return
        try:
            await prov_notify.send_message(_esc_lines(lines))
        except Exception as exc:
            err = str(exc).lower()
            if "chat not found" in err or "chat_id is empty" in err:
                notify_gave_up = True
                log.warning(
                    "owner_groups_lockdown_notify_stopped",
                    hint="הגדר notify_chat_id למזהה מספרי (מ־@userinfobot) או /start מול הבוט; מדלגים על התראות",
                    error=str(exc),
                )
            else:
                log.warning("owner_groups_lockdown_progress_notify_failed", error=str(exc))

    report_groups: list[dict[str, Any]] = []
    session_notes: list[dict[str, Any]] = []
    seen_peer: set[int] = set()
    write_audit_csv = bool(parameters.get("write_audit_csv", True))
    audit_csv_rows: list[dict[str, Any]] = []

    await _notify_he(
        [
            "🔔 נעילת קבוצות בעלים — התחלה",
            f"סשנים בסריקה: {len(meta_paths)}",
            f"קבצי json שנוצרו ליד session: {sidecar_created}",
            f"מחיקה מלאה: {purge_all_messages}",
            f"כולל אדמין (מחיקה בלבד): {include_admin_groups}",
            f"dry_run: {dry_run}",
        ],
    )

    for meta_json in meta_paths:
        stem = meta_json.stem
        session_base = str(meta_json.with_suffix(""))
        params: dict[str, Any] = {
            "session_stem": stem,
            **{k: v for k, v in parameters.items() if k in ("__secrets__", "string_session")},
        }

        try:
            async with async_telegram_client(session_base, params) as client:
                if not await client.is_user_authorized():
                    session_notes.append(
                        {"session": stem, "skipped": True, "reason": "not_authorized"},
                    )
                    await _notify_he([f"⚠️ סשן {stem} לא מחובר — מדלג"])
                    continue

                me_for_purge = await client.get_me()
                me_id = getattr(me_for_purge, "id", None) if me_for_purge else None
                await _notify_he([f"📂 סשן {stem}: סורק דיאלוגים…"])

                n_grp_session = 0
                async for dialog in client.iter_dialogs():
                    entity = dialog.entity
                    is_owner, can_bulk_delete = await _dialog_owner_and_bulk_delete(
                        client,
                        entity,
                        me_id,
                    )
                    if not is_owner and not (include_admin_groups and can_bulk_delete):
                        continue

                    if isinstance(entity, Channel):
                        if not entity.megagroup and not entity.broadcast:
                            continue
                    elif isinstance(entity, Chat):
                        pass
                    else:
                        continue

                    peer_id = utils.get_peer_id(entity)
                    if peer_id in seen_peer:
                        continue

                    title = _entity_label(entity)
                    entity_type_str = (
                        "group"
                        if isinstance(entity, Channel) and entity.megagroup
                        else "channel"
                        if isinstance(entity, Channel)
                        else "chat"
                    )
                    member_count = _entity_member_count(entity)
                    entry: dict[str, Any] = {
                        "session": stem,
                        "peer_id": peer_id,
                        "title": title,
                        "role": "owner" if is_owner else "admin_delete",
                        "type": "megagroup"
                        if isinstance(entity, Channel) and entity.megagroup
                        else "broadcast"
                        if isinstance(entity, Channel)
                        else "chat",
                    }

                    if not dry_run:
                        if purge_all_messages:
                            from nexus.worker.tasks.group_message_purge import _purge_entity

                            del_stats, del_errs = await _purge_entity(
                                client,
                                entity,
                                max_messages=max_per_chat,
                                only_own_messages=only_own,
                                me_id=me_id,
                            )
                            entry["messages_deleted"] = del_stats
                            entry["delete_errors"] = del_errs
                            entry["today_message_ids_count"] = 0
                            entry["purge_mode"] = "full"
                        else:
                            ids = await _collect_today_message_ids(client, entity, tz)
                            entry["today_message_ids_count"] = len(ids)
                            del_stats, del_errs = await _delete_ids(client, entity, ids)
                            entry["messages_deleted"] = del_stats
                            entry["delete_errors"] = del_errs
                            entry["purge_mode"] = "today_only"
                    else:
                        if purge_all_messages:
                            entry["today_message_ids_count"] = 0
                            entry["messages_deleted"] = 0
                            entry["delete_errors"] = []
                            entry["purge_mode"] = "full_dry_run"
                        else:
                            ids = await _collect_today_message_ids(client, entity, tz)
                            entry["today_message_ids_count"] = len(ids)
                            entry["messages_deleted"] = 0
                            entry["delete_errors"] = []
                            entry["purge_mode"] = "today_only_dry_run"

                    if is_owner:
                        if isinstance(entity, Chat):
                            lock = await _lockdown_basic_chat(client, entity, dry_run=dry_run)
                        else:
                            lock = await _lockdown_megagroup_or_channel(
                                client,
                                entity,
                                dry_run=dry_run,
                            )
                        entry["steps"] = lock.get("steps", [])
                        entry["errors"] = list(lock.get("errors", [])) + list(
                            entry.get("delete_errors", []),
                        )
                    else:
                        entry["steps"] = ["lockdown_skipped_not_owner"]
                        entry["errors"] = list(entry.get("delete_errors", []))

                    if write_audit_csv:
                        final_sess = str(Path(session_base).with_suffix(".session").resolve())
                        meta_path_str = str(meta_json.resolve())
                        audit_csv_rows.append(
                            {
                                "Session_Name": stem,
                                "Original_Session_Path": final_sess,
                                "Final_Session_Path": final_sess,
                                "Group_Name": title.replace("\r", " ").replace("\n", " "),
                                "Group_ID": str(peer_id),
                                "Entity_Type": entity_type_str,
                                "Member_Count": str(member_count),
                                "Premium_Count": "0",
                                "Boost_Premiums": "0",
                                "Role": "owner" if is_owner else "admin",
                                "JSON_File_Path": meta_path_str,
                                "TData_Folder_Path": "",
                                "Archive_File_Path": "",
                            },
                        )

                    seen_peer.add(peer_id)
                    report_groups.append(entry)
                    n_grp_session += 1
                    steps_short = ", ".join(str(x) for x in entry.get("steps", [])[:5])
                    await _notify_he(
                        [
                            f"✅ קבוצה הושלמה: {title}",
                            f"סשן: {stem}",
                            f"נמחקו {entry.get('messages_deleted', 0)} הודעות (מצב: {entry.get('purge_mode', '')})",
                            f"הרשאות/נעילה: {steps_short or '—'}",
                            f"שגיאות: {len(entry.get('errors', []))}",
                        ],
                    )
                    await asyncio.sleep(0.35)

                await _notify_he([f"🏁 סשן {stem} הסתיים — {n_grp_session} קבוצות (בעלים/אדמין)"])

        except Exception as exc:
            log.warning("owner_groups_lockdown_session_failed", session=stem, error=str(exc))
            session_notes.append(
                {"session": stem, "skipped": True, "reason": str(exc)},
            )
            await _notify_he([f"❌ שגיאה בסשן {stem}: {str(exc)[:350]}"])

    audit_csv_path: Path | None = None
    if write_audit_csv and audit_csv_rows:
        try:
            audit_csv_path = _write_session_audit_csv(
                audit_csv_rows,
                _resolve_audit_report_dir(parameters),
            )
        except Exception as exc:
            log.warning("owner_groups_lockdown_audit_csv_failed", error=str(exc))
    if audit_csv_path:
        await _notify_he(
            [
                "📄 דוח CSV נשמר בתיקיית הפרויקט",
                str(audit_csv_path),
            ],
        )

    summary = {
        "status": "ok",
        "dry_run": dry_run,
        "timezone": tz_name,
        "purge_all_messages": purge_all_messages,
        "include_admin_groups": include_admin_groups,
        "auto_create_meta_json": auto_create_meta_json,
        "sidecar_json_created": sidecar_created,
        "write_audit_csv": write_audit_csv,
        "audit_csv_path": str(audit_csv_path) if audit_csv_path else None,
        "progress_notify": progress_notify,
        "sessions_considered": len(meta_paths),
        "max_sessions_applied": raw_max,
        "groups_touched": len(report_groups),
        "groups": report_groups,
        "session_notes": session_notes,
    }

    if not skip_notify:
        try:
            prov = prov_notify if prov_notify is not None else None
            if prov is None:
                from nexus.shared.notifications.providers.telegram import TelegramProvider

                prov = TelegramProvider.from_task_parameters(parameters)
            lines = [
                "🔒 סיכום נעילת קבוצות (בעלים + אופציונלי אדמין)",
                f"dry_run={dry_run} · אזור_זמן={tz_name}",
                f"מחיקה מלאה={purge_all_messages} · כולל_אדמין={include_admin_groups}",
                f"סה״כ קבוצות שעובדו: {len(report_groups)}",
                "",
            ]
            for g in report_groups[:35]:
                t = g.get("title", "?")
                s = g.get("session", "?")
                errc = len([x for x in g.get("errors", []) if x])
                lines.append(
                    f"• {t} ({s}) — נמחקו={g.get('messages_deleted', 0)} שגיאות={errc}",
                )
            if len(report_groups) > 35:
                lines.append(f"… ועוד {len(report_groups) - 35} קבוצות")
            body = _esc_lines(lines)
            if len(body) > 3900:
                body = body[:3890] + _esc("\n…נחתך")
            await prov.send_message(body)
        except Exception as exc:
            log.warning("owner_groups_lockdown_notify_failed", error=str(exc))
            summary["notify_error"] = str(exc)

    return summary
