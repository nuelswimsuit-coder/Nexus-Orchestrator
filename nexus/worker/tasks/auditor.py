"""
seo.watchdog.audit — SEO invite usage, member_audit rows, premium density support.

Uses owner (staged) Telethon sessions. Invite usage comes from MTProto
``messages.GetExportedChatInvitesRequest`` (exported invites expose ``usage``),
not ``GetInviteStatusRequest`` (not exposed in Telethon under that name).
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import structlog

from nexus.shared.management_store import (
    sync_apply_member_participants,
    sync_get_member_audit_map,
    sync_list_groups_minimal,
    sync_upsert_seo_invite_snapshot,
)
from nexus.shared.staged_accounts import discover_session_meta_json_files, staged_accounts_root
from nexus.worker.task_registry import registry
from nexus.worker.tasks.account_mapper import (
    _asset_kind,
    _is_managed,
    _member_count,
    _parse_proxy_pool,
    _proxy_for_index,
)

log = structlog.get_logger(__name__)

_DEFAULT_STAGED = staged_accounts_root()


def _parse_seo_group_filter() -> set[int] | None:
    raw = (os.getenv("SEO_GROUP_IDS_JSON") or "").strip()
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("seo_group_ids_json_invalid")
        return None
    if not isinstance(data, list):
        return None
    out: set[int] = set()
    for x in data:
        try:
            out.add(int(x))
        except (TypeError, ValueError):
            continue
    return out or None


def _default_subscription_days() -> int:
    try:
        d = int(os.getenv("SEO_DEFAULT_SUBSCRIPTION_DAYS", "30"))
        return d if d in (30, 60) else 30
    except ValueError:
        return 30


def _participant_limit() -> int | None:
    raw = (os.getenv("NEXUS_HEALTH_PARTICIPANT_LIMIT") or "").strip()
    if raw.isdigit():
        n = int(raw)
        return None if n <= 0 else n
    return 5000


def _norm_invite_fragment(link: str | None) -> str | None:
    if not link:
        return None
    u = link.strip().rstrip("/").lower()
    if "t.me/" in u:
        u = u.split("t.me/", 1)[-1]
    return u.split("?", 1)[0]


def _invite_slug(link: str | None) -> str | None:
    frag = _norm_invite_fragment(link)
    if frag and frag.startswith("+"):
        return frag[1:]
    if frag and "+" in frag:
        return frag.split("+", 1)[-1]
    return frag


def _parse_iso_utc(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _with_flood_retry(
    op: Callable[[], Any],
    *,
    context: str,
    max_retries: int = 5,
) -> Any:
    from telethon.errors import FloodWaitError  # type: ignore
    from telethon.errors.rpcerrorlist import PeerFloodError  # type: ignore

    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            return op()
        except PeerFloodError as exc:
            log.warning("seo_auditor_peer_flood", context=context, error=str(exc))
            raise
        except FloodWaitError as exc:
            sec = int(getattr(exc, "seconds", 30) or 30)
            wait = sec + random.uniform(0.5, 2.5)
            log.warning(
                "seo_auditor_flood_wait",
                context=context,
                seconds=round(wait, 1),
                attempt=attempt,
            )
            time.sleep(wait)
            last_exc = exc
    if last_exc:
        raise last_exc
    raise RuntimeError("seo_auditor_flood_retry_exhausted")


def _usage_for_stored_invite(client: Any, entity: Any, stored_link: str | None) -> int | None:
    from telethon.tl.functions.messages import GetExportedChatInvitesRequest  # type: ignore
    from telethon.tl.types import Channel  # type: ignore

    if not isinstance(entity, Channel) or not entity.megagroup:
        return None
    want = _norm_invite_fragment(stored_link)
    if not want:
        return None

    def _call() -> Any:
        return client(GetExportedChatInvitesRequest(peer=entity))

    try:
        res = _with_flood_retry(_call, context="get_exported_invites")
    except Exception as exc:
        log.debug("seo_auditor_exported_invites_failed", error=str(exc))
        return None

    for inv in getattr(res, "invites", None) or []:
        link = getattr(inv, "link", None)
        if not link:
            continue
        if _norm_invite_fragment(str(link)) == want:
            return int(getattr(inv, "usage", 0) or 0)
    return None


def _invite_row_for_entity(
    entity_id: int,
    session_label: str,
    minimal_rows: list[dict[str, Any]],
) -> str | None:
    for r in minimal_rows:
        if int(r["group_id"]) == int(entity_id) and r.get("session_owner") == session_label:
            inv = r.get("invite_link")
            if inv:
                return str(inv)
    for r in minimal_rows:
        if int(r["group_id"]) == int(entity_id) and r.get("invite_link"):
            return str(r["invite_link"])
    return None


def _check_early_churn(
    *,
    group_id: int,
    prev: dict[int, dict[str, Any]],
    present_ids: set[int],
) -> None:
    now = datetime.now(timezone.utc)
    for uid, meta in prev.items():
        if meta.get("status") != "Active" or uid in present_ids:
            continue
        jd = _parse_iso_utc(meta.get("join_date"))
        if jd is None:
            continue
        if jd.tzinfo is None:
            jd = jd.replace(tzinfo=timezone.utc)
        dur = int(meta.get("subscription_duration_days") or 30)
        period_end = jd + timedelta(days=dur)
        if now < period_end:
            log.warning(
                "seo_auditor_early_churn",
                group_id=group_id,
                user_id=uid,
                join_date=meta.get("join_date"),
                subscription_days=dur,
                period_end=period_end.isoformat(),
            )


def _audit_one_group(
    client: Any,
    entity: Any,
    *,
    session_label: str,
    group_id: int,
    stored_invite: str | None,
    participant_limit: int | None,
    default_sub_days: int,
    minimal_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    from telethon.tl.types import Channel, Chat  # type: ignore

    if not isinstance(entity, (Chat, Channel)):
        return {"group_id": group_id, "skipped": True, "reason": "not_chat"}
    if isinstance(entity, Channel) and entity.broadcast:
        return {"group_id": group_id, "skipped": True, "reason": "broadcast"}

    total_members = _member_count(client, entity)
    prev = sync_get_member_audit_map(group_id)

    rows: list[dict[str, Any]] = []
    scanned = 0
    premium = 0
    deleted = 0
    slug = _invite_slug(stored_invite or _invite_row_for_entity(group_id, session_label, minimal_rows))

    try:
        for u in client.iter_participants(entity):
            scanned += 1
            if participant_limit is not None and scanned > participant_limit:
                break
            uid = int(u.id)
            if getattr(u, "premium", False):
                premium += 1
            if getattr(u, "deleted", False):
                deleted += 1
                st = "Deleted"
            elif getattr(u, "restricted", False):
                st = "Banned"
            else:
                st = "Active"
            rows.append({
                "user_id": uid,
                "is_premium": bool(getattr(u, "premium", False)),
                "status": st,
                "invite_slug": slug,
            })
    except Exception as exc:
        log.warning("seo_auditor_iter_participants_failed", group_id=group_id, error=str(exc))
        return {"group_id": group_id, "error": str(exc)}

    present_ids = {int(r["user_id"]) for r in rows}
    _check_early_churn(group_id=group_id, prev=prev, present_ids=present_ids)

    sync_apply_member_participants(
        group_id=group_id,
        rows=rows,
        default_subscription_days=default_sub_days,
    )

    inv_link = stored_invite or _invite_row_for_entity(group_id, session_label, minimal_rows)
    usage: int | None = None
    try:
        from telethon.errors.rpcerrorlist import PeerFloodError  # type: ignore

        usage = _usage_for_stored_invite(client, entity, inv_link)
    except PeerFloodError as exc:
        log.warning("seo_auditor_invite_peer_flood", group_id=group_id, error=str(exc))
    except Exception as exc:
        log.warning("seo_auditor_invite_usage_skipped", group_id=group_id, error=str(exc))

    if usage is None:
        usage = 0

    ghost_delta = max(0, int(usage) - int(total_members))
    sync_upsert_seo_invite_snapshot(
        group_id=group_id,
        invite_link=inv_link,
        usage_count=int(usage),
        participant_count=int(total_members),
        ghost_delta=int(ghost_delta),
    )

    prem_pct = round(100.0 * premium / scanned, 2) if scanned else 0.0
    alive_ratio = round((scanned - deleted) / scanned, 4) if scanned else 0.0

    log.info(
        "seo_auditor_group_ok",
        group_id=group_id,
        session_owner=session_label,
        premium_pct=prem_pct,
        alive_ratio=alive_ratio,
        invite_usage=int(usage),
        ghost_delta=int(ghost_delta),
        participants_scanned=scanned,
    )
    print(
        f"[SEO-WATCHDOG] audit_ok group_id={group_id} session={session_label!r} "
        f"premium_pct={prem_pct} alive_ratio={alive_ratio} usage={usage} ghost_delta={ghost_delta}",
        flush=True,
    )

    return {
        "group_id": group_id,
        "premium_pct": prem_pct,
        "alive_ratio": alive_ratio,
        "invite_usage": int(usage),
        "ghost_delta": int(ghost_delta),
        "participants_scanned": scanned,
    }


def _scan_one_session(
    meta_json: Path,
    proxy: Any,
    *,
    participant_limit: int | None,
    default_sub_days: int,
    group_filter: set[int] | None,
    minimal_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    from telethon.sync import TelegramClient  # type: ignore

    with open(meta_json, encoding="utf-8") as f:
        meta = json.load(f)

    api_id = int(meta["api_id"])
    api_hash = str(meta["api_hash"])
    session_file = str(meta_json.with_suffix(""))
    session_label = meta_json.stem

    client = TelegramClient(session_file, api_id, api_hash, proxy=proxy)
    client.connect()
    if not client.is_user_authorized():
        client.disconnect()
        raise PermissionError(f"Session not authorized: {session_label}")

    audited: list[dict[str, Any]] = []
    errors: list[str] = []

    try:
        from telethon.errors.rpcerrorlist import PeerFloodError  # type: ignore

        for dialog in client.iter_dialogs():
            try:
                entity = dialog.entity
                if not _is_managed(client, entity):
                    continue
                gid = int(getattr(entity, "id", 0))
                if group_filter is not None and gid not in group_filter:
                    continue
                if _asset_kind(entity) not in ("group", "supergroup"):
                    continue
                stored = _invite_row_for_entity(gid, session_label, minimal_rows)
                out = _audit_one_group(
                    client,
                    entity,
                    session_label=session_label,
                    group_id=gid,
                    stored_invite=stored,
                    participant_limit=participant_limit,
                    default_sub_days=default_sub_days,
                    minimal_rows=minimal_rows,
                )
                if out.get("error"):
                    errors.append(f"{gid}: {out['error']}")
                elif not out.get("skipped"):
                    audited.append(out)
                time.sleep(random.uniform(0.4, 1.2))
            except PeerFloodError as exc:
                log.warning("seo_auditor_session_peer_flood", session=session_label, error=str(exc))
                errors.append(f"peer_flood: {exc}")
                break
            except Exception as exc:
                errors.append(str(exc))
                log.warning("seo_auditor_dialog_failed", session=session_label, error=str(exc))
    finally:
        try:
            client.disconnect()
        except Exception:
            pass

    return {
        "session_file": session_label,
        "status": "ok",
        "audited": audited,
        "errors": errors,
    }


def _run_audit_job(
    staged_dir: Path,
    *,
    session_start_offset: int,
    participant_limit: int | None,
    default_sub_days: int,
) -> dict[str, Any]:
    staged_dir = Path(staged_dir)
    metas = discover_session_meta_json_files(staged_dir)
    if not metas:
        return {
            "status": "completed",
            "sessions": [],
            "message": "no staged session meta files",
        }

    n = len(metas)
    off = int(session_start_offset) % n
    metas_rotated = metas[off:] + metas[:off]

    pool = _parse_proxy_pool()
    minimal_rows = sync_list_groups_minimal()
    group_filter = _parse_seo_group_filter()

    sessions_out: list[dict[str, Any]] = []
    for idx, meta_path in enumerate(metas_rotated):
        proxy = _proxy_for_index(pool, idx) if pool else None
        if idx > 0:
            time.sleep(random.uniform(2.0, 6.0))
        try:
            one = _scan_one_session(
                meta_path,
                proxy,
                participant_limit=participant_limit,
                default_sub_days=default_sub_days,
                group_filter=group_filter,
                minimal_rows=minimal_rows,
            )
            sessions_out.append(one)
        except Exception as exc:
            log.error("seo_auditor_session_failed", session=meta_path.stem, error=str(exc))
            sessions_out.append({
                "session_file": meta_path.stem,
                "status": "failed",
                "error": str(exc),
                "audited": [],
                "errors": [str(exc)],
            })

    total_audits = sum(len(s.get("audited") or []) for s in sessions_out)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "staged_dir": str(staged_dir),
        "sessions": sessions_out,
        "total_group_audits": total_audits,
        "status": "completed",
    }


@registry.register("seo.watchdog.audit")
async def seo_watchdog_audit(parameters: dict[str, Any]) -> dict[str, Any]:
    """Staged owner sessions: participant audit, invite usage, member_audit + snapshot."""
    t0 = time.monotonic()
    staged_dir = Path(parameters.get("staged_dir", str(_DEFAULT_STAGED)))
    raw_off = parameters.get("session_start_offset", 0)
    if raw_off in (-1, "-1") or str(raw_off).strip() == "-1":
        session_start_offset = int(time.time() // 3600)
    else:
        try:
            session_start_offset = int(raw_off)
        except (TypeError, ValueError):
            session_start_offset = 0
    default_sub_days = int(parameters.get("subscription_days", _default_subscription_days()))
    if default_sub_days not in (30, 60):
        default_sub_days = 30

    raw_lim = parameters.get("participant_limit")
    if raw_lim is None or raw_lim == "":
        participant_limit = _participant_limit()
    else:
        participant_limit = int(raw_lim)
        if participant_limit <= 0:
            participant_limit = None

    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: _run_audit_job(
                staged_dir,
                session_start_offset=session_start_offset,
                participant_limit=participant_limit,
                default_sub_days=default_sub_days,
            ),
        )
    except Exception as exc:
        log.exception("seo_watchdog_audit_failed", error=str(exc))
        return {
            "status": "failed",
            "error": str(exc),
            "duration_s": round(time.monotonic() - t0, 2),
        }

    result["duration_s"] = round(time.monotonic() - t0, 2)
    return result
