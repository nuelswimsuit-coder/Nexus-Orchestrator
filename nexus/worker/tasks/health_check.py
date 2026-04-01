"""
management.group_health_scan — Telethon scrape of managed groups into telefix.db.

Per staged session: full channel info, optional primary invite export, participant
stats (premium / deleted / active_real), persisted via management_store.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from nexus.shared.management_store import sync_upsert_group_bundle
from nexus.shared.staged_accounts import discover_session_meta_json_files, staged_accounts_root
from nexus.worker.task_registry import registry
from nexus.worker.tasks.account_mapper import (
    _asset_kind,
    _controlled_warmup_delay_s,
    _is_managed,
    _member_count,
    _parse_proxy_pool,
    _proxy_for_index,
)

log = structlog.get_logger(__name__)

_DEFAULT_STAGED = staged_accounts_root()


def _invite_link_from_export(client: Any, entity: Any) -> str | None:
    from telethon.errors import RPCError  # type: ignore
    from telethon.tl.functions.messages import ExportChatInviteRequest  # type: ignore

    try:
        r = client(ExportChatInviteRequest(peer=entity))
        return getattr(r, "link", None) or None
    except RPCError:
        return None
    except Exception as exc:
        log.debug("health_check_export_invite_failed", error=str(exc))
        return None


def _creator_id_from_full(client: Any, entity: Any) -> int | None:
    from telethon.tl.functions.channels import GetFullChannelRequest  # type: ignore
    from telethon.tl.types import Channel  # type: ignore

    if not isinstance(entity, Channel):
        return None
    try:
        r = client(GetFullChannelRequest(channel=entity))
        fc = getattr(r, "full_chat", None)
        if fc is None:
            return None
        return int(getattr(fc, "creator_id", 0) or 0) or None
    except Exception as exc:
        log.debug("health_check_creator_id_failed", error=str(exc))
        return None


def _admin_log_invite_hint(client: Any, entity: Any) -> str | None:
    from telethon.tl.functions.channels import GetAdminLogRequest  # type: ignore
    from telethon.tl.types import Channel  # type: ignore

    if not isinstance(entity, Channel):
        return None
    try:
        r = client(
            GetAdminLogRequest(
                channel=entity,
                q="",
                events_filter=None,
                search="",
                admins=[],
                max_id=0,
                min_id=0,
                limit=20,
            )
        )
        for ev in getattr(r, "events", []) or []:
            action = getattr(ev, "action", None)
            if action is None:
                continue
            link = getattr(action, "link", None)
            if link:
                return str(link)
    except Exception as exc:
        log.debug("health_check_admin_log_invite_failed", error=str(exc))
    return None


def _participant_breakdown(
    client: Any,
    entity: Any,
    total_members: int,
    limit: int | None,
) -> tuple[int, int, int, bool]:
    """
    Returns (premium_count, deleted_count, active_real_count, partial).
    active_real_count = max(0, total_members - deleted_est) when scan covers all
    participants; when capped, scales deleted estimate from the sample.
    """
    from telethon.tl.types import Channel, Chat  # type: ignore

    if not isinstance(entity, (Chat, Channel)):
        return 0, 0, 0, False
    if isinstance(entity, Channel) and entity.broadcast:
        return 0, 0, total_members, False

    premium = 0
    deleted = 0
    scanned = 0
    partial = False
    try:
        for u in client.iter_participants(entity):
            scanned += 1
            if limit is not None and scanned > limit:
                partial = True
                break
            if getattr(u, "premium", False):
                premium += 1
            if getattr(u, "deleted", False):
                deleted += 1
    except Exception as exc:
        log.warning("health_check_iter_participants_failed", error=str(exc))
        return 0, 0, max(0, total_members), True

    if partial and scanned > 0 and total_members > scanned:
        ratio_del = deleted / scanned
        est_deleted = min(total_members, int(round(total_members * ratio_del)))
        active_real = max(0, total_members - est_deleted)
        return premium, est_deleted, active_real, True

    active_real = max(0, total_members - deleted)
    return premium, deleted, active_real, partial


def _scan_one_session(
    meta_json: Path,
    proxy: Any,
    participant_limit: int | None,
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

    written = 0
    errors: list[str] = []

    try:
        for dialog in client.iter_dialogs():
            entity = dialog.entity
            if not _is_managed(client, entity):
                continue
            kind = _asset_kind(entity)
            if kind not in ("group", "supergroup"):
                continue

            title = getattr(entity, "title", None) or str(getattr(entity, "id", ""))
            username = getattr(entity, "username", None)
            is_public = bool(
                isinstance(entity, Channel)
                and not getattr(entity, "megagroup", False)
                and getattr(entity, "username", None)
            )
            if isinstance(entity, Channel) and entity.megagroup:
                is_public = bool(username)

            total_members = _member_count(client, entity)
            prem, del_c, active_r, partial = _participant_breakdown(
                client, entity, total_members, participant_limit
            )

            invite = _invite_link_from_export(client, entity) or _admin_log_invite_hint(
                client, entity
            )
            creator_id = _creator_id_from_full(client, entity)

            try:
                sync_upsert_group_bundle(
                    session_owner=session_label,
                    group_id=int(entity.id),
                    title=str(title) if title else None,
                    username=str(username) if username else None,
                    is_public=is_public,
                    invite_link=invite,
                    creator_id=creator_id,
                    total_members=total_members,
                    premium_count=prem,
                    deleted_count=del_c,
                    active_real_count=active_r,
                )
                written += 1
                if partial:
                    log.info(
                        "health_check_group_partial_scan",
                        session=session_label,
                        group_id=entity.id,
                    )
            except Exception as exc:
                err = f"{entity.id}: {exc}"
                errors.append(err)
                log.warning("health_check_upsert_failed", session=session_label, error=err)

        return {
            "session_file": session_label,
            "status": "ok",
            "groups_written": written,
            "errors": errors,
        }
    finally:
        try:
            client.disconnect()
        except Exception:
            pass


def _run_health_job(
    staged_dir: Path,
    *,
    controlled_warmup: bool,
    warmup_mu_min_s: float,
    warmup_mu_max_s: float,
    warmup_sigma_min_s: float,
    warmup_sigma_max_s: float,
    cooldown_min_s: float,
    cooldown_max_s: float,
    participant_limit: int | None,
) -> dict[str, Any]:
    staged_dir = Path(staged_dir)
    metas = discover_session_meta_json_files(staged_dir)
    pool = _parse_proxy_pool()
    sessions_out: list[dict[str, Any]] = []

    for idx, meta_path in enumerate(metas):
        proxy = _proxy_for_index(pool, idx) if pool else None
        if controlled_warmup and idx > 0:
            delay = _controlled_warmup_delay_s(
                warmup_mu_min_s,
                warmup_mu_max_s,
                warmup_sigma_min_s,
                warmup_sigma_max_s,
            )
            time.sleep(delay)
        elif idx > 0:
            time.sleep(random.uniform(cooldown_min_s, cooldown_max_s))

        try:
            one = _scan_one_session(meta_path, proxy, participant_limit)
            sessions_out.append(one)
        except Exception as exc:
            log.error(
                "health_check_session_failed",
                session=meta_path.stem,
                error=str(exc),
            )
            sessions_out.append({
                "session_file": meta_path.stem,
                "status": "failed",
                "error": str(exc),
                "groups_written": 0,
                "errors": [],
            })

    total_written = sum(s.get("groups_written") or 0 for s in sessions_out)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "staged_dir": str(staged_dir),
        "sessions": sessions_out,
        "total_groups_written": total_written,
        "status": "completed",
    }


@registry.register("management.group_health_scan")
async def group_health_scan(parameters: dict[str, Any]) -> dict[str, Any]:
    """Iterate staged Telethon sessions and upsert management dashboard tables."""
    t0 = time.monotonic()
    staged_dir = Path(parameters.get("staged_dir", str(_DEFAULT_STAGED)))
    controlled = parameters.get("controlled_warmup")
    controlled_warmup = True if controlled is None else bool(controlled)

    warmup_mu_min_s = float(parameters.get("warmup_mu_min_s", 30))
    warmup_mu_max_s = float(parameters.get("warmup_mu_max_s", 60))
    warmup_sigma_min_s = float(parameters.get("warmup_sigma_min_s", 5))
    warmup_sigma_max_s = float(parameters.get("warmup_sigma_max_s", 15))
    cooldown_min_s = float(parameters.get("cooldown_min_s", 8))
    cooldown_max_s = float(parameters.get("cooldown_max_s", 45))

    raw_lim = parameters.get("participant_limit")
    if raw_lim is None or raw_lim == "":
        env_lim = os.getenv("NEXUS_HEALTH_PARTICIPANT_LIMIT")
        participant_limit = int(env_lim) if env_lim and env_lim.isdigit() else 5000
    else:
        participant_limit = int(raw_lim)
    if participant_limit <= 0:
        participant_limit = None

    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: _run_health_job(
                staged_dir,
                controlled_warmup=controlled_warmup,
                warmup_mu_min_s=warmup_mu_min_s,
                warmup_mu_max_s=warmup_mu_max_s,
                warmup_sigma_min_s=warmup_sigma_min_s,
                warmup_sigma_max_s=warmup_sigma_max_s,
                cooldown_min_s=cooldown_min_s,
                cooldown_max_s=cooldown_max_s,
                participant_limit=participant_limit,
            ),
        )
    except Exception as exc:
        log.exception("management_group_health_scan_failed", error=str(exc))
        return {
            "status": "failed",
            "error": str(exc),
            "duration_s": round(time.monotonic() - t0, 2),
        }

    result["duration_s"] = round(time.monotonic() - t0, 2)
    return result
