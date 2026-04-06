"""
management.sentinel_seo — Global search rank + shadowban heuristic + optional title rename.

Uses a dedicated "clean" Telethon session (NEXUS_SEO_PROBE_SESSION) for SearchRequest.
Runs on the asyncio event loop (async Telethon) so scans yield between iterations and the
bot / other coroutines stay responsive when sharing a process.

Auto-rename requires NEXUS_SEO_AUTO_RENAME=1 and an admin session stem in task params.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

import structlog

from nexus.shared.config import settings
from nexus.shared.management_store import (
    management_db_path,
    sync_list_groups_minimal,
    sync_upsert_rank_tracker_row,
)
from nexus.shared.staged_accounts import discover_session_meta_json_files, staged_accounts_root
from nexus.shared.tg_connection import telethon_connect_kwargs_for_meta_json, telegram_network_slot
from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

# Yield between group/keyword RPCs so aiogram and other tasks get CPU on a shared loop.
_TELETHON_ITER_PAUSE_S = float(
    (os.getenv("NEXUS_SENTINEL_SEO_ITER_PAUSE_S") or "1.0").strip() or 1.0
)
_SEARCH_RPC_ATTEMPTS = 8


def _resolve_probe_meta(staged_dir: Path, probe_stem: str) -> Path | None:
    stem = probe_stem.strip()
    if not stem:
        return None
    p = Path(stem)
    if p.is_file() and p.suffix.lower() == ".json":
        return p
    if p.is_dir():
        return None
    direct = staged_dir / f"{stem}.json"
    if direct.is_file():
        return direct
    for path in discover_session_meta_json_files(staged_dir):
        if path.stem == stem:
            return path
    return None


async def _ensure_connected(client: Any) -> None:
    if client.is_connected():
        return
    await client.connect()


async def _entity_reachable_async(
    client: Any, username: str | None, invite_link: str | None
) -> bool:
    from telethon.errors import RPCError  # type: ignore

    for raw in (invite_link, f"@{username}" if username else None, username):
        if not raw:
            continue
        try:
            await _ensure_connected(client)
            await client.get_entity(raw)
            return True
        except RPCError:
            continue
        except Exception:
            continue
    return False


async def _search_rank_and_presence_async(
    client: Any,
    query: str,
    target_username: str | None,
    target_id: int,
) -> tuple[int | None, bool]:
    from telethon.errors import FloodWaitError  # type: ignore
    from telethon.tl.functions.contacts import SearchRequest  # type: ignore

    q = query.strip().lstrip("@")
    if not q:
        return None, False

    res = None
    for attempt in range(_SEARCH_RPC_ATTEMPTS):
        try:
            await _ensure_connected(client)
            res = await client(SearchRequest(q=q, limit=50))
            break
        except FloodWaitError as fw:
            wait_s = min(max(1, int(getattr(fw, "seconds", 60) or 60)), 3600)
            log.warning(
                "sentinel_seo_flood_wait",
                query=q,
                seconds=wait_s,
                attempt=attempt + 1,
            )
            await asyncio.sleep(wait_s)
        except (ConnectionError, OSError) as exc:
            log.warning(
                "sentinel_seo_connection_error",
                query=q,
                error=str(exc),
                attempt=attempt + 1,
            )
            try:
                await client.connect()
            except Exception as e2:
                log.warning("sentinel_seo_reconnect_failed", error=str(e2))
            await asyncio.sleep(0.75)
        except Exception as exc:
            log.warning("sentinel_seo_search_failed", query=q, error=str(exc))
            return None, False

    if res is None:
        return None, False

    chats = list(getattr(res, "chats", []) or [])
    uname = (target_username or "").lower().lstrip("@")
    for i, ch in enumerate(chats):
        cid = int(getattr(ch, "id", 0) or 0)
        cuser = (getattr(ch, "username", None) or "").lower()
        if cid == target_id or (uname and cuser == uname):
            return i + 1, True
    return None, False


async def _maybe_rename_async(
    admin_client: Any,
    entity: Any,
    target_title: str,
    max_attempts: int,
    cooldown_s: float,
) -> bool:
    from telethon.tl.functions.channels import EditTitleRequest  # type: ignore
    from telethon.tl.types import Channel  # type: ignore

    title = target_title[:128]
    if not isinstance(entity, Channel) or not entity.megagroup:
        try:
            await _ensure_connected(admin_client)
            await admin_client(EditTitleRequest(channel=entity, title=title))
            return True
        except Exception as exc:
            log.warning("sentinel_seo_rename_failed", error=str(exc))
            return False

    for attempt in range(max(1, max_attempts)):
        try:
            await _ensure_connected(admin_client)
            await admin_client(EditTitleRequest(channel=entity, title=title))
            return True
        except Exception as exc:
            log.warning(
                "sentinel_seo_rename_attempt_failed",
                attempt=attempt + 1,
                error=str(exc),
            )
        await asyncio.sleep(cooldown_s)
    return False


async def _run_sentinel_job_async(parameters: dict[str, Any]) -> dict[str, Any]:
    from telethon import TelegramClient  # type: ignore

    staged_dir = Path(parameters.get("staged_dir", str(staged_accounts_root())))
    probe_stem = (
        parameters.get("probe_session_stem")
        or os.getenv("NEXUS_SEO_PROBE_SESSION", "").strip()
        or (settings.nexus_seo_probe_session or "").strip()
    )
    meta = _resolve_probe_meta(staged_dir, probe_stem)
    if meta is None:
        return {
            "status": "failed",
            "error": "NEXUS_SEO_PROBE_SESSION not set or meta .json not found under staged_dir",
        }

    phrases_in = parameters.get("seo_keyword_phrases") or []
    if isinstance(phrases_in, str):
        phrases_in = [p for p in phrases_in.split(",") if p.strip()]
    extra_phrases = [str(p).strip() for p in phrases_in if str(p).strip()]

    gm_filter = parameters.get("group_metadata_ids")
    groups = sync_list_groups_minimal()
    if gm_filter:
        want = {int(x) for x in gm_filter}
        groups = [g for g in groups if g["id"] in want]

    auto_rename = bool(parameters.get("auto_rename"))
    if not auto_rename:
        auto_rename = settings.nexus_seo_auto_rename or (
            os.getenv("NEXUS_SEO_AUTO_RENAME", "").strip().lower() in ("1", "true", "yes")
        )
    target_title = (
        parameters.get("target_title")
        or os.getenv("NEXUS_SEO_TARGET_TITLE", "").strip()
        or (settings.nexus_seo_target_title or "").strip()
    )
    raw_max = parameters.get("max_rename_attempts")
    if raw_max is not None:
        max_attempts = int(raw_max)
    else:
        env_m = (os.getenv("NEXUS_SEO_AUTO_RENAME_MAX") or "").strip()
        max_attempts = int(env_m) if env_m.isdigit() else settings.nexus_seo_auto_rename_max

    raw_cd = parameters.get("rename_cooldown_s")
    if raw_cd is not None:
        cooldown_s = float(raw_cd)
    else:
        env_cd = (os.getenv("NEXUS_SEO_RENAME_COOLDOWN_S") or "").strip()
        cooldown_s = float(env_cd) if env_cd else settings.nexus_seo_rename_cooldown_s

    admin_stem = (parameters.get("admin_session_stem") or "").strip()
    admin_meta = _resolve_probe_meta(staged_dir, admin_stem) if admin_stem else None

    with open(meta, encoding="utf-8") as f:
        meta_probe = json.load(f)
    api_id = int(meta_probe["api_id"])
    api_hash = str(meta_probe["api_hash"])
    session_file = str(meta.with_suffix(""))
    t_kw = telethon_connect_kwargs_for_meta_json(meta)

    probe = TelegramClient(session_file, api_id, api_hash, **t_kw)
    admin_client: Any | None = None
    pause = max(0.05, _TELETHON_ITER_PAUSE_S)

    try:
        await probe.connect()
        if not probe.is_user_authorized():
            await probe.disconnect()
            raise PermissionError(f"Session not authorized: {meta.stem}")

        if auto_rename and admin_meta and target_title:
            with open(admin_meta, encoding="utf-8") as f:
                meta_admin = json.load(f)
            api_id_a = int(meta_admin["api_id"])
            api_hash_a = str(meta_admin["api_hash"])
            session_file_a = str(admin_meta.with_suffix(""))
            t_kw_a = telethon_connect_kwargs_for_meta_json(admin_meta)
            admin_client = TelegramClient(session_file_a, api_id_a, api_hash_a, **t_kw_a)
            await admin_client.connect()
            if not admin_client.is_user_authorized():
                await admin_client.disconnect()
                raise PermissionError(f"Session not authorized: {admin_meta.stem}")

        updated = 0
        for g in groups:
            await asyncio.sleep(pause)
            gid_row = g["id"]
            tg_id = int(g["group_id"])
            uname = g.get("username")
            invite = g.get("invite_link")

            reachable = await _entity_reachable_async(probe, uname, invite)
            queries: list[str] = []
            for p in extra_phrases:
                if len(p.split()) >= 2:
                    queries.append(p)
            if uname:
                queries.append(uname)
            seen_q: set[str] = set()
            queries = [q for q in queries if not (q in seen_q or seen_q.add(q))]

            if not queries:
                continue

            any_shadow = False
            for phrase in queries:
                await asyncio.sleep(pause)
                rank, in_search = await _search_rank_and_presence_async(
                    probe, phrase, uname, tg_id
                )
                shadow = reachable and not in_search
                if shadow:
                    any_shadow = True
                sync_upsert_rank_tracker_row(
                    group_metadata_id=gid_row,
                    keyword_phrase=phrase,
                    current_rank=rank,
                    is_shadowbanned=shadow,
                )
                updated += 1

            if auto_rename and admin_client and target_title and any_shadow and reachable:
                try:
                    ent = None
                    if invite:
                        await _ensure_connected(admin_client)
                        ent = await admin_client.get_entity(invite)
                    elif uname:
                        await _ensure_connected(admin_client)
                        ent = await admin_client.get_entity(uname)
                    if ent is not None and int(getattr(ent, "id", 0)) == tg_id:
                        await _maybe_rename_async(
                            admin_client,
                            ent,
                            target_title,
                            max_attempts=max_attempts,
                            cooldown_s=cooldown_s,
                        )
                except Exception as exc:
                    log.warning("sentinel_seo_auto_rename_skipped", error=str(exc))

        return {
            "status": "completed",
            "groups_considered": len(groups),
            "rank_rows_updated": updated,
            "db_path": str(management_db_path()),
        }
    finally:
        try:
            await probe.disconnect()
        except Exception:
            pass
        if admin_client is not None:
            try:
                await admin_client.disconnect()
            except Exception:
                pass


@registry.register("management.sentinel_seo")
async def sentinel_seo(parameters: dict[str, Any]) -> dict[str, Any]:
    t0 = time.monotonic()
    try:
        async with telegram_network_slot(task_name="management.sentinel_seo"):
            result = await _run_sentinel_job_async(parameters)
    except Exception as exc:
        log.exception("management_sentinel_seo_failed", error=str(exc))
        return {"status": "failed", "error": str(exc), "duration_s": round(time.monotonic() - t0, 2)}

    result["duration_s"] = round(time.monotonic() - t0, 2)
    return result
