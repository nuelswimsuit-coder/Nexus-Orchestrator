"""
swarm.lurkers.tick — passive read receipts for non-speaking vault sessions.

Periodically (via master opt-in scheduler or manual enqueue), picks random
sessions that are **not** on any enabled ``swarm.group_warmer`` roster,
connects with Telethon, loads recent history via ``messages.GetHistoryRequest``,
then marks it read via ``messages.ReadHistoryRequest``. No sends, reactions,
or typing — disconnect when done.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
from pathlib import Path
from typing import Any

import structlog

from nexus.services.session_vault import discover_meta_paths_from_session_sqlite
from nexus.worker.services.tg_session import async_telegram_client
from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

SWARM_GROUPS_KEY = "nexus:swarm:warmer:groups"
_DEFAULT_BATCH = 100
_DEFAULT_HISTORY_LIMIT = 50


def _norm_session_base(path_str: str) -> str:
    p = Path(path_str.strip())
    try:
        return str(p.resolve())
    except OSError:
        return str(p)


def _session_stem_from_base(session_base: str) -> str:
    return Path(session_base).name


async def _warmer_speaker_session_bases(redis: Any) -> set[str]:
    """Normalized session bases listed as speakers under Redis warmer groups."""
    bases: set[str] = set()
    stems: set[str] = set()
    if redis is None:
        return bases
    try:
        raw = await redis.get(SWARM_GROUPS_KEY)
        if not raw:
            return bases
        txt = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
        data = json.loads(txt)
    except Exception as exc:
        log.warning("lurkers_warmer_groups_read_failed", error=str(exc))
        return bases
    if not isinstance(data, dict):
        return bases
    for cfg in data.values():
        if not isinstance(cfg, dict):
            continue
        if not cfg.get("enabled", True):
            continue
        for s in cfg.get("sessions") or []:
            if not isinstance(s, dict):
                continue
            sp = str(s.get("session_path", "")).strip()
            if not sp:
                continue
            bases.add(_norm_session_base(sp))
    return bases


def _meta_is_non_speaker(meta: Path, speaker_bases: set[str], speaker_stems: set[str]) -> bool:
    base = _norm_session_base(str(meta.with_suffix("")))
    stem = meta.stem
    if base in speaker_bases:
        return False
    if stem in speaker_stems:
        return False
    return True


async def _collect_speaker_stems(redis: Any) -> tuple[set[str], set[str]]:
    bases = await _warmer_speaker_session_bases(redis)
    stems = {_session_stem_from_base(b) for b in bases}
    return bases, stems


async def _lurk_one_session(
    meta_json: Path,
    group_id: int,
    parameters: dict[str, Any],
    history_limit: int,
) -> dict[str, Any]:
    from telethon.tl.functions.messages import GetHistoryRequest, ReadHistoryRequest  # type: ignore

    session_base = str(meta_json.with_suffix(""))
    stem = meta_json.stem
    try:
        async with async_telegram_client(session_base, parameters) as client:
            if not await client.is_user_authorized():
                return {"stem": stem, "ok": False, "error": "unauthorized"}
            entity = await client.get_entity(int(group_id))
            peer = await client.get_input_entity(entity)
            hist = await client(
                GetHistoryRequest(
                    peer=peer,
                    offset_id=0,
                    offset_date=None,
                    add_offset=0,
                    limit=int(history_limit),
                    max_id=0,
                    min_id=0,
                    hash=0,
                )
            )
            msgs = list(getattr(hist, "messages", None) or [])
            ids: list[int] = []
            for m in msgs:
                mid = getattr(m, "id", None)
                if mid is not None:
                    try:
                        ids.append(int(mid))
                    except (TypeError, ValueError):
                        pass
            max_id = max(ids, default=0)
            if max_id > 0:
                await client(ReadHistoryRequest(peer=peer, max_id=max_id))
            return {"stem": stem, "ok": True, "read_up_to": max_id, "fetched": len(msgs)}
    except Exception as exc:
        log.debug("lurker_session_failed", stem=stem, error=str(exc))
        return {"stem": stem, "ok": False, "error": str(exc)[:200]}


@registry.register("swarm.lurkers.tick")
async def lurkers_tick(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    One batch of passive read-history passes.

    Parameters
    ----------
    group_id : int — Telegram supergroup / channel id (same as ``swarm.group_warmer``).
    batch_size : int — max sessions per tick (default 100).
    history_limit : int — GetHistory limit (default 50).
    """
    redis = parameters.get("__redis__")
    group_id = parameters.get("group_id")
    if group_id is None:
        gid_raw = (os.getenv("NEXUS_LURKERS_GROUP_ID") or "").strip()
        if gid_raw:
            try:
                group_id = int(gid_raw)
            except ValueError:
                group_id = None
    if group_id is None:
        return {"status": "failed", "error": "group_id required (parameter or NEXUS_LURKERS_GROUP_ID)"}

    try:
        batch_size = int(parameters.get("batch_size", _DEFAULT_BATCH))
    except (TypeError, ValueError):
        batch_size = _DEFAULT_BATCH
    batch_size = max(1, min(500, batch_size))

    try:
        history_limit = int(parameters.get("history_limit", _DEFAULT_HISTORY_LIMIT))
    except (TypeError, ValueError):
        history_limit = _DEFAULT_HISTORY_LIMIT
    history_limit = max(1, min(100, history_limit))

    speaker_bases, speaker_stems = await _collect_speaker_stems(redis)
    all_meta = list(discover_meta_paths_from_session_sqlite())
    pool = [m for m in all_meta if _meta_is_non_speaker(m, speaker_bases, speaker_stems)]
    if len(pool) <= batch_size:
        chosen = list(pool)
        random.shuffle(chosen)
    else:
        chosen = random.sample(pool, batch_size)

    if not chosen:
        return {
            "status": "completed",
            "group_id": int(group_id),
            "selected": 0,
            "results": [],
            "note": "no_eligible_sessions",
        }

    results = await asyncio.gather(
        *[_lurk_one_session(m, int(group_id), parameters, history_limit) for m in chosen],
        return_exceptions=False,
    )
    ok_n = sum(1 for r in results if r.get("ok"))
    log.info(
        "lurkers_tick_done",
        group_id=int(group_id),
        attempted=len(chosen),
        ok=ok_n,
        speakers_excluded=len(speaker_bases),
    )
    return {
        "status": "completed",
        "group_id": int(group_id),
        "selected": len(chosen),
        "ok": ok_n,
        "failed": len(chosen) - ok_n,
        "results": results,
    }
