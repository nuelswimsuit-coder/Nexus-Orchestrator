"""
management.vault_spambot_weekly — weekly @SpamBot probe for vault sessions.

Persists flags to Redis (session vault meta) and SQLite ``vault_session_telegram_health``.
Dispatched from Master cron when ``NEXUS_SPAMBOT_WEEKLY_CRON_ENABLED=1``; the job
self-gates to once every 7 days unless ``force: true`` is passed in parameters.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog

from nexus.services.session_vault import (
    _default_meta_record,
    discover_all_meta_json_files,
    export_string_session_sync,
    merge_meta_row,
)
from nexus.shared.management_store import upsert_vault_session_spambot_health
from nexus.shared.tg_connection import (
    telethon_connect_kwargs_for_meta_json,
    telegram_network_slot,
)
from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

_SPAMBOT_LAST_KEY = "nexus:session_vault:spambot_weekly:last_completed_at"
_DEFAULT_INTERVAL_DAYS = 7


def _classify_spambot_reply(text: str) -> tuple[bool, str]:
    t = (text or "").lower()
    if any(k in t for k in ("no limits", "good news", "no problems", "nothing is wrong")):
        return False, "ok_keyword"
    if any(
        k in t
        for k in (
            "limited",
            "spam",
            "restricted",
            "unfortunately",
            "suspended",
            "flood",
            "ban",
            "blocked",
        )
    ):
        return True, "suspected_keyword"
    return False, "unknown"


async def _probe_one_spambot(meta_json: Path, redis: Any) -> dict[str, Any]:
    stem = meta_json.stem
    checked_at = datetime.now(timezone.utc).isoformat()
    try:
        export = await asyncio.to_thread(export_string_session_sync, meta_json)
    except Exception as exc:
        log.warning("spambot_export_failed", stem=stem, error=str(exc))
        return {"stem": stem, "error": str(exc), "shadowban_suspected": None}

    from telethon import TelegramClient  # type: ignore[import-untyped]
    from telethon.errors import (  # type: ignore[import-untyped]
        FloodWaitError,
        RPCError,
        UserDeactivatedBanError,
        UserDeactivatedError,
    )
    from telethon.sessions import StringSession  # type: ignore[import-untyped]

    t_kw = telethon_connect_kwargs_for_meta_json(meta_json)
    snippet = ""
    suspected: bool | None = None
    scan_class = "unknown"

    try:
        async with telegram_network_slot(task_name=f"spambot:{stem}"):
            async with TelegramClient(
                StringSession(export["string_session"]),
                export["api_id"],
                export["api_hash"],
                **t_kw,
            ) as client:
                await client.send_message("SpamBot", "/start")
                await asyncio.sleep(2.5)
                msgs = await client.get_messages("SpamBot", limit=6)
                parts: list[str] = []
                for m in msgs:
                    msg = getattr(m, "message", None) or getattr(m, "raw_text", None)
                    if msg:
                        parts.append(str(msg))
                snippet = "\n".join(parts)[:2000]
                suspected, scan_class = _classify_spambot_reply(snippet)
    except UserDeactivatedError as exc:
        log.warning("spambot_user_deactivated", stem=stem, error=str(exc))
        return {
            "stem": stem,
            "error": "UserDeactivatedError",
            "shadowban_suspected": True,
            "spambot_reply_snippet": "",
            "scan_class": "user_deactivated",
        }
    except UserDeactivatedBanError as exc:
        log.warning("spambot_user_deactivated_ban", stem=stem, error=str(exc))
        return {
            "stem": stem,
            "error": "UserDeactivatedBanError",
            "shadowban_suspected": True,
            "spambot_reply_snippet": "",
            "scan_class": "user_deactivated_ban",
        }
    except FloodWaitError as exc:
        wait_s = int(getattr(exc, "seconds", 60) or 60)
        log.warning("spambot_flood_wait", stem=stem, seconds=wait_s)
        return {
            "stem": stem,
            "error": f"FloodWaitError:{wait_s}",
            "shadowban_suspected": None,
            "spambot_reply_snippet": "",
            "scan_class": "flood_wait",
        }
    except RPCError as exc:
        log.warning("spambot_rpc_error", stem=stem, error=str(exc))
        return {
            "stem": stem,
            "error": str(exc),
            "shadowban_suspected": None,
            "spambot_reply_snippet": snippet,
            "scan_class": "rpc_error",
        }
    except Exception as exc:
        log.warning("spambot_probe_failed", stem=stem, error=str(exc))
        return {
            "stem": stem,
            "error": str(exc),
            "shadowban_suspected": None,
            "spambot_reply_snippet": snippet,
            "scan_class": "error",
        }

    row = _default_meta_record(meta_json)
    row.update(
        {
            "spambot_checked_at": checked_at,
            "shadowban_suspected": bool(suspected),
            "spambot_scan_class": scan_class,
            "spambot_reply_snippet": snippet[:500],
            "checked_at": checked_at,
        }
    )
    try:
        await merge_meta_row(redis, meta_json, row)
    except Exception as exc:
        log.warning("spambot_redis_merge_failed", stem=stem, error=str(exc))

    try:
        await upsert_vault_session_spambot_health(
            session_stem=stem,
            spambot_checked_at=checked_at,
            shadowban_suspected=bool(suspected),
            spambot_reply_snippet=snippet,
        )
    except Exception as exc:
        log.warning("spambot_sqlite_upsert_failed", stem=stem, error=str(exc))

    return {
        "stem": stem,
        "error": None,
        "shadowban_suspected": bool(suspected),
        "spambot_reply_snippet": snippet[:500],
        "scan_class": scan_class,
    }


@registry.register("management.vault_spambot_weekly")
async def vault_spambot_weekly(parameters: dict[str, Any]) -> dict[str, Any]:
    t0 = time.monotonic()
    redis = parameters.get("__redis__")
    if redis is None:
        return {
            "status": "failed",
            "error": "Redis client missing (__redis__); cannot persist session flags",
            "duration_s": round(time.monotonic() - t0, 2),
        }

    force = bool(parameters.get("force"))
    interval_days = int(parameters.get("interval_days") or _DEFAULT_INTERVAL_DAYS)
    if interval_days < 1:
        interval_days = _DEFAULT_INTERVAL_DAYS

    if not force:
        try:
            raw = await redis.get(_SPAMBOT_LAST_KEY)
            if raw:
                txt = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
                last = datetime.fromisoformat(txt.replace("Z", "+00:00"))
                if last.tzinfo is None:
                    last = last.replace(tzinfo=timezone.utc)
                if datetime.now(timezone.utc) - last < timedelta(days=interval_days):
                    return {
                        "status": "skipped",
                        "reason": f"last_run_within_{interval_days}d",
                        "last_completed_at": txt,
                        "duration_s": round(time.monotonic() - t0, 2),
                    }
        except Exception as exc:
            log.debug("spambot_weekly_gate_parse_failed", error=str(exc))

    paths = discover_all_meta_json_files()
    if not paths:
        return {
            "status": "completed",
            "sessions": [],
            "message": "no_meta_json_sessions",
            "duration_s": round(time.monotonic() - t0, 2),
        }

    max_sessions = parameters.get("max_sessions")
    if max_sessions is not None:
        try:
            cap = int(max_sessions)
            if cap > 0:
                paths = paths[:cap]
        except (TypeError, ValueError):
            pass

    results = await asyncio.gather(*(_probe_one_spambot(p, redis) for p in paths))
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        await redis.set(_SPAMBOT_LAST_KEY, now_iso)
    except Exception as exc:
        log.warning("spambot_weekly_last_key_failed", error=str(exc))

    suspected_n = sum(1 for r in results if r.get("shadowban_suspected") is True)
    err_n = sum(1 for r in results if r.get("error"))
    log.info(
        "vault_spambot_weekly_done",
        sessions=len(results),
        suspected=suspected_n,
        errors=err_n,
        duration_s=round(time.monotonic() - t0, 2),
    )
    return {
        "status": "completed",
        "generated_at": now_iso,
        "sessions_total": len(results),
        "shadowban_suspected_count": suspected_n,
        "error_count": err_n,
        "sessions": results,
        "duration_s": round(time.monotonic() - t0, 2),
    }
