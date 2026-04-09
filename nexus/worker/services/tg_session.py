"""
Async Telethon helpers for worker tasks: per-session ``*.json`` api_id/api_hash
(vault layout) with TELEFIX_* fallback, and consistent MTProto error classification.

Uses :class:`telethon.sessions.StringSession` when the task parameters carry a leased
``string_session`` (Master vault lease path) to avoid repeated SQLite session file I/O.
"""

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal

import structlog

from nexus.services.session_vault import vault_meta_resolve_api_credentials
from nexus.shared.telethon_human_hesitation import await_human_hesitation_tasks
from nexus.shared.tg_connection import (
    telegram_network_slot,
    telethon_connect_kwargs_for_session_base,
)

log = structlog.get_logger(__name__)


def _global_telethon_creds(parameters: dict[str, Any]) -> tuple[int, str]:
    sec = parameters.get("__secrets__", {})
    api_id = int(sec.get("TELEFIX_API_ID") or os.getenv("TELEFIX_API_ID", "0") or "0")
    api_hash = str(sec.get("TELEFIX_API_HASH") or os.getenv("TELEFIX_API_HASH", "") or "")
    return api_id, api_hash


def resolve_telethon_creds(session_base: str, parameters: dict[str, Any]) -> tuple[int, str]:
    """
    Prefer ``<session_base>.json`` with api_id / api_hash (Telethon vault pairing);
    else TELEFIX_* from parameters secrets or environment.
    """
    meta = Path(session_base).with_suffix(".json")
    if meta.is_file():
        try:
            data = json.loads(meta.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                creds = vault_meta_resolve_api_credentials(data)
                if creds:
                    return creds[0], creds[1]
        except (OSError, ValueError, TypeError) as exc:
            log.debug("tg_session_meta_read_failed", path=str(meta), error=str(exc))
    return _global_telethon_creds(parameters)


def classify_telethon_account_error(exc: BaseException) -> Literal["ban", "flood", "other"]:
    """Map Telethon errors to backoff / pool-removal behavior."""
    try:
        from telethon.errors import (  # type: ignore[import-untyped]
            AuthKeyDuplicatedError,
            AuthKeyUnregisteredError,
            FloodWaitError,
            PhoneNumberBannedError,
            UserDeactivatedBanError,
            UserDeactivatedError,
        )
    except ImportError:
        return "other"
    if isinstance(exc, FloodWaitError):
        return "flood"
    if isinstance(
        exc,
        (
            UserDeactivatedError,
            UserDeactivatedBanError,
            AuthKeyUnregisteredError,
            AuthKeyDuplicatedError,
            PhoneNumberBannedError,
        ),
    ):
        return "ban"
    return "other"


def flood_wait_seconds(exc: BaseException) -> int:
    try:
        from telethon.errors import FloodWaitError  # type: ignore[import-untyped]
    except ImportError:
        return 60
    if isinstance(exc, FloodWaitError):
        return int(getattr(exc, "seconds", 60) or 60)
    return 60


@asynccontextmanager
async def async_telegram_client(
    session_base: str,
    parameters: dict[str, Any],
) -> AsyncIterator[Any]:
    """
    Connected async Telethon client for ``session_base`` (path without ``.session``),
    or for a leased ``string_session`` string in ``parameters`` (in-memory session).
    """
    from telethon import TelegramClient  # type: ignore[import-untyped]
    from telethon.sessions import StringSession  # type: ignore[import-untyped]

    api_id, api_hash = resolve_telethon_creds(session_base, parameters)
    if not api_id or not api_hash:
        raise ValueError(
            "Telethon api_id/api_hash missing: add <stem>.json next to the session or set TELEFIX_API_ID / TELEFIX_API_HASH"
        )

    leased = (parameters.get("string_session") or "").strip()
    raw_stem = str(parameters.get("session_stem") or Path(session_base).name).strip()
    extra = telethon_connect_kwargs_for_session_base(
        session_base,
        raw_stem if raw_stem else None,
    )

    # Do not use ``async with TelegramClient`` — Telethon's ``__aenter__`` calls
    # ``start()``, which prompts for phone interactively when the session is empty.
    # Workers must only ``connect()`` to existing authorized sessions.
    async with telegram_network_slot(task_name="async_telegram_client"):
        if leased:
            client = TelegramClient(
                StringSession(leased),
                api_id,
                api_hash,
                **extra,
            )
        else:
            client = TelegramClient(session_base, api_id, api_hash, **extra)
        await client.connect()
        try:
            setattr(client, "_nexus_human_hesitation_tasks", [])
            yield client
            await await_human_hesitation_tasks(client)
        finally:
            await client.disconnect()
