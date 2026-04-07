"""
Post-send human hesitation simulation for Telethon user clients.

After ``send_message`` succeeds, optionally schedule a background delete (2%) or
quick edit with a trailing emoji (3%), mutually exclusive. Callers using a
short-lived client must ``await await_human_hesitation_tasks(client)`` before
disconnect (see :func:`nexus.worker.services.tg_session.async_telegram_client`).
"""

from __future__ import annotations

import asyncio
import random
from typing import Any

import structlog

log = structlog.get_logger(__name__)

_EDIT_EMOJIS = ("😅", "🤷‍♂️", "🤔")


def _message_plain_text(msg: Any) -> str:
    raw = (getattr(msg, "message", None) or getattr(msg, "raw_text", None) or "") or ""
    return str(raw)


def _hesitation_bucket(client: Any) -> list[asyncio.Task[Any]]:
    bucket = getattr(client, "_nexus_human_hesitation_tasks", None)
    if bucket is None:
        bucket = []
        setattr(client, "_nexus_human_hesitation_tasks", bucket)
    return bucket


async def _regret_delete_path(client: Any, chat: Any, msg_id: int) -> None:
    try:
        await asyncio.sleep(random.randint(15, 60))
        await client.delete_messages(chat, message_ids=[msg_id])
    except Exception as exc:
        log.debug("telethon_regret_delete_failed", msg_id=msg_id, error=str(exc))


async def _quick_edit_path(client: Any, chat: Any, msg_id: int, original_text: str) -> None:
    try:
        await asyncio.sleep(random.randint(5, 15))
        emoji = random.choice(_EDIT_EMOJIS)
        new_text = f"{original_text}{emoji}"
        await client.edit_message(chat, msg_id, new_text)
    except Exception as exc:
        log.debug("telethon_quick_edit_failed", msg_id=msg_id, error=str(exc))


def schedule_post_send_human_hesitation(client: Any, chat: Any, msg: Any) -> None:
    """
    After a successful ``client.send_message``: 2% regret-delete, 3% quick-edit
    (mutually exclusive). Tasks are appended to ``client._nexus_human_hesitation_tasks``.
    """
    msg_id = getattr(msg, "id", None)
    if msg_id is None:
        return
    mid = int(msg_id)
    r = random.random()
    if r < 0.02:
        task = asyncio.create_task(_regret_delete_path(client, chat, mid))
    elif r < 0.05:
        task = asyncio.create_task(_quick_edit_path(client, chat, mid, _message_plain_text(msg)))
    else:
        return
    _hesitation_bucket(client).append(task)


async def await_human_hesitation_tasks(client: Any) -> None:
    """Await all hesitation tasks for this client (no-op if none)."""
    bucket = getattr(client, "_nexus_human_hesitation_tasks", None)
    if not bucket:
        return
    await asyncio.gather(*bucket, return_exceptions=True)
    bucket.clear()
