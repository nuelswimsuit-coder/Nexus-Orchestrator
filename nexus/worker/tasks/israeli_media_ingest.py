"""
Daily / scheduled ingest of public Telegram channel media into the Redis-backed meme DB.

Requires a vault Telethon session (``NEXUS_MEME_INGEST_SESSION`` or ``parameters.session_base``)
and ``NEXUS_MEME_TG_CHANNELS`` (or ``parameters.channels``).
"""

from __future__ import annotations

import os
from typing import Any

import structlog

from nexus.services.israeli_media_meme_engine import ingest_israeli_telegram_media
from nexus.worker.services.tg_session import async_telegram_client
from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)


@registry.register("swarm.israeli_media.ingest")
async def israeli_media_ingest(parameters: dict[str, Any]) -> dict[str, Any]:
    redis = parameters.get("__redis__")
    session_base = (
        (parameters.get("session_base") or os.getenv("NEXUS_MEME_INGEST_SESSION") or "").strip()
    )
    if not session_base:
        return {
            "status": "failed",
            "error": (
                "Set NEXUS_MEME_INGEST_SESSION or parameters.session_base "
                "(Telethon stem, no .session)"
            ),
        }
    try:
        async with async_telegram_client(session_base, parameters) as client:
            out = await ingest_israeli_telegram_media(client, redis, parameters)
            log.info("israeli_media_ingest_done", **{k: v for k, v in out.items() if k != "errors"})
            return out
    except Exception as exc:
        log.warning("israeli_media_ingest_failed", error=str(exc))
        return {"status": "failed", "error": str(exc)}
