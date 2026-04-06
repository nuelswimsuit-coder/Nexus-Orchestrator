"""
Central news ingest — runs on a schedule (master dispatches every ~5 minutes).

Scrapes Ynet / N12 / optional Telegram RSS (+ GNews) once, caches to Redis, and
publishes ``nexus:swarm:news_digest`` for swarm consumers.
"""

from __future__ import annotations

from typing import Any

import structlog

from nexus.services.recent_news_digest import refresh_central_news_digest_cache
from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)


@registry.register("swarm.news_digest.refresh")
async def news_digest_refresh(parameters: dict[str, Any]) -> dict[str, Any]:
    redis = parameters.get("__redis__")
    out = await refresh_central_news_digest_cache(redis)
    log.info("news_digest_refresh_done", **{k: v for k, v in out.items() if k != "error"})
    return out
