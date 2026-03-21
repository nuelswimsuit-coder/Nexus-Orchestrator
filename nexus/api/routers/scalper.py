"""
Ultimate Scalper API — simulation toggle, status, optional sentiment ingest.
"""

from __future__ import annotations

import json
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from nexus.api.dependencies import RedisDep
from nexus.master.services.ultimate_scalper import (
    LEDGER_KEY,
    build_scalper_dashboard_payload,
    read_simulation_mode,
    write_simulation_mode,
)
from nexus.worker.tasks.openclaw import publish_openclaw_news_sentiment

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/scalper", tags=["scalper"])


class SimulationModeBody(BaseModel):
    simulation: bool = Field(description="True = virtual $1000 ledger; False = live wallet")


class NewsSentimentBody(BaseModel):
    score: float = Field(ge=0.0, le=10.0, description="0–10 bullish intensity")
    channel_title: str = Field(max_length=500)
    excerpt: str = ""
    source: str = "telegram"
    agent_fingerprint: str | None = Field(
        default=None,
        max_length=200,
        description="Stable id for swarm keyword consensus (session, worker, etc.)",
    )


@router.get("/status")
async def get_scalper_status(redis: RedisDep) -> dict[str, Any]:
    """Dashboard: mode, race progress, velocity, Openclaw sentiment, alpha source."""
    try:
        return await build_scalper_dashboard_payload(redis)
    except Exception as exc:
        log.exception("scalper_status_failed", error=str(exc))
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/simulation-mode")
async def set_simulation_mode(body: SimulationModeBody, redis: RedisDep) -> dict[str, Any]:
    await write_simulation_mode(redis, body.simulation)
    log.info("scalper_simulation_mode_set", simulation=body.simulation)
    return {
        "simulation_mode": await read_simulation_mode(redis),
        "ok": True,
    }


@router.get("/ledger")
async def get_virtual_ledger(redis: RedisDep, limit: int = 50) -> dict[str, Any]:
    """Recent scalper ledger lines (simulation + live entries)."""
    lim = max(1, min(limit, 200))
    raw = await redis.lrange(LEDGER_KEY, -lim, -1)
    rows: list[dict[str, Any]] = []
    for line in raw:
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            rows.append({"raw": line})
    return {"entries": rows, "count": len(rows)}


@router.post("/ingest-news-sentiment")
async def ingest_news_sentiment(body: NewsSentimentBody) -> dict[str, str]:
    """
    Feed OpenClaw/Telegram-derived sentiment for the scalper (or dry-run feeds).
    """
    await publish_openclaw_news_sentiment(
        score=body.score,
        channel_title=body.channel_title,
        excerpt=body.excerpt,
        source=body.source,
        agent_fingerprint=body.agent_fingerprint,
    )
    return {"status": "ok"}
