"""
GET  /api/notifications/status — ChatOps bridge connectivity status.
GET  /api/super-scraper/status — Super-scraper hunting status.

Note: Panic/kill-switch endpoints have been consolidated into
/api/system/panic (system.py) which uses the canonical SYSTEM_STATE:PANIC
Redis key and nexus:system:control pub/sub channel.
"""

from __future__ import annotations

import json
import os

import structlog
from fastapi import APIRouter, Request
from pydantic import BaseModel

from nexus.worker.tasks.super_scraper import SUPER_CANDIDATES_KEY, SUPER_STATUS_KEY

log = structlog.get_logger(__name__)

router = APIRouter(tags=["notifications"])


# ── ChatOps status ─────────────────────────────────────────────────────────────

class ChatOpsProviderStatus(BaseModel):
    name: str
    connected: bool
    mode: str     # "mock" | "twilio" | "evolution" | "live"
    detail: str


class ChatOpsStatusResponse(BaseModel):
    providers: list[ChatOpsProviderStatus]
    any_connected: bool


@router.get(
    "/notifications/status",
    response_model=ChatOpsStatusResponse,
    summary="ChatOps bridge connectivity (WhatsApp + Telegram)",
)
async def get_notifications_status() -> ChatOpsStatusResponse:
    """
    Return the connection status of all registered notification providers.
    Used by the dashboard header to show the ChatOps status indicators.

    Detection is based on environment variables — no live ping is performed.
    """
    providers: list[ChatOpsProviderStatus] = []

    # ── WhatsApp ───────────────────────────────────────────────────────────────
    wa_mode = os.getenv("WHATSAPP_PROVIDER", "mock").lower()
    wa_to   = os.getenv("WHATSAPP_TO_NUMBER", "+0000000000")
    wa_connected = wa_mode in ("twilio", "evolution")

    if wa_mode == "twilio":
        wa_detail = "Twilio" if os.getenv("TWILIO_ACCOUNT_SID") else "Twilio (missing credentials)"
        wa_connected = bool(os.getenv("TWILIO_ACCOUNT_SID"))
    elif wa_mode == "evolution":
        wa_detail = "Evolution API" if os.getenv("EVOLUTION_API_URL") else "Evolution (missing URL)"
        wa_connected = bool(os.getenv("EVOLUTION_API_URL"))
    else:
        wa_detail = f"Mock (logging only) → {wa_to}"

    providers.append(ChatOpsProviderStatus(
        name="whatsapp",
        connected=wa_connected,
        mode=wa_mode,
        detail=wa_detail,
    ))

    # ── Telegram ───────────────────────────────────────────────────────────────
    tg_token   = os.getenv("TELEGRAM_BOT_TOKEN", "")
    tg_chat_id = os.getenv("TELEGRAM_ADMIN_CHAT_ID", "")
    tg_connected = bool(tg_token and tg_chat_id)

    providers.append(ChatOpsProviderStatus(
        name="telegram",
        connected=tg_connected,
        mode="live" if tg_connected else "unconfigured",
        detail=(
            f"Bot configured → chat {tg_chat_id}"
            if tg_connected
            else "Set TELEGRAM_BOT_TOKEN + TELEGRAM_ADMIN_CHAT_ID in .env"
        ),
    ))

    return ChatOpsStatusResponse(
        providers=providers,
        any_connected=any(p.connected for p in providers),
    )


# ── Super-scraper hunting status ───────────────────────────────────────────────

class SuperScraperStatusResponse(BaseModel):
    # "idle" | "hunting" | "discovering" | "awaiting_approval" | "postponed" | "completed"
    status: str
    detail: str
    updated_at: str
    candidates_pending: int


@router.get(
    "/super-scraper/status",
    response_model=SuperScraperStatusResponse,
    summary="Super-scraper hunting status",
)
async def get_super_scraper_status(request: Request) -> SuperScraperStatusResponse:
    """
    Return the current state of the Strategic Super-Scraper.
    Used by the Operational Intelligence panel's Hunting Status card.
    """
    redis = request.app.state.redis

    raw_status = await redis.get(SUPER_STATUS_KEY)
    raw_candidates = await redis.get(SUPER_CANDIDATES_KEY)

    status_str = "idle"
    detail = "No hunt in progress"
    updated_at = ""

    if raw_status:
        try:
            d = json.loads(raw_status)
            status_str = d.get("status", "idle")
            detail     = d.get("detail", "")
            updated_at = d.get("updated_at", "")
        except Exception:
            pass

    candidates_count = 0
    if raw_candidates:
        try:
            candidates = json.loads(raw_candidates)
            if isinstance(candidates, list):
                for c in candidates:
                    candidates_count += len(c.get("groups", []))
        except Exception:
            pass

    return SuperScraperStatusResponse(
        status=status_str,
        detail=detail,
        updated_at=updated_at,
        candidates_pending=candidates_count,
    )
