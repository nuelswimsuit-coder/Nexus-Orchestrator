"""
GET  /api/notifications/status — ChatOps bridge connectivity status.
GET  /api/super-scraper/status — Super-scraper hunting status.
POST /api/panic                — Emergency kill-switch: halts all active
                                 processes, drains the task queue, and sends
                                 an emergency Telegram report.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

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


# ── PANIC / Emergency Kill-Switch ─────────────────────────────────────────────

PANIC_KEY     = "nexus:panic:active"
PANIC_KEY_TTL = 300   # 5 minutes — auto-clears after system settles


class PanicResponse(BaseModel):
    activated:    bool
    message:      str
    activated_at: str
    tasks_drained: int


@router.post(
    "/panic",
    response_model=PanicResponse,
    summary="🚨 Emergency kill-switch — halt all activity",
)
async def trigger_panic(request: Request) -> PanicResponse:
    """
    PANIC / מצב חירום — Emergency kill-switch.

    Actions taken
    -------------
    1. Sets ``nexus:panic:active`` in Redis (all loops check this flag).
    2. Drains the ARQ task queue (cancels pending jobs).
    3. Sends an emergency Telegram report via the notifier.
    4. Returns the count of drained tasks and activation timestamp.

    The flag auto-expires after 5 minutes so the system can recover.
    To clear manually: DELETE the ``nexus:panic:active`` Redis key.
    """
    redis = request.app.state.redis
    now   = datetime.now(timezone.utc).isoformat()

    payload = json.dumps({
        "active":       True,
        "activated_at": now,
        "activated_by": "dashboard",
    })
    await redis.set(PANIC_KEY, payload, ex=PANIC_KEY_TTL)

    # ── Drain ARQ task queue ───────────────────────────────────────────────────
    drained = 0
    try:
        queue_len = await redis.llen("arq:queue:nexus:tasks")
        if queue_len and queue_len > 0:
            await redis.delete("arq:queue:nexus:tasks")
            drained = queue_len
    except Exception as exc:
        log.warning("panic_queue_drain_error", error=str(exc))

    # ── Send emergency Telegram report ────────────────────────────────────────
    tg_token   = os.getenv("TELEGRAM_BOT_TOKEN", "")
    tg_chat_id = os.getenv("TELEGRAM_ADMIN_CHAT_ID", "")
    if tg_token and tg_chat_id:
        try:
            import httpx  # noqa: PLC0415
            emergency_text = (
                "🚨 *NEXUS EMERGENCY SHUTDOWN*\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"⏰ Activated: {now[:19].replace('T', ' ')} UTC\n"
                f"🗑️ Tasks drained: {drained}\n"
                "🛑 All active processes halted\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "⚠️ SYSTEM IS IN PANIC MODE\n"
                "Redis key nexus:panic:active is set.\n"
                "Auto-clears in 5 minutes.\n"
                "Dashboard: http://localhost:3000"
            )
            tg_url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
            async with httpx.AsyncClient(timeout=8.0) as client:
                await client.post(tg_url, json={
                    "chat_id":    tg_chat_id,
                    "text":       emergency_text,
                    "parse_mode": "Markdown",
                })
            log.info("panic_telegram_sent", chat_id=tg_chat_id)
        except Exception as exc:
            log.warning("panic_telegram_error", error=str(exc))

    log.warning(
        "panic_activated",
        drained=drained,
        activated_at=now,
        hint="nexus:panic:active set in Redis. Auto-expires in 300s.",
    )

    return PanicResponse(
        activated=True,
        message=f"🚨 PANIC activated — {drained} task(s) drained. Emergency report sent.",
        activated_at=now,
        tasks_drained=drained,
    )


class PanicResetResponse(BaseModel):
    cleared: bool
    message: str


@router.post(
    "/panic/reset",
    response_model=PanicResetResponse,
    summary="Clear the PANIC flag and resume normal operation",
)
async def reset_panic(request: Request) -> PanicResetResponse:
    """
    Clear ``nexus:panic:active`` from Redis and allow all loops to resume.
    Should be called from the dashboard "Resume" button after a panic event.
    """
    redis = request.app.state.redis
    await redis.delete(PANIC_KEY)
    log.info("panic_cleared", by="dashboard")
    return PanicResetResponse(
        cleared=True,
        message="✅ Panic cleared — system resuming normal operation",
    )
