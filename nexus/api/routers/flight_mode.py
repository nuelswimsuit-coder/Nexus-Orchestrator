"""
Flight Mode API — System status and manual recovery endpoints.

GET  /api/flight-mode/status
    Returns the current flight mode state and live Stability Score.
    Polled by the React dashboard overlay every 3 seconds.

POST /api/flight-mode/recover
    Operator-triggered manual recovery.  Clears Autonomous Flight Mode,
    restores normal trading, and removes the MINIMAL_CORE_MODE flag.
    Requires no authentication beyond network access (protect via VPN).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import structlog
from fastapi import APIRouter
from pydantic import BaseModel

from nexus.api.dependencies import RedisDep
from nexus.master.flight_mode import FlightModeEngine
from nexus.master.sentinel import STABILITY_SCORE_KEY

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/flight-mode", tags=["flight-mode"])


# ── Request / response models ──────────────────────────────────────────────────

class RecoverRequest(BaseModel):
    operator: str = "dashboard"


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get(
    "/status",
    summary="Current flight mode state + Stability Score",
)
async def get_flight_mode_status(redis: RedisDep) -> dict:
    """
    Return a combined payload with:
    - flight_mode  : { active, triggered_at, score, reason } or { active: false }
    - stability    : { score, threshold, critical, updated_at }
    - timestamp    : ISO-8601 UTC

    The React overlay polls this endpoint every 3 s to decide whether to
    show the full-screen "מצב טיסה" overlay.
    """
    engine = FlightModeEngine(redis=redis)
    state  = await engine.get_state()

    stability_raw = await redis.get(STABILITY_SCORE_KEY)
    stability: dict = (
        json.loads(stability_raw)
        if stability_raw
        else {"score": 100.0, "threshold": 40, "critical": False, "updated_at": None}
    )

    return {
        "flight_mode": state or {"active": False},
        "stability":   stability,
        "timestamp":   datetime.now(timezone.utc).isoformat(),
    }


@router.post(
    "/recover",
    summary="Manual system recovery — clears Autonomous Flight Mode",
)
async def recover_system(body: RecoverRequest, redis: RedisDep) -> dict:
    """
    Operator-triggered recovery.

    Clears the flight mode lock, restores real-money trading capability,
    removes the MINIMAL_CORE_MODE flag, and re-enables GOD MODE.

    This is the backend for the dashboard "System Recovery / שחזור מערכת"
    button and the Telegram `system_recovery` callback.
    """
    engine = FlightModeEngine(redis=redis)

    is_currently_active = await engine.is_active()
    if not is_currently_active:
        return {
            "status":  "already_recovered",
            "message": "Flight Mode was not active",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    result = await engine.deactivate(operator=body.operator)
    log.info("flight_mode_manual_recovery_api", operator=body.operator)
    return result
