"""
Polymarket wallet access and real-balance safety brake.

Reads signing material only from environment variables (never logs secrets).
If the live USDC balance falls 30% or more below the first observed baseline
for this deployment, trading is halted and the operator is notified on Telegram.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import httpx
import structlog

log = structlog.get_logger(__name__)

# Same env names as PolymarketClient — single source of truth for operators.
ENV_PRIVATE_KEY = "POLYMARKET_RELAYER_KEY"
ENV_PRIVATE_KEY_ALT = "NEXUS_POLY_PRIVATE_KEY"
ENV_POLY_LEGACY = "POLY_PRIVATE_KEY"
ENV_FUNDER = "POLYMARKET_SIGNER_ADDRESS"

REDIS_BASELINE_KEY = "nexus:scalper:real_balance_baseline_usd"
REDIS_BRAKE_KEY = "nexus:scalper:safety_brake_active"
DRAWDOWN_HALT_FRACTION = 0.30

PANIC_STATE_KEY = "SYSTEM_STATE:PANIC"
PANIC_META_KEY = "SYSTEM_STATE:PANIC_META"


def get_polymarket_private_key() -> str:
    """Return the raw 0x-prefixed private key, or empty string if unset."""
    return (
        (os.getenv(ENV_PRIVATE_KEY) or "").strip()
        or (os.getenv(ENV_PRIVATE_KEY_ALT) or "").strip()
        or (os.getenv(ENV_POLY_LEGACY) or "").strip()
    )


def get_polymarket_funder_address() -> str:
    return (os.getenv(ENV_FUNDER) or "").strip()


def require_signing_material() -> tuple[str, str]:
    """
    Return (private_key, funder). Raises ValueError if either is missing.
    """
    key = get_polymarket_private_key()
    funder = get_polymarket_funder_address()
    if not key or not funder:
        raise ValueError(
            f"{ENV_PRIVATE_KEY} and {ENV_FUNDER} must be set for live wallet access"
        )
    return key, funder


async def _send_telegram_alert(text: str) -> None:
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.getenv("TELEGRAM_ADMIN_CHAT_ID") or "").strip()
    if not token or not chat_id:
        log.warning("wallet_manager_telegram_skipped", reason="token_or_chat_missing")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            await client.post(url, json={"chat_id": chat_id, "text": text})
        log.info("wallet_manager_telegram_sent")
    except Exception as exc:
        log.error("wallet_manager_telegram_error", error=str(exc))


async def evaluate_real_balance_safety_brake(
    redis: Any,
    balance_usd: float,
) -> bool:
    """
    Persist first-seen balance as baseline; if balance drops by >=30% from
    baseline, set halt flag and alert Telegram.

    Returns True if trading must stop (brake active or newly triggered).
    """
    try:
        if await redis.get(REDIS_BRAKE_KEY):
            return True
    except Exception:
        pass

    baseline_raw = None
    try:
        baseline_raw = await redis.get(REDIS_BASELINE_KEY)
    except Exception as exc:
        log.debug("wallet_manager_baseline_read_failed", error=str(exc))

    if not baseline_raw:
        try:
            await redis.set(REDIS_BASELINE_KEY, f"{balance_usd:.6f}")
        except Exception:
            pass
        log.info(
            "wallet_manager_baseline_initialized",
            baseline_usd=round(balance_usd, 2),
        )
        return False

    try:
        baseline = float(baseline_raw)
    except ValueError:
        baseline = balance_usd

    if baseline <= 0:
        return False

    floor = baseline * (1.0 - DRAWDOWN_HALT_FRACTION)
    if balance_usd < floor:
        msg = (
            "🚨 NEXUS — SYSTEM COMPROMISED / SAFETY BRAKE\n"
            f"Live balance: ${balance_usd:.2f}\n"
            f"Baseline: ${baseline:.2f}\n"
            f"30% drawdown floor: ${floor:.2f}\n"
            "All automated trading halted. Assume credentials may be exposed — "
            "rotate Polymarket signing keys in your vault and revoke old API access.\n"
            "Panic flag set; workers honor SYSTEM_STATE:PANIC."
        )
        try:
            await redis.set(REDIS_BRAKE_KEY, "1")
            from nexus.trading.config import PREDICTION_MANUAL_HALT_KEY

            await redis.set(
                PREDICTION_MANUAL_HALT_KEY,
                "safety_brake_30pct",
            )
            await redis.set(PANIC_STATE_KEY, "true")
            await redis.set(
                PANIC_META_KEY,
                json.dumps(
                    {
                        "reason": "drawdown_30pct_wallet_brake",
                        "balance_usd": round(balance_usd, 2),
                        "baseline_usd": round(baseline, 2),
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }
                ),
            )
        except Exception as exc:
            log.error("wallet_manager_brake_redis_failed", error=str(exc))
        await _send_telegram_alert(msg)
        log.critical(
            "wallet_manager_safety_brake_triggered",
            balance_usd=round(balance_usd, 2),
            baseline_usd=round(baseline, 2),
        )
        return True

    return False
