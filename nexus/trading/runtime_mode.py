"""
Runtime trading mode override (Redis).

When ``nexus:control:trading_mode`` is set, workers honour it for prediction
and Polymarket order paths. If the key is absent, behaviour falls back to
``nexus.trading.config.PAPER_TRADING``.
"""

from __future__ import annotations

from typing import Any

from nexus.trading.config import PAPER_TRADING

TRADING_MODE_REDIS_KEY = "nexus:control:trading_mode"


async def effective_paper_trading(redis: Any | None) -> bool:
    """
    Return True if orders should be simulated (paper), False for live CLOB.

    Redis values (case-insensitive): ``paper`` | ``sim`` | ``simulation`` → paper;
    ``live`` | ``real`` | ``production`` | ``race`` → live.
    """
    if redis is None:
        return PAPER_TRADING
    try:
        raw = await redis.get(TRADING_MODE_REDIS_KEY)
    except Exception:
        return PAPER_TRADING
    if raw is None:
        return PAPER_TRADING
    v = raw.decode() if isinstance(raw, bytes) else str(raw)
    v = v.strip().lower()
    if v in ("live", "real", "production", "race", "master-race"):
        return False
    if v in ("paper", "sim", "simulation", "virtual"):
        return True
    return PAPER_TRADING
