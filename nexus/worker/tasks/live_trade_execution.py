from __future__ import annotations

from typing import Any

import structlog

from nexus.trading.config import PAPER_TRADING_AMOUNT_USD
from nexus.trading.polymarket_client import KILL_SWITCH_BALANCE_USD, PolymarketClient, place_order

log = structlog.get_logger(__name__)


async def get_live_balance_usd() -> float:
    """
    Fetch the currently available USDC balance from Polymarket.
    """
    client = PolymarketClient()
    return await client.get_balance_usdc()


async def execute_live_trade(
    *,
    redis: Any,
    signal: str,
    binance_data: dict[str, Any],
    poly_data: dict[str, Any],
) -> dict[str, Any]:
    """
    Execute a live trade only after a strict balance pre-check.
    """
    balance_usd = await get_live_balance_usd()
    min_required = max(KILL_SWITCH_BALANCE_USD, PAPER_TRADING_AMOUNT_USD)

    if balance_usd < min_required:
        log.error(
            "live_trade_execution_blocked_low_balance",
            signal=signal,
            balance_usd=round(balance_usd, 2),
            min_required_usd=round(min_required, 2),
        )
        return {
            "executed": False,
            "status": "blocked_low_balance",
            "balance_usd": round(balance_usd, 2),
            "min_required_usd": round(min_required, 2),
        }

    result = await place_order(
        signal=signal,
        binance_data=binance_data,
        poly_data=poly_data,
        redis=redis,
    )
    log.info(
        "live_trade_execution",
        signal=signal,
        balance_usd=round(balance_usd, 2),
        order_id=result.get("order_id"),
        status=result.get("status"),
    )
    return {
        "executed": True,
        "status": result.get("status", "success"),
        "balance_usd": round(balance_usd, 2),
        "order_id": result.get("order_id"),
    }
