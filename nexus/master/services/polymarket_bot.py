"""
Polymarket Bot Service — Nexus Poly Trader (master side)

Periodically dispatches ``trading.polymarket_bot_tick`` to a Linux worker.  Each tick
uses the Polymarket CLOB client (``py-clob-client`` on PyPI — Polymarket’s official
Python SDK), samples BTC/USDT via Binance websocket (one trade print per tick when
``websockets`` is installed), refreshes Gamma for *Will Bitcoin hit $X by …?*,
applies entry/stop rules, and writes PnL to Redis for the dashboard.

Enable with ``POLYMARKET_BOT_ENABLED=1``.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import structlog

from nexus.master.dispatcher import CapabilityNotAvailableError
from nexus.shared.schemas import TaskPayload, WorkerCapability

log = structlog.get_logger(__name__)

_TICK_INTERVAL_S = float(os.environ.get("POLYMARKET_BOT_TICK_INTERVAL_S", "20"))
_STARTUP_DELAY_S = float(os.environ.get("POLYMARKET_BOT_STARTUP_DELAY_S", "10"))


class PolymarketBotService:
    """
    Dispatches short ARQ jobs on a fixed cadence so the Linux worker stays within
    default ``TASK_DEFAULT_TIMEOUT`` while still monitoring via websocket each tick.
    """

    def __init__(self, dispatcher: Any) -> None:
        self._dispatcher = dispatcher
        self._running = False

    def stop(self) -> None:
        self._running = False

    async def run_loop(self) -> None:
        self._running = True
        enabled = os.environ.get("POLYMARKET_BOT_ENABLED", "").lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        if not enabled:
            log.info("polymarket_bot_service_disabled", hint="set POLYMARKET_BOT_ENABLED=1")
            while self._running:
                await asyncio.sleep(3600)
            return

        log.info(
            "polymarket_bot_service_started",
            tick_interval_s=_TICK_INTERVAL_S,
        )
        await asyncio.sleep(_STARTUP_DELAY_S)

        while self._running:
            await self._dispatch_tick()
            await asyncio.sleep(_TICK_INTERVAL_S)

    async def _dispatch_tick(self) -> None:
        params = {
            "max_bet_usd":    float(os.environ.get("POLYMARKET_BOT_MAX_BET_USD", "10")),
            "yes_ceiling":    float(os.environ.get("POLYMARKET_BOT_YES_CEILING", "0.40")),
            "proximity_pct":  float(os.environ.get("POLYMARKET_BOT_PROXIMITY", "0.005")),
            "stop_loss_pct":  float(os.environ.get("POLYMARKET_BOT_STOP_LOSS", "0.20")),
        }

        task = TaskPayload(
            task_type="trading.polymarket_bot_tick",
            parameters=params,
            project_id="nexus-poly-trader",
            priority=4,
            required_capabilities=[WorkerCapability.LINUX],
        )

        try:
            job_id = await self._dispatcher.dispatch(task)
            log.debug("polymarket_bot_tick_dispatched", job_id=job_id)
        except CapabilityNotAvailableError as exc:
            log.warning("polymarket_bot_no_linux_worker", error=str(exc))
        except Exception as exc:
            log.error("polymarket_bot_dispatch_error", error=str(exc))
