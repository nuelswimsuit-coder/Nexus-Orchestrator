"""
Polymarket Bot Service — Nexus Poly Trader (master side)

Periodically dispatches ``trading.polymarket_bot_tick`` to a Linux worker.  Each tick
uses the Polymarket CLOB client (``py-clob-client`` on PyPI — Polymarket's official
Python SDK), samples BTC/USDT via Binance websocket (one trade print per tick when
``websockets`` is installed), refreshes Gamma for *Will Bitcoin hit $X by …?*,
applies entry/stop rules, and writes PnL to Redis for the dashboard.

Enable with ``POLYMARKET_BOT_ENABLED=1``.

FORCE_BUY override
------------------
Publish the string ``FORCE_BUY`` to the Redis key ``nexus:polymarket:command``
(or set it as a plain string value) to bypass the "awaiting intent stream" /
startup-delay block and fire an immediate trade tick regardless of the
``POLYMARKET_BOT_ENABLED`` flag.

    redis-cli SET nexus:polymarket:command FORCE_BUY
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import structlog

from nexus.core.dispatcher import CapabilityNotAvailableError
from nexus.shared.config import settings
from nexus.shared.schemas import TaskPayload, WorkerCapability

log = structlog.get_logger(__name__)

_TICK_INTERVAL_S = float(os.environ.get("POLYMARKET_BOT_TICK_INTERVAL_S", "20"))
_STARTUP_DELAY_S = float(os.environ.get("POLYMARKET_BOT_STARTUP_DELAY_S", "10"))
_HEARTBEAT_INTERVAL_S = 10.0

# Redis key polled for operator override commands
_COMMAND_KEY = "nexus:polymarket:command"
_FORCE_BUY_CMD = "FORCE_BUY"


class PolymarketBotService:
    """
    Dispatches short ARQ jobs on a fixed cadence so the Linux worker stays within
    default ``TASK_DEFAULT_TIMEOUT`` while still monitoring via websocket each tick.

    A background coroutine polls ``nexus:polymarket:command`` in Redis every second.
    When ``FORCE_BUY`` is found the "awaiting intent stream" / disabled guard is
    bypassed and an immediate tick is dispatched.
    """

    def __init__(self, dispatcher: Any) -> None:
        self._dispatcher = dispatcher
        self._running = False
        self._force_buy_event: asyncio.Event | None = None

    def stop(self) -> None:
        self._running = False

    async def _redis_command_watcher(self) -> None:
        """Poll ``nexus:polymarket:command`` for FORCE_BUY and signal the main loop."""
        try:
            from nexus.shared.redis_util import connect_async_redis_with_fallback  # noqa: PLC0415

            redis_url = getattr(settings, "redis_url", None) or os.environ.get(
                "REDIS_URL", "redis://127.0.0.1:6379/0"
            )
            r = await connect_async_redis_with_fallback(str(redis_url))
        except Exception as exc:
            log.warning("polymarket_force_buy_watcher_redis_unavailable", error=str(exc))
            return

        while self._running:
            try:
                cmd = await r.get(_COMMAND_KEY)
                if cmd and str(cmd).strip().upper() == _FORCE_BUY_CMD:
                    log.warning(
                        "polymarket_force_buy_received",
                        hint="Bypassing awaiting-intent-stream block — firing immediate tick",
                    )
                    print(
                        "\033[1;31m⚡ [FORCE_BUY] Command received — bypassing intent stream, executing trade NOW\033[0m",
                        flush=True,
                    )
                    # Consume the command so it does not re-trigger on the next poll
                    await r.delete(_COMMAND_KEY)
                    if self._force_buy_event is not None:
                        self._force_buy_event.set()
            except Exception:
                pass
            await asyncio.sleep(1.0)

        try:
            await r.aclose()
        except Exception:
            pass

    async def run_loop(self) -> None:
        self._running = True
        self._force_buy_event = asyncio.Event()

        raw = os.environ.get("POLYMARKET_BOT_ENABLED")
        if raw is not None and str(raw).strip() != "":
            enabled = str(raw).strip().lower() in ("1", "true", "yes", "on")
        else:
            enabled = settings.polymarket_bot_enabled

        # Start the FORCE_BUY watcher unconditionally so the override works even
        # when the bot is otherwise disabled / awaiting intent stream.
        asyncio.get_event_loop().create_task(self._redis_command_watcher())

        if not enabled:
            log.info(
                "polymarket_bot_service_disabled",
                hint="set POLYMARKET_BOT_ENABLED=1 or send FORCE_BUY to nexus:polymarket:command",
            )
            # Block here but remain responsive to FORCE_BUY overrides
            while self._running:
                assert self._force_buy_event is not None
                try:
                    await asyncio.wait_for(
                        asyncio.shield(self._force_buy_event.wait()), timeout=60.0
                    )
                except asyncio.TimeoutError:
                    continue
                if self._force_buy_event.is_set():
                    self._force_buy_event.clear()
                    log.info("polymarket_force_buy_override_while_disabled")
                    await self._dispatch_tick()
            return

        log.info(
            "polymarket_bot_service_started",
            tick_interval_s=_TICK_INTERVAL_S,
        )

        # Startup delay — but skip it immediately if FORCE_BUY arrives first
        assert self._force_buy_event is not None
        try:
            await asyncio.wait_for(
                asyncio.shield(self._force_buy_event.wait()), timeout=_STARTUP_DELAY_S
            )
        except asyncio.TimeoutError:
            pass

        if self._force_buy_event.is_set():
            self._force_buy_event.clear()
            log.info("polymarket_force_buy_skipped_startup_delay")
            await self._dispatch_tick()

        asyncio.get_event_loop().create_task(self._heartbeat_loop())

        while self._running:
            # Wait for either the normal tick interval or a FORCE_BUY signal
            assert self._force_buy_event is not None
            try:
                await asyncio.wait_for(
                    asyncio.shield(self._force_buy_event.wait()), timeout=_TICK_INTERVAL_S
                )
            except asyncio.TimeoutError:
                pass

            if self._force_buy_event.is_set():
                self._force_buy_event.clear()
                print(
                    "\033[1;31m⚡ [FORCE_BUY] Overriding tick cadence — dispatching immediate trade\033[0m",
                    flush=True,
                )

            await self._dispatch_tick()

    async def _heartbeat_loop(self) -> None:
        """Print a liveness message every 10 seconds so the operator can confirm the bot is alive."""
        while self._running:
            print(
                "\033[1;34m🔍 [SCANNING] Checking Polymarket for BTC opportunities...\033[0m",
                flush=True,
            )
            await asyncio.sleep(_HEARTBEAT_INTERVAL_S)

    async def _dispatch_tick(self) -> None:
        params = {
            # AGGRESSIVE TEST MODE — low threshold, small liquidity filter, safe bet size
            "max_bet_usd":       float(os.environ.get("POLYMARKET_BOT_MAX_BET_USD", "5")),
            "yes_ceiling":       float(os.environ.get("POLYMARKET_BOT_YES_CEILING", "0.95")),
            "proximity_pct":     float(os.environ.get("POLYMARKET_BOT_PROXIMITY", "0.05")),
            "stop_loss_pct":     float(os.environ.get("POLYMARKET_BOT_STOP_LOSS", "0.20")),
            "min_liquidity_usd": float(os.environ.get("POLYMARKET_BOT_MIN_LIQUIDITY_USD", "10")),
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
