"""
poly5m.binance_velocity_feed — Sub-second BTCUSDT stream → Redis price velocity.

Connects to the public Binance websocket trade stream, maintains a 60-second
rolling window of prints, and publishes:

  nexus:poly5m:btc_feed
      { price, velocity_pct_60s, updated_at, symbol, samples_60s }

Enable on workers with ``POLY5M_VELOCITY_FEED=1`` (started from ARQ ``startup``).
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

import structlog

from nexus.shared.config import settings

log = structlog.get_logger(__name__)

POLY5M_BTC_FEED_KEY = "nexus:poly5m:btc_feed"
BINANCE_WS_URL = os.getenv(
    "POLY5M_BINANCE_WS",
    "wss://stream.binance.com:9443/ws/btcusdt@trade",
)
VELOCITY_WINDOW_S = float(os.getenv("POLY5M_VELOCITY_WINDOW_S", "60"))
PUBLISH_MIN_INTERVAL_S = float(os.getenv("POLY5M_FEED_PUBLISH_INTERVAL_S", "0.25"))
_SYMBOL = os.getenv("POLY5M_BINANCE_SYMBOL", "BTCUSDT")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def run_binance_velocity_feed(redis: Any, stop_event: asyncio.Event) -> None:
    """
    Loop until ``stop_event`` is set. Requires ``websockets`` package.
    """
    try:
        import websockets  # type: ignore[import-untyped]
    except ImportError:
        log.error(
            "poly5m_velocity_missing_websockets",
            hint="pip install websockets",
        )
        return

    history: deque[tuple[float, float]] = deque()
    last_pub = 0.0
    last_price: float | None = None

    while not stop_event.is_set():
        try:
            async with websockets.connect(
                BINANCE_WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
            ) as ws:
                log.info("poly5m_binance_ws_connected", url=BINANCE_WS_URL)
                async for raw in ws:
                    if stop_event.is_set():
                        break
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    px = msg.get("p")
                    if px is None:
                        continue
                    price = float(px)
                    ts = time.time()
                    last_price = price
                    history.append((ts, price))
                    cutoff = ts - VELOCITY_WINDOW_S
                    while history and history[0][0] < cutoff:
                        history.popleft()

                    vel_pct = 0.0
                    if len(history) >= 2:
                        oldest = history[0][1]
                        if oldest > 0:
                            vel_pct = (price - oldest) / oldest * 100.0

                    if ts - last_pub < PUBLISH_MIN_INTERVAL_S:
                        continue
                    last_pub = ts

                    payload = {
                        "symbol": _SYMBOL,
                        "price": round(price, 2),
                        "velocity_pct_60s": round(vel_pct, 6),
                        "samples_60s": len(history),
                        "window_s": VELOCITY_WINDOW_S,
                        "updated_at": _now_iso(),
                    }
                    try:
                        await redis.set(
                            POLY5M_BTC_FEED_KEY,
                            json.dumps(payload),
                            ex=120,
                        )
                    except Exception as exc:
                        log.debug("poly5m_feed_redis_failed", error=str(exc))
        except asyncio.CancelledError:
            break
        except Exception as exc:
            if stop_event.is_set():
                break
            log.warning("poly5m_binance_ws_error", error=str(exc))
            await asyncio.sleep(min(5.0, 1.0 + (time.time() % 3)))

    log.info("poly5m_binance_ws_stopped", last_price=last_price)


async def _velocity_worker_entry(ctx: dict[str, Any]) -> None:
    redis = ctx.get("redis")
    if redis is None:
        import redis.asyncio as redis_asyncio

        redis = redis_asyncio.from_url(settings.redis_url, decode_responses=True)
        ctx["_poly5m_velocity_redis_owned"] = redis
    stop_event: asyncio.Event = ctx["_poly5m_velocity_stop"]
    try:
        await run_binance_velocity_feed(redis, stop_event)
    finally:
        if ctx.pop("_poly5m_velocity_redis_owned", None) is not None:
            await redis.aclose()


def attach_velocity_feed_to_worker_ctx(ctx: dict[str, Any]) -> None:
    """Call from ARQ worker ``startup`` when POLY5M_VELOCITY_FEED is enabled."""
    if os.getenv("POLY5M_VELOCITY_FEED", "").strip().lower() not in {"1", "true", "yes"}:
        return
    stop = asyncio.Event()
    ctx["_poly5m_velocity_stop"] = stop
    ctx["_poly5m_velocity_task"] = asyncio.create_task(
        _velocity_worker_entry(ctx),
        name="poly5m-binance-velocity",
    )
    log.info("poly5m_velocity_feed_scheduled", key=POLY5M_BTC_FEED_KEY)


async def detach_velocity_feed_from_worker_ctx(ctx: dict[str, Any]) -> None:
    """Call from ARQ worker ``shutdown``."""
    if stop := ctx.pop("_poly5m_velocity_stop", None):
        stop.set()
    if task := ctx.pop("_poly5m_velocity_task", None):
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
