"""
Polymarket BTC strike bot — Linux worker **tick** task.

Each ``trading.polymarket_bot_tick`` run completes within one ARQ job (fits default
``TASK_DEFAULT_TIMEOUT``).  The master dispatches ticks on an interval.  Per tick:

- BTC/USDT: one Binance **websocket** trade message when ``websockets`` is installed
  (otherwise REST), written to Redis.
- Gamma: resolve *Will Bitcoin hit $X by …?* (highest-volume match).
- Buy YES when spot is within 0.5 % of strike and YES < $0.40 (max $10).
- Stop-loss: if YES drops 20 % below entry, place a SELL.

PnL + status are written to Redis for the dashboard.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any

import httpx
import structlog

from nexus.trading.poly_bot_state import (
    BTC_TTL_S,
    PNL_TTL_S,
    POLY_BOT_BTC_KEY,
    POLY_BOT_OPEN_POS_KEY,
    POLY_BOT_PNL_KEY,
    POLY_BOT_STATUS_KEY,
    STATUS_TTL_S,
)
from nexus.trading.polymarket_client import PolymarketClient, TradingHalted
from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

POLYMARKET_GAMMA_URL = "https://gamma-api.polymarket.com/markets"
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
BINANCE_REST_URL = "https://api.binance.com/api/v3/ticker/price"

TICK_LOCK_KEY = "nexus:poly:tick_lock"
TICK_LOCK_TTL_S = 55

HIT_Q_RE = re.compile(r"(?i)will bitcoin hit")
STRIKE_RE = re.compile(r"\$\s*([0-9,]+(?:\.[0-9]+)?)")


def _is_hit_market_question(q: str) -> bool:
    return bool(HIT_Q_RE.search(q or ""))


def _parse_strike_usd(question: str) -> float | None:
    m = STRIKE_RE.search(question or "")
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _parse_gamma_market(market: dict[str, Any]) -> dict[str, Any] | None:
    q = market.get("question") or ""
    if not _is_hit_market_question(q):
        return None
    strike = _parse_strike_usd(q)
    if strike is None or strike <= 0:
        return None

    raw_prices = market.get("outcomePrices", '["0.5","0.5"]')
    if isinstance(raw_prices, str):
        try:
            raw_prices = json.loads(raw_prices)
        except Exception:
            raw_prices = ["0.5", "0.5"]
    yes_price = float(raw_prices[0]) if raw_prices else 0.5
    no_price = float(raw_prices[1]) if len(raw_prices) > 1 else 0.5

    raw_token_ids = market.get("clobTokenIds", "[]")
    if isinstance(raw_token_ids, str):
        try:
            raw_token_ids = json.loads(raw_token_ids)
        except Exception:
            raw_token_ids = []
    clob_token_ids: list[str] = raw_token_ids if isinstance(raw_token_ids, list) else []

    if not clob_token_ids:
        return None

    return {
        "market_question": q,
        "strike_usd": strike,
        "yes_price": round(yes_price, 4),
        "no_price": round(no_price, 4),
        "market_id": str(market.get("id", "")),
        "clob_token_ids": clob_token_ids,
    }


async def _fetch_hit_market(client: httpx.AsyncClient) -> dict[str, Any] | None:
    params = {
        "q": "Will Bitcoin hit",
        "active": "true",
        "closed": "false",
        "order": "volume",
        "ascending": "false",
        "limit": "20",
    }
    res = await client.get(POLYMARKET_GAMMA_URL, params=params, timeout=15.0)
    res.raise_for_status()
    markets = res.json()
    if not isinstance(markets, list):
        return None
    for m in markets:
        parsed = _parse_gamma_market(m)
        if parsed:
            return parsed
    return None


async def _btc_price_websocket_once() -> float | None:
    try:
        import websockets
    except ImportError:
        return None
    try:
        async with websockets.connect(
            BINANCE_WS_URL,
            ping_interval=20,
            ping_timeout=15,
            close_timeout=4,
        ) as ws:
            raw = await asyncio.wait_for(ws.recv(), timeout=12.0)
            msg = json.loads(raw)
            p = msg.get("p")
            if p is not None:
                return float(p)
    except Exception as exc:
        log.debug("polymarket_bot_ws_once_failed", error=str(exc))
    return None


async def _btc_price_rest(client: httpx.AsyncClient) -> float | None:
    try:
        r = await client.get(BINANCE_REST_URL, params={"symbol": "BTCUSDT"}, timeout=10.0)
        r.raise_for_status()
        return float(r.json()["price"])
    except Exception as exc:
        log.warning("polymarket_bot_binance_rest_failed", error=str(exc))
        return None


async def _read_realized_pnl(redis: Any) -> float:
    raw = await redis.get("nexus:poly:realized_pnl")
    if raw is None:
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return 0.0


async def _write_realized_pnl(redis: Any, value: float) -> None:
    await redis.set("nexus:poly:realized_pnl", f"{value:.6f}")


async def _write_pnl(
    redis: Any,
    *,
    btc_spot: float | None,
    market: dict[str, Any] | None,
    open_pos: dict[str, Any] | None,
    realized_pnl: float,
    last_action: str,
    detail: str = "",
) -> None:
    unrealized = 0.0
    if open_pos and market:
        entry = float(open_pos.get("entry_price", 0))
        sh = float(open_pos.get("shares", 0))
        mark = float(market.get("yes_price", 0))
        unrealized = (mark - entry) * sh

    active_token_id = open_pos.get("token_id") if open_pos else None

    payload = {
        "realized_pnl_usd": round(realized_pnl, 4),
        "unrealized_pnl_usd": round(unrealized, 4),
        "total_pnl_usd": round(realized_pnl + unrealized, 4),
        "btc_spot": btc_spot,
        "target_strike": market.get("strike_usd") if market else None,
        "yes_price": market.get("yes_price") if market else None,
        "market_question": market.get("market_question") if market else None,
        "open_position": open_pos,
        "token_id": active_token_id,
        "yes_token_id": active_token_id,
        "within_target_band": False,
        "last_action": last_action,
        "detail": detail,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if btc_spot and market and market.get("strike_usd"):
        strike = float(market["strike_usd"])
        if strike > 0:
            payload["within_target_band"] = abs(btc_spot - strike) / strike <= 0.005

    await redis.set(POLY_BOT_PNL_KEY, json.dumps(payload), ex=PNL_TTL_S)


async def _write_status(
    redis: Any,
    *,
    stage: str,
    detail: str,
    active: bool = True,
) -> None:
    node_id = os.getenv("NODE_ID", "worker")
    body = {
        "active": active,
        "stage": stage,
        "detail": detail,
        "node_id": node_id,
        "last_heartbeat": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    await redis.set(POLY_BOT_STATUS_KEY, json.dumps(body), ex=STATUS_TTL_S)


async def _write_clob_heartbeat(redis: Any) -> None:
    """Write CLOB heartbeat keys every tick so the dashboard shows ACTIVE."""
    ts = datetime.now(timezone.utc).isoformat()
    try:
        await redis.set("nexus:clob:heartbeat", ts, ex=STATUS_TTL_S)
        await redis.set("nexus:clob:status", "ACTIVE", ex=STATUS_TTL_S)
    except Exception as exc:
        log.debug("clob_heartbeat_write_failed", error=str(exc))


async def _run_tick(redis: Any, params: dict[str, Any]) -> dict[str, Any]:
    max_bet = float(params.get("max_bet_usd", 10.0))
    yes_ceiling = float(params.get("yes_ceiling", 0.40))
    proximity = float(params.get("proximity_pct", 0.005))
    stop_loss_pct = float(params.get("stop_loss_pct", 0.20))

    await _write_status(redis, stage="polymarket_bot_tick", detail="tick start")

    btc = await _btc_price_websocket_once()
    async with httpx.AsyncClient(timeout=15.0) as http_client:
        if btc is None:
            btc = await _btc_price_rest(http_client)
        market: dict[str, Any] | None = None
        try:
            market = await _fetch_hit_market(http_client)
        except Exception as exc:
            log.warning("polymarket_bot_gamma_error", error=str(exc))

    if btc is not None:
        await redis.set(
            POLY_BOT_BTC_KEY,
            json.dumps({
                "price": btc,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }),
            ex=BTC_TTL_S,
        )

    client_pm = PolymarketClient()
    realized = await _read_realized_pnl(redis)
    open_pos_raw = await redis.get(POLY_BOT_OPEN_POS_KEY)
    open_pos: dict[str, Any] | None = json.loads(open_pos_raw) if open_pos_raw else None

    last_action = "idle"
    detail = ""

    if open_pos and market:
        entry = float(open_pos["entry_price"])
        yes_px = float(market["yes_price"])
        floor_px = entry * (1.0 - stop_loss_pct)
        if yes_px <= floor_px:
            token_id = str(open_pos["token_id"])
            shares = float(open_pos["shares"])
            mq = str(open_pos.get("market_question", ""))
            tick = client_pm.get_tick_size(token_id)
            sell_res = None
            try:
                sell_res = await client_pm.place_sell_async(
                    token_id=token_id,
                    price=max(0.01, yes_px),
                    size=shares,
                    market_question=mq,
                    tick_size=tick,
                    redis=redis,
                )
            except asyncio.TimeoutError as exc:
                detail = f"stop_loss_timeout:{exc}"
                last_action = "stop_loss_timeout"
                log.error("polymarket_bot_stop_loss_timeout", error=str(exc))
            if sell_res is not None:
                if sell_res.success:
                    pnl_leg = (yes_px - entry) * shares
                    realized += pnl_leg
                    await _write_realized_pnl(redis, realized)
                    open_pos = None
                    await redis.delete(POLY_BOT_OPEN_POS_KEY)
                    last_action = "stop_loss_sell"
                    detail = sell_res.to_log_text()
                else:
                    last_action = "stop_loss_failed"
                    detail = sell_res.error or "sell_failed"

    if (
        open_pos is None
        and btc is not None
        and market
        and market.get("strike_usd")
    ):
        strike = float(market["strike_usd"])
        yes_px = float(market["yes_price"])
        near = strike > 0 and abs(btc - strike) / strike <= proximity
        if near and yes_px < yes_ceiling:
            token_id = str(market["clob_token_ids"][0])
            mq = str(market["market_question"])
            tick = client_pm.get_tick_size(token_id)
            try:
                buy_res = await client_pm.place_order_async(
                    token_id=token_id,
                    side="YES",
                    price=yes_px,
                    market_question=mq,
                    budget_usd=max_bet,
                    tick_size=tick,
                    redis=redis,
                )
            except (TradingHalted, asyncio.TimeoutError) as exc:
                last_action = "buy_blocked"
                detail = str(exc)
                log.warning("polymarket_bot_buy_blocked", error=str(exc))
            else:
                if buy_res.success:
                    open_pos = {
                        "token_id": token_id,
                        "entry_price": yes_px,
                        "shares": float(buy_res.shares),
                        "market_id": market.get("market_id"),
                        "market_question": mq,
                    }
                    await redis.set(POLY_BOT_OPEN_POS_KEY, json.dumps(open_pos))
                    last_action = "buy_yes"
                    detail = buy_res.to_log_text()
                else:
                    last_action = "buy_failed"
                    detail = buy_res.error or "buy_failed"

    await _write_pnl(
        redis,
        btc_spot=btc,
        market=market,
        open_pos=open_pos,
        realized_pnl=realized,
        last_action=last_action,
        detail=detail,
    )
    await _write_status(redis, stage="polymarket_bot_tick", detail="tick done")

    return {
        "status": "ok",
        "btc_spot": btc,
        "last_action": last_action,
        "detail": detail,
        "worker_id": os.getenv("NODE_ID", "worker"),
        "tick_at": datetime.now(timezone.utc).isoformat(),
    }


@registry.register("trading.polymarket_bot_tick")
async def polymarket_bot_tick(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Single scheduling tick — safe under default ARQ ``job_timeout`` (300 s).

    Parameters mirror the master service env-driven payload (max_bet_usd, etc.).
    """
    redis = parameters.get("__redis__")
    if redis is None:
        return {"status": "error", "detail": "no_redis"}

    worker_id = os.getenv("NODE_ID", "worker")
    # Always emit a heartbeat — independent of private-key / trading state.
    await _write_clob_heartbeat(redis)

    got = await redis.set(TICK_LOCK_KEY, worker_id, nx=True, ex=TICK_LOCK_TTL_S)
    if not got:
        holder = await redis.get(TICK_LOCK_KEY)
        # Still refresh heartbeat even when the lock is held by another worker.
        await _write_clob_heartbeat(redis)
        return {"status": "skipped", "detail": "tick_lock_held", "lock_holder": holder}

    t0 = time.monotonic()
    try:
        result = await _run_tick(redis, parameters)
        # Refresh heartbeat after a successful tick.
        await _write_clob_heartbeat(redis)
        return result
    finally:
        try:
            await redis.delete(TICK_LOCK_KEY)
        except Exception:
            pass
        log.info(
            "polymarket_bot_tick_complete",
            worker_id=worker_id,
            duration_s=round(time.monotonic() - t0, 3),
        )


# Back-compat alias: long session name → same tick (one shot)
@registry.register("trading.polymarket_bot_session")
async def polymarket_bot_session(parameters: dict[str, Any]) -> dict[str, Any]:
    """Deprecated: use ``trading.polymarket_bot_tick``; master dispatches ticks on a cadence."""
    return await polymarket_bot_tick(parameters)


# Legacy alias: bare task_type dispatched by older Redis queue entries or
# external callers that haven't adopted the namespaced form yet.
@registry.register("polymarket_bot")
async def polymarket_bot_legacy(parameters: dict[str, Any]) -> dict[str, Any]:
    """Legacy alias for ``trading.polymarket_bot_tick`` — handles stale queue entries."""
    return await polymarket_bot_tick(parameters)
