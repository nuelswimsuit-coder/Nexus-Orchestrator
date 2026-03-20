"""
prediction.cross_exchange — Cross-Exchange Predictor

Fetches real-time BTC/USDT data from Binance and compares with Polymarket
Yes/No odds.  Triggers a "High Confidence Buy" signal when:
  • Binance order book shows buy-side imbalance > 70 %
  • Polymarket "Yes" price is still below $0.52  (market has NOT priced it in)

This gap indicates the crowd prediction market is lagging behind live order-
flow, creating a statistically measurable arbitrage opportunity.

Task types
----------
prediction.cross_exchange
    Live BTC/USDT cross-exchange analysis.
    Parameters:
        symbol : str  — Binance trading pair (default: "BTCUSDT")

Background collector
--------------------
run_arbitrage_collector(redis)
    Async background loop — call from the API lifespan to sample Binance spot
    price and Polymarket Yes price every 2 seconds and persist them as a
    capped time-series in Redis (key: nexus:arbitrage:timeseries).
"""

from __future__ import annotations

import asyncio
import json as _json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import ccxt
import httpx
import structlog

from nexus.shared.system_settings import read_system_settings
from nexus.trading.config import (
    PAPER_TRADING,
    PAPER_TRADING_AMOUNT_USD,
    PAPER_TRADING_COOLDOWN_S,
    PAPER_TRADING_MAX_HISTORY,
    PAPER_TRADING_REDIS_KEY,
)
from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
ARBITRAGE_TIMESERIES_KEY = "nexus:arbitrage:timeseries"
TIMESERIES_MAX_POINTS    = 30
COLLECTOR_INTERVAL_S     = 2.0

# Redis key tracking the timestamp of the last virtual trade (cooldown guard)
_LAST_TRADE_TS_KEY = "nexus:paper_trading:last_trade_ts"

# Performance stats key — aggregated win/loss/pnl counters
PAPER_STATS_KEY    = "nexus:stats:paper"

# Seconds after entry before a paper trade is eligible for settlement
SETTLEMENT_DELAY_S = 300  # 5 minutes

BINANCE_TICKER_URL   = "https://api.binance.com/api/v3/ticker/price"
BINANCE_DEPTH_URL    = "https://api.binance.com/api/v3/depth"
POLYMARKET_GAMMA_URL = "https://gamma-api.polymarket.com/markets"

IMBALANCE_THRESHOLD    = 0.70   # buy-side fraction that triggers the signal
POLYMARKET_YES_CEILING = 0.52   # Yes price must be BELOW this to be "lagging"
ORDER_BOOK_LIMIT       = 20     # depth levels to pull from Binance

# ── Automated trade thresholds (stricter than signal thresholds) ───────────────
# A trade is only executed when BOTH of these are satisfied simultaneously,
# providing a tighter confirmation window than the display signal (70 %).
TRADE_IMBALANCE_THRESHOLD = 0.80   # buy-side fraction required to trigger a trade
TRADE_MIN_GAP             = 0.03   # minimum arbitrage gap (3 %) required to trade


def _prediction_throttle_delay_s() -> float:
    """
    Dynamic throttle delay between prediction cycles.
    Defaults to 1.0s to smooth CPU spikes when Redis/HTTP workloads burst.
    """
    env_val = os.getenv("NEXUS_PREDICTION_THROTTLE_DELAY")
    if env_val:
        try:
            return max(1.0, float(env_val))
        except ValueError:
            pass
    dynamic = read_system_settings()
    # Keep at least 1 second even if power_limit is raised.
    return max(1.0, 1.0 if dynamic.get("power_limit", 30) <= 30 else 0.5)


# ── CCXT Binance client factory ────────────────────────────────────────────────

def _make_binance_client() -> ccxt.binance:
    """
    Return a CCXT Binance client configured for stability in live execution.

    Options applied
    ---------------
    enableRateLimit            — respect Binance's rate-limit headers automatically
    adjustForTimeDifference    — sync local clock against server time to prevent
                                 "Timestamp ahead of server" (1021) errors
    recvWindow                 — extend the valid request window to 10 s (default 5 s)
                                 to absorb small clock drift during execution
    """
    return ccxt.binance({
        "apiKey":          os.getenv("BINANCE_API_KEY", ""),
        "secret":          os.getenv("BINANCE_API_SECRET", ""),
        "enableRateLimit": True,
        "options": {
            "adjustForTimeDifference": True,
            "recvWindow":              10000,
        },
    })


# ── Binance data fetcher ───────────────────────────────────────────────────────

async def fetch_binance_data(symbol: str = "BTCUSDT") -> dict[str, Any]:
    """
    Pull the spot price and top-N order book levels for `symbol` from Binance.

    Returns a dict with the price, raw bid/ask totals, computed buy/sell
    percentages, and an imbalance label.
    """
    async with httpx.AsyncClient(timeout=10.0) as client:
        price_res, depth_res = await asyncio.gather(
            client.get(BINANCE_TICKER_URL, params={"symbol": symbol}),
            client.get(BINANCE_DEPTH_URL, params={"symbol": symbol, "limit": ORDER_BOOK_LIMIT}),
        )
        price_res.raise_for_status()
        depth_res.raise_for_status()

    btc_price  = float(price_res.json()["price"])
    depth_data = depth_res.json()

    total_bids = sum(float(qty) for _, qty in depth_data.get("bids", []))
    total_asks = sum(float(qty) for _, qty in depth_data.get("asks", []))
    total_vol  = total_bids + total_asks

    buy_pct  = (total_bids / total_vol) if total_vol > 0 else 0.5
    sell_pct = 1.0 - buy_pct

    return {
        "price":                btc_price,
        "total_bids":           round(total_bids, 4),
        "total_asks":           round(total_asks, 4),
        "buy_pct":              round(buy_pct, 4),
        "sell_pct":             round(sell_pct, 4),
        "imbalance_direction":  "BUY" if buy_pct >= sell_pct else "SELL",
        "imbalance_strength":   round(max(buy_pct, sell_pct), 4),
    }


# ── Polymarket data fetcher ────────────────────────────────────────────────────

async def fetch_polymarket_btc_odds() -> dict[str, Any]:
    """
    Fetch the highest-volume active BTC price market from Polymarket's
    public Gamma API (no API key required).

    Returns the Yes/No prices along with the market question and volume.
    """
    params = {
        "q":         "bitcoin price",
        "active":    "true",
        "closed":    "false",
        "order":     "volume",
        "ascending": "false",
        "limit":     "5",
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        res = await client.get(POLYMARKET_GAMMA_URL, params=params)
        res.raise_for_status()

    markets = res.json()
    if not markets:
        return {
            "market_found":     False,
            "market_question":  None,
            "yes_price":        None,
            "no_price":         None,
        }

    market   = markets[0]
    question = market.get("question", "Unknown")

    # outcomePrices arrives as a JSON-encoded string or a list
    raw_prices = market.get("outcomePrices", '["0.5","0.5"]')
    if isinstance(raw_prices, str):
        try:
            raw_prices = _json.loads(raw_prices)
        except Exception:
            raw_prices = ["0.5", "0.5"]

    yes_price = float(raw_prices[0]) if len(raw_prices) > 0 else 0.5
    no_price  = float(raw_prices[1]) if len(raw_prices) > 1 else 0.5

    # clobTokenIds arrives as a JSON-encoded string or a list
    # Index 0 = YES token, index 1 = NO token
    raw_token_ids = market.get("clobTokenIds", "[]")
    if isinstance(raw_token_ids, str):
        try:
            raw_token_ids = _json.loads(raw_token_ids)
        except Exception:
            raw_token_ids = []
    clob_token_ids: list[str] = raw_token_ids if isinstance(raw_token_ids, list) else []

    return {
        "market_found":    True,
        "market_question": question,
        "yes_price":       round(yes_price, 4),
        "no_price":        round(no_price, 4),
        "market_id":       market.get("id"),
        "volume":          market.get("volume"),
        "clob_token_ids":  clob_token_ids,
    }


# ── Core analysis ─────────────────────────────────────────────────────────────

async def run_cross_exchange_analysis(symbol: str = "BTCUSDT") -> dict[str, Any]:
    """
    Orchestrates both fetchers in parallel, then applies signal logic.

    Signal matrix
    -------------
    HIGH_CONFIDENCE_BUY   buy_pct > 70 %  AND  Yes < $0.52
    BUY_BIAS              buy_pct > 70 %  (Polymarket already caught up)
    POLYMARKET_LAGGING    Yes < $0.52     (no order-book confirmation yet)
    NEUTRAL               neither edge detected
    """
    t0 = time.monotonic()

    binance_result, poly_result = await asyncio.gather(
        fetch_binance_data(symbol),
        fetch_polymarket_btc_odds(),
        return_exceptions=True,
    )

    errors: list[str] = []

    if isinstance(binance_result, Exception):
        log.error("binance_fetch_failed", error=str(binance_result))
        errors.append(f"Binance: {binance_result}")
        binance_result = None

    if isinstance(poly_result, Exception):
        log.error("polymarket_fetch_failed", error=str(poly_result))
        errors.append(f"Polymarket: {poly_result}")
        poly_result = None

    # ── Signal logic ──────────────────────────────────────────────────────────
    signal         = "NEUTRAL"
    signal_label   = "No Signal"
    high_confidence = False
    arbitrage_gap: float | None = None

    if binance_result and poly_result and poly_result.get("market_found"):
        buy_pct   = binance_result["buy_pct"]
        yes_price = poly_result["yes_price"]

        has_ob_signal  = buy_pct   > IMBALANCE_THRESHOLD
        has_poly_lag   = yes_price < POLYMARKET_YES_CEILING

        if has_ob_signal and has_poly_lag:
            signal          = "HIGH_CONFIDENCE_BUY"
            signal_label    = "High Confidence Buy"
            high_confidence = True
            arbitrage_gap   = round(POLYMARKET_YES_CEILING - yes_price, 4)
            log.info(
                "prediction_signal_triggered",
                signal=signal,
                buy_pct=round(buy_pct * 100, 1),
                yes_price=yes_price,
                gap=arbitrage_gap,
            )
        elif has_ob_signal:
            signal       = "BUY_BIAS"
            signal_label = "Buy Bias (Polymarket Aligned)"
        elif has_poly_lag:
            signal       = "POLYMARKET_LAGGING"
            signal_label = "Polymarket Lagging (No OB Confirmation)"

    return {
        "status":          "completed" if not errors else "partial",
        "signal":          signal,
        "signal_label":    signal_label,
        "high_confidence": high_confidence,
        "arbitrage_gap":   arbitrage_gap,
        "binance":         binance_result,
        "polymarket":      poly_result,
        "thresholds": {
            "imbalance_threshold":    IMBALANCE_THRESHOLD,
            "polymarket_yes_ceiling": POLYMARKET_YES_CEILING,
        },
        "errors":     errors,
        "duration_s": round(time.monotonic() - t0, 3),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


# ── Paper-trading execution layer ─────────────────────────────────────────────

async def _save_virtual_trade(
    redis: Any,
    signal: str,
    binance_data: dict[str, Any],
    poly_data: dict[str, Any],
) -> dict[str, Any]:
    """
    Persist a virtual trade entry to Redis under PAPER_TRADING_REDIS_KEY.

    Computes a naïve "potential profit" assuming the YES outcome resolves
    fully (yes_price → 1.0), sized at PAPER_TRADING_AMOUNT_USD.

    The list is capped at PAPER_TRADING_MAX_HISTORY entries (newest-first via
    LPUSH + LTRIM).
    """
    entry_yes_price      = poly_data.get("yes_price", 0.5) or 0.5
    virtual_amount       = PAPER_TRADING_AMOUNT_USD
    # Potential profit if Polymarket YES resolves (price goes to 1.0)
    potential_profit     = round((1.0 - entry_yes_price) * virtual_amount, 4)

    virtual_shares = round(virtual_amount / entry_yes_price, 1) if entry_yes_price > 0 else 0.0

    trade: dict[str, Any] = {
        "id":                   str(uuid.uuid4()),
        "timestamp":            datetime.now(timezone.utc).isoformat(),
        "signal":               signal,
        "direction":            "YES",
        "side":                 "YES",
        "entry_yes_price":      round(entry_yes_price, 4),
        "price":                round(entry_yes_price, 4),
        "shares":               virtual_shares,
        "entry_binance_price":  round(binance_data.get("price", 0.0), 2),
        "virtual_amount_usd":   virtual_amount,
        "spent_usd":            virtual_amount,
        "potential_profit_usd": potential_profit,
        "market_question":      poly_data.get("market_question", "BTC Price Market"),
        "market_id":            poly_data.get("market_id"),
        "status":               "success",
        "paper":                True,
        "log_text": (
            f"[PAPER] Bought {virtual_shares:.1f} shares of YES "
            f"@ ${entry_yes_price:.3f}"
        ),
    }

    await redis.lpush(PAPER_TRADING_REDIS_KEY, _json.dumps(trade))
    await redis.ltrim(PAPER_TRADING_REDIS_KEY, 0, PAPER_TRADING_MAX_HISTORY - 1)

    log.info(
        "virtual_trade_saved",
        trade_id=trade["id"],
        signal=signal,
        entry_yes_price=entry_yes_price,
        potential_profit_usd=potential_profit,
        paper_trading=True,
    )
    return trade


async def maybe_execute_trade(
    redis: Any,
    signal: str,
    binance_data: dict[str, Any],
    poly_data: dict[str, Any],
) -> None:
    """
    Gate function between signal detection and order execution.

    If PAPER_TRADING is True  → log a virtual trade with cooldown protection.
    If PAPER_TRADING is False → forward to polymarket_client.place_order().

    The cooldown prevents flooding Redis with duplicate trades when the same
    signal persists across multiple 2-second collector cycles.
    """
    # ── Cooldown guard ────────────────────────────────────────────────────────
    last_ts_raw = await redis.get(_LAST_TRADE_TS_KEY)
    if last_ts_raw:
        try:
            last_ts = datetime.fromisoformat(last_ts_raw)
            elapsed = (datetime.now(timezone.utc) - last_ts).total_seconds()
            if elapsed < PAPER_TRADING_COOLDOWN_S:
                return  # still in cooldown, skip silently
        except Exception:
            pass  # malformed timestamp — proceed

    now_iso = datetime.now(timezone.utc).isoformat()
    await redis.set(_LAST_TRADE_TS_KEY, now_iso)

    if PAPER_TRADING:
        await _save_virtual_trade(redis, signal, binance_data, poly_data)
    else:
        # Live trading path — kill switch and timeout checks live inside place_order()
        from nexus.trading.polymarket_client import (  # noqa: PLC0415
            TradingHalted,
            place_order,
        )
        try:
            await place_order(
                signal=signal,
                binance_data=binance_data,
                poly_data=poly_data,
                redis=redis,
            )
        except TradingHalted as exc:
            log.error(
                "trading_kill_switch_triggered",
                reason=str(exc),
                signal=signal,
            )
        except asyncio.TimeoutError:
            log.error("live_order_timeout", signal=signal)
        except Exception as exc:
            log.error("live_order_failed", error=str(exc), signal=signal)


# ── Paper-trade settlement ────────────────────────────────────────────────────

async def _recompute_paper_stats(redis: Any) -> None:
    """
    Scan all settled paper trades and persist aggregated performance stats
    to PAPER_STATS_KEY as a JSON string.

    Stats computed:
        total_trades  — count of settled (win + loss) trades
        wins          — count of winning settlements
        losses        — count of losing settlements
        virtual_pnl   — sum of realized_pnl_usd across settled trades
        win_streak    — consecutive wins from the most-recent settled trade
    """
    raw_entries: list[str] = await redis.lrange(PAPER_TRADING_REDIS_KEY, 0, -1)

    # Trades are stored newest-first (LPUSH)
    trades: list[dict[str, Any]] = []
    for raw in raw_entries:
        try:
            trades.append(_json.loads(raw))
        except Exception:
            pass

    total = 0
    wins  = 0
    losses = 0
    virtual_pnl = 0.0

    # Streak: count consecutive wins starting from the most-recent settled trade
    win_streak = 0
    streak_broken = False
    for trade in trades:
        status = trade.get("status")
        if status not in ("win", "loss"):
            continue  # skip open trades without breaking the streak
        if not streak_broken:
            if status == "win":
                win_streak += 1
            else:
                streak_broken = True

    for trade in trades:
        status = trade.get("status")
        if status not in ("win", "loss"):
            continue
        total += 1
        if status == "win":
            wins += 1
        else:
            losses += 1
        virtual_pnl += trade.get("realized_pnl_usd", 0.0)

    stats: dict[str, Any] = {
        "total_trades": total,
        "wins":         wins,
        "losses":       losses,
        "virtual_pnl":  round(virtual_pnl, 4),
        "win_streak":   win_streak,
        "updated_at":   datetime.now(timezone.utc).isoformat(),
    }
    await redis.set(PAPER_STATS_KEY, _json.dumps(stats))
    log.debug("paper_stats_updated", total=total, wins=wins, losses=losses, pnl=round(virtual_pnl, 4))


async def _settle_open_trades(redis: Any, current_btc_price: float) -> None:
    """
    Iterate the paper trade list and settle any open trade whose age exceeds
    SETTLEMENT_DELAY_S (5 minutes).

    Settlement logic:
        All virtual trades use a BUY (UP) prediction.
        WIN  → current BTC price is strictly above entry price
        LOSS → current BTC price is at or below entry price

        Realized P&L:
            WIN  → +potential_profit_usd
            LOSS → -virtual_amount_usd  (entire stake lost)

    After settlement, nexus:stats:paper is recomputed.
    """
    raw_entries: list[str] = await redis.lrange(PAPER_TRADING_REDIS_KEY, 0, -1)
    if not raw_entries:
        return

    now = datetime.now(timezone.utc)
    any_settled = False

    for idx, raw in enumerate(raw_entries):
        try:
            trade: dict[str, Any] = _json.loads(raw)
        except Exception:
            continue

        if trade.get("status") != "open":
            continue

        ts_str = trade.get("timestamp", "")
        try:
            trade_ts = datetime.fromisoformat(ts_str)
        except Exception:
            continue

        age_s = (now - trade_ts).total_seconds()
        if age_s < SETTLEMENT_DELAY_S:
            continue

        entry_price = float(trade.get("entry_binance_price", 0) or 0)
        if entry_price <= 0:
            continue

        won = current_btc_price > entry_price
        pnl: float
        if won:
            pnl = trade.get("potential_profit_usd", 0.0)
        else:
            pnl = -trade.get("virtual_amount_usd", 0.0)

        trade["status"]              = "win" if won else "loss"
        trade["exit_binance_price"]  = round(current_btc_price, 2)
        trade["realized_pnl_usd"]    = round(pnl, 4)
        trade["settled_at"]          = now.isoformat()

        await redis.lset(PAPER_TRADING_REDIS_KEY, idx, _json.dumps(trade))
        any_settled = True

        log.info(
            "paper_trade_settled",
            trade_id=trade.get("id"),
            result=trade["status"],
            entry_price=entry_price,
            exit_price=round(current_btc_price, 2),
            realized_pnl=trade["realized_pnl_usd"],
            age_s=round(age_s),
        )

    if any_settled:
        await _recompute_paper_stats(redis)


# ── Arbitrage time-series collector ──────────────────────────────────────────

async def collect_arbitrage_datapoint(redis: Any) -> dict[str, Any]:
    """
    Fetch a single Binance spot price + Polymarket Yes price snapshot and
    append it to the Redis time-series list, capped to TIMESERIES_MAX_POINTS.

    Parameters
    ----------
    redis : redis.asyncio.Redis  — shared async Redis client (decode_responses=True)
    """
    binance_price: float | None = None
    poly_price: float | None    = None

    binance_result, poly_result = await asyncio.gather(
        fetch_binance_data("BTCUSDT"),
        fetch_polymarket_btc_odds(),
        return_exceptions=True,
    )

    if not isinstance(binance_result, Exception):
        binance_price = binance_result.get("price")

    if not isinstance(poly_result, Exception) and poly_result.get("market_found"):
        poly_price = poly_result.get("yes_price")

    datapoint: dict[str, Any] = {
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "binance_price": binance_price,
        "poly_price":    poly_price,
    }

    entry = _json.dumps(datapoint)
    await redis.rpush(ARBITRAGE_TIMESERIES_KEY, entry)
    await redis.ltrim(ARBITRAGE_TIMESERIES_KEY, -TIMESERIES_MAX_POINTS, -1)

    # ── Paper trade settlement (5-minute mark) ─────────────────────────────
    if binance_price is not None:
        try:
            await _settle_open_trades(redis, binance_price)
        except Exception as exc:
            log.warning("paper_trade_settlement_error", error=str(exc))

    # ── Automated trade evaluation ─────────────────────────────────────────
    # Only fire when BOTH sources returned valid data AND the stricter trade
    # thresholds are met: buy-side imbalance > 80 % AND gap > 3 %.
    if (
        not isinstance(binance_result, Exception)
        and not isinstance(poly_result, Exception)
        and poly_result.get("market_found")
    ):
        buy_pct       = binance_result.get("buy_pct", 0.0)
        yes_price     = poly_result.get("yes_price", 1.0) or 1.0
        arbitrage_gap = max(POLYMARKET_YES_CEILING - yes_price, 0.0)

        is_high_confidence = (
            buy_pct   > IMBALANCE_THRESHOLD
            and yes_price < POLYMARKET_YES_CEILING
        )
        meets_trade_thresholds = (
            buy_pct       > TRADE_IMBALANCE_THRESHOLD
            and arbitrage_gap > TRADE_MIN_GAP
        )

        if is_high_confidence and meets_trade_thresholds:
            try:
                await maybe_execute_trade(
                    redis,
                    "HIGH_CONFIDENCE_BUY",
                    binance_result,
                    poly_result,
                )
            except Exception as exc:
                log.warning(
                    "trade_execution_error",
                    error=str(exc),
                    buy_pct=round(buy_pct * 100, 1),
                    gap=round(arbitrage_gap * 100, 1),
                )

    return datapoint


async def run_arbitrage_collector(redis: Any) -> None:
    """
    Continuous background coroutine — poll Binance + Polymarket every
    COLLECTOR_INTERVAL_S seconds and persist paired price data to Redis.

    Designed to be launched as an asyncio.Task from the API lifespan so it
    runs alongside request handling without blocking.
    """
    log.info(
        "arbitrage_collector_started",
        interval_s=COLLECTOR_INTERVAL_S,
        throttle_delay_s=_prediction_throttle_delay_s(),
    )
    while True:
        try:
            await collect_arbitrage_datapoint(redis)
        except asyncio.CancelledError:
            log.info("arbitrage_collector_stopped")
            raise
        except Exception as exc:
            log.warning("arbitrage_collector_error", error=str(exc))
        await asyncio.sleep(max(COLLECTOR_INTERVAL_S, _prediction_throttle_delay_s()))


# ── Task handler ──────────────────────────────────────────────────────────────

@registry.register("prediction.cross_exchange")
async def cross_exchange(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    ARQ-dispatchable task wrapper around run_cross_exchange_analysis().

    Parameters
    ----------
    symbol : str  — Binance trading pair (default: "BTCUSDT")
    """
    symbol = parameters.get("symbol", "BTCUSDT")
    result = await run_cross_exchange_analysis(symbol)
    await asyncio.sleep(_prediction_throttle_delay_s())
    return result
