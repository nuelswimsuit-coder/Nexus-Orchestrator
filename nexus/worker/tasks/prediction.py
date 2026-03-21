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
from datetime import datetime, timezone
from typing import Any

import ccxt
import httpx
import redis.asyncio as redis_asyncio
import structlog

from nexus.shared.config import settings
from nexus.shared.system_settings import read_system_settings
from nexus.trading.polymarket_client import KILL_SWITCH_BALANCE_USD
from nexus.trading.config import (
    PAPER_TRADING,
    PAPER_TRADING_AMOUNT_USD,
    PAPER_TRADING_COOLDOWN_S,
    PAPER_TRADING_MAX_HISTORY,
    PAPER_TRADING_REDIS_KEY,
    PREDICTION_MANUAL_HALT_KEY,
)
from nexus.trading.runtime_mode import effective_paper_trading
from nexus.worker.tasks.live_trade_execution import execute_live_trade, get_live_balance_usd
from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
ARBITRAGE_TIMESERIES_KEY = "nexus:arbitrage:timeseries"
TIMESERIES_MAX_POINTS    = 30
COLLECTOR_INTERVAL_S     = 2.0

# Linux/API collector: ~30s momentum ≈ 15 × 2s steps between reference and now
BINANCE_VELOCITY_KEY     = "nexus:binance:velocity_30s"
_VELOCITY_WINDOW_POINTS  = 16

# Redis key tracking the timestamp of the last live execution (cooldown guard)
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


def compute_btc_prediction_ci(
    binance_result: dict[str, Any] | None,
    poly_yes: float | None,
) -> dict[str, float | None]:
    """
    Heuristic 5m BTC fair-value band from order-book skew + Polymarket lag.

    Returns pred_mid (point estimate) and symmetric % band [ci_low, ci_high]
    interpreted as the engine's implied short-horizon range.
    """
    if not binance_result:
        return {"pred_mid": None, "ci_low": None, "ci_high": None}

    btc = float(binance_result.get("price") or 0.0)
    if btc <= 0:
        return {"pred_mid": None, "ci_low": None, "ci_high": None}

    buy_pct = float(binance_result.get("buy_pct", 0.5))
    yes     = float(poly_yes) if poly_yes is not None else 0.5

    ob_skew = (buy_pct - 0.5) * 2.0
    lag_lift = max(POLYMARKET_YES_CEILING - yes, 0.0) * 0.0012
    pred_mid = btc * (1.0 + ob_skew * 0.00085 + lag_lift)

    if buy_pct > IMBALANCE_THRESHOLD and yes < POLYMARKET_YES_CEILING:
        half_w = 0.00115
    elif buy_pct > IMBALANCE_THRESHOLD or yes < POLYMARKET_YES_CEILING:
        half_w = 0.0028
    else:
        half_w = 0.0058

    return {
        "pred_mid": round(pred_mid, 2),
        "ci_low":   round(pred_mid * (1.0 - half_w), 2),
        "ci_high":  round(pred_mid * (1.0 + half_w), 2),
    }


# ── Automated trade thresholds (stricter than signal thresholds) ───────────────
# A trade is only executed when BOTH of these are satisfied simultaneously,
# providing a tighter confirmation window than the display signal (70 %).
TRADE_IMBALANCE_THRESHOLD = 0.80   # buy-side fraction required to trigger a trade
TRADE_MIN_GAP             = 0.03   # minimum arbitrage gap (3 %) required to trade


async def _publish_binance_velocity_30s(redis: Any) -> None:
    """Derive BTC spot momentum over ~30s from the arbitrage time-series."""
    try:
        raw_list = await redis.lrange(ARBITRAGE_TIMESERIES_KEY, 0, -1)
        if len(raw_list) < _VELOCITY_WINDOW_POINTS:
            return
        now_dp = _json.loads(raw_list[-1])
        ref_dp = _json.loads(raw_list[-_VELOCITY_WINDOW_POINTS])
        p_now = now_dp.get("binance_price")
        p_ref = ref_dp.get("binance_price")
        if p_now is None or p_ref is None or float(p_ref) <= 0:
            return
        mom = (float(p_now) / float(p_ref) - 1.0) * 100.0
        payload = {
            "momentum_pct_30s": round(mom, 4),
            "price_now":        float(p_now),
            "price_ref":        float(p_ref),
            "ref_timestamp":    ref_dp.get("timestamp"),
            "updated_at":       datetime.now(timezone.utc).isoformat(),
        }
        await redis.set(BINANCE_VELOCITY_KEY, _json.dumps(payload))
    except Exception as exc:
        log.debug("binance_velocity_publish_failed", error=str(exc))


async def _set_node_intent(intent: str, redis_client: Any | None = None) -> None:
    """Best-effort intent broadcast to Redis for node dashboards."""
    node_id = settings.node_id or os.getenv("NODE_ID", "master")
    client = redis_client or redis_asyncio.from_url(settings.redis_url, decode_responses=True)
    try:
        await client.set(f"node:{node_id}:intent", intent)
        await client.set("node:intent", intent)
    except Exception as exc:
        log.debug("prediction_intent_publish_failed", error=str(exc))
    finally:
        if redis_client is None:
            await client.aclose()


async def _set_node_vision(vision: str, redis_client: Any | None = None) -> None:
    """Best-effort near-term vision broadcast to Redis for node dashboards."""
    node_id = settings.node_id or os.getenv("NODE_ID", "master")
    client = redis_client or redis_asyncio.from_url(settings.redis_url, decode_responses=True)
    try:
        await client.set(f"node:{node_id}:vision", vision)
        await client.set("node:vision", vision)
    except Exception as exc:
        log.debug("prediction_vision_publish_failed", error=str(exc))
    finally:
        if redis_client is None:
            await client.aclose()


def _predict_next_5m_vision(
    binance_data: dict[str, Any] | None,
    poly_data: dict[str, Any] | None,
) -> str:
    """
    Simple heuristic that forecasts what this node is likely to do next.
    """
    if not binance_data or not poly_data or not poly_data.get("market_found"):
        return "Next 5m: stabilize data feeds and keep scanning cross-exchange divergence."

    buy_pct = float(binance_data.get("buy_pct", 0.5))
    yes_price = float(poly_data.get("yes_price", 0.5))
    gap = max(POLYMARKET_YES_CEILING - yes_price, 0.0)

    if buy_pct > TRADE_IMBALANCE_THRESHOLD and gap > TRADE_MIN_GAP:
        return "Next 5m: maintain high-confidence BUY watch and prepare execution window."
    if buy_pct > IMBALANCE_THRESHOLD:
        return "Next 5m: monitor order-book momentum for a possible confidence upgrade."
    if yes_price < POLYMARKET_YES_CEILING:
        return "Next 5m: Polymarket lag detected, waiting for order-book confirmation."
    return "Next 5m: neutral scan mode across BTCUSDT with continuous arbitrage sampling."


async def _push_node_history(task_line: str, redis_client: Any | None = None) -> None:
    """Keep a rolling node-local history list for terminal monitors."""
    node_id = settings.node_id or os.getenv("NODE_ID", "master")
    client = redis_client or redis_asyncio.from_url(settings.redis_url, decode_responses=True)
    try:
        await client.lpush(f"node:{node_id}:history", task_line)
        await client.ltrim(f"node:{node_id}:history", 0, 4)
        await client.lpush("node:history", task_line)
        await client.ltrim("node:history", 0, 4)
    except Exception as exc:
        log.debug("prediction_history_publish_failed", error=str(exc))
    finally:
        if redis_client is None:
            await client.aclose()


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
    await _set_node_intent(f"Prediction: fetching Binance + Polymarket data for {symbol}")

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
    await _set_node_intent(f"Prediction: computing signal matrix for {symbol}")
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
        "prediction_ci": compute_btc_prediction_ci(
            binance_result,
            poly_result.get("yes_price") if poly_result and poly_result.get("market_found") else None,
        ),
    }


# ── Live execution layer ───────────────────────────────────────────────────────

async def maybe_execute_trade(
    redis: Any,
    signal: str,
    binance_data: dict[str, Any],
    poly_data: dict[str, Any],
) -> None:
    """
    Gate function between signal detection and order execution.
    """
    try:
        if await redis.get(PREDICTION_MANUAL_HALT_KEY):
            log.warning("prediction_trade_skipped_manual_halt", signal=signal)
            return
    except Exception:
        pass

    # ── Cooldown guard ────────────────────────────────────────────────────────
    await _set_node_intent(f"Prediction: evaluating execution gate for {signal}", redis)
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

    try:
        paper_now = await effective_paper_trading(redis)
        intent = (
            "Prediction: trade conditions met, dispatching paper (simulation) trade"
            if paper_now
            else "Prediction: trade conditions met, dispatching live execution"
        )
        await _set_node_intent(intent, redis)
        await execute_live_trade(
            redis=redis,
            signal=signal,
            binance_data=binance_data,
            poly_data=poly_data,
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


async def apply_prediction_manual_override(redis: Any) -> dict[str, Any]:
    """
    Engage manual halt (blocks new prediction-market orders) and force-close
    any paper trades still marked ``open`` in Redis history.
    """
    now = datetime.now(timezone.utc).isoformat()
    await redis.set(PREDICTION_MANUAL_HALT_KEY, now)

    raw_entries: list[str] = await redis.lrange(PAPER_TRADING_REDIS_KEY, 0, -1)
    killed = 0
    for idx, raw in enumerate(raw_entries):
        try:
            trade: dict[str, Any] = _json.loads(raw)
        except Exception:
            continue
        if trade.get("status") != "open":
            continue
        entry_px = float(trade.get("entry_binance_price", 0) or 0)
        trade["status"]             = "killed"
        trade["realized_pnl_usd"]   = 0.0
        trade["exit_binance_price"] = round(entry_px, 2) if entry_px > 0 else None
        trade["settled_at"]         = now
        trade["kill_reason"]        = "manual_override"
        await redis.lset(PAPER_TRADING_REDIS_KEY, idx, _json.dumps(trade))
        killed += 1

    if killed:
        await _recompute_paper_stats(redis)

    log.warning("prediction_manual_override", open_positions_closed=killed)
    return {
        "halted":                 True,
        "halted_at":              now,
        "open_positions_closed":  killed,
    }


async def clear_prediction_manual_override(redis: Any) -> None:
    await redis.delete(PREDICTION_MANUAL_HALT_KEY)


# ── Arbitrage time-series collector ──────────────────────────────────────────

async def collect_arbitrage_datapoint(redis: Any) -> dict[str, Any]:
    """
    Fetch a single Binance spot price + Polymarket Yes price snapshot and
    append it to the Redis time-series list, capped to TIMESERIES_MAX_POINTS.

    Parameters
    ----------
    redis : redis.asyncio.Redis  — shared async Redis client (decode_responses=True)
    """
    await _set_node_intent("Scanning high-volatility pairs to maximize ROI", redis)
    await _set_node_vision(
        "Next 5m: collect order-book imbalance and Polymarket lag signals.",
        redis,
    )
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

    ci: dict[str, float | None] = {"pred_mid": None, "ci_low": None, "ci_high": None}
    if not isinstance(binance_result, Exception):
        py = poly_price if not isinstance(poly_result, Exception) else None
        ci = compute_btc_prediction_ci(binance_result, py)

    datapoint: dict[str, Any] = {
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "binance_price": binance_price,
        "poly_price":    poly_price,
        "pred_mid":      ci.get("pred_mid"),
        "ci_low":        ci.get("ci_low"),
        "ci_high":       ci.get("ci_high"),
    }

    entry = _json.dumps(datapoint)
    await redis.rpush(ARBITRAGE_TIMESERIES_KEY, entry)
    await redis.ltrim(ARBITRAGE_TIMESERIES_KEY, -TIMESERIES_MAX_POINTS, -1)
    await _publish_binance_velocity_30s(redis)

    # ── Automated trade evaluation ─────────────────────────────────────────
    # Only fire when BOTH sources returned valid data AND the stricter trade
    # thresholds are met: buy-side imbalance > 80 % AND gap > 3 %.
    if (
        not isinstance(binance_result, Exception)
        and not isinstance(poly_result, Exception)
        and poly_result.get("market_found")
    ):
        await _set_node_intent("Prediction collector: scoring opportunity thresholds", redis)
        buy_pct   = binance_result.get("buy_pct", 0.0)
        yes_price = poly_result.get("yes_price", 1.0) or 1.0

        # Production safety: verify live balance before peak opportunity scoring.
        # Simulation skips the real balance gate so the worker runs without a funded wallet.
        live_balance = 0.0
        balance_ok = False
        if await effective_paper_trading(redis):
            live_balance = 100.0
            balance_ok = True
        else:
            try:
                live_balance = await get_live_balance_usd()
                balance_ok = live_balance >= max(KILL_SWITCH_BALANCE_USD, PAPER_TRADING_AMOUNT_USD)
            except Exception as exc:
                log.error("live_balance_check_failed", error=str(exc))

        arbitrage_gap = max(POLYMARKET_YES_CEILING - yes_price, 0.0) if balance_ok else 0.0

        is_high_confidence = (
            buy_pct   > IMBALANCE_THRESHOLD
            and yes_price < POLYMARKET_YES_CEILING
        )
        meets_trade_thresholds = (
            buy_pct       > TRADE_IMBALANCE_THRESHOLD
            and arbitrage_gap > TRADE_MIN_GAP
        )

        if not balance_ok:
            log.warning(
                "trade_skipped_balance_guard",
                balance_usd=round(live_balance, 2),
                min_required_usd=round(max(KILL_SWITCH_BALANCE_USD, PAPER_TRADING_AMOUNT_USD), 2),
            )
        elif is_high_confidence and meets_trade_thresholds:
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

    if not isinstance(binance_result, Exception) and not isinstance(poly_result, Exception):
        await _set_node_vision(_predict_next_5m_vision(binance_result, poly_result), redis)
    else:
        await _set_node_vision(
            "Next 5m: retry data providers and keep prediction engine warm.",
            redis,
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
    await _set_node_intent(f"Prediction task: analyzing cross-exchange signal for {symbol}")
    await _set_node_vision(
        f"Next 5m: evaluate {symbol} for imbalance and cross-market mispricing."
    )
    result = await run_cross_exchange_analysis(symbol)
    if result.get("status") in {"completed", "partial"}:
        await _push_node_history(
            f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}Z] Prediction ({symbol}) "
            f"{result.get('signal', 'NEUTRAL')} status={result.get('status')}"
        )
    await _set_node_intent(
        f"Prediction task complete: {result.get('signal', 'NEUTRAL')} on {symbol}"
    )
    await _set_node_vision(
        _predict_next_5m_vision(result.get("binance"), result.get("polymarket"))
    )
    await asyncio.sleep(_prediction_throttle_delay_s())
    return result
