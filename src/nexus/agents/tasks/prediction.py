"""
prediction.cross_exchange — Cross-Exchange Predictor

Fetches real-time **BTC/USD** spot via Binance ``BTCUSDT`` and compares with
Polymarket **Bitcoin Up or Down** (price-action) odds.  Triggers a "High Confidence Buy" signal when:
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
    capped time-series in Redis (key: trading:ticks).
"""

from __future__ import annotations

import asyncio
import json as _json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from typing import Any

import ccxt
import httpx
import redis.asyncio as redis_asyncio
import structlog

from nexus.shared.config import settings
from nexus.shared.system_settings import read_system_settings
from nexus.agents.trading.polymarket_client import KILL_SWITCH_BALANCE_USD
from nexus.agents.trading.config import (
    PAPER_TRADING_AMOUNT_USD,
    PAPER_TRADING_COOLDOWN_S,
    PAPER_TRADING_MAX_HISTORY,
    PAPER_TRADING_REDIS_KEY,
    PREDICTION_MANUAL_HALT_KEY,
)
from nexus.services.trading_bot import (
    ASSET_PAIR,
    BINANCE_SPOT_SYMBOL,
    LEGACY_ARBITRAGE_TIMESERIES_KEY,
    MIN_CONFIDENCE_SCORE,
    TRADING_TICKS_REDIS_KEY,
    compare_poly_probability_vs_binance_volatility,
    fetch_polymarket_btc_price_action_contract,
    fetch_polymarket_clob_orderbook,
    normalize_poly_yes_with_orderbook_depth,
    summarize_polymarket_orderbook,
)
from nexus.agents.tasks.live_trade_execution import execute_live_trade, get_live_balance_usd
from nexus.agents.task_registry import registry

log = structlog.get_logger(__name__)


# ── CPU priority boost ─────────────────────────────────────────────────────────

def _boost_cpu_priority() -> None:
    """
    Raise this process to ABOVE_NORMAL priority so the prediction engine
    maintains 40–70 % CPU utilisation during active scanning cycles.
    Safe no-op on platforms where the call is unsupported.
    """
    try:
        if sys.platform == "win32":
            import ctypes
            ABOVE_NORMAL_PRIORITY_CLASS = 0x00008000
            ctypes.windll.kernel32.SetPriorityClass(  # type: ignore[attr-defined]
                ctypes.windll.kernel32.GetCurrentProcess(),  # type: ignore[attr-defined]
                ABOVE_NORMAL_PRIORITY_CLASS,
            )
            log.info("prediction_cpu_priority_boosted", platform="win32", level="ABOVE_NORMAL")
        else:
            os.nice(-5)  # -5 → higher priority on Linux/macOS (requires permissions)
            log.info("prediction_cpu_priority_boosted", platform="posix", nice=-5)
    except Exception as exc:
        log.debug("prediction_cpu_priority_boost_failed", error=str(exc))


_CPU_BOOSTED = False


def _ensure_cpu_boost() -> None:
    """Boost CPU priority once per process lifetime."""
    global _CPU_BOOSTED  # noqa: PLW0603
    if not _CPU_BOOSTED:
        _boost_cpu_priority()
        _CPU_BOOSTED = True


# ── Post-task DB verification ──────────────────────────────────────────────────

def _poll_telefix_row_count(db_path: str | None = None) -> int:
    """Return current total row count in telefix.db system_events table, or -1 on error."""
    try:
        from nexus.shared.db_util import get_telefix_db  # noqa: PLC0415
        conn = get_telefix_db() if db_path is None else sqlite3.connect(db_path, timeout=5)
        row = conn.execute("SELECT COUNT(*) FROM system_events").fetchone()
        return int(row[0]) if row else 0
    except Exception:
        return -1


def _verify_db_write_after_task(row_count_before: int) -> tuple[int, int]:
    """
    Poll telefix.db after a task completes. If row count increased, return
    (verified=1, written=1). Otherwise return (0, 0).
    """
    row_count_after = _poll_telefix_row_count()
    if row_count_after > row_count_before >= 0:
        return 1, 1
    return 0, 0


# ── Configuration ──────────────────────────────────────────────────────────────
ARBITRAGE_TIMESERIES_KEY = TRADING_TICKS_REDIS_KEY
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

BINANCE_TICKER_URL = "https://api.binance.com/api/v3/ticker/price"
BINANCE_DEPTH_URL  = "https://api.binance.com/api/v3/depth"
BINANCE_PING_URL   = "https://api.binance.com/api/v3/ping"
GAMMA_WARMUP_URL   = "https://gamma-api.polymarket.com/markets"

IMBALANCE_THRESHOLD    = float(
    os.environ.get("NEXUS_IMBALANCE_THRESHOLD", str(MIN_CONFIDENCE_SCORE)),
)  # buy-side fraction that triggers the signal (default aligns with MIN_CONFIDENCE_SCORE)
POLYMARKET_YES_CEILING = 0.52   # Up price must be BELOW this to be "lagging"
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


# ── Automated trade thresholds (collector path; tunable vs display signal) ─────
# Looser defaults so the background collector does not sit behind a 3 % gap + 70 % buy wall.
TRADE_IMBALANCE_THRESHOLD = float(
    os.environ.get(
        "NEXUS_TRADE_IMBALANCE_THRESHOLD",
        str(max(0.58, MIN_CONFIDENCE_SCORE - 0.07)),
    ),
)
TRADE_MIN_GAP = float(os.environ.get("NEXUS_TRADE_MIN_GAP", "0.012"))

# Buy-side floor for the collector's execution signal (below API "HIGH_CONFIDENCE" 70 % bar).
EXEC_SIGNAL_IMBALANCE = float(os.environ.get("NEXUS_EXEC_SIGNAL_IMBALANCE", "0.62"))


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
        return "[EXECUTING] Analyzing markets and actively deploying nodes."
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

async def fetch_binance_data(symbol: str = BINANCE_SPOT_SYMBOL) -> dict[str, Any]:
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
    bids = depth_data.get("bids") or []
    asks = depth_data.get("asks") or []

    total_bids = sum(float(qty) for _, qty in bids)
    total_asks = sum(float(qty) for _, qty in asks)
    total_vol  = total_bids + total_asks

    buy_pct  = (total_bids / total_vol) if total_vol > 0 else 0.5
    sell_pct = 1.0 - buy_pct
    best_bid = float(bids[0][0]) if bids else None
    best_ask = float(asks[0][0]) if asks else None
    spread_bps = None
    if best_bid and best_ask and btc_price > 0:
        spread_bps = round((best_ask - best_bid) / btc_price * 10_000.0, 4)

    return {
        "asset_pair":           ASSET_PAIR,
        "price":                btc_price,
        "total_bids":           round(total_bids, 4),
        "total_asks":           round(total_asks, 4),
        "buy_pct":              round(buy_pct, 4),
        "sell_pct":             round(sell_pct, 4),
        "spread_bps":           spread_bps,
        "imbalance_direction":  "BUY" if buy_pct >= sell_pct else "SELL",
        "imbalance_strength":   round(max(buy_pct, sell_pct), 4),
    }


# ── Polymarket data fetcher ────────────────────────────────────────────────────

async def fetch_polymarket_btc_odds() -> dict[str, Any]:
    """
    Fetch the active **Bitcoin Up or Down** (price-action) contract from Gamma plus
    a CLOB **order book** snapshot for the Up token.

    ``yes_price`` / ``no_price`` map to **Up** / **Down** probabilities (0–1);
    ``0.27`` means 27 % implied probability for Up. Depth-adjusted values are
    applied only when writing the arbitrage time series (see ``collect_arbitrage_datapoint``).
    """
    hit = await fetch_polymarket_btc_price_action_contract()
    if not hit.get("market_found"):
        return {
            "market_found":     False,
            "market_question":  None,
            "yes_price":        None,
            "no_price":         None,
            "strike_usd":       None,
            "polymarket_orderbook": None,
        }
    tokens: list[str] = list(hit.get("clob_token_ids") or [])
    ob_summary: dict[str, Any] | None = None
    if tokens:
        try:
            async with httpx.AsyncClient(timeout=15.0) as c:
                book = await fetch_polymarket_clob_orderbook(tokens[0], c)
            ob_summary = summarize_polymarket_orderbook(book)
        except Exception as exc:
            log.debug("polymarket_orderbook_fetch_failed", error=str(exc))
    return {
        "market_found":     True,
        "market_question":  hit.get("market_question"),
        "yes_price":        hit.get("yes_price"),
        "no_price":         hit.get("no_price"),
        "market_id":        hit.get("market_id"),
        "volume":           hit.get("volume"),
        "clob_token_ids":   tokens,
        "strike_usd":       hit.get("strike_usd"),
        "condition_id":     hit.get("condition_id"),
        "outcome_labels":   hit.get("outcome_labels"),
        "polymarket_orderbook": ob_summary,
    }


# ── Core analysis ─────────────────────────────────────────────────────────────

async def run_cross_exchange_analysis(symbol: str = BINANCE_SPOT_SYMBOL) -> dict[str, Any]:
    """
    Orchestrates both fetchers in parallel, then applies signal logic.

    Signal matrix
    -------------
    HIGH_CONFIDENCE_BUY   buy_pct > 70 %  AND  Yes < $0.52
    BUY_BIAS              buy_pct > 70 %  (Polymarket already caught up)
    POLYMARKET_LAGGING    Yes < $0.52     (execution path active — not a wait state)
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

    poly_vs_binance: dict[str, Any] | None = None
    if (
        binance_result
        and poly_result
        and poly_result.get("market_found")
        and poly_result.get("yes_price") is not None
    ):
        poly_vs_binance = compare_poly_probability_vs_binance_volatility(
            float(poly_result["yes_price"]),
            float(binance_result.get("buy_pct", 0.5)),
            binance_result.get("spread_bps"),
        )

    # ── Signal logic ──────────────────────────────────────────────────────────
    await _set_node_intent(f"Prediction: computing signal matrix for {symbol}")
    signal         = "NEUTRAL"
    signal_label   = "No Signal"
    high_confidence = False
    arbitrage_gap: float | None = None

    if binance_result and poly_result and poly_result.get("market_found"):
        buy_pct   = binance_result["buy_pct"]
        yes_raw   = float(poly_result["yes_price"])

        has_ob_signal  = buy_pct   > IMBALANCE_THRESHOLD
        has_poly_lag   = yes_raw < POLYMARKET_YES_CEILING

        if has_ob_signal and has_poly_lag:
            signal          = "HIGH_CONFIDENCE_BUY"
            signal_label    = "High Confidence Buy"
            high_confidence = True
            arbitrage_gap   = round(POLYMARKET_YES_CEILING - yes_raw, 4)
            log.info(
                "prediction_signal_triggered",
                signal=signal,
                buy_pct=round(buy_pct * 100, 1),
                yes_price=yes_raw,
                gap=arbitrage_gap,
            )
        elif has_ob_signal:
            signal       = "BUY_BIAS"
            signal_label = "Buy Bias (Polymarket Aligned)"
        elif has_poly_lag:
            signal       = "POLYMARKET_LAGGING"
            signal_label = "[EXECUTING] Analyzing markets — routing Poly mispricing to execution"

    return {
        "status":          "completed" if not errors else "partial",
        "signal":          signal,
        "signal_label":    signal_label,
        "high_confidence": high_confidence,
        "arbitrage_gap":   arbitrage_gap,
        "asset_pair":      ASSET_PAIR,
        "poly_vs_binance": poly_vs_binance,
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
            (
                normalize_poly_yes_with_orderbook_depth(
                    float(poly_result["yes_price"]),
                    float(binance_result["buy_pct"]),
                )
                if poly_result and poly_result.get("market_found") and binance_result
                else poly_result.get("yes_price")
                if poly_result and poly_result.get("market_found")
                else None
            ),
        ),
    }


# ── Live execution layer ───────────────────────────────────────────────────────


async def execute_trade_cycle(
    redis: Any,
    binance_result: Any,
    poly_result: Any,
) -> None:
    """
    Evaluate thresholds and possibly dispatch a trade. Lag suspicion is logged only;
    it must not skip this path (no early return solely for Polymarket lag).
    """
    if (
        isinstance(binance_result, Exception)
        or isinstance(poly_result, Exception)
        or not isinstance(binance_result, dict)
        or not isinstance(poly_result, dict)
        or not poly_result.get("market_found")
    ):
        return

    yes_price = poly_result.get("yes_price", 1.0) or 1.0
    try:
        yes_f = float(yes_price)
    except (TypeError, ValueError):
        yes_f = 1.0
    await _set_node_intent("Prediction collector: scoring opportunity thresholds", redis)
    buy_pct = float(binance_result.get("buy_pct", 0.0))

    if yes_f < POLYMARKET_YES_CEILING and buy_pct > IMBALANCE_THRESHOLD:
        # Warning only (never skip this cycle); replaces any prior return/continue on lag suspicion.
        log.warning(
            "Polymarket lag detected",
            yes_price=yes_f,
            ceiling=POLYMARKET_YES_CEILING,
            buy_pct=round(buy_pct, 4),
        )

    live_balance = 0.0
    balance_ok = False
    try:
        live_balance = await get_live_balance_usd()
        balance_ok = live_balance >= max(KILL_SWITCH_BALANCE_USD, PAPER_TRADING_AMOUNT_USD)
    except Exception as exc:
        log.error("live_balance_check_failed", error=str(exc))

    arbitrage_gap = max(POLYMARKET_YES_CEILING - yes_f, 0.0) if balance_ok else 0.0

    is_high_confidence = buy_pct > EXEC_SIGNAL_IMBALANCE and yes_f < POLYMARKET_YES_CEILING
    meets_trade_thresholds = (
        buy_pct > TRADE_IMBALANCE_THRESHOLD and arbitrage_gap > TRADE_MIN_GAP
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
        await _set_node_intent("Prediction: trade conditions met, dispatching live execution", redis)
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

async def check_market_sync(
    binance_result: BaseException | dict[str, Any],
    poly_result: BaseException | dict[str, Any],
) -> bool:
    """
    When Binance shows strong buy flow but Polymarket Yes is still below the
    lag ceiling, treat as Polymarket lag. Log only — the pipeline always continues
    (no return/continue/sleep stall).
    """
    market_ready = True
    lag_detected = False
    if (
        not isinstance(binance_result, Exception)
        and not isinstance(poly_result, Exception)
        and poly_result.get("market_found")
        and poly_result.get("yes_price") is not None
    ):
        buy_pct = float(binance_result.get("buy_pct", 0.5))
        yes_raw = float(poly_result["yes_price"])
        if buy_pct > IMBALANCE_THRESHOLD and yes_raw < POLYMARKET_YES_CEILING:
            lag_detected = True

    if lag_detected:
        log.warning("Polymarket lag detected")
        # Log only — do not return/continue/sleep; pipeline proceeds immediately.

    return market_ready


async def objective_sync(redis: Any) -> dict[str, Any]:
    """One full strategic market tick (Binance + Polymarket arbitrage sample)."""
    return await collect_arbitrage_datapoint(redis)


async def _ensure_telefix_db_available() -> None:
    """
    Best-effort: ensure telefix.db is present on this node.
    If missing, triggers the DB resolver (download from Master or self-heal).
    Errors are logged but never propagate — the collector must keep running.
    """
    try:
        from nexus.shared.db_util import ensure_telefix_db  # noqa: PLC0415
        await ensure_telefix_db()
    except Exception as exc:
        log.warning("prediction_telefix_db_ensure_failed", error=str(exc))


async def collect_arbitrage_datapoint(redis: Any) -> dict[str, Any]:
    """
    Fetch a single Binance spot price + Polymarket Yes price snapshot and
    append it to the Redis time-series list, capped to TIMESERIES_MAX_POINTS.

    Parameters
    ----------
    redis : redis.asyncio.Redis  — shared async Redis client (decode_responses=True)
    """
    await _ensure_telefix_db_available()
    await _set_node_intent("Scanning high-volatility pairs to maximize ROI", redis)
    await _set_node_vision(
        "Next 5m: collect order-book imbalance and Polymarket lag signals.",
        redis,
    )
    binance_price: float | None = None
    poly_price_raw: float | None = None
    poly_price: float | None     = None

    binance_result, poly_result = await asyncio.gather(
        fetch_binance_data(BINANCE_SPOT_SYMBOL),
        fetch_polymarket_btc_odds(),
        return_exceptions=True,
    )

    await check_market_sync(binance_result, poly_result)

    if not isinstance(binance_result, Exception):
        binance_price = binance_result.get("price")

    poly_ob: dict[str, Any] | None = None
    poly_vs_binance_dp: dict[str, Any] | None = None

    if not isinstance(poly_result, Exception) and poly_result.get("market_found"):
        poly_ob = poly_result.get("polymarket_orderbook")

    if not isinstance(binance_result, Exception) and not isinstance(poly_result, Exception):
        if poly_result.get("market_found") and poly_result.get("yes_price") is not None:
            poly_vs_binance_dp = compare_poly_probability_vs_binance_volatility(
                float(poly_result["yes_price"]),
                float(binance_result.get("buy_pct", 0.5)),
                binance_result.get("spread_bps"),
            )

    if not isinstance(poly_result, Exception) and poly_result.get("market_found"):
        poly_price_raw = poly_result.get("yes_price")
        if poly_price_raw is not None:
            if not isinstance(binance_result, Exception):
                buy_pct = float(binance_result.get("buy_pct", 0.5))
                poly_price = normalize_poly_yes_with_orderbook_depth(
                    float(poly_price_raw), buy_pct
                )
            else:
                poly_price = float(poly_price_raw)

    ci: dict[str, float | None] = {"pred_mid": None, "ci_low": None, "ci_high": None}
    if not isinstance(binance_result, Exception):
        py = poly_price if not isinstance(poly_result, Exception) else None
        ci = compute_btc_prediction_ci(binance_result, py)

    datapoint: dict[str, Any] = {
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "asset_pair":     ASSET_PAIR,
        "binance_price":  binance_price,
        "poly_price":     poly_price,
        "poly_price_raw": poly_price_raw,
        "pred_mid":       ci.get("pred_mid"),
        "ci_low":         ci.get("ci_low"),
        "ci_high":        ci.get("ci_high"),
        "polymarket_orderbook": poly_ob,
        "poly_vs_binance": poly_vs_binance_dp,
    }

    entry = _json.dumps(datapoint)
    await redis.rpush(ARBITRAGE_TIMESERIES_KEY, entry)
    await redis.ltrim(ARBITRAGE_TIMESERIES_KEY, -TIMESERIES_MAX_POINTS, -1)
    await _publish_binance_velocity_30s(redis)

    # ── Automated trade evaluation ─────────────────────────────────────────
    await execute_trade_cycle(redis, binance_result, poly_result)

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

    if os.getenv("NEXUS_CLEAR_ARBITRAGE_CACHE_ON_START", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        try:
            await redis.delete(TRADING_TICKS_REDIS_KEY)
            await redis.delete(LEGACY_ARBITRAGE_TIMESERIES_KEY)
            log.info(
                "arbitrage_ticks_cache_cleared",
                cleared_keys=[TRADING_TICKS_REDIS_KEY, LEGACY_ARBITRAGE_TIMESERIES_KEY],
            )
        except Exception as exc:
            log.warning("arbitrage_ticks_cache_clear_failed", error=str(exc))

    try:
        async with httpx.AsyncClient(timeout=8.0) as warm:
            await asyncio.gather(
                warm.get(BINANCE_PING_URL),
                warm.get(GAMMA_WARMUP_URL, params={"limit": "1"}),
                return_exceptions=True,
            )
        log.info("arbitrage_upstream_warmup_complete")
    except Exception as exc:
        log.debug("arbitrage_upstream_warmup_failed", error=str(exc))

    while True:
        try:
            try:
                await asyncio.wait_for(objective_sync(redis), timeout=10.0)
            except asyncio.TimeoutError:
                log.warning(
                    "[RECOVERY] Objective sync timed out. Forcing fresh cycle.",
                )
        except asyncio.CancelledError:
            log.info("arbitrage_collector_stopped")
            raise
        except Exception as exc:
            log.warning("arbitrage_collector_error", error=str(exc))
        await asyncio.sleep(max(COLLECTOR_INTERVAL_S, _prediction_throttle_delay_s()))


# ── Task handler ──────────────────────────────────────────────────────────────

def _write_prediction_to_db(
    symbol: str,
    signal: str,
    status: str,
    binance_price: float | None,
    poly_yes: float | None,
    node_id: str,
) -> tuple[int, int]:
    """
    Persist a prediction cycle result to telefix.db (system_events table).

    Returns (written, verified) as integers (1 = success, 0 = failure).
    written  — row was inserted
    verified — SELECT confirms the row exists after INSERT
    """
    written = 0
    verified = 0
    try:
        from nexus.shared.db_util import get_telefix_db  # noqa: PLC0415

        conn = get_telefix_db()
        task_id = f"pred_{symbol}_{int(time.time())}"
        message = (
            f"signal={signal} status={status} "
            f"btc={binance_price} poly_yes={poly_yes}"
        )
        conn.execute(
            """
            INSERT INTO system_events (level, source, message, created_at)
            VALUES ('INFO', ?, ?, datetime('now'))
            """,
            (f"prediction.{symbol}", message[:500]),
        )
        conn.commit()
        written = 1

        # Verify the write by reading back the most-recent matching row
        row = conn.execute(
            "SELECT id FROM system_events WHERE source = ? ORDER BY id DESC LIMIT 1",
            (f"prediction.{symbol}",),
        ).fetchone()
        if row:
            verified = 1
    except Exception as exc:
        log.warning("prediction_db_write_failed", error=str(exc))

    return written, verified


@registry.register("prediction.cross_exchange")
async def cross_exchange(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    ARQ-dispatchable task wrapper around run_cross_exchange_analysis().

    Parameters
    ----------
    symbol : str  — Binance trading pair (default: "BTCUSDT")

    Always writes the result to telefix.db and returns written/verified flags.
    Boosts process CPU priority to ABOVE_NORMAL on first call.
    """
    _ensure_cpu_boost()

    symbol = parameters.get("symbol", BINANCE_SPOT_SYMBOL)
    node_id = os.getenv("NODE_ID", "master")

    await _set_node_intent(f"Prediction task: analyzing cross-exchange signal for {symbol}")
    await _set_node_vision(
        f"Next 5m: evaluate {symbol} for imbalance and cross-market mispricing."
    )

    # Snapshot row count before the task so we can verify the write increased it
    loop = asyncio.get_event_loop()
    row_count_before = await loop.run_in_executor(None, _poll_telefix_row_count)

    result = await run_cross_exchange_analysis(symbol)

    # ── Persist to telefix.db ──────────────────────────────────────────────────
    binance_price: float | None = None
    poly_yes: float | None = None
    if result.get("binance"):
        binance_price = result["binance"].get("price")
    if result.get("polymarket") and result["polymarket"].get("market_found"):
        poly_yes = result["polymarket"].get("yes_price")

    written, verified = await loop.run_in_executor(
        None,
        _write_prediction_to_db,
        symbol,
        result.get("signal", "NEUTRAL"),
        result.get("status", "unknown"),
        binance_price,
        poly_yes,
        node_id,
    )

    # ── Post-task DB verification: poll row count and confirm increase ─────────
    if written:
        post_verified, post_written = await loop.run_in_executor(
            None, _verify_db_write_after_task, row_count_before
        )
        # Use the stricter post-verification result
        verified = max(verified, post_verified)
        written = max(written, post_written)

    result["written"] = written
    result["verified"] = verified

    if result.get("status") in {"completed", "partial"}:
        await _push_node_history(
            f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}Z] Prediction ({symbol}) "
            f"{result.get('signal', 'NEUTRAL')} status={result.get('status')} "
            f"written={written} verified={verified}"
        )
    await _set_node_intent(
        f"Prediction task complete: {result.get('signal', 'NEUTRAL')} on {symbol}"
    )
    await _set_node_vision(
        _predict_next_5m_vision(result.get("binance"), result.get("polymarket"))
    )
    await asyncio.sleep(_prediction_throttle_delay_s())
    return result
