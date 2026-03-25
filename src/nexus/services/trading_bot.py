"""
Trading bot — cross-venue tick alignment for arbitrage sampling.

* Redis list ``trading:ticks`` is the canonical store for paired Binance / Polymarket
  samples (replaces legacy ``nexus:arbitrage:timeseries``).
* Polymarket side targets **Bitcoin price action** intraday contracts (Gamma title
  *Bitcoin Up or Down — …*), i.e. Chainlink BTC/USD window markets — not the older
  *Will Bitcoin hit $X?* family unless you override via env.
* Polymarket outcome prices are **probabilities on 0–1** (e.g. ``0.27`` ≡ 27 % for
  the **Up** token). These are compared to Binance top-of-book imbalance and spread
  (volatility proxy) via ``compare_poly_probability_vs_binance_volatility``.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from typing import Any

import httpx
import structlog

log = structlog.get_logger(__name__)

TRADING_TICKS_REDIS_KEY = "trading:ticks"
LEGACY_ARBITRAGE_TIMESERIES_KEY = "nexus:arbitrage:timeseries"

POLYMARKET_GAMMA_URL = "https://gamma-api.polymarket.com/markets"
POLYMARKET_CLOB_BOOK_URL = "https://clob.polymarket.com/book"

BINANCE_TICKER_URL = "https://api.binance.com/api/v3/ticker/price"
BINANCE_DEPTH_URL = "https://api.binance.com/api/v3/depth"

# Canonical cross-venue label vs Binance spot symbol (USDT pair tracks USD index).
ASSET_PAIR = "BTC/USD"
BINANCE_SPOT_SYMBOL = "BTCUSDT"
ORDER_BOOK_LIMIT = 20

# Safety / strike calibration (0–1). Former default 0.95 → 0.70 for sandbox-friendly gates.
MIN_CONFIDENCE_SCORE = float(os.environ.get("NEXUS_MIN_CONFIDENCE_SCORE", "0.70"))
MASTER_STRIKE_MIN_CONFIDENCE_PCT = MIN_CONFIDENCE_SCORE * 100.0

# Optional: pin a specific Gamma market id (string) for BTC price-action.
POLYMARKET_BTC_PRICE_ACTION_MARKET_ID = (os.environ.get("POLYMARKET_BTC_PRICE_ACTION_MARKET_ID") or "").strip()

# "Bitcoin Price Action" on Polymarket is implemented as *Bitcoin Up or Down* 15m windows.
BTC_PRICE_ACTION_Q_RE = re.compile(r"(?i)^bitcoin up or down\b")

HIT_Q_RE = re.compile(r"(?i)will bitcoin hit")
STRIKE_RE = re.compile(r"\$\s*([0-9,]+(?:\.[0-9]+)?)")


def poly_price_to_probability(yes_or_up_price: float) -> dict[str, float]:
    """
    Map a Polymarket outcome price to an explicit probability.

    Example: ``0.27`` → 27 % for that outcome (Up / Yes token).
    """
    p = float(yes_or_up_price)
    p = max(0.0, min(1.0, p))
    return {"probability_01": p, "probability_pct": round(p * 100.0, 4)}


def compare_poly_probability_vs_binance_volatility(
    poly_up_probability_01: float,
    binance_buy_pct: float,
    spread_bps: float | None,
) -> dict[str, Any]:
    """
    Relate implied **Up** probability (0–1) to Binance order-book pressure.

    ``buy_pct`` is bid-notional share of top-N depth (same semantics as prediction).
    ``spread_bps`` is (best_ask − best_bid) / mid · 10_000 when available; wider
    spread ⇒ higher friction / vol proxy and slightly damps alignment.
    """
    prob = poly_price_to_probability(poly_up_probability_01)
    ob_pressure = (float(binance_buy_pct) - 0.5) * 2.0
    implied_edge = float(prob["probability_01"]) - 0.5
    vol_penalty = 0.0
    if spread_bps is not None and spread_bps > 0:
        vol_penalty = min(1.0, float(spread_bps) / 50.0) * 0.1
    alignment = ob_pressure * implied_edge - vol_penalty
    return {
        "poly_probability_01": prob["probability_01"],
        "poly_probability_pct": prob["probability_pct"],
        "binance_buy_pct": float(binance_buy_pct),
        "spread_bps": spread_bps,
        "alignment_score": round(alignment, 6),
    }


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


def _parse_gamma_hit_market(market: dict[str, Any]) -> dict[str, Any] | None:
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

    return {
        "market_question": q,
        "strike_usd": round(strike, 2),
        "yes_price": round(yes_price, 4),
        "no_price": round(no_price, 4),
        "market_id": market.get("id"),
        "volume": market.get("volume"),
        "clob_token_ids": clob_token_ids,
    }


def _parse_gamma_btc_price_action_market(market: dict[str, Any]) -> dict[str, Any] | None:
    q = market.get("question") or ""
    if not BTC_PRICE_ACTION_Q_RE.search(q):
        return None

    raw_prices = market.get("outcomePrices", '["0.5","0.5"]')
    if isinstance(raw_prices, str):
        try:
            raw_prices = json.loads(raw_prices)
        except Exception:
            raw_prices = ["0.5", "0.5"]
    up_price = float(raw_prices[0]) if raw_prices else 0.5
    down_price = float(raw_prices[1]) if len(raw_prices) > 1 else 0.5

    raw_token_ids = market.get("clobTokenIds", "[]")
    if isinstance(raw_token_ids, str):
        try:
            raw_token_ids = json.loads(raw_token_ids)
        except Exception:
            raw_token_ids = []
    clob_token_ids: list[str] = raw_token_ids if isinstance(raw_token_ids, list) else []

    return {
        "market_question": q,
        "yes_price": round(up_price, 4),
        "no_price": round(down_price, 4),
        "strike_usd": None,
        "market_id": market.get("id"),
        "volume": market.get("volume"),
        "clob_token_ids": clob_token_ids,
        "condition_id": market.get("conditionId"),
        "outcome_labels": ("Up", "Down"),
    }


def normalize_poly_yes_with_orderbook_depth(
    yes_price: float,
    buy_pct: float,
    *,
    strength: float = 0.12,
) -> float:
    """
    Nudge Polymarket **Up** / Yes probability (0–1) toward Binance bid-side depth pressure.

    ``buy_pct`` is the bid-notional share of total top-N book (same as prediction.fetch_binance_data).
    """
    pressure = (float(buy_pct) - 0.5) * 2.0
    adj = float(yes_price) + pressure * float(strength)
    return round(max(0.0, min(1.0, adj)), 4)


def _gamma_market_to_contract(parsed: dict[str, Any], *, market_found: bool) -> dict[str, Any]:
    return {
        "market_found": market_found,
        "market_question": parsed.get("market_question") if market_found else None,
        "yes_price": parsed.get("yes_price") if market_found else None,
        "no_price": parsed.get("no_price") if market_found else None,
        "strike_usd": parsed.get("strike_usd") if market_found else None,
        "market_id": parsed.get("market_id") if market_found else None,
        "volume": parsed.get("volume") if market_found else None,
        "clob_token_ids": (parsed.get("clob_token_ids") or []) if market_found else [],
        "condition_id": parsed.get("condition_id") if market_found else None,
        "outcome_labels": parsed.get("outcome_labels") if market_found else None,
    }


async def fetch_polymarket_btc_price_action_contract(
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    """
    Resolve the active **Bitcoin Up or Down** (price-action) contract on Polymarket.

    Set ``POLYMARKET_BTC_PRICE_ACTION_MARKET_ID`` to pin a specific Gamma ``id``.
    Otherwise picks the highest-volume active matching market.
    """

    async def _fetch_one(c: httpx.AsyncClient, market_id: str) -> dict[str, Any] | None:
        res = await c.get(f"{POLYMARKET_GAMMA_URL}/{market_id}")
        if res.status_code == 404:
            return None
        res.raise_for_status()
        m = res.json()
        if not isinstance(m, dict):
            return None
        parsed = _parse_gamma_btc_price_action_market(m)
        return parsed

    async def _scan_list(c: httpx.AsyncClient) -> dict[str, Any]:
        params = {
            "closed": "false",
            "active": "true",
            "order": "volume",
            "ascending": "false",
            "limit": "120",
        }
        res = await c.get(POLYMARKET_GAMMA_URL, params=params)
        res.raise_for_status()
        markets = res.json()
        if not isinstance(markets, list) or not markets:
            return _gamma_market_to_contract({}, market_found=False)
        best: dict[str, Any] | None = None
        best_vol = -1.0
        for m in markets:
            if not isinstance(m, dict):
                continue
            parsed = _parse_gamma_btc_price_action_market(m)
            if not parsed:
                continue
            try:
                vol = float(m.get("volumeNum") or m.get("volume") or 0.0)
            except (TypeError, ValueError):
                vol = 0.0
            if vol > best_vol:
                best_vol = vol
                best = parsed
        if not best:
            return _gamma_market_to_contract({}, market_found=False)
        return _gamma_market_to_contract(best, market_found=True)

    async def _run(c: httpx.AsyncClient) -> dict[str, Any]:
        if POLYMARKET_BTC_PRICE_ACTION_MARKET_ID:
            parsed = await _fetch_one(c, POLYMARKET_BTC_PRICE_ACTION_MARKET_ID)
            if parsed:
                return _gamma_market_to_contract(parsed, market_found=True)
            log.warning(
                "polymarket_price_action_id_not_found",
                market_id=POLYMARKET_BTC_PRICE_ACTION_MARKET_ID,
            )
        return await _scan_list(c)

    if client is not None:
        return await _run(client)
    async with httpx.AsyncClient(timeout=10.0) as c:
        return await _run(c)


async def fetch_polymarket_btc_hit_contract(
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    """
    Highest-volume **active** *Will Bitcoin hit $…?* market from Gamma.

    Kept for legacy call sites; the prediction pipeline uses
    ``fetch_polymarket_btc_price_action_contract`` for BTC/USD price-action.
    """
    params = {
        "q": "Will Bitcoin hit",
        "active": "true",
        "closed": "false",
        "order": "volume",
        "ascending": "false",
        "limit": "20",
    }

    async def _run(c: httpx.AsyncClient) -> dict[str, Any]:
        res = await c.get(POLYMARKET_GAMMA_URL, params=params)
        res.raise_for_status()
        markets = res.json()
        if not isinstance(markets, list) or not markets:
            return {
                "market_found": False,
                "market_question": None,
                "yes_price": None,
                "no_price": None,
                "strike_usd": None,
            }
        for m in markets:
            if not isinstance(m, dict):
                continue
            parsed = _parse_gamma_hit_market(m)
            if not parsed:
                continue
            return {
                "market_found": True,
                "market_question": parsed["market_question"],
                "yes_price": parsed["yes_price"],
                "no_price": parsed["no_price"],
                "strike_usd": parsed["strike_usd"],
                "market_id": parsed.get("market_id"),
                "volume": parsed.get("volume"),
                "clob_token_ids": parsed.get("clob_token_ids") or [],
            }
        return {
            "market_found": False,
            "market_question": None,
            "yes_price": None,
            "no_price": None,
            "strike_usd": None,
        }

    if client is not None:
        return await _run(client)
    async with httpx.AsyncClient(timeout=10.0) as c:
        return await _run(c)


async def fetch_polymarket_clob_orderbook(
    token_id: str,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    """Full CLOB order book for a Polymarket outcome token."""

    async def _run(c: httpx.AsyncClient) -> dict[str, Any]:
        res = await c.get(POLYMARKET_CLOB_BOOK_URL, params={"token_id": str(token_id)})
        res.raise_for_status()
        data = res.json()
        if not isinstance(data, dict):
            return {"bids": [], "asks": [], "raw": data}
        return data

    if client is not None:
        return await _run(client)
    async with httpx.AsyncClient(timeout=15.0) as c:
        return await _run(c)


def summarize_polymarket_orderbook(book: dict[str, Any]) -> dict[str, Any]:
    """Compact bid/ask notionals + best prices for Redis telemetry."""
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    bid_n = ask_n = 0.0
    best_bid = best_ask = None
    try:
        for row in bids[:50]:
            p, s = float(row["price"]), float(row["size"])
            bid_n += p * s
            best_bid = p if best_bid is None else max(best_bid, p)
    except (KeyError, TypeError, ValueError):
        pass
    try:
        for row in asks[:50]:
            p, s = float(row["price"]), float(row["size"])
            ask_n += p * s
            best_ask = p if best_ask is None else min(best_ask, p)
    except (KeyError, TypeError, ValueError):
        pass
    spread_bps = None
    if best_bid is not None and best_ask is not None and best_ask > 0 and best_bid > 0:
        mid = (best_bid + best_ask) / 2.0
        if mid > 0:
            spread_bps = round((best_ask - best_bid) / mid * 10_000.0, 4)
    tot = bid_n + ask_n
    bid_share = (bid_n / tot) if tot > 0 else 0.5
    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread_bps": spread_bps,
        "bid_notional_top50": round(bid_n, 4),
        "ask_notional_top50": round(ask_n, 4),
        "poly_bid_share": round(bid_share, 4),
        "bid_levels": len(bids),
        "ask_levels": len(asks),
    }


async def fetch_binance_orderbook_snapshot(
    symbol: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    """
    Binance spot mid, top-of-book spread (bps), and bid-notional share (volatility context).
    """
    sym = (symbol or BINANCE_SPOT_SYMBOL).upper()

    async def _run(c: httpx.AsyncClient) -> dict[str, Any]:
        price_res, depth_res = await asyncio.gather(
            c.get(BINANCE_TICKER_URL, params={"symbol": sym}),
            c.get(BINANCE_DEPTH_URL, params={"symbol": sym, "limit": ORDER_BOOK_LIMIT}),
        )
        price_res.raise_for_status()
        depth_res.raise_for_status()
        mid = float(price_res.json()["price"])
        depth_data = depth_res.json()
        bids = depth_data.get("bids") or []
        asks = depth_data.get("asks") or []
        total_bids = sum(float(qty) for _, qty in bids)
        total_asks = sum(float(qty) for _, qty in asks)
        total_vol = total_bids + total_asks
        buy_pct = (total_bids / total_vol) if total_vol > 0 else 0.5
        best_bid = float(bids[0][0]) if bids else None
        best_ask = float(asks[0][0]) if asks else None
        spread_bps = None
        if best_bid and best_ask and mid > 0:
            spread_bps = round((best_ask - best_bid) / mid * 10_000.0, 4)
        return {
            "symbol": sym,
            "price": mid,
            "buy_pct": round(buy_pct, 4),
            "spread_bps": spread_bps,
            "best_bid": best_bid,
            "best_ask": best_ask,
        }

    if client is not None:
        return await _run(client)
    async with httpx.AsyncClient(timeout=10.0) as c:
        return await _run(c)
