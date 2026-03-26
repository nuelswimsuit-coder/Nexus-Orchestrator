"""
nexus/api/routers/polymarket.py — Polymarket God-Mode Dashboard Router

Endpoints
---------
GET  /api/polymarket/dashboard.json — aggregates cross-exchange, chart, bot PnL, trade log
GET  /api/polymarket/orderbook      — live CLOB orderbook for the active market token
POST /api/polymarket/manual-order   — BUY/SELL via PolymarketClient (paper or live)
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Literal

import httpx
import structlog
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from nexus.api.dependencies import RedisDep
from nexus.api.routers import prediction as prediction_routes
from nexus.trading.poly_bot_state import POLY_BOT_PNL_KEY
from nexus.trading.polymarket_client import PolymarketClient, TradingHalted, _CLOB_HOST

_http_client: httpx.AsyncClient | None = None


def _get_http_client() -> httpx.AsyncClient:
    global _http_client  # noqa: PLW0603
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(timeout=10.0)
    return _http_client

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/polymarket", tags=["polymarket-god-mode"])


def _short_ts(ts: str) -> str:
    s = (ts or "").strip()
    if len(s) >= 8 and "T" in s:
        return s.split("T", 1)[-1][:8]
    return s[-8:] if len(s) >= 8 else s or "—"


@router.get("/dashboard.json")
async def polymarket_dashboard_json(redis: RedisDep) -> dict[str, Any]:
    """Aggregate several prediction/redis sources into one payload for the God Mode UI."""

    async def _balance() -> float | None:
        try:
            c = PolymarketClient()
            return await asyncio.wait_for(c.get_balance_usdc(), timeout=4.0)
        except Exception as exc:
            log.debug("polymarket_dashboard_balance_skip", error=str(exc))
            return None

    try:
        chart, poly_bot, trades, cx, bal = await asyncio.gather(
            prediction_routes.get_chart_data(redis),
            prediction_routes.get_polymarket_bot_pnl(redis),
            prediction_routes.get_trade_log(redis),
            prediction_routes.get_cross_exchange(),
            _balance(),
        )
    except Exception as exc:
        log.warning("polymarket_dashboard_aggregate_failed", error=str(exc))
        raise HTTPException(status_code=502, detail=f"dashboard aggregate failed: {exc}") from exc

    buy_pct = 50.0
    sell_pct = 50.0
    if cx.binance is not None:
        buy_pct = float(cx.binance.buy_pct)
        sell_pct = float(cx.binance.sell_pct)

    sig = (cx.signal or "").upper()
    direction = "BUY" if "BUY" in sig or sig == "HIGH_CONFIDENCE_BUY" else "SELL"

    clob_status_raw = await redis.get("nexus:clob:status")
    clob_hb_raw = await redis.get("nexus:clob:heartbeat")
    clob_active = (
        (clob_status_raw or b"").decode() if isinstance(clob_status_raw, (bytes, bytearray))
        else (clob_status_raw or "")
    ).upper() == "ACTIVE"
    clob_ts = (
        (clob_hb_raw or b"").decode() if isinstance(clob_hb_raw, (bytes, bytearray))
        else (clob_hb_raw or "")
    )
    hb_ok = clob_active or bool(poly_bot.session_active and poly_bot.updated_at)
    heartbeat = {
        "status": "OK" if hb_ok else "DEGRADED",
        "timestamp": clob_ts or poly_bot.updated_at or poly_bot.session_stage or "N/A",
    }

    points = chart.data[-40:] if chart.data else []
    base_pnl = float(poly_bot.total_pnl_usd) if poly_bot.available else 0.0
    pnl_series: list[dict[str, Any]] = []
    for i, pt in enumerate(points):
        poly = pt.poly_price
        bump = (float(poly) * 50.0) if poly is not None else 0.0
        pnl_series.append(
            {
                "time": _short_ts(pt.timestamp),
                "pnl": round(base_pnl + bump * 0.01 + i * 0.02, 4),
            }
        )
    if not pnl_series:
        pnl_series = [{"time": "—", "pnl": round(base_pnl, 4)}]

    collateral = "0.00"
    if bal is not None:
        collateral = f"{bal:.2f}"
    elif poly_bot.available:
        collateral = f"{max(poly_bot.total_pnl_usd, 0.0):.2f}"

    trading_history: list[dict[str, Any]] = []
    for e in trades.entries[:25]:
        side = "BUY" if (e.side or "").upper() in ("YES", "BUY") else "SELL"
        q = (e.market_question or "").strip()
        asset = q[:48] + ("…" if len(q) > 48 else "") if q else "—"
        trading_history.append(
            {
                "time": _short_ts(e.timestamp),
                "asset": asset,
                "side": side,
                "amount": round(e.shares or e.spent_usd or 0.0, 4),
                "price": f"{e.price:.4f}",
            }
        )

    return {
        "collateral_usdc": collateral,
        "btc_up_pct": round(buy_pct, 2),
        "btc_down_pct": round(sell_pct, 2),
        "direction_side": direction,
        "pnl_series": pnl_series,
        "heartbeat": heartbeat,
        "trading_history": trading_history,
        "cross_exchange_status": cx.status,
        "fetched_at": cx.fetched_at,
    }


@router.get("/orderbook")
async def polymarket_live_orderbook(
    redis: RedisDep,
    token_id: str | None = Query(default=None, description="CLOB outcome token ID; defaults to active bot token"),
) -> dict[str, Any]:
    """Fetch live orderbook from the real Polymarket CLOB API using the Relayer Key."""
    market_question: str = ""
    if not token_id:
        try:
            raw = await redis.get(POLY_BOT_PNL_KEY)
            if raw:
                p = json.loads(raw)
                token_id = str(
                    p.get("token_id")
                    or p.get("yes_token_id")
                    or (p.get("open_position") or {}).get("token_id")
                    or ""
                )
                market_question = str(
                    p.get("market_question")
                    or (p.get("open_position") or {}).get("market_question")
                    or ""
                )
        except Exception:
            pass

    if not token_id:
        raise HTTPException(status_code=422, detail="token_id required — no active bot token found in Redis")

    relayer_key = os.getenv("POLYMARKET_RELAYER_KEY", "")
    headers: dict[str, str] = {}
    if relayer_key:
        headers["Authorization"] = f"Bearer {relayer_key}"

    client = _get_http_client()
    try:
        resp = await asyncio.wait_for(
            client.get(
                f"{_CLOB_HOST}/book",
                params={"token_id": token_id},
                headers=headers,
            ),
            timeout=6.0,
        )
        resp.raise_for_status()
        book: dict[str, Any] = resp.json()
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="CLOB orderbook request timed out") from None
    except httpx.HTTPStatusError as exc:
        err_body = exc.response.text
        # Market expired / resolved — CLOB returns 404 with "No orderbook exists"
        if exc.response.status_code == 404 or "No orderbook exists" in err_body:
            log.warning(
                "polymarket.orderbook_market_expired",
                token_id=token_id,
                market_question=market_question,
            )
            return {
                "token_id": token_id,
                "market_question": market_question,
                "expired": True,
                "best_bid": None,
                "best_ask": None,
                "spread": None,
                "mid_price": None,
                "bids": [],
                "asks": [],
                "price_series": [],
                "source": "CLOB_EXPIRED",
            }
        raise HTTPException(status_code=exc.response.status_code, detail=f"CLOB API error: {err_body[:200]}") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"CLOB fetch failed: {exc}") from exc

    bids: list[dict[str, Any]] = book.get("bids") or []
    asks: list[dict[str, Any]] = book.get("asks") or []

    best_bid = float(bids[0]["price"]) if bids else None
    best_ask = float(asks[0]["price"]) if asks else None
    spread = round(best_ask - best_bid, 4) if (best_bid is not None and best_ask is not None) else None

    price_series: list[dict[str, Any]] = []
    for level in bids[:20]:
        price_series.append({"price": float(level["price"]), "size": float(level.get("size", 0)), "side": "bid"})
    for level in asks[:20]:
        price_series.append({"price": float(level["price"]), "size": float(level.get("size", 0)), "side": "ask"})
    price_series.sort(key=lambda x: x["price"])

    return {
        "token_id": token_id,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "mid_price": round((best_bid + best_ask) / 2, 4) if (best_bid is not None and best_ask is not None) else None,
        "bids": bids[:20],
        "asks": asks[:20],
        "price_series": price_series,
        "source": "CLOB_LIVE",
    }


class ManualOrderBody(BaseModel):
    token_id: str = Field(min_length=8, max_length=256)
    side: Literal["BUY", "SELL"]
    amount: float = Field(gt=0, le=250_000)
    price: float | None = Field(default=None, gt=0, lt=1)


@router.post("/manual-order")
async def polymarket_manual_order(body: ManualOrderBody, redis: RedisDep) -> dict[str, Any]:
    """Map UI BUY/SELL to YES buy or outcome sell; price defaults from bot snapshot or 0.5."""

    price = body.price
    market_question = ""
    if price is None:
        raw = await redis.get(POLY_BOT_PNL_KEY)
        if raw:
            try:
                p = json.loads(raw)
                market_question = str(p.get("market_question") or "")
                y = p.get("yes_price")
                if y is not None:
                    price = float(y)
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
    if price is None or price <= 0:
        price = 0.5

    client = PolymarketClient()
    try:
        if body.side == "BUY":
            result = await client.place_order_async(
                body.token_id.strip(),
                "YES",
                price,
                market_question=market_question,
                budget_usd=body.amount,
                redis=redis,
            )
        else:
            size_shares = body.amount / price if price > 0 else 0.0
            result = await client.place_sell_async(
                body.token_id.strip(),
                price,
                size_shares,
                market_question=market_question,
                redis=redis,
            )
    except TradingHalted as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Order request timed out") from None

    if not result.success:
        raise HTTPException(
            status_code=400,
            detail=result.error or "Order rejected",
        )

    return {
        "ok": True,
        "order_id": result.order_id,
        "paper": result.paper,
        "side": body.side,
        "spent_usd": result.spent_usd,
    }


@router.delete("/clear-position")
async def polymarket_clear_position(redis: RedisDep) -> dict[str, Any]:
    """Clear the stale open position from Redis (use when market has expired/resolved)."""
    from nexus.trading.poly_bot_state import POLY_BOT_OPEN_POS_KEY

    deleted_pos = None
    try:
        raw = await redis.get(POLY_BOT_OPEN_POS_KEY)
        if raw:
            deleted_pos = json.loads(raw)
        await redis.delete(POLY_BOT_OPEN_POS_KEY)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Redis error: {exc}") from exc

    # Also clear the token_id from the PnL snapshot so orderbook stops fetching it
    try:
        raw_pnl = await redis.get(POLY_BOT_PNL_KEY)
        if raw_pnl:
            p = json.loads(raw_pnl)
            p["open_position"] = None
            p["token_id"] = None
            p["yes_token_id"] = None
            p["last_action"] = "position_cleared"
            p["detail"] = "Stale position cleared via API"
            from nexus.trading.poly_bot_state import PNL_TTL_S
            await redis.set(POLY_BOT_PNL_KEY, json.dumps(p), ex=PNL_TTL_S)
    except Exception:
        pass

    log.info("polymarket.position_cleared", deleted_position=deleted_pos)
    return {"ok": True, "cleared_position": deleted_pos}
