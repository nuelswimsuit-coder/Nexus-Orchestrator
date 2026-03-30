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
import time
from pathlib import Path
from typing import Any, Literal

import httpx
import structlog
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from nexus.api.dependencies import RedisDep
from nexus.api.polymarket_manual_errors import MANUAL_ORDER_ENRICH_REV, enrich_manual_order_error
from nexus.api.routers import prediction as prediction_routes
from nexus.trading.poly_bot_state import POLY_BOT_PNL_KEY
from nexus.trading.polymarket_client import (
    PolymarketClient,
    TradingHalted,
    _CLOB_HOST,
    get_polymarket_clob_funder_address,
)
from nexus.trading.wallet_manager import get_polymarket_private_key

_http_client: httpx.AsyncClient | None = None


def _get_http_client() -> httpx.AsyncClient:
    global _http_client  # noqa: PLW0603
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(timeout=10.0)
    return _http_client

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/polymarket", tags=["polymarket"])

# #region agent log
_DEBUG_LOG_PATH = Path(__file__).resolve().parents[3] / "debug-7ec1ca.log"


def _agent_debug_ndjson(hypothesis_id: str, location: str, message: str, data: dict[str, Any]) -> None:
    try:
        payload = {
            "sessionId": "7ec1ca",
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
            "runId": os.environ.get("NEXUS_DEBUG_RUN_ID", "pre-fix"),
        }
        with _DEBUG_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=str) + "\n")
    except Exception:
        pass


# #endregion

_MANUAL_ORDER_HEADERS = {"X-Nexus-Manual-Order-Enrich": MANUAL_ORDER_ENRICH_REV}


def _short_ts(ts: str) -> str:
    s = (ts or "").strip()
    if len(s) >= 8 and "T" in s:
        return s.split("T", 1)[-1][:8]
    return s[-8:] if len(s) >= 8 else s or "—"


def _extract_position_clob_token_id(p: dict[str, Any]) -> str:
    """Best-effort CLOB outcome token id from a Data API `/positions` row."""
    for key in ("asset", "assetId", "tokenId", "token_id", "tokenID", "clobTokenId"):
        v = p.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    raw: Any = p.get("clobTokenIds")
    if isinstance(raw, str) and raw.strip():
        try:
            raw = json.loads(raw)
        except Exception:
            raw = []
    if isinstance(raw, list) and raw:
        oi = p.get("outcomeIndex")
        if isinstance(oi, int) and 0 <= oi < len(raw):
            tok = raw[oi]
        else:
            outcome = str(p.get("outcome") or "Yes").strip().lower()
            idx = 1 if outcome in ("no", "down", "n") else 0
            idx = min(idx, len(raw) - 1)
            tok = raw[idx]
        if tok is not None and str(tok).strip():
            return str(tok).strip()
    return ""


@router.get("/dashboard.json")
async def polymarket_dashboard_json(redis: RedisDep) -> dict[str, Any]:
    """Aggregate several prediction/redis sources into one payload for the God Mode UI."""

    async def _balance() -> float | None:
        try:
            c = PolymarketClient()
            result = await asyncio.wait_for(c.get_balance_usdc(), timeout=4.0)
            return result
        except Exception as exc:
            log.debug("polymarket_dashboard_balance_skip", error=str(exc))
            return None

    async def _portfolio_value() -> tuple[float, float, float, list[dict[str, Any]]]:
        """Fetch portfolio value + cash + positions from Polymarket data API.

        Uses POLYMARKET_PORTFOLIO_ADDRESS if set (personal account),
        otherwise falls back to POLYMARKET_SIGNER_ADDRESS (bot wallet).
        Returns (portfolio_total, cash, positions_value, positions_list).
        """
        address = (
            os.getenv("POLYMARKET_PORTFOLIO_ADDRESS", "").strip()
            or os.getenv("POLYMARKET_SIGNER_ADDRESS", "").strip()
        )
        # #region agent log
        _agent_debug_ndjson(
            "H1",
            "polymarket.py:_portfolio_value",
            "env and address resolution",
            {
                "has_portfolio_env": bool((os.getenv("POLYMARKET_PORTFOLIO_ADDRESS") or "").strip()),
                "has_signer_env": bool((os.getenv("POLYMARKET_SIGNER_ADDRESS") or "").strip()),
                "has_wallet_private_key": bool(get_polymarket_private_key()),
                "has_relayer_key_raw": bool((os.getenv("POLYMARKET_RELAYER_KEY") or "").strip()),
                "address_len_for_data_api": len(address),
            },
        )
        # #endregion
        if not address:
            return (0.0, 0.0, 0.0, [])
        addr = address.lower()
        try:
            client = _get_http_client()
            value_resp, pos_resp = await asyncio.gather(
                asyncio.wait_for(
                    client.get(f"https://data-api.polymarket.com/value?user={addr}"),
                    timeout=5.0,
                ),
                asyncio.wait_for(
                    client.get(f"https://data-api.polymarket.com/positions?user={addr}&sizeThreshold=.01&limit=50"),
                    timeout=5.0,
                ),
                return_exceptions=True,
            )
            # #region agent log
            _agent_debug_ndjson(
                "H2",
                "polymarket.py:_portfolio_value",
                "data-api http outcomes",
                {
                    "value_is_exc": isinstance(value_resp, Exception),
                    "value_status": getattr(value_resp, "status_code", None),
                    "pos_is_exc": isinstance(pos_resp, Exception),
                    "pos_status": getattr(pos_resp, "status_code", None),
                },
            )
            # #endregion
            total_val = 0.0
            if not isinstance(value_resp, Exception) and value_resp.status_code == 200:
                vdata = value_resp.json()
                if isinstance(vdata, list) and vdata:
                    total_val = float(vdata[0].get("value", 0) or 0)

            positions_value = 0.0
            positions_list: list[dict[str, Any]] = []
            if not isinstance(pos_resp, Exception) and pos_resp.status_code == 200:
                raw_positions = pos_resp.json()
                if isinstance(raw_positions, list):
                    for p in raw_positions:
                        cur_val = float(
                            p.get("currentValue") or p.get("curValue") or p.get("value") or 0
                        )
                        positions_value += cur_val
                        token_id = _extract_position_clob_token_id(p)
                        slug = str(p.get("slug") or "").strip()
                        positions_list.append({
                            "title": str(p.get("title") or p.get("slug") or "")[:60],
                            "slug": slug,
                            "outcome": str(p.get("outcome") or "YES"),
                            "size": float(p.get("size") or 0),
                            "avg_price": float(p.get("avgPrice") or 0),
                            "cur_price": float(p.get("curPrice") or 0),
                            "current_value": cur_val,
                            "cash_pnl": float(p.get("cashPnl") or 0),
                            "percent_pnl": float(p.get("percentPnl") or 0),
                            "end_date": str(p.get("endDate") or ""),
                            "token_id": token_id,
                        })

            # /value can be 0 while /positions still lists markets; avoid a totally empty UI
            if total_val <= 0 and positions_value > 0:
                total_val = positions_value

            # Cash = total portfolio value minus open positions
            cash = max(total_val - positions_value, 0.0)
            # #region agent log
            _agent_debug_ndjson(
                "H3",
                "polymarket.py:_portfolio_value",
                "parsed portfolio aggregates",
                {
                    "total_val": total_val,
                    "positions_value": positions_value,
                    "positions_list_len": len(positions_list),
                    "cash": cash,
                },
            )
            # #endregion
            return (total_val, cash, positions_value, positions_list)
        except Exception as exc:
            log.debug("polymarket_dashboard_portfolio_skip", error=str(exc))
            # #region agent log
            _agent_debug_ndjson(
                "H2",
                "polymarket.py:_portfolio_value",
                "portfolio fetch exception",
                {"exc_type": type(exc).__name__},
            )
            # #endregion
        return (0.0, 0.0, 0.0, [])

    try:
        chart, poly_bot, trades, cx, bal, portfolio_tuple = await asyncio.gather(
            prediction_routes.get_chart_data(redis),
            prediction_routes.get_polymarket_bot_pnl(redis),
            prediction_routes.get_trade_log(redis),
            prediction_routes.get_cross_exchange(),
            _balance(),
            _portfolio_value(),
        )
    except Exception as exc:
        log.warning("polymarket_dashboard_aggregate_failed", error=str(exc))
        raise HTTPException(status_code=502, detail=f"dashboard aggregate failed: {exc}") from exc

    portfolio_val, portfolio_cash, portfolio_positions, portfolio_positions_list = portfolio_tuple

    # #region agent log
    _agent_debug_ndjson(
        "H4",
        "polymarket.py:polymarket_dashboard_json",
        "dashboard aggregate snapshot",
        {
            "portfolio_val": portfolio_val,
            "clob_balance": bal,
            "positions_list_len": len(portfolio_positions_list),
            "collateral_branch": "portfolio"
            if (portfolio_val is not None and portfolio_val > 0)
            else "clob"
            if (bal is not None and bal > 0)
            else "bot_or_zero",
        },
    )
    # #endregion

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

    # Prefer: portfolio_value (data-api) > CLOB balance > bot PnL
    collateral = "0.00"
    if portfolio_val is not None and portfolio_val > 0:
        collateral = f"{portfolio_val:.2f}"
    elif bal is not None and bal > 0:
        collateral = f"{bal:.2f}"
    elif poly_bot.available and poly_bot.total_pnl_usd > 0:
        collateral = f"{poly_bot.total_pnl_usd:.2f}"

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

    signer = os.getenv("POLYMARKET_SIGNER_ADDRESS", "")

    portfolio_address = (
        os.getenv("POLYMARKET_PORTFOLIO_ADDRESS", "").strip()
        or signer
    )

    # ── Break-even / total deposited tracker ──────────────────────────────────
    # Stored in Redis as a float so it persists across restarts.
    # The frontend/bot can POST /api/polymarket/set-deposit to update it.
    _DEPOSIT_KEY = "nexus:poly:total_deposited"
    try:
        dep_raw = await redis.get(_DEPOSIT_KEY)
        total_deposited = float((dep_raw or b"0").decode() if isinstance(dep_raw, (bytes, bytearray)) else (dep_raw or "0"))
    except Exception:
        total_deposited = 0.0

    # Realized P&L = sum of cashPnl across all positions
    realized_pnl = sum(p.get("cash_pnl", 0) for p in portfolio_positions_list)
    # Withdrawn amount stored in Redis
    _WITHDRAWN_KEY = "nexus:poly:total_withdrawn"
    try:
        wd_raw = await redis.get(_WITHDRAWN_KEY)
        total_withdrawn = float((wd_raw or b"0").decode() if isinstance(wd_raw, (bytes, bytearray)) else (wd_raw or "0"))
    except Exception:
        total_withdrawn = 0.0

    # Break-even: current_value + withdrawn - deposited
    current_effective = portfolio_val if portfolio_val > 0 else (bal or 0.0)
    break_even_delta = current_effective + total_withdrawn - total_deposited

    return {
        "collateral_usdc": collateral,
        "portfolio_value": portfolio_val,
        "portfolio_cash": portfolio_cash,
        "portfolio_positions": portfolio_positions,
        "portfolio_positions_list": portfolio_positions_list,
        "portfolio_address": portfolio_address,
        "clob_balance": bal or 0.0,
        "btc_up_pct": round(buy_pct, 2),
        "btc_down_pct": round(sell_pct, 2),
        "direction_side": direction,
        "pnl_series": pnl_series,
        "heartbeat": heartbeat,
        "trading_history": trading_history,
        "cross_exchange_status": cx.status,
        "fetched_at": cx.fetched_at,
        "signer_address": signer,
        "clob_funder_address": get_polymarket_clob_funder_address(),
        "total_deposited": total_deposited,
        "total_withdrawn": total_withdrawn,
        "break_even_delta": round(break_even_delta, 2),
        "realized_pnl": round(realized_pnl, 2),
        "manual_order_error_enrich": MANUAL_ORDER_ENRICH_REV,
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
        return {
            "no_position": True,
            "source": "NO_ACTIVE_POSITION",
            "market_question": "No active bot position",
            "bids": [],
            "asks": [],
            "mid_price": 0.0,
            "spread": 0.0,
            "price_series": [],
            "token_id": None,
        }

    # CLOB GET /book is public — do not send POLYMARKET_RELAYER_KEY as Bearer (hex private key
    # breaks some stacks and is not valid L2 auth; see Polymarket CLOB "Get book" docs).
    client = _get_http_client()
    try:
        resp = await asyncio.wait_for(
            client.get(
                f"{_CLOB_HOST}/book",
                params={"token_id": token_id},
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


@router.post("/manual-order", response_model=None)
async def polymarket_manual_order(
    body: ManualOrderBody, redis: RedisDep
) -> JSONResponse:
    """Map UI BUY/SELL to YES buy or outcome sell; price defaults from bot snapshot or 0.5.

    Every response includes header ``X-Nexus-Manual-Order-Enrich`` so DevTools can prove which
    API build handled the request (HTTPException would drop injected headers).
    """
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
        return JSONResponse(
            status_code=503,
            content={"detail": str(exc)},
            headers=_MANUAL_ORDER_HEADERS,
        )
    except asyncio.TimeoutError:
        return JSONResponse(
            status_code=504,
            content={"detail": "Order request timed out"},
            headers=_MANUAL_ORDER_HEADERS,
        )

    if not result.success:
        raw_err = result.error or "Order rejected"
        return JSONResponse(
            status_code=400,
            content={"detail": enrich_manual_order_error(raw_err, body.side)},
            headers=_MANUAL_ORDER_HEADERS,
        )

    return JSONResponse(
        status_code=200,
        content={
            "ok": True,
            "order_id": result.order_id,
            "paper": result.paper,
            "side": body.side,
            "spent_usd": result.spent_usd,
        },
        headers=_MANUAL_ORDER_HEADERS,
    )


class DepositBody(BaseModel):
    amount: float = Field(gt=0, le=10_000_000)


@router.post("/set-deposit")
async def polymarket_set_deposit(body: DepositBody, redis: RedisDep) -> dict[str, Any]:
    """Set the total amount deposited to Polymarket (for break-even tracking)."""
    await redis.set("nexus:poly:total_deposited", str(body.amount))
    return {"ok": True, "total_deposited": body.amount}


@router.post("/set-withdrawn")
async def polymarket_set_withdrawn(body: DepositBody, redis: RedisDep) -> dict[str, Any]:
    """Set the total amount withdrawn from Polymarket (for break-even tracking)."""
    await redis.set("nexus:poly:total_withdrawn", str(body.amount))
    return {"ok": True, "total_withdrawn": body.amount}


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
