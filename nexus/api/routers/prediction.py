"""
Prediction router — Cross-Exchange Predictor API

Endpoints
---------
GET  /api/prediction/cross-exchange
    Fetches live BTC/USDT data from Binance and the highest-volume active
    BTC market on Polymarket, then returns a unified signal payload.

GET  /api/prediction/chart-data
    Returns the last 30 paired (timestamp, binance_price, poly_price) data
    points collected by the background arbitrage collector (2 s cadence).
    Data is read from the Redis key nexus:arbitrage:timeseries.
    Newer points may include pred_mid / ci_low / ci_high (AI fair-value band).

GET  /api/prediction/polymarket-bot
    Live PnL and worker telemetry for the Polymarket BTC strike bot
    (Redis keys nexus:poly:pnl, nexus:poly:session_status; ticks from
    ``trading.polymarket_bot_tick`` on the Linux worker).

POST /api/prediction/manual-override
    Sets Redis halt flag, blocks new Polymarket orders, closes paper trades
    still marked ``open``.

POST /api/prediction/manual-override/clear  — remove halt flag.
GET  /api/prediction/manual-override/status — whether halt is active.

GET  /api/prediction/poly5m-scalper
    Dashboard snapshot for NEXUS-POLY-SCALPER-5M (Redis ``nexus:poly5m:dashboard``).

Ultimate 5m scalper UI also uses ``/api/scalper/*`` — see ``nexus.api.routers.scalper``.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import structlog
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from nexus.api.dependencies import RedisDep
from nexus.trading.config import (
    PAPER_TRADING,
    PAPER_TRADING_REDIS_KEY,
    PREDICTION_MANUAL_HALT_KEY,
)
from nexus.trading.runtime_mode import effective_paper_trading
from nexus.trading.poly_bot_state import POLY_BOT_PNL_KEY, POLY_BOT_STATUS_KEY

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/prediction", tags=["prediction"])

ARBITRAGE_TIMESERIES_KEY = "nexus:arbitrage:timeseries"
PAPER_STATS_KEY          = "nexus:stats:paper"


# ── Response schemas ───────────────────────────────────────────────────────────

class BinanceSnapshot(BaseModel):
    price:                float
    total_bids:           float
    total_asks:           float
    buy_pct:              float
    sell_pct:             float
    imbalance_direction:  str
    imbalance_strength:   float


class PolymarketSnapshot(BaseModel):
    """Gamma/CLOB snapshot; ``clob_token_ids`` index 0 = YES outcome token."""

    model_config = ConfigDict(extra="ignore")

    market_found:     bool
    market_question:  Optional[str]   = None
    yes_price:        Optional[float] = None
    no_price:         Optional[float] = None
    market_id:        Optional[str]   = None
    volume:           Optional[Any]   = None
    slug:             Optional[str]   = None
    clob_token_ids:   Optional[list[str]] = Field(default=None, description="CLOB token IDs; [0]=YES")


class PredictionCIBand(BaseModel):
    pred_mid: Optional[float] = None
    ci_low:   Optional[float] = None
    ci_high:  Optional[float] = None


class SignalThresholds(BaseModel):
    imbalance_threshold:    float
    polymarket_yes_ceiling: float


class CrossExchangeResponse(BaseModel):
    status:          str
    signal:          str
    signal_label:    str
    high_confidence: bool
    arbitrage_gap:   Optional[float] = None
    binance:         Optional[BinanceSnapshot]    = None
    polymarket:      Optional[PolymarketSnapshot] = None
    thresholds:      SignalThresholds
    errors:          list[str]
    duration_s:      float
    fetched_at:      str
    prediction_ci:   Optional[PredictionCIBand] = None


# ── Route ─────────────────────────────────────────────────────────────────────

class Poly5mScalperDashboardResponse(BaseModel):
    """Live snapshot from ``nexus:poly5m:dashboard`` (5m Polymarket scalper)."""

    model_config = ConfigDict(extra="ignore")

    updated_at: str | None = None
    event_id: str | None = None
    decision: str | None = None
    blocked: bool = False
    block_reason: str = ""
    btc_price: float | None = None
    velocity_pct_60s: float | None = None
    sentiment: dict[str, Any] = {}
    market_found: bool | None = None
    market_question: str | None = None
    yes_price: float | None = None
    paper_trading: bool = True
    wins: int = 0
    losses: int = 0
    win_loss_ratio: float | None = None
    loss_streak: int = 0
    trading_halted: bool = False
    velocity_feed_key: str | None = None
    project: str | None = None


@router.get(
    "/poly5m-scalper",
    response_model=Poly5mScalperDashboardResponse,
    summary="Poly 5m scalper: velocity, Openclaw/Telefix sentiment, win/loss",
)
async def get_poly5m_scalper_dashboard(redis: RedisDep) -> Poly5mScalperDashboardResponse:
    from nexus.master.services.poly_5m_scalper import read_dashboard_snapshot

    raw = await read_dashboard_snapshot(redis)
    return Poly5mScalperDashboardResponse(**raw)


@router.get(
    "/cross-exchange",
    response_model=CrossExchangeResponse,
    summary="Live cross-exchange BTC signal (Binance vs Polymarket)",
)
async def get_cross_exchange() -> CrossExchangeResponse:
    """
    Fetches real-time BTC/USDT price and order book from Binance, compares
    against the highest-volume active BTC market on Polymarket, and returns
    a unified signal.

    Signal matrix
    -------------
    HIGH_CONFIDENCE_BUY   order-book buy > 70 %  AND  Polymarket Yes < $0.52
    BUY_BIAS              order-book buy > 70 %  (Polymarket already caught up)
    POLYMARKET_LAGGING    Polymarket Yes < $0.52  (no order-book confirmation)
    NEUTRAL               no edge detected
    """
    from nexus.worker.tasks.prediction import run_cross_exchange_analysis

    try:
        result = await run_cross_exchange_analysis()
    except Exception as exc:
        log.exception("cross_exchange_endpoint_failed", error=str(exc))
        raise HTTPException(
            status_code=502,
            detail=f"Cross-exchange analysis failed: {exc}",
        ) from exc

    binance_raw   = result.get("binance")
    poly_raw      = result.get("polymarket")
    thresholds    = result["thresholds"]
    ci_raw        = result.get("prediction_ci") or {}

    return CrossExchangeResponse(
        status          = result["status"],
        signal          = result["signal"],
        signal_label    = result["signal_label"],
        high_confidence = result["high_confidence"],
        arbitrage_gap   = result.get("arbitrage_gap"),
        binance         = BinanceSnapshot(**binance_raw)     if binance_raw   else None,
        polymarket      = PolymarketSnapshot(**poly_raw)     if poly_raw      else None,
        thresholds      = SignalThresholds(**thresholds),
        errors          = result.get("errors", []),
        duration_s      = result["duration_s"],
        fetched_at      = result["fetched_at"],
        prediction_ci   = (
            PredictionCIBand(**ci_raw) if ci_raw.get("pred_mid") is not None else None
        ),
    )


# ── Polymarket bot (Nexus Poly Trader) ─────────────────────────────────────────

class PolymarketBotPnLResponse(BaseModel):
    available: bool = False
    realized_pnl_usd: float = 0.0
    unrealized_pnl_usd: float = 0.0
    total_pnl_usd: float = 0.0
    btc_spot: Optional[float] = None
    target_strike: Optional[float] = None
    yes_price: Optional[float] = None
    market_question: Optional[str] = None
    open_position: Optional[Dict[str, Any]] = None
    within_target_band: bool = False
    last_action: str = ""
    detail: str = ""
    session_active: bool = False
    session_stage: str = ""
    session_node_id: str = ""
    updated_at: str = ""


@router.get(
    "/polymarket-bot",
    response_model=PolymarketBotPnLResponse,
    summary="Live PnL and session telemetry for the Polymarket BTC strike bot",
)
async def get_polymarket_bot_pnl(redis: RedisDep) -> PolymarketBotPnLResponse:
    """
    Reads Redis keys written by the Linux worker ``trading.polymarket_bot_tick``
    handler (PnL snapshot + heartbeat).
    """
    raw_pnl = await redis.get(POLY_BOT_PNL_KEY)
    raw_st = await redis.get(POLY_BOT_STATUS_KEY)

    session_active = False
    session_stage = ""
    session_node_id = ""
    if raw_st:
        try:
            st = json.loads(raw_st)
            session_active = bool(st.get("active", False))
            session_stage = str(st.get("stage", ""))
            session_node_id = str(st.get("node_id", ""))
        except Exception:
            pass

    if not raw_pnl:
        return PolymarketBotPnLResponse(
            available=False,
            session_active=session_active,
            session_stage=session_stage,
            session_node_id=session_node_id,
        )

    try:
        p = json.loads(raw_pnl)
    except Exception:
        return PolymarketBotPnLResponse(
            available=False,
            session_active=session_active,
            session_stage=session_stage,
            session_node_id=session_node_id,
        )

    return PolymarketBotPnLResponse(
        available=True,
        realized_pnl_usd=float(p.get("realized_pnl_usd", 0)),
        unrealized_pnl_usd=float(p.get("unrealized_pnl_usd", 0)),
        total_pnl_usd=float(p.get("total_pnl_usd", 0)),
        btc_spot=p.get("btc_spot"),
        target_strike=p.get("target_strike"),
        yes_price=p.get("yes_price"),
        market_question=p.get("market_question"),
        open_position=p.get("open_position"),
        within_target_band=bool(p.get("within_target_band", False)),
        last_action=str(p.get("last_action", "")),
        detail=str(p.get("detail", "")),
        session_active=session_active,
        session_stage=session_stage,
        session_node_id=session_node_id,
        updated_at=str(p.get("updated_at", "")),
    )


# ── Chart data endpoint ────────────────────────────────────────────────────────

class ArbitrageDataPoint(BaseModel):
    timestamp:     str
    binance_price: Optional[float] = None
    poly_price:    Optional[float] = None
    pred_mid:      Optional[float] = None
    ci_low:        Optional[float] = None
    ci_high:       Optional[float] = None


class ArbitrageChartDataResponse(BaseModel):
    data:  List[ArbitrageDataPoint]
    total: int


@router.get(
    "/chart-data",
    response_model=ArbitrageChartDataResponse,
    summary="Arbitrage time-series: last 30 Binance vs Polymarket price points",
)
async def get_chart_data(redis: RedisDep) -> ArbitrageChartDataResponse:
    """
    Return the last 30 paired (timestamp, binance_price, poly_price) snapshots
    collected by the background arbitrage collector that runs every 2 seconds.

    The data is stored in the Redis list nexus:arbitrage:timeseries.  An empty
    list is returned if the collector has not yet produced any data.
    """
    raw_entries: list[str] = await redis.lrange(ARBITRAGE_TIMESERIES_KEY, 0, -1)

    points: list[ArbitrageDataPoint] = []
    for entry in raw_entries:
        try:
            obj = json.loads(entry)
            points.append(
                ArbitrageDataPoint(
                    timestamp     = obj["timestamp"],
                    binance_price = obj.get("binance_price"),
                    poly_price    = obj.get("poly_price"),
                    pred_mid      = obj.get("pred_mid"),
                    ci_low        = obj.get("ci_low"),
                    ci_high       = obj.get("ci_high"),
                )
            )
        except Exception as exc:
            log.warning("chart_data_parse_error", error=str(exc), raw=entry[:80])

    return ArbitrageChartDataResponse(data=points, total=len(points))


# ── Paper trading endpoints ────────────────────────────────────────────────────

class VirtualTradeEntry(BaseModel):
    id:                   str
    timestamp:            str
    signal:               str
    direction:            str
    entry_yes_price:      float
    entry_binance_price:  float
    virtual_amount_usd:   float
    potential_profit_usd: float
    market_question:      str
    market_id:            Optional[str] = None
    status:               str


class PaperTradesResponse(BaseModel):
    trades:               List[VirtualTradeEntry]
    total:                int
    total_virtual_pnl:    float
    paper_trading_enabled: bool


class TradingModeResponse(BaseModel):
    paper_trading:        bool
    virtual_trade_count:  int


@router.get(
    "/paper-trades",
    response_model=PaperTradesResponse,
    summary="Virtual trade history (paper trading mode)",
)
async def get_paper_trades(redis: RedisDep) -> PaperTradesResponse:
    """
    Return all virtual trades logged while PAPER_TRADING is True, stored in
    Redis under nexus:paper_trading:history (newest-first, capped at 100).

    Also returns the sum of potential profits across all open virtual trades
    as `total_virtual_pnl`.
    """
    raw_entries: list[str] = await redis.lrange(PAPER_TRADING_REDIS_KEY, 0, -1)

    trades: list[VirtualTradeEntry] = []
    total_pnl = 0.0
    for entry in raw_entries:
        try:
            obj = json.loads(entry)
            trade = VirtualTradeEntry(**obj)
            trades.append(trade)
            total_pnl += trade.potential_profit_usd
        except Exception as exc:
            log.warning("paper_trade_parse_error", error=str(exc), raw=entry[:80])

    paper_now = await effective_paper_trading(redis)
    return PaperTradesResponse(
        trades=trades,
        total=len(trades),
        total_virtual_pnl=round(total_pnl, 4),
        paper_trading_enabled=paper_now,
    )


@router.get(
    "/trading-mode",
    response_model=TradingModeResponse,
    summary="Current trading mode (paper vs live) and virtual trade count",
)
async def get_trading_mode(redis: RedisDep) -> TradingModeResponse:
    """
    Lightweight endpoint — returns whether paper trading is active and how
    many virtual trades have been logged.  Polled by the dashboard header
    to render the Simulation Mode badge.
    """
    count = await redis.llen(PAPER_TRADING_REDIS_KEY)
    paper_now = await effective_paper_trading(redis)
    return TradingModeResponse(
        paper_trading=paper_now,
        virtual_trade_count=int(count),
    )


# ── Performance stats endpoint ─────────────────────────────────────────────────

class PaperPerformanceResponse(BaseModel):
    total_trades: int
    wins:         int
    losses:       int
    virtual_pnl:  float
    win_streak:   int
    win_rate:     float
    updated_at:   Optional[str] = None


@router.get(
    "/performance",
    response_model=PaperPerformanceResponse,
    summary="Aggregated paper-trading win rate and virtual P&L",
)
async def get_performance(redis: RedisDep) -> PaperPerformanceResponse:
    """
    Return the aggregated performance stats for all settled paper trades,
    stored in Redis under nexus:stats:paper.

    Stats are updated automatically by the background arbitrage collector
    each time a trade is settled (5 minutes after entry).

    win_rate is expressed as a percentage (0–100).
    """
    raw = await redis.get(PAPER_STATS_KEY)
    if not raw:
        return PaperPerformanceResponse(
            total_trades=0,
            wins=0,
            losses=0,
            virtual_pnl=0.0,
            win_streak=0,
            win_rate=0.0,
        )

    try:
        data: Dict[str, Any] = json.loads(raw)
    except Exception as exc:
        log.warning("paper_stats_parse_error", error=str(exc))
        raise HTTPException(status_code=500, detail="Corrupted stats data") from exc

    total    = int(data.get("total_trades", 0))
    wins     = int(data.get("wins", 0))
    win_rate = round(wins / total * 100, 1) if total > 0 else 0.0

    return PaperPerformanceResponse(
        total_trades=total,
        wins=wins,
        losses=int(data.get("losses", 0)),
        virtual_pnl=float(data.get("virtual_pnl", 0.0)),
        win_streak=int(data.get("win_streak", 0)),
        win_rate=win_rate,
        updated_at=data.get("updated_at"),
    )


# ── Live trade log endpoint ────────────────────────────────────────────────────

class TradeLogEntry(BaseModel):
    timestamp:       str
    side:            str = "YES"
    price:           float = 0.0
    shares:          float = 0.0
    spent_usd:       float = 0.0
    market_question: str = ""
    status:          str = "success"
    log_text:        str = ""
    paper:           bool = True
    order_id:        Optional[str] = None


class TradeLogResponse(BaseModel):
    entries:       List[TradeLogEntry]
    total:         int
    paper_trading: bool
    kill_switch_balance_usd: float


@router.get(
    "/trade-log",
    response_model=TradeLogResponse,
    summary="Latest automated trade log (newest first, max 20)",
)
async def get_trade_log(redis: RedisDep) -> TradeLogResponse:
    """
    Return the most recent automated trade actions from the paper-trading
    history log.  Newest entries are returned first.

    Each entry includes:
      - ``log_text``  — human-readable description ("Bought 4.2 shares of YES @ $0.480")
      - ``status``    — "success" | "failed" | "halted" | "timeout" | "skipped"
      - ``paper``     — True while PAPER_TRADING mode is active

    The ``kill_switch_balance_usd`` field shows the threshold below which all
    trading is halted ($90).
    """
    from nexus.trading.polymarket_client import KILL_SWITCH_BALANCE_USD

    raw_entries: list[str] = await redis.lrange(PAPER_TRADING_REDIS_KEY, 0, 19)

    entries: list[TradeLogEntry] = []
    for entry in raw_entries:
        try:
            obj = json.loads(entry)
            entries.append(
                TradeLogEntry(
                    timestamp       = obj.get("timestamp", ""),
                    side            = obj.get("side") or obj.get("direction", "YES"),
                    price           = float(obj.get("price") or obj.get("entry_yes_price", 0.0)),
                    shares          = float(obj.get("shares", 0.0)),
                    spent_usd       = float(
                        obj.get("spent_usd") or obj.get("virtual_amount_usd", 0.0)
                    ),
                    market_question = obj.get("market_question", ""),
                    status          = obj.get("status", "success"),
                    log_text        = obj.get("log_text", ""),
                    paper           = bool(obj.get("paper", True)),
                    order_id        = obj.get("order_id"),
                )
            )
        except Exception as exc:
            log.warning("trade_log_parse_error", error=str(exc), raw=entry[:80])

    return TradeLogResponse(
        entries=entries,
        total=len(entries),
        paper_trading=PAPER_TRADING,
        kill_switch_balance_usd=KILL_SWITCH_BALANCE_USD,
    )


# ── Manual override (volatility kill-switch) ────────────────────────────────────


class ManualOverrideResponse(BaseModel):
    halted:                 bool
    halted_at:              str
    open_positions_closed:  int


class ManualOverrideStatusResponse(BaseModel):
    active:     bool
    halted_at:  Optional[str] = None


@router.post(
    "/manual-override",
    response_model=ManualOverrideResponse,
    summary="Halt prediction-market orders and close open paper positions",
)
async def post_manual_override(redis: RedisDep) -> ManualOverrideResponse:
    from nexus.worker.tasks.prediction import apply_prediction_manual_override

    data = await apply_prediction_manual_override(redis)
    return ManualOverrideResponse(**data)


@router.post(
    "/manual-override/clear",
    response_model=ManualOverrideStatusResponse,
    summary="Clear prediction manual halt — allow automated orders again",
)
async def post_manual_override_clear(redis: RedisDep) -> ManualOverrideStatusResponse:
    from nexus.worker.tasks.prediction import clear_prediction_manual_override

    await clear_prediction_manual_override(redis)
    return ManualOverrideStatusResponse(active=False, halted_at=None)


@router.get(
    "/manual-override/status",
    response_model=ManualOverrideStatusResponse,
    summary="Whether prediction manual override is engaged",
)
async def get_manual_override_status(redis: RedisDep) -> ManualOverrideStatusResponse:
    raw = await redis.get(PREDICTION_MANUAL_HALT_KEY)
    if not raw:
        return ManualOverrideStatusResponse(active=False)
    return ManualOverrideStatusResponse(active=True, halted_at=str(raw))
