"""
NEXUS-ULTIMATE-SCALPER — dual-mode 5m Polymarket UP engine.

* Simulation: virtual $1,000, ledger in Redis, no CLOB calls.
* Live: ``POLYMARKET_RELAYER_KEY`` / ``NEXUS_POLY_PRIVATE_KEY`` via ``wallet_manager``;
  30% drawdown brake + Telegram (see ``nexus.agents.trading.wallet_manager``).

Signals
-------
* Binance spot momentum ~30s from ``nexus:binance:velocity_30s`` (collector in
  ``prediction.collect_arbitrage_datapoint``).
* OpenClaw Telegram sentiment ``nexus:openclaw:news_sentiment`` (score 0–10).

Entry rule: Strategy Brain fusion (OpenClaw + Binance + fleet + swarm) with
half-Kelly sizing; Gamma event ``POLY_5M_EVENT_ID`` (default 5m BTC). Preempt
path when ≥10 agents match salient keywords within TTL.

Enable background ticks: ``NEXUS_POLY_SCALPER_ENABLED=1`` (API lifespan).
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import uuid
from datetime import datetime, timezone
from typing import Any

import structlog

from nexus.core.engine.position_manager import (
    daily_budget_usd,
    ensure_live_positions_injected,
    position_engine_heartbeat_snapshot,
    target_goal_usd,
)
from nexus.services.poly_5m_scalper import POLY_EVENT_ID, fetch_poly5m_market
from nexus.shared.kill_switch import KILL_SWITCH_SCALPER_HALT_KEY
from nexus.services.strategy_brain import (
    build_strategy_snapshot,
    load_alpha_feed,
    load_sentiment_heatmap,
    load_strategy_snapshot,
    preempt_window_active,
    push_alpha_feed_event,
)
from nexus.agents.trading.config import PREDICTION_MANUAL_HALT_KEY
from nexus.agents.trading.polymarket_client import PolymarketClient, TradingHalted
from nexus.agents.trading.wallet_manager import REDIS_BRAKE_KEY, evaluate_real_balance_safety_brake
from nexus.agents.tasks.openclaw import OPENCLAW_NEWS_SENTIMENT_KEY
from nexus.agents.tasks.prediction import BINANCE_VELOCITY_KEY, fetch_binance_data

log = structlog.get_logger(__name__)


def _decode_redis_str(raw: Any) -> str:
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace").strip()
    return str(raw).strip()


SIM_MODE_KEY = "nexus:scalper:simulation_mode"
V_BAL_KEY = "nexus:scalper:virtual_balance"
LEDGER_KEY = "nexus:scalper:virtual_ledger"
PENDING_KEY = "nexus:scalper:pending_settlements"
LAST_ENTRY_TS_KEY = "nexus:scalper:last_entry_ts"
LAST_ALPHA_KEY = "nexus:scalper:last_alpha_source"
LAST_LIVE_SIGNAL_KEY = "nexus:scalper:last_live_signal_alpha"
RACE_STATE_KEY = "nexus:scalper:race_state"
LEDGER_MAX = 200
COMPOUND_RESERVE_KEY = "nexus:scalper:compound_reserve"
SESSION_START_ISO_KEY = "nexus:scalper:session_start_iso"
SESSION_START_BAL_KEY = "nexus:scalper:session_start_balance_usd"
SESSION_TRACK_MODE_KEY = "nexus:scalper:session_track_simulation"

SIM_START_USD = 1000.0
TARGET_GAIN_PCT = 1000.0

NEWS_SCORE_MIN = 9.0
MOMENTUM_MIN_PCT = 2.0
BET_FRACTION = 0.20
KELLY_HALF_CAP = 0.30
MASTER_STRIKE_MULT = 1.35
SETTLEMENT_DELAY_S = 300
ENTRY_COOLDOWN_S = 45
TICK_SLEEP_S = float(os.getenv("NEXUS_POLY_SCALPER_INTERVAL_S") or "5")
STEALTH_JITTER_FRAC = float(os.getenv("NEXUS_SCALPER_STEALTH_JITTER", "0.35") or "0")

SIMULATION_MODE: bool = False  # LIVE operations — simulation permanently disabled


def simulation_mode_from_env() -> bool:
    v = (os.getenv("POLY_SCALPER_SIMULATION_MODE") or "false").strip().lower()
    return v in ("1", "true", "yes", "on")


async def read_simulation_mode(redis: Any) -> bool:
    raw = await redis.get(SIM_MODE_KEY)
    if raw is not None and str(raw).strip() != "":
        return str(raw).strip().lower() in ("1", "true", "yes", "on")
    return False  # LIVE mode by default — simulation disabled


async def _current_mode_balance_usd(redis: Any, simulation: bool) -> float:
    if simulation:
        return await _virtual_balance(redis)
    try:
        return float(await PolymarketClient().get_balance_usdc())
    except Exception:
        return 0.0


async def _write_session_tracking(
    redis: Any, *, simulation: bool, balance_usd: float
) -> None:
    now = datetime.now(timezone.utc)
    await redis.set(SESSION_START_ISO_KEY, now.isoformat())
    await redis.set(SESSION_START_BAL_KEY, f"{float(balance_usd):.6f}")
    await redis.set(SESSION_TRACK_MODE_KEY, "1" if simulation else "0")


async def ensure_scalper_session_tracking(
    redis: Any, *, simulation: bool, balance_usd: float
) -> None:
    """Initialize or reset session clock when keys are missing or mode changed."""
    raw_mode = await redis.get(SESSION_TRACK_MODE_KEY)
    raw_start = await redis.get(SESSION_START_ISO_KEY)
    expected = "1" if simulation else "0"
    if raw_start is None or _decode_redis_str(raw_mode) != expected:
        await _write_session_tracking(redis, simulation=simulation, balance_usd=balance_usd)


async def write_simulation_mode(redis: Any, simulation: bool) -> None:
    await redis.set(SIM_MODE_KEY, "1" if simulation else "0")
    bal = await _current_mode_balance_usd(redis, simulation)
    await _write_session_tracking(redis, simulation=simulation, balance_usd=bal)


def _half_kelly_fraction(model_p: float, yes_px: float) -> float:
    if yes_px <= 0.02 or yes_px >= 0.98:
        return 0.0
    edge = model_p - yes_px
    if edge <= 0:
        return 0.0
    denom = max(1e-6, 1.0 - yes_px)
    return max(0.0, min(KELLY_HALF_CAP, 0.5 * edge / denom))


async def _read_compound_reserve(redis: Any) -> float:
    try:
        raw = await redis.get(COMPOUND_RESERVE_KEY)
        if raw is None:
            return 0.0
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return 0.0


async def _virtual_balance(redis: Any) -> float:
    raw = await redis.get(V_BAL_KEY)
    if raw is None:
        await redis.set(V_BAL_KEY, f"{SIM_START_USD:.4f}")
        return SIM_START_USD
    try:
        return float(raw)
    except ValueError:
        await redis.set(V_BAL_KEY, f"{SIM_START_USD:.4f}")
        return SIM_START_USD


async def _append_ledger(redis: Any, entry: dict[str, Any]) -> None:
    line = json.dumps(entry, default=str)
    await redis.rpush(LEDGER_KEY, line)
    await redis.ltrim(LEDGER_KEY, -LEDGER_MAX, -1)


async def _load_velocity(redis: Any) -> dict[str, Any] | None:
    raw = await redis.get(BINANCE_VELOCITY_KEY)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


async def _load_sentiment(redis: Any) -> dict[str, Any] | None:
    raw = await redis.get(OPENCLAW_NEWS_SENTIMENT_KEY)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


async def _persist_race_state(
    redis: Any,
    *,
    simulation: bool,
    balance: float,
    baseline: float,
) -> dict[str, Any]:
    if baseline <= 0:
        baseline = SIM_START_USD if simulation else daily_budget_usd()
    if simulation:
        target_mult = 1.0 + (TARGET_GAIN_PCT / 100.0)
        target_bal = baseline * target_mult
        gain_pct = TARGET_GAIN_PCT
    else:
        target_bal = float(target_goal_usd())
        if baseline <= 0:
            baseline = daily_budget_usd()
        gain_pct = ((target_bal - baseline) / baseline) * 100.0 if baseline > 0 else 0.0
    denom = target_bal - baseline
    raw_pct = ((balance - baseline) / denom) * 100.0 if denom > 0 else 0.0
    progress_pct = max(0.0, min(100.0, raw_pct))
    payload = {
        "simulation": simulation,
        "balance_usd": round(balance, 2),
        "baseline_usd": round(baseline, 2),
        "target_usd": round(target_bal, 2),
        "progress_pct": round(progress_pct, 2),
        "target_gain_pct": round(gain_pct, 2),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    await redis.set(RACE_STATE_KEY, json.dumps(payload))
    return payload


async def _process_pending_settlements(redis: Any, simulation: bool) -> None:
    raws = await redis.lrange(PENDING_KEY, 0, -1)
    if not raws:
        return
    await redis.delete(PENDING_KEY)
    now = datetime.now(timezone.utc)
    vbal = await _virtual_balance(redis) if simulation else 0.0

    binance_snap: dict[str, Any] | None = None
    for raw in raws:
        try:
            p = json.loads(raw)
        except json.JSONDecodeError:
            continue
        settle_at = datetime.fromisoformat(str(p["settle_at"]).replace("Z", "+00:00"))
        if settle_at > now:
            await redis.rpush(PENDING_KEY, raw)
            continue

        if not simulation:
            await redis.rpush(PENDING_KEY, raw)
            continue

        if binance_snap is None:
            try:
                binance_snap = await fetch_binance_data("BTCUSDT")
            except Exception as exc:
                log.warning("scalper_settlement_binance_failed", error=str(exc))
                await redis.rpush(PENDING_KEY, raw)
                return

        exit_px = float(binance_snap.get("price") or 0.0)
        entry_px = float(p.get("entry_binance") or 0.0)
        bet = float(p.get("bet_usd") or 0.0)
        yes_px = float(p.get("yes_price") or 0.5)
        shares = bet / yes_px if yes_px > 0 else 0.0
        won = exit_px > entry_px and entry_px > 0

        if won:
            payout = shares * 1.0
            vbal += payout
            pnl = payout - bet
            alpha = {
                "channel_title": p.get("channel_title") or "",
                "excerpt": p.get("excerpt") or "",
                "score": p.get("news_score"),
            }
            await redis.set(LAST_ALPHA_KEY, json.dumps(alpha))
            if pnl > 0:
                try:
                    await redis.incrbyfloat(COMPOUND_RESERVE_KEY, pnl * 0.5)
                except Exception:
                    pass
        else:
            pnl = -bet

        await _append_ledger(
            redis,
            {
                "id": p.get("id"),
                "event": "settled",
                "mode": "simulation",
                "won": won,
                "pnl_usd": round(pnl, 4),
                "bet_usd": bet,
                "entry_binance": entry_px,
                "exit_binance": exit_px,
                "settled_at": now.isoformat(),
            },
        )

    if simulation:
        await redis.set(V_BAL_KEY, f"{vbal:.4f}")


async def poly_scalper_tick(redis: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"acted": False, "reason": "idle"}

    try:
        panic = await redis.get("SYSTEM_STATE:PANIC")
        if panic == "true":
            out["reason"] = "panic"
            return out
    except Exception:
        pass

    if await redis.get(KILL_SWITCH_SCALPER_HALT_KEY):
        out["reason"] = "nexus_kill_switch"
        return out

    if await redis.get(PREDICTION_MANUAL_HALT_KEY):
        out["reason"] = "manual_halt"
        return out

    try:
        if await redis.get(REDIS_BRAKE_KEY):
            out["reason"] = "safety_brake"
            return out
    except Exception:
        pass

    simulation = await read_simulation_mode(redis)
    await _process_pending_settlements(redis, simulation)

    velocity = await _load_velocity(redis)
    sentiment = await _load_sentiment(redis)
    mom = float(velocity.get("momentum_pct_30s", 0.0)) if velocity else 0.0
    news_score = float(sentiment.get("score", 0.0)) if sentiment else 0.0

    poly = await fetch_poly5m_market(POLY_EVENT_ID)
    yes_probe: float | None = None
    if poly.get("market_found"):
        try:
            yp = float(poly.get("yes_price") or 0.0)
            yes_probe = yp if yp > 0 else None
        except (TypeError, ValueError):
            yes_probe = None

    strat = await build_strategy_snapshot(
        redis,
        yes_price=yes_probe,
        openclaw=sentiment,
        velocity=velocity,
    )
    preempt_active = await preempt_window_active(redis)

    threshold_ok = news_score > NEWS_SCORE_MIN and mom > MOMENTUM_MIN_PCT
    if not threshold_ok and preempt_active:
        threshold_ok = news_score >= 7.0 and mom >= 1.0
    if not threshold_ok and strat.get("master_strike"):
        threshold_ok = news_score >= 8.5 and mom >= 1.5

    if not threshold_ok:
        out["reason"] = "thresholds_not_met"
        out["news_score"] = news_score
        out["momentum_pct_30s"] = mom
        out["strategy"] = strat
        if simulation:
            bal = await _virtual_balance(redis)
        else:
            try:
                bal = await PolymarketClient().get_balance_usdc()
            except Exception:
                bal = 0.0
        baseline = SIM_START_USD if simulation else daily_budget_usd()
        await _persist_race_state(redis, simulation=simulation, balance=bal, baseline=baseline)
        return out

    last_raw = await redis.get(LAST_ENTRY_TS_KEY)
    if last_raw:
        try:
            last_ts = datetime.fromisoformat(str(last_raw).replace("Z", "+00:00"))
            if (datetime.now(timezone.utc) - last_ts).total_seconds() < ENTRY_COOLDOWN_S:
                out["reason"] = "cooldown"
                return out
        except ValueError:
            pass

    if not poly.get("market_found"):
        out["reason"] = "no_polymarket_market"
        return out

    token_ids: list[str] = poly.get("clob_token_ids") or []
    if not token_ids:
        out["reason"] = "no_token_id"
        return out

    yes_price = float(poly.get("yes_price") or 0.5)
    market_q = str(poly.get("market_question") or "BTC Market")

    binance_snap = await fetch_binance_data("BTCUSDT")
    entry_btc = float(binance_snap.get("price") or 0.0)

    channel_title = str(sentiment.get("channel_title") or "unknown") if sentiment else ""
    excerpt = str(sentiment.get("excerpt") or "") if sentiment else ""

    model_p = min(0.92, max(0.08, float(strat["confidence_pct"]) / 100.0))
    kelly = _half_kelly_fraction(model_p, yes_price)
    kelly *= float(strat.get("phase_aggression") or 1.0)
    if strat.get("master_strike"):
        kelly *= MASTER_STRIKE_MULT
    if preempt_active:
        kelly *= 1.12
    bet_frac = max(
        BET_FRACTION * 0.5,
        min(0.36, kelly if kelly >= 0.055 else BET_FRACTION),
    )

    if simulation:
        vbal = await _virtual_balance(redis)
        reserve = await _read_compound_reserve(redis)
        boost = min(reserve, vbal * 0.25)
        effective = vbal + boost
        bet = round(effective * bet_frac, 2)
        min_bet = 1.0
        if bet < min_bet or vbal < bet:
            out["reason"] = "insufficient_virtual_balance"
            return out

        vbal -= bet
        await redis.set(V_BAL_KEY, f"{vbal:.4f}")
        pos_id = str(uuid.uuid4())[:12]
        settle_at = datetime.now(timezone.utc).timestamp() + SETTLEMENT_DELAY_S
        pending = {
            "id": pos_id,
            "bet_usd": bet,
            "yes_price": yes_price,
            "entry_binance": entry_btc,
            "settle_at": datetime.fromtimestamp(settle_at, tz=timezone.utc).isoformat(),
            "channel_title": channel_title,
            "excerpt": excerpt[:400],
            "news_score": news_score,
        }
        await redis.rpush(PENDING_KEY, json.dumps(pending))
        await redis.set(LAST_ENTRY_TS_KEY, datetime.now(timezone.utc).isoformat())
        await _append_ledger(
            redis,
            {
                "id": pos_id,
                "event": "open_sim",
                "bet_usd": bet,
                "side": "YES",
                "yes_price": yes_price,
                "market": market_q,
                "news_score": news_score,
                "momentum_30s": mom,
                "channel_title": channel_title,
                "bet_fraction": round(bet_frac, 4),
                "confidence_pct": strat.get("confidence_pct"),
                "market_phase": strat.get("market_phase"),
                "master_strike": strat.get("master_strike"),
                "opened_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        await _persist_race_state(redis, simulation=True, balance=vbal, baseline=SIM_START_USD)
        await push_alpha_feed_event(
            redis,
            kind="MASTER_STRIKE" if strat.get("master_strike") else "SCALPER_ENTRY",
            detail=(
                f"SIM YES ${bet:.2f} · frac={bet_frac:.3f} · phase={strat.get('market_phase')} "
                f"· conf={strat.get('confidence_pct')}%"
            ),
            channel=channel_title,
            score=news_score,
        )
        log.info(
            "poly_scalper_sim_entry",
            bet_usd=bet,
            news_score=news_score,
            momentum_pct=mom,
            confidence=strat.get("confidence_pct"),
        )
        out.update(
            {
                "acted": True,
                "mode": "simulation",
                "bet_usd": bet,
                "id": pos_id,
                "strategy": strat,
                "bet_fraction": bet_frac,
            }
        )
        return out

    client = PolymarketClient()
    try:
        balance = await client.get_balance_usdc()
    except Exception as exc:
        log.error("poly_scalper_live_balance_failed", error=str(exc))
        out["reason"] = "balance_fetch_failed"
        return out

    if await evaluate_real_balance_safety_brake(redis, balance):
        out["reason"] = "safety_brake_triggered"
        return out

    reserve = await _read_compound_reserve(redis)
    boost = min(reserve, balance * 0.25)
    effective_live = balance + boost
    bet = round(effective_live * bet_frac, 2)
    if bet < 1.0 or balance < bet:
        out["reason"] = "insufficient_live_balance"
        return out

    await redis.set(LAST_ENTRY_TS_KEY, datetime.now(timezone.utc).isoformat())

    try:
        tr = await client.place_order_async(
            token_id=token_ids[0],
            side="YES",
            price=yes_price,
            market_question=market_q,
            budget_usd=bet,
            force_live=True,
        )
    except TradingHalted as exc:
        log.warning("poly_scalper_trading_halted", error=str(exc))
        out["reason"] = "trading_halted"
        return out
    except Exception as exc:
        log.error("poly_scalper_live_order_failed", error=str(exc))
        out["reason"] = "order_failed"
        return out

    entry = {
        "event": "open_live",
        "success": tr.success,
        "bet_usd": bet,
        "side": "YES",
        "yes_price": yes_price,
        "market": market_q,
        "news_score": news_score,
        "momentum_30s": mom,
        "channel_title": channel_title,
        "bet_fraction": round(bet_frac, 4),
        "confidence_pct": strat.get("confidence_pct"),
        "market_phase": strat.get("market_phase"),
        "master_strike": strat.get("master_strike"),
        "order_id": tr.order_id,
        "opened_at": datetime.now(timezone.utc).isoformat(),
    }
    await _append_ledger(redis, entry)
    await push_alpha_feed_event(
        redis,
        kind="MASTER_STRIKE" if strat.get("master_strike") else "LIVE_ENTRY",
        detail=(
            f"LIVE YES ${bet:.2f} · frac={bet_frac:.3f} · phase={strat.get('market_phase')} "
            f"· conf={strat.get('confidence_pct')}%"
        ),
        channel=channel_title,
        score=news_score,
    )
    if tr.success:
        live_sig = {
            "channel_title": channel_title,
            "excerpt": excerpt[:400],
            "score": news_score,
            "note": "live_entry_signal",
        }
        await redis.set(LAST_LIVE_SIGNAL_KEY, json.dumps(live_sig))

    try:
        balance_after = await client.get_balance_usdc()
    except Exception:
        balance_after = balance

    await _persist_race_state(
        redis,
        simulation=False,
        balance=balance_after,
        baseline=daily_budget_usd(),
    )
    log.info(
        "poly_scalper_live_entry",
        bet_usd=bet,
        success=tr.success,
        news_score=news_score,
        momentum_pct=mom,
    )
    out.update(
        {
            "acted": True,
            "mode": "live",
            "bet_usd": bet,
            "order_ok": tr.success,
            "strategy": strat,
            "bet_fraction": bet_frac,
        }
    )
    return out


async def run_poly_scalper_loop(redis: Any) -> None:
    log.info("ultimate_scalper_loop_started", tick_s=TICK_SLEEP_S)
    while True:
        try:
            if await redis.get(KILL_SWITCH_SCALPER_HALT_KEY) == "1":
                log.critical("ultimate_scalper_service_loop_exit_kill_switch")
                return
            await poly_scalper_tick(redis)
        except asyncio.CancelledError:
            log.info("ultimate_scalper_loop_stopped")
            raise
        except Exception as exc:
            log.warning("ultimate_scalper_tick_error", error=str(exc))
        jitter = 0.0
        if STEALTH_JITTER_FRAC > 0:
            jitter = random.uniform(0, STEALTH_JITTER_FRAC) * max(1.5, TICK_SLEEP_S)
        await asyncio.sleep(max(1.5, TICK_SLEEP_S) + jitter)


def compute_yield_metrics(
    *,
    session_start_iso: str | None,
    start_balance_usd: float | None,
    current_balance_usd: float,
) -> dict[str, Any]:
    """
    PPM and projected 24h yield. Denominator floored to 1 minute so the first
    minute never divides by zero or spikes on sub-minute uptime.
    """
    now = datetime.now(timezone.utc)
    empty: dict[str, Any] = {
        "session_start_time": None,
        "uptime_minutes": 0.0,
        "start_balance_usd": None,
        "current_balance_usd": round(current_balance_usd, 2),
        "profit_usd": None,
        "profit_per_minute_usd": None,
        "estimated_daily_profit_usd": None,
    }
    if not session_start_iso or start_balance_usd is None:
        return empty
    try:
        start_dt = datetime.fromisoformat(str(session_start_iso).replace("Z", "+00:00"))
    except ValueError:
        return empty
    uptime_min = max(0.0, (now - start_dt).total_seconds() / 60.0)
    denom_min = max(uptime_min, 1.0)
    profit = float(current_balance_usd) - float(start_balance_usd)
    ppm = profit / denom_min
    est_daily = ppm * 1440.0
    return {
        "session_start_time": session_start_iso,
        "uptime_minutes": round(uptime_min, 2),
        "start_balance_usd": round(float(start_balance_usd), 2),
        "current_balance_usd": round(float(current_balance_usd), 2),
        "profit_usd": round(profit, 4),
        "profit_per_minute_usd": round(ppm, 6),
        "estimated_daily_profit_usd": round(est_daily, 2),
    }


async def build_scalper_dashboard_payload(redis: Any) -> dict[str, Any]:
    simulation = await read_simulation_mode(redis)
    velocity = await _load_velocity(redis)
    sentiment = await _load_sentiment(redis)
    vbal = await _virtual_balance(redis) if simulation else None
    live_bal: float | None = None
    if not simulation:
        try:
            live_bal = await PolymarketClient().get_balance_usdc()
        except Exception:
            live_bal = None

    balance = float(vbal) if simulation else float(live_bal or 0.0)
    await ensure_scalper_session_tracking(redis, simulation=simulation, balance_usd=balance)

    raw_sess_start = await redis.get(SESSION_START_ISO_KEY)
    raw_sess_bal = await redis.get(SESSION_START_BAL_KEY)
    sess_start_iso: str | None = _decode_redis_str(raw_sess_start) or None
    start_bal: float | None = None
    bal_s = _decode_redis_str(raw_sess_bal)
    if bal_s:
        try:
            start_bal = float(bal_s)
        except (TypeError, ValueError):
            start_bal = None
    yield_metrics = compute_yield_metrics(
        session_start_iso=sess_start_iso,
        start_balance_usd=start_bal,
        current_balance_usd=balance,
    )
    yield_metrics["session_mode_simulation"] = simulation

    baseline = SIM_START_USD if simulation else daily_budget_usd()
    race = await _persist_race_state(
        redis,
        simulation=simulation,
        balance=balance,
        baseline=baseline,
    )

    live_positions = await ensure_live_positions_injected(redis)

    last_alpha_raw = await redis.get(LAST_ALPHA_KEY)
    last_alpha: dict[str, Any] | None = None
    if last_alpha_raw:
        try:
            last_alpha = json.loads(last_alpha_raw)
        except json.JSONDecodeError:
            last_alpha = None

    live_sig_raw = await redis.get(LAST_LIVE_SIGNAL_KEY)
    last_live_signal: dict[str, Any] | None = None
    if live_sig_raw:
        try:
            last_live_signal = json.loads(live_sig_raw)
        except json.JSONDecodeError:
            last_live_signal = None

    pending_n = await redis.llen(PENDING_KEY)

    brake = False
    try:
        brake = bool(await redis.get(REDIS_BRAKE_KEY))
    except Exception:
        pass

    strat_snap = await load_strategy_snapshot(redis)
    if strat_snap is None:
        yes_dash: float | None = None
        try:
            pm = await fetch_poly5m_market(POLY_EVENT_ID)
            if pm.get("market_found"):
                yp = float(pm.get("yes_price") or 0.0)
                yes_dash = yp if yp > 0 else None
        except Exception:
            yes_dash = None
        strat_snap = await build_strategy_snapshot(
            redis,
            yes_price=yes_dash,
            openclaw=sentiment,
            velocity=velocity,
        )

    heatmap = await load_sentiment_heatmap(redis)
    alpha_feed = await load_alpha_feed(redis, 20)
    compound_reserve = await _read_compound_reserve(redis)
    current_balance: float | None = (
        round(float(vbal), 2) if simulation and vbal is not None else None
    )
    if not simulation and live_bal is not None:
        current_balance = round(float(live_bal), 2)

    return {
        "project": "NEXUS-ULTIMATE-SCALPER",
        "simulation_mode": simulation,
        "virtual_balance_usd": round(vbal, 2) if vbal is not None else None,
        "live_balance_usd": round(live_bal, 2) if live_bal is not None else None,
        "current_balance": current_balance,
        "binance_velocity": velocity,
        "openclaw_sentiment": sentiment,
        "race_to_1000": race,
        "last_winning_trade_alpha": last_alpha,
        "last_live_entry_alpha": last_live_signal,
        "last_alpha_source": last_alpha,
        "pending_settlements": int(pending_n),
        "safety_brake_active": brake,
        "poly_5m_event_id": POLY_EVENT_ID,
        "strategy_brain": strat_snap,
        "fleet_sentiment_heatmap": heatmap,
        "alpha_source_feed": alpha_feed,
        "compound_reserve_usd": round(compound_reserve, 4),
        "yield_metrics": yield_metrics,
        "daily_budget_usd": daily_budget_usd(),
        "target_goal_usd": target_goal_usd(),
        "position_engine": await position_engine_heartbeat_snapshot(redis),
        "live_positions": live_positions,
        "thresholds": {
            "news_score_min": NEWS_SCORE_MIN,
            "momentum_pct_min": MOMENTUM_MIN_PCT,
            "bet_fraction_legacy": BET_FRACTION,
            "sizing_model": "half_kelly_capped",
            "kelly_half_cap": KELLY_HALF_CAP,
        },
    }
