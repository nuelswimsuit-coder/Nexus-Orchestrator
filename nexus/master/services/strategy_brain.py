"""
Strategy Brain — Master Trader intelligence loop.

Reads OpenClaw sentiment, paper-trading stats, and worker heartbeats from Redis,
computes a Master Confidence score, Kelly-style sizing hint, and optional
“strike” mode when sentiment + swarm load align. Exposes state for the API
and dashboard via ``nexus:war_room:intel``.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from datetime import datetime, timezone
from typing import Any

import structlog

from nexus.shared.fleet_redis import get_fleet_counter_snapshot
from nexus.shared.operator_targets import load_operator_target_patterns
from nexus.shared.swarm_signals import SWARM_SIGNAL_KEY
from nexus.worker.tasks.prediction import BINANCE_VELOCITY_KEY

log = structlog.get_logger(__name__)

WAR_ROOM_KEY = "nexus:war_room:intel"
WAR_ROOM_TTL_S = int(os.getenv("NEXUS_WAR_ROOM_TTL", "120"))

OPENCLAW_SENTIMENT_KEY = "nexus:openclaw:news_sentiment"
PAPER_STATS_KEY = "nexus:stats:paper"
POLY_PNL_KEY = "nexus:poly:pnl"
HEARTBEAT_PREFIX = "nexus:heartbeat:"


def kelly_fraction(win_prob: float, payoff_ratio: float) -> float:
    """
    Kelly criterion: f* = p - (1-p)/b, clamped to [0, 0.25].
    ``payoff_ratio`` b = avg_win / avg_loss (use 1.0 if unknown).
    """
    p = max(0.0, min(1.0, win_prob))
    b = max(0.01, payoff_ratio)
    f = p - (1.0 - p) / b
    return max(0.0, min(0.25, f))


async def _heartbeat_worker_count(redis: Any) -> int:
    """Count worker heartbeats (best-effort SCAN)."""
    n = 0
    try:
        cursor: int | str = 0
        pat = f"{HEARTBEAT_PREFIX}*"
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match=pat, count=128)
            for key in keys:
                k = key.decode() if isinstance(key, (bytes, bytearray)) else str(key)
                raw = await redis.get(k)
                if not raw:
                    continue
                try:
                    hb = json.loads(raw)
                    if hb.get("role") == "worker":
                        n += 1
                except Exception:
                    continue
            if cursor == 0:
                break
    except Exception as exc:
        log.debug("strategy_brain_heartbeat_scan_error", error=str(exc))
    return n


async def _recent_whale_hits(redis: Any, limit: int = 200) -> int:
    """Count recent swarm signal lines mentioning whale / flash news."""
    hits = 0
    keywords = ("whale", "flash", "breaking", "surge", "liquidat")
    try:
        lines = await redis.lrange(SWARM_SIGNAL_KEY, 0, limit - 1)
        for raw in lines or []:
            low = str(raw).lower()
            if any(k in low for k in keywords):
                hits += 1
    except Exception:
        pass
    return hits


async def _sentiment_score(redis: Any) -> tuple[float, str, str]:
    try:
        raw = await redis.get(OPENCLAW_SENTIMENT_KEY)
        if not raw:
            return 5.0, "", ""
        data = json.loads(raw)
        return (
            float(data.get("score", 5.0)),
            str(data.get("channel_title", "") or ""),
            str(data.get("excerpt", "") or "")[:200],
        )
    except Exception:
        return 5.0, "", ""


async def _paper_stats(redis: Any) -> dict[str, Any]:
    out: dict[str, Any] = {
        "virtual_pnl": 0.0,
        "wins": 0,
        "losses": 0,
        "total_trades": 0,
        "win_rate": 0.0,
    }
    try:
        raw = await redis.get(PAPER_STATS_KEY)
        if not raw:
            return out
        p = json.loads(raw)
        total = int(p.get("total_trades", 0) or 0)
        wins = int(p.get("wins", 0) or 0)
        out["virtual_pnl"] = float(p.get("virtual_pnl", 0.0) or 0.0)
        out["wins"] = wins
        out["losses"] = int(p.get("losses", 0) or 0)
        out["total_trades"] = total
        out["win_rate"] = round(wins / total * 100.0, 1) if total > 0 else 0.0
    except Exception:
        pass
    return out


async def _real_pnl_hint(redis: Any) -> float:
    try:
        raw = await redis.get(POLY_PNL_KEY)
        if not raw:
            return 0.0
        data = json.loads(raw)
        return float(data.get("session_pnl_usd", data.get("pnl_usd", 0.0)) or 0.0)
    except Exception:
        return 0.0


def _build_sentiment_heatmap(sentiment: float, channel: str) -> list[list[float]]:
    """6×8 mood matrix (0–100), anchored on OpenClaw score."""
    base = max(0.0, min(10.0, sentiment)) / 10.0
    seed = sum(ord(c) for c in channel[:32]) if channel else 7
    grid: list[list[float]] = []
    for r in range(6):
        row: list[float] = []
        for c in range(8):
            wobble = 0.12 * math.sin((r * 1.1 + c * 0.9) + seed * 0.01)
            v = (base + wobble) * 100.0
            row.append(round(max(0.0, min(100.0, v)), 1))
        grid.append(row)
    return grid


async def compute_war_room_payload(redis: Any) -> dict[str, Any]:
    sent_score, alpha_ch, _ex = await _sentiment_score(redis)
    paper = await _paper_stats(redis)
    workers = await _heartbeat_worker_count(redis)
    whale_hits = await _recent_whale_hits(redis)
    real_pnl = await _real_pnl_hint(redis)

    win_p = paper["win_rate"] / 100.0 if paper["total_trades"] > 0 else 0.45
    payoff = 1.2
    kelly = kelly_fraction(win_p, payoff)

    # Confidence: sentiment (early signal) + backtest win rate + swarm presence
    sent_part = (sent_score / 10.0) * 40.0
    win_part = min(40.0, paper["win_rate"] * 0.4)
    swarm_part = min(20.0, math.log1p(max(workers, 1)) * 4.0)
    confidence = round(max(0.0, min(100.0, sent_part + win_part + swarm_part)), 1)

    strike = bool(
        whale_hits >= int(os.getenv("NEXUS_SWARM_WHALE_THRESHOLD", "10"))
        and sent_score >= float(os.getenv("NEXUS_STRIKE_SENTIMENT_MIN", "7.5"))
    )
    reinvest_pct = 50.0 if strike else float(os.getenv("NEXUS_DEFAULT_REINVEST_PCT", "15"))

    target_profit_usd = float(os.getenv("NEXUS_RACE_TARGET_PROFIT_USD", "1000.0"))
    sim_pnl = paper["virtual_pnl"]
    race_pct = round(max(0.0, min(100.0, (sim_pnl / target_profit_usd) * 100.0)), 2)

    fusion = await load_strategy_snapshot(redis)
    if fusion and fusion.get("confidence_pct") is not None:
        confidence = float(fusion["confidence_pct"])

    return {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "master_confidence_pct": confidence,
        "openclaw_sentiment": round(sent_score, 2),
        "top_alpha_channel": alpha_ch or "(no OpenClaw pulse yet)",
        "paper": paper,
        "real_pnl_usd": round(real_pnl, 4),
        "sim_pnl_usd": round(sim_pnl, 4),
        "race_to_1000_pct": race_pct,
        "race_target_profit_usd": target_profit_usd,
        "kelly_fraction": round(kelly, 4),
        "swarm_workers_seen": workers,
        "swarm_whale_hits": whale_hits,
        "aggressive_strike": strike,
        "strike_reinvest_pct": reinvest_pct,
        "sentiment_heatmap": _build_sentiment_heatmap(sent_score, alpha_ch),
        "strategy_fusion": fusion,
        "fleet_sentiment_cells": await load_sentiment_heatmap(redis),
        "alpha_source_feed": await load_alpha_feed(redis, 16),
    }


async def persist_war_room(redis: Any) -> dict[str, Any]:
    payload = await compute_war_room_payload(redis)
    try:
        await redis.set(WAR_ROOM_KEY, json.dumps(payload), ex=WAR_ROOM_TTL_S)
    except Exception as exc:
        log.warning("war_room_persist_error", error=str(exc))
    return payload


class StrategyBrainService:
    """Background loop: refresh war-room intel for API + UI."""

    def __init__(self, redis: Any, interval_s: float | None = None) -> None:
        self._redis = redis
        self._interval = float(
            interval_s or os.getenv("NEXUS_STRATEGY_BRAIN_INTERVAL", "30")
        )
        self._running = False

    async def run_loop(self) -> None:
        self._running = True
        log.info("strategy_brain_started", interval_s=self._interval)
        import asyncio

        while self._running:
            try:
                await persist_war_room(self._redis)
            except Exception as exc:
                log.warning("strategy_brain_tick_error", error=str(exc))
            await asyncio.sleep(self._interval)

    def stop(self) -> None:
        self._running = False


# ── Execution fusion (Ultimate Scalper, fleet alpha, keyword swarm) ────────────

STRATEGY_SNAPSHOT_KEY = "nexus:strategy_brain:snapshot"
STRATEGY_SNAPSHOT_TTL_S = 120
POLY_YES_TRACK_KEY = "nexus:strategy_brain:poly_yes_last"
POLY_YES_MOVE_TS_KEY = "nexus:strategy_brain:poly_yes_move_ts"
ALPHA_FEED_KEY = "nexus:strategy_brain:alpha_feed"
ALPHA_FEED_MAX = 80
PREEMPT_UNTIL_KEY = "nexus:strategy_brain:preempt_until"
SWARM_KEY_PREFIX = "nexus:swarm:kw:"
SWARM_AGENT_TTL_S = 7200
HEATMAP_CHANNELS_KEY = "nexus:strategy_brain:sentiment_heatmap"

SWARM_TRIGGER_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"sec\s*approval", "SEC Approval"),
    (r"whale\s*dump", "Whale Dump"),
    (r"etf\s*approval", "ETF Approval"),
    (r"rate\s*cut", "Fed Rate Cut"),
    (r"liquidation\s*cascade", "Liquidation Cascade"),
    (r"hack|exploit|drain", "Security Event"),
    (r"(binance|coinbase).{0,12}investigation", "Exchange Investigation"),
) + load_operator_target_patterns()


def _kw_slug(s: str) -> str:
    return hashlib.sha256(s.lower().encode("utf-8", errors="ignore")).hexdigest()[:16]


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return None


async def record_swarm_keyword_hit(redis: Any, label: str, agent_fingerprint: str) -> int:
    key = f"{SWARM_KEY_PREFIX}{_kw_slug(label)}:agents"
    try:
        await redis.sadd(key, agent_fingerprint[:120])
        await redis.expire(key, SWARM_AGENT_TTL_S)
        return int(await redis.scard(key))
    except Exception as exc:
        log.debug("swarm_record_failed", label=label, error=str(exc))
        return 0


async def ingest_text_for_swarm(redis: Any, text: str, agent_fingerprint: str) -> dict[str, int]:
    low = text.lower()
    counts: dict[str, int] = {}
    for pattern, label in SWARM_TRIGGER_PATTERNS:
        try:
            if re.search(pattern, low, re.I):
                c = await record_swarm_keyword_hit(redis, label, agent_fingerprint)
                counts[label] = c
        except re.error:
            continue
    return counts


async def _max_swarm_consensus(redis: Any) -> tuple[int, str | None]:
    best_n = 0
    best_l: str | None = None
    for _, label in SWARM_TRIGGER_PATTERNS:
        key = f"{SWARM_KEY_PREFIX}{_kw_slug(label)}:agents"
        try:
            n = int(await redis.scard(key))
        except Exception:
            n = 0
        if n > best_n:
            best_n = n
            best_l = label
    return best_n, best_l


async def _update_poly_move_clock(redis: Any, yes_price: float | None) -> None:
    if yes_price is None or yes_price <= 0:
        return
    raw = await redis.get(POLY_YES_TRACK_KEY)
    prev: float | None = None
    if raw is not None:
        try:
            prev = float(raw)
        except ValueError:
            prev = None
    await redis.set(POLY_YES_TRACK_KEY, f"{yes_price:.6f}")
    if prev is None or prev <= 0:
        return
    if abs(yes_price - prev) >= 0.02:
        await redis.set(
            POLY_YES_MOVE_TS_KEY,
            datetime.now(timezone.utc).isoformat(),
        )


def classify_market_phase(momentum_pct: float, news_score: float) -> str:
    if momentum_pct <= -2.0 or news_score < 4.0:
        return "panic"
    if momentum_pct >= 3.0 and news_score > 7.0:
        return "euphoria"
    if abs(momentum_pct) < 0.5 and (news_score >= 9.0 or news_score <= 3.0):
        return "manipulation"
    return "neutral"


def phase_aggression_multiplier(phase: str) -> float:
    return {
        "panic": 0.55,
        "manipulation": 0.5,
        "euphoria": 1.12,
        "neutral": 1.0,
    }.get(phase, 1.0)


def compute_fusion_confidence(
    *,
    news_score: float,
    momentum_pct: float,
    fleet_premium_ratio: float,
    swarm_agents: int,
    gap_seconds: float | None,
) -> float:
    n = max(0.0, min(10.0, news_score)) * 6.0
    m = max(0.0, min(1.0, abs(momentum_pct) / 5.0)) * 22.0
    f = max(0.0, min(1.0, fleet_premium_ratio)) * 14.0
    s = max(0.0, min(1.0, swarm_agents / 10.0)) * 28.0
    g = 0.0
    if gap_seconds is not None and 5.0 <= gap_seconds <= 600.0:
        g = min(10.0, gap_seconds / 60.0)
    return max(0.0, min(100.0, n + m + f + s + g))


def sentiment_arbitrage_gap_seconds(
    telegram_ts: datetime | None,
    poly_move_ts: datetime | None,
) -> float | None:
    if not telegram_ts or not poly_move_ts:
        return None
    return abs((poly_move_ts - telegram_ts).total_seconds())


def master_strike_eligible(
    confidence: float,
    swarm_agents: int,
    news_score: float,
    momentum_pct: float,
    *,
    momentum_floor: float = 2.0,
) -> bool:
    return (
        confidence >= 95.0
        and swarm_agents >= 4
        and news_score >= 9.0
        and momentum_pct >= momentum_floor
    )


async def push_alpha_feed_event(
    redis: Any,
    *,
    kind: str,
    detail: str,
    channel: str = "",
    score: float | None = None,
) -> None:
    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "kind": kind[:80],
        "detail": detail[:500],
        "channel": channel[:200],
        "score": score,
    }
    try:
        await redis.lpush(ALPHA_FEED_KEY, json.dumps(row, default=str))
        await redis.ltrim(ALPHA_FEED_KEY, 0, ALPHA_FEED_MAX - 1)
    except Exception as exc:
        log.debug("alpha_feed_push_failed", error=str(exc))


async def _persist_heatmap_cell(
    redis: Any,
    channel: str,
    score: float,
    momentum: float,
) -> None:
    raw = await redis.get(HEATMAP_CHANNELS_KEY)
    data: dict[str, Any] = {}
    if raw:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            data = {}
    ch = channel[:80] or "unknown"
    data[ch] = {
        "score": round(score, 2),
        "momentum_hint": round(momentum, 3),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if len(data) > 40:
        for k in list(data.keys())[:-40]:
            data.pop(k, None)
    try:
        await redis.set(HEATMAP_CHANNELS_KEY, json.dumps(data), ex=86400)
    except Exception:
        pass


async def build_strategy_snapshot(
    redis: Any,
    *,
    yes_price: float | None = None,
    openclaw: dict[str, Any] | None = None,
    velocity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if openclaw is None:
        raw = await redis.get(OPENCLAW_SENTIMENT_KEY)
        openclaw = {}
        if raw:
            try:
                openclaw = json.loads(raw)
            except json.JSONDecodeError:
                openclaw = {}

    if velocity is None:
        rawv = await redis.get(BINANCE_VELOCITY_KEY)
        velocity = {}
        if rawv:
            try:
                velocity = json.loads(rawv)
            except json.JSONDecodeError:
                velocity = {}

    news_score = float(openclaw.get("score") or 0.0)
    mom = float(velocity.get("momentum_pct_30s") or 0.0)
    channel = str(openclaw.get("channel_title") or "")

    fleet = await get_fleet_counter_snapshot(redis)
    managed = max(1, int(fleet.get("total_managed_members") or 0))
    premium = int(fleet.get("total_premium_members") or 0)
    fleet_ratio = min(1.0, premium / float(managed))

    swarm_n, swarm_label = await _max_swarm_consensus(redis)

    tg_ts = _parse_iso(str(openclaw.get("updated_at") or ""))
    poly_ts = _parse_iso(str(await redis.get(POLY_YES_MOVE_TS_KEY) or ""))
    gap_s = sentiment_arbitrage_gap_seconds(tg_ts, poly_ts)

    phase = classify_market_phase(mom, news_score)
    conf = compute_fusion_confidence(
        news_score=news_score,
        momentum_pct=mom,
        fleet_premium_ratio=fleet_ratio,
        swarm_agents=swarm_n,
        gap_seconds=gap_s,
    )
    strike = master_strike_eligible(conf, swarm_n, news_score, mom)

    now = datetime.now(timezone.utc)
    if swarm_n >= 10:
        until = now.timestamp() + 90.0
        preempt_until_ts = datetime.fromtimestamp(until, tz=timezone.utc).isoformat()
        try:
            await redis.set(PREEMPT_UNTIL_KEY, preempt_until_ts, ex=120)
        except Exception:
            pass
        await push_alpha_feed_event(
            redis,
            kind="SWARM_PREEMPT",
            detail=f"{swarm_n} agents · {swarm_label or 'keyword'}",
            channel=channel,
            score=news_score,
        )

    await _update_poly_move_clock(redis, float(yes_price) if yes_price is not None else None)

    snap = {
        "updated_at": now.isoformat(),
        "market_phase": phase,
        "phase_aggression": phase_aggression_multiplier(phase),
        "confidence_pct": round(conf, 2),
        "master_strike": strike,
        "sentiment_arbitrage_gap_s": round(gap_s, 2) if gap_s is not None else None,
        "fleet_alpha": {
            "premium_members": premium,
            "managed_members": int(fleet.get("total_managed_members") or 0),
            "premium_ratio": round(fleet_ratio, 4),
        },
        "swarm": {
            "max_agent_consensus": swarm_n,
            "top_label": swarm_label,
            "preempt_active": swarm_n >= 10,
        },
        "inputs": {
            "news_score": news_score,
            "momentum_pct_30s": mom,
            "openclaw_channel": channel,
            "poly_yes_price": yes_price,
        },
    }
    try:
        await redis.set(STRATEGY_SNAPSHOT_KEY, json.dumps(snap), ex=STRATEGY_SNAPSHOT_TTL_S)
    except Exception as exc:
        log.warning("strategy_snapshot_set_failed", error=str(exc))

    await _persist_heatmap_cell(redis, channel, news_score, mom)
    return snap


async def load_strategy_snapshot(redis: Any) -> dict[str, Any] | None:
    raw = await redis.get(STRATEGY_SNAPSHOT_KEY)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


async def preempt_window_active(redis: Any) -> bool:
    raw = await redis.get(PREEMPT_UNTIL_KEY)
    if not raw:
        return False
    ts = _parse_iso(str(raw))
    if not ts:
        return False
    return datetime.now(timezone.utc) < ts


async def load_sentiment_heatmap(redis: Any) -> dict[str, Any]:
    raw = await redis.get(HEATMAP_CHANNELS_KEY)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


async def load_alpha_feed(redis: Any, limit: int = 24) -> list[dict[str, Any]]:
    lim = max(1, min(limit, 80))
    try:
        lines = await redis.lrange(ALPHA_FEED_KEY, 0, lim - 1)
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for line in lines:
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out
