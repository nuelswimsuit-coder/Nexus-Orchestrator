"""
NEXUS-POLY-SCALPER-5M — Bitcoin 5m Polymarket vs Binance velocity + Telefix/OpenClaw intel.

For the dual-mode “Ultimate” engine (virtual $1000 vs live wallet, OpenClaw score > 9 + 2%% / 30s
momentum, race-to-1000%% UI), see ``nexus.services.ultimate_scalper``.

Coordinates:
  • Worker-published Binance websocket feed (``nexus:poly5m:btc_feed``, task in
    ``nexus.agents.tasks.poly5m_velocity`` when ``POLY5M_VELOCITY_FEED=1``).
  • Polymarket Gamma event (default ``POLY_5M_EVENT_ID``) → active CLOB market.
  • Telefix ``telefix.db`` — metrics/settings and any text tables with BTC +
    urgent keywords (Pump, Dump, SEC, Fed, Elon, Liquidation, Whale).
  • Optional Ollama sentiment (``POLY5M_OLLAMA_URL``); else fast heuristic 1–10.

Master enable: ``POLY5M_SCALPER_ENABLED=1`` (see ``scripts/start_master.py``).

Risk
----
  • Max ``POLY5M_MAX_BET_USD`` per 5m cycle (default $5).
  • Trading stops after ``POLY5M_MAX_CONSECUTIVE_LOSSES`` settled losses (default 3).
  • Honors ``SYSTEM_STATE:PANIC`` and ``nexus:prediction:manual_halt``.

Redis
-----
nexus:poly5m:btc_feed, nexus:poly5m:dashboard, nexus:poly5m:stats,
nexus:poly5m:loss_streak, nexus:poly5m:trading_halted, nexus:poly5m:pending
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import aiosqlite
import httpx
import structlog

from nexus.services.api.services.telefix_bridge import DB_PATH as _DEFAULT_TELEFIX_DB
from nexus.agents.trading.config import (
    PAPER_TRADING_MAX_HISTORY,
    PAPER_TRADING_REDIS_KEY,
    PREDICTION_MANUAL_HALT_KEY,
)
from nexus.agents.trading.polymarket_client import PolymarketClient, TradingHalted
from nexus.shared.kill_switch import KILL_SWITCH_SCALPER_HALT_KEY
from nexus.shared.power_profile import REDIS_POLY_CYCLE_KEY
from nexus.agents.trading.wallet_manager import REDIS_BRAKE_KEY, evaluate_real_balance_safety_brake
from nexus.agents.tasks.openclaw import OPENCLAW_NEWS_SENTIMENT_KEY
from nexus.agents.tasks.poly5m_velocity import POLY5M_BTC_FEED_KEY
from nexus.agents.tasks.prediction import BINANCE_VELOCITY_KEY, fetch_binance_data

log = structlog.get_logger(__name__)

GAMMA_BASE = os.getenv("POLY5M_GAMMA_HOST", "https://gamma-api.polymarket.com")
POLY_EVENT_ID = os.getenv("POLY_5M_EVENT_ID", "1773891600")

CYCLE_S = int(os.getenv("POLY5M_CYCLE_SECONDS", "300"))
MAX_BET_USD = float(os.getenv("POLY5M_MAX_BET_USD", "5"))
LOSS_STREAK_HALT = int(os.getenv("POLY5M_MAX_CONSECUTIVE_LOSSES", "3"))
VELOCITY_UP_THRESH = float(os.getenv("POLY5M_VELOCITY_UP_THRESH_PCT", "0.012"))
VELOCITY_DOWN_THRESH = float(os.getenv("POLY5M_VELOCITY_DOWN_THRESH_PCT", "-0.012"))

DASHBOARD_KEY = "nexus:poly5m:dashboard"
STATS_KEY = "nexus:poly5m:stats"
LOSS_STREAK_KEY = "nexus:poly5m:loss_streak"
HALT_KEY = "nexus:poly5m:trading_halted"
PENDING_KEY = "nexus:poly5m:pending"
PANIC_REDIS_KEY = "SYSTEM_STATE:PANIC"

URGENT_KEYWORDS = [
    "pump",
    "dump",
    "sec",
    "fed",
    "elon",
    "liquidation",
    "whale move",
    "whale",
]

_STRIKE_RE = re.compile(
    r"(?:price|strike|open|reference|chainlink)[^\d$]{0,24}[\$]?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?|[0-9]+(?:\.[0-9]+)?)",
    re.I,
)
_PRICE_ANY = re.compile(r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)\b")


def _telefix_db_path() -> str:
    return (os.environ.get("TELEFIX_DB_PATH") or _DEFAULT_TELEFIX_DB).strip()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_json_list(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return []
    return []


async def _gamma_get_json(client: httpx.AsyncClient, url: str) -> Any | None:
    try:
        r = await client.get(
            url,
            headers={"User-Agent": "Nexus-Orchestrator/1.0"},
            timeout=15.0,
        )
        if r.status_code != 200:
            return None
        return r.json()
    except Exception as exc:
        log.warning("poly5m_gamma_request_failed", url=url, error=str(exc))
        return None


async def fetch_poly5m_market(event_id: str) -> dict[str, Any]:
    """Resolve Gamma event → best active CLOB market (prefer accepting orders)."""
    out: dict[str, Any] = {
        "market_found": False,
        "market_question": None,
        "yes_price": None,
        "no_price": None,
        "clob_token_ids": [],
        "strike_hint": None,
        "event_title": None,
        "raw_market_id": None,
    }
    async with httpx.AsyncClient() as client:
        data = await _gamma_get_json(client, f"{GAMMA_BASE}/events?id={event_id}")
        event: dict[str, Any] | None = None
        if isinstance(data, list) and data:
            event = data[0]
        elif isinstance(data, dict) and data.get("markets"):
            event = data

        if not event:
            markets = await _gamma_get_json(
                client,
                f"{GAMMA_BASE}/markets?q=Bitcoin+Up+or+Down&active=true&closed=false&limit=8",
            )
            if isinstance(markets, list) and markets:
                m = markets[0]
                event = {"title": m.get("question", ""), "markets": [m]}

        if not event:
            log.warning("poly5m_event_not_found", event_id=event_id)
            return out

        out["event_title"] = event.get("title") or event.get("ticker")
        markets = event.get("markets") or []
        if not markets and event.get("question"):
            markets = [event]

        chosen: dict[str, Any] | None = None
        for m in markets:
            if not isinstance(m, dict):
                continue
            if m.get("closed"):
                continue
            if m.get("active") is False:
                continue
            if m.get("acceptingOrders") is False:
                continue
            chosen = m
            break
        if chosen is None:
            for m in markets:
                if isinstance(m, dict) and not m.get("closed"):
                    chosen = m
                    break
        if chosen is None:
            return out

        raw_prices = _parse_json_list(chosen.get("outcomePrices"))
        if len(raw_prices) < 2:
            try:
                raw_prices = json.loads(chosen.get("outcomePrices") or "[]")
            except Exception:
                raw_prices = ["0.5", "0.5"]
        yes_p = float(raw_prices[0]) if raw_prices else 0.5
        no_p = float(raw_prices[1]) if len(raw_prices) > 1 else 0.5

        tokens = _parse_json_list(chosen.get("clobTokenIds"))
        if not tokens and isinstance(chosen.get("clobTokenIds"), str):
            tokens = _parse_json_list(chosen["clobTokenIds"])

        strike = _extract_strike(
            str(chosen.get("question", "")),
            str(chosen.get("description", "")),
        )

        out.update(
            market_found=True,
            market_question=chosen.get("question", ""),
            yes_price=round(yes_p, 4),
            no_price=round(no_p, 4),
            clob_token_ids=[str(t) for t in tokens if t],
            strike_hint=strike,
            raw_market_id=str(chosen.get("id", "")),
        )
    return out


def _extract_strike(question: str, description: str) -> float | None:
    blob = f"{question}\n{description}"
    m = _STRIKE_RE.search(blob)
    if m:
        try:
            return float(m.group(1).replace(",", ""))
        except ValueError:
            pass
    for m in _PRICE_ANY.finditer(blob):
        try:
            v = float(m.group(1).replace(",", ""))
            if 1_000 < v < 5_000_000:
                return v
        except ValueError:
            continue
    return None


async def _load_text_intel_snippets(db_path: str, limit: int = 80) -> list[str]:
    if not os.path.exists(db_path):
        return []
    snippets: list[str] = []
    uri = f"file:{db_path.replace(chr(92), '/')}?mode=ro"
    try:
        async with aiosqlite.connect(uri, uri=True, timeout=5) as db:
            await db.execute("PRAGMA busy_timeout = 5000")
            db.row_factory = aiosqlite.Row

            for q in (
                "SELECT key, value FROM metrics ORDER BY rowid DESC LIMIT ?",
                "SELECT key, value FROM settings ORDER BY rowid DESC LIMIT ?",
            ):
                try:
                    async with db.execute(q, (limit,)) as cur:
                        async for row in cur:
                            k = str(row["key"] or "")
                            v = str(row["value"] or "")
                            if not v or len(v) < 8:
                                continue
                            low = (k + " " + v).lower()
                            if not any(x in low for x in ("btc", "bitcoin", "crypto")):
                                continue
                            if not any(kw in low for kw in URGENT_KEYWORDS):
                                continue
                            snippets.append(v[:500])
                except aiosqlite.Error:
                    continue

            try:
                async with db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ) as cur:
                    tables = [str(r[0]) for r in await cur.fetchall()]
            except aiosqlite.Error:
                tables = []

            text_col_hints = (
                "body",
                "text",
                "message",
                "content",
                "title",
                "snippet",
                "description",
                "raw",
            )
            safe_tables = [
                t for t in tables if t.isidentifier() and not t.startswith("sqlite_")
            ]
            for t in safe_tables[:25]:
                try:
                    async with db.execute(f'PRAGMA table_info("{t}")') as cur:
                        cols = [str(r[1]) for r in await cur.fetchall()]
                except aiosqlite.Error:
                    continue
                use = [c for c in cols if c.lower() in text_col_hints]
                if not use:
                    continue
                col = use[0]
                try:
                    sql = f'SELECT "{col}" AS blob FROM "{t}" ORDER BY rowid DESC LIMIT 12'
                    async with db.execute(sql) as cur:
                        async for row in cur:
                            v = str(row["blob"] or "")
                            if len(v) < 12:
                                continue
                            low = v.lower()
                            if "btc" not in low and "bitcoin" not in low:
                                continue
                            if not any(kw in low for kw in URGENT_KEYWORDS):
                                continue
                            snippets.append(v[:500])
                except aiosqlite.Error:
                    continue
    except Exception as exc:
        log.debug("poly5m_telefix_scan_failed", error=str(exc))
    return snippets[:40]


async def _ollama_sentiment_score(text: str) -> float | None:
    url = os.getenv("POLY5M_OLLAMA_URL", "").strip()
    model = os.getenv("POLY5M_OLLAMA_MODEL", "llama3.2:1b").strip()
    if not url:
        return None
    prompt = (
        "Rate crypto headline sentiment for BTC 1-10 only. "
        "1=crash 10=moon. Reply with single digit.\n\n"
        f"{text[:800]}"
    )
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            r = await client.post(
                url.rstrip("/") + "/api/generate",
                json={"model": model, "prompt": prompt, "stream": False},
            )
            r.raise_for_status()
            resp = r.json()
            raw = (resp.get("response") or "").strip()
        m = re.search(r"\b(10|[1-9])\b", raw)
        if not m:
            return None
        return float(m.group(1))
    except Exception:
        return None


def _heuristic_sentiment_score(text: str) -> float:
    low = text.lower()
    score = 5.5
    bull = (
        "etf",
        "approve",
        "rally",
        "surge",
        "moon",
        "pump",
        "adoption",
        "ath",
        "whale buy",
    )
    bear = (
        "dump",
        "crash",
        "liquidation",
        "hack",
        "ban",
        "sec charges",
        "lawsuit",
        "fraud",
        "sell-off",
    )
    for w in bull:
        if w in low:
            score += 1.1
    for w in bear:
        if w in low:
            score -= 1.2
    if "fed" in low and any(x in low for x in ("cut", "dovish", "pause")):
        score += 0.8
    if "fed" in low and "hawkish" in low:
        score -= 0.9
    if "elon" in low and any(x in low for x in ("bitcoin", "btc")):
        score += 0.4
    return max(1.0, min(10.0, score))


async def synthesize_openclaw_sentiment(snippets: list[str]) -> dict[str, Any]:
    if not snippets:
        return {
            "score": 5.0,
            "label": "neutral",
            "flash": False,
            "flash_side": None,
            "headline": "",
            "source": "none",
        }
    joined = " \n".join(snippets[:5])
    llm = await _ollama_sentiment_score(joined)
    if llm is not None:
        score = llm
        src = "ollama"
    else:
        acc = sum(_heuristic_sentiment_score(s) for s in snippets[:5])
        score = acc / min(5, len(snippets))
        src = "heuristic"

    label = "bullish" if score >= 6.5 else ("bearish" if score <= 4.5 else "neutral")
    flash = score > 8.0 or score < 2.0
    flash_side: str | None = None
    if flash:
        flash_side = "YES" if score > 8.0 else "NO"
    return {
        "score": round(score, 2),
        "label": label,
        "flash": flash,
        "flash_side": flash_side,
        "headline": snippets[0][:240],
        "source": src,
    }


async def _system_blocks_trading(redis: Any) -> tuple[bool, str]:
    try:
        if await redis.get(PANIC_REDIS_KEY) == "true":
            return True, "system_panic"
        if await redis.get(KILL_SWITCH_SCALPER_HALT_KEY):
            return True, "nexus_kill_switch_halt"
        if await redis.get(PREDICTION_MANUAL_HALT_KEY):
            return True, "prediction_manual_halt"
        if await redis.get(HALT_KEY):
            return True, "poly5m_loss_streak"
    except Exception:
        pass
    return False, ""


def _yes_wins_at_settlement(last_px: float, strike: float) -> bool:
    return last_px >= strike


@dataclass
class Poly5mScalperService:
    redis: Any
    event_id: str = field(default_factory=lambda: POLY_EVENT_ID)

    async def _read_btc_feed(self) -> dict[str, Any]:
        raw = await self.redis.get(POLY5M_BTC_FEED_KEY)
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}

    async def _update_stats(self, won: bool | None) -> dict[str, Any]:
        raw = await self.redis.get(STATS_KEY)
        data = {"wins": 0, "losses": 0, "settled": 0, "updated_at": _now_iso()}
        if raw:
            try:
                data.update(json.loads(raw))
            except json.JSONDecodeError:
                pass
        if won is True:
            data["wins"] = int(data.get("wins", 0)) + 1
            data["settled"] = int(data.get("settled", 0)) + 1
        elif won is False:
            data["losses"] = int(data.get("losses", 0)) + 1
            data["settled"] = int(data.get("settled", 0)) + 1
        data["updated_at"] = _now_iso()
        await self.redis.set(STATS_KEY, json.dumps(data))
        return data

    async def _bump_loss_streak(self, lost: bool) -> int:
        cur = int(await self.redis.get(LOSS_STREAK_KEY) or 0)
        if lost:
            cur += 1
        else:
            cur = 0
        await self.redis.set(LOSS_STREAK_KEY, str(cur))
        if cur >= LOSS_STREAK_HALT:
            await self.redis.set(HALT_KEY, "loss_streak")
            log.warning("poly5m_panic_switch", consecutive_losses=cur)
        return cur

    async def settle_pending(self, btc_price: float) -> None:
        raw = await self.redis.get(PENDING_KEY)
        if not raw:
            return
        try:
            pending = json.loads(raw)
        except json.JSONDecodeError:
            await self.redis.delete(PENDING_KEY)
            return
        strike = float(pending.get("strike", 0) or 0)
        side = pending.get("side")
        if strike <= 0 or side not in {"YES", "NO"}:
            await self.redis.delete(PENDING_KEY)
            return
        yw = _yes_wins_at_settlement(btc_price, strike)
        won = (side == "YES" and yw) or (side == "NO" and not yw)
        await self._update_stats(won)
        await self._bump_loss_streak(not won)
        await self.redis.delete(PENDING_KEY)
        log.info(
            "poly5m_settled",
            side=side,
            strike=strike,
            last_px=btc_price,
            won=won,
        )

    async def run_cycle(self) -> dict[str, Any]:
        blocked, reason = await _system_blocks_trading(self.redis)
        feed = await self._read_btc_feed()
        btc_px = float(feed.get("price") or 0)
        velocity = float(feed.get("velocity_pct_60s") or 0)

        intel = await _load_text_intel_snippets(_telefix_db_path())
        sent = await synthesize_openclaw_sentiment(intel)
        market = await fetch_poly5m_market(self.event_id)

        summary: dict[str, Any] = {
            "updated_at": _now_iso(),
            "event_id": self.event_id,
            "blocked": blocked,
            "block_reason": reason,
            "btc_price": btc_px,
            "velocity_pct_60s": velocity,
            "sentiment": sent,
            "market_found": market.get("market_found"),
            "market_question": market.get("market_question"),
            "yes_price": market.get("yes_price"),
            "paper_trading": False,
        }

        if btc_px > 0:
            await self.settle_pending(btc_px)

        if blocked:
            summary["decision"] = "BLOCKED"
            await self.redis.set(DASHBOARD_KEY, json.dumps(summary), ex=600)
            return summary

        if not market.get("market_found") or not market.get("clob_token_ids"):
            summary["decision"] = "NO_MARKET"
            await self.redis.set(DASHBOARD_KEY, json.dumps(summary), ex=600)
            return summary

        strike = market.get("strike_hint")
        if strike is None or strike <= 0:
            strike = btc_px
        if strike <= 0:
            summary["decision"] = "NO_STRIKE"
            await self.redis.set(DASHBOARD_KEY, json.dumps(summary), ex=600)
            return summary

        momentum_up = velocity >= VELOCITY_UP_THRESH
        momentum_down = velocity <= VELOCITY_DOWN_THRESH

        decision = "HOLD"
        side: str | None = None
        if sent["flash"]:
            decision = "NEWS_FLASH"
            side = sent["flash_side"]
        elif momentum_up and sent["label"] == "bullish":
            decision = "MOMENTUM_BULL"
            side = "YES"
        elif momentum_down and sent["label"] == "bearish":
            decision = "MOMENTUM_BEAR"
            side = "NO"

        summary["decision"] = decision
        summary["side"] = side
        summary["strike_usd"] = strike

        if side is None:
            await self.redis.set(DASHBOARD_KEY, json.dumps(summary), ex=600)
            return summary

        tokens: list[str] = list(market["clob_token_ids"])
        if len(tokens) < 2:
            summary["decision"] = "NO_CLOB_TOKENS"
            await self.redis.set(DASHBOARD_KEY, json.dumps(summary), ex=600)
            return summary
        token_id = tokens[0] if side == "YES" else tokens[1]
        limit_px = float(
            market["yes_price"] if side == "YES" else market["no_price"] or 0.5
        )
        if limit_px <= 0.01:
            limit_px = 0.5

        client = PolymarketClient()
        try:
            live_bal = await client.get_balance_usdc()
            if await evaluate_real_balance_safety_brake(self.redis, live_bal):
                summary["decision"] = "WALLET_SAFETY_BRAKE"
                summary["block_reason"] = "drawdown_30pct"
                await self.redis.set(DASHBOARD_KEY, json.dumps(summary), ex=600)
                return summary
        except Exception as exc:
            log.warning("poly5m_wallet_brake_check_failed", error=str(exc))

        try:
            result = await client.place_order_async(
                token_id=token_id,
                side=side,
                price=round(min(max(limit_px, 0.01), 0.99), 4),
                market_question=market.get("market_question") or "BTC 5m",
                budget_usd=MAX_BET_USD,
                force_live=True,
            )
        except TradingHalted as exc:
            summary["decision"] = "TRADING_HALTED"
            summary["error"] = str(exc)
            await self.redis.set(DASHBOARD_KEY, json.dumps(summary), ex=600)
            return summary
        except Exception as exc:
            summary["decision"] = "ORDER_ERROR"
            summary["error"] = str(exc)[:200]
            await self.redis.set(DASHBOARD_KEY, json.dumps(summary), ex=600)
            return summary

        entry = result.to_redis_entry()
        try:
            await self.redis.lpush(PAPER_TRADING_REDIS_KEY, json.dumps(entry))
            await self.redis.ltrim(
                PAPER_TRADING_REDIS_KEY, 0, PAPER_TRADING_MAX_HISTORY - 1
            )
        except Exception:
            pass

        await self.redis.set(
            PENDING_KEY,
            json.dumps(
                {
                    "side": side,
                    "strike": strike,
                    "entry_binance": btc_px,
                    "opened_at": _now_iso(),
                }
            ),
            ex=7200,
        )

        raw_stats = await self.redis.get(STATS_KEY)
        stats: dict[str, Any] = {"wins": 0, "losses": 0}
        if raw_stats:
            try:
                stats.update(json.loads(raw_stats))
            except json.JSONDecodeError:
                pass
        settled = int(stats.get("wins", 0)) + int(stats.get("losses", 0))
        wlr = (int(stats.get("wins", 0)) / settled) if settled > 0 else 0.0
        summary["last_order"] = entry
        summary["win_loss_ratio"] = round(wlr, 4)
        summary["wins"] = int(stats.get("wins", 0))
        summary["losses"] = int(stats.get("losses", 0))
        summary["loss_streak"] = int(await self.redis.get(LOSS_STREAK_KEY) or 0)

        await self.redis.set(DASHBOARD_KEY, json.dumps(summary), ex=600)
        log.info(
            "poly5m_cycle_executed",
            decision=decision,
            side=side,
            velocity=velocity,
            sentiment=sent["score"],
        )
        return summary

    async def _effective_cycle_seconds(self) -> int:
        raw = await self.redis.get(REDIS_POLY_CYCLE_KEY)
        if raw is None:
            return CYCLE_S
        try:
            s = raw.decode() if isinstance(raw, (bytes, bytearray)) else str(raw)
            v = int(s.strip())
            if 10 <= v <= 3600:
                return v
        except (TypeError, ValueError):
            pass
        return CYCLE_S

    async def run_loop(self) -> None:
        log.info("poly5m_scalper_loop_started", event_id=self.event_id)
        while True:
            try:
                if await self.redis.get(KILL_SWITCH_SCALPER_HALT_KEY) == "1":
                    log.critical("poly5m_scalper_loop_stopped_kill_switch")
                    return
                cycle_s = await self._effective_cycle_seconds()
                now = time.time()
                sleep_s = cycle_s - (now % cycle_s) + 0.25
                await asyncio.sleep(sleep_s)
                if await self.redis.get(KILL_SWITCH_SCALPER_HALT_KEY) == "1":
                    log.critical("poly5m_scalper_loop_stopped_kill_switch")
                    return
                await self.run_cycle()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception("poly5m_scalper_cycle_error", error=str(exc))
                await asyncio.sleep(5.0)


async def read_dashboard_snapshot(redis: Any) -> dict[str, Any]:
    base: dict[str, Any] = {}
    raw = await redis.get(DASHBOARD_KEY)
    if raw:
        try:
            base = json.loads(raw)
        except json.JSONDecodeError:
            base = {}
    if not base:
        base = {
            "updated_at": _now_iso(),
            "event_id": POLY_EVENT_ID,
            "blocked": False,
            "decision": "NO_DATA",
            "sentiment": {
                "score": 5.0,
                "label": "neutral",
                "flash": False,
                "headline": "",
            },
        }
    raw_stats = await redis.get(STATS_KEY)
    if raw_stats:
        try:
            st = json.loads(raw_stats)
            base.setdefault("wins", int(st.get("wins", 0)))
            base.setdefault("losses", int(st.get("losses", 0)))
            s = int(st.get("wins", 0)) + int(st.get("losses", 0))
            if s > 0:
                base.setdefault(
                    "win_loss_ratio", round(int(st.get("wins", 0)) / s, 4)
                )
        except json.JSONDecodeError:
            pass
    try:
        base["loss_streak"] = int(await redis.get(LOSS_STREAK_KEY) or 0)
        base["velocity_feed_key"] = POLY5M_BTC_FEED_KEY
        base["trading_halted"] = bool(await redis.get(HALT_KEY))
    except Exception:
        pass
    return base


# ── Ultimate Scalper (1000% race, sim $1000 vs live, /api/scalper) ───────────
SIM_MODE_KEY = "nexus:scalper:simulation_mode"
SCALPER_LEDGER_KEY = "nexus:scalper:virtual_ledger"
ULTIMATE_PENDING_KEY = "nexus:scalper:pending_settlements"
ULTIMATE_ENTRY_COOLDOWN_KEY = "nexus:scalper:last_entry_ts"
LAST_ALPHA_KEY = "nexus:scalper:last_alpha_source"
RACE_STATE_KEY = "nexus:scalper:race_state"
V_BAL_KEY = "nexus:scalper:virtual_balance"

LEDGER_KEY = SCALPER_LEDGER_KEY

SIM_START_USD = 1000.0
REAL_DISPLAY_START_USD = 100.0
TARGET_GAIN_PCT = 1000.0
ULTIMATE_NEWS_MIN = 9.0
ULTIMATE_MOMENTUM_MIN = 2.0
ULTIMATE_BET_FRAC = 0.20
ULTIMATE_SETTLEMENT_S = 300
ULTIMATE_COOLDOWN_S = 45
ULTIMATE_TICK_S = 5.0
ULTIMATE_LEDGER_MAX = 200


def simulation_mode_from_env() -> bool:
    v = (os.getenv("POLY_SCALPER_SIMULATION_MODE") or "false").strip().lower()
    return v in ("1", "true", "yes", "on")


async def read_simulation_mode(redis: Any) -> bool:
    raw = await redis.get(SIM_MODE_KEY)
    if raw is not None and str(raw).strip() != "":
        return str(raw).strip().lower() in ("1", "true", "yes", "on")
    return False  # LIVE mode by default — simulation disabled


async def write_simulation_mode(redis: Any, simulation: bool) -> None:
    await redis.set(SIM_MODE_KEY, "true" if simulation else "false")


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


async def _append_scalper_ledger(redis: Any, entry: dict[str, Any]) -> None:
    await redis.rpush(SCALPER_LEDGER_KEY, json.dumps(entry, default=str))
    await redis.ltrim(SCALPER_LEDGER_KEY, -ULTIMATE_LEDGER_MAX, -1)


async def _load_binance_velocity_30s(redis: Any) -> dict[str, Any] | None:
    raw = await redis.get(BINANCE_VELOCITY_KEY)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


async def _load_openclaw_redis_sentiment(redis: Any) -> dict[str, Any] | None:
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
        baseline = SIM_START_USD if simulation else REAL_DISPLAY_START_USD
    target_mult = 1.0 + (TARGET_GAIN_PCT / 100.0)
    target_bal = baseline * target_mult
    denom = target_bal - baseline
    raw_pct = ((balance - baseline) / denom) * 100.0 if denom > 0 else 0.0
    progress_pct = max(0.0, min(100.0, raw_pct))
    payload = {
        "simulation":       simulation,
        "balance_usd":      round(balance, 2),
        "baseline_usd":     round(baseline, 2),
        "target_usd":       round(target_bal, 2),
        "progress_pct":     round(progress_pct, 2),
        "target_gain_pct":  TARGET_GAIN_PCT,
        "updated_at":       _now_iso(),
    }
    await redis.set(RACE_STATE_KEY, json.dumps(payload))
    return payload


async def _ultimate_momentum_pct(redis: Any, feed: dict[str, Any]) -> float:
    vel = await _load_binance_velocity_30s(redis)
    if vel and vel.get("momentum_pct_30s") is not None:
        return float(vel["momentum_pct_30s"])
    return float(feed.get("velocity_pct_60s") or 0.0)


async def _ultimate_news_score(redis: Any, intel_snippets: list[str]) -> tuple[float, str, str]:
    redis_sent = await _load_openclaw_redis_sentiment(redis)
    r_score = float(redis_sent["score"]) if redis_sent and redis_sent.get("score") is not None else 0.0
    channel = str(redis_sent.get("channel_title") or "") if redis_sent else ""
    excerpt = str(redis_sent.get("excerpt") or "") if redis_sent else ""
    if intel_snippets:
        syn = await synthesize_openclaw_sentiment(intel_snippets)
        s2 = float(syn.get("score") or 0.0)
        if s2 > r_score:
            return s2, syn.get("headline") or "", str(syn.get("source") or "telefix")
    return r_score, excerpt[:400], channel or "redis"


async def _process_ultimate_pending(redis: Any, simulation: bool) -> None:
    raws = await redis.lrange(ULTIMATE_PENDING_KEY, 0, -1)
    if not raws:
        return
    await redis.delete(ULTIMATE_PENDING_KEY)
    now = datetime.now(timezone.utc)
    keep: list[str] = []
    vbal = await _virtual_balance(redis) if simulation else 0.0
    binance_snap: dict[str, Any] | None = None

    for idx, raw in enumerate(raws):
        try:
            p = json.loads(raw)
        except json.JSONDecodeError:
            continue
        settle_at = datetime.fromisoformat(str(p["settle_at"]).replace("Z", "+00:00"))
        if settle_at > now:
            keep.append(raw)
            continue
        if not simulation:
            keep.append(raw)
            continue

        if binance_snap is None:
            try:
                binance_snap = await fetch_binance_data("BTCUSDT")
            except Exception as exc:
                log.warning("ultimate_settlement_binance_failed", error=str(exc))
                for item in keep:
                    await redis.rpush(ULTIMATE_PENDING_KEY, item)
                for j in range(idx, len(raws)):
                    await redis.rpush(ULTIMATE_PENDING_KEY, raws[j])
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
            await redis.set(
                LAST_ALPHA_KEY,
                json.dumps(
                    {
                        "channel_title": p.get("channel_title") or "",
                        "excerpt":       p.get("excerpt") or "",
                        "score":         p.get("news_score"),
                    }
                ),
            )
        else:
            pnl = -bet

        await _append_scalper_ledger(
            redis,
            {
                "id":            p.get("id"),
                "event":         "ultimate_settled",
                "mode":          "simulation",
                "won":           won,
                "pnl_usd":       round(pnl, 4),
                "bet_usd":       bet,
                "entry_binance": entry_px,
                "exit_binance":  exit_px,
                "settled_at":    now.isoformat(),
            },
        )

    for item in keep:
        await redis.rpush(ULTIMATE_PENDING_KEY, item)

    if simulation:
        await redis.set(V_BAL_KEY, f"{vbal:.4f}")


async def _ultimate_blocked(redis: Any) -> tuple[bool, str]:
    blocked, reason = await _system_blocks_trading(redis)
    if blocked:
        return True, reason
    try:
        if await redis.get(REDIS_BRAKE_KEY):
            return True, "wallet_safety_brake"
    except Exception:
        pass
    return False, ""


async def ultimate_compound_tick(redis: Any) -> dict[str, Any]:
    """
    OpenClaw/Telegram score > 9 AND ~30s spot momentum > 2% → 20% of balance YES.
    """
    out: dict[str, Any] = {"acted": False, "reason": "idle"}
    blocked, br = await _ultimate_blocked(redis)
    if blocked:
        out["reason"] = br
        return out

    simulation = await read_simulation_mode(redis)
    await _process_ultimate_pending(redis, simulation)

    feed_raw = await redis.get(POLY5M_BTC_FEED_KEY)
    feed: dict[str, Any] = {}
    if feed_raw:
        try:
            feed = json.loads(feed_raw)
        except json.JSONDecodeError:
            feed = {}

    intel = await _load_text_intel_snippets(_telefix_db_path())
    news_score, excerpt_hint, channel_hint = await _ultimate_news_score(redis, intel)
    momentum = await _ultimate_momentum_pct(redis, feed)

    if news_score <= ULTIMATE_NEWS_MIN or momentum <= ULTIMATE_MOMENTUM_MIN:
        out["reason"] = "thresholds_not_met"
        out["news_score"] = news_score
        out["momentum_pct"] = momentum
        bal = await _virtual_balance(redis) if simulation else await PolymarketClient().get_balance_usdc()
        baseline = SIM_START_USD if simulation else REAL_DISPLAY_START_USD
        await _persist_race_state(redis, simulation=simulation, balance=bal, baseline=baseline)
        return out

    last_raw = await redis.get(ULTIMATE_ENTRY_COOLDOWN_KEY)
    if last_raw:
        try:
            last_ts = datetime.fromisoformat(str(last_raw).replace("Z", "+00:00"))
            if (datetime.now(timezone.utc) - last_ts).total_seconds() < ULTIMATE_COOLDOWN_S:
                out["reason"] = "cooldown"
                return out
        except ValueError:
            pass

    market = await fetch_poly5m_market(POLY_EVENT_ID)
    if not market.get("market_found") or not market.get("clob_token_ids"):
        out["reason"] = "no_market"
        return out

    tokens = list(market["clob_token_ids"])
    if not tokens:
        out["reason"] = "no_token"
        return out

    yes_price = float(market.get("yes_price") or 0.5)
    market_q = str(market.get("market_question") or "BTC 5m")

    btc_px = float(feed.get("price") or 0)
    if btc_px <= 0:
        try:
            b = await fetch_binance_data("BTCUSDT")
            btc_px = float(b.get("price") or 0)
        except Exception:
            btc_px = 0.0
    if btc_px <= 0:
        out["reason"] = "no_btc_price"
        return out

    redis_sent = await _load_openclaw_redis_sentiment(redis)
    channel_title = str(redis_sent.get("channel_title") or channel_hint or "Telegram/OpenClaw")
    excerpt = str(redis_sent.get("excerpt") or excerpt_hint or "")[:400]

    if simulation:
        vbal = await _virtual_balance(redis)
        bet = round(vbal * ULTIMATE_BET_FRAC, 2)
        if bet < 1.0 or vbal < bet:
            out["reason"] = "insufficient_virtual_balance"
            return out
        vbal -= bet
        await redis.set(V_BAL_KEY, f"{vbal:.4f}")

        pos_id = str(uuid.uuid4())[:12]
        settle_at = datetime.now(timezone.utc).timestamp() + ULTIMATE_SETTLEMENT_S
        pending = {
            "id":             pos_id,
            "bet_usd":        bet,
            "yes_price":      yes_price,
            "entry_binance":  btc_px,
            "settle_at":      datetime.fromtimestamp(settle_at, tz=timezone.utc).isoformat(),
            "channel_title":  channel_title,
            "excerpt":        excerpt,
            "news_score":     news_score,
        }
        await redis.rpush(ULTIMATE_PENDING_KEY, json.dumps(pending))
        await redis.set(ULTIMATE_ENTRY_COOLDOWN_KEY, datetime.now(timezone.utc).isoformat())
        await _append_scalper_ledger(
            redis,
            {
                "id":            pos_id,
                "event":         "ultimate_open_sim",
                "bet_usd":       bet,
                "side":          "YES",
                "yes_price":     yes_price,
                "market":        market_q,
                "news_score":    news_score,
                "momentum":      momentum,
                "channel_title": channel_title,
                "opened_at":     _now_iso(),
            },
        )
        await _persist_race_state(redis, simulation=True, balance=vbal, baseline=SIM_START_USD)
        log.info(
            "ultimate_scalper_sim_entry",
            bet_usd=bet,
            news_score=news_score,
            momentum_pct=momentum,
        )
        out.update({"acted": True, "mode": "simulation", "bet_usd": bet})
        return out

    client = PolymarketClient()
    try:
        balance = await client.get_balance_usdc()
    except Exception as exc:
        log.error("ultimate_live_balance_failed", error=str(exc))
        out["reason"] = "balance_fetch_failed"
        return out

    if await evaluate_real_balance_safety_brake(redis, balance):
        out["reason"] = "safety_brake"
        return out

    bet = round(balance * ULTIMATE_BET_FRAC, 2)
    if bet < 1.0 or balance < bet:
        out["reason"] = "insufficient_live_balance"
        return out

    await redis.set(ULTIMATE_ENTRY_COOLDOWN_KEY, datetime.now(timezone.utc).isoformat())

    try:
        tr = await client.place_order_async(
            token_id=tokens[0],
            side="YES",
            price=round(min(max(yes_price, 0.01), 0.99), 4),
            market_question=market_q,
            budget_usd=bet,
            force_live=True,
        )
    except TradingHalted as exc:
        log.warning("ultimate_trading_halted", error=str(exc))
        out["reason"] = "trading_halted"
        return out
    except Exception as exc:
        log.error("ultimate_order_failed", error=str(exc))
        out["reason"] = "order_failed"
        return out

    await _append_scalper_ledger(
        redis,
        {
            "event":         "ultimate_open_live",
            "success":       tr.success,
            "bet_usd":       bet,
            "side":          "YES",
            "yes_price":     yes_price,
            "market":        market_q,
            "news_score":    news_score,
            "momentum":      momentum,
            "channel_title": channel_title,
            "order_id":      tr.order_id,
            "opened_at":     _now_iso(),
        },
    )
    try:
        balance_after = await client.get_balance_usdc()
    except Exception:
        balance_after = balance
    await _persist_race_state(
        redis,
        simulation=False,
        balance=balance_after,
        baseline=REAL_DISPLAY_START_USD,
    )
    log.info(
        "ultimate_scalper_live_entry",
        bet_usd=bet,
        success=tr.success,
        news_score=news_score,
        momentum_pct=momentum,
    )
    out.update({"acted": True, "mode": "live", "bet_usd": bet, "order_ok": tr.success})
    return out


async def build_scalper_dashboard_payload(redis: Any) -> dict[str, Any]:
    snap = await read_dashboard_snapshot(redis)
    simulation = await read_simulation_mode(redis)
    vbal = await _virtual_balance(redis) if simulation else None
    live_bal: float | None = None
    if not simulation:
        try:
            live_bal = await PolymarketClient().get_balance_usdc()
        except Exception:
            live_bal = None

    balance = float(vbal) if simulation else float(live_bal or 0.0)
    baseline = SIM_START_USD if simulation else REAL_DISPLAY_START_USD
    race = await _persist_race_state(
        redis,
        simulation=simulation,
        balance=balance,
        baseline=baseline,
    )

    vel30 = await _load_binance_velocity_30s(redis)
    redis_sent = await _load_openclaw_redis_sentiment(redis)

    last_alpha: dict[str, Any] | None = None
    raw_alpha = await redis.get(LAST_ALPHA_KEY)
    if raw_alpha:
        try:
            last_alpha = json.loads(raw_alpha)
        except json.JSONDecodeError:
            last_alpha = None

    pending_n = await redis.llen(ULTIMATE_PENDING_KEY)
    brake = False
    try:
        brake = bool(await redis.get(REDIS_BRAKE_KEY))
    except Exception:
        pass

    snap.update(
        {
            "project":             "NEXUS-ULTIMATE-SCALPER",
            "simulation_mode":     simulation,
            "virtual_balance_usd": round(vbal, 2) if vbal is not None else None,
            "live_balance_usd":    round(live_bal, 2) if live_bal is not None else None,
            "binance_velocity":    vel30,
            "openclaw_sentiment":  redis_sent,
            "race_to_1000":        race,
            "last_alpha_source":   last_alpha,
            "pending_settlements": int(pending_n),
            "safety_brake_active": brake,
            "thresholds": {
                "news_score_min":   ULTIMATE_NEWS_MIN,
                "momentum_pct_min": ULTIMATE_MOMENTUM_MIN,
                "bet_fraction":     ULTIMATE_BET_FRAC,
            },
        }
    )
    return snap


async def run_poly_scalper_loop(redis: Any) -> None:
    """
    ``POLY5M_SCALPER_API_LOOP=1`` → legacy 5m ``Poly5mScalperService`` loop.

    Otherwise (e.g. ``NEXUS_POLY_SCALPER_ENABLED=1`` on API) → Ultimate Scalper
    loop (Strategy Brain, Kelly sizing, Gamma event ``POLY_5M_EVENT_ID``).
    """
    if os.getenv("POLY5M_SCALPER_API_LOOP", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }:
        svc = Poly5mScalperService(redis=redis)
        await svc.run_loop()
        return

    from nexus.services.ultimate_scalper import run_poly_scalper_loop as _ultimate_loop

    await _ultimate_loop(redis)
