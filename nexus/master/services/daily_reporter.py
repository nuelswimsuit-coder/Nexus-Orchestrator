"""
Daily PnL Telegram digest — \"Race to 1,000%\" (10×) scalper journey.

Pulls simulated ledger PnL from Redis, compound reserve, live USDC balance via
PolymarketClient, and Strategy Brain market phase. Sends a Hebrew MarkdownV2
message through TelegramProvider (same path as wallet safety alerts).

Schedule: default once per day at local (NEXUS_DAILY_PNL_HOUR,
NEXUS_DAILY_PNL_MINUTE), wake every 60s like ReportingService. Override with
NEXUS_DAILY_PNL_INTERVAL_S for a fixed sleep loop (e.g. 86400).
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog

from nexus.master.services.reporting import REPORT_SENDING_KEY, REPORT_SENDING_TTL
from nexus.master.services.strategy_brain import build_strategy_snapshot, load_strategy_snapshot
from nexus.master.services.ultimate_scalper import (
    COMPOUND_RESERVE_KEY,
    LEDGER_KEY,
    REAL_DISPLAY_START_USD,
    SIM_START_USD,
    TARGET_GAIN_PCT,
    V_BAL_KEY,
    read_simulation_mode,
)
from nexus.shared.notifications.providers.telegram import TelegramProvider, _esc
from nexus.trading.polymarket_client import PolymarketClient
from nexus.trading.wallet_manager import get_polymarket_funder_address, get_polymarket_private_key

log = structlog.get_logger(__name__)

LEDGER_SCAN_MAX = 400


def _phase_hebrew(phase: str) -> str:
    return {
        "panic": "פאניקה",
        "euphoria": "אופוריה",
        "manipulation": "מניפולציה",
        "neutral": "ניטרלי",
    }.get(phase, phase or "ניטרלי")


def _architect_footer(phase: str) -> str:
    m = {
        "panic": (
            "כשהשוק נושף קר — אנחנו לא רצים עם ההמון. "
            "קונים סדר עדיפויות, לא פחד מוגזם."
        ),
        "euphoria": (
            "כולם רוקדים על השולחן — מצוין. "
            "אצלנו הרגליים נשארות על הרצפה והגזירה נשארת חדה."
        ),
        "manipulation": (
            "מישהו מסובב את הקלפים בכוונה — לא נופלים לרעש. "
            "רק למחיר, רק ללוגיקה."
        ),
        "neutral": (
            "שגרה טקטית. TeleFix/Nexus לא רודפים אחרי רעש — "
            "בונים עומק ומחכים לזווית."
        ),
    }
    return m.get(phase) or m["neutral"]


def _progress_bar(progress_pct: float, width: int = 14) -> str:
    p = max(0.0, min(100.0, progress_pct))
    filled = int(round(width * p / 100.0))
    bar = "[" + "█" * filled + "░" * (width - filled) + "]"
    return f"{bar} {p:.1f}%"


def _parse_entry_ts(entry: dict[str, Any]) -> datetime | None:
    for key in ("settled_at", "closed_at", "opened_at", "ts", "timestamp", "updated_at"):
        raw = entry.get(key)
        if not raw:
            continue
        try:
            return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            continue
    return None


async def _read_compound_reserve_usd(redis: Any) -> float:
    try:
        raw = await redis.get(COMPOUND_RESERVE_KEY)
        if raw is None:
            return 0.0
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return 0.0


async def _read_sim_balance(redis: Any) -> float:
    raw = await redis.get(V_BAL_KEY)
    if raw is None:
        return float(SIM_START_USD)
    try:
        return float(raw)
    except ValueError:
        return float(SIM_START_USD)


async def _fetch_live_balance_usd() -> float | None:
    if not get_polymarket_private_key() or not get_polymarket_funder_address():
        return None
    try:
        return float(await PolymarketClient().get_balance_usdc())
    except Exception as exc:
        log.debug("daily_reporter_balance_fetch_failed", error=str(exc))
        return None


async def _ledger_pnl_totals(redis: Any, *, since: datetime) -> tuple[float, float]:
    """Return (pnl_sum_last_window, pnl_sum_all_scanned) for entries with pnl_usd."""
    try:
        lines = await redis.lrange(LEDGER_KEY, -LEDGER_SCAN_MAX, -1)
    except Exception as exc:
        log.warning("daily_reporter_ledger_read_failed", error=str(exc))
        return 0.0, 0.0

    window_total = 0.0
    all_total = 0.0
    for raw in lines or []:
        try:
            entry = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            continue
        if "pnl_usd" not in entry:
            continue
        try:
            pnl = float(entry["pnl_usd"])
        except (TypeError, ValueError):
            continue
        all_total += pnl
        ts = _parse_entry_ts(entry)
        if ts is not None and ts >= since:
            window_total += pnl
        elif ts is None:
            # Settlements always have timestamps; if missing, skip from 24h bucket
            pass

    return window_total, all_total


def _compute_race(
    *,
    simulation: bool,
    balance_usd: float,
    baseline_usd: float,
    target_gain_pct: float = TARGET_GAIN_PCT,
) -> dict[str, Any]:
    baseline = float(baseline_usd) if baseline_usd > 0 else (
        SIM_START_USD if simulation else REAL_DISPLAY_START_USD
    )
    target_mult = 1.0 + (target_gain_pct / 100.0)
    target_bal = baseline * target_mult
    denom = target_bal - baseline
    raw_pct = ((balance_usd - baseline) / denom) * 100.0 if denom > 0 else 0.0
    progress_pct = max(0.0, min(100.0, raw_pct))
    total_gain_pct = ((balance_usd - baseline) / baseline) * 100.0 if baseline > 0 else 0.0
    remaining_gain_pct = max(0.0, target_gain_pct - total_gain_pct)
    distance_usd = max(0.0, target_bal - balance_usd)
    return {
        "simulation": simulation,
        "balance_usd": round(balance_usd, 2),
        "baseline_usd": round(baseline, 2),
        "target_usd": round(target_bal, 2),
        "progress_pct": round(progress_pct, 2),
        "total_gain_pct": round(total_gain_pct, 2),
        "remaining_gain_pct": round(remaining_gain_pct, 2),
        "distance_usd": round(distance_usd, 2),
        "target_gain_pct": target_gain_pct,
    }


async def collect_daily_pnl_context(redis: Any) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    since_24h = now - timedelta(hours=24)
    simulation = await read_simulation_mode(redis)
    if simulation:
        balance = await _read_sim_balance(redis)
        baseline = float(SIM_START_USD)
    else:
        balance = float(await _fetch_live_balance_usd() or 0.0)
        baseline = float(REAL_DISPLAY_START_USD)

    pnl_24h, ledger_cumulative = await _ledger_pnl_totals(redis, since=since_24h)
    compound = await _read_compound_reserve_usd(redis)
    race = _compute_race(
        simulation=simulation,
        balance_usd=balance,
        baseline_usd=baseline,
    )

    strat = await load_strategy_snapshot(redis)
    if strat is None:
        strat = await build_strategy_snapshot(redis, yes_price=None)

    phase = str(strat.get("market_phase") or "neutral")
    live_wallet = await _fetch_live_balance_usd()

    return {
        "generated_at": now.isoformat(),
        "simulation_mode": simulation,
        "race": race,
        "ledger_pnl_24h_usd": round(pnl_24h, 4),
        "ledger_cumulative_pnl_usd": round(ledger_cumulative, 4),
        "compound_reserve_usd": round(compound, 4),
        "live_wallet_usdc": None if live_wallet is None else round(live_wallet, 4),
        "strategy": strat,
        "market_phase": phase,
    }


def format_hebrew_daily_report(ctx: dict[str, Any]) -> str:
    race = ctx["race"]
    strat = ctx.get("strategy") or {}
    phase = str(ctx.get("market_phase") or "neutral")
    phase_h = _phase_hebrew(phase)
    bar = _esc(_progress_bar(float(race["progress_pct"])))

    dt_local = datetime.now().strftime("%Y-%m-%d")
    header = f"🚀 *{_esc('NEXUS MASTER REPORT')}\\- {_esc(dt_local)}*"

    sim = bool(ctx.get("simulation_mode"))
    mode_line = _esc("סימולציה") if sim else _esc("לייב")

    dist_esc = _esc(f"{float(race['distance_usd']):.2f}")
    prog_esc = _esc(str(race["progress_pct"]))
    phase_esc = _esc(phase_h)
    status = (
        f"{_esc('סטטוס מרוץ ל־1,000% (יעד 10x)')}: "
        f"{prog_esc}{_esc('% מהמסלול הושלמו')}\\. "
        f"{_esc('נשארו בערך')}\\ ${dist_esc} "
        f"{_esc('עד קו הסיום')}\\. "
        f"{_esc('המוח (Strategy Brain) במצב')}\\: *{phase_esc}*\\."
    )

    daily = ctx["ledger_pnl_24h_usd"]
    compound = ctx["compound_reserve_usd"]
    aggression = float(strat.get("phase_aggression") or 1.0)
    strike = bool(strat.get("master_strike"))
    strike_bit = _esc(" · STRIKE פעיל") if strike else ""

    daily_esc = _esc(f"{float(daily):+.2f}")
    compound_esc = _esc(f"{float(compound):.2f}")
    gain_esc = _esc(str(race["total_gain_pct"]))
    rem_esc = _esc(str(race["remaining_gain_pct"]))
    agg_esc = _esc(f"{aggression:.2f}x")

    lbl_sim_24h = _esc("רווח/הפסד סימולציה \\(24ש\\) מהפנקס")
    lbl_compound = _esc("רזרבת קומפאונד \\(Redis\\)")
    lbl_gain = _esc("צמיחה כוללת מבסיס")
    lbl_remain = _esc("נותר ליעד הצמיחה המופיע במסלול")
    lbl_lev = _esc("מצב מסלול / מנוף")
    lbl_agg = _esc("אגרסיה")
    pct_sym = _esc("%")

    metrics = "\n".join(
        [
            f"• *{lbl_sim_24h}\\:* `{daily_esc} USD`",
            f"• *{lbl_compound}\\:* `${compound_esc}`",
            f"• *{lbl_gain}\\:* `{gain_esc}{pct_sym}`",
            f"• *{lbl_remain}\\:* `{rem_esc}{pct_sym}`",
            f"• *{lbl_lev}\\:* `{mode_line}` · " f"*{lbl_agg}\\:* `{agg_esc}`{strike_bit}",
        ]
    )

    lbl_wallet = _esc("יתרת ארנק USDC \\(Polymarket\\)")
    lw = ctx.get("live_wallet_usdc")
    if lw is None:
        wallet_na = _esc("לא זמין — חסרות הרשאות / מפתח")
        wallet_line = f"• *{lbl_wallet}\\:* `{wallet_na}`"
    else:
        lw_esc = _esc(f"{float(lw):.2f}")
        wallet_line = f"• *{lbl_wallet}\\:* `${lw_esc}`"

    footer = _esc(_architect_footer(phase))

    parts = [
        header,
        "",
        bar,
        "",
        status,
        "",
        f"*{_esc('מדדים')}*",
        metrics,
        wallet_line,
        "",
        f"*{_esc('האדריכל')}\\:*",
        f"_{footer}_",
    ]
    return "\n".join(parts)


async def send_daily_pnl_report(
    redis: Any,
    telegram: TelegramProvider | None,
    *,
    flash_dashboard: bool = True,
) -> dict[str, Any]:
    ctx = await collect_daily_pnl_context(redis)

    if flash_dashboard and redis is not None:
        try:
            await redis.set(
                REPORT_SENDING_KEY,
                json.dumps(
                    {
                        "sending": True,
                        "period": "DAILY_PNL_RACE",
                        "started_at": datetime.now(timezone.utc).isoformat(),
                    }
                ),
                ex=REPORT_SENDING_TTL,
            )
        except Exception as exc:
            log.debug("daily_reporter_flash_skipped", error=str(exc))

    text = format_hebrew_daily_report(ctx)
    if telegram:
        await telegram.send_message(text)
        log.info("daily_pnl_report_sent", phase=ctx.get("market_phase"))
    else:
        log.warning("daily_pnl_report_skipped", reason="telegram_provider_missing")

    return ctx


class DailyPnLReporter:
    """
    Background scheduler: default local wall-clock slot, or fixed interval.
    """

    def __init__(
        self,
        redis: Any,
        telegram: TelegramProvider | None,
        *,
        report_hour: int | None = None,
        report_minute: int | None = None,
        interval_s: float | None = None,
    ) -> None:
        self._redis = redis
        self._telegram = telegram
        self._report_hour = int(
            report_hour if report_hour is not None else os.getenv("NEXUS_DAILY_PNL_HOUR", "7")
        )
        self._report_minute = int(
            report_minute
            if report_minute is not None
            else os.getenv("NEXUS_DAILY_PNL_MINUTE", "30")
        )
        raw_iv = os.getenv("NEXUS_DAILY_PNL_INTERVAL_S", "").strip()
        self._interval_s = (
            interval_s if interval_s is not None else (float(raw_iv) if raw_iv else None)
        )
        self._running = False

    def stop(self) -> None:
        self._running = False

    async def send_report(self) -> dict[str, Any]:
        return await send_daily_pnl_report(self._redis, self._telegram)

    async def run_loop(self) -> None:
        self._running = True
        if self._interval_s and self._interval_s > 0:
            log.info("daily_pnl_reporter_interval_mode", interval_s=self._interval_s)
            while self._running:
                try:
                    await self.send_report()
                except Exception as exc:
                    log.error("daily_pnl_reporter_tick_error", error=str(exc))
                await asyncio.sleep(self._interval_s)
            return

        log.info(
            "daily_pnl_reporter_clock_mode",
            at=f"{self._report_hour:02d}:{self._report_minute:02d}",
            tz_hint="local server time",
        )
        last_fired = ""
        while self._running:
            await asyncio.sleep(60)
            now = datetime.now()
            today = now.strftime("%Y-%m-%d")
            if (
                now.hour == self._report_hour
                and now.minute == self._report_minute
                and last_fired != today
            ):
                last_fired = today
                try:
                    await self.send_report()
                except Exception as exc:
                    log.error("daily_pnl_reporter_fire_error", error=str(exc))
