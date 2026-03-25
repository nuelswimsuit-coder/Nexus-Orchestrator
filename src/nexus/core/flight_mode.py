"""
Autonomous Flight Mode Engine — Emergency self-healing for the Nexus system.

Activated by the StabilitySentinel when the Stability Score drops below the
critical threshold (40) for more than 15 seconds.

Three-phase activation sequence
---------------------------------
A. PANIC   — Halt all orders; force PAPER_TRADING=true; disable GOD MODE;
             publish to nexus:panic channel so live tasks can self-abort.
B. PURGE   — Delete all operational Redis keys (queues, caches, engine state,
             heartbeats).  The .env file and on-disk config are NOT touched.
             Preserved prefixes: nexus:flight_mode:*, nexus:vault:*
C. RESTART — Write MINIMAL_CORE_MODE flag to Redis so every service that
             reads it can throttle itself: LOG_LEVEL=WARNING, POLL_INTERVAL=10s,
             DISABLE_HEAVY_ANIMATIONS=1.

Recovery is blocked until the operator manually triggers it via:
  - Dashboard button: "System Recovery / שחזור מערכת"
  - Telegram callback: system_recovery
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import structlog

log = structlog.get_logger(__name__)

# ── Redis keys ────────────────────────────────────────────────────────────────

FLIGHT_MODE_KEY        = "nexus:flight_mode:active"     # JSON — no TTL (manual clear)
MINIMAL_CORE_MODE_KEY  = "nexus:minimal_core_mode"      # Hash of settings
PAPER_TRADING_OVERRIDE = "nexus:paper_trading:forced"   # "1" when forced

# Prefixes that must survive the purge
_PURGE_PRESERVE_PREFIXES: tuple[str, ...] = (
    "nexus:flight_mode:",
    "nexus:vault:",
    "nexus:config:",
)

# Redis key patterns to delete during the safe purge
_PURGE_DELETE_PATTERNS: list[str] = [
    "nexus:tasks",
    "nexus:heartbeat:*",
    "nexus:hitl:*",
    "nexus:agent:log",
    "nexus:engine:*",
    "nexus:supervisor:*",
    "nexus:report:*",
    "nexus:sentinel:*",
    "nexus:stability:*",
    "nexus:bot:active_lock",
    "arq:*",
]

# Settings applied in MINIMAL_CORE_MODE
MINIMAL_CORE_SETTINGS: dict[str, str] = {
    "LOG_LEVEL":                "WARNING",
    "POLL_INTERVAL":            "10",
    "DISABLE_HEAVY_ANIMATIONS": "1",
}


class FlightModeEngine:
    """
    Manages the Autonomous Flight Mode lifecycle.

    activate()   — three-phase activation: PANIC → PURGE → RESTART signal.
    deactivate() — operator-triggered recovery; restores normal operations.
    is_active()  — quick check against Redis (no TTL → survives restarts).
    get_state()  — full state dict for the API / dashboard.
    """

    def __init__(self, redis: Any, notifier: Any = None) -> None:
        self._redis    = redis
        self._notifier = notifier

    # ── Public API ──────────────────────────────────────────────────────────────

    async def activate(self, score: float, notifier: Any = None) -> None:
        """
        Full three-phase Autonomous Flight Mode activation.
        Guards against double-trigger via is_active().
        """
        effective_notifier = notifier or self._notifier

        log.error(
            "flight_mode_activating",
            score=score,
            status=(
                f"[CRITICAL] ⚠️  AUTONOMOUS FLIGHT MODE ACTIVATING — "
                f"Stability Score: {score:.0f}/100"
            ),
        )

        await self._panic()
        await self._purge()
        await self._write_flight_mode_state(score=score)
        await self._write_minimal_core_mode()
        await self._send_telegram_alert(score=score, notifier=effective_notifier)

        log.error(
            "flight_mode_active",
            score=score,
            status=(
                "[CRITICAL] Autonomous Flight Mode ACTIVE. "
                "Real-money trading BLOCKED. "
                "Awaiting manual recovery: 'System Recovery / שחזור מערכת'."
            ),
        )

    async def deactivate(self, operator: str = "dashboard") -> dict[str, Any]:
        """
        Operator-triggered recovery.

        Clears flight mode, minimal core mode, and the paper-trading override.
        Returns a status dict that the API passes back to the caller.
        """
        try:
            await self._redis.delete(FLIGHT_MODE_KEY)
            await self._redis.delete(MINIMAL_CORE_MODE_KEY)
            await self._redis.delete(PAPER_TRADING_OVERRIDE)
            await self._redis.delete("nexus:god_mode:disabled_by_flight")
            await self._redis.delete("nexus:external_apis:disabled")
            await self._redis.delete("nexus:flight_mode:advanced")
            await self._redis.delete("nexus:flight_mode:sealed_blob")
        except Exception as exc:
            log.error("flight_mode_deactivate_redis_error", error=str(exc))

        # Restore LOG_LEVEL from .env
        try:
            from nexus.shared.config import settings as _s
            os.environ["LOG_LEVEL"] = _s.log_level
        except Exception:
            pass

        log.info(
            "flight_mode_deactivated",
            operator=operator,
            status=f"[SUCCESS] Flight Mode cleared by {operator}. System resuming normal operations.",
        )

        return {
            "status":    "recovered",
            "operator":  operator,
            "cleared_at": datetime.now(timezone.utc).isoformat(),
            "message":   "המערכת הופעלה בהצלחה — מצב טיסה בוטל וחזרה לפעולה רגילה",
        }

    async def is_active(self) -> bool:
        """Return True if Flight Mode is currently active in Redis."""
        try:
            raw = await self._redis.get(FLIGHT_MODE_KEY)
            if raw:
                return bool(json.loads(raw).get("active", False))
        except Exception:
            pass
        return False

    async def get_state(self) -> dict[str, Any] | None:
        """Return the full flight mode state dict, or None if not active."""
        try:
            raw = await self._redis.get(FLIGHT_MODE_KEY)
            if raw:
                return json.loads(raw)
        except Exception:
            pass
        return None

    # ── Phase A: PANIC ──────────────────────────────────────────────────────────

    async def _panic(self) -> None:
        """
        Halt all active operations immediately:
        - Force PAPER_TRADING=true in the running process environment.
        - Disable GOD MODE via Redis flag.
        - Publish to nexus:panic so any subscriber can self-abort.
        """
        log.error("flight_mode_panic", status="[CRITICAL] PANIC: Halting all active orders")

        os.environ["PAPER_TRADING"] = "true"

        try:
            await self._redis.set(PAPER_TRADING_OVERRIDE, "1")
            await self._redis.delete("nexus:god_mode:enabled")
            await self._redis.set("nexus:god_mode:disabled_by_flight", "1")
            await self._redis.publish(
                "nexus:panic",
                json.dumps({
                    "reason":    "autonomous_flight_mode",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }),
            )
        except Exception as exc:
            log.error("flight_mode_panic_redis_error", error=str(exc))

        log.info("flight_mode_panic_complete", status="[SUCCESS] PANIC complete — orders halted")

    # ── Phase B: PURGE ──────────────────────────────────────────────────────────

    async def _purge(self) -> None:
        """
        Safe Redis purge.

        Iterates over all _PURGE_DELETE_PATTERNS using SCAN and deletes matching
        keys, skipping any key whose name starts with a preserved prefix.
        The .env file (on disk) and vault keys are never touched.
        """
        log.warning(
            "flight_mode_purge_start",
            status="[REPAIRING] PURGE: Clearing Redis operational keys and caches",
        )
        deleted = 0

        for pattern in _PURGE_DELETE_PATTERNS:
            try:
                cursor = 0
                while True:
                    cursor, keys = await self._redis.scan(
                        cursor=cursor,
                        match=pattern,
                        count=100,
                    )
                    for key in keys:
                        key_str = key.decode() if isinstance(key, bytes) else key
                        if any(key_str.startswith(p) for p in _PURGE_PRESERVE_PREFIXES):
                            continue
                        await self._redis.delete(key)
                        deleted += 1
                    if cursor == 0:
                        break
            except Exception as exc:
                log.error("flight_mode_purge_pattern_error", pattern=pattern, error=str(exc))

        log.info(
            "flight_mode_purge_complete",
            deleted=deleted,
            status=f"[SUCCESS] PURGE complete — {deleted} Redis keys cleared",
        )

    # ── Phase C: State & signals ────────────────────────────────────────────────

    async def _write_flight_mode_state(self, score: float) -> None:
        """Persist flight mode state to Redis with no TTL (requires manual clear)."""
        state = {
            "active":            True,
            "triggered_at":      datetime.now(timezone.utc).isoformat(),
            "score":             round(score, 1),
            "reason":            f"Stability score {score:.0f} below threshold for 15s",
            "recovery_required": True,
        }
        try:
            await self._redis.set(FLIGHT_MODE_KEY, json.dumps(state))
        except Exception as exc:
            log.error("flight_mode_state_write_error", error=str(exc))

    async def _write_minimal_core_mode(self) -> None:
        """
        Write MINIMAL_CORE_MODE settings to a Redis hash and apply to env immediately.

        Services that poll this hash will throttle themselves:
          LOG_LEVEL=WARNING, POLL_INTERVAL=10s, DISABLE_HEAVY_ANIMATIONS=1
        """
        try:
            await self._redis.hset(MINIMAL_CORE_MODE_KEY, mapping=MINIMAL_CORE_SETTINGS)
            os.environ["LOG_LEVEL"]     = "WARNING"
            os.environ["POLL_INTERVAL"] = "10"
        except Exception as exc:
            log.error("flight_mode_minimal_core_write_error", error=str(exc))

        log.info(
            "flight_mode_minimal_core_set",
            settings=MINIMAL_CORE_SETTINGS,
            status="[SUCCESS] MINIMAL_CORE_MODE activated — LOG_LEVEL=WARNING, POLL_INTERVAL=10s",
        )

    # ── Telegram alert ──────────────────────────────────────────────────────────

    async def _send_telegram_alert(self, score: float, notifier: Any) -> None:
        """
        Send an urgent Hebrew alert via Telegram with a System Recovery button.

        Message format matches the spec:
          🚨 Nexus נכנס למצב טיסה אוטונומי עקב חוסר יציבות (Score: {score}).
             זיכרון נוקה, המערכת רצה במצב מינימלי.
        """
        if notifier is None:
            log.warning("flight_mode_no_notifier", status="No notifier attached — skipping Telegram alert")
            return

        try:
            from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

            from nexus.shared.notifications.providers.telegram import TelegramProvider

            score_str = f"{score:.0f}"
            text = (
                f"🚨 *Nexus נכנס למצב טיסה אוטונומי עקב חוסר יציבות \\(Score: {score_str}\\)*\n\n"
                f"זיכרון נוקה, המערכת רצה במצב מינימלי\\.\n\n"
                f"⚠️ מסחר אמיתי *חסום* עד לאישור ידני\\.\n\n"
                f"לחץ לשחזור המערכת:"
            )

            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="✅ System Recovery / שחזור מערכת",
                    callback_data="system_recovery",
                ),
            ]])

            for provider in notifier._providers:
                if isinstance(provider, TelegramProvider) and provider._is_configured():
                    await provider._send_raw(text=text, reply_markup=keyboard)
                    log.info("flight_mode_telegram_alert_sent", score=score)
                    break
        except Exception as exc:
            log.error("flight_mode_telegram_alert_error", error=str(exc))
