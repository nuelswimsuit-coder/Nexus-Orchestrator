"""
Nexus Brain — self-optimizing ML layer for Project Nexus.

Responsibilities
----------------
1. LOG ANALYZER      — Scans ARQ job results in Redis for WinError 121 patterns.
2. PREDICTIVE SCALING — Auto-decreases worker concurrency and increases retry
                        backoff when Windows error thresholds are exceeded.
3. ADAPTIVE DELAYS   — Reinforcement-learning loop for Telegram safety_delay:
                        +15% on FloodWaitError, -5% after 100 consecutive successes.
4. AUTO-UPGRADE      — 24-hour scheduled task that reads error logs, sends them
                        to Gemini for analysis, and delivers refactor proposals
                        to Jacob via the Telegram admin bot.

State is persisted in Redis under ``nexus:brain:state`` (JSON) so it survives
process restarts.

Usage
-----
    from src.nexus.core.brain import get_brain

    brain = await get_brain()          # singleton, lazily initialised
    await brain.start()                # launch background loops

    # In task handlers:
    await brain.record_flood_wait(e.seconds)
    await brain.record_task_success()
    delay = brain.get_safety_delay()
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import structlog

log = structlog.get_logger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

BRAIN_STATE_KEY = "nexus:brain:state"

WIN_ERROR_THRESHOLD: int = int(os.getenv("NEXUS_BRAIN_WIN_ERROR_THRESHOLD", "5"))
SAFETY_DELAY_DEFAULT: float = float(os.getenv("NEXUS_BRAIN_SAFETY_DELAY_DEFAULT", "1.0"))
SAFETY_DELAY_MIN: float = 0.1
SAFETY_DELAY_MAX: float = 60.0

ANALYSIS_INTERVAL_S: float = 300.0   # 5 minutes between job-history scans
LOG_REVIEW_INTERVAL_S: float = 86_400.0  # 24 hours

# Repo root — two levels up from src/nexus/core/
_REPO_ROOT = Path(__file__).resolve().parents[4]
_COMBINED_LOG = _REPO_ROOT / "logs" / "combined_launcher.log"
_DEBUG_LOG = _REPO_ROOT / "launcher_debug.txt"

# How many tail lines to send to Gemini for analysis
_LOG_TAIL_LINES = 200


# ── State dataclass ────────────────────────────────────────────────────────────

@dataclass
class BrainState:
    safety_delay: float = field(default_factory=lambda: SAFETY_DELAY_DEFAULT)
    win_error_count: int = 0
    success_streak: int = 0
    max_jobs_override: int | None = None
    retry_backoff_multiplier: float = 1.0
    last_log_review_ts: float = 0.0

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str) -> "BrainState":
        data = json.loads(raw)
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


# ── NexusBrain ─────────────────────────────────────────────────────────────────

class NexusBrain:
    """
    Async singleton that monitors Redis job history, adapts worker settings,
    and manages Telegram safety delays via a lightweight RL loop.
    """

    def __init__(self, redis_url: str, bot_token: str, admin_chat_id: str, gemini_api_key: str) -> None:
        self._redis_url = redis_url
        self._bot_token = bot_token
        self._admin_chat_id = admin_chat_id
        self._gemini_api_key = gemini_api_key
        self._state = BrainState()
        self._redis: Any = None
        self._lock = asyncio.Lock()
        self._background_tasks: list[asyncio.Task] = []

    # ── Redis helpers ──────────────────────────────────────────────────────────

    async def _get_redis(self) -> Any:
        if self._redis is None:
            try:
                import redis.asyncio as aioredis  # type: ignore[import-untyped]
                self._redis = await aioredis.from_url(
                    self._redis_url,
                    decode_responses=True,
                    socket_keepalive=True,
                    socket_connect_timeout=10,
                )
            except Exception as exc:
                log.warning("brain_redis_connect_failed", error=str(exc))
                raise
        return self._redis

    async def load_state(self) -> None:
        """Load BrainState from Redis; silently use defaults on failure."""
        try:
            r = await self._get_redis()
            raw = await r.get(BRAIN_STATE_KEY)
            if raw:
                self._state = BrainState.from_json(raw)
                log.info("brain_state_loaded", safety_delay=self._state.safety_delay,
                         win_error_count=self._state.win_error_count,
                         success_streak=self._state.success_streak)
        except Exception as exc:
            log.warning("brain_state_load_failed", error=str(exc))

    async def save_state(self) -> None:
        """Persist BrainState to Redis."""
        try:
            r = await self._get_redis()
            await r.set(BRAIN_STATE_KEY, self._state.to_json())
        except Exception as exc:
            log.warning("brain_state_save_failed", error=str(exc))

    # ── Public API ─────────────────────────────────────────────────────────────

    def get_safety_delay(self) -> float:
        """Return the current adaptive safety delay in seconds."""
        return self._state.safety_delay

    async def record_flood_wait(self, seconds: int) -> None:
        """
        Called when a Telegram session receives a FloodWaitError.
        Increases safety_delay by 15% (capped at SAFETY_DELAY_MAX).
        """
        async with self._lock:
            old = self._state.safety_delay
            new = min(old * 1.15, SAFETY_DELAY_MAX)
            self._state.safety_delay = round(new, 3)
            self._state.success_streak = 0  # reset streak on error
            log.warning(
                "brain_flood_wait_recorded",
                flood_wait_s=seconds,
                safety_delay_before=old,
                safety_delay_after=self._state.safety_delay,
            )
            await self.save_state()

    async def record_task_success(self) -> None:
        """
        Called after each successful task completion.
        After 100 consecutive successes, decreases safety_delay by 5%.
        """
        async with self._lock:
            self._state.success_streak += 1
            if self._state.success_streak >= 100:
                old = self._state.safety_delay
                new = max(old * 0.95, SAFETY_DELAY_MIN)
                self._state.safety_delay = round(new, 3)
                self._state.success_streak = 0
                log.info(
                    "brain_safety_delay_optimised",
                    safety_delay_before=old,
                    safety_delay_after=self._state.safety_delay,
                    reason="100_consecutive_successes",
                )
                await self.save_state()

    # ── Predictive scaling ─────────────────────────────────────────────────────

    async def analyze_job_history(self) -> None:
        """
        Scan recent ARQ job result keys for WinError 121 occurrences.
        If the count exceeds WIN_ERROR_THRESHOLD, trigger predictive scaling.
        """
        try:
            r = await self._get_redis()
        except Exception:
            return

        win_error_count = 0
        try:
            cursor = 0
            while True:
                cursor, keys = await r.scan(cursor, match="arq:job:*", count=200)
                for key in keys:
                    try:
                        raw = await r.get(key)
                        if raw and ("WinError 121" in raw or "winerror.*121" in raw.lower()):
                            win_error_count += 1
                    except Exception:
                        pass
                if cursor == 0:
                    break
        except Exception as exc:
            log.warning("brain_job_scan_failed", error=str(exc))
            return

        async with self._lock:
            self._state.win_error_count = win_error_count

        log.info("brain_job_history_analysed", win_error_121_count=win_error_count,
                 threshold=WIN_ERROR_THRESHOLD)

        if win_error_count >= WIN_ERROR_THRESHOLD:
            await self._apply_predictive_scaling(win_error_count)

    async def _apply_predictive_scaling(self, error_count: int) -> None:
        """
        Reduce WorkerSettings.max_jobs by 1 (min 1) and increase
        retry_backoff_multiplier by 0.5 to back off Windows workers.
        """
        try:
            from nexus.worker.listener import WorkerSettings  # type: ignore[import-untyped]
        except ImportError:
            log.warning("brain_scaling_skipped", reason="WorkerSettings not importable")
            return

        async with self._lock:
            current_jobs = getattr(WorkerSettings, "max_jobs", 4)
            new_jobs = max(1, current_jobs - 1)
            WorkerSettings.max_jobs = new_jobs

            self._state.max_jobs_override = new_jobs
            self._state.retry_backoff_multiplier = round(
                self._state.retry_backoff_multiplier + 0.5, 2
            )
            await self.save_state()

        log.warning(
            "brain_predictive_scaling_applied",
            win_error_121_count=error_count,
            max_jobs_before=current_jobs,
            max_jobs_after=new_jobs,
            retry_backoff_multiplier=self._state.retry_backoff_multiplier,
        )

        await self._send_telegram(
            f"🧠 *Nexus Brain — Predictive Scaling*\n\n"
            f"Detected *{error_count}* WinError 121 occurrences (threshold: {WIN_ERROR_THRESHOLD}).\n"
            f"• `max_jobs` reduced: `{current_jobs}` → `{new_jobs}`\n"
            f"• `retry_backoff_multiplier` → `{self._state.retry_backoff_multiplier}`\n\n"
            f"_No human intervention required._"
        )

    # ── 24-hour log review ─────────────────────────────────────────────────────

    async def _read_log_tail(self, n: int = _LOG_TAIL_LINES) -> str:
        """Read the last N lines from the combined launcher log."""
        for log_path in (_COMBINED_LOG, _DEBUG_LOG):
            if log_path.exists():
                try:
                    lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
                    return "\n".join(lines[-n:])
                except Exception:
                    continue
        return "(no log file found)"

    async def _query_gemini(self, log_tail: str) -> str:
        """Send log tail to Gemini and return a refactor proposal."""
        if not self._gemini_api_key:
            return "(Gemini API key not configured — set GEMINI_API_KEY in .env)"

        prompt = (
            "You are a senior Python architect reviewing the runtime logs of "
            "Nexus-Orchestrator, a multi-node task orchestration system built on "
            "ARQ, Redis, Telethon, and FastAPI.\n\n"
            "Analyse the following log tail and:\n"
            "1. Identify the top 3 recurring errors or warnings.\n"
            "2. For each, propose a concrete code refactor or configuration change "
            "that would eliminate or reduce the issue.\n"
            "3. Flag any critical issues that require immediate attention.\n\n"
            "Be concise. Use bullet points. Output in plain text (no markdown).\n\n"
            f"--- LOG TAIL (last {_LOG_TAIL_LINES} lines) ---\n{log_tail}\n--- END ---"
        )

        try:
            import google.generativeai as genai  # type: ignore[import-untyped]
            genai.configure(api_key=self._gemini_api_key)
            model = genai.GenerativeModel("gemini-2.0-flash")
            response = await asyncio.get_event_loop().run_in_executor(
                None, lambda: model.generate_content(prompt)
            )
            return response.text.strip()
        except ImportError:
            return "(google-generativeai not installed — run: pip install google-generativeai)"
        except Exception as exc:
            log.warning("brain_gemini_failed", error=str(exc))
            return f"(Gemini analysis failed: {exc})"

    async def _send_telegram(self, text: str) -> None:
        """Send a message to the Telegram admin chat via Bot API."""
        if not self._bot_token or not self._admin_chat_id:
            log.warning("brain_telegram_skipped", reason="bot_token or admin_chat_id not set")
            return

        try:
            import httpx  # type: ignore[import-untyped]
            url = f"https://api.telegram.org/bot{self._bot_token}/sendMessage"
            payload = {
                "chat_id": self._admin_chat_id,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(url, json=payload)
                if resp.status_code != 200:
                    log.warning("brain_telegram_send_failed",
                                status=resp.status_code, body=resp.text[:200])
        except Exception as exc:
            log.warning("brain_telegram_error", error=str(exc))

    async def _scheduled_log_review(self) -> None:
        """
        Background loop: every 24 hours, read the error log, send it to Gemini
        for analysis, and push the refactor proposal to Jacob via Telegram.
        """
        # Wait until the next review window if we reviewed recently
        async with self._lock:
            elapsed = time.time() - self._state.last_log_review_ts
        wait = max(0.0, LOG_REVIEW_INTERVAL_S - elapsed)
        if wait > 0:
            log.info("brain_log_review_scheduled", next_review_in_h=round(wait / 3600, 1))
            await asyncio.sleep(wait)

        while True:
            log.info("brain_log_review_starting")
            try:
                log_tail = await self._read_log_tail()
                analysis = await self._query_gemini(log_tail)

                message = (
                    "🧠 *Nexus Brain — 24h Log Review*\n\n"
                    f"*Gemini Refactor Proposals:*\n\n{analysis}\n\n"
                    f"_Log source: `{_COMBINED_LOG.name}`_"
                )
                await self._send_telegram(message)

                async with self._lock:
                    self._state.last_log_review_ts = time.time()
                    await self.save_state()

                log.info("brain_log_review_complete")
            except Exception as exc:
                log.error("brain_log_review_error", error=str(exc))

            await asyncio.sleep(LOG_REVIEW_INTERVAL_S)

    # ── Analysis loop ──────────────────────────────────────────────────────────

    async def _analysis_loop(self) -> None:
        """Run analyze_job_history every ANALYSIS_INTERVAL_S seconds."""
        while True:
            try:
                await self.analyze_job_history()
            except Exception as exc:
                log.error("brain_analysis_loop_error", error=str(exc))
            await asyncio.sleep(ANALYSIS_INTERVAL_S)

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Initialise state from Redis and launch background tasks:
        - Job history analysis loop (every 5 minutes)
        - 24-hour log review loop
        """
        await self.load_state()
        self._background_tasks = [
            asyncio.create_task(self._analysis_loop(), name="brain_analysis_loop"),
            asyncio.create_task(self._scheduled_log_review(), name="brain_log_review"),
        ]
        log.info(
            "brain_started",
            safety_delay=self._state.safety_delay,
            win_error_threshold=WIN_ERROR_THRESHOLD,
            analysis_interval_s=ANALYSIS_INTERVAL_S,
            log_review_interval_h=LOG_REVIEW_INTERVAL_S / 3600,
        )

    async def stop(self) -> None:
        """Cancel background tasks and persist final state."""
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        await self.save_state()
        log.info("brain_stopped")


# ── Singleton factory ──────────────────────────────────────────────────────────

_brain_instance: NexusBrain | None = None


async def get_brain() -> NexusBrain:
    """
    Return the process-level NexusBrain singleton.
    Reads configuration from environment / nexus settings on first call.
    """
    global _brain_instance
    if _brain_instance is None:
        try:
            from nexus.shared.config import settings  # type: ignore[import-untyped]
            redis_url = settings.redis_url
            bot_token = settings.telegram_bot_token
            admin_chat_id = settings.telegram_admin_chat_id
            gemini_api_key = settings.gemini_api_key
        except Exception:
            redis_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
            bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
            admin_chat_id = os.getenv("TELEGRAM_ADMIN_CHAT_ID", "")
            gemini_api_key = os.getenv("GEMINI_API_KEY", "")

        _brain_instance = NexusBrain(
            redis_url=redis_url,
            bot_token=bot_token,
            admin_chat_id=admin_chat_id,
            gemini_api_key=gemini_api_key,
        )
    return _brain_instance
