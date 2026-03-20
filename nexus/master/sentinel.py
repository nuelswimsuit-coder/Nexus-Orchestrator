"""
Nexus Sentinel — System Stability Monitor & Autonomous AI Error Management.

This module contains two complementary sentinel systems:

1. StabilitySentinel  (pre-existing)
   Runs every CHECK_INTERVAL_S (default 5 s), computes a composite Stability
   Score from four live Redis signals, and triggers Autonomous Flight Mode
   if the score stays below STABILITY_CRITICAL_THRESHOLD for too long.

2. SentinelEngine  (AI Diagnostic Engine — Nexus Sentinel AI)
   Intercepts worker exceptions via Redis pub/sub, sends the last 50 log lines
   to Gemini AI for crash analysis, and autonomously executes the recommended
   action (restart / cooldown / stop).  Also performs predictive anomaly
   detection on Binance latency and RAM usage, and handles failover logic
   when the Windows worker heartbeat is lost.

Stability Score signals (StabilitySentinel)
-------------------------------------------
  1. Decision engine state   — "warning"        → −30 pts
  2. Stuck-loop detection    — stuck key present → −25 pts
  3. Worker heartbeats       — no workers found  → −25 pts
  4. Session health ratio    — <0.3              → −30 pts, <0.5 → −15 pts

SentinelEngine Redis Keys
-------------------------
nexus:sentinel:ai:status   — current engine status JSON
nexus:sentinel:ai:events   — AI diagnosis events (capped at 20)
nexus:sentinel:ai:metrics  — rolling system metrics history (capped at 30)
nexus:sentinel:errors      — pub/sub: workers publish error events here
nexus:sentinel:failover    — pub/sub: failover directives for workers
nexus:agent:log            — shared: sentinel writes purple [SENTINEL-AI] entries

Usage
-----
    # StabilitySentinel
    sentinel = StabilitySentinel(redis=redis_pool, notifier=notifier_service)
    await sentinel.start()
    ...
    sentinel.stop()

    # SentinelEngine (AI Diagnostic)
    engine = SentinelEngine(redis=arq_pool, gemini_api_key=key)
    asyncio.create_task(engine.run_loop(), name="sentinel-ai")

    # API lifespan shim
    asyncio.create_task(run_stability_monitor(redis), name="stability_monitor")
"""

from __future__ import annotations

import asyncio
import glob
import json
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

import httpx
import psutil
import structlog

from nexus.master.flight_mode import FlightModeEngine

log = structlog.get_logger(__name__)


# ═════════════════════════════════════════════════════════════════════════════
# PART 1 — StabilitySentinel (original)
# ═════════════════════════════════════════════════════════════════════════════

CHECK_INTERVAL_S             = int(os.getenv("SENTINEL_CHECK_INTERVAL_S",  "5"))
STABILITY_CRITICAL_THRESHOLD = int(os.getenv("STABILITY_CRITICAL_THRESHOLD", "40"))
STABILITY_CRITICAL_WINDOW_S  = int(os.getenv("STABILITY_CRITICAL_WINDOW_S", "15"))

ENGINE_STATE_KEY  = "nexus:engine:state"
STUCK_STATE_KEY   = "nexus:engine:stuck"
HEARTBEAT_PATTERN = "nexus:heartbeat:*"

SESSIONS_DIR = os.getenv(
    "TELEFIX_SESSIONS_DIR",
    r"C:\Users\Yarin\Desktop\Mangement Ahu\sessions",
)

STABILITY_SCORE_KEY = "nexus:stability:score"
STABILITY_SCORE_TTL = 30


class StabilitySentinel:
    """
    Background monitor that tracks the Nexus Stability Score and triggers
    Autonomous Flight Mode when the system is critically unstable.
    """

    def __init__(
        self,
        redis: Any,
        notifier: Any = None,
        dispatcher: Any = None,
    ) -> None:
        self._redis        = redis
        self._notifier     = notifier
        self._dispatcher   = dispatcher
        self._running      = False
        self._below_threshold_since: float | None = None
        self._flight_engine = FlightModeEngine(redis=redis, notifier=notifier)

    async def start(self) -> None:
        self._running = True
        asyncio.create_task(self._loop(), name="nexus-stability-sentinel")
        log.info(
            "stability_sentinel_started",
            check_interval_s=CHECK_INTERVAL_S,
            critical_threshold=STABILITY_CRITICAL_THRESHOLD,
            critical_window_s=STABILITY_CRITICAL_WINDOW_S,
            status="[SUCCESS] Stability Sentinel started — monitoring system health",
        )

    def stop(self) -> None:
        self._running = False
        log.info("stability_sentinel_stopped", status="[SUCCESS] Stability Sentinel stopped")

    async def _loop(self) -> None:
        while self._running:
            try:
                await self._tick()
            except Exception as exc:
                log.error("sentinel_tick_error", error=str(exc))
            await asyncio.sleep(CHECK_INTERVAL_S)

    async def _tick(self) -> None:
        score = await self._compute_stability_score()
        await self._write_score(score)

        if score < STABILITY_CRITICAL_THRESHOLD:
            now = time.monotonic()
            if self._below_threshold_since is None:
                self._below_threshold_since = now
                log.warning(
                    "sentinel_score_critical",
                    score=score,
                    threshold=STABILITY_CRITICAL_THRESHOLD,
                    status=(
                        f"[REPAIRING] Stability score {score:.0f} < "
                        f"{STABILITY_CRITICAL_THRESHOLD} — monitoring for "
                        f"{STABILITY_CRITICAL_WINDOW_S}s before triggering Flight Mode"
                    ),
                )
            elapsed = now - self._below_threshold_since
            if elapsed >= STABILITY_CRITICAL_WINDOW_S:
                already_active = await self._flight_engine.is_active()
                if not already_active:
                    log.error(
                        "sentinel_flight_mode_trigger",
                        score=score,
                        elapsed_s=round(elapsed, 1),
                        status=(
                            f"[CRITICAL] Score {score:.0f} below threshold for "
                            f"{elapsed:.0f}s — activating Autonomous Flight Mode"
                        ),
                    )
                    await self._flight_engine.activate(
                        score=score,
                        notifier=self._notifier,
                    )
                self._below_threshold_since = None
        else:
            if self._below_threshold_since is not None:
                log.info(
                    "sentinel_score_recovered",
                    score=score,
                    status=f"[SUCCESS] Stability score recovered to {score:.0f} — threat window cleared",
                )
            self._below_threshold_since = None

    async def _compute_stability_score(self) -> float:
        score: float = 100.0

        try:
            raw_state = await self._redis.get(ENGINE_STATE_KEY)
            if raw_state:
                state_data = json.loads(raw_state)
                if state_data.get("state") == "warning":
                    score -= 30
        except Exception:
            pass

        try:
            if await self._redis.get(STUCK_STATE_KEY):
                score -= 25
        except Exception:
            pass

        try:
            cursor = 0
            worker_count = 0
            while True:
                cursor, keys = await self._redis.scan(
                    cursor=cursor, match=HEARTBEAT_PATTERN, count=50,
                )
                worker_count += len(keys)
                if cursor == 0:
                    break
            if worker_count == 0:
                score -= 25
        except Exception:
            pass

        try:
            active = len(glob.glob(os.path.join(SESSIONS_DIR, "adders", "*.json")))
            frozen = len(glob.glob(os.path.join(SESSIONS_DIR, "frozen", "*.json")))
            total  = active + frozen
            if total > 0:
                ratio = active / total
                if ratio < 0.3:
                    score -= 30
                elif ratio < 0.5:
                    score -= 15
        except Exception:
            pass

        return max(0.0, min(100.0, score))

    async def _write_score(self, score: float) -> None:
        try:
            payload = json.dumps({
                "score":       round(score, 1),
                "threshold":   STABILITY_CRITICAL_THRESHOLD,
                "critical":    score < STABILITY_CRITICAL_THRESHOLD,
                "below_since": self._below_threshold_since,
                "updated_at":  datetime.now(timezone.utc).isoformat(),
            })
            await self._redis.set(STABILITY_SCORE_KEY, payload, ex=STABILITY_SCORE_TTL)
        except Exception as exc:
            log.debug("sentinel_score_write_error", error=str(exc))


async def run_stability_monitor(redis: Any) -> None:
    """
    Module-level coroutine used by the API lifespan to run the StabilitySentinel
    as a background task without needing a notifier reference.
    """
    sentinel = StabilitySentinel(redis=redis)
    await sentinel.start()
    # Keep the coroutine alive so the task doesn't immediately finish
    while True:
        await asyncio.sleep(3600)


# ═════════════════════════════════════════════════════════════════════════════
# PART 2 — SentinelEngine (AI Diagnostic Engine)
# ═════════════════════════════════════════════════════════════════════════════

# ── Redis keys ────────────────────────────────────────────────────────────────

AI_STATUS_KEY   = "nexus:sentinel:ai:status"
AI_EVENTS_KEY   = "nexus:sentinel:ai:events"
AI_METRICS_KEY  = "nexus:sentinel:ai:metrics"
ERROR_CHANNEL   = "nexus:sentinel:errors"
FAILOVER_CH     = "nexus:sentinel:failover"
AGENT_LOG_KEY   = "nexus:agent:log"

WINDOWS_WORKER_KEY = "nexus:heartbeat:worker-windows"

# ── Thresholds ────────────────────────────────────────────────────────────────

LATENCY_THRESHOLD_MS        = 2_000
MEMORY_THRESHOLD_PERCENT    = 90.0
ANOMALY_CONSECUTIVE_CYCLES  = 3
METRIC_WINDOW               = 10
MAX_AI_EVENTS               = 20
HEARTBEAT_SCAN_INTERVAL     = 30.0
RELAYER_CHECK_INTERVAL      = 60.0
MAIN_LOOP_INTERVAL          = 30.0

GEMINI_MODEL    = "gemini-2.0-flash"
GEMINI_TIMEOUT  = 30.0

BINANCE_PING_URL       = "https://api.binance.com/api/v3/ping"
POLYMARKET_RELAYER_URL = "https://clob.polymarket.com"
BACKUP_RPC_ENV_KEY     = "SENTINEL_BACKUP_RPC_URL"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sentinel_log_entry(message: str, level: str = "sentinel") -> dict[str, Any]:
    return {
        "ts":       _now_iso(),
        "level":    level,
        "message":  f"[SENTINEL-AI] {message}",
        "metadata": {"source": "sentinel_ai"},
    }


class SentinelEngine:
    """
    Autonomous AI Error Management & Recovery Engine.

    Intercepts exceptions from workers/dispatcher via Redis pub/sub, calls
    Gemini AI to diagnose crashes, and executes the recommended action
    (restart / cooldown / stop) without human intervention.

    Also performs predictive anomaly detection on Binance latency and RAM,
    and handles failover when the Windows worker heartbeat is lost.
    """

    def __init__(
        self,
        redis: Any,
        gemini_api_key: str = "",
        node_id: str = "master",
        dispatcher: Any | None = None,
        notifier: Any | None = None,
    ) -> None:
        self._redis         = redis
        self._api_key       = gemini_api_key or os.environ.get("GEMINI_API_KEY", "")
        self._node_id       = node_id
        self._dispatcher    = dispatcher
        self._notifier      = notifier

        self._latency_history: deque[float] = deque(maxlen=METRIC_WINDOW)
        self._ram_history:     deque[float] = deque(maxlen=METRIC_WINDOW)
        self._latency_bad_cycles = 0
        self._ram_bad_cycles     = 0

        self._last_heartbeat_scan  = 0.0
        self._last_relayer_check   = 0.0
        self._windows_worker_online: bool | None = None

        self._rpc_url      = POLYMARKET_RELAYER_URL
        self._backup_rpc   = os.environ.get(BACKUP_RPC_ENV_KEY, "")
        self._rpc_switched = False

        self._cooldowns: dict[str, float] = {}
        self._running = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run_loop(self) -> None:
        self._running = True
        log.info("sentinel_ai_started", node_id=self._node_id)
        await self._write_status("active")
        await self._push_agent_log("מערכת Sentinel AI הופעלה — ניטור רציף החל.", "info")

        asyncio.create_task(
            self._error_subscriber_loop(),
            name="sentinel-error-subscriber",
        )

        while self._running:
            try:
                await self._run_cycle()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("sentinel_cycle_error", error=str(exc))
            await asyncio.sleep(MAIN_LOOP_INTERVAL)

    async def stop(self) -> None:
        self._running = False
        await self._write_status("stopped")

    # ── Main monitoring cycle ─────────────────────────────────────────────────

    async def _run_cycle(self) -> None:
        now = time.monotonic()
        latency_ms = await self._measure_binance_latency()
        ram_pct    = self._measure_ram_percent()

        self._latency_history.append(latency_ms)
        self._ram_history.append(ram_pct)

        await self._persist_metric({"ts": _now_iso(), "latency_ms": latency_ms, "ram_pct": ram_pct})
        await self._check_latency_anomaly(latency_ms)
        await self._check_ram_anomaly(ram_pct)

        if now - self._last_heartbeat_scan >= HEARTBEAT_SCAN_INTERVAL:
            self._last_heartbeat_scan = now
            await self._check_worker_heartbeats()

        if now - self._last_relayer_check >= RELAYER_CHECK_INTERVAL:
            self._last_relayer_check = now
            await self._check_relayer_health()

    # ── Metrics ───────────────────────────────────────────────────────────────

    async def _measure_binance_latency(self) -> float:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                t0 = time.monotonic()
                await client.get(BINANCE_PING_URL)
                return (time.monotonic() - t0) * 1000.0
        except Exception:
            return 9_999.0

    def _measure_ram_percent(self) -> float:
        try:
            return psutil.virtual_memory().percent
        except Exception:
            return 0.0

    # ── Anomaly detection ─────────────────────────────────────────────────────

    async def _check_latency_anomaly(self, latency_ms: float) -> None:
        if latency_ms > LATENCY_THRESHOLD_MS:
            self._latency_bad_cycles += 1
            log.warning("sentinel_latency_high", latency_ms=round(latency_ms, 1),
                        consecutive=self._latency_bad_cycles)
        else:
            self._latency_bad_cycles = 0
            return

        if self._latency_bad_cycles >= ANOMALY_CONSECUTIVE_CYCLES:
            self._latency_bad_cycles = 0
            msg = (
                f"זוהה סף חביון קריטי ל-Binance — {round(latency_ms)}ms "
                f"במשך {ANOMALY_CONSECUTIVE_CYCLES} מחזורים. מבצע התאוששות מונעת."
            )
            await self._push_agent_log(msg, "warning")
            await self._record_event(
                event_type="preemptive_recovery", trigger="latency_threshold",
                metric_value=latency_ms, action_taken="cooldown_prediction_tasks",
                reason_he=msg,
            )
            await self._trigger_preemptive_recovery("prediction.cross_exchange", reason=msg)

    async def _check_ram_anomaly(self, ram_pct: float) -> None:
        if ram_pct > MEMORY_THRESHOLD_PERCENT:
            self._ram_bad_cycles += 1
            log.warning("sentinel_ram_high", ram_pct=round(ram_pct, 1),
                        consecutive=self._ram_bad_cycles)
        else:
            self._ram_bad_cycles = 0
            return

        if self._ram_bad_cycles >= ANOMALY_CONSECUTIVE_CYCLES:
            self._ram_bad_cycles = 0
            msg = (
                f"זוהתה סכנת קיפאון במעבד (Worker) — שימוש בזיכרון {round(ram_pct)}% "
                f"במשך {ANOMALY_CONSECUTIVE_CYCLES} מחזורים. מבצע אתחול מונע."
            )
            await self._push_agent_log(msg, "warning")
            await self._record_event(
                event_type="preemptive_recovery", trigger="ram_threshold",
                metric_value=ram_pct, action_taken="restart_worker_module",
                reason_he=msg,
            )
            await self._trigger_preemptive_recovery("worker_module", reason=msg)

    # ── Heartbeat failover ────────────────────────────────────────────────────

    async def _check_worker_heartbeats(self) -> None:
        try:
            windows_hb     = await self._redis.get(WINDOWS_WORKER_KEY)
            windows_online = windows_hb is not None

            if self._windows_worker_online is None:
                self._windows_worker_online = windows_online
                return

            if self._windows_worker_online and not windows_online:
                self._windows_worker_online = False
                msg = (
                    "מעבד Windows אינו מגיב — מעביר משימות Polymarket "
                    "בעדיפות גבוהה למעבד Linux."
                )
                log.warning("sentinel_windows_worker_offline", failover="linux")
                await self._push_agent_log(msg, "warning")
                await self._record_event(
                    event_type="failover", trigger="worker_windows_heartbeat_lost",
                    metric_value=0, action_taken="reassign_polymarket_to_linux",
                    reason_he=msg,
                )
                await self._publish_failover_directive({
                    "type": "reassign", "from_node": "worker-windows",
                    "to_node": "worker-linux",
                    "task_types": ["prediction.cross_exchange"],
                    "reason": msg, "ts": _now_iso(),
                })
                if self._notifier:
                    try:
                        await self._notifier.send(f"🛡 [SENTINEL-AI] Failover — {msg}")
                    except Exception:
                        pass

            elif not self._windows_worker_online and windows_online:
                self._windows_worker_online = True
                msg = "מעבד Windows חזר לאוויר — מסיר הפניית Failover."
                log.info("sentinel_windows_worker_recovered")
                await self._push_agent_log(msg, "info")
                await self._publish_failover_directive({
                    "type": "cancel", "node": "worker-windows", "reason": msg, "ts": _now_iso(),
                })
        except Exception as exc:
            log.error("sentinel_heartbeat_check_error", error=str(exc))

    # ── RPC health ────────────────────────────────────────────────────────────

    async def _check_relayer_health(self) -> None:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(self._rpc_url)
                if resp.status_code < 500:
                    if self._rpc_switched:
                        self._rpc_url      = POLYMARKET_RELAYER_URL
                        self._rpc_switched = False
                        await self._push_agent_log(
                            "חיבור Relayer API ראשי שוחזר — מחזיר לנקודת הקצה הראשית.", "info"
                        )
                    return
        except Exception:
            pass

        if not self._rpc_switched and self._backup_rpc:
            self._rpc_switched = True
            self._rpc_url      = self._backup_rpc
            msg = f"Relayer API אינו נגיש — עובר לספק RPC גיבוי: {self._backup_rpc}"
            log.warning("sentinel_rpc_switched_to_backup", backup=self._backup_rpc)
            await self._push_agent_log(msg, "warning")
            await self._record_event(
                event_type="rpc_failover", trigger="relayer_unreachable",
                metric_value=0, action_taken=f"switch_to_backup:{self._backup_rpc}",
                reason_he=msg,
            )
        elif not self._backup_rpc:
            log.warning("sentinel_rpc_unavailable_no_backup")
            await self._push_agent_log("Relayer API אינו נגיש וללא RPC גיבוי מוגדר — ממתין.", "warning")

    # ── Error subscriber ──────────────────────────────────────────────────────

    async def _error_subscriber_loop(self) -> None:
        log.info("sentinel_error_subscriber_started")
        while self._running:
            try:
                pubsub = self._redis.pubsub()
                await pubsub.subscribe(ERROR_CHANNEL)
                async for message in pubsub.listen():
                    if not self._running:
                        break
                    if message.get("type") != "message":
                        continue
                    try:
                        data = json.loads(message["data"])
                        await self._handle_error_event(data)
                    except Exception as exc:
                        log.error("sentinel_error_parse_fail", error=str(exc))
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.error("sentinel_subscriber_error", error=str(exc))
                await asyncio.sleep(5)

    async def _handle_error_event(self, event: dict[str, Any]) -> None:
        node_id   = event.get("node_id", "unknown")
        task_type = event.get("task_type", "unknown")
        error_msg = event.get("error", "")
        tb        = event.get("traceback", "")

        log.error("sentinel_error_received", node_id=node_id,
                  task_type=task_type, error=error_msg)

        if self._is_in_cooldown(task_type):
            log.info("sentinel_error_in_cooldown", task_type=task_type)
            return

        await self._push_agent_log(f"שגיאה ב-{task_type} על {node_id}: {error_msg[:120]}", "error")

        ai_result = await self._ai_diagnose(
            node_id=node_id, task_type=task_type,
            error_msg=error_msg, traceback=tb,
        )
        if ai_result:
            await self._execute_ai_action(ai_result=ai_result, node_id=node_id, task_type=task_type)

    # ── AI Diagnostic ─────────────────────────────────────────────────────────

    async def _ai_diagnose(
        self,
        node_id: str,
        task_type: str,
        error_msg: str,
        traceback: str,
    ) -> dict[str, Any] | None:
        if not self._api_key:
            log.warning("sentinel_ai_no_api_key")
            return None

        try:
            raw_entries = await self._redis.lrange(AGENT_LOG_KEY, -50, -1)
            log_text = "\n".join(
                e if isinstance(e, str) else json.dumps(e, ensure_ascii=False)
                for e in (raw_entries or [])
            )
        except Exception:
            log_text = "(agent log unavailable)"

        prompt = f"""You are an expert SRE analyzing a crash in the Nexus Orchestrator distributed trading system.

Node: {node_id}
Task Type: {task_type}
Error: {error_msg}

Traceback:
{traceback[:2000] if traceback else "(no traceback)"}

Last 50 agent log lines:
{log_text[-3000:] if log_text else "(no logs)"}

Analyze this crash. Determine the root cause and choose ONE of:
- transient network error (retry will likely succeed)
- logic bug (restart won't help, needs code fix)
- rate limit (need to wait/cooldown)

Return ONLY a valid JSON object with this exact shape:
{{"action": "restart" | "stop" | "cooldown", "reason": "<1-2 sentence explanation in English>", "reason_he": "<same explanation in Hebrew>", "cooldown_minutes": <int, only if action is cooldown, else 0>}}

Do not include any text outside the JSON."""

        try:
            import google.generativeai as genai  # type: ignore[import]
            genai.configure(api_key=self._api_key)
            model = genai.GenerativeModel(GEMINI_MODEL)

            loop = asyncio.get_event_loop()
            response = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: model.generate_content(
                        prompt,
                        generation_config=genai.GenerationConfig(
                            temperature=0.2,
                            max_output_tokens=512,
                        ),
                    ),
                ),
                timeout=GEMINI_TIMEOUT,
            )
            raw = response.text.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip()

            result = json.loads(raw)
            log.info("sentinel_ai_diagnosis", action=result.get("action"), reason=result.get("reason"))
            return result

        except ImportError:
            log.warning("sentinel_ai_genai_not_installed", hint="pip install google-generativeai")
            return None
        except asyncio.TimeoutError:
            log.warning("sentinel_ai_timeout")
            return None
        except Exception as exc:
            log.error("sentinel_ai_error", error=str(exc))
            return None

    async def _execute_ai_action(
        self,
        ai_result: dict[str, Any],
        node_id: str,
        task_type: str,
    ) -> None:
        action        = ai_result.get("action", "stop")
        reason        = ai_result.get("reason", "")
        reason_he     = ai_result.get("reason_he", reason)
        cooldown_mins = int(ai_result.get("cooldown_minutes", 5) or 5)

        await self._record_event(
            event_type="ai_diagnosis", trigger=f"error:{task_type}@{node_id}",
            metric_value=0, action_taken=action,
            reason_he=reason_he, ai_reason_en=reason,
        )

        if action == "restart":
            await self._push_agent_log(f"Gemini המליץ אתחול — {reason_he}", "sentinel")
            log.info("sentinel_executing_restart", node_id=node_id, task_type=task_type)
            await self._publish_failover_directive({
                "type": "restart", "node_id": node_id,
                "task_type": task_type, "reason": reason, "ts": _now_iso(),
            })

        elif action == "cooldown":
            await self._push_agent_log(
                f"Gemini המליץ המתנה {cooldown_mins} דקות — {reason_he}", "sentinel"
            )
            self._set_cooldown(task_type, minutes=cooldown_mins)
            log.info("sentinel_executing_cooldown", task_type=task_type, minutes=cooldown_mins)

        elif action == "stop":
            await self._push_agent_log(
                f"Gemini המליץ עצירה — דרושה התערבות אנושית: {reason_he}", "error"
            )
            log.warning("sentinel_executing_stop", node_id=node_id, task_type=task_type, reason=reason)
            if self._notifier:
                try:
                    await self._notifier.send(f"🛑 [SENTINEL-AI] עצירת חירום — {reason_he}")
                except Exception:
                    pass

    # ── Preemptive recovery ───────────────────────────────────────────────────

    async def _trigger_preemptive_recovery(self, module: str, reason: str) -> None:
        await self._publish_failover_directive({
            "type": "preemptive_restart", "module": module, "reason": reason, "ts": _now_iso(),
        })
        log.info("sentinel_preemptive_recovery_triggered", module=module)
        if self._notifier:
            try:
                await self._notifier.send(f"⚡ [SENTINEL-AI] התאוששות מונעת — {reason}")
            except Exception:
                pass

    # ── Cooldown ──────────────────────────────────────────────────────────────

    def _set_cooldown(self, task_type: str, minutes: int = 5) -> None:
        self._cooldowns[task_type] = time.monotonic() + minutes * 60

    def _is_in_cooldown(self, task_type: str) -> bool:
        return time.monotonic() < self._cooldowns.get(task_type, 0)

    # ── Redis helpers ─────────────────────────────────────────────────────────

    async def _push_agent_log(self, message: str, level: str = "sentinel") -> None:
        entry = _sentinel_log_entry(message, level=level)
        try:
            await self._redis.rpush(AGENT_LOG_KEY, json.dumps(entry, ensure_ascii=False))
            await self._redis.ltrim(AGENT_LOG_KEY, -200, -1)
        except Exception as exc:
            log.warning("sentinel_agent_log_write_error", error=str(exc))

    async def _record_event(
        self,
        event_type: str,
        trigger: str,
        metric_value: float,
        action_taken: str,
        reason_he: str,
        ai_reason_en: str = "",
    ) -> None:
        event = {
            "ts":           _now_iso(),
            "event_type":   event_type,
            "trigger":      trigger,
            "metric_value": round(metric_value, 2),
            "action_taken": action_taken,
            "reason_he":    reason_he,
            "ai_reason_en": ai_reason_en,
        }
        try:
            await self._redis.rpush(AI_EVENTS_KEY, json.dumps(event, ensure_ascii=False))
            await self._redis.ltrim(AI_EVENTS_KEY, -MAX_AI_EVENTS, -1)
        except Exception as exc:
            log.warning("sentinel_event_write_error", error=str(exc))

    async def _persist_metric(self, metric: dict[str, Any]) -> None:
        try:
            await self._redis.rpush(AI_METRICS_KEY, json.dumps(metric))
            await self._redis.ltrim(AI_METRICS_KEY, -30, -1)
        except Exception:
            pass

    async def _write_status(self, state: str) -> None:
        status = {
            "state":        state,
            "node_id":      self._node_id,
            "updated_at":   _now_iso(),
            "rpc_url":      self._rpc_url,
            "rpc_switched": self._rpc_switched,
        }
        try:
            await self._redis.set(AI_STATUS_KEY, json.dumps(status))
        except Exception:
            pass

    async def _publish_failover_directive(self, directive: dict[str, Any]) -> None:
        try:
            await self._redis.publish(FAILOVER_CH, json.dumps(directive, ensure_ascii=False))
        except Exception as exc:
            log.warning("sentinel_failover_publish_error", error=str(exc))

    # ── Public class-method (for workers) ────────────────────────────────────

    @classmethod
    async def report_error(
        cls,
        redis: Any,
        node_id: str,
        task_type: str,
        error: str,
        traceback: str = "",
        severity: str = "error",
    ) -> None:
        """
        Workers/dispatcher call this to report an exception to the Sentinel.

            await SentinelEngine.report_error(
                redis=arq_pool,
                node_id="worker-windows",
                task_type="prediction.cross_exchange",
                error=str(exc),
                traceback=traceback_str,
            )
        """
        payload = {
            "node_id":   node_id,
            "task_type": task_type,
            "error":     error,
            "traceback": traceback,
            "severity":  severity,
            "ts":        _now_iso(),
        }
        try:
            await redis.publish(ERROR_CHANNEL, json.dumps(payload, ensure_ascii=False))
        except Exception as exc:
            log.warning("sentinel_report_error_publish_failed", error=str(exc))

    # ── Live status snapshot (for API) ────────────────────────────────────────

    async def get_status_snapshot(self) -> dict[str, Any]:
        return {
            "state":                "active" if self._running else "stopped",
            "node_id":              self._node_id,
            "latency_ms":           list(self._latency_history)[-1] if self._latency_history else None,
            "ram_pct":              list(self._ram_history)[-1] if self._ram_history else None,
            "latency_bad_cycles":   self._latency_bad_cycles,
            "ram_bad_cycles":       self._ram_bad_cycles,
            "windows_worker_online": self._windows_worker_online,
            "rpc_url":              self._rpc_url,
            "rpc_switched":         self._rpc_switched,
            "updated_at":           _now_iso(),
        }
