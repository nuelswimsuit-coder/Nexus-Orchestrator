"""
Decision Engine — Autonomous Profit Optimizer & Orchestrator.

The AutonomousOrchestrator is the "brain" of the Nexus system.  It runs
on a configurable interval (default 5 minutes), reads live operational data
from telefix.db and the cluster, and produces a ranked list of "Target
Actions" — concrete tasks to dispatch to workers.

Weighted Scoring Algorithm
--------------------------
Each candidate action is scored on three axes (0–100 each):

    profitability_score  — How much ROI improvement this action drives.
                           Based on: user pipeline size, target group count,
                           time since last run, forecast history.

    safety_score         — How safe it is to run right now.
                           Based on: active vs frozen session ratio,
                           CPU/RAM headroom, time since last adder run
                           (flood-wait cooldown proxy).

    resource_cost        — How expensive this action is.
                           Based on: estimated CPU spike, session count needed,
                           current worker load.

    composite = (profitability_score * W_PROFIT
               + safety_score        * W_SAFETY
               - resource_cost       * W_COST)

The composite score maps to a confidence value (0–100).
Actions with confidence < HITL_THRESHOLD are flagged as requires_approval=True
and surfaced as HITL tasks in the dashboard.

Target Actions
--------------
SCALE_SCRAPE    — Scrape source groups for fresh users.
SCALE_ADD       — Push users into target groups.
SCALE_WORKERS   — Deploy an additional worker container.
PAUSE_ACTIVITY  — Throttle all operations (no sessions / high CPU).
FORECAST_UPDATE — Re-run the financial forecast.
EMERGENCY_WARMUP— Warm up frozen sessions to restore capacity.
QUARANTINE      — Freeze a specific session showing ban signals.

RGB State Signals (written to Redis nexus:engine:state)
-------------------------------------------------------
"idle"        — Engine is not running.
"calculating" — Engine is actively scoring decisions.  Dashboard: Deep Indigo.
"dispatching" — Engine dispatched a profit task.  Dashboard: Gold/Yellow flash.
"warning"     — Engine detected a safety issue.  Dashboard: Red pulse.
"""

from __future__ import annotations

import asyncio
import glob
import json
import math
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import aiosqlite
import structlog

log = structlog.get_logger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

TELEFIX_DB   = r"C:\Users\Yarin\Desktop\Mangement Ahu\data\telefix.db"
SESSIONS_DIR = r"C:\Users\Yarin\Desktop\Mangement Ahu\sessions"

HITL_THRESHOLD       = 60    # confidence below this → requires human approval
STALE_SCRAPE_HOURS   = 6     # hours before scrape data is stale
MIN_USERS_FOR_ADDER  = 50    # minimum scraped users before running adder
WORKER_LOAD_THRESHOLD = 0.75 # fraction of max_jobs before recommending scale-out
FORECAST_STALE_DAYS  = 7     # days before re-running financial forecast
FLOOD_COOLDOWN_HOURS = 2     # minimum hours between adder runs (flood-wait proxy)
MIN_SAFETY_RATIO     = 0.3   # active/(active+frozen) below this → emergency warmup

# Scoring weights (must sum to a sensible composite)
W_PROFIT = 0.50
W_SAFETY = 0.35
W_COST   = 0.15

# Redis keys
ENGINE_STATE_KEY = "nexus:engine:state"
ENGINE_STATE_TTL = 600   # 10 min
AGENT_LOG_KEY    = "nexus:agent:log"
AGENT_LOG_MAX    = 200

# Stuck-loop detection
# After STUCK_CYCLE_THRESHOLD consecutive WARNING cycles for the same action,
# a high-priority STUCK alert is sent via Telegram with a Force Run button.
STUCK_CYCLE_THRESHOLD = 3          # 3 cycles × 1 min = 3 min (faster with 60s cycles)
STUCK_STATE_KEY       = "nexus:engine:stuck"   # JSON: {action_type, cycles, first_seen}
STUCK_STATE_TTL       = 3600       # 1 hour

# Auto-execute thresholds (Phase 20 speed boost)
AUTO_EXECUTE_THRESHOLD = 60        # confidence level for auto-execute after repetitions
AUTO_EXECUTE_CYCLES = 3            # repeat count before auto-execute

# Auto-thresholding
# Redis hash: nexus:engine:approval_streak  field=task_type  value=consecutive_approvals
# After APPROVAL_STREAK_THRESHOLD consecutive approvals of the same task type,
# lower the per-type threshold by THRESHOLD_REDUCTION points.
APPROVAL_STREAK_KEY       = "nexus:engine:approval_streak"
APPROVAL_STREAK_THRESHOLD = 3
THRESHOLD_REDUCTION       = 5
MIN_THRESHOLD             = 40     # never go below this
# Per-type threshold overrides: nexus:engine:threshold:<task_type>  value=int  TTL=7d
THRESHOLD_OVERRIDE_PREFIX = "nexus:engine:threshold:"
THRESHOLD_OVERRIDE_TTL    = 86400 * 7


# ── Enumerations ───────────────────────────────────────────────────────────────

class DecisionType(str, Enum):
    SCALE_SCRAPE     = "scale_scrape"
    SCALE_ADD        = "scale_add"
    SCALE_WORKERS    = "scale_workers"
    PAUSE_ACTIVITY   = "pause_activity"
    FORECAST_UPDATE  = "forecast_update"
    EMERGENCY_WARMUP = "emergency_warmup"
    QUARANTINE       = "quarantine"
    HITL_REQUIRED    = "hitl_required"
    IDLE             = "idle"


class EngineState(str, Enum):
    IDLE        = "idle"
    CALCULATING = "calculating"
    DISPATCHING = "dispatching"
    WARNING     = "warning"


# ── Data models ────────────────────────────────────────────────────────────────

@dataclass
class ScoredAction:
    """A candidate action with its three component scores."""
    decision_type: DecisionType
    title: str
    reasoning: str
    profitability_score: float   # 0–100
    safety_score: float          # 0–100
    resource_cost: float         # 0–100 (higher = more expensive)
    action_task_type: str
    action_params: dict[str, Any] = field(default_factory=dict)

    @property
    def composite(self) -> float:
        return (
            self.profitability_score * W_PROFIT
            + self.safety_score * W_SAFETY
            - self.resource_cost * W_COST
        )

    @property
    def confidence(self) -> int:
        return max(0, min(100, int(self.composite)))

    @property
    def requires_approval(self) -> bool:
        return self.confidence < HITL_THRESHOLD

    @property
    def roi_impact(self) -> str:
        if self.profitability_score >= 80:
            return f"High ROI impact (+{int(self.profitability_score * 0.3):.0f}% est.)"
        if self.profitability_score >= 50:
            return f"Medium ROI impact (+{int(self.profitability_score * 0.2):.0f}% est.)"
        return "Maintenance / safety action"

    def to_decision_dict(self) -> dict[str, Any]:
        return {
            "decision_type": self.decision_type.value,
            "title": self.title,
            "reasoning": self.reasoning,
            "confidence": self.confidence,
            "roi_impact": self.roi_impact,
            "action_task_type": self.action_task_type,
            "action_params": self.action_params,
            "requires_approval": self.requires_approval,
            "profitability_score": round(self.profitability_score, 1),
            "safety_score": round(self.safety_score, 1),
            "resource_cost": round(self.resource_cost, 1),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }


@dataclass
class EngineContext:
    """Snapshot of all data the engine uses to reason."""
    scraped_users: int = 0
    users_pipeline: int = 0
    managed_groups: int = 0
    source_groups: int = 0
    target_groups: int = 0
    pending_enrollments: int = 0
    last_scraper_run_ts: float = 0.0
    last_adder_run_ts: float = 0.0
    last_forecast_run_ts: float = 0.0
    active_sessions: int = 0
    frozen_sessions: int = 0
    manager_sessions: int = 0
    active_workers: int = 0
    total_active_jobs: int = 0
    max_jobs_per_worker: int = 4
    forecast_history: list[str] = field(default_factory=list)
    price_per_group: float = 0.0
    cost_per_k: float = 0.0
    k_per_group: float = 1.0
    db_available: bool = False


# ── Data collection ────────────────────────────────────────────────────────────

async def _collect_context(
    cluster_workers: int = 0,
    total_active_jobs: int = 0,
    max_jobs_per_worker: int = 4,
) -> EngineContext:
    ctx = EngineContext(
        active_workers=cluster_workers,
        total_active_jobs=total_active_jobs,
        max_jobs_per_worker=max_jobs_per_worker,
    )

    # Session file counts (fast, no DB needed)
    ctx.active_sessions  = len(glob.glob(os.path.join(SESSIONS_DIR, "adders",   "*.json")))
    ctx.frozen_sessions  = len(glob.glob(os.path.join(SESSIONS_DIR, "frozen",   "*.json")))
    ctx.manager_sessions = len(glob.glob(os.path.join(SESSIONS_DIR, "managers", "*.json")))

    if not os.path.exists(TELEFIX_DB):
        return ctx

    try:
        uri = f"file:{TELEFIX_DB.replace(chr(92), '/')}?mode=ro"
        async with aiosqlite.connect(uri, uri=True, timeout=5) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA busy_timeout = 5000")

            async with db.execute(
                "SELECT COUNT(DISTINCT user_id) AS n FROM scraped_users"
            ) as c:
                r = await c.fetchone()
                ctx.scraped_users = r["n"] if r else 0

            async with db.execute("SELECT COUNT(*) AS n FROM users") as c:
                r = await c.fetchone()
                ctx.users_pipeline = r["n"] if r else 0

            async with db.execute("SELECT COUNT(*) AS n FROM managed_groups") as c:
                r = await c.fetchone()
                ctx.managed_groups = r["n"] if r else 0

            async with db.execute(
                "SELECT COUNT(*) AS n FROM targets WHERE role='source'"
            ) as c:
                r = await c.fetchone()
                ctx.source_groups = r["n"] if r else 0

            async with db.execute(
                "SELECT COUNT(*) AS n FROM targets WHERE role='target'"
            ) as c:
                r = await c.fetchone()
                ctx.target_groups = r["n"] if r else 0

            async with db.execute(
                "SELECT COUNT(*) AS n FROM enrollments WHERE status='PENDING'"
            ) as c:
                r = await c.fetchone()
                ctx.pending_enrollments = r["n"] if r else 0

            async with db.execute(
                "SELECT key, value FROM metrics WHERE key LIKE 'last_run:%'"
            ) as c:
                for row in await c.fetchall():
                    k, v = row["key"], float(row["value"] or 0)
                    if k == "last_run:scraper":
                        ctx.last_scraper_run_ts = v
                    elif k == "last_run:adder":
                        ctx.last_adder_run_ts = v
                    elif k == "last_run:forecast":
                        ctx.last_forecast_run_ts = v

            async with db.execute(
                "SELECT value FROM settings WHERE key='forecast:history'"
            ) as c:
                r = await c.fetchone()
                if r and r["value"]:
                    ctx.forecast_history = [
                        d.strip() for d in r["value"].split(",") if d.strip()
                    ]

        ctx.db_available = True
    except Exception as exc:
        log.error("decision_engine_db_error", error=str(exc))

    return ctx


# ── Financial model ────────────────────────────────────────────────────────────

def _estimate_roi(ctx: EngineContext) -> dict[str, Any]:
    num_groups   = max(ctx.target_groups, 1)
    k_per_group  = max(ctx.k_per_group, 1.0)
    cost_per_k   = max(ctx.cost_per_k, 0.5)
    price_per_grp = max(ctx.price_per_group, 10.0)

    total_cost      = num_groups * k_per_group * cost_per_k
    potential_rev   = num_groups * price_per_grp
    realistic_rev   = potential_rev * 0.8
    realistic_net   = realistic_rev - total_cost
    roi             = (realistic_net / total_cost * 100) if total_cost > 0 else 0
    break_even      = math.ceil(total_cost / price_per_grp) if price_per_grp > 0 else 0

    return {
        "roi_percent":       int(roi),
        "realistic_net":     round(realistic_net, 2),
        "break_even_groups": break_even,
        "total_cost":        round(total_cost, 2),
    }


# ── Scoring helpers ────────────────────────────────────────────────────────────

def _session_safety_ratio(ctx: EngineContext) -> float:
    """Active sessions as a fraction of total (0–1). Higher = safer."""
    total = ctx.active_sessions + ctx.frozen_sessions
    if total == 0:
        return 0.0
    return ctx.active_sessions / total


def _worker_load(ctx: EngineContext) -> float:
    """Current worker load fraction (0–1)."""
    capacity = ctx.active_workers * ctx.max_jobs_per_worker
    if capacity == 0:
        return 0.0
    return min(1.0, ctx.total_active_jobs / capacity)


def _hours_since(ts: float) -> float:
    return (time.time() - ts) / 3600


# ── Scoring rules ──────────────────────────────────────────────────────────────

def _score_scrape(ctx: EngineContext) -> ScoredAction | None:
    hours_stale = _hours_since(ctx.last_scraper_run_ts)
    if hours_stale < STALE_SCRAPE_HOURS:
        return None
    if ctx.active_sessions < 1:
        return None

    # Profitability: staler data = higher urgency
    staleness_factor = min(1.0, (hours_stale - STALE_SCRAPE_HOURS) / 24)
    profitability = 40 + staleness_factor * 50

    # Safety: more active sessions = safer to run
    safety = min(100, _session_safety_ratio(ctx) * 100 * 1.2)

    # Cost: moderate CPU spike, uses sessions
    resource_cost = 30 + (1 - _session_safety_ratio(ctx)) * 20

    return ScoredAction(
        decision_type=DecisionType.SCALE_SCRAPE,
        title=f"Scrape {ctx.source_groups} source group(s) for fresh users",
        reasoning=(
            f"User data is {hours_stale:.1f}h stale (threshold: {STALE_SCRAPE_HOURS}h). "
            f"{ctx.source_groups} source groups available. "
            f"{ctx.active_sessions} active sessions ready. "
            f"Pipeline has {ctx.scraped_users} users — refreshing will improve add success rate."
        ),
        profitability_score=profitability,
        safety_score=safety,
        resource_cost=resource_cost,
        action_task_type="telegram.auto_scrape",
        action_params={"force": False},
    )


def _score_adder(ctx: EngineContext) -> ScoredAction | None:
    if ctx.scraped_users < MIN_USERS_FOR_ADDER:
        return None
    if ctx.target_groups < 1:
        return None
    hours_since_add = _hours_since(ctx.last_adder_run_ts)
    if hours_since_add < FLOOD_COOLDOWN_HOURS:
        return None

    safety_ratio = _session_safety_ratio(ctx)
    fill_ratio   = ctx.users_pipeline / max(ctx.target_groups * 500, 1)

    # Profitability: more users + more targets = higher value
    profitability = min(95, 50 + fill_ratio * 30 + (ctx.target_groups / 10) * 10)

    # Safety: penalise low session health
    safety = max(10, safety_ratio * 100)

    # Cost: adder is CPU-intensive and uses many sessions
    resource_cost = 40 + (1 - safety_ratio) * 30

    return ScoredAction(
        decision_type=DecisionType.SCALE_ADD,
        title=f"Add {ctx.scraped_users} users to {ctx.target_groups} target group(s)",
        reasoning=(
            f"{ctx.scraped_users} scraped users ready. "
            f"{ctx.target_groups} target groups need filling. "
            f"Last adder run: {hours_since_add:.1f}h ago (cooldown: {FLOOD_COOLDOWN_HOURS}h). "
            f"Session health: {safety_ratio:.0%} active. "
            f"Estimated ROI: fill {ctx.target_groups} groups at current pipeline rate."
        ),
        profitability_score=profitability,
        safety_score=safety,
        resource_cost=resource_cost,
        action_task_type="telegram.auto_add",
        action_params={},
    )


def _score_emergency_warmup(ctx: EngineContext) -> ScoredAction | None:
    safety_ratio = _session_safety_ratio(ctx)
    if safety_ratio >= MIN_SAFETY_RATIO or ctx.frozen_sessions == 0:
        return None

    return ScoredAction(
        decision_type=DecisionType.EMERGENCY_WARMUP,
        title=f"Emergency warmup — {ctx.frozen_sessions} frozen session(s)",
        reasoning=(
            f"Session safety ratio is {safety_ratio:.0%} "
            f"(threshold: {MIN_SAFETY_RATIO:.0%}). "
            f"{ctx.frozen_sessions} sessions are frozen. "
            "Running warmup to restore operational capacity before next add cycle."
        ),
        profitability_score=20,
        safety_score=90,
        resource_cost=15,
        action_task_type="telegram.run_warmup",
        action_params={"max_sessions": min(ctx.frozen_sessions, 10)},
    )


def _score_scale_workers(ctx: EngineContext) -> ScoredAction | None:
    if ctx.active_workers < 1:
        return None
    load = _worker_load(ctx)
    if load < WORKER_LOAD_THRESHOLD:
        return None

    return ScoredAction(
        decision_type=DecisionType.SCALE_WORKERS,
        title="Deploy additional worker node",
        reasoning=(
            f"Worker load at {load:.0%} "
            f"({ctx.total_active_jobs} active jobs / "
            f"{ctx.active_workers * ctx.max_jobs_per_worker} capacity). "
            "Scaling out will reduce task queue latency and increase throughput."
        ),
        profitability_score=60,
        safety_score=85,
        resource_cost=10,
        action_task_type="nexus.scale_worker",
        action_params={"count": 1},
    )


def _score_forecast(ctx: EngineContext) -> ScoredAction | None:
    days_since = _hours_since(ctx.last_forecast_run_ts) / 24
    if days_since < FORECAST_STALE_DAYS:
        return None

    return ScoredAction(
        decision_type=DecisionType.FORECAST_UPDATE,
        title="Update financial forecast model",
        reasoning=(
            f"Last forecast was {days_since:.0f} days ago "
            f"(threshold: {FORECAST_STALE_DAYS} days). "
            f"Forecast history has {len(ctx.forecast_history)} data points. "
            "Refreshing will improve ROI projections for future decisions."
        ),
        profitability_score=35,
        safety_score=100,
        resource_cost=5,
        action_task_type="telegram.run_forecast",
        action_params={},
    )


def _score_idle_worker_opportunity(ctx: EngineContext) -> ScoredAction | None:
    """
    Opportunity Cost Rule — if workers are idle, pivot to the most profitable task.

    When workers have capacity AND no scrape/add is running, this rule
    calculates the real-time opportunity cost of inaction and recommends
    the highest-ROI task to fill the idle slots.

    Priority order:
      1. Warmup (if frozen sessions > 30% of total) — restores capacity
      2. Scrape (if data is stale) — fills the pipeline
      3. Add (if users are ready) — generates revenue
    """
    if ctx.active_workers < 1:
        return None

    # Only fire if workers have significant idle capacity
    load = _worker_load(ctx)
    if load > 0.3:
        return None  # workers are busy enough

    # Determine the highest-value idle action
    safety_ratio = _session_safety_ratio(ctx)
    hours_stale  = _hours_since(ctx.last_scraper_run_ts)
    hours_add    = _hours_since(ctx.last_adder_run_ts)

    # Case 1: Session health is critical — warmup first
    if safety_ratio < 0.4 and ctx.frozen_sessions > 0:
        return ScoredAction(
            decision_type=DecisionType.EMERGENCY_WARMUP,
            title=f"Idle worker → pivot to warmup ({ctx.frozen_sessions} frozen sessions)",
            reasoning=(
                f"Workers at {load:.0%} load (idle). "
                f"Session health critical: {safety_ratio:.0%} active. "
                "Warming up frozen sessions maximises future throughput."
            ),
            profitability_score=55,
            safety_score=95,
            resource_cost=10,
            action_task_type="telegram.run_warmup",
            action_params={"max_sessions": min(ctx.frozen_sessions, 10)},
        )

    # Case 2: Data is stale — scrape to fill pipeline
    if hours_stale >= STALE_SCRAPE_HOURS and ctx.active_sessions > 0:
        return ScoredAction(
            decision_type=DecisionType.SCALE_SCRAPE,
            title=f"Idle worker → pivot to scrape ({hours_stale:.0f}h stale data)",
            reasoning=(
                f"Workers at {load:.0%} load (idle). "
                f"Scrape data is {hours_stale:.0f}h old. "
                "Filling the pipeline now maximises add success rate."
            ),
            profitability_score=65,
            safety_score=80,
            resource_cost=20,
            action_task_type="telegram.auto_scrape",
            action_params={"force": False},
        )

    # Case 3: Users ready — add to groups for revenue
    if ctx.scraped_users >= MIN_USERS_FOR_ADDER and hours_add >= FLOOD_COOLDOWN_HOURS:
        return ScoredAction(
            decision_type=DecisionType.SCALE_ADD,
            title=f"Idle worker → pivot to add ({ctx.scraped_users} users ready)",
            reasoning=(
                f"Workers at {load:.0%} load (idle). "
                f"{ctx.scraped_users} users ready to add. "
                "Adding now converts pipeline users to revenue."
            ),
            profitability_score=75,
            safety_score=max(10, safety_ratio * 100),
            resource_cost=35,
            action_task_type="telegram.auto_add",
            action_params={},
        )

    return None


def _score_pause(ctx: EngineContext) -> ScoredAction | None:
    if ctx.active_sessions > 0:
        return None

    return ScoredAction(
        decision_type=DecisionType.PAUSE_ACTIVITY,
        title="Pause Telegram operations — no active sessions",
        reasoning=(
            f"0 active sessions in {SESSIONS_DIR}/adders/. "
            f"{ctx.frozen_sessions} sessions are frozen. "
            "All Telegram operations are blocked until sessions are warmed up."
        ),
        profitability_score=0,
        safety_score=100,
        resource_cost=0,
        action_task_type="",
        action_params={},
    )


# ── AutonomousOrchestrator ─────────────────────────────────────────────────────

class AutonomousOrchestrator:
    """
    The autonomous brain of the Nexus system.

    Runs on a configurable interval, scores all candidate actions using the
    weighted algorithm, dispatches the top action (if confidence is high
    enough), and writes its reasoning to the Redis agent log.

    Usage
    -----
        orchestrator = AutonomousOrchestrator(dispatcher, redis)
        asyncio.create_task(orchestrator.run_loop(interval_seconds=300))

    The orchestrator writes to two Redis keys:
        nexus:engine:state  — current state ("idle" / "calculating" / "dispatching")
        nexus:agent:log     — list of reasoning log entries (LPUSH)
    """

    def __init__(
        self,
        dispatcher: Any,   # nexus.master.dispatcher.Dispatcher
        redis: Any,        # redis.asyncio.Redis
        notifier: Any = None,  # nexus.shared.notifications.service.NotificationService
    ) -> None:
        self._dispatcher = dispatcher
        self._redis = redis
        self._notifier = notifier
        self._running = False
        # In-memory stuck counter: action_type → consecutive WARNING cycles
        self._stuck_cycles: dict[str, int] = {}
        # Auto-execute tracking: action_type → (count, last_confidence)
        self._repeat_tracker: dict[str, tuple[int, float]] = {}
        # Pulse logging
        self._last_pulse_time = time.time()
        self._current_state = "idle"

    # ── Public API ─────────────────────────────────────────────────────────────

    async def run_loop(self, interval_seconds: int = 300) -> None:
        """
        Background loop — runs the scoring cycle every `interval_seconds`.
        Designed to be launched as an asyncio task.
        """
        self._running = True
        log.info("autonomous_orchestrator_started", interval_s=interval_seconds)
        await self._log_entry("info", "Autonomous Orchestrator started", {
            "interval_s": interval_seconds,
        })

        while self._running:
            try:
                await self._cycle()
            except Exception as exc:
                log.error("orchestrator_cycle_error", error=str(exc))
                await self._log_entry("error", f"Cycle error: {exc}", {})

            await asyncio.sleep(interval_seconds)

    async def run_once(
        self,
        cluster_workers: int = 0,
        total_active_jobs: int = 0,
    ) -> list[dict[str, Any]]:
        """
        Run a single scoring cycle and return the ranked decisions.
        Used by the API endpoint for on-demand analysis.
        """
        return await self._cycle(
            cluster_workers=cluster_workers,
            total_active_jobs=total_active_jobs,
            dispatch=False,
        )

    def stop(self) -> None:
        self._running = False

    # ── Phase 20: Speed boost helpers ──────────────────────────────────────────

    async def _check_auto_execute(self, action_type: str, confidence: float) -> bool:
        """
        Check if this action should auto-execute due to repetition.
        Returns True if: action repeated ≥ AUTO_EXECUTE_CYCLES times 
        with confidence ≥ AUTO_EXECUTE_THRESHOLD.
        """
        if confidence < AUTO_EXECUTE_THRESHOLD:
            # Clear counter if confidence dropped below threshold
            self._repeat_tracker.pop(action_type, None)
            return False

        count, last_conf = self._repeat_tracker.get(action_type, (0, 0.0))
        
        # Increment if confidence is still above threshold
        if confidence >= AUTO_EXECUTE_THRESHOLD:
            count += 1
            self._repeat_tracker[action_type] = (count, confidence)
            
            if count >= AUTO_EXECUTE_CYCLES:
                await self._log_entry("decision",
                    f"Auto-execute trigger: {action_type} repeated {count}× "
                    f"with avg confidence {confidence:.0f} — preventing stagnation",
                    {"auto_execute_trigger": True, "repeat_count": count},
                )
                return True
        
        return False

    async def _pulse_log(self, ctx: Any, fin: dict) -> None:
        """
        Emit a system pulse log every 60 seconds with live stats.
        Format: [PULSE] | Workers: 2 | Total Sessions: 42 | Daily ROI: +12% | Status: High Alert
        """
        current_time = time.time()
        
        # Only pulse once per minute to avoid spam
        if current_time - self._last_pulse_time < 60:
            return
        
        self._last_pulse_time = current_time

        # Collect live stats from desktop projects (via the explorer service)
        try:
            from nexus.master.services.explorer import get_budget_widget_data
            budget_data = await get_budget_widget_data(self._redis)
            daily_pnl = budget_data.get("daily_pnl", 0) if budget_data.get("available") else 0
        except Exception:
            daily_pnl = 0

        # Worker count from cluster (fallback to 0)
        worker_count = getattr(ctx, 'cluster_workers', 0)
        
        # Total sessions from context
        total_sessions = getattr(ctx, 'active_sessions', 0) + getattr(ctx, 'frozen_sessions', 0)
        
        # ROI percentage
        roi_pct = fin.get('roi_percent', 0)
        
        # System status based on current state
        if hasattr(self, '_current_state'):
            status_map = {
                "calculating": "Processing",
                "dispatching": "Active",
                "warning": "High Alert", 
                "idle": "Optimal",
            }
            status = status_map.get(self._current_state, "Unknown")
        else:
            status = "Optimal"

        # Format the pulse log
        pulse_msg = (
            f"[PULSE] | Workers: {worker_count} | "
            f"Sessions: {total_sessions} | "
            f"Daily ROI: {daily_pnl:+.1f}% | "
            f"Status: {status}"
        )

        await self._log_entry("info", pulse_msg, {
            "pulse": True,
            "workers": worker_count,
            "sessions": total_sessions, 
            "daily_roi_pct": daily_pnl,
            "system_status": status,
        })

    # ── Internal ───────────────────────────────────────────────────────────────

    async def _cycle(
        self,
        cluster_workers: int | None = None,
        total_active_jobs: int | None = None,
        dispatch: bool = True,
    ) -> list[dict[str, Any]]:
        """
        One full reasoning cycle:
        1. Set state → calculating (RGB: Deep Indigo)
        2. Collect context from DB + cluster
        3. Score all candidate actions
        4. Log reasoning
        5. Dispatch top action if confidence ≥ HITL_THRESHOLD
        6. Set state → dispatching (RGB: Gold) or idle
        """
        await self._set_state(EngineState.CALCULATING)
        await self._log_entry("decision", "── Starting decision cycle ──", {})

        # Collect context
        workers = cluster_workers
        jobs = total_active_jobs
        if workers is None and self._dispatcher is not None:
            workers = 0
            jobs = len(getattr(self._dispatcher, "_in_flight", {}))

        ctx = await _collect_context(
            cluster_workers=workers or 0,
            total_active_jobs=jobs or 0,
        )
        fin = _estimate_roi(ctx)

        await self._log_entry("info",
            f"Context: {ctx.scraped_users} scraped users, "
            f"{ctx.active_sessions} active sessions, "
            f"{ctx.target_groups} target groups, "
            f"ROI est. {fin['roi_percent']}%",
            {"scraped": ctx.scraped_users, "sessions": ctx.active_sessions},
        )

        # Score all rules
        rule_fns = [
            lambda: _score_pause(ctx),
            lambda: _score_emergency_warmup(ctx),
            lambda: _score_scrape(ctx),
            lambda: _score_adder(ctx),
            lambda: _score_idle_worker_opportunity(ctx),
            lambda: _score_scale_workers(ctx),
            lambda: _score_forecast(ctx),
        ]

        actions: list[ScoredAction] = []
        for fn in rule_fns:
            action = fn()
            if action is not None:
                actions.append(action)
                await self._log_entry("info",
                    f"Scored: [{action.decision_type.value}] "
                    f"P={action.profitability_score:.0f} "
                    f"S={action.safety_score:.0f} "
                    f"C={action.resource_cost:.0f} "
                    f"→ confidence={action.confidence}",
                    {"type": action.decision_type.value, "conf": action.confidence},
                )

        # Sort by composite score
        actions.sort(key=lambda a: a.composite, reverse=True)

        if not actions:
            await self._log_entry("info", "No actions needed — system is optimal.", {})
            await self._set_state(EngineState.IDLE)
            await self._pulse_log(ctx, fin)
            return []

        top = actions[0]
        await self._log_entry("decision",
            f"Top action: [{top.decision_type.value}] "
            f"confidence={top.confidence} "
            f"— {top.title}",
            top.to_decision_dict(),
        )

        # Resolve effective threshold for this action type (may be lowered by auto-thresholding)
        effective_threshold = await self._get_effective_threshold(top.decision_type.value)

        # Dispatch top action if auto-approved and dispatcher is available
        if dispatch and self._dispatcher is not None:
            if top.action_task_type and top.confidence >= effective_threshold:
                # Reset stuck counter on successful dispatch
                self._stuck_cycles.pop(top.decision_type.value, None)
                await self._dispatch_action(top, fin)
            elif top.confidence < effective_threshold:
                gap = effective_threshold - top.confidence
                
                # ── Auto-execute logic (Phase 20 speed boost) ──────────────────
                action_type = top.decision_type.value
                should_auto_exec = await self._check_auto_execute(action_type, top.confidence)
                
                if should_auto_exec:
                    await self._log_entry("action",
                        f"AUTO-EXECUTE: {top.title} (repeated {AUTO_EXECUTE_CYCLES}× "
                        f"with conf ≥ {AUTO_EXECUTE_THRESHOLD})",
                        {"auto_execute": True, "confidence": top.confidence},
                    )
                    self._stuck_cycles.pop(action_type, None)
                    self._repeat_tracker.pop(action_type, None)
                    await self._dispatch_action(top, fin)
                else:
                    await self._log_entry(
                        "warning",
                        f"Action requires approval (conf {top.confidence} < "
                        f"threshold {effective_threshold}, gap={gap}): {top.title}",
                        {
                            "hitl": True,
                            "confidence": top.confidence,
                            "threshold": effective_threshold,
                            "gap": gap,
                            "action_type": action_type,
                        },
                    )
                    await self._set_state(EngineState.WARNING)
                    # ── Stuck-loop detection ───────────────────────────────────────
                    await self._check_stuck(top, gap)
            else:
                await self._log_entry("info",
                    f"Action has no task to dispatch: {top.title}", {})
                await self._set_state(EngineState.IDLE)
        else:
            await self._set_state(EngineState.IDLE)

        # ── Pulse log (every cycle) ────────────────────────────────────────────
        await self._pulse_log(ctx, fin)

        return [a.to_decision_dict() for a in actions]

    async def _dispatch_action(self, action: ScoredAction, fin: dict) -> None:
        """Dispatch the top action as a Nexus task."""
        from nexus.shared.schemas import TaskPayload

        await self._log_entry("action",
            f"Dispatching: {action.title} → task={action.action_task_type}",
            {"task_type": action.action_task_type},
        )
        await self._set_state(EngineState.DISPATCHING)

        try:
            task = TaskPayload(
                task_type=action.action_task_type,
                parameters=action.action_params,
                project_id="telefix",
                priority=2,
            )
            job_id = await self._dispatcher.dispatch(task)
            await self._log_entry("action",
                f"Dispatched {action.action_task_type} → job_id={job_id}",
                {"job_id": job_id, "task_type": action.action_task_type},
            )
        except Exception as exc:
            await self._log_entry("error",
                f"Dispatch failed: {exc}", {"error": str(exc)})
        finally:
            await asyncio.sleep(2)  # hold "dispatching" state briefly for RGB flash
            await self._set_state(EngineState.IDLE)

    async def _set_state(self, state: EngineState) -> None:
        """Write engine state to Redis for the dashboard RGB sync."""
        self._current_state = state.value  # Store for pulse logging
        if self._redis is None:
            return
        payload = json.dumps({
            "state": state.value,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
        await self._redis.set(ENGINE_STATE_KEY, payload, ex=ENGINE_STATE_TTL)

    async def _log_entry(
        self,
        level: str,
        message: str,
        metadata: dict[str, Any],
    ) -> None:
        """Append an entry to the Redis agent log."""
        entry = json.dumps({
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "message": message,
            "metadata": metadata,
        })
        log.debug("agent_log", level=level, message=message)
        if self._redis is None:
            return
        await self._redis.lpush(AGENT_LOG_KEY, entry)
        await self._redis.ltrim(AGENT_LOG_KEY, 0, AGENT_LOG_MAX - 1)

    # ── Stuck-loop detection ───────────────────────────────────────────────────

    async def _check_stuck(self, action: ScoredAction, gap: int) -> None:
        """
        Detect when the same action has been blocked by the confidence threshold
        for STUCK_CYCLE_THRESHOLD consecutive cycles (≈ 15 minutes).

        On detection:
        1. Sends a high-priority STUCK alert via Telegram with a Force Run button.
        2. Logs a CRITICAL entry to the agent log.
        3. Writes the stuck state to Redis for the dashboard.
        """
        action_type = action.decision_type.value
        self._stuck_cycles[action_type] = self._stuck_cycles.get(action_type, 0) + 1
        cycles = self._stuck_cycles[action_type]

        # Reset counters for other action types (only track the current top action)
        for k in list(self._stuck_cycles.keys()):
            if k != action_type:
                self._stuck_cycles[k] = 0

        await self._log_entry(
            "warning",
            f"Stuck cycle {cycles}/{STUCK_CYCLE_THRESHOLD} for [{action_type}] "
            f"(conf={action.confidence}, gap={gap})",
            {"action_type": action_type, "cycles": cycles, "gap": gap},
        )

        if cycles < STUCK_CYCLE_THRESHOLD:
            return

        # ── STUCK threshold reached ────────────────────────────────────────────
        self._stuck_cycles[action_type] = 0  # reset after alert

        dashboard_url = "http://localhost:3000/dashboard"
        try:
            from nexus.shared.config import settings as _s
            dashboard_url = _s.telegram_dashboard_url or dashboard_url
        except Exception:
            pass

        stuck_msg = (
            f"🚨 *NEXUS — STUCK LOOP DETECTED*\n\n"
            f"⏱ The agent has been blocked for \\~{STUCK_CYCLE_THRESHOLD * 5} minutes\\.\n\n"
            f"🎯 *Action:* `{action_type}`\n"
            f"📊 *Confidence:* `{action.confidence}` / threshold `{HITL_THRESHOLD}`\n"
            f"📉 *Gap:* Need `\\+{gap}` more confidence points\n\n"
            f"📝 *Reason:* {self._esc(action.reasoning[:200])}\n\n"
            f"🔗 [Open Dashboard]({self._esc(dashboard_url)})\n\n"
            f"_Use the Force Run button below to bypass the confidence check\\._"
        )

        # Write stuck state to Redis for dashboard
        if self._redis:
            stuck_payload = json.dumps({
                "action_type": action_type,
                "confidence": action.confidence,
                "threshold": HITL_THRESHOLD,
                "gap": gap,
                "cycles": STUCK_CYCLE_THRESHOLD,
                "task_type": action.action_task_type,
                "task_params": action.action_params,
                "detected_at": datetime.now(timezone.utc).isoformat(),
            })
            await self._redis.set(STUCK_STATE_KEY, stuck_payload, ex=STUCK_STATE_TTL)

        await self._log_entry(
            "error",
            f"STUCK LOOP: [{action_type}] blocked for {STUCK_CYCLE_THRESHOLD} cycles. "
            f"Confidence={action.confidence}, gap={gap}. Sending alert.",
            {"stuck": True, "action_type": action_type},
        )
        log.error(
            "orchestrator_stuck_loop",
            action_type=action_type,
            confidence=action.confidence,
            gap=gap,
            cycles=STUCK_CYCLE_THRESHOLD,
        )

        # Send Telegram alert with Force Run inline button
        if self._notifier:
            try:
                from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

                from nexus.shared.notifications.providers.telegram import TelegramProvider
                for provider in self._notifier._providers:
                    if isinstance(provider, TelegramProvider) and provider._is_configured():
                        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                            InlineKeyboardButton(
                                text="⚡ Force Run (bypass confidence)",
                                callback_data=f"force_run:{action.action_task_type}",
                            ),
                            InlineKeyboardButton(
                                text="🚫 Dismiss",
                                callback_data="stuck_dismiss",
                            ),
                        ]])
                        await provider._send_raw(text=stuck_msg, reply_markup=keyboard)
                        log.info("stuck_alert_sent_telegram", action_type=action_type)
                        break
            except Exception as exc:
                log.error("stuck_alert_telegram_error", error=str(exc))

    @staticmethod
    def _esc(text: str) -> str:
        """Escape text for MarkdownV2."""
        import re
        return re.sub(r"([_\*\[\]\(\)~`>#+\-=|{}.!\\])", r"\\\1", str(text))

    # ── Auto-thresholding ──────────────────────────────────────────────────────

    async def _get_effective_threshold(self, action_type: str) -> int:
        """
        Return the effective confidence threshold for this action type.
        May be lower than HITL_THRESHOLD if the operator has approved it
        APPROVAL_STREAK_THRESHOLD times in a row.
        """
        if self._redis is None:
            return HITL_THRESHOLD
        try:
            override_key = f"{THRESHOLD_OVERRIDE_PREFIX}{action_type}"
            val = await self._redis.get(override_key)
            if val is not None:
                return max(MIN_THRESHOLD, int(val))
        except Exception:
            pass
        return HITL_THRESHOLD

    async def record_approval(self, action_type: str, approved: bool) -> None:
        """
        Record a human approval/rejection for auto-thresholding.

        Called by the HITL resolve endpoint after an operator decision.
        After APPROVAL_STREAK_THRESHOLD consecutive approvals of the same
        action type, lowers the threshold by THRESHOLD_REDUCTION points.
        """
        if self._redis is None:
            return
        try:
            streak_key = APPROVAL_STREAK_KEY
            if approved:
                # Increment streak counter
                new_streak = await self._redis.hincrby(streak_key, action_type, 1)
                log.info(
                    "approval_streak",
                    action_type=action_type,
                    streak=new_streak,
                    threshold_reduction_at=APPROVAL_STREAK_THRESHOLD,
                )
                if new_streak >= APPROVAL_STREAK_THRESHOLD:
                    # Lower the threshold for this action type
                    override_key = f"{THRESHOLD_OVERRIDE_PREFIX}{action_type}"
                    current = await self._redis.get(override_key)
                    current_threshold = int(current) if current else HITL_THRESHOLD
                    new_threshold = max(MIN_THRESHOLD, current_threshold - THRESHOLD_REDUCTION)
                    await self._redis.set(
                        override_key, str(new_threshold), ex=THRESHOLD_OVERRIDE_TTL
                    )
                    # Reset streak counter
                    await self._redis.hset(streak_key, action_type, 0)
                    log.info(
                        "auto_threshold_lowered",
                        action_type=action_type,
                        old_threshold=current_threshold,
                        new_threshold=new_threshold,
                    )
                    await self._log_entry(
                        "decision",
                        f"Auto-threshold lowered for [{action_type}]: "
                        f"{current_threshold} → {new_threshold} "
                        f"(after {APPROVAL_STREAK_THRESHOLD} consecutive approvals)",
                        {
                            "action_type": action_type,
                            "old_threshold": current_threshold,
                            "new_threshold": new_threshold,
                        },
                    )
            else:
                # Rejection resets the streak
                await self._redis.hset(streak_key, action_type, 0)
        except Exception as exc:
            log.error("approval_streak_error", error=str(exc))


# ── Standalone function (used by API endpoint) ─────────────────────────────────

async def run_decision_engine(
    cluster_workers: int = 0,
    total_active_jobs: int = 0,
    max_jobs_per_worker: int = 4,
) -> list[Any]:
    """
    Run one scoring cycle without a dispatcher (API use only).
    Returns a list of Decision-compatible dicts.
    """
    ctx = await _collect_context(
        cluster_workers=cluster_workers,
        total_active_jobs=total_active_jobs,
        max_jobs_per_worker=max_jobs_per_worker,
    )
    rule_fns = [
        lambda: _score_pause(ctx),
        lambda: _score_emergency_warmup(ctx),
        lambda: _score_scrape(ctx),
        lambda: _score_adder(ctx),
        lambda: _score_idle_worker_opportunity(ctx),
        lambda: _score_scale_workers(ctx),
        lambda: _score_forecast(ctx),
    ]

    actions: list[ScoredAction] = []
    for fn in rule_fns:
        a = fn()
        if a is not None:
            actions.append(a)

    actions.sort(key=lambda a: a.composite, reverse=True)

    # Convert to Decision-compatible dicts for the existing API schema
    results = []
    for a in actions:
        d = a.to_decision_dict()
        results.append(type("Decision", (), {
            "to_dict": lambda self, _d=d: _d,
            **{k: v for k, v in d.items()},
        })())

    return results
