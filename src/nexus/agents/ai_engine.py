"""
Nexus AI Engine — OperationalFlow (replaces simulation logic).

Every "AI Thinking" decision emits a REAL task to the Redis/ARQ queue.
A callback_verifier checks telefix.db after each task: if the row count
increases, the task is marked VERIFIED and WRITTEN in Redis.

Design
------
- OperationalFlow.think() analyses the system state and picks the next action.
- Each action maps to a real ARQ task_type (telegram.auto_scrape, etc.).
- Decisions are published to Redis under 'nexus:ai:decisions' for the UI.
- callback_verifier() polls telefix.db row count before/after and writes
  verification status to 'nexus:ai:task_status:<task_id>'.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

log = structlog.get_logger(__name__)

# ── Redis keys ────────────────────────────────────────────────────────────────
AI_DECISIONS_KEY = "nexus:ai:decisions"
AI_TASK_STATUS_PREFIX = "nexus:ai:task_status:"
ARQ_QUEUE_NAME = "nexus:tasks"
HEARTBEAT_KEY_PREFIX = "nexus:heartbeat:"
MAX_DECISIONS = 200

# ── telefix.db path resolution ────────────────────────────────────────────────
_DEFAULT_TELEFIX_PATHS = [
    Path(os.getenv("TELEFIX_DB_PATH", "")).expanduser() if os.getenv("TELEFIX_DB_PATH") else None,
    Path("C:/Users/Yarin/Desktop/Nexus-Orchestrator/telefix.db"),
    Path(__file__).resolve().parents[4] / "telefix.db",
    Path(__file__).resolve().parents[4] / "data" / "telefix.db",
]


def _find_telefix_db() -> Path | None:
    for p in _DEFAULT_TELEFIX_PATHS:
        if p and p.is_file() and p.stat().st_size > 0:
            return p
    return None


# Task-type → telefix.db table mapping for real verification
_TASK_TABLE_MAP: dict[str, str] = {
    "telegram.auto_scrape": "scraped_users",
    "telegram.scrape_group": "scraped_users",
    "telegram.super_scraper": "scraped_users",
    "telegram.group_warmer": "groups",
    "telegram.telegram_adder": "added_users",
    "trading.polymarket_bot_tick": "trades",
    "trading.live_trade_execution": "trades",
}
_DEFAULT_VERIFY_TABLE = "scraped_users"


def _telefix_row_count(table: str = _DEFAULT_VERIFY_TABLE) -> int:
    """Return row count for *table* in telefix.db, or 0 if DB/table unavailable."""
    db_path = _find_telefix_db()
    if db_path is None:
        return 0
    try:
        conn = sqlite3.connect(str(db_path), check_same_thread=False, timeout=5)
        try:
            cur = conn.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
            row = cur.fetchone()
            return int(row[0]) if row else 0
        except sqlite3.OperationalError:
            return 0
        finally:
            conn.close()
    except Exception:
        return 0


# ── Decision model ────────────────────────────────────────────────────────────

class AIDecision:
    __slots__ = ("task_id", "task_type", "project_id", "priority", "params",
                 "reason", "ts", "status")

    def __init__(
        self,
        task_type: str,
        project_id: str = "default",
        priority: int = 8,
        params: dict[str, Any] | None = None,
        reason: str = "",
    ) -> None:
        self.task_id = str(uuid.uuid4())
        self.task_type = task_type
        self.project_id = project_id
        self.priority = priority
        self.params = params or {}
        self.reason = reason
        self.ts = datetime.now(timezone.utc).isoformat()
        self.status = "PENDING"

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "project_id": self.project_id,
            "priority": self.priority,
            "params": self.params,
            "reason": self.reason,
            "ts": self.ts,
            "status": self.status,
        }


# ── OperationalFlow ───────────────────────────────────────────────────────────

class OperationalFlow:
    """
    AI decision engine that emits real tasks instead of simulations.

    Usage (inside an async context with a Redis client):
        flow = OperationalFlow(redis)
        await flow.think()
    """

    def __init__(self, redis: Any, redis_dsn: str = "redis://127.0.0.1:6379/0") -> None:
        self._redis = redis
        self._redis_dsn = redis_dsn

    async def think(self) -> AIDecision | None:
        """
        Analyse system state and decide the next action.
        Emits a real ARQ task and publishes the decision to Redis.
        Returns the AIDecision or None if no action is needed.
        """
        decision = await self._pick_next_action()
        if decision is None:
            return None

        await self._enqueue_task(decision)
        await self._publish_decision(decision)

        log.info(
            "ai_engine_decision",
            task_id=decision.task_id,
            task_type=decision.task_type,
            reason=decision.reason,
        )
        print(
            f"\033[1;36m🤖 [AI-ENGINE] Decision: {decision.task_type} "
            f"(priority={decision.priority}) — {decision.reason}\033[0m",
            flush=True,
        )

        asyncio.create_task(
            self._callback_verifier(decision),
            name=f"ai-verify-{decision.task_id[:8]}",
        )
        return decision

    async def _pick_next_action(self) -> AIDecision | None:
        """
        Heuristic decision tree:
        1. If telefix.db is empty/missing → trigger auto_scrape (high priority).
        2. If idle workers detected → dispatch auto_scrape to keep CPU busy.
        3. If no open Polymarket position → trigger polymarket_bot_tick.
        4. Default: auto_scrape with high priority.
        """
        row_count = _telefix_row_count(_DEFAULT_VERIFY_TABLE)

        # Priority 1: DB is empty — immediate scrape
        if row_count == 0:
            return AIDecision(
                task_type="telegram.auto_scrape",
                priority=10,
                params={"triggered_by": "ai_engine", "reason": "telefix_db_empty"},
                reason="telefix.db is empty — InitialScrape required",
            )

        # Priority 2: Check for idle workers
        idle_nodes = await self._get_idle_nodes()
        if idle_nodes:
            return AIDecision(
                task_type="telegram.auto_scrape",
                priority=9,
                params={"triggered_by": "ai_engine", "idle_nodes": idle_nodes},
                reason=f"Idle workers detected: {idle_nodes[:3]}",
            )

        # Priority 3: Check Polymarket open position
        open_pos = await self._redis.get("nexus:poly:open_position")
        if open_pos is None:
            return AIDecision(
                task_type="trading.polymarket_bot_tick",
                priority=8,
                params={"triggered_by": "ai_engine", "max_bet_usd": 5.0},
                reason="No open Polymarket position — scanning for opportunity",
            )

        # Default: keep scraping
        return AIDecision(
            task_type="telegram.auto_scrape",
            priority=8,
            params={"triggered_by": "ai_engine"},
            reason=f"Routine scrape cycle (db_rows={row_count})",
        )

    async def _get_idle_nodes(self) -> list[str]:
        idle: list[str] = []
        try:
            cursor = 0
            while True:
                cursor, keys = await self._redis.scan(
                    cursor=cursor, match=f"{HEARTBEAT_KEY_PREFIX}*", count=200
                )
                for key in keys:
                    raw = await self._redis.get(key)
                    if raw is None:
                        continue
                    try:
                        hb = json.loads(raw)
                    except Exception:
                        continue
                    cpu = float(hb.get("cpu_percent", 100))
                    if cpu < 10.0:
                        idle.append(hb.get("node_id", str(key)))
                if cursor == 0:
                    break
        except Exception:
            pass
        return idle

    async def _enqueue_task(self, decision: AIDecision) -> None:
        """Push the decision as a real ARQ job onto the queue."""
        try:
            from arq import create_pool  # type: ignore[import]
            from arq.connections import RedisSettings  # type: ignore[import]
            from nexus.shared.constants import TASK_DEFAULT_TIMEOUT  # noqa: PLC0415
            from nexus.shared.schemas import TaskPayload  # noqa: PLC0415

            payload = TaskPayload(
                task_type=decision.task_type,
                parameters=decision.params,
                priority=decision.priority,
                project_id=decision.project_id,
            )
            # Override task_id so verifier can track it
            payload.task_id = decision.task_id

            pool = await create_pool(
                RedisSettings.from_dsn(self._redis_dsn),
                default_queue_name=ARQ_QUEUE_NAME,
            )
            job = await pool.enqueue_job(
                "execute_task",
                task_payload=payload.model_dump_for_wire(),
                _job_id=decision.task_id,
                _queue_name=ARQ_QUEUE_NAME,
                _expires=TASK_DEFAULT_TIMEOUT,
            )
            await pool.aclose()

            decision.status = "QUEUED"
            jid = job.job_id if job else decision.task_id
            print(
                f"\033[1;32m🚀 [AI-ENGINE] Task enqueued: {decision.task_type} "
                f"job_id={jid}\033[0m",
                flush=True,
            )
        except Exception as exc:
            decision.status = "ENQUEUE_FAILED"
            log.error("ai_engine_enqueue_failed", error=str(exc), task_type=decision.task_type)
            print(f"\033[1;31m[AI-ENGINE] Enqueue failed: {exc}\033[0m", flush=True)

    async def _publish_decision(self, decision: AIDecision) -> None:
        """Write decision to Redis list for UI consumption."""
        try:
            payload = json.dumps(decision.to_dict(), ensure_ascii=False)
            await self._redis.rpush(AI_DECISIONS_KEY, payload)
            await self._redis.ltrim(AI_DECISIONS_KEY, -MAX_DECISIONS, -1)
            await self._redis.set(
                f"{AI_TASK_STATUS_PREFIX}{decision.task_id}",
                json.dumps({"status": decision.status, "ts": decision.ts}),
                ex=3600,
            )
        except Exception as exc:
            log.warning("ai_engine_publish_failed", error=str(exc))

    async def _callback_verifier(self, decision: AIDecision) -> None:
        """
        After a task is enqueued, poll the relevant telefix.db table row count.
        If new_count > old_count within 120 s → verified=1, written=1 in the
        UI notification payload.  No mock/simulation paths exist here.
        """
        verify_table = _TASK_TABLE_MAP.get(decision.task_type, _DEFAULT_VERIFY_TABLE)
        rows_before = _telefix_row_count(verify_table)
        deadline = time.monotonic() + 120.0
        verified = False

        while time.monotonic() < deadline:
            await asyncio.sleep(10)
            rows_after = _telefix_row_count(verify_table)
            if rows_after > rows_before:
                verified = True
                break

        rows_after = _telefix_row_count(verify_table)
        delta = rows_after - rows_before
        status = "VERIFIED_WRITTEN" if verified else "UNVERIFIED"

        notification: dict[str, Any] = {
            "status": status,
            "task_type": decision.task_type,
            "verify_table": verify_table,
            "rows_before": rows_before,
            "rows_after": rows_after,
            "delta": delta,
            # UI flags — consumed by the dashboard notification renderer
            "verified": 1 if verified else 0,
            "written": 1 if verified else 0,
            "ts": datetime.now(timezone.utc).isoformat(),
        }

        try:
            await self._redis.set(
                f"{AI_TASK_STATUS_PREFIX}{decision.task_id}",
                json.dumps(notification, ensure_ascii=False),
                ex=3600,
            )
            # Also push to the decisions list so the UI toast picks it up
            await self._redis.rpush(
                AI_DECISIONS_KEY,
                json.dumps({**decision.to_dict(), **notification}, ensure_ascii=False),
            )
            await self._redis.ltrim(AI_DECISIONS_KEY, -MAX_DECISIONS, -1)
        except Exception:
            pass

        color = "\033[1;32m" if verified else "\033[1;33m"
        print(
            f"{color}[AI-VERIFIER] task_id={decision.task_id[:8]} "
            f"table={verify_table} status={status} "
            f"rows_before={rows_before} rows_after={rows_after} delta={delta} "
            f"verified={notification['verified']} written={notification['written']}\033[0m",
            flush=True,
        )
        log.info(
            "ai_engine_verified",
            task_id=decision.task_id,
            task_type=decision.task_type,
            verify_table=verify_table,
            status=status,
            rows_before=rows_before,
            rows_after=rows_after,
            delta=delta,
            verified=notification["verified"],
            written=notification["written"],
        )


# ── Standalone loop (used by nexus_core or as a standalone service) ───────────

async def run_operational_flow_loop(
    redis_dsn: str = "redis://127.0.0.1:6379/0",
    interval_s: int = 60,
) -> None:
    """
    Continuously run OperationalFlow.think() every `interval_s` seconds.
    Designed to run as a background asyncio task or standalone process.
    """
    import redis.asyncio as aioredis  # type: ignore[import-not-found]

    print(
        "\033[1;35m"
        "╔══════════════════════════════════════════════════════╗\n"
        "║  🤖 [AI-ENGINE] OperationalFlow ACTIVE               ║\n"
        "║     Simulation replaced with real task emission.     ║\n"
        "╚══════════════════════════════════════════════════════╝"
        "\033[0m",
        flush=True,
    )

    _RECONNECT_DELAY = 5.0
    while True:
        client = None
        try:
            client = aioredis.from_url(redis_dsn, decode_responses=True)
            flow = OperationalFlow(client, redis_dsn)

            while True:
                try:
                    await flow.think()
                except Exception as exc:
                    log.warning("ai_engine_think_error", error=str(exc))
                    print(f"⚠️  [AI-ENGINE] think() error: {exc}", flush=True)
                await asyncio.sleep(interval_s)

        except asyncio.CancelledError:
            return
        except Exception as exc:
            print(
                f"⚠️  [AI-ENGINE] Redis disconnected: {exc} — "
                f"reconnecting in {_RECONNECT_DELAY}s…",
                flush=True,
            )
        finally:
            if client is not None:
                try:
                    await client.aclose()
                except Exception:
                    pass
        await asyncio.sleep(_RECONNECT_DELAY)


def _run_ai_engine_process(redis_dsn: str, interval_s: int = 60) -> None:
    """Top-level target for multiprocessing.Process (must be picklable on Windows)."""
    asyncio.run(run_operational_flow_loop(redis_dsn, interval_s))


if __name__ == "__main__":
    _dsn = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    asyncio.run(run_operational_flow_loop(_dsn))
