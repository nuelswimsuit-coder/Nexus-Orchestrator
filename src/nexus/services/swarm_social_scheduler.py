"""
Swarm Social Synthesis — master-side scheduler for ``swarm.group_warmer``.

Polls Redis every minute, dispatches a tick when ``next_run_at`` is due (or
missing on first run). Uses a short-lived Redis lock to avoid duplicate jobs.
Optional one-time seed from ``SWARM_WARMER_CONFIG`` (path to JSON file).
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any

import structlog

from nexus.core.dispatcher import Dispatcher
from nexus.shared.schemas import TaskPayload

log = structlog.get_logger(__name__)

SWARM_GROUPS_KEY = "nexus:swarm:warmer:groups"
SWARM_LOCK_PREFIX = "nexus:swarm:warmer:lock:"
SWARM_STATE_PREFIX = "nexus:swarm:warmer:state:"


class SwarmSocialScheduler:
    def __init__(self, dispatcher: Dispatcher, redis: Any) -> None:
        self._dispatcher = dispatcher
        self._redis = redis
        self._seed_attempted = False

    async def maybe_seed_from_config_file(self) -> None:
        path = os.getenv("SWARM_WARMER_CONFIG", "").strip()
        if not path or not os.path.isfile(path):
            return
        try:
            exists = await self._redis.get(SWARM_GROUPS_KEY)
            if exists:
                return
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                await self._redis.set(SWARM_GROUPS_KEY, json.dumps(data, ensure_ascii=False))
                log.info("swarm_warmer_seeded_from_file", path=path, groups=len(data))
        except Exception as exc:
            log.warning("swarm_warmer_seed_failed", path=path, error=str(exc))

    async def run_loop(self, interval_s: float = 60.0) -> None:
        log.info("swarm_social_scheduler_started", interval_s=interval_s)
        while True:
            await asyncio.sleep(interval_s)
            try:
                if not self._seed_attempted:
                    self._seed_attempted = True
                    await self.maybe_seed_from_config_file()
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("swarm_social_tick_error", error=str(exc))

    async def _tick(self) -> None:
        raw = await self._redis.get(SWARM_GROUPS_KEY)
        if not raw:
            return
        try:
            groups = json.loads(raw)
        except Exception:
            return
        if not isinstance(groups, dict) or not groups:
            return

        now = datetime.now(timezone.utc)
        for group_key, cfg in groups.items():
            if not isinstance(cfg, dict):
                continue
            if not cfg.get("enabled", True):
                continue
            gid = cfg.get("group_id")
            if gid is None:
                continue
            sessions = cfg.get("sessions") or []
            if not sessions:
                continue

            st_raw = await self._redis.get(f"{SWARM_STATE_PREFIX}{group_key}")
            st: dict[str, Any] = {}
            if st_raw:
                try:
                    st = json.loads(st_raw)
                except Exception:
                    st = {}
            nrun_s = st.get("next_run_at")
            if nrun_s:
                try:
                    nrun = datetime.fromisoformat(str(nrun_s).replace("Z", "+00:00"))
                    if nrun.tzinfo is None:
                        nrun = nrun.replace(tzinfo=timezone.utc)
                    if now < nrun:
                        continue
                except Exception:
                    pass

            lock_key = f"{SWARM_LOCK_PREFIX}{group_key}"
            got = await self._redis.set(lock_key, "1", nx=True, ex=900)
            if not got:
                continue

            task = TaskPayload(
                task_type="swarm.group_warmer",
                parameters={
                    "group_key": str(group_key),
                    "group_id": int(gid),
                    "sessions": sessions,
                    "timezone": str(cfg.get("timezone", "UTC") or "UTC"),
                    "action": "tick",
                    "group_title": str(cfg.get("group_title", "") or ""),
                    "engagement_mode": str(cfg.get("engagement_mode", "") or ""),
                },
                project_id="swarm-social",
            )
            try:
                job_id = await self._dispatcher.dispatch(task)
                log.info("swarm_group_warmer_dispatched", group_key=group_key, job_id=job_id)
            except Exception as exc:
                log.error("swarm_group_warmer_dispatch_failed", group_key=group_key, error=str(exc))
                await self._redis.delete(lock_key)
