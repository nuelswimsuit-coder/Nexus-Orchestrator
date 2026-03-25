"""
Israeli Ghostwriter Community — master-side scheduler for ``ghostwriter.community_vibe``.

Polls Redis on an interval, optionally merges ``is_israeli`` rows from telefix.db,
and dispatches a vibe job when ``next_run_at`` is due (default: every few hours).
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any

import structlog

from nexus.agents.ghostwriter.community_manager import (
    fetch_israeli_groups,
    merge_israeli_db_rows_into_groups_config,
)
from nexus.master.dispatcher import Dispatcher
from nexus.shared.schemas import TaskPayload

log = structlog.get_logger(__name__)

GROUPS_KEY = "nexus:ghostwriter:israeli:groups"
STATE_PREFIX = "nexus:ghostwriter:israeli:state:"
LOCK_PREFIX = "nexus:ghostwriter:israeli:lock:"


class IsraeliGhostwriterScheduler:
    def __init__(self, dispatcher: Dispatcher, redis: Any) -> None:
        self._dispatcher = dispatcher
        self._redis = redis

    async def run_loop(self, interval_s: float = 60.0) -> None:
        log.info("israeli_ghostwriter_scheduler_started", interval_s=interval_s)
        while True:
            await asyncio.sleep(interval_s)
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("israeli_ghostwriter_tick_error", error=str(exc))

    async def _maybe_sync_db_into_groups(self) -> None:
        if os.getenv("GHOSTWRITER_ISRAELI_SYNC_DB", "1").strip().lower() not in {
            "1",
            "true",
            "yes",
            "on",
        }:
            return
        raw = await self._redis.get(GROUPS_KEY)
        existing: dict[str, Any] = {}
        if raw:
            try:
                existing = json.loads(raw)
            except Exception:
                existing = {}
        if not isinstance(existing, dict):
            existing = {}
        rows = fetch_israeli_groups()
        merged = merge_israeli_db_rows_into_groups_config(existing, rows)
        await self._redis.set(GROUPS_KEY, json.dumps(merged, ensure_ascii=False))

    async def _tick(self) -> None:
        await self._maybe_sync_db_into_groups()

        raw = await self._redis.get(GROUPS_KEY)
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
            uname = str(cfg.get("username", "") or "").strip().lstrip("@")
            if gid is None and not uname:
                log.debug("israeli_ghostwriter_skip_no_entity", group_key=group_key)
                continue
            sessions: list[dict[str, Any]] = list(cfg.get("sessions") or [])
            if not sessions:
                continue

            st_raw = await self._redis.get(f"{STATE_PREFIX}{group_key}")
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

            lock_key = f"{LOCK_PREFIX}{group_key}"
            got = await self._redis.set(lock_key, "1", nx=True, ex=900)
            if not got:
                continue

            sess0 = sessions[0] if sessions else {}
            session_path = str(sess0.get("session_path", "") or "").strip()
            if not session_path:
                await self._redis.delete(lock_key)
                continue

            task = TaskPayload(
                task_type="ghostwriter.community_vibe",
                parameters={
                    "group_key": str(group_key),
                    "group_id": gid,
                    "username": uname,
                    "session_path": session_path,
                    "invite_link": str(cfg.get("invite_link", "") or ""),
                    "group_title": str(cfg.get("group_title", "") or ""),
                    "join_if_needed": bool(cfg.get("join_if_needed", True)),
                },
                project_id="ghostwriter-israeli",
            )
            try:
                job_id = await self._dispatcher.dispatch(task)
                log.info(
                    "ghostwriter_community_vibe_dispatched",
                    group_key=group_key,
                    job_id=job_id,
                )
            except Exception as exc:
                log.error(
                    "ghostwriter_community_vibe_dispatch_failed",
                    group_key=group_key,
                    error=str(exc),
                )
                await self._redis.delete(lock_key)
