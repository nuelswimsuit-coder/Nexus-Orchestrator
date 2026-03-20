"""
Watchdog — Process Health Monitor.

Monitors the Master and Worker processes and restarts them if they:
  - Exceed the configured memory limit (default 1 GB RSS)
  - Hang for more than HANG_TIMEOUT_S seconds (no heartbeat in Redis)
  - Exit unexpectedly

The Watchdog runs as a separate asyncio task inside the Master process.
It checks process health every CHECK_INTERVAL_S seconds.

Usage
-----
    watchdog = Watchdog(redis=redis)
    watchdog.register("master", pid=os.getpid(), restart_cmd=None)
    watchdog.register("worker", pid=worker_pid, restart_cmd=["python", "scripts/start_worker.py"])
    asyncio.create_task(watchdog.run())

Redis heartbeat keys
--------------------
Each process writes its heartbeat to:
    nexus:heartbeat:<node_id>   (with a TTL)

The Watchdog reads these keys to detect hangs.  A missing key after
HANG_TIMEOUT_S seconds triggers a restart.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import time
from dataclasses import dataclass, field
from typing import Any

import psutil
import structlog

log = structlog.get_logger(__name__)

CHECK_INTERVAL_S   = int(os.getenv("WATCHDOG_CHECK_INTERVAL", "10"))  # self-heal: 10 s default
HANG_TIMEOUT_S     = int(os.getenv("WATCHDOG_HANG_TIMEOUT", "60"))
MAX_MEMORY_MB      = float(os.getenv("WATCHDOG_MAX_MEMORY_MB", "1024"))
MAX_RESTARTS       = int(os.getenv("WATCHDOG_MAX_RESTARTS", "5"))
RESTART_COOLDOWN_S = int(os.getenv("WATCHDOG_RESTART_COOLDOWN", "10"))

WATCHDOG_STATUS_KEY = "nexus:watchdog:status"
WATCHDOG_STATUS_TTL = 120


@dataclass
class WatchedProcess:
    name: str
    node_id: str
    pid: int | None = None
    restart_cmd: list[str] | None = None
    restart_count: int = 0
    last_restart_ts: float = 0.0
    _proc: Any = field(default=None, repr=False)

    def is_alive(self) -> bool:
        if self.pid is None:
            return False
        try:
            proc = psutil.Process(self.pid)
            return proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
        except psutil.NoSuchProcess:
            return False

    def memory_mb(self) -> float:
        if self.pid is None:
            return 0.0
        try:
            return psutil.Process(self.pid).memory_info().rss / (1024 * 1024)
        except psutil.NoSuchProcess:
            return 0.0


class Watchdog:
    """
    Monitors registered processes and restarts them on failure.

    Checks every CHECK_INTERVAL_S seconds:
    1. Process liveness (psutil)
    2. Memory usage (RSS > MAX_MEMORY_MB → restart)
    3. Heartbeat freshness (Redis key TTL expired → hang detected → restart)
    """

    def __init__(self, redis: Any = None) -> None:
        self._redis = redis
        self._processes: dict[str, WatchedProcess] = {}
        self._running = False

    def register(
        self,
        name: str,
        node_id: str,
        pid: int | None = None,
        restart_cmd: list[str] | None = None,
    ) -> None:
        """Register a process to watch."""
        self._processes[name] = WatchedProcess(
            name=name,
            node_id=node_id,
            pid=pid or os.getpid(),
            restart_cmd=restart_cmd,
        )
        log.info(
            "watchdog_registered",
            name=name,
            node_id=node_id,
            pid=pid,
            max_memory_mb=MAX_MEMORY_MB,
            hang_timeout_s=HANG_TIMEOUT_S,
        )

    def update_pid(self, name: str, pid: int) -> None:
        """Update the PID for a watched process (after restart)."""
        if name in self._processes:
            self._processes[name].pid = pid

    async def run(self) -> None:
        """Background loop — checks all registered processes."""
        self._running = True
        log.info(
            "watchdog_started",
            processes=list(self._processes.keys()),
            check_interval_s=CHECK_INTERVAL_S,
        )

        while self._running:
            await asyncio.sleep(CHECK_INTERVAL_S)
            for name, proc in list(self._processes.items()):
                await self._check(proc)

    def stop(self) -> None:
        self._running = False

    async def _check(self, proc: WatchedProcess) -> None:
        """Run all health checks for one process."""
        issues: list[str] = []

        # ── 1. Liveness check ──────────────────────────────────────────────────
        if not proc.is_alive():
            issues.append(f"process {proc.pid} is not running")

        # ── 2. Memory check ────────────────────────────────────────────────────
        mem_mb = proc.memory_mb()
        if mem_mb > MAX_MEMORY_MB:
            issues.append(f"memory {mem_mb:.0f} MB > limit {MAX_MEMORY_MB:.0f} MB")

        # ── 3. Heartbeat / hang check ──────────────────────────────────────────
        if self._redis and proc.node_id:
            heartbeat_key = f"nexus:heartbeat:{proc.node_id}"
            try:
                raw = await self._redis.get(heartbeat_key)
                if raw is None:
                    issues.append(
                        f"heartbeat missing for {proc.node_id} "
                        f"(hang timeout: {HANG_TIMEOUT_S}s)"
                    )
            except Exception:
                pass  # Redis unavailable — skip heartbeat check

        if not issues:
            log.debug(
                "watchdog_healthy",
                name=proc.name,
                pid=proc.pid,
                memory_mb=round(mem_mb, 1),
                status=f"[SUCCESS] {proc.name} healthy (PID {proc.pid}, {mem_mb:.0f} MB RSS)",
            )
            return

        # ── Issues detected — attempt self-healing restart ─────────────────────
        for issue in issues:
            log.warning(
                "watchdog_issue_detected",
                name=proc.name,
                issue=issue,
                status=f"ERROR: {proc.name} unhealthy — {issue}. ACTION: Restarting {proc.name}...",
            )

        await self._restart(proc, reason="; ".join(issues))

    async def _restart(self, proc: WatchedProcess, reason: str) -> None:
        """Attempt to restart a failed process."""
        if proc.restart_count >= MAX_RESTARTS:
            log.error(
                "watchdog_max_restarts_reached",
                name=proc.name,
                max_restarts=MAX_RESTARTS,
                reason=reason,
                status=(
                    f"[CRITICAL] {proc.name} exceeded max restarts ({MAX_RESTARTS}). "
                    "ACTION: Manual intervention required."
                ),
            )
            await self._write_status(proc.name, "max_restarts_reached", reason)
            return

        cooldown_remaining = RESTART_COOLDOWN_S - (time.time() - proc.last_restart_ts)
        if cooldown_remaining > 0:
            log.debug(
                "watchdog_restart_cooldown",
                name=proc.name,
                wait_s=round(cooldown_remaining, 1),
                status=f"[REPAIRING] {proc.name} restart cooldown: {cooldown_remaining:.0f}s remaining...",
            )
            await asyncio.sleep(cooldown_remaining)

        if proc.restart_cmd is None:
            log.warning(
                "watchdog_no_restart_cmd",
                name=proc.name,
                hint="Register with restart_cmd to enable auto-restart",
                status=f"[CRITICAL] {proc.name} has no restart_cmd — cannot auto-restart",
            )
            await self._write_status(proc.name, "failed_no_restart_cmd", reason)
            return

        log.warning(
            "watchdog_restarting",
            name=proc.name,
            attempt=proc.restart_count + 1,
            max_restarts=MAX_RESTARTS,
            reason=reason,
            status=(
                f"[REPAIRING] Restarting {proc.name} "
                f"(attempt {proc.restart_count + 1}/{MAX_RESTARTS}): {reason}"
            ),
        )

        try:
            new_proc = subprocess.Popen(
                proc.restart_cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            proc.pid = new_proc.pid
            proc.restart_count += 1
            proc.last_restart_ts = time.time()

            log.info(
                "watchdog_restarted",
                name=proc.name,
                new_pid=new_proc.pid,
                restart_count=proc.restart_count,
                status=(
                    f"[SUCCESS] {proc.name} restarted (PID {new_proc.pid}, "
                    f"restart #{proc.restart_count})"
                ),
            )
            await self._write_status(proc.name, "restarted", reason)

        except Exception as exc:
            log.error(
                "watchdog_restart_failed",
                name=proc.name,
                error=str(exc),
                status=f"[CRITICAL] {proc.name} restart FAILED: {exc}",
            )
            await self._write_status(proc.name, "restart_failed", str(exc))

    async def _write_status(self, name: str, status: str, detail: str) -> None:
        if self._redis is None:
            return
        import json
        from datetime import datetime, timezone
        payload = json.dumps({
            "process": name,
            "status": status,
            "detail": detail,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        await self._redis.set(WATCHDOG_STATUS_KEY, payload, ex=WATCHDOG_STATUS_TTL)
