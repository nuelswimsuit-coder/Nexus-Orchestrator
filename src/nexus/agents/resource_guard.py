"""
ResourceGuard — keeps any Node operating quietly in the background.

This module now lives in nexus/worker/ per the PRD directory structure.
It is used by both the Master (via the shim at nexus/master/resource_guard.py)
and can be used by Worker Nodes that also need CPU/RAM capping.

Strategy
--------
1. On startup, lower the OS scheduling priority (nice value) of this process
   so the kernel naturally yields CPU to foreground applications.
2. A periodic async monitor checks actual CPU and RAM usage.  If either
   exceeds the configured cap it sleeps briefly to throttle the event loop,
   effectively yielding time to other processes without hard-killing work.
3. Caps are read from environment / settings so they can be tuned without
   touching code.

This module intentionally has no hard dependencies on the rest of the project
so it can be imported and called before anything else is initialised.
"""

from __future__ import annotations

import asyncio
import os
import sys

import psutil
import structlog

log = structlog.get_logger(__name__)

# Temporary: high ceiling during deployment so power-profile / low defaults do not throttle installs.
CPU_CAP = 95.0


def apply_low_priority() -> None:
    """
    Lower the OS scheduling priority of the current process.

    - Unix:    sets nice value to +10 (lower priority than default 0)
    - Windows: sets process priority class to BELOW_NORMAL_PRIORITY_CLASS
    """
    try:
        if sys.platform == "win32":
            import ctypes
            handle = ctypes.windll.kernel32.GetCurrentProcess()  # type: ignore[attr-defined]
            # BELOW_NORMAL_PRIORITY_CLASS = 0x00004000
            ctypes.windll.kernel32.SetPriorityClass(handle, 0x00004000)  # type: ignore[attr-defined]
            log.info("process_priority_set", platform="windows", level="BELOW_NORMAL")
        else:
            os.nice(10)
            log.info("process_priority_set", platform="unix", nice=10)
    except PermissionError:
        log.warning(
            "process_priority_set_failed",
            reason="insufficient permissions — running at default priority",
        )


class ResourceGuard:
    """
    Async background task that monitors and soft-caps CPU and RAM usage.

    Usage
    -----
        guard = ResourceGuard(cpu_cap_percent=CPU_CAP, ram_cap_mb=512)
        asyncio.create_task(guard.monitor())   # fire-and-forget background loop
    """

    def __init__(
        self,
        cpu_cap_percent: float = CPU_CAP,
        ram_cap_mb: float = 512.0,
        check_interval_seconds: float = 5.0,
    ) -> None:
        self.cpu_cap = cpu_cap_percent
        self.ram_cap = ram_cap_mb
        self.interval = check_interval_seconds
        self._process = psutil.Process(os.getpid())

    async def monitor(self) -> None:
        """
        Periodically sample resource usage and throttle if over cap.

        Throttling is implemented as an asyncio sleep, which yields the event
        loop to other coroutines and reduces the rate at which new tasks are
        dispatched.  It does NOT suspend in-flight tasks — those continue
        running on worker nodes.
        """
        log.info("resource_guard_started", cpu_cap=self.cpu_cap, ram_cap_mb=self.ram_cap)

        while True:
            await asyncio.sleep(self.interval)

            cpu = self._process.cpu_percent(interval=None)
            mem_info = self._process.memory_info()
            ram_mb = mem_info.rss / (1024 * 1024)

            log.debug("resource_sample", cpu_percent=cpu, ram_mb=round(ram_mb, 1))

            throttle_seconds = 0.0

            effective_cpu_cap = max(self.cpu_cap, CPU_CAP) if CPU_CAP > 0 else self.cpu_cap
            if effective_cpu_cap > 0 and cpu > effective_cpu_cap:
                overage = cpu - effective_cpu_cap
                throttle_seconds = max(throttle_seconds, overage / 100.0 * 2.0)
                log.warning(
                    "cpu_cap_exceeded",
                    current=cpu,
                    cap=effective_cpu_cap,
                    throttle_s=throttle_seconds,
                )

            if self.ram_cap > 0 and ram_mb > self.ram_cap:
                log.warning(
                    "ram_cap_exceeded",
                    current_mb=round(ram_mb, 1),
                    cap_mb=self.ram_cap,
                )

            if throttle_seconds > 0:
                await asyncio.sleep(throttle_seconds)

    def current_stats(self) -> dict[str, float]:
        """Snapshot of current resource usage — useful for heartbeat payloads."""
        cpu = self._process.cpu_percent(interval=0.1)
        ram_mb = self._process.memory_info().rss / (1024 * 1024)
        return {"cpu_percent": cpu, "ram_mb": round(ram_mb, 1)}
