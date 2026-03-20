from __future__ import annotations

import functools
import math
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, TypeVar

import psutil
import structlog

log = structlog.get_logger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


@dataclass(slots=True)
class NodeResourceConfig:
    cpu_limit: float = 1.0
    ram_limit: float = 1.0
    gpu_limit: float = 1.0
    role: str = "worker"


def _clamp_fraction(value: Any, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    return max(0.05, min(1.0, parsed))


def load_node_config(config_path: Path | None = None) -> NodeResourceConfig:
    """
    Load per-node limits from node_config.json.

    Defaults to the repository root node_config.json when no path is provided.
    """
    import json

    if config_path is None:
        config_path = Path(__file__).resolve().parents[2] / "node_config.json"

    data: dict[str, Any] = {}
    if config_path.exists():
        try:
            raw = json.loads(config_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                data = raw
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("node_config_parse_failed", path=str(config_path), error=str(exc))

    cfg = NodeResourceConfig(
        cpu_limit=_clamp_fraction(data.get("cpu_limit"), 1.0),
        ram_limit=_clamp_fraction(data.get("ram_limit"), 1.0),
        gpu_limit=_clamp_fraction(data.get("gpu_limit"), 1.0),
        role=str(data.get("role", "worker")),
    )
    log.info(
        "node_config_loaded",
        path=str(config_path),
        role=cfg.role,
        cpu_limit=cfg.cpu_limit,
        ram_limit=cfg.ram_limit,
        gpu_limit=cfg.gpu_limit,
    )
    return cfg


class GlobalResourceManager:
    """
    Enforces CPU affinity, RAM soft cap, and GPU memory fraction.
    """

    def __init__(
        self,
        cpu_limit: float,
        ram_limit: float,
        gpu_limit: float,
        check_interval_s: float = 2.0,
    ) -> None:
        self.cpu_limit = _clamp_fraction(cpu_limit, 1.0)
        self.ram_limit = _clamp_fraction(ram_limit, 1.0)
        self.gpu_limit = _clamp_fraction(gpu_limit, 1.0)
        self.check_interval_s = max(0.5, float(check_interval_s))
        self._root_process = psutil.Process(os.getpid())
        self._stop_event = threading.Event()
        self._monitor_thread: threading.Thread | None = None

    def start(self) -> None:
        self._enforce_once()
        if self._monitor_thread and self._monitor_thread.is_alive():
            return
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="global-resource-manager",
            daemon=True,
        )
        self._monitor_thread.start()
        log.info(
            "global_resource_manager_started",
            cpu_limit=self.cpu_limit,
            ram_limit=self.ram_limit,
            gpu_limit=self.gpu_limit,
            check_interval_s=self.check_interval_s,
        )

    def stop(self) -> None:
        self._stop_event.set()
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=2.0)

    def _monitor_loop(self) -> None:
        while not self._stop_event.is_set():
            self._enforce_once()
            self._stop_event.wait(self.check_interval_s)

    def _enforce_once(self) -> None:
        processes = self._get_process_tree()
        if not processes:
            return
        self._apply_cpu_affinity(processes)
        self._apply_gpu_fraction()
        self._apply_ram_soft_cap(processes)

    def _get_process_tree(self) -> list[psutil.Process]:
        try:
            children = self._root_process.children(recursive=True)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            children = []
        return [self._root_process, *children]

    def _apply_cpu_affinity(self, processes: list[psutil.Process]) -> None:
        total_cores = psutil.cpu_count(logical=True) or 1
        allowed = max(1, math.ceil(total_cores * self.cpu_limit))
        allowed_cores = list(range(allowed))
        for proc in processes:
            try:
                proc.cpu_affinity(allowed_cores)
            except (AttributeError, psutil.AccessDenied, psutil.NoSuchProcess):
                continue

    def _apply_gpu_fraction(self) -> None:
        try:
            import torch  # noqa: PLC0415
        except Exception:
            return
        try:
            if torch.cuda.is_available():
                for idx in range(torch.cuda.device_count()):
                    torch.cuda.set_per_process_memory_fraction(self.gpu_limit, device=idx)
        except Exception as exc:
            log.debug("gpu_fraction_apply_failed", error=str(exc))

    def _apply_ram_soft_cap(self, processes: list[psutil.Process]) -> None:
        total_ram = psutil.virtual_memory().total
        limit_bytes = int(total_ram * self.ram_limit)
        rss_bytes = 0
        for proc in processes:
            try:
                rss_bytes += int(proc.memory_info().rss)
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                continue

        if rss_bytes <= limit_bytes:
            return

        # Soft throttle: briefly suspend children to avoid runaway pressure.
        over_ratio = (rss_bytes - limit_bytes) / max(limit_bytes, 1)
        sleep_s = min(1.5, max(0.05, over_ratio * 0.5))
        suspended: list[psutil.Process] = []
        for proc in processes[1:]:
            try:
                proc.suspend()
                suspended.append(proc)
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                continue
        time.sleep(sleep_s)
        for proc in suspended:
            try:
                proc.resume()
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                continue

        log.warning(
            "ram_soft_cap_exceeded",
            rss_mb=round(rss_bytes / (1024 * 1024), 1),
            limit_mb=round(limit_bytes / (1024 * 1024), 1),
            throttle_s=round(sleep_s, 3),
        )


def with_process_limits(
    config: NodeResourceConfig | None = None,
    *,
    config_path: Path | None = None,
) -> Callable[[F], F]:
    """
    Decorator to run a function under node resource limits.
    """

    def _decorator(func: F) -> F:
        @functools.wraps(func)
        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            cfg = config or load_node_config(config_path=config_path)
            manager = GlobalResourceManager(
                cpu_limit=cfg.cpu_limit,
                ram_limit=cfg.ram_limit,
                gpu_limit=cfg.gpu_limit,
            )
            manager.start()
            try:
                return func(*args, **kwargs)
            finally:
                manager.stop()

        return _wrapped  # type: ignore[return-value]

    return _decorator
