"""
RED Metrics — pure Redis-backed, no external Prometheus library required.

Keys
----
    nexus:metrics:counter:<name>:<labels_hash>   → INCR integer
    nexus:metrics:gauge:<name>:<labels_hash>      → SET float string
    nexus:metrics:histogram:<name>:<labels_hash>  → LIST of float strings (capped 1 000)

Prometheus export
-----------------
    GET /metrics  →  text/plain; version=0.0.4

Usage
-----
    from nexus.shared.metrics import metrics

    await metrics.record_request("/api/tasks", "POST", 200, 42.5)
    await metrics.record_task("process_url", "worker-1", "ok", 120.0)
    snapshot = await metrics.get_dashboard_snapshot()
"""

from __future__ import annotations

import hashlib
import json
import time
from enum import Enum
from typing import Any

import structlog

log = structlog.get_logger(__name__)

_HISTOGRAM_MAX_LEN = 1000
_COUNTER_WINDOW_KEY = "nexus:metrics:counter:requests_window"


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

class MetricType(str, Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"


def _labels_hash(labels: dict[str, str]) -> str:
    if not labels:
        return "default"
    canonical = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
    return hashlib.md5(canonical.encode()).hexdigest()[:8]  # noqa: S324


def _metric_key(mtype: MetricType, name: str, labels: dict[str, str] | None = None) -> str:
    lhash = _labels_hash(labels or {})
    return f"nexus:metrics:{mtype.value}:{name}:{lhash}"


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------

class MetricsCollector:
    """Thread-safe, async metrics collector backed by Redis."""

    def __init__(self) -> None:
        self._redis: Any | None = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_redis(self) -> Any:
        if self._redis is None:
            import redis.asyncio as aioredis  # noqa: PLC0415
            from nexus.shared.config import settings  # noqa: PLC0415

            self._redis = aioredis.from_url(
                settings.redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
        return self._redis

    async def _incr(self, key: str, amount: int = 1) -> None:
        try:
            r = await self._get_redis()
            await r.incrby(key, amount)
        except Exception as exc:  # pylint: disable=broad-except
            log.warning("metrics_incr_failed", key=key, error=str(exc))

    async def _set(self, key: str, value: float) -> None:
        try:
            r = await self._get_redis()
            await r.set(key, str(value))
        except Exception as exc:  # pylint: disable=broad-except
            log.warning("metrics_set_failed", key=key, error=str(exc))

    async def _push_histogram(self, key: str, value: float) -> None:
        try:
            r = await self._get_redis()
            pipe = r.pipeline(transaction=False)
            pipe.rpush(key, str(value))
            pipe.ltrim(key, -_HISTOGRAM_MAX_LEN, -1)
            await pipe.execute()
        except Exception as exc:  # pylint: disable=broad-except
            log.warning("metrics_histogram_failed", key=key, error=str(exc))

    async def _get_counter(self, key: str) -> int:
        try:
            r = await self._get_redis()
            val = await r.get(key)
            return int(val) if val else 0
        except Exception:  # pylint: disable=broad-except
            return 0

    async def _get_gauge(self, key: str) -> float:
        try:
            r = await self._get_redis()
            val = await r.get(key)
            return float(val) if val else 0.0
        except Exception:  # pylint: disable=broad-except
            return 0.0

    async def _get_histogram_values(self, key: str) -> list[float]:
        try:
            r = await self._get_redis()
            vals = await r.lrange(key, 0, -1)
            return [float(v) for v in vals]
        except Exception:  # pylint: disable=broad-except
            return []

    # ------------------------------------------------------------------
    # RED Metrics — Requests
    # ------------------------------------------------------------------

    async def record_request(
        self,
        route: str,
        method: str,
        status: int,
        duration_ms: float,
    ) -> None:
        labels = {"route": route, "method": method, "status": str(status)}
        await self._incr(_metric_key(MetricType.COUNTER, "requests_total", labels))
        if status >= 500:
            err_labels = {"route": route, "method": method}
            await self._incr(
                _metric_key(MetricType.COUNTER, "requests_errors_total", err_labels)
            )
        await self._push_histogram(
            _metric_key(MetricType.HISTOGRAM, "request_duration_ms", {"route": route}),
            duration_ms,
        )
        # Also keep a sliding window counter (score = unix timestamp)
        try:
            r = await self._get_redis()
            now = time.time()
            pipe = r.pipeline(transaction=False)
            pipe.zadd(_COUNTER_WINDOW_KEY, {f"{now}:{route}": now})
            pipe.zremrangebyscore(_COUNTER_WINDOW_KEY, 0, now - 300)
            await pipe.execute()
        except Exception:  # pylint: disable=broad-except
            pass

    # ------------------------------------------------------------------
    # RED Metrics — Tasks
    # ------------------------------------------------------------------

    async def record_task(
        self,
        task_type: str,
        worker_id: str,
        status: str,
        duration_ms: float,
    ) -> None:
        labels = {"task_type": task_type, "worker_id": worker_id}
        await self._incr(_metric_key(MetricType.COUNTER, "tasks_total", {"task_type": task_type}))
        if status not in ("ok", "success"):
            await self._incr(
                _metric_key(
                    MetricType.COUNTER,
                    "tasks_errors_total",
                    {"task_type": task_type},
                )
            )
        await self._push_histogram(
            _metric_key(MetricType.HISTOGRAM, "task_duration_ms", {"task_type": task_type}),
            duration_ms,
        )
        _ = labels  # kept for future per-worker histograms

    # ------------------------------------------------------------------
    # System Metrics
    # ------------------------------------------------------------------

    async def set_worker_load(
        self,
        worker_id: str,
        cpu: float,
        ram_mb: float,
        active_jobs: int,
    ) -> None:
        base = f"nexus:metrics:gauge:worker_load:{worker_id}"
        try:
            r = await self._get_redis()
            pipe = r.pipeline(transaction=False)
            pipe.set(f"{base}:cpu", str(cpu))
            pipe.set(f"{base}:ram_mb", str(ram_mb))
            pipe.set(f"{base}:active_jobs", str(active_jobs))
            await pipe.execute()
        except Exception as exc:  # pylint: disable=broad-except
            log.warning("set_worker_load_failed", worker_id=worker_id, error=str(exc))

    async def set_queue_depth(self, queue_name: str, depth: int) -> None:
        await self._set(
            _metric_key(MetricType.GAUGE, "queue_depth", {"queue": queue_name}),
            float(depth),
        )

    async def set_circuit_state(self, worker_id: str, state: str) -> None:
        try:
            r = await self._get_redis()
            await r.set(
                f"nexus:metrics:gauge:circuit_state:{worker_id}", state
            )
        except Exception as exc:  # pylint: disable=broad-except
            log.warning("set_circuit_state_failed", worker_id=worker_id, error=str(exc))

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    async def get_rate(self, metric: str, window_seconds: int = 60) -> float:
        """Return events/sec for a named metric over the last ``window_seconds``."""
        try:
            r = await self._get_redis()
            now = time.time()
            cutoff = now - window_seconds
            # Use the sliding window for requests
            if metric == "requests":
                count = await r.zcount(_COUNTER_WINDOW_KEY, cutoff, now)
                return count / window_seconds
            # Fallback: derive from counter key patterns
            pattern = f"nexus:metrics:counter:{metric}:*"
            keys = await r.keys(pattern)
            total = 0
            for k in keys:
                val = await r.get(k)
                if val:
                    total += int(val)
            return total / window_seconds
        except Exception:  # pylint: disable=broad-except
            return 0.0

    async def get_error_rate(self, task_type: str = "*") -> float:
        """Return error fraction (0.0–1.0) for a task type or all tasks."""
        try:
            r = await self._get_redis()
            if task_type == "*":
                total_keys = await r.keys("nexus:metrics:counter:tasks_total:*")
                error_keys = await r.keys("nexus:metrics:counter:tasks_errors_total:*")
            else:
                lhash = _labels_hash({"task_type": task_type})
                total_keys = [f"nexus:metrics:counter:tasks_total:{lhash}"]
                error_keys = [f"nexus:metrics:counter:tasks_errors_total:{lhash}"]

            async def _sum(keys: list[str]) -> int:
                total = 0
                for k in keys:
                    val = await r.get(k)
                    if val:
                        total += int(val)
                return total

            total = await _sum(total_keys)
            errors = await _sum(error_keys)
            if total == 0:
                return 0.0
            return errors / total
        except Exception:  # pylint: disable=broad-except
            return 0.0

    async def get_p50_p95_p99(self, metric: str) -> dict[str, float]:
        """Return {p50, p95, p99} from a named histogram metric."""
        try:
            r = await self._get_redis()
            pattern = f"nexus:metrics:histogram:{metric}:*"
            keys = await r.keys(pattern)
            all_values: list[float] = []
            for k in keys:
                raw = await r.lrange(k, 0, -1)
                all_values.extend(float(v) for v in raw)

            if not all_values:
                return {"p50": 0.0, "p95": 0.0, "p99": 0.0}

            sorted_vals = sorted(all_values)
            n = len(sorted_vals)

            def _percentile(p: float) -> float:
                idx = int(p / 100 * n)
                return sorted_vals[min(idx, n - 1)]

            return {
                "p50": round(_percentile(50), 3),
                "p95": round(_percentile(95), 3),
                "p99": round(_percentile(99), 3),
            }
        except Exception:  # pylint: disable=broad-except
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0}

    async def get_dashboard_snapshot(self) -> dict:
        """Return a full metrics summary dict suitable for the dashboard API."""
        try:
            r = await self._get_redis()

            # Counters
            requests_total = 0
            for k in await r.keys("nexus:metrics:counter:requests_total:*"):
                val = await r.get(k)
                if val:
                    requests_total += int(val)

            tasks_total = 0
            for k in await r.keys("nexus:metrics:counter:tasks_total:*"):
                val = await r.get(k)
                if val:
                    tasks_total += int(val)

            # Error rates
            req_error_rate = await self.get_error_rate("*")
            # Rates
            req_rate = await self.get_rate("requests", 60)
            task_rate = await self.get_rate("tasks_total", 60)

            # Percentiles
            req_latency = await self.get_p50_p95_p99("request_duration_ms")
            task_latency = await self.get_p50_p95_p99("task_duration_ms")

            # Queue depths
            queue_keys = await r.keys("nexus:metrics:gauge:queue_depth:*")
            queues: dict[str, int] = {}
            for k in queue_keys:
                name = k.split(":")[-1]
                val = await r.get(k)
                queues[name] = int(float(val)) if val else 0

            # Worker loads
            worker_load_keys = await r.keys("nexus:metrics:gauge:worker_load:*:cpu")
            workers: list[dict] = []
            for k in worker_load_keys:
                parts = k.split(":")
                wid = parts[4] if len(parts) > 4 else "unknown"
                base = f"nexus:metrics:gauge:worker_load:{wid}"
                cpu = float(await r.get(f"{base}:cpu") or 0)
                ram = float(await r.get(f"{base}:ram_mb") or 0)
                jobs = int(float(await r.get(f"{base}:active_jobs") or 0))
                workers.append({"worker_id": wid, "cpu": cpu, "ram_mb": ram, "active_jobs": jobs})

            return {
                "requests": {
                    "total": requests_total,
                    "rate_per_sec": round(req_rate, 3),
                    "error_rate": round(req_error_rate, 4),
                    "latency_ms": req_latency,
                },
                "tasks": {
                    "total": tasks_total,
                    "rate_per_sec": round(task_rate, 3),
                    "error_rate": round(await self.get_error_rate("*"), 4),
                    "latency_ms": task_latency,
                },
                "queues": queues,
                "workers": workers,
                "timestamp": time.time(),
            }
        except Exception as exc:  # pylint: disable=broad-except
            log.error("get_dashboard_snapshot_failed", error=str(exc))
            return {"error": str(exc), "timestamp": time.time()}

    # ------------------------------------------------------------------
    # Prometheus export
    # ------------------------------------------------------------------

    async def export_prometheus(self) -> str:
        """Format all Redis metrics as Prometheus text format 0.0.4."""
        lines: list[str] = []

        try:
            r = await self._get_redis()

            async def _scan_counters() -> None:
                for k in await r.keys("nexus:metrics:counter:*"):
                    val = await r.get(k)
                    if val is None:
                        continue
                    parts = k.split(":")
                    # nexus:metrics:counter:<name>:<lhash>
                    metric_name = parts[3] if len(parts) > 3 else k
                    prom_name = f"nexus_{metric_name}".replace("-", "_")
                    lines.append(f"# TYPE {prom_name} counter")
                    lines.append(f'{prom_name} {int(val)}')

            async def _scan_gauges() -> None:
                for k in await r.keys("nexus:metrics:gauge:*"):
                    val = await r.get(k)
                    if val is None:
                        continue
                    try:
                        fval = float(val)
                    except ValueError:
                        continue
                    parts = k.split(":")
                    metric_name = "_".join(parts[3:]) if len(parts) > 3 else k
                    prom_name = f"nexus_{metric_name}".replace("-", "_").replace(".", "_")
                    lines.append(f"# TYPE {prom_name} gauge")
                    lines.append(f"{prom_name} {fval}")

            async def _scan_histograms() -> None:
                for k in await r.keys("nexus:metrics:histogram:*"):
                    raw = await r.lrange(k, 0, -1)
                    if not raw:
                        continue
                    vals = sorted(float(v) for v in raw)
                    n = len(vals)
                    parts = k.split(":")
                    metric_name = parts[3] if len(parts) > 3 else k
                    prom_name = f"nexus_{metric_name}".replace("-", "_")
                    total_sum = sum(vals)

                    def _p(pct: float) -> float:
                        idx = int(pct / 100 * n)
                        return vals[min(idx, n - 1)]

                    lines.append(f"# TYPE {prom_name} summary")
                    lines.append(f'{prom_name}{{quantile="0.5"}} {_p(50):.3f}')
                    lines.append(f'{prom_name}{{quantile="0.95"}} {_p(95):.3f}')
                    lines.append(f'{prom_name}{{quantile="0.99"}} {_p(99):.3f}')
                    lines.append(f"{prom_name}_sum {total_sum:.3f}")
                    lines.append(f"{prom_name}_count {n}")

            await _scan_counters()
            await _scan_gauges()
            await _scan_histograms()

        except Exception as exc:  # pylint: disable=broad-except
            log.error("export_prometheus_failed", error=str(exc))
            lines.append(f"# ERROR {exc}")

        return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# FastAPI route helper
# ---------------------------------------------------------------------------

async def metrics_endpoint(request: Any = None):  # noqa: ARG001
    """FastAPI-compatible endpoint that returns Prometheus text format."""
    try:
        from starlette.responses import Response  # noqa: PLC0415
    except ImportError:
        raise RuntimeError("starlette is required for metrics_endpoint")  # noqa: TRY003

    body = await metrics.export_prometheus()
    return Response(
        content=body,
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

metrics = MetricsCollector()
