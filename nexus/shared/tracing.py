"""
Distributed tracing for the Nexus platform.

Provides async-safe trace context propagation via contextvars, HTTP header
injection/extraction, task payload embedding, and a FastAPI middleware that
correlates every request with a trace.

Usage
-----
    from nexus.shared.tracing import tracer, traced, TraceMiddleware

    # Create a root trace
    ctx = tracer.start_trace({"session_id": "abc", "worker_id": "w1"})

    # Child span via context manager
    with traced("my-operation"):
        ...

    # FastAPI — add middleware in lifespan setup:
    app.add_middleware(TraceMiddleware)
"""

from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

import structlog

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class TraceContext:
    trace_id: str
    span_id: str
    parent_span_id: str | None
    started_at: float  # time.monotonic()
    baggage: dict[str, str] = field(default_factory=dict)
    name: str = ""


# ---------------------------------------------------------------------------
# Header / task-payload keys
# ---------------------------------------------------------------------------

_H_TRACE_ID = "x-trace-id"
_H_SPAN_ID = "x-span-id"
_H_PARENT_SPAN_ID = "x-parent-span-id"
_BAGGAGE_PREFIX = "x-baggage-"

_TASK_TRACE_KEY = "__trace__"


# ---------------------------------------------------------------------------
# Core tracer
# ---------------------------------------------------------------------------

class Tracer:
    """Async-safe distributed tracer backed by contextvars."""

    _ctx_var: ContextVar[TraceContext | None] = ContextVar(
        "nexus_trace_context", default=None
    )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start_trace(self, baggage: dict[str, str] | None = None) -> TraceContext:
        """Create a new root span and install it in the current context."""
        ctx = TraceContext(
            trace_id=str(uuid.uuid4()),
            span_id=str(uuid.uuid4()),
            parent_span_id=None,
            started_at=time.monotonic(),
            baggage=dict(baggage or {}),
        )
        self._ctx_var.set(ctx)
        structlog.contextvars.bind_contextvars(trace_id=ctx.trace_id, span_id=ctx.span_id)
        return ctx

    def start_span(self, name: str) -> TraceContext:
        """Create a child span that inherits the current trace context."""
        parent = self._ctx_var.get()
        if parent is None:
            # No active trace — start a new root
            ctx = self.start_trace()
            ctx.name = name
            return ctx

        ctx = TraceContext(
            trace_id=parent.trace_id,
            span_id=str(uuid.uuid4()),
            parent_span_id=parent.span_id,
            started_at=time.monotonic(),
            baggage=dict(parent.baggage),
            name=name,
        )
        self._ctx_var.set(ctx)
        structlog.contextvars.bind_contextvars(span_id=ctx.span_id)
        return ctx

    def get_current(self) -> TraceContext | None:
        return self._ctx_var.get()

    # ------------------------------------------------------------------
    # HTTP propagation
    # ------------------------------------------------------------------

    def inject_to_headers(self, ctx: TraceContext) -> dict[str, str]:
        headers: dict[str, str] = {
            "X-Trace-Id": ctx.trace_id,
            "X-Span-Id": ctx.span_id,
        }
        if ctx.parent_span_id:
            headers["X-Parent-Span-Id"] = ctx.parent_span_id
        for k, v in ctx.baggage.items():
            headers[f"X-Baggage-{k}"] = v
        return headers

    def extract_from_headers(self, headers: dict[str, str]) -> TraceContext | None:
        # Normalise header names to lowercase
        h = {k.lower(): v for k, v in headers.items()}
        trace_id = h.get(_H_TRACE_ID)
        if not trace_id:
            return None
        baggage = {
            k[len(_BAGGAGE_PREFIX):]: v
            for k, v in h.items()
            if k.startswith(_BAGGAGE_PREFIX)
        }
        ctx = TraceContext(
            trace_id=trace_id,
            span_id=h.get(_H_SPAN_ID, str(uuid.uuid4())),
            parent_span_id=h.get(_H_PARENT_SPAN_ID),
            started_at=time.monotonic(),
            baggage=baggage,
        )
        return ctx

    # ------------------------------------------------------------------
    # Task-payload propagation
    # ------------------------------------------------------------------

    def inject_to_task(self, task_payload: dict, ctx: TraceContext) -> dict:
        """Embed the trace context inside a task's metadata dict."""
        payload = dict(task_payload)
        payload[_TASK_TRACE_KEY] = {
            "trace_id": ctx.trace_id,
            "span_id": ctx.span_id,
            "parent_span_id": ctx.parent_span_id,
            "baggage": ctx.baggage,
        }
        return payload

    def extract_from_task(self, task_payload: dict) -> TraceContext | None:
        raw = task_payload.get(_TASK_TRACE_KEY)
        if not raw:
            return None
        return TraceContext(
            trace_id=raw.get("trace_id", str(uuid.uuid4())),
            span_id=str(uuid.uuid4()),  # new span for this processing step
            parent_span_id=raw.get("span_id"),
            started_at=time.monotonic(),
            baggage=raw.get("baggage", {}),
        )

    # ------------------------------------------------------------------
    # Span completion
    # ------------------------------------------------------------------

    async def finish_span(
        self,
        ctx: TraceContext,
        status: str = "ok",
        error: str | None = None,
    ) -> None:
        duration_ms = (time.monotonic() - ctx.started_at) * 1000
        log_fn = log.error if status == "error" else log.info
        log_fn(
            "span_finished",
            trace_id=ctx.trace_id,
            span_id=ctx.span_id,
            parent_span_id=ctx.parent_span_id,
            name=ctx.name,
            status=status,
            duration_ms=round(duration_ms, 3),
            error=error,
            **ctx.baggage,
        )
        # Attempt to emit to the event store when Redis is available
        try:
            from nexus.shared.config import settings  # noqa: PLC0415
            import redis.asyncio as aioredis  # noqa: PLC0415

            client: Any = aioredis.from_url(settings.redis_url, decode_responses=True)
            import json  # noqa: PLC0415

            event = {
                "trace_id": ctx.trace_id,
                "span_id": ctx.span_id,
                "parent_span_id": ctx.parent_span_id,
                "name": ctx.name,
                "status": status,
                "duration_ms": round(duration_ms, 3),
                "error": error,
                "baggage": ctx.baggage,
            }
            key = f"nexus:traces:{ctx.trace_id}"
            async with client:
                pipe = client.pipeline(transaction=False)
                pipe.rpush(key, json.dumps(event))
                pipe.expire(key, 3600)  # 1-hour TTL
                await pipe.execute()
        except Exception:  # pylint: disable=broad-except
            pass  # tracing must never crash the caller


# ---------------------------------------------------------------------------
# Context manager helper
# ---------------------------------------------------------------------------

@contextmanager
def traced(name: str):
    """Synchronous context manager that wraps a block in a child span.

    Calls finish_span synchronously (fire-and-forget via asyncio if possible).
    For async code prefer ``async with`` pattern or call tracer directly.
    """
    ctx = tracer.start_span(name)
    status = "ok"
    error_msg: str | None = None
    try:
        yield ctx
    except Exception as exc:  # pylint: disable=broad-except
        status = "error"
        error_msg = str(exc)
        raise
    finally:
        import asyncio  # noqa: PLC0415

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(tracer.finish_span(ctx, status=status, error=error_msg))
        except RuntimeError:
            # No running event loop — skip async finish
            pass


# ---------------------------------------------------------------------------
# FastAPI middleware
# ---------------------------------------------------------------------------

try:
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.requests import Request
    from starlette.responses import Response

    class TraceMiddleware(BaseHTTPMiddleware):
        """Extracts or creates a trace for every incoming HTTP request."""

        async def dispatch(self, request: Request, call_next):  # type: ignore[override]
            incoming_headers = dict(request.headers)
            ctx = tracer.extract_from_headers(incoming_headers)
            if ctx is None:
                ctx = tracer.start_trace(
                    {
                        "path": request.url.path,
                        "method": request.method,
                    }
                )
            else:
                # Promote extracted context into ContextVar
                tracer._ctx_var.set(ctx)
                structlog.contextvars.bind_contextvars(
                    trace_id=ctx.trace_id, span_id=ctx.span_id
                )

            ctx.name = f"{request.method} {request.url.path}"

            try:
                response: Response = await call_next(request)
            except Exception:
                await tracer.finish_span(ctx, status="error")
                raise

            response.headers["X-Trace-Id"] = ctx.trace_id
            await tracer.finish_span(ctx, status=str(response.status_code))
            return response

except ImportError:
    # Starlette not installed — TraceMiddleware simply unavailable
    class TraceMiddleware:  # type: ignore[no-redef]
        pass


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

tracer = Tracer()
