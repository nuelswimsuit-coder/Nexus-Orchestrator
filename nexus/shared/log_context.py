"""
Log correlation context — structlog integration.

Every log record emitted anywhere in the process automatically includes
whichever of {trace_id, session_id, task_id, worker_id, node_id} have been
set in the current async context.

Usage
-----
    from nexus.shared.log_context import LogContext, configure_structlog, log_context

    configure_structlog()   # once at startup

    LogContext.set_trace_id("abc123")

    with log_context(task_id="task-42", worker_id="w1"):
        log = CorrelatedLogger("my-module")
        log.info("doing work", extra_field="value")
"""

from __future__ import annotations

import logging
import os
import sys
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

import structlog


# ---------------------------------------------------------------------------
# Context storage
# ---------------------------------------------------------------------------

_ctx_trace_id: ContextVar[str | None] = ContextVar("log_trace_id", default=None)
_ctx_session_id: ContextVar[str | None] = ContextVar("log_session_id", default=None)
_ctx_task_id: ContextVar[str | None] = ContextVar("log_task_id", default=None)
_ctx_worker_id: ContextVar[str | None] = ContextVar("log_worker_id", default=None)
_ctx_node_id: ContextVar[str | None] = ContextVar("log_node_id", default=None)


class LogContext:
    """Namespace for setting and reading per-async-task correlation IDs."""

    @staticmethod
    def set_trace_id(trace_id: str) -> None:
        _ctx_trace_id.set(trace_id)

    @staticmethod
    def set_session_id(session_id: str) -> None:
        _ctx_session_id.set(session_id)

    @staticmethod
    def set_task_id(task_id: str) -> None:
        _ctx_task_id.set(task_id)

    @staticmethod
    def set_worker_id(worker_id: str) -> None:
        _ctx_worker_id.set(worker_id)

    @staticmethod
    def set_node_id(node_id: str) -> None:
        _ctx_node_id.set(node_id)

    @staticmethod
    def get_all() -> dict[str, str]:
        """Return all currently-set correlation IDs (None values omitted)."""
        raw = {
            "trace_id": _ctx_trace_id.get(),
            "session_id": _ctx_session_id.get(),
            "task_id": _ctx_task_id.get(),
            "worker_id": _ctx_worker_id.get(),
            "node_id": _ctx_node_id.get(),
        }
        return {k: v for k, v in raw.items() if v is not None}

    @staticmethod
    def clear() -> None:
        _ctx_trace_id.set(None)
        _ctx_session_id.set(None)
        _ctx_task_id.set(None)
        _ctx_worker_id.set(None)
        _ctx_node_id.set(None)


# ---------------------------------------------------------------------------
# Custom structlog processor
# ---------------------------------------------------------------------------

def structlog_processor(
    logger: Any,  # noqa: ARG001
    method: str,  # noqa: ARG001
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Inject all active LogContext values into every log record."""
    ctx = LogContext.get_all()
    for k, v in ctx.items():
        event_dict.setdefault(k, v)
    return event_dict


# ---------------------------------------------------------------------------
# Structlog configuration
# ---------------------------------------------------------------------------

def configure_structlog(level: str | None = None, node_id: str | None = None) -> None:
    """Configure structlog for the current process.

    Call once at startup.  Safe to call multiple times (idempotent).

    Parameters
    ----------
    level:
        Log level string — defaults to the ``LOG_LEVEL`` env var or ``"INFO"``.
    node_id:
        Optional node identifier to bind to every log line.
    """
    env_level = os.environ.get("LOG_LEVEL", "INFO")
    log_level_str = (level or env_level).upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    is_dev = os.environ.get("NEXUS_ENV", "production").lower() in ("dev", "development", "local")

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog_processor,                          # correlation IDs
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.CallsiteParameterAdder(
            [
                structlog.processors.CallsiteParameter.FILENAME,
                structlog.processors.CallsiteParameter.LINENO,
            ]
        ),
    ]

    if is_dev:
        final_processor: structlog.types.Processor = structlog.dev.ConsoleRenderer(colors=True)
    else:
        final_processor = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=shared_processors
        + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            final_processor,
        ],
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)

    if node_id:
        structlog.contextvars.bind_contextvars(node_id=node_id)


# ---------------------------------------------------------------------------
# Correlated logger wrapper
# ---------------------------------------------------------------------------

class CorrelatedLogger:
    """Structlog wrapper that binds LogContext values on every call."""

    def __init__(self, name: str) -> None:
        self._log = structlog.get_logger(name)
        self._bound: dict[str, Any] = {}

    def bind(self, **kwargs: Any) -> "CorrelatedLogger":
        """Return a new CorrelatedLogger with extra bound fields."""
        new = CorrelatedLogger.__new__(CorrelatedLogger)
        new._log = self._log.bind(**kwargs)  # type: ignore[attr-defined]
        new._bound = {**self._bound, **kwargs}
        return new

    def _get_logger(self) -> Any:
        ctx = LogContext.get_all()
        if ctx:
            return self._log.bind(**ctx)
        return self._log

    def info(self, msg: str, **kwargs: Any) -> None:
        self._get_logger().info(msg, **kwargs)

    def warning(self, msg: str, **kwargs: Any) -> None:
        self._get_logger().warning(msg, **kwargs)

    # alias
    warn = warning

    def error(self, msg: str, **kwargs: Any) -> None:
        self._get_logger().error(msg, **kwargs)

    def debug(self, msg: str, **kwargs: Any) -> None:
        self._get_logger().debug(msg, **kwargs)

    def critical(self, msg: str, **kwargs: Any) -> None:
        self._get_logger().critical(msg, **kwargs)

    def exception(self, msg: str, **kwargs: Any) -> None:
        self._get_logger().exception(msg, **kwargs)


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------

@contextmanager
def log_context(**kwargs: str):
    """Set multiple log context values for the duration of the block.

    Example
    -------
        with log_context(task_id="t-1", worker_id="w-2"):
            log.info("doing work")
    """
    tokens: dict[str, Any] = {}
    setters = {
        "trace_id": (_ctx_trace_id, LogContext.set_trace_id),
        "session_id": (_ctx_session_id, LogContext.set_session_id),
        "task_id": (_ctx_task_id, LogContext.set_task_id),
        "worker_id": (_ctx_worker_id, LogContext.set_worker_id),
        "node_id": (_ctx_node_id, LogContext.set_node_id),
    }

    for key, value in kwargs.items():
        if key in setters:
            ctx_var, setter = setters[key]
            tokens[key] = ctx_var.set(value)
        else:
            # For unknown keys bind them into structlog contextvars directly
            structlog.contextvars.bind_contextvars(**{key: value})

    try:
        yield
    finally:
        for key, token in tokens.items():
            ctx_var, _ = setters[key]
            ctx_var.reset(token)
        # Clear any unknown keys that may have been bound
        extra_keys = [k for k in kwargs if k not in setters]
        if extra_keys:
            structlog.contextvars.unbind_contextvars(*extra_keys)
