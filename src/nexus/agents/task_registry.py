
"""
Task Registry — maps task_type strings to async handler functions.

Design
------
Handlers are plain async functions with a consistent signature:

    async def my_handler(parameters: dict[str, Any]) -> Any:
        ...

They are registered via the `@registry.register("some.task_type")` decorator.
The worker's `execute_task` ARQ function looks up the handler by task_type
and calls it.

Adding a new task type
----------------------
1. Define an async handler function anywhere in the codebase.
2. Decorate it with @registry.register("your.task_type").
3. Import the module containing it in listener.py so the decorator runs.

That's it — no config files, no class hierarchies.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

log = structlog.get_logger(__name__)

HandlerFn = Callable[[dict[str, Any]], Awaitable[Any]]


class TaskRegistry:
    """Central registry mapping task_type strings to async handler functions."""

    def __init__(self) -> None:
        self._handlers: dict[str, HandlerFn] = {}

    def register(self, task_type: str) -> Callable[[HandlerFn], HandlerFn]:
        """Decorator: register an async function as the handler for `task_type`."""
        def decorator(fn: HandlerFn) -> HandlerFn:
            if task_type in self._handlers:
                log.warning("task_type_overwritten", task_type=task_type, new_handler=fn.__name__)
            self._handlers[task_type] = fn
            log.debug("task_type_registered", task_type=task_type, handler=fn.__name__)
            return fn
        return decorator

    async def execute(self, task_type: str, parameters: dict[str, Any]) -> Any:
        """
        Look up and call the handler for `task_type`.

        Raises
        ------
        KeyError   — no handler registered for this task_type.
        Exception  — any exception raised by the handler propagates up so
                     the worker can capture it as a TaskResult with status=FAILED.
        """
        handler = self._handlers.get(task_type)
        if handler is None:
            raise KeyError(f"No handler registered for task_type='{task_type}'")
        return await handler(parameters)

    @property
    def registered_types(self) -> list[str]:
        return list(self._handlers.keys())


# Module-level singleton — import this in handler modules and listener.py.
registry = TaskRegistry()


# ── Built-in example handlers ──────────────────────────────────────────────────
# These demonstrate the pattern.  Replace or extend with real agent tasks.

@registry.register("system.echo")
async def _echo(parameters: dict[str, Any]) -> Any:
    """Trivial smoke-test task: returns whatever it receives."""
    log.info("echo_task_executed", parameters=parameters)
    return {"echo": parameters}


@registry.register("system.sleep")
async def _sleep(parameters: dict[str, Any]) -> Any:
    """Sleep for `seconds` — useful for testing timeout and concurrency logic."""
    seconds = float(parameters.get("seconds", 1.0))
    log.info("sleep_task_started", seconds=seconds)
    await asyncio.sleep(seconds)
    return {"slept_seconds": seconds}
