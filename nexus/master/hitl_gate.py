"""
HITL Gate — Human-in-the-Loop pause mechanism.

Architecture overview
---------------------
When the Master's dispatcher encounters a TaskPayload with
`requires_approval=True`, it calls `HitlGate.request_approval()` before
enqueuing the task onto the worker queue.

Flow:
    1. Dispatcher calls `await gate.request_approval(task)`
    2. Gate publishes a HitlRequest to Redis channel HITL_REQUEST_CHANNEL.
    3. Gate suspends (asyncio.Event) — the worker slot is NOT consumed.
    4. An external approval process (CLI, web UI, Slack bot) subscribes to
       HITL_REQUEST_CHANNEL, shows the context to a human, and publishes a
       HitlResponse to HITL_RESPONSE_CHANNEL.
    5. Gate's listener coroutine receives the response, resolves the Event.
    6. If approved → dispatcher proceeds to enqueue.
       If rejected  → dispatcher raises TaskRejectedError.
    7. If HITL_APPROVAL_TIMEOUT elapses with no response → TimeoutError.

Current state: STUB
-------------------
The Redis pub/sub wiring is sketched but the actual subscription loop is
marked TODO.  The `request_approval` method currently logs and auto-approves
so the rest of the system can be developed and tested without a live HITL UI.
Remove the auto-approve shortcut once the approval UI is built.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import structlog
from redis.asyncio import Redis

from nexus.shared.constants import (
    HITL_APPROVAL_TIMEOUT,
    HITL_REQUEST_CHANNEL,
)
from nexus.shared.schemas import HitlRequest, HitlResponse, TaskPayload

log = structlog.get_logger(__name__)


class TaskRejectedError(Exception):
    """Raised when a human operator rejects a task at the HITL gate."""


class HitlGate:
    """
    Manages the suspend/resume lifecycle for tasks requiring human approval.

    One HitlGate instance lives on the Master for the lifetime of the process.
    """

    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        # Maps request_id → asyncio.Event that unblocks when a response arrives.
        self._pending: dict[str, asyncio.Event] = {}
        # Maps request_id → the HitlResponse received from the operator.
        self._responses: dict[str, HitlResponse] = {}

    async def start(self) -> None:
        """
        Launch the background listener that watches HITL_RESPONSE_CHANNEL.

        Call once at master startup:
            asyncio.create_task(gate.start())
        """
        # TODO: implement Redis pub/sub subscription loop.
        #
        # Skeleton:
        #   pubsub = self._redis.pubsub()
        #   await pubsub.subscribe(HITL_RESPONSE_CHANNEL)
        #   async for message in pubsub.listen():
        #       if message["type"] == "message":
        #           response = HitlResponse.model_validate_json(message["data"])
        #           self._handle_response(response)
        #
        log.info("hitl_gate_started", status="stub — auto-approving all requests")

    def _handle_response(self, response: HitlResponse) -> None:
        """Called by the subscription loop when a human decision arrives."""
        self._responses[response.request_id] = response
        event = self._pending.get(response.request_id)
        if event:
            event.set()
        else:
            log.warning("hitl_response_orphaned", request_id=response.request_id)

    async def request_approval(self, task: TaskPayload) -> None:
        """
        Suspend dispatch of `task` until a human approves or rejects it.

        Raises
        ------
        TaskRejectedError   — operator explicitly rejected the task.
        asyncio.TimeoutError — no response within HITL_APPROVAL_TIMEOUT.
        """
        if not task.requires_approval:
            return

        expires_at = datetime.now(timezone.utc) + timedelta(seconds=HITL_APPROVAL_TIMEOUT)
        hitl_request = HitlRequest(
            task_id=task.task_id,
            task_type=task.task_type,
            context=task.approval_context or f"Task '{task.task_type}' requires approval.",
            expires_at=expires_at,
        )

        log.info(
            "hitl_approval_requested",
            task_id=task.task_id,
            task_type=task.task_type,
            context=hitl_request.context,
        )

        # Publish the request so the approval UI can pick it up.
        await self._redis.publish(
            HITL_REQUEST_CHANNEL,
            hitl_request.model_dump_json(),
        )

        # ── AUTO-APPROVE STUB ──────────────────────────────────────────────────
        # Remove this block once a real approval UI is wired to HITL_RESPONSE_CHANNEL.
        # Replace with the asyncio.Event wait below.
        log.warning(
            "hitl_auto_approving",
            task_id=task.task_id,
            reason="HITL UI not yet implemented — remove auto-approve for production",
        )
        return
        # ── END STUB ───────────────────────────────────────────────────────────

        # Real implementation (unreachable until stub is removed):
        event = asyncio.Event()
        self._pending[hitl_request.request_id] = event

        try:
            await asyncio.wait_for(event.wait(), timeout=HITL_APPROVAL_TIMEOUT)
        except asyncio.TimeoutError:
            log.error("hitl_approval_timeout", task_id=task.task_id)
            raise
        finally:
            self._pending.pop(hitl_request.request_id, None)

        response = self._responses.pop(hitl_request.request_id)
        if not response.approved:
            log.info("hitl_task_rejected", task_id=task.task_id, reason=response.reason)
            raise TaskRejectedError(
                f"Task {task.task_id} rejected by {response.reviewer_id}: {response.reason}"
            )

        log.info("hitl_task_approved", task_id=task.task_id, reviewer=response.reviewer_id)
