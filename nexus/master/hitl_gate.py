"""
HITL Gate — Human-in-the-Loop pause mechanism.

Architecture
------------
When the Master's dispatcher encounters a TaskPayload with
`requires_approval=True`, it calls `HitlGate.request_approval()` before
enqueuing the task onto the worker queue.

Flow:
    1. Dispatcher calls `await gate.request_approval(task)`
    2. Gate registers an asyncio.Event keyed by request_id.
    3. Gate publishes a HitlRequest to HITL_REQUEST_CHANNEL.
       → The API's HitlStore receives it and surfaces it in the dashboard.
       → NotificationService fires alerts (WhatsApp, etc.) to the operator.
    4. Gate suspends on `asyncio.wait_for(event.wait(), timeout=...)`.
       The worker queue is NOT touched — no worker capacity is consumed.
    5. The operator clicks Approve/Reject in the React dashboard.
       → The API's HitlStore publishes a HitlResponse to HITL_RESPONSE_CHANNEL.
    6. The background listener in `start()` receives the response and calls
       `_handle_response()`, which stores the decision and sets the Event.
    7. `request_approval()` unblocks:
       - approved  → fires resolved notification, returns normally.
       - rejected  → fires resolved notification, raises TaskRejectedError.
       - timed out → raises asyncio.TimeoutError.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import structlog
from redis.asyncio import Redis

from nexus.shared.constants import (
    HITL_APPROVAL_TIMEOUT,
    HITL_PENDING_KEY_PREFIX,
    HITL_PENDING_TTL,
    HITL_REQUEST_CHANNEL,
    HITL_RESPONSE_CHANNEL,
)
from nexus.shared.notifications.service import NotificationService
from nexus.shared.schemas import HitlRequest, HitlResponse, TaskPayload

log = structlog.get_logger(__name__)


class TaskRejectedError(Exception):
    """Raised when a human operator rejects a task at the HITL gate."""


class HitlGate:
    """
    Manages the suspend/resume lifecycle for tasks requiring human approval.

    One HitlGate instance lives on the Master for the lifetime of the process.
    It must be started with `await gate.start()` before any tasks are dispatched.

    Parameters
    ----------
    redis               : Async Redis client (shared with ARQ pool).
    notification_service: Optional NotificationService.  When provided, HITL
                          events (requested / approved / rejected) are fanned
                          out to all registered providers (WhatsApp, etc.).
    """

    def __init__(
        self,
        redis: Redis,
        notification_service: NotificationService | None = None,
    ) -> None:
        self._redis = redis
        self._notifier = notification_service
        # request_id → asyncio.Event set when a response arrives.
        self._pending: dict[str, asyncio.Event] = {}
        # request_id → the HitlResponse received from the operator.
        self._responses: dict[str, HitlResponse] = {}
        self._listener_task: asyncio.Task | None = None  # type: ignore[type-arg]

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Subscribe to HITL_RESPONSE_CHANNEL and start the background listener.

        The listener runs inside a resilient wrapper that automatically
        restarts it if the pubsub connection drops (Redis reconnect, network
        blip, etc.).  This ensures the HITL gate stays alive for the entire
        lifetime of the master process.

        Must be called once before any `request_approval()` calls.
        """
        self._listener_task = asyncio.create_task(
            self._listen_with_restart(), name="hitl-gate-listener"
        )
        log.info("hitl_gate_started", listening_on=HITL_RESPONSE_CHANNEL)

    async def stop(self) -> None:
        """Cancel the background listener gracefully on master shutdown."""
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass

    # ── Public API ─────────────────────────────────────────────────────────────

    async def request_approval(self, task: TaskPayload) -> None:
        """
        Suspend dispatch of `task` until a human approves or rejects it.

        This coroutine blocks the calling `dispatch()` call — and only that
        call — until a decision arrives on HITL_RESPONSE_CHANNEL.  All other
        concurrent dispatches continue normally because this is async.

        Raises
        ------
        TaskRejectedError    — operator explicitly rejected the task.
        asyncio.TimeoutError — no response within HITL_APPROVAL_TIMEOUT seconds.
        """
        if not task.requires_approval:
            return

        expires_at = datetime.now(timezone.utc) + timedelta(seconds=HITL_APPROVAL_TIMEOUT)
        hitl_request = HitlRequest(
            task_id=task.task_id,
            task_type=task.task_type,
            project_id=task.project_id,
            context=task.approval_context or f"Task '{task.task_type}' requires approval.",
            expires_at=expires_at,
        )

        # Register the event BEFORE publishing — prevents a race where the
        # response arrives before we start waiting.
        event = asyncio.Event()
        self._pending[hitl_request.request_id] = event

        log.info(
            "hitl_approval_requested",
            task_id=task.task_id,
            task_type=task.task_type,
            project_id=task.project_id,
            request_id=hitl_request.request_id,
            context=hitl_request.context,
        )

        # Write to the durable Redis key FIRST so the API can serve it
        # even if it started after this publish (pub/sub is fire-and-forget).
        pending_key = f"{HITL_PENDING_KEY_PREFIX}{hitl_request.request_id}"
        await self._redis.set(
            pending_key,
            hitl_request.model_dump_json(),
            ex=HITL_PENDING_TTL,
        )

        # Also publish to the channel for real-time delivery to live subscribers.
        await self._redis.publish(
            HITL_REQUEST_CHANNEL,
            hitl_request.model_dump_json(),
        )

        # Fire notification (non-blocking — failures are swallowed inside
        # NotificationService so they never interrupt the gate flow).
        # request_id is passed so TelegramProvider can embed it in button
        # callback_data, allowing the bot to resolve the correct HITL request.
        if self._notifier:
            asyncio.create_task(
                self._notifier.notify_hitl_requested(
                    task_id=task.task_id,
                    task_type=task.task_type,
                    project_id=task.project_id,
                    context=hitl_request.context,
                    request_id=hitl_request.request_id,
                ),
                name=f"notify-hitl-{task.task_id}",
            )

        # Block until the operator responds or the timeout elapses.
        try:
            await asyncio.wait_for(event.wait(), timeout=float(HITL_APPROVAL_TIMEOUT))
        except asyncio.TimeoutError:
            log.error(
                "hitl_approval_timeout",
                task_id=task.task_id,
                request_id=hitl_request.request_id,
                timeout_s=HITL_APPROVAL_TIMEOUT,
            )
            raise
        finally:
            self._pending.pop(hitl_request.request_id, None)
            # Remove the durable key so the dashboard stops showing it
            try:
                await self._redis.delete(
                    f"{HITL_PENDING_KEY_PREFIX}{hitl_request.request_id}"
                )
            except Exception:
                pass

        response = self._responses.pop(hitl_request.request_id)

        # Fire resolved notification.
        if self._notifier:
            asyncio.create_task(
                self._notifier.notify_hitl_resolved(
                    task_id=task.task_id,
                    approved=response.approved,
                    reviewer_id=response.reviewer_id,
                    reason=response.reason,
                ),
                name=f"notify-hitl-resolved-{task.task_id}",
            )

        if not response.approved:
            log.info(
                "hitl_task_rejected",
                task_id=task.task_id,
                reviewer=response.reviewer_id,
                reason=response.reason,
            )
            raise TaskRejectedError(
                f"Task {task.task_id} rejected by {response.reviewer_id}: {response.reason}"
            )

        log.info(
            "hitl_task_approved",
            task_id=task.task_id,
            reviewer=response.reviewer_id,
        )

    # ── Internal ───────────────────────────────────────────────────────────────

    def _handle_response(self, response: HitlResponse) -> None:
        """
        Called by the listener when a HitlResponse arrives on Redis.

        Stores the response and sets the asyncio.Event so the suspended
        `request_approval()` coroutine can unblock and read the decision.
        """
        self._responses[response.request_id] = response
        event = self._pending.get(response.request_id)
        if event:
            event.set()
            log.debug(
                "hitl_event_set",
                request_id=response.request_id,
                approved=response.approved,
            )
        else:
            log.warning(
                "hitl_response_orphaned",
                request_id=response.request_id,
                task_id=response.task_id,
            )

    async def _listen_with_restart(self) -> None:
        """
        Resilient wrapper around `_listen()`.

        If the pubsub connection drops for any reason other than an explicit
        cancellation (e.g. Redis restart, network blip), this wrapper waits
        2 seconds and restarts the listener automatically.

        This ensures the HITL gate never permanently loses its subscription
        for the lifetime of the master process.
        """
        restart_delay = 2.0
        while True:
            try:
                await self._listen()
                # _listen() only returns normally on CancelledError (re-raised)
                # or if the pubsub loop exits cleanly — treat as unexpected.
                log.warning(
                    "hitl_gate_listener_exited_unexpectedly",
                    hint="Restarting in 2s",
                )
            except asyncio.CancelledError:
                # Explicit stop() call — do not restart.
                log.info("hitl_gate_listener_stopped")
                raise
            except Exception as exc:
                log.error(
                    "hitl_gate_listener_error",
                    error=str(exc),
                    restart_in_s=restart_delay,
                )

            # Brief pause before reconnecting so we don't hammer Redis
            await asyncio.sleep(restart_delay)
            log.info("hitl_gate_listener_restarting", channel=HITL_RESPONSE_CHANNEL)

    async def _listen(self) -> None:
        """
        Background coroutine: subscribe to HITL_RESPONSE_CHANNEL and route
        incoming decisions to the correct waiting `request_approval()` call.

        Raises asyncio.CancelledError on explicit stop().
        May raise other exceptions on connection failure — caught by
        _listen_with_restart() which will restart this coroutine.
        """
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(HITL_RESPONSE_CHANNEL)
        log.info("hitl_gate_subscribed", channel=HITL_RESPONSE_CHANNEL)

        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                try:
                    response = HitlResponse.model_validate_json(message["data"])
                    self._handle_response(response)
                except Exception as exc:
                    log.error(
                        "hitl_response_parse_error",
                        error=str(exc),
                        raw=message.get("data"),
                    )
        except asyncio.CancelledError:
            try:
                await pubsub.unsubscribe(HITL_RESPONSE_CHANNEL)
            except Exception:
                pass
            raise
