"""
HitlStore — server-side state for pending HITL requests.

Architecture
------------
The master's HitlGate writes each HitlRequest to TWO places:
  1. Redis pub/sub channel HITL_REQUEST_CHANNEL  — real-time delivery
  2. Redis key nexus:hitl:pending:<request_id>   — durable store (2h TTL)

HitlStore uses BOTH:
  - The pub/sub listener populates _pending in real time.
  - On startup and on every GET /api/hitl/pending call, it also scans the
    durable keys so requests published before the API started are not lost.

This means the dashboard will always show pending tasks even if:
  - The API server restarted after the master published the request.
  - The pub/sub message was missed due to a network blip.
"""

from __future__ import annotations

import asyncio

import structlog
from redis.asyncio import Redis

from nexus.shared.constants import (
    HITL_PENDING_KEY_PREFIX,
    HITL_REQUEST_CHANNEL,
    HITL_RESPONSE_CHANNEL,
)
from nexus.shared.schemas import HitlRequest, HitlResponse

log = structlog.get_logger(__name__)


class HitlStore:
    """
    Maintains a live dict of pending HITL requests and bridges
    operator decisions back to the master via Redis pub/sub.
    """

    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        # request_id → HitlRequest
        self._pending: dict[str, HitlRequest] = {}
        self._listener_task: asyncio.Task | None = None  # type: ignore[type-arg]

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Subscribe to HITL_REQUEST_CHANNEL and start the listener loop."""
        # Recover any requests that were published before we started
        await self._sync_from_redis()

        self._listener_task = asyncio.create_task(
            self._listen(), name="hitl-store-listener"
        )
        log.info("hitl_store_started", recovered=len(self._pending))

    async def stop(self) -> None:
        """Cancel the background listener gracefully."""
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        log.info("hitl_store_stopped")

    # ── Public API ─────────────────────────────────────────────────────────────

    def pending(self) -> list[HitlRequest]:
        """Return all currently pending HITL requests, newest first."""
        return sorted(
            self._pending.values(),
            key=lambda r: r.requested_at,
            reverse=True,
        )

    async def refresh(self) -> None:
        """Re-sync from Redis durable keys (called by the GET endpoint)."""
        await self._sync_from_redis()

    async def resolve(
        self,
        request_id: str,
        approved: bool,
        reviewer_id: str = "dashboard",
        reason: str | None = None,
    ) -> HitlResponse:
        """
        Record a human decision and publish it to the master via Redis.

        Raises
        ------
        KeyError — request_id not found in pending requests.
        """
        # Try in-memory first; if missing, check Redis durable store
        if request_id not in self._pending:
            await self._sync_from_redis()

        if request_id not in self._pending:
            raise KeyError(f"No pending HITL request with id={request_id!r}")

        hitl_req = self._pending.pop(request_id)
        response = HitlResponse(
            request_id=request_id,
            task_id=hitl_req.task_id,
            approved=approved,
            reviewer_id=reviewer_id,
            reason=reason,
        )

        # Publish the decision to the master's HitlGate listener
        await self._redis.publish(HITL_RESPONSE_CHANNEL, response.model_dump_json())

        # Remove the durable key so the dashboard stops showing it
        try:
            await self._redis.delete(f"{HITL_PENDING_KEY_PREFIX}{request_id}")
        except Exception:
            pass

        log.info(
            "hitl_resolved",
            request_id=request_id,
            task_id=hitl_req.task_id,
            approved=approved,
            reviewer=reviewer_id,
        )
        return response

    # ── Internal ───────────────────────────────────────────────────────────────

    async def _sync_from_redis(self) -> None:
        """
        Scan nexus:hitl:pending:* keys and load any requests not already
        in _pending.  This recovers requests published before the API started
        or missed due to a pub/sub gap.
        """
        try:
            pattern = f"{HITL_PENDING_KEY_PREFIX}*".encode()
            cursor = 0
            found = 0
            while True:
                cursor, keys = await self._redis.scan(
                    cursor=cursor, match=pattern, count=100
                )
                for key in keys:
                    raw = await self._redis.get(key)
                    if raw is None:
                        continue
                    try:
                        req = HitlRequest.model_validate_json(raw)
                        if req.request_id not in self._pending:
                            self._pending[req.request_id] = req
                            found += 1
                            log.info(
                                "hitl_request_recovered",
                                request_id=req.request_id,
                                task_id=req.task_id,
                                task_type=req.task_type,
                            )
                    except Exception as exc:
                        log.error("hitl_key_parse_error", key=key, error=str(exc))
                if cursor == 0:
                    break
            if found:
                log.info("hitl_store_synced", recovered=found, total=len(self._pending))
        except Exception as exc:
            log.error("hitl_store_sync_error", error=str(exc))

    async def _listen(self) -> None:
        """
        Background coroutine: subscribe to HITL_REQUEST_CHANNEL and
        populate self._pending as new requests arrive from the master.
        """
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(HITL_REQUEST_CHANNEL)
        log.info("hitl_store_subscribed", channel=HITL_REQUEST_CHANNEL)

        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                try:
                    req = HitlRequest.model_validate_json(message["data"])
                    self._pending[req.request_id] = req
                    log.info(
                        "hitl_request_received",
                        request_id=req.request_id,
                        task_id=req.task_id,
                        task_type=req.task_type,
                    )
                except Exception as exc:
                    log.error("hitl_request_parse_error", error=str(exc))
        except asyncio.CancelledError:
            await pubsub.unsubscribe(HITL_REQUEST_CHANNEL)
            raise
