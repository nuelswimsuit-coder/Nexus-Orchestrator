"""
Pydantic v2 data contracts shared between the Master and all Worker nodes.

These models are the single source of truth for what travels over the wire.
Both sides validate against them, so a malformed payload is caught immediately
at the boundary rather than deep inside business logic.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

# ── Enumerations ───────────────────────────────────────────────────────────────

class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    # HITL pause: task is suspended, waiting for a human decision before
    # the worker is allowed to continue execution.
    AWAITING_APPROVAL = "awaiting_approval"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class NodeRole(str, Enum):
    MASTER = "master"
    WORKER = "worker"


# ── Core task models ───────────────────────────────────────────────────────────

class TaskPayload(BaseModel):
    """
    Represents a unit of work dispatched by the Master to a Worker.

    The `task_type` string is resolved by the worker's TaskRegistry to a
    concrete handler function.  `parameters` is an open dict so any handler
    can receive arbitrary typed arguments without requiring a new schema per
    task type — handlers are responsible for validating their own parameters.
    """

    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str = Field(
        ...,
        description="Dot-separated handler name, e.g. 'file.process' or 'llm.summarise'",
    )
    parameters: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=5, ge=1, le=10)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # ── HITL metadata ──────────────────────────────────────────────────────────
    # Set by the master when a task is flagged as requiring human approval
    # before the worker executes it.  The HITL gate in master/hitl_gate.py
    # checks this flag and blocks dispatch until approval is received.
    requires_approval: bool = False
    approval_context: str | None = Field(
        default=None,
        description="Human-readable description shown to the approver in the HITL UI",
    )

    model_config = {"frozen": True}


class TaskResult(BaseModel):
    """
    Returned by a Worker after it finishes (or fails) a TaskPayload.

    The master's dispatcher collects these and updates its internal state.
    """

    task_id: str
    worker_id: str
    status: TaskStatus
    output: Any | None = None
    error: str | None = None
    started_at: datetime | None = None
    finished_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    duration_seconds: float | None = None


# ── HITL approval models ───────────────────────────────────────────────────────

class HitlRequest(BaseModel):
    """
    Published to HITL_REQUEST_CHANNEL when a task requires human sign-off.

    The approval UI (CLI prompt, web dashboard, Slack bot, etc.) subscribes
    to this channel, displays the context to the operator, and sends back a
    HitlResponse.  Until that response arrives the task stays in
    TaskStatus.AWAITING_APPROVAL and the worker slot is not consumed.
    """

    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_id: str
    task_type: str
    context: str
    requested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: datetime | None = None


class HitlResponse(BaseModel):
    """
    Sent back by the human operator (or an automated policy engine) to
    either approve or reject a pending task.
    """

    request_id: str
    task_id: str
    approved: bool
    reviewer_id: str = "human"
    reason: str | None = None
    responded_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ── Node heartbeat ─────────────────────────────────────────────────────────────

class NodeHeartbeat(BaseModel):
    """
    Periodically published by each node so the master can track liveness
    and resource utilisation across the cluster.
    """

    node_id: str
    role: NodeRole
    cpu_percent: float
    ram_used_mb: float
    active_jobs: int
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
