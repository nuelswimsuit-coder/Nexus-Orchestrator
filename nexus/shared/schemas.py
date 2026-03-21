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


# ── Worker capability tags ─────────────────────────────────────────────────────
# Tasks declare which capabilities they require; workers declare which they
# provide.  The dispatcher only routes a task to workers whose declared
# capabilities are a superset of the task's required_capabilities set.
#
# Add new tags here as the cluster grows.  Tags are plain strings so they
# can be extended without a schema migration.
#
# Examples: "linux-only", "windows-only", "high-ram", "gpu", "docker"

class WorkerCapability(str, Enum):
    LINUX = "linux-only"
    WINDOWS = "windows-only"
    HIGH_RAM = "high-ram"
    GPU = "gpu"
    DOCKER = "docker"
    ANY = "any"          # Default — any worker may execute this task.


# ── Core task models ───────────────────────────────────────────────────────────

class TaskPayload(BaseModel):
    """
    Represents a unit of work dispatched by the Master to a Worker.

    New fields (Phase 2 refactor)
    ------------------------------
    project_id            : Groups tasks by project for filtering, billing, and
                            audit.  Defaults to "default".
    required_capabilities : Set of WorkerCapability tags the executing worker
                            must declare.  The dispatcher uses this for routing.
                            An empty set (or {"any"}) means any worker qualifies.
    injected_secrets      : Populated by the Vault at dispatch time — never set
                            by callers directly.  Contains decrypted key/value
                            pairs that the handler needs (e.g. API tokens).
                            Exists only in-memory on the worker; never persisted.
    """

    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str = Field(
        ...,
        description="Dot-separated handler name, e.g. 'file.process' or 'llm.summarise'",
    )
    parameters: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=5, ge=1, le=10)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # ── Project context ────────────────────────────────────────────────────────
    project_id: str = Field(
        default="default",
        description="Project this task belongs to — used for routing, audit, and billing",
    )

    # ── Capability-based routing ───────────────────────────────────────────────
    # The dispatcher checks that the target worker's declared capabilities
    # are a superset of this set before enqueuing.  Use WorkerCapability tags.
    required_capabilities: list[str] = Field(
        default_factory=list,
        description="Worker must declare all listed capabilities to receive this task",
    )

    # ── Secrets (injected at dispatch time by the Vault) ───────────────────────
    # Callers must NOT set this field — it is populated by Vault.inject().
    # Workers read secrets from here; the field is excluded from logs.
    injected_secrets: dict[str, str] = Field(
        default_factory=dict,
        description="Decrypted secrets injected by the master Vault at dispatch time",
        exclude=True,       # excluded from model_dump() by default → never logged
        repr=False,
    )

    # ── HITL metadata ──────────────────────────────────────────────────────────
    requires_approval: bool = False
    approval_context: str | None = Field(
        default=None,
        description="Human-readable description shown to the approver in the HITL UI",
    )

    # ── ARQ job lifetime (optional) ────────────────────────────────────────────
    # When set, overrides TASK_DEFAULT_TIMEOUT for this enqueue only (long-running
    # sessions e.g. Polymarket bot websocket loop on the Linux worker).
    job_expires_seconds: int | None = Field(
        default=None,
        ge=60,
        description="ARQ _expires for this job; omit to use TASK_DEFAULT_TIMEOUT",
    )

    model_config = {"frozen": True}

    def model_dump_for_wire(self) -> dict[str, Any]:
        """
        Serialise the payload for transmission over Redis / ARQ.

        Includes injected_secrets (needed by the worker) but marks them
        clearly so they are never accidentally logged.  The worker's
        execute_task handler receives the full dict including secrets.
        """
        return self.model_dump(exclude=set())  # include all fields including secrets


class TaskResult(BaseModel):
    """
    Returned by a Worker after it finishes (or fails) a TaskPayload.
    """

    task_id: str
    worker_id: str
    status: TaskStatus
    output: Any | None = None
    error: str | None = None
    started_at: datetime | None = None
    finished_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    duration_seconds: float | None = None
    project_id: str = "default"


# ── HITL approval models ───────────────────────────────────────────────────────

class HitlRequest(BaseModel):
    """
    Published to HITL_REQUEST_CHANNEL when a task requires human sign-off.
    """

    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_id: str
    task_type: str
    project_id: str = "default"
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
    Periodically published by each node so the master can track liveness,
    resource utilisation, and hardware identity across the cluster.

    Phase 3 additions
    -----------------
    local_ip          : LAN IP address of the node (for the dashboard HUD).
    cpu_model         : Human-readable CPU model string (e.g. "AMD Ryzen 9 5900X").
    gpu_model         : GPU model string, or "N/A" if no GPU / detection failed.
    ram_total_mb      : Total installed RAM in MB.
    active_tasks_count: Number of tasks currently executing on this node.
    os_info           : OS platform string (e.g. "Windows 11", "Ubuntu 22.04").
    """

    node_id: str
    role: NodeRole
    cpu_percent: float
    ram_used_mb: float
    active_jobs: int
    # Capabilities this worker declares — used by the dispatcher for routing.
    capabilities: list[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # ── Hardware identity (Phase 3) ────────────────────────────────────────────
    local_ip: str = Field(default="unknown")
    cpu_model: str = Field(default="unknown")
    gpu_model: str = Field(default="N/A")
    ram_total_mb: float = Field(default=0.0)
    active_tasks_count: int = Field(default=0)
    os_info: str = Field(default="unknown")


# ── Fleet audit / mapper → dashboard ───────────────────────────────────────────


class FleetScanPhase(str, Enum):
    """Lifecycle phase for fleet-wide Telegram mapper / scraper runs."""

    STARTED = "started"
    PROGRESS = "progress"
    ENDED = "ended"


class FleetScanEvent(BaseModel):
    """
    Published to Redis channel ``nexus:fleet:scan`` and mirrored to
    ``nexus:fleet:scan:status`` so HTTP/SSE clients can show a progress bar.
    """

    phase: FleetScanPhase
    task_id: str | None = None
    task_type: str | None = None
    detail: str = ""
    groups_found_delta: int | None = Field(
        default=None,
        description="Optional: groups discovered in this event (mapper batch)",
    )
    managed_members_total: int | None = None
    premium_members_total: int | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class FleetAuditGroupEntry(BaseModel):
    """One managed or discovered group in a fleet audit snapshot."""

    group_id: str | int | None = None
    title: str = ""
    username: str | None = None
    link: str | None = None
    owner_session: str = ""
    member_count: int = 0
    premium_members: int = 0


class FleetAuditResults(BaseModel):
    """
    Full audit snapshot: stored in Redis (``nexus:fleet:audit:latest``) and
    optionally appended to SQLite table ``nexus_fleet_audit`` via telefix_bridge.
    """

    run_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_id: str | None = None
    worker_id: str | None = None
    source: str = Field(default="mapper", description="mapper | worker | api")
    groups: list[FleetAuditGroupEntry] = Field(default_factory=list)
    scanned_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    total_managed_members: int = 0
    total_premium_members: int = 0
