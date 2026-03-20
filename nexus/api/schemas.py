"""
API-layer Pydantic models.

These are the request/response bodies for the HTTP API.  They are kept
separate from nexus/shared/schemas.py (the wire contract between master and
workers) to allow the API surface to evolve independently.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from nexus.shared.schemas import NodeRole

# ── Cluster status ─────────────────────────────────────────────────────────────

class NodeStatus(BaseModel):
    node_id: str
    role: NodeRole
    cpu_percent: float
    ram_used_mb: float
    active_jobs: int
    last_seen: datetime
    online: bool
    # Phase 3 hardware identity
    local_ip: str = "unknown"
    cpu_model: str = "unknown"
    gpu_model: str = "N/A"
    ram_total_mb: float = 0.0
    active_tasks_count: int = 0
    os_info: str = "unknown"


class ResourceCaps(BaseModel):
    cpu_cap_percent: float
    ram_cap_mb: float


class QueueStats(BaseModel):
    queue_name: str
    pending_jobs: int


class ClusterStatusResponse(BaseModel):
    nodes: list[NodeStatus]
    master_resource_caps: ResourceCaps
    queues: list[QueueStats]
    timestamp: datetime


# ── HITL ───────────────────────────────────────────────────────────────────────

class HitlPendingItem(BaseModel):
    request_id: str
    task_id: str
    task_type: str
    context: str
    requested_at: datetime
    expires_at: datetime | None = None


class HitlPendingResponse(BaseModel):
    items: list[HitlPendingItem]
    total: int


class HitlResolveRequest(BaseModel):
    request_id: str = Field(..., description="The request_id from the pending HITL item")
    approved: bool = Field(..., description="True to approve and proceed, False to reject")
    reviewer_id: str = Field(default="dashboard", description="Identity of the approver")
    reason: str | None = Field(default=None, description="Optional note from the reviewer")


class HitlResolveResponse(BaseModel):
    request_id: str
    task_id: str
    approved: bool
    reviewer_id: str
    responded_at: datetime
    message: str


# ── Generic error ──────────────────────────────────────────────────────────────

class ErrorResponse(BaseModel):
    detail: str
    extra: dict[str, Any] | None = None
