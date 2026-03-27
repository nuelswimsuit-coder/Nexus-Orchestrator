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

from nexus.shared.schemas import FleetAuditResults, NodeRole

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
    # Phase 4 extended hardware
    motherboard: str = "N/A"
    cpu_temp_c: float = -1.0
    display_name: str = ""


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


# ── Cluster health (async probes for dashboard grid) ────────────────────────────


class ClusterHealthNode(BaseModel):
    """One fleet node with Redis read latency and UI labels."""

    node_id: str
    role: NodeRole
    online: bool
    status: str = Field(description="ok | degraded | offline")
    probe_latency_ms: float = Field(description="Redis GET timing for this node's heartbeat key")
    cpu_percent: float
    ram_used_mb: float
    active_jobs: int
    last_seen: datetime
    local_ip: str = "unknown"
    cpu_model: str = "unknown"
    gpu_model: str = "N/A"
    ram_total_mb: float = 0.0
    os_info: str = "unknown"
    display_label: str = Field(
        description="Short operator label, e.g. מחשב מאסטר עובד ומנהל בהתאמה / לפטופ לינוקס עובד"
    )
    # Phase 4 extended hardware
    motherboard: str = "N/A"
    cpu_temp_c: float = -1.0
    display_name: str = ""


class TargetHeatCell(BaseModel):
    id: str
    label: str
    intensity: float = Field(ge=0.0, le=100.0, description="0–100 heat for UI")


class ClusterHealthResponse(BaseModel):
    redis_ok: bool
    redis_ping_ms: float | None = None
    nodes: list[ClusterHealthNode]
    workers_online: int
    swarm_activity: list[str]
    targets: list[TargetHeatCell]
    timestamp: datetime


# ── Fleet assets (Telefix managed_groups × Redis mapper counters) ──────────────


class FleetAssetRow(BaseModel):
    """One managed Telegram group with scraped member aggregates."""

    group_id: str
    title: str
    member_count: int
    premium_members: int
    session_owner: str
    status: str = "MONITORING"
    last_automation: str | None = None


class FleetAssetsResponse(BaseModel):
    groups: list[FleetAssetRow]
    total_managed_members: int
    total_premium_members: int
    latest_audit: FleetAuditResults | None = None
    db_available: bool
    queried_at: datetime


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
