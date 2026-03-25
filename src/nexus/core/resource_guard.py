"""
Backward-compatibility shim.

ResourceGuard now lives in nexus/agents/resource_guard.py (shared between
master and worker nodes per the PRD directory structure).

All existing imports of `nexus.core.resource_guard` continue to work
without modification.
"""

from nexus.agents.resource_guard import (  # noqa: F401
    CPU_CAP,
    ResourceGuard,
    apply_low_priority,
)
