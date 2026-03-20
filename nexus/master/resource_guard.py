"""
Backward-compatibility shim.

ResourceGuard now lives in nexus/worker/resource_guard.py (shared between
master and worker nodes per the PRD directory structure).

All existing imports of `nexus.master.resource_guard` continue to work
without modification.
"""

from nexus.worker.resource_guard import (  # noqa: F401
    ResourceGuard,
    apply_low_priority,
)
