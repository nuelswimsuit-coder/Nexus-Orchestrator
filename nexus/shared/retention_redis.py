"""Redis keys for Retention Guardian (group member monitoring + dashboard)."""

from __future__ import annotations

RETENTION_HEALTH_SNAPSHOT_KEY = "nexus:retention:health:snapshot"
RETENTION_HEALTH_TTL_S = 8 * 60 * 60  # refreshed each run (~4h); keep 8h buffer
