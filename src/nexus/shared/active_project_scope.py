"""
Active project scope — shared between API, workers, and nexus_core CLI.

Redis key ``nexus:active_project`` holds JSON metadata for the dashboard
and data-endpoint filtering. Falls back to ``global_mission`` when unset.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

REDIS_ACTIVE_PROJECT_KEY = "nexus:active_project"
GLOBAL_MISSION_REDIS_KEY = "global_mission"

# Canonical presets surfaced in the dashboard switcher
KNOWN_PROJECT_PRESETS: list[dict[str, str]] = [
    {
        "project_id": "nuel",
        "display_name": "NUEL",
        "project_type": "ecommerce_swimwear",
    },
    {
        "project_id": "management_ahu",
        "display_name": "Management Ahu",
        "project_type": "operations_legal",
    },
]


def normalize_project_id(raw: str) -> str:
    s = (raw or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s or "default"


def resolve_project_type(project_id: str, display_name: str | None = None) -> str:
    pid = normalize_project_id(project_id)
    dn = (display_name or "").lower()
    if pid == "nuel" or "nuel" in dn:
        return "ecommerce_swimwear"
    # default / telefix-style ids keep legacy Telefix dashboard behaviour
    if pid in (
        "default",
        "management_ahu",
        "telefix",
        "mangement_ahu",
        "managementahu",
    ):
        return "operations_legal"
    if "management ahu" in dn or "mangement ahu" in dn:
        return "operations_legal"
    return "generic"


def default_display_name(project_id: str, project_type: str) -> str:
    pid = normalize_project_id(project_id)
    if project_type == "ecommerce_swimwear":
        return "NUEL"
    if project_type == "operations_legal":
        return "Management Ahu"
    for p in KNOWN_PROJECT_PRESETS:
        if p["project_id"] == pid:
            return p["display_name"]
    return project_id.strip() or "Default"


def scrape_status_redis_key(project_id: str) -> str:
    """Per-project scrape status (telegram.auto_scrape)."""
    return f"nexus:scrape:status:{normalize_project_id(project_id)}"


LEGACY_SCRAPE_STATUS_KEY = "nexus:scrape:status"


def is_operations_legal_context(meta: dict[str, Any]) -> bool:
    return meta.get("project_type") == "operations_legal"


def is_ecommerce_swimwear_context(meta: dict[str, Any]) -> bool:
    return meta.get("project_type") == "ecommerce_swimwear"


def build_active_project_meta(project_id: str, display_name: str | None = None) -> dict[str, Any]:
    pid = normalize_project_id(project_id)
    ptype = resolve_project_type(pid, display_name)
    dn = display_name.strip() if display_name else default_display_name(pid, ptype)
    return {
        "project_id": pid,
        "display_name": dn,
        "project_type": ptype,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def parse_active_project_json(raw: str | None) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        data = json.loads(raw)
        if isinstance(data, dict) and data.get("project_id"):
            return data
    except Exception:
        return None
    return None
