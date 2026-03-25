"""
OpenClaw core module adapter.

This module centralizes OpenClaw task-building logic used by API surfaces.
"""

from __future__ import annotations

from typing import Any


def build_openclaw_parameters(
    *,
    mode: str,
    query: str,
    project_id: str,
    max_leads: int = 50,
    location: str = "",
) -> dict[str, Any]:
    """
    Build normalized parameters for the OpenClaw task handler.
    """
    payload: dict[str, Any] = {
        "mode": mode,
        "query": query,
        "project_id": project_id,
        "max_leads": max(1, min(int(max_leads), 200)),
    }
    if location.strip():
        payload["location"] = location.strip()
    return payload
