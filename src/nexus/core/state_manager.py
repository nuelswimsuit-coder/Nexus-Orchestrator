"""
Centralised domain state — Pydantic-validated views over Redis keys.

Use :class:`StateManager` for ``global_mission`` and related orchestration fields
so writers (core) and readers (UI, workers) share one contract.
"""

from __future__ import annotations

import sys
from typing import Any

from pydantic import BaseModel, Field

_GREEN_BOLD = "\033[1;32m"
_RESET = "\033[0m"


class GlobalMissionState(BaseModel):
    """Redis ``global_mission`` plus optional task metadata."""

    mission: str = ""
    task_type: str = ""
    priority: int = 0
    project_id: str = ""
    meta: dict[str, Any] = Field(default_factory=dict)


class StateManager:
    """Async helpers over Redis string keys (decode_responses clients)."""

    def __init__(self, mission_key: str = "global_mission") -> None:
        self._mission_key = mission_key

    async def get_mission(self, redis: Any) -> GlobalMissionState:
        print(
            f"{_GREEN_BOLD}[DATABASE] CONNECTED ✓  StateManager online — key={self._mission_key!r}{_RESET}",
            flush=True,
        )
        raw = await redis.get(self._mission_key)
        if raw is None:
            return GlobalMissionState()
        text = raw.decode() if isinstance(raw, (bytes, bytearray)) else str(raw)
        text = text.strip()
        return GlobalMissionState(mission=text, project_id=text)

    async def set_mission(self, redis: Any, state: GlobalMissionState) -> None:
        await redis.set(self._mission_key, state.mission or state.project_id)
