"""
Moltbot core module adapter.

Keeps canonical payload construction for bot.moltbot dispatches.
"""

from __future__ import annotations

from typing import Any


def build_moltbot_parameters(
    *,
    session_file: str,
    action: str = "launch_scrape",
    query: str = "",
    max_items: int = 100,
) -> dict[str, Any]:
    """
    Build normalized parameters for the Moltbot task handler.
    """
    return {
        "session_file": session_file,
        "action": action,
        "query": query,
        "max_items": max(1, min(int(max_items), 500)),
    }
