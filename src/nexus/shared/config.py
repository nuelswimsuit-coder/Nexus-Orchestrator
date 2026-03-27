"""
Compatibility entrypoint — canonical settings live in ``nexus.shared.config``.
Import this module or ``nexus.shared.config``; both load the same ``settings`` singleton.
"""

from nexus.shared.config import (  # noqa: F401
    Settings,
    apply_polymarket_wallet_alignment,
    log_polymarket_wallet_mismatch_at_startup,
    settings,
)

__all__ = [
    "Settings",
    "apply_polymarket_wallet_alignment",
    "log_polymarket_wallet_mismatch_at_startup",
    "settings",
]
