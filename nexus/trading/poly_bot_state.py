"""
Redis keys for Nexus Poly Trader — shared by master service, worker task, and API.
"""

from __future__ import annotations

POLY_BOT_BTC_KEY: str = "nexus:poly:btc_spot"
POLY_BOT_PNL_KEY: str = "nexus:poly:pnl"
POLY_BOT_STATUS_KEY: str = "nexus:poly:session_status"
POLY_BOT_OPEN_POS_KEY: str = "nexus:poly:open_position"

STATUS_TTL_S: int = 120
PNL_TTL_S: int = 600
BTC_TTL_S: int = 60
