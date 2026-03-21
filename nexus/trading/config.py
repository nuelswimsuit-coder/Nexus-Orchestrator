"""
nexus/trading/config.py — Production Trading Configuration
==========================================================

This deployment is configured for LIVE execution by default.
"""

from __future__ import annotations

# ── Trading mode switch ────────────────────────────────────────────────────────
#
#  SIMULATION_MODE True  → paper trades only (no on-chain orders).
#  SIMULATION_MODE False → live CLOB execution (requires wallet + POLY_* keys).
#
SIMULATION_MODE: bool = True
PAPER_TRADING: bool = SIMULATION_MODE

# ── Execution sizing / history parameters ─────────────────────────────────────

# Simulated order size in USD per virtual trade
PAPER_TRADING_AMOUNT_USD: float = 2.0

# Redis key where virtual trade history is stored (LPUSH, newest-first)
PAPER_TRADING_REDIS_KEY: str = "nexus:paper_trading:history"

# Set by POST /api/prediction/manual-override — blocks new Polymarket orders
PREDICTION_MANUAL_HALT_KEY: str = "nexus:prediction:manual_halt"

# Maximum number of virtual trades to keep in Redis history
PAPER_TRADING_MAX_HISTORY: int = 100

# Minimum seconds between consecutive virtual trades (prevents signal spam)
PAPER_TRADING_COOLDOWN_S: int = 30
