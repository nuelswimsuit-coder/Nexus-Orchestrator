"""Shared manual-order error copy for Polymarket God Mode (BUY vs SELL)."""

from __future__ import annotations

import os
from typing import Literal

from nexus.trading.polymarket_client import get_polymarket_clob_funder_address


def enrich_manual_order_error(err: str | None, side: Literal["BUY", "SELL"]) -> str:
    """
    PolyApiException often returns the same generic string for BUY (no USDC) and SELL
    (no outcome-token balance). We must branch on *request* side first so a red
    "מכירה" click never shows USDC-deposit-only guidance.
    """
    if not err:
        return "Order rejected"
    low = err.lower()
    if "not enough balance" not in low and "balance: 0" not in low:
        return err

    su = (side or "BUY").strip().upper()
    if su not in ("BUY", "SELL"):
        su = "BUY"

    funder = (get_polymarket_clob_funder_address() or "").strip()
    portfolio = (os.getenv("POLYMARKET_PORTFOLIO_ADDRESS") or "").strip()
    addr = f"CLOB maker: {funder[:6]}…{funder[-4]}." if funder and len(funder) >= 10 else ""
    deposit_line = ""
    if funder:
        deposit_line = (
            f"\n\nDeposit USDC to the signing wallet used by this API (full address): {funder}\n"
            "Polymarket: https://polymarket.com/"
        )
    mismatch = ""
    if portfolio and funder and portfolio.lower() != funder.lower():
        mismatch = (
            f"\n\nUI portfolio {portfolio[:6]}…{portfolio[-4]} (POLYMARKET_PORTFOLIO_ADDRESS) ≠ maker above — "
            "the positions table is not the same on-chain account as this API key.\n"
            "Fix A: deposit USDC on the maker address (the one that signs / holds L2 API).\n"
            "Fix B: use the private key for the wallet that already has USDC (the portfolio address).\n"
            "Fix C: set POLYMARKET_SYNC_WALLET_ENV=1 (default) so Nexus overwrites "
            "POLYMARKET_PORTFOLIO_ADDRESS with your signing key — or remove the wrong "
            "POLYMARKET_PORTFOLIO_ADDRESS line if you disabled sync on purpose."
        )

    if su == "SELL":
        return (
            f"{err}\n\n"
            "This was a SELL: CLOB needs outcome-token (share) balance on the maker for this token_id. "
            "It is not asking for USDC. “Balance 0” here usually means the maker wallet does not hold those shares."
            f"\n{addr}{mismatch}\n\n"
            "Fix: use POLYMARKET_RELAYER_KEY for the wallet that actually holds the position, or remove "
            "POLYMARKET_PORTFOLIO_ADDRESS so UI and trading refer to the same account."
            f"{deposit_line}\n\n"
            "מכירה: נדרשות מניות על כתובת ה-maker — לא USDC."
        )

    return (
        f"{err}\n\n"
        "This was a BUY: CLOB spends USDC collateral (and allowance) on the maker wallet."
        f"\n{addr}{mismatch}{deposit_line}\n\n"
        "Set POLYMARKET_API_* (L2) so balance matches the app, or use Polymarket to refresh allowance after deposit.\n\n"
        "קנייה: נדרש USDC על כתובת החתימה / maker."
    )
