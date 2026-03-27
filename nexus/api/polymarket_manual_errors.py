"""Shared manual-order error copy for Polymarket God Mode (BUY vs SELL)."""

from __future__ import annotations

import os
from typing import Literal

from nexus.trading.polymarket_client import get_polymarket_clob_funder_address

# Bump when changing enrich copy; exposed on GET /api/polymarket/dashboard.json as manual_order_error_enrich.
MANUAL_ORDER_ENRICH_REV = "v4"

# Appended to every enriched balance error — if you never see this in the UI, the client is not
# hitting the same Python codebase that defines this module (stale worker, wrong host, or old venv).
_ENRICH_FOOTER = f"\n\n— nexus:polymarket-enrich@{MANUAL_ORDER_ENRICH_REV}"


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
            "the positions table is not the same on-chain account as this API key."
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
            f"{_ENRICH_FOOTER}"
        )

    return (
        f"{err}\n\n"
        "This was a BUY: CLOB spends USDC collateral (and allowance) on the maker wallet."
        f"\n{addr}{mismatch}{deposit_line}\n\n"
        "Set POLYMARKET_API_* (L2) so balance matches the app, or use Polymarket to refresh allowance after deposit.\n\n"
        "קנייה: נדרש USDC על כתובת החתימה / maker."
        f"{_ENRICH_FOOTER}"
    )
