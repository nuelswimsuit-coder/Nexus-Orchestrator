"""Shared manual-order error copy for Polymarket God Mode (BUY vs SELL)."""

from __future__ import annotations

import os
from typing import Literal

from nexus.trading.polymarket_client import get_polymarket_clob_funder_address


def enrich_manual_order_error(err: str | None, side: Literal["BUY", "SELL"]) -> str:
    """
    When CLOB returns zero balance, append operator guidance (signing wallet vs portfolio UI).

    BUY: collateral is USDC on the signing wallet. SELL: CLOB checks outcome-token (shares)
    balance for this token_id on the signing wallet — not USDC.
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
    addr_hint = ""
    if funder and len(funder) >= 10:
        addr_hint = f" CLOB maker (signing wallet): {funder[:6]}…{funder[-4]}."
    mismatch = ""
    if portfolio and funder and portfolio.lower() != funder.lower():
        mismatch = (
            " POLYMARKET_PORTFOLIO_ADDRESS (UI) differs from the CLOB signing/maker address — "
            "the bot/API only sees positions and collateral on the signing wallet."
        )
    if su == "SELL":
        return (
            f"{err}\n\n"
            "מכירה (SELL): CLOB דורש יתרת מניות (outcome tokens) של אותו token על כתובת החתימה — לא USDC. "
            "אם הפוזיציה מוצגת בחשבון אחר (UI), המפתח הנוכחי לא מחזיק את אותן המניות ולכן המכירה נכשלת."
            f"{addr_hint}{mismatch}\n\n"
            "SELL: Use the same Polymarket account as POLYMARKET_RELAYER_KEY / POLYMARKET_SIGNER_ADDRESS, "
            "or export/switch keys so the signing wallet holds those shares."
        )
    return (
        f"{err}\n\n"
        "CLOB has no USDC (or allowance) for the wallet that signs orders "
        "(POLYMARKET_RELAYER_KEY must derive POLYMARKET_SIGNER_ADDRESS; that address must hold USDC on Polymarket)."
        f"{addr_hint}{mismatch}\n\n"
        "אין USDC בכתובת החתימה — הפקד ל־Polymarket על אותה כתובת או עדכן מפתח/כתובת לחשבון הממומן."
    )
