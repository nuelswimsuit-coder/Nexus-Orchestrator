#!/usr/bin/env python3
"""
One-off: print relayer-derived EOA, .env signer/portfolio, and Polygon USDC (bridged) balances.

Usage (from repo root):
  python scripts/check_wallet_sync.py

Requires: httpx, eth-account, python-dotenv (already in Nexus env).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from dotenv import load_dotenv

load_dotenv(_REPO / ".env", override=False)

import httpx  # noqa: E402

# Polygon PoS bridged USDC (USDC.e) — common collateral for Polymarket CTF flow
USDC_POLYGON = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

_DEFAULT_RPCS = (
    "https://polygon-bor.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon.drpc.org",
    "https://polygon-rpc.com",
)


def _polygon_rpc_urls() -> list[str]:
    env = (os.getenv("POLYGON_RPC_URL") or "").strip()
    out: list[str] = []
    if env:
        out.append(env)
    for u in _DEFAULT_RPCS:
        if u not in out:
            out.append(u)
    return out


def _pad_addr(addr: str) -> str:
    h = addr.lower().replace("0x", "")
    return h.rjust(64, "0")


def erc20_balance_wei(contract: str, owner: str) -> int | None:
    owner = owner.strip()
    if not owner.startswith("0x") or len(owner) != 42:
        return None
    data = "0x70a08231" + _pad_addr(owner)
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [{"to": contract, "data": data}, "latest"],
    }
    j = None
    last_err: str | None = None
    for rpc in _polygon_rpc_urls():
        try:
            with httpx.Client(timeout=12.0) as c:
                r = c.post(rpc, json=payload)
                r.raise_for_status()
                j = r.json()
                break
        except Exception as exc:
            last_err = str(exc)
            j = None
            continue
    if j is None:
        print(f"RPC error (all endpoints failed): {last_err}")
        return None
    err = j.get("error")
    if err:
        print(f"eth_call error: {err}")
        return None
    res = j.get("result")
    if not res or res == "0x":
        return 0
    return int(res, 16)


def main() -> None:
    sync_env = (os.getenv("POLYMARKET_SYNC_WALLET_ENV") or "1").strip()
    key = (
        (os.getenv("POLYMARKET_RELAYER_KEY") or "").strip()
        or (os.getenv("NEXUS_POLY_PRIVATE_KEY") or "").strip()
        or (os.getenv("POLY_PRIVATE_KEY") or "").strip()
    )
    derived = ""
    if key:
        from nexus.trading.polymarket_client import _signing_key_format_error

        fmt_err = _signing_key_format_error(key)
        if fmt_err:
            print(fmt_err)
        else:
            try:
                from eth_account import Account

                derived = Account.from_key(key).address
            except Exception as exc:
                print(f"Could not derive from POLYMARKET_RELAYER_KEY: {exc}")

    portfolio_from_file = (os.getenv("POLYMARKET_PORTFOLIO_ADDRESS") or "").strip()

    # Import after snapshot: loading nexus.shared.config runs apply_polymarket_wallet_alignment() once.
    from nexus.shared.config import apply_polymarket_wallet_alignment

    apply_polymarket_wallet_alignment()

    signer = (os.getenv("POLYMARKET_SIGNER_ADDRESS") or "").strip()
    portfolio = (os.getenv("POLYMARKET_PORTFOLIO_ADDRESS") or "").strip()

    legacy_portfolio: str | None = None
    if (
        derived
        and portfolio_from_file
        and portfolio_from_file.lower() != derived.lower()
    ):
        legacy_portfolio = portfolio_from_file

    print("=== Polymarket wallet sync check ===")
    print(f"POLYMARKET_SYNC_WALLET_ENV={sync_env!r} (default 1 = signer/portfolio forced to relayer EOA in-process)")
    print(f"POLYGON_RPC candidates: {', '.join(_polygon_rpc_urls()[:4])} ...")
    print(f"Derived from RELAYER_KEY (CLOB signing EOA): {derived or '(no key)'}")
    if legacy_portfolio and sync_env.lower() not in ("0", "false", "no", "off"):
        print(
            f"  (.env had POLYMARKET_PORTFOLIO_ADDRESS={legacy_portfolio} - ignored when sync is on; effective below)"
        )
    print(f"POLYMARKET_SIGNER_ADDRESS (effective):   {signer or '(unset)'}")
    print(f"POLYMARKET_PORTFOLIO_ADDRESS (effective): {portfolio or '(unset)'}")
    if derived and signer and derived.lower() != signer.lower():
        print("WARNING: signer env does not match derived EOA (unless POLYMARKET_ALLOW_FUNDER_ENV_MISMATCH=1).")
    if derived and portfolio and derived.lower() != portfolio.lower():
        print("WARNING: portfolio env does not match derived EOA (set POLYMARKET_SYNC_WALLET_ENV=1 or align .env).")

    addrs: list[tuple[str, str]] = []
    if derived:
        addrs.append(("CLOB signing wallet (use this for deposits / Polymarket balance)", derived))
    if legacy_portfolio and legacy_portfolio.lower() != (derived or "").lower():
        addrs.append(
            (
                "Legacy .env POLYMARKET_PORTFOLIO_ADDRESS (on-chain USDC only; not debited by CLOB if different from signing)",
                legacy_portfolio,
            )
        )
    if signer and all(a[1].lower() != signer.lower() for a in addrs):
        addrs.append(("POLYMARKET_SIGNER_ADDRESS", signer))
    if portfolio and all(a[1].lower() != portfolio.lower() for a in addrs):
        addrs.append(("POLYMARKET_PORTFOLIO_ADDRESS", portfolio))

    print()
    print(f"USDC ({USDC_POLYGON}) balance on Polygon (raw wei, 6 decimals):")
    for label, addr in addrs:
        bal = erc20_balance_wei(USDC_POLYGON, addr)
        if bal is None:
            print(f"  {label}: (n/a)")
        else:
            human = bal / 1_000_000
            print(f"  {label} {addr}: {human:.6f} USDC (wei={bal})")

    if derived and legacy_portfolio and legacy_portfolio.lower() != derived.lower():
        d_bal = erc20_balance_wei(USDC_POLYGON, derived)
        l_bal = erc20_balance_wei(USDC_POLYGON, legacy_portfolio)
        if (
            d_bal is not None
            and l_bal is not None
            and d_bal == 0
            and l_bal > 0
        ):
            print()
            print(
                "ACTION: On-chain USDC is on the legacy portfolio address, not on the CLOB signing wallet. "
                "Either deposit to Polymarket / send USDC to the signing address above, or set "
                "POLYMARKET_RELAYER_KEY to the private key of the wallet that holds your Polymarket funds."
            )

    print()
    print("Done.")


if __name__ == "__main__":
    main()
