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
    key = (
        (os.getenv("POLYMARKET_RELAYER_KEY") or "").strip()
        or (os.getenv("NEXUS_POLY_PRIVATE_KEY") or "").strip()
        or (os.getenv("POLY_PRIVATE_KEY") or "").strip()
    )
    derived = ""
    if key:
        try:
            from eth_account import Account

            derived = Account.from_key(key).address
        except Exception as exc:
            print(f"Could not derive from POLYMARKET_RELAYER_KEY: {exc}")

    signer = (os.getenv("POLYMARKET_SIGNER_ADDRESS") or "").strip()
    portfolio = (os.getenv("POLYMARKET_PORTFOLIO_ADDRESS") or "").strip()

    print("=== Polymarket wallet sync check ===")
    print(f"POLYGON_RPC candidates: {', '.join(_polygon_rpc_urls()[:4])} ...")
    print(f"Derived from RELAYER_KEY: {derived or '(no key)'}")
    print(f"POLYMARKET_SIGNER_ADDRESS:   {signer or '(unset)'}")
    print(f"POLYMARKET_PORTFOLIO_ADDRESS: {portfolio or '(unset)'}")
    if derived and signer and derived.lower() != signer.lower():
        print("WARNING: signer env does not match derived EOA (unless POLYMARKET_ALLOW_FUNDER_ENV_MISMATCH=1).")
    if derived and portfolio and derived.lower() != portfolio.lower():
        print("WARNING: portfolio env does not match derived EOA (set POLYMARKET_SYNC_WALLET_ENV=1 or align .env).")

    addrs: list[tuple[str, str]] = []
    if derived:
        addrs.append(("derived (signing)", derived))
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

    print()
    print("Done.")


if __name__ == "__main__":
    main()
