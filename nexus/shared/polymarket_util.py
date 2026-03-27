"""
Polymarket CLOB preflight: refresh allowance cache and optional on-chain USDC approve.

Requires ``py-clob-client`` for live clients. On-chain approve is optional:
``pip install web3`` and ``POLYMARKET_AUTO_APPROVE_USDC=1`` plus MATIC for gas.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import structlog

log = structlog.get_logger(__name__)

_POLYGON_RPC_DEFAULT = "https://polygon-bor.publicnode.com"


async def preflight_live_clob_order(client: Any) -> None:
    """
    Before a live CLOB order: refresh the exchange's view of USDC balance/allowance.

    Polymarket's API can return stale allowance; ``update_balance_allowance`` syncs state.
    Optionally submits an on-chain ``approve`` when enabled (see ``try_auto_approve_usdc_polygon``).
    """
    fn = getattr(client, "sync_collateral_allowance_async", None)
    if fn is None:
        return
    await fn()
    loop = asyncio.get_event_loop()
    clob = getattr(client, "clob", None)
    await loop.run_in_executor(None, try_auto_approve_usdc_polygon, clob)


def try_auto_approve_usdc_polygon(clob: Any) -> bool:
    """
    If allowance is still zero but you hold USDC, optionally approve the exchange contract.

    Set ``POLYMARKET_AUTO_APPROVE_USDC=1``, install ``web3``, fund the key with MATIC on Polygon.
    """
    if clob is None:
        return False
    if (os.getenv("POLYMARKET_AUTO_APPROVE_USDC") or "").strip().lower() not in ("1", "true", "yes", "on"):
        return False
    try:
        from web3 import Web3
    except ImportError:
        log.warning("polymarket.auto_approve_skipped", reason="web3 not installed (pip install web3)")
        return False

    pk = (
        (os.getenv("POLYMARKET_RELAYER_KEY") or "").strip()
        or (os.getenv("NEXUS_POLY_PRIVATE_KEY") or "").strip()
        or (os.getenv("POLY_PRIVATE_KEY") or "").strip()
    )
    if not pk:
        return False

    try:
        from eth_account import Account
    except ImportError:
        return False

    rpc = (os.getenv("POLYGON_RPC_URL") or _POLYGON_RPC_DEFAULT).strip()
    w3 = Web3(Web3.HTTPProvider(rpc))
    if not w3.is_connected():
        log.warning("polymarket.auto_approve_skipped", reason="Polygon RPC unreachable")
        return False

    acct = Account.from_key(pk)
    spender = clob.get_exchange_address()
    token_addr = w3.to_checksum_address(clob.get_collateral_address())

    erc20_abi = [
        {
            "constant": False,
            "inputs": [
                {"name": "_spender", "type": "address"},
                {"name": "_value", "type": "uint256"},
            ],
            "name": "approve",
            "outputs": [{"name": "", "type": "bool"}],
            "type": "function",
        },
        {
            "constant": True,
            "inputs": [{"name": "_owner", "type": "address"}],
            "name": "balanceOf",
            "outputs": [{"name": "balance", "type": "uint256"}],
            "type": "function",
        },
        {
            "constant": True,
            "inputs": [
                {"name": "_owner", "type": "address"},
                {"name": "_spender", "type": "address"},
            ],
            "name": "allowance",
            "outputs": [{"name": "", "type": "uint256"}],
            "type": "function",
        },
    ]
    contract = w3.eth.contract(address=token_addr, abi=erc20_abi)
    owner = w3.to_checksum_address(acct.address)
    spend = w3.to_checksum_address(spender)

    try:
        allowance = contract.functions.allowance(owner, spend).call()
        bal = contract.functions.balanceOf(owner).call()
    except Exception as exc:
        log.warning("polymarket.auto_approve_read_failed", error=str(exc))
        return False

    if bal == 0:
        log.info("polymarket.auto_approve_skip", reason="zero USDC balance on Polygon for signer")
        return False
    if allowance > 10**12:
        return False

    max_u256 = 2**256 - 1
    nonce = w3.eth.get_transaction_count(owner)
    gas_price = w3.eth.gas_price

    approve_fn = contract.functions.approve(spend, max_u256)
    gas_est = approve_fn.estimate_gas({"from": owner})
    tx = approve_fn.build_transaction(
        {
            "from": owner,
            "nonce": nonce,
            "gas": int(gas_est * 1.25),
            "gasPrice": gas_price,
            "chainId": 137,
        }
    )

    signed = acct.sign_transaction(tx)
    raw = getattr(signed, "rawTransaction", None) or getattr(signed, "raw_transaction", None)
    if raw is None:
        return False
    try:
        h = w3.eth.send_raw_transaction(raw)
    except Exception as exc:
        log.warning("polymarket.auto_approve_send_failed", error=str(exc))
        return False
    log.info("polymarket.usdc_approve_tx_sent", tx_hash=h.hex(), spender=spend)
    return True
