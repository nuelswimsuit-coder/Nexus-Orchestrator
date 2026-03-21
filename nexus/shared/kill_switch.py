"""
NEXUS emergency kill-switch — single module for full-system halt.

Fast path (milliseconds)
------------------------
* Redis ``SYSTEM_STATE:PANIC``, manual prediction halt, scalper halt key
* Pub/Sub: ``TERMINATE`` + ``FORCE_STOP`` on ``nexus:system:control``
* Clear ultimate / poly5m pending virtual settlements

Slow path (async completion)
----------------------------
* Flatten Polymarket exposure (open orders, tracked bot position)
* Optional USDC evacuation to ``NEXUS_SAFE_HAVEN_WALLET`` (requires ``web3``)
* Wipe trading API keys from **this process** ``os.environ``
* Telegram: SYSTEM HALTED — SECURE report

Environment
-----------
TELEGRAM_ADMIN_USER_ID      Telegram user id allowed for /terminate_nexus_now
NEXUS_KILL_SWITCH_API_TOKEN If set, POST /api/system/kill-switch requires header
                            X-Nexus-Kill-Auth matching this value.
NEXUS_SAFE_HAVEN_WALLET     0x destination for optional evacuation
NEXUS_KILL_SWITCH_EVACUATE  Set to 1/true to attempt on-chain USDC sweep (best-effort)
NEXUS_USDC_TOKEN_POLYGON    ERC20 contract (default: bridged USDC.e on Polygon)
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from typing import Any

import structlog

log = structlog.get_logger(__name__)

# ── Redis / pubsub (must match worker listener + system router) ───────────────
PANIC_KEY: str = "SYSTEM_STATE:PANIC"
PANIC_META: str = "SYSTEM_STATE:PANIC_META"
PANIC_CHANNEL: str = "nexus:system:control"

MSG_TERMINATE: str = "TERMINATE"
MSG_FORCE_STOP: str = "FORCE_STOP"
MSG_RESUME: str = "RESUME"

# Stops API/Master scalper asyncio loops when set to "1"
KILL_SWITCH_SCALPER_HALT_KEY: str = "nexus:kill_switch:scalper_halt"
KILL_SWITCH_REPORT_META_KEY: str = "nexus:kill_switch:last_report"

# Ultimate scalper pending (virtual + live coordination)
SCALPER_PENDING_KEY: str = "nexus:scalper:pending_settlements"
POLY5M_PENDING_KEY: str = "nexus:poly5m:pending"

# Secrets removed from process memory after trigger (names only, never values)
_ENV_SECRET_KEYS: tuple[str, ...] = (
    "POLYMARKET_RELAYER_KEY",
    "NEXUS_POLY_PRIVATE_KEY",
    "POLY_PRIVATE_KEY",
    "POLYMARKET_API_KEY",
    "POLYMARKET_API_SECRET",
    "POLYMARKET_API_PASSPHRASE",
    "POLY_BUILDER_API_KEY",
    "POLY_BUILDER_SECRET",
    "POLY_BUILDER_PASSPHRASE",
    "GEMINI_API_KEY",
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "TELEGRAM_BOT_TOKEN",
    "TELEFIX_BOT_TOKEN",
)


def kill_switch_api_token_expected() -> str:
    return (os.getenv("NEXUS_KILL_SWITCH_API_TOKEN") or "").strip()


def verify_kill_switch_http_auth(header_value: str | None) -> bool:
    expected = kill_switch_api_token_expected()
    if not expected:
        return True
    got = (header_value or "").strip()
    return bool(got) and got == expected


async def engage_immediate(
    redis: Any,
    *,
    reason: str,
    source: str,
) -> dict[str, Any]:
    """
    Sub-10ms: panic flag, halts, pending queue wipe, dual pub/sub kill messages.
    """
    t0 = time.monotonic()
    activated_at = datetime.now(timezone.utc).isoformat()

    from nexus.trading.config import PREDICTION_MANUAL_HALT_KEY
    from nexus.trading.wallet_manager import REDIS_BRAKE_KEY

    await redis.set(PANIC_KEY, "true")
    await redis.set(
        PANIC_META,
        json.dumps({
            "activated_at": activated_at,
            "reason": reason,
            "activated_by": source,
            "protocol": "NEXUS_KILL_SWITCH",
        }),
    )
    await redis.set(PREDICTION_MANUAL_HALT_KEY, "1")
    await redis.set(KILL_SWITCH_SCALPER_HALT_KEY, "1")
    await redis.set(REDIS_BRAKE_KEY, "kill_switch")
    await redis.delete(SCALPER_PENDING_KEY)
    await redis.delete(POLY5M_PENDING_KEY)

    await redis.publish(PANIC_CHANNEL, MSG_TERMINATE)
    await redis.publish(PANIC_CHANNEL, MSG_FORCE_STOP)

    elapsed_ms = round((time.monotonic() - t0) * 1000)
    log.critical(
        "kill_switch_immediate_engaged",
        source=source,
        reason=reason,
        elapsed_ms=elapsed_ms,
    )
    return {
        "status": "KILL_SWITCH_PHASE1",
        "activated_at": activated_at,
        "elapsed_ms": elapsed_ms,
    }


def wipe_local_trading_env() -> list[str]:
    """Clear configured secret keys from this process only (not other workers)."""
    cleared: list[str] = []
    for k in _ENV_SECRET_KEYS:
        if k in os.environ:
            os.environ.pop(k, None)
            cleared.append(k)
    if cleared:
        log.warning("kill_switch_env_cleared", keys=cleared)
    return cleared


async def _collect_worker_ids(redis: Any) -> list[str]:
    out: list[str] = []
    try:
        cur = 0
        while True:
            cur, keys = await redis.scan(cur, match="nexus:heartbeat:*", count=200)
            for k in keys:
                out.append(str(k).replace("nexus:heartbeat:", ""))
            if cur == 0:
                break
    except Exception as exc:
        log.debug("kill_switch_heartbeat_scan_failed", error=str(exc))
    return sorted(set(out))


async def _cancel_open_clob_orders(client: Any) -> int:
    """Best-effort cancel using py-clob-client surface (varies by version)."""
    clob = getattr(client, "clob", None) or getattr(client, "_clob", None)
    if clob is None:
        return 0
    cancelled = 0
    for name in ("cancel_all", "cancel_all_orders", "cancel_orders"):
        fn = getattr(clob, name, None)
        if callable(fn):
            try:
                await asyncio.to_thread(fn)
                cancelled += 1
                log.info("kill_switch_clob_cancel_batch", method=name)
                break
            except Exception as exc:
                log.warning("kill_switch_clob_cancel_failed", method=name, error=str(exc))
    # Fallback: fetch orders and cancel individually
    if cancelled == 0:
        get_orders = getattr(clob, "get_orders", None)
        cancel = getattr(clob, "cancel", None) or getattr(clob, "cancel_order", None)
        if callable(get_orders) and callable(cancel):
            try:
                orders = await asyncio.to_thread(get_orders) or []
                if not isinstance(orders, list):
                    orders = list(orders) if orders else []
                for o in orders:
                    oid = None
                    if isinstance(o, dict):
                        oid = o.get("id") or o.get("orderID") or o.get("order_id")
                    if oid:
                        try:
                            await asyncio.to_thread(cancel, oid)
                            cancelled += 1
                        except Exception as exc:
                            log.debug("kill_switch_order_cancel_one_failed", error=str(exc))
            except Exception as exc:
                log.warning("kill_switch_get_orders_failed", error=str(exc))
    return cancelled


async def flatten_polymarket_exposure(redis: Any) -> dict[str, Any]:
    """
    Cancel CLOB orders; sell Polymarket-bot tracked position; simulation already cleared.
    """
    from nexus.trading.poly_bot_state import POLY_BOT_OPEN_POS_KEY

    out: dict[str, Any] = {"orders_cancelled": 0, "bot_sell": None, "errors": []}
    try:
        from nexus.trading.polymarket_client import PolymarketClient

        client = PolymarketClient()
        out["orders_cancelled"] = await _cancel_open_clob_orders(client)
    except Exception as exc:
        out["errors"].append(f"clob_cancel:{exc}")

    try:
        raw = await redis.get(POLY_BOT_OPEN_POS_KEY)
        if raw:
            pos = json.loads(raw)
            from nexus.trading.polymarket_client import PolymarketClient

            pm = PolymarketClient()
            token_id = str(pos.get("token_id") or "")
            shares = float(pos.get("shares") or 0)
            mq = str(pos.get("market_question") or "")
            if token_id and shares > 0:
                tick = pm.get_tick_size(token_id)
                # Aggressive sell — emergency flatten
                tr = await pm.place_sell_async(
                    token_id=token_id,
                    price=0.01,
                    size=shares,
                    market_question=mq,
                    tick_size=tick,
                    force_live=True,
                )
                out["bot_sell"] = {
                    "success": tr.success,
                    "error": tr.error,
                    "order_id": tr.order_id,
                }
                if tr.success:
                    await redis.delete(POLY_BOT_OPEN_POS_KEY)
    except Exception as exc:
        out["errors"].append(f"bot_flatten:{exc}")

    return out


async def evacuate_usdc_best_effort() -> dict[str, Any]:
    """
    Optional sweep of Polygon USDC to NEXUS_SAFE_HAVEN_WALLET.
    Requires: pip install web3, private key + funder envs, NEXUS_KILL_SWITCH_EVACUATE=1.
    """
    if os.getenv("NEXUS_KILL_SWITCH_EVACUATE", "").strip().lower() not in (
        "1", "true", "yes", "on",
    ):
        return {"attempted": False, "reason": "NEXUS_KILL_SWITCH_EVACUATE not enabled"}

    dest = (os.getenv("NEXUS_SAFE_HAVEN_WALLET") or "").strip()
    if not dest.startswith("0x") or len(dest) < 42:
        return {"attempted": False, "reason": "NEXUS_SAFE_HAVEN_WALLET missing or invalid"}

    try:
        from web3 import Web3  # type: ignore[import-not-found]
    except ImportError:
        return {"attempted": False, "reason": "web3 package not installed"}

    from nexus.trading.wallet_manager import get_polymarket_funder_address, get_polymarket_private_key

    pk = get_polymarket_private_key()
    funder = get_polymarket_funder_address()
    if not pk or not funder:
        return {"attempted": False, "reason": "no signing material for evacuation"}

    token = (
        os.getenv("NEXUS_USDC_TOKEN_POLYGON") or "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
    ).strip()
    rpc = (os.getenv("POLYGON_RPC_URL") or "https://polygon-rpc.com").strip()

    erc20_abi = [
        {
            "constant": False,
            "inputs": [
                {"name": "_to", "type": "address"},
                {"name": "_value", "type": "uint256"},
            ],
            "name": "transfer",
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
    ]

    try:
        w3 = Web3(Web3.HTTPProvider(rpc))
        if not w3.is_connected():
            return {"attempted": True, "ok": False, "error": "RPC not connected"}
        c = w3.eth.contract(address=Web3.to_checksum_address(token), abi=erc20_abi)
        acct = w3.eth.account.from_key(pk)
        bal = int(c.functions.balanceOf(Web3.to_checksum_address(funder)).call())
        if bal <= 0:
            return {"attempted": True, "ok": True, "note": "zero_usdc_balance", "wei": 0}
        # Leave a tiny buffer for gas if native MATIC low — still attempt full transfer
        nonce = w3.eth.get_transaction_count(acct.address)
        tx = c.functions.transfer(Web3.to_checksum_address(dest), bal).build_transaction(
            {
                "from": acct.address,
                "nonce": nonce,
                "gas": 120_000,
                "gasPrice": w3.eth.gas_price,
                "chainId": 137,
            }
        )
        signed = acct.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        return {
            "attempted": True,
            "ok": True,
            "tx_hash": tx_hash.hex(),
            "amount_raw": bal,
        }
    except Exception as exc:
        log.error("kill_switch_evacuate_failed", error=str(exc))
        return {"attempted": True, "ok": False, "error": str(exc)[:200]}


async def _last_known_balance_usdc() -> float | None:
    try:
        from nexus.trading.polymarket_client import PolymarketClient

        return float(await PolymarketClient().get_balance_usdc())
    except Exception:
        return None


async def send_secure_halt_telegram(
    *,
    activated_at: str,
    balance_usd: float | None,
    workers: list[str],
    exposure: dict[str, Any],
    evac: dict[str, Any],
    env_cleared: list[str],
) -> None:
    try:
        from nexus.shared.notifications.providers.telegram import TelegramProvider, _esc

        provider = TelegramProvider()
        bal_txt = f"${balance_usd:.2f}" if balance_usd is not None else "unknown"
        wstr = ", ".join(workers) if workers else "none"
        lines = [
            "🔒 *SYSTEM HALTED — SECURE*",
            "",
            "_Nexus emergency kill\\-switch completed\\._",
            "",
            f"⏰ `{_esc(activated_at[:19].replace('T', ' '))} UTC`",
            f"💰 *Last known USDC \\(CLOB\\):* `{_esc(bal_txt)}`",
            f"🖥️ *Worker heartbeats:* `{_esc(wstr)}`",
            f"🧾 *CLOB cancels:* `{exposure.get('orders_cancelled', 0)}`",
            f"📤 *Evacuation:* `{_esc(str(evac.get('ok', evac.get('reason', 'n/a'))))}`",
            f"🗝️ *Env keys cleared \\(this process\\):* `{len(env_cleared)}`",
            "",
            "_Workers received TERMINATE \\+ FORCE\\_STOP\\._",
            "_Trading keys cleared from API/Master memory \\— restart to reload\\._",
        ]
        await provider.send_message("\n".join(lines))
        log.info("kill_switch_secure_telegram_sent")
    except Exception as exc:
        log.error("kill_switch_secure_telegram_failed", error=str(exc))


async def complete_kill_switch_slow_path(
    redis: Any,
    *,
    phase1: dict[str, Any],
    evacuate: bool = False,
) -> dict[str, Any]:
    """
    After :func:`engage_immediate`: flatten positions, optional evac, env wipe, Telegram.
    Safe to run in a background task.
    """
    workers = await _collect_worker_ids(redis)
    balance = await _last_known_balance_usdc()
    exposure = await flatten_polymarket_exposure(redis)
    evac: dict[str, Any] = {"attempted": False}
    if evacuate:
        evac = await evacuate_usdc_best_effort()
    env_cleared = wipe_local_trading_env()

    report = {
        **phase1,
        "workers_seen": workers,
        "last_balance_usdc": balance,
        "exposure": exposure,
        "evacuation": evac,
        "env_keys_cleared": env_cleared,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        await redis.set(KILL_SWITCH_REPORT_META_KEY, json.dumps(report), ex=86400 * 7)
    except Exception:
        pass

    await send_secure_halt_telegram(
        activated_at=phase1["activated_at"],
        balance_usd=balance,
        workers=workers,
        exposure=exposure,
        evac=evac,
        env_cleared=env_cleared,
    )
    log.critical("kill_switch_full_complete")
    return report


async def run_full_kill_switch(
    redis: Any,
    *,
    reason: str,
    source: str,
    evacuate: bool = False,
) -> dict[str, Any]:
    """Immediate + slow path in one call (e.g. Telegram direct / CLI)."""
    phase1 = await engage_immediate(redis, reason=reason, source=source)
    return await complete_kill_switch_slow_path(redis, phase1=phase1, evacuate=evacuate)


async def clear_kill_switch_aux_flags(redis: Any) -> None:
    """Used by panic reset — allow scalpers to run again after operator clears panic."""
    try:
        await redis.delete(KILL_SWITCH_SCALPER_HALT_KEY, KILL_SWITCH_REPORT_META_KEY)
        from nexus.trading.wallet_manager import REDIS_BRAKE_KEY

        # Only clear brake if it was kill-switch (avoid clobbering real drawdown brake)
        raw = await redis.get(REDIS_BRAKE_KEY)
        if raw == "kill_switch":
            await redis.delete(REDIS_BRAKE_KEY)
    except Exception as exc:
        log.warning("kill_switch_aux_clear_failed", error=str(exc))


def schedule_kill_switch_completion(
    redis: Any,
    *,
    phase1: dict[str, Any],
    evacuate: bool = False,
) -> asyncio.Task[dict[str, Any]]:
    """Run slow path after HTTP handler already executed :func:`engage_immediate`."""

    async def _runner() -> dict[str, Any]:
        return await complete_kill_switch_slow_path(
            redis, phase1=phase1, evacuate=evacuate
        )

    return asyncio.create_task(_runner(), name="nexus-kill-switch-completion")
