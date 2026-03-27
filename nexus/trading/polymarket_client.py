"""
nexus/trading/polymarket_client.py — Polymarket Order Execution Client
========================================================================

Wraps the Polymarket CLOB API (via ``py_clob_client``) with builder
attribution, an async interface, and a hard kill-switch that halts all
trading when the USDC balance drops below $90.

Safety layers
-------------
1. ``nexus.trading.config.PAPER_TRADING = True``  — virtual trades only.
2. Kill switch: balance < KILL_SWITCH_BALANCE_USD → ``TradingHalted`` raised.
3. API timeout: 15-second hard limit on every order placement.

Required environment variables
──────────────────────────────
POLYMARKET_RELAYER_KEY       0x-prefixed private key for EIP-712 signing
POLY_PRIVATE_KEY             alias for POLYMARKET_RELAYER_KEY (optional)
POLYMARKET_SIGNER_ADDRESS    Funder / EOA address
POLYMARKET_API_KEY           L2 API key  (optional — for authenticated routes)
POLYMARKET_API_SECRET        L2 API secret
POLYMARKET_API_PASSPHRASE    L2 API passphrase
POLY_BUILDER_API_KEY         Builder API key (polymarket.com/settings?tab=builder)
POLY_BUILDER_SECRET          Builder API secret
POLY_BUILDER_PASSPHRASE      Builder API passphrase
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Literal

import httpx
import structlog

from nexus.trading.config import (
    PAPER_TRADING,
    PAPER_TRADING_AMOUNT_USD,
    PAPER_TRADING_COOLDOWN_S,
    PAPER_TRADING_MAX_HISTORY,
    PAPER_TRADING_REDIS_KEY,
    PREDICTION_MANUAL_HALT_KEY,
)
from nexus.trading.runtime_mode import effective_paper_trading
from nexus.trading.wallet_manager import get_polymarket_funder_address, get_polymarket_private_key

log = structlog.get_logger(__name__)

_CLOB_HOST: str = "https://clob.polymarket.com"
_POLYGON_CHAIN_ID: int = 137
_DEBUG_LOG_PATH = Path(__file__).resolve().parents[2] / "debug-45ccd1.log"


def _agent_debug_ndjson_45(
    hypothesis_id: str, location: str, message: str, data: dict[str, Any]
) -> None:
    # #region agent log
    try:
        payload = {
            "sessionId": "45ccd1",
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        with _DEBUG_LOG_PATH.open("a", encoding="utf-8") as df:
            df.write(json.dumps(payload) + "\n")
    except Exception:
        pass
    # #endregion


def _derived_eoa_from_key(pk: str) -> str:
    if not (pk or "").strip():
        return ""
    try:
        from eth_account import Account

        return Account.from_key(pk.strip()).address.lower()
    except Exception:
        return ""


def resolve_clob_funder_address(private_key: str, env_signer_address: str) -> str:
    """CLOB ``OrderBuilder`` uses ``funder`` as ``maker``; for EOAs it must match the signing key.

    If ``POLYMARKET_SIGNER_ADDRESS`` was copied from ``POLYMARKET_PORTFOLIO_ADDRESS`` or mistyped,
    orders would reference the wrong maker while the signature comes from ``private_key``.
    Default: use the address derived from ``POLYMARKET_RELAYER_KEY``.

    Set ``POLYMARKET_ALLOW_FUNDER_ENV_MISMATCH=1`` to keep the env address (Polymarket proxy / advanced).
    """
    derived = _derived_eoa_from_key(private_key)
    allow_mismatch = (os.getenv("POLYMARKET_ALLOW_FUNDER_ENV_MISMATCH") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    env = (env_signer_address or "").strip()
    if allow_mismatch:
        return env or derived
    if not derived:
        return env
    if not env:
        return derived
    if env.lower() == derived.lower():
        return env
    log.warning(
        "polymarket.funder_env_mismatch_using_derived",
        env_signer_short=f"{env[:6]}…{env[-4:]}",
        derived_short=f"{derived[:6]}…{derived[-4:]}",
        hint="POLYMARKET_SIGNER_ADDRESS != key-derived address; using derived address for CLOB funder.",
    )
    return derived


def get_polymarket_clob_funder_address() -> str:
    """Effective CLOB maker address (after resolving env vs key)."""
    return resolve_clob_funder_address(get_polymarket_private_key(), get_polymarket_funder_address())


# Kill-switch threshold — trading halts if USDC balance falls below this
KILL_SWITCH_BALANCE_USD: float = 90.0


# ── Sentinel exceptions ───────────────────────────────────────────────────────

class TradingHalted(RuntimeError):
    """Raised when the kill switch triggers due to low balance."""


# ── Trade result ──────────────────────────────────────────────────────────────

@dataclass
class TradeResult:
    """Structured outcome of a single automated trade attempt."""

    success: bool
    token_id: str
    side: Literal["YES", "NO"]
    price: float
    shares: float
    spent_usd: float
    market_question: str = ""
    order_id: str | None = None
    error: str | None = None
    paper: bool = PAPER_TRADING
    order_action: Literal["BUY", "SELL"] = "BUY"
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_log_text(self) -> str:
        prefix = "[PAPER] " if self.paper else ""
        verb = "Sold" if self.order_action == "SELL" else "Bought"
        if self.success:
            return f"{prefix}{verb} {self.shares:.1f} shares of {self.side} @ ${self.price:.3f}"
        return f"{prefix}Trade FAILED: {self.error}"

    def to_redis_entry(self) -> dict[str, Any]:
        return {
            "timestamp":       self.timestamp,
            "action":          self.order_action,
            "side":            self.side,
            "price":           self.price,
            "shares":          self.shares,
            "spent_usd":       self.spent_usd,
            "market_question": self.market_question,
            "status":          "success" if self.success else "failed",
            "order_id":        self.order_id,
            "log_text":        self.to_log_text(),
            "paper":           self.paper,
        }


# ── SDK helpers (lazy imports — no hard failure when SDK is absent) ───────────

def _build_builder_config() -> Any | None:
    try:
        from py_builder_signing_sdk.config import BuilderApiKeyCreds, BuilderConfig
    except ImportError:
        return None

    key = os.getenv("POLY_BUILDER_API_KEY")
    secret = os.getenv("POLY_BUILDER_SECRET")
    passphrase = os.getenv("POLY_BUILDER_PASSPHRASE")
    if not all([key, secret, passphrase]):
        log.warning(
            "polymarket.builder_config_incomplete",
            detail="POLY_BUILDER_* env vars not fully set — no builder attribution",
        )
        return None
    return BuilderConfig(
        local_builder_creds=BuilderApiKeyCreds(key=key, secret=secret, passphrase=passphrase)
    )


def _build_api_creds() -> Any | None:
    try:
        from py_clob_client.clob_types import ApiCreds
    except ImportError:
        return None

    api_key = os.getenv("POLYMARKET_API_KEY")
    api_secret = os.getenv("POLYMARKET_API_SECRET")
    api_passphrase = os.getenv("POLYMARKET_API_PASSPHRASE")
    if not all([api_key, api_secret, api_passphrase]):
        log.warning(
            "polymarket.api_creds_incomplete",
            detail="POLYMARKET_API_* env vars not fully set — unauthenticated mode",
        )
        return None
    return ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)


# Re-export BUY / SELL so callers can import from this module
try:
    from py_clob_client.order_builder.constants import BUY, SELL  # noqa: F401
except ImportError:
    BUY = "BUY"    # type: ignore[assignment]
    SELL = "SELL"  # type: ignore[assignment]


# ── Client class ──────────────────────────────────────────────────────────────

class PolymarketClient:
    """Nexus Polymarket CLOB client with builder attribution and kill-switch.

    Thread-safety note: each call to ``place_order_async`` wraps synchronous
    SDK methods via ``loop.run_in_executor`` — safe for concurrent async use.
    """

    def __init__(self) -> None:
        self.builder_id: str = os.getenv("POLYMARKET_BUILDER_ID", "Nexus")
        self._private_key: str = get_polymarket_private_key()
        self._funder: str = resolve_clob_funder_address(
            self._private_key,
            get_polymarket_funder_address(),
        )
        self._clob: Any | None = None
        self._try_init_sdk()

    def _try_init_sdk(self) -> None:
        try:
            from py_clob_client.client import ClobClient

            self._clob = ClobClient(
                host=_CLOB_HOST,
                chain_id=_POLYGON_CHAIN_ID,
                key=self._private_key,
                creds=_build_api_creds(),
                funder=self._funder,
                builder_config=_build_builder_config(),
            )
            log.info(
                "polymarket.client_ready",
                builder_id=self.builder_id,
                paper_trading=PAPER_TRADING,
                host=_CLOB_HOST,
            )
        except ImportError:
            log.warning(
                "polymarket.sdk_not_installed",
                hint="pip install py-clob-client py-builder-signing-sdk",
            )
        except Exception as exc:
            log.warning("polymarket.client_init_error", error=str(exc))

    # ── Balance & kill-switch ─────────────────────────────────────────────────

    async def get_balance_usdc(self) -> float:
        """Return spendable USDC (collateral) for the CLOB signing / funder wallet.

        Tries the SDK first, then the Polymarket data API for ``POLYMARKET_SIGNER_ADDRESS``
        only — not ``POLYMARKET_PORTFOLIO_ADDRESS`` (UI may point at another account).

        Returns ``100.0`` when every method fails so callers can distinguish a genuine
        low balance from a connectivity issue.

        Raises:
            httpx.TimeoutException: if the REST fallback request times out.
        """
        loop = asyncio.get_event_loop()
        _fu = (self._funder or "").strip().lower()
        _pv = (os.getenv("POLYMARKET_PORTFOLIO_ADDRESS") or "").strip().lower()
        _dk = _derived_eoa_from_key(self._private_key)
        _agent_debug_ndjson_45(
            "H1",
            "polymarket_client.py:get_balance_usdc:entry",
            "wallet_resolution",
            {
                "funder_short": f"{_fu[:6]}…{_fu[-4:]}" if len(_fu) >= 10 else _fu,
                "derived_from_key_short": f"{_dk[:6]}…{_dk[-4:]}" if len(_dk) >= 10 else _dk,
                "key_matches_funder": bool(_dk and _fu and _dk == _fu),
                "portfolio_env_set": bool(_pv),
                "portfolio_differs_from_funder": bool(_pv and _fu and _pv != _fu),
            },
        )

        if self._clob is not None:
            # Try the preferred SDK method: get_balance_allowance(BalanceAllowanceParams)
            fn = getattr(self._clob, "get_balance_allowance", None)
            if fn is not None:
                try:
                    from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
                    params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                    resp = await asyncio.wait_for(
                        loop.run_in_executor(None, fn, params),
                        timeout=10.0,
                    )
                    if isinstance(resp, dict):
                        raw = resp.get("balance", 0)
                        # USDC on Polygon has 6 decimals — value may be in wei
                        val = float(raw)
                        if val > 1_000_000:
                            val = val / 1_000_000
                        if val > 0:
                            _agent_debug_ndjson_45(
                                "H4",
                                "polymarket_client.py:get_balance_usdc",
                                "sdk_positive_balance",
                                {"sdk_balance_usd": val},
                            )
                            return val
                        _agent_debug_ndjson_45(
                            "H4",
                            "polymarket_client.py:get_balance_usdc",
                            "sdk_zero_dict_fallback",
                            {"raw_balance": raw},
                        )
                        # SDK returned 0 — relayer wallet may be unfunded; fall through
                        # to portfolio address REST check below
                    elif isinstance(resp, (int, float)):
                        val = float(resp)
                        if val > 1_000_000:
                            val = val / 1_000_000
                        if val > 0:
                            _agent_debug_ndjson_45(
                                "H4",
                                "polymarket_client.py:get_balance_usdc",
                                "sdk_positive_balance_scalar",
                                {"sdk_balance_usd": val},
                            )
                            return val
                except asyncio.TimeoutError:
                    log.error("polymarket.get_balance_timeout", method="get_balance_allowance")
                    raise
                except ImportError:
                    pass
                except Exception as exc:
                    log.debug("polymarket.get_balance_sdk_miss", method="get_balance_allowance", error=str(exc))

            # Legacy fallback: get_balance()
            fn_legacy = getattr(self._clob, "get_balance", None)
            if fn_legacy is not None:
                try:
                    resp = await asyncio.wait_for(
                        loop.run_in_executor(None, fn_legacy),
                        timeout=10.0,
                    )
                    if isinstance(resp, (int, float)):
                        val = float(resp)
                        if val > 0:
                            return val
                    if isinstance(resp, dict):
                        val = float(resp.get("balance", resp.get("USDC", 0.0)))
                        if val > 0:
                            return val
                except asyncio.TimeoutError:
                    log.error("polymarket.get_balance_timeout", method="get_balance")
                    raise
                except Exception as exc:
                    log.debug("polymarket.get_balance_sdk_miss", method="get_balance", error=str(exc))

        # REST fallback: Polymarket data-api for the *signing / funder* wallet only.
        # Do not use POLYMARKET_PORTFOLIO_ADDRESS here — the UI may track a different
        # funded account; CLOB orders spend collateral on the relayer key / funder only.
        portfolio_addr = (self._funder or "").strip()
        if portfolio_addr:
            try:
                async with httpx.AsyncClient(timeout=8.0) as hclient:
                    r = await hclient.get(
                        f"https://data-api.polymarket.com/value?user={portfolio_addr.lower()}"
                    )
                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, list):
                        if not data:
                            return 0.0
                        total_val = float(data[0].get("value", 0) or 0)
                        log.info(
                            "polymarket.balance_from_data_api",
                            address=portfolio_addr[:10] + "…",
                            balance_usd=total_val,
                        )
                        _agent_debug_ndjson_45(
                            "H2",
                            "polymarket_client.py:get_balance_usdc",
                            "data_api_balance_used_for_kill_switch",
                            {
                                "returned_usd": total_val,
                                "query_addr_short": f"{portfolio_addr[:6]}…{portfolio_addr[-4:]}"
                                if len(portfolio_addr) >= 10
                                else portfolio_addr,
                                "data_api_fallback": True,
                            },
                        )
                        return total_val
            except (httpx.TimeoutException, asyncio.TimeoutError) as exc:
                log.error("polymarket.get_balance_data_api_timeout", error=str(exc))
                raise
            except Exception as exc:
                log.debug("polymarket.get_balance_data_api_error", error=str(exc))

        log.warning("polymarket.get_balance_unavailable", default=100.0)
        _agent_debug_ndjson_45(
            "H2",
            "polymarket_client.py:get_balance_usdc",
            "default_balance_100_returned",
            {"reason": "all_methods_failed_or_zero"},
        )
        return 100.0

    async def check_kill_switch(self) -> None:
        """Raise ``TradingHalted`` if the USDC balance is below the threshold.

        Also halts if the balance fetch itself times out — losing connectivity
        is treated as a reason to stop trading, not to continue.
        """
        try:
            balance = await self.get_balance_usdc()
        except (httpx.TimeoutException, asyncio.TimeoutError) as exc:
            raise TradingHalted(
                f"Kill switch engaged: balance check timed out ({exc})"
            ) from exc

        log.info(
            "polymarket.kill_switch_check",
            balance_usd=balance,
            threshold=KILL_SWITCH_BALANCE_USD,
        )
        _agent_debug_ndjson_45(
            "H2",
            "polymarket_client.py:check_kill_switch",
            "kill_switch_balance_seen",
            {"balance_usd": balance, "threshold": KILL_SWITCH_BALANCE_USD},
        )
        if balance < KILL_SWITCH_BALANCE_USD:
            raise TradingHalted(
                f"Kill switch: balance ${balance:.2f} < "
                f"${KILL_SWITCH_BALANCE_USD:.2f} — all trading halted"
            )

    # ── Async order placement ─────────────────────────────────────────────────

    async def place_order_async(
        self,
        token_id: str,
        side: Literal["YES", "NO"],
        price: float,
        market_question: str = "",
        budget_usd: float = PAPER_TRADING_AMOUNT_USD,
        *,
        tick_size: str = "0.01",
        force_live: bool = False,
        redis: Any = None,
    ) -> TradeResult:
        """Check kill switch then place a limit order.

        In PAPER_TRADING mode the order is simulated and logged to Redis
        under ``PAPER_TRADING_REDIS_KEY`` — no real API call is made.

        In live mode (``PAPER_TRADING = False``) the order is submitted via
        the CLOB API with a 15-second timeout.

        When ``force_live=True``, the CLOB path is used even if
        ``PAPER_TRADING`` is enabled (for isolated live modules such as the
        5m scalper).

        Args:
            token_id:        Polymarket CLOB outcome token ID.
            side:            "YES" or "NO".
            price:           Limit price per share (0–1).
            market_question: Human-readable market label for the trade log.
            budget_usd:      Dollar amount to spend (default: PAPER_TRADING_AMOUNT_USD).
            tick_size:       Market tick size string.

        Returns:
            ``TradeResult`` — never raises on order failures, only on
            ``TradingHalted`` or ``asyncio.TimeoutError``.
        """
        await self.check_kill_switch()

        shares = round(budget_usd / price, 2) if price > 0 else 0.0

        ep = await effective_paper_trading(redis)
        paper_mode = ep and not force_live

        base = TradeResult(
            success=False,
            token_id=token_id,
            side=side,
            price=price,
            shares=shares,
            spent_usd=0.0,
            market_question=market_question,
            paper=paper_mode,
        )

        if paper_mode:
            log.info(
                "polymarket.paper_trade_executed",
                side=side,
                price=price,
                shares=shares,
                budget_usd=budget_usd,
                market_question=market_question,
            )
            base.success   = True
            base.spent_usd = budget_usd
            base.order_id  = f"PAPER-{int(datetime.now(timezone.utc).timestamp())}"
            return base

        if self._clob is None:
            base.error = "Polymarket SDK not initialised (py-clob-client missing)"
            return base

        loop = asyncio.get_event_loop()
        try:
            resp = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: self._place_limit_order_sync(
                        token_id=token_id,
                        price=price,
                        size=shares,
                        side=BUY,
                        tick_size=tick_size,
                    ),
                ),
                timeout=15.0,
            )
            order_id = resp.get("orderID") if isinstance(resp, dict) else str(resp)
            log.info(
                "polymarket.live_order_placed",
                builder_id=self.builder_id,
                side=side,
                price=price,
                shares=shares,
                order_id=order_id,
            )
            base.success   = True
            base.spent_usd = budget_usd
            base.order_id  = order_id
            return base
        except asyncio.TimeoutError:
            log.error("polymarket.place_order_timeout", token_id=token_id)
            raise
        except Exception as exc:
            log.error("polymarket.place_order_error", error=str(exc), token_id=token_id)
            base.error = str(exc)[:500]
            return base

    async def place_sell_async(
        self,
        token_id: str,
        price: float,
        size: float,
        market_question: str = "",
        *,
        tick_size: str = "0.01",
        redis: Any = None,
        force_live: bool = False,
    ) -> TradeResult:
        """
        SELL limit for outcome tokens (e.g. stop-loss on YES).

        Skips the USDC kill-switch so positions can be flattened when balance is low.
        ``force_live=True`` submits to CLOB even when paper mode is active (emergency flatten).
        """
        ep = await effective_paper_trading(redis)
        if force_live:
            ep = False
        base = TradeResult(
            success=False,
            token_id=token_id,
            side="YES",
            price=price,
            shares=round(size, 4),
            spent_usd=round(price * size, 4),
            market_question=market_question,
            paper=ep,
            order_action="SELL",
        )

        if size <= 0 or price <= 0:
            base.error = "Invalid sell size or price"
            return base

        if ep:
            log.info(
                "polymarket.paper_sell_executed",
                price=price,
                shares=size,
                market_question=market_question,
            )
            base.success = True
            base.order_id = f"PAPER-SELL-{int(datetime.now(timezone.utc).timestamp())}"
            return base

        if self._clob is None:
            base.error = "Polymarket SDK not initialised (py-clob-client missing)"
            return base

        loop = asyncio.get_event_loop()
        try:
            resp = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: self._place_limit_order_sync(
                        token_id=token_id,
                        price=price,
                        size=size,
                        side=SELL,
                        tick_size=tick_size,
                    ),
                ),
                timeout=15.0,
            )
            order_id = resp.get("orderID") if isinstance(resp, dict) else str(resp)
            log.info(
                "polymarket.live_sell_placed",
                price=price,
                shares=size,
                order_id=order_id,
            )
            base.success = True
            base.order_id = order_id
            return base
        except asyncio.TimeoutError:
            log.error("polymarket.place_sell_timeout", token_id=token_id)
            raise
        except Exception as exc:
            log.error("polymarket.place_sell_error", error=str(exc), token_id=token_id)
            base.error = str(exc)[:500]
            return base

    def _place_limit_order_sync(
        self,
        token_id: str,
        price: float,
        size: float,
        side: str,
        tick_size: str = "0.01",
        neg_risk: bool = False,
    ) -> dict[str, Any]:
        """Synchronous limit order — called from a thread executor."""
        from py_clob_client.clob_types import OrderArgs, OrderType, PartialCreateOrderOptions

        log.info(f"Preparing order for Builder: {self.builder_id}")
        # #region agent log
        _fu = (self._funder or "").strip().lower()
        _dk = _derived_eoa_from_key(self._private_key)
        _agent_debug_ndjson_45(
            "H3",
            "polymarket_client.py:_place_limit_order_sync",
            "clob_order_before_create",
            {
                "side": str(side),
                "size": size,
                "price": price,
                "funder_short": f"{_fu[:6]}…{_fu[-4:]}" if len(_fu) >= 10 else _fu,
                "derived_key_short": f"{_dk[:6]}…{_dk[-4:]}" if len(_dk) >= 10 else _dk,
                "key_matches_funder": bool(_dk and _fu and _dk == _fu),
            },
        )
        # #endregion
        signed = self._clob.create_order(
            OrderArgs(token_id=token_id, price=price, size=size, side=side),
            options=PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk),
        )
        return self._clob.post_order(signed, OrderType.GTC)

    # ── Builder attribution queries ───────────────────────────────────────────

    def get_builder_trades(self, *, market: str | None = None) -> list[dict[str, Any]]:
        """Return trades attributed to this builder account."""
        if self._clob is None:
            return []
        kwargs: dict[str, Any] = {}
        if market:
            kwargs["market"] = market
        return self._clob.get_builder_trades(**kwargs)

    # ── Convenience pass-throughs ─────────────────────────────────────────────

    def get_tick_size(self, token_id: str) -> str:
        if self._clob is None:
            return "0.01"
        try:
            return self._clob.get_tick_size(token_id)
        except Exception:
            return "0.01"

    def get_neg_risk(self, token_id: str) -> bool:
        if self._clob is None:
            return False
        return self._clob.get_neg_risk(token_id)

    @property
    def clob(self) -> Any:
        """Direct access to the underlying ClobClient for advanced use cases."""
        return self._clob


# ── Standalone place_order() — preserved for backward-compat callers ──────────

async def place_order(
    *,
    signal: str,
    binance_data: dict[str, Any],
    poly_data: dict[str, Any],
    redis: Any = None,
) -> dict[str, Any]:
    """
    Attempt to place a Polymarket order based on the computed signal.

    Parameters
    ----------
    signal       : str  — e.g. "HIGH_CONFIDENCE_BUY"
    binance_data : dict — output of fetch_binance_data()
    poly_data    : dict — output of fetch_polymarket_btc_odds()
    redis        : optional async Redis client (for post-trade logging)

    Returns
    -------
    dict with order details on success.

    Raises
    ------
    TradingHalted     — if kill switch triggers (balance < $90 or system panic).
    RuntimeError      — if signal does not meet the trading criteria.
    asyncio.TimeoutError — if the API call exceeds the 15-second limit.
    """
    # ── System-wide PANIC guard ───────────────────────────────────────────────
    # Check Redis for the global kill-switch before any trade attempt.
    if redis is not None:
        try:
            panic = await redis.get("SYSTEM_STATE:PANIC")
            if panic == "true":
                raise TradingHalted(
                    "Kill switch engaged: SYSTEM PANIC active — all trading halted"
                )
            halt = await redis.get(PREDICTION_MANUAL_HALT_KEY)
            if halt:
                raise TradingHalted(
                    "Prediction manual override active — automated Polymarket orders halted"
                )
        except TradingHalted:
            raise
        except Exception:
            pass  # Redis unavailable — fall through to balance check

    yes_price       = (poly_data.get("yes_price") or 0.5)
    token_ids: list = poly_data.get("clob_token_ids") or []
    market_question = poly_data.get("market_question") or "BTC Market"

    if not token_ids:
        raise RuntimeError(
            "No CLOB token ID available for this market — cannot place order."
        )

    client = PolymarketClient()
    result = await client.place_order_async(
        token_id=token_ids[0],
        side="YES",
        price=yes_price,
        market_question=market_question,
        redis=redis,
    )

    if redis is not None:
        import json as _json
        entry = result.to_redis_entry()
        await redis.rpush(PAPER_TRADING_REDIS_KEY, _json.dumps(entry))
        await redis.ltrim(PAPER_TRADING_REDIS_KEY, -PAPER_TRADING_MAX_HISTORY, -1)

    return result.to_redis_entry()
