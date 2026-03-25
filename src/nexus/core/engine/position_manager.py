"""
Live trading hub — daily budget, profit target, vault-linked alpha signals, and
Redis-backed “Live Positions” for the dashboard.

Operational mode defaults to **LIVE_OPS** (status **ACTIVE**) with budget **$100**
and target **$1000**, and the dashboard is instructed to use the **graphical TUI**
view. Override with env ``NEXUS_POSITION_DAILY_BUDGET_USD``,
``NEXUS_POSITION_TARGET_GOAL_USD``, or CLI ``--budget`` / ``--target`` when
running this module as a script.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from nexus.services.poly_5m_scalper import POLY_EVENT_ID, fetch_poly5m_market
from nexus.services.strategy_brain import load_alpha_feed

_NEXUS_REPO_ROOT = Path(__file__).resolve().parents[3]
(_NEXUS_REPO_ROOT / "logs").mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=str(_NEXUS_REPO_ROOT / "logs" / "nexus_runtime.log"),
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

log = structlog.get_logger(__name__)

# Live-ops defaults (dashboard + heartbeat)
POSITION_OPS_MODE = "LIVE_OPS"
POSITION_OPS_STATUS_ACTIVE = "ACTIVE"
DEFAULT_DAILY_BUDGET_USD = 100.0
DEFAULT_TARGET_GOAL_USD = 1000.0
DASHBOARD_DISPLAY_GRAPHICAL_TUI = "graphical_tui_dashboard"

LIVE_POSITIONS_KEY = "nexus:live_trading:positions"
_INJECT_META_KEY = "nexus:live_trading:positions_meta"
POSITION_ENGINE_HEARTBEAT_KEY = "nexus:live_trading:position_engine_heartbeat"
SEEN_ALPHA_FP_KEY = "nexus:live_trading:seen_alpha_msg_fp"
POSITION_PROGRESS_KEY = "nexus:live_trading:target_progress"
# SET when ``--turbo`` / ``NEXUS_POSITION_TURBO`` — workers may raise scheduling priority.
POSITION_TURBO_ACTIVE_KEY = "nexus:live_trading:turbo_active"
# Mirror of OpenClaw Polymarket CLOB snapshot (optional gate for alpha signal rows).
ORDER_BOOK_SYNC_KEY = "nexus:openclaw:polymarket_orderbook"

# Fleet / pub mirror: workers SET ``nexus:heartbeat`` (TTL) alongside ``nexus:heartbeat:<id>``.
FLEET_HEARTBEAT_PREFIX = "nexus:heartbeat:"
HEARTBEAT_PUB_MIRROR_KEY = "nexus:heartbeat"
LINUX_WORKER_ANCHOR_IP = (os.getenv("SENTINEL_LINUX_WORKER_IP") or "10.100.102.20").strip().lower()

# Temporary bypass: UI stays ACTIVE / online for scanning even when vault shows 0 sessions.
# Disable with NEXUS_POSITION_FORCE_ACTIVE=0
FORCE_ACTIVE_BYPASS_ZERO_NODES = os.getenv(
    "NEXUS_POSITION_FORCE_ACTIVE", "1"
).strip().lower() not in ("0", "false", "no", "off")

# Dashboard / internal sync: live execution (not paper); daily budget baseline USD.
POSITION_LIVE_EXECUTION = True
INTERNAL_BUDGET_SYNC_USD = DEFAULT_DAILY_BUDGET_USD


def daily_budget_usd() -> float:
    raw = (os.getenv("NEXUS_POSITION_DAILY_BUDGET_USD") or "").strip()
    if not raw:
        return max(1.0, float(INTERNAL_BUDGET_SYNC_USD))
    try:
        return max(1.0, float(raw))
    except ValueError:
        return max(1.0, float(INTERNAL_BUDGET_SYNC_USD))


def target_goal_usd() -> float:
    raw = (os.getenv("NEXUS_POSITION_TARGET_GOAL_USD") or "").strip()
    if not raw:
        return max(1.0, float(DEFAULT_TARGET_GOAL_USD))
    try:
        return max(1.0, float(raw))
    except ValueError:
        return max(1.0, float(DEFAULT_TARGET_GOAL_USD))


def _decode(raw: Any) -> str:
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace").strip()
    return str(raw).strip()


def _alpha_fingerprint(ev: dict[str, Any]) -> str:
    blob = json.dumps(
        {
            "ts": str(ev.get("ts") or ev.get("at") or ""),
            "kind": str(ev.get("kind") or ""),
            "detail": str(ev.get("detail") or "")[:240],
            "channel": str(ev.get("channel") or ev.get("channel_title") or "")[:120],
        },
        sort_keys=True,
    )
    return hashlib.sha256(blob.encode()).hexdigest()[:24]


def _ingest_hb_dict(
    d: dict[str, Any],
    dedupe_id: str,
    *,
    seen_workers: set[str],
    seen_masters: set[str],
    linux_anchor_seen: list[bool],
) -> None:
    role = str(d.get("role") or "").lower()
    lip = _norm_ip(str(d.get("local_ip") or ""))
    if lip == LINUX_WORKER_ANCHOR_IP:
        linux_anchor_seen[0] = True
    if role == "worker":
        if "linux" in str(d.get("os_info") or "").lower():
            linux_anchor_seen[0] = True
        if dedupe_id and dedupe_id not in seen_workers:
            seen_workers.add(dedupe_id)
    elif role == "master":
        if dedupe_id and dedupe_id not in seen_masters:
            seen_masters.add(dedupe_id)


async def _fleet_heartbeat_summary(redis: Any) -> dict[str, Any]:
    """
    SCAN ``nexus:heartbeat:*`` plus GET ``nexus:heartbeat`` (last pub mirror)
    so Linux workers (e.g. 10.100.102.20) count even when session vault is empty.
    """
    seen_workers: set[str] = set()
    seen_masters: set[str] = set()
    linux_flag = [False]
    mirror_present = False
    try:
        raw_mirror = await redis.get(HEARTBEAT_PUB_MIRROR_KEY)
    except Exception:
        raw_mirror = None
    if raw_mirror:
        mirror_present = True
        try:
            d = json.loads(_decode(raw_mirror))
            if isinstance(d, dict):
                mid = str(d.get("node_id") or "").strip() or "mirror"
                _ingest_hb_dict(
                    d,
                    mid,
                    seen_workers=seen_workers,
                    seen_masters=seen_masters,
                    linux_anchor_seen=linux_flag,
                )
        except json.JSONDecodeError:
            pass

    try:
        cursor = 0
        while True:
            cursor, keys = await redis.scan(
                cursor=cursor, match=f"{FLEET_HEARTBEAT_PREFIX}*", count=120
            )
            for k in keys or []:
                ks = _decode(k)
                if ks == HEARTBEAT_PUB_MIRROR_KEY:
                    continue
                suffix = ks[len(FLEET_HEARTBEAT_PREFIX) :].strip()
                if not suffix:
                    continue
                try:
                    raw = await redis.get(ks)
                except Exception:
                    continue
                if not raw:
                    continue
                try:
                    hb = json.loads(_decode(raw))
                except json.JSONDecodeError:
                    continue
                if isinstance(hb, dict):
                    hid = str(hb.get("node_id") or "").strip() or suffix
                    _ingest_hb_dict(
                        hb,
                        hid,
                        seen_workers=seen_workers,
                        seen_masters=seen_masters,
                        linux_anchor_seen=linux_flag,
                    )
            if cursor == 0:
                break
    except Exception as exc:
        log.debug("fleet_heartbeat_scan_failed", error=str(exc))

    return {
        "workers_online": len(seen_workers),
        "masters_online": len(seen_masters),
        "linux_anchor_seen": linux_flag[0],
        "mirror_present": mirror_present,
        "anchor_ip": LINUX_WORKER_ANCHOR_IP,
    }


def _norm_ip(ip: str) -> str:
    return (ip or "").strip().lower()


def turbo_mode_active() -> bool:
    """True when master position pipeline runs in turbo (CLI ``--turbo`` or env)."""
    return os.getenv("NEXUS_POSITION_TURBO", "").strip().lower() in ("1", "true", "yes", "on")


async def _order_book_sync_blocks_signal_push(redis: Any, *, turbo_boost: bool) -> bool:
    """
    Optional: defer alpha signal rows until the Polymarket CLOB mirror key exists.
    Enable with ``NEXUS_POSITION_ORDERBOOK_SYNC_REQUIRED=1``. ``turbo_boost``
    (``--turbo`` / ``NEXUS_POSITION_TURBO``) bypasses this gate.
    """
    raw = (os.getenv("NEXUS_POSITION_ORDERBOOK_SYNC_REQUIRED") or "").strip().lower()
    if raw not in ("1", "true", "yes", "on"):
        return False
    if turbo_boost:
        return False
    try:
        ob = await redis.get(ORDER_BOOK_SYNC_KEY)
    except Exception:
        ob = None
    return not bool(ob)


async def _wait_for_fleet_workers(
    redis: Any,
    *,
    turbo: bool,
    timeout_s: float | None = None,
    interval_s: float = 1.0,
) -> None:
    """
    Block until at least one worker heartbeat exists, so live-ops injection aligns with fleet.

    Skipped entirely when ``turbo`` is True (``--turbo`` / ``NEXUS_POSITION_TURBO``).
    Disable with ``NEXUS_POSITION_HEARTBEAT_WAIT_S=0`` or negative timeout.
    """
    if turbo:
        return
    raw_to = (os.getenv("NEXUS_POSITION_HEARTBEAT_WAIT_S") or "").strip()
    if raw_to.lower() in ("0", "false", "no", "off", "skip"):
        return
    lim = timeout_s
    if lim is None:
        try:
            lim = float(raw_to) if raw_to else 90.0
        except ValueError:
            lim = 90.0
    if lim <= 0:
        return
    deadline = time.monotonic() + lim
    while time.monotonic() < deadline:
        fl = await _fleet_heartbeat_summary(redis)
        if int(fl.get("workers_online") or 0) > 0:
            return
        await asyncio.sleep(max(0.2, interval_s))


async def touch_position_engine_heartbeat(
    redis: Any,
    *,
    fleet: dict[str, Any] | None = None,
    turbo: bool | None = None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    fl = fleet if fleet is not None else await _fleet_heartbeat_summary(redis)
    turbo_on = turbo_mode_active() if turbo is None else turbo
    payload = json.dumps(
        {
            "status": POSITION_OPS_STATUS_ACTIVE,
            "mode": POSITION_OPS_MODE,
            "display": DASHBOARD_DISPLAY_GRAPHICAL_TUI,
            "budget_usd": daily_budget_usd(),
            "target_goal_usd": target_goal_usd(),
            "updated_at": now,
            "component": "position_manager",
            "forced_active_bypass": FORCE_ACTIVE_BYPASS_ZERO_NODES,
            "fleet_workers_online": int(fl.get("workers_online") or 0),
            "fleet_masters_online": int(fl.get("masters_online") or 0),
            "fleet_linux_anchor_seen": bool(fl.get("linux_anchor_seen")),
            "fleet_heartbeat_mirror": bool(fl.get("mirror_present")),
            "turbo_mode": turbo_on,
        },
        ensure_ascii=False,
    )
    try:
        await redis.set(POSITION_ENGINE_HEARTBEAT_KEY, payload, ex=90)
    except Exception as exc:
        log.debug("position_engine_heartbeat_set_failed", error=str(exc))
    try:
        if turbo_on:
            await redis.set(POSITION_TURBO_ACTIVE_KEY, "1", ex=120)
        else:
            await redis.delete(POSITION_TURBO_ACTIVE_KEY)
    except Exception as exc:
        log.debug("position_turbo_flag_set_failed", error=str(exc))


async def position_engine_heartbeat_snapshot(redis: Any) -> dict[str, Any]:
    """Dashboard: engine liveness (TTL on heartbeat key ⇒ offline when stale)."""
    try:
        raw = await redis.get(POSITION_ENGINE_HEARTBEAT_KEY)
    except Exception:
        raw = None
    if not raw:
        return {
            "status": "offline",
            "display": "Offline",
            "updated_at": None,
        }
    try:
        data = json.loads(_decode(raw))
    except json.JSONDecodeError:
        return {
            "status": "offline",
            "display": "Offline",
            "updated_at": None,
        }
    st = str(data.get("status") or POSITION_OPS_STATUS_ACTIVE)
    ts = data.get("updated_at")
    mode = str(data.get("mode") or POSITION_OPS_MODE)
    disp_key = str(data.get("display") or "").strip().lower()
    online = st.lower() in ("online", "active")
    if FORCE_ACTIVE_BYPASS_ZERO_NODES:
        online = True
        st = POSITION_OPS_STATUS_ACTIVE
    if disp_key == DASHBOARD_DISPLAY_GRAPHICAL_TUI or "tui" in disp_key:
        display_label = "Graphical TUI Dashboard"
    else:
        display_label = "Online" if online else "Offline"
    return {
        "status": "online" if online else "offline",
        "display": display_label,
        "mode": mode,
        "mode_status": st,
        "budget_usd": data.get("budget_usd"),
        "target_goal_usd": data.get("target_goal_usd"),
        "updated_at": ts,
        "forced_active_bypass": FORCE_ACTIVE_BYPASS_ZERO_NODES,
        "fleet_workers_online": data.get("fleet_workers_online"),
        "fleet_linux_anchor_seen": data.get("fleet_linux_anchor_seen"),
    }


async def _vault_sessions_summary(redis: Any) -> dict[str, Any]:
    try:
        from nexus.services.session_vault import SessionHealth, get_commander_snapshot

        snap = await get_commander_snapshot(redis)
    except Exception as exc:
        log.debug("position_manager_vault_snapshot_failed", error=str(exc))
        return {"sessions_total": 0, "sessions_green": 0, "session_stems": []}

    green = sum(1 for r in snap if str(r.get("health")) == SessionHealth.GREEN.value)
    stems = [str(r.get("session_stem")) for r in snap if r.get("session_stem")][:48]
    return {
        "sessions_total": len(snap),
        "sessions_green": green,
        "session_stems": stems,
    }


def _vault_fields(vault: dict[str, Any]) -> dict[str, Any]:
    return {
        "vault_sessions_total": vault.get("sessions_total"),
        "vault_sessions_green": vault.get("sessions_green"),
    }


def _dedupe_positions(rows: list[dict[str, Any]], max_n: int = 48) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for r in rows:
        rid = str(r.get("id") or "")
        if not rid or rid in seen:
            continue
        seen.add(rid)
        out.append(r)
        if len(out) >= max_n:
            break
    return out


def _merge_new_in_front(
    new_rows: list[dict[str, Any]],
    existing: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not new_rows:
        return existing
    return _dedupe_positions(new_rows + existing, 48)


async def _pop_new_alpha_as_position_rows(
    redis: Any,
    vault: dict[str, Any],
    budget: float,
    *,
    turbo_boost: bool = False,
) -> list[dict[str, Any]]:
    """Treat unseen alpha-feed lines as new messages → Live Position rows."""
    if await _order_book_sync_blocks_signal_push(redis, turbo_boost=turbo_boost):
        log.warning(
            "order_book_sync",
            detail="deferring alpha signal rows until Polymarket order book mirror is present",
            turbo_boost=turbo_boost,
        )
        return []
    alpha = await load_alpha_feed(redis, 48)
    market_q = ""
    yes_px: float | None = None
    try:
        pm = await fetch_poly5m_market(POLY_EVENT_ID)
        if pm.get("market_found"):
            market_q = str(pm.get("question") or pm.get("market_question") or "Poly 5m")
            yp = float(pm.get("yes_price") or 0.0)
            yes_px = yp if yp > 0 else None
    except Exception as exc:
        log.debug("position_manager_poly5m_context_failed", error=str(exc))

    slice_usd = min(25.0, max(5.0, budget * 0.12))
    now = datetime.now(timezone.utc).isoformat()
    vf = _vault_fields(vault)
    out: list[dict[str, Any]] = []

    for ev in alpha:
        fp = _alpha_fingerprint(ev)
        try:
            already = await redis.sismember(SEEN_ALPHA_FP_KEY, fp)
        except Exception:
            already = True
        if already:
            continue
        try:
            await redis.sadd(SEEN_ALPHA_FP_KEY, fp)
            await redis.expire(SEEN_ALPHA_FP_KEY, 604800)
        except Exception:
            pass

        ch = str(ev.get("channel") or ev.get("channel_title") or "alpha")
        kind = str(ev.get("kind") or "ALPHA")
        detail = str(ev.get("detail") or "")[:180]
        row = {
            "id": f"alpha-msg-{fp}",
            "status": "NEW_SIGNAL",
            "side": "YES",
            "notional_usd": round(slice_usd, 2),
            "market": market_q or f"γ {POLY_EVENT_ID}",
            "yes_price": yes_px,
            "source_channel": ch[:80],
            "signal_kind": kind,
            "detail": detail,
            "opened_at": str(ev.get("ts") or ev.get("at") or now),
            "injected": True,
            "message_fingerprint": fp,
            **vf,
        }
        out.append(row)

    if out:
        log.info("live_positions_new_alpha_messages", count=len(out))
    return out


def _ledger_has_open_live(rows: list[dict[str, Any]]) -> bool:
    """Live execution only — paper / sim ledger rows are ignored."""
    for row in rows:
        ev = str(row.get("event") or "")
        if ev == "open_live":
            return True
    return False


async def _recent_ledger_tail(redis: Any, n: int = 30) -> list[dict[str, Any]]:
    try:
        raw = await redis.lrange("nexus:scalper:virtual_ledger", -n, -1)
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for line in raw or []:
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


async def _build_alpha_scan_positions(
    redis: Any,
    budget: float,
    vault: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    alpha = await load_alpha_feed(redis, 32)
    market_q = ""
    yes_px: float | None = None
    try:
        pm = await fetch_poly5m_market(POLY_EVENT_ID)
        if pm.get("market_found"):
            market_q = str(pm.get("question") or pm.get("market_question") or "Poly 5m")
            yp = float(pm.get("yes_price") or 0.0)
            yes_px = yp if yp > 0 else None
    except Exception as exc:
        log.debug("position_manager_poly5m_context_failed", error=str(exc))

    slice_usd = min(25.0, max(5.0, budget * 0.12))
    now = datetime.now(timezone.utc).isoformat()
    vf = _vault_fields(vault)

    if alpha:
        for i, ev in enumerate(alpha[:12]):
            ch = str(ev.get("channel") or ev.get("channel_title") or "alpha")
            kind = str(ev.get("kind") or "ALPHA")
            detail = str(ev.get("detail") or "")[:180]
            rows.append(
                {
                    "id": f"alpha-{i}",
                    "status": "LIVE_SCAN",
                    "side": "YES",
                    "notional_usd": round(slice_usd, 2),
                    "market": market_q or f"γ {POLY_EVENT_ID}",
                    "yes_price": yes_px,
                    "source_channel": ch[:80],
                    "signal_kind": kind,
                    "detail": detail,
                    "opened_at": str(ev.get("ts") or ev.get("at") or now),
                    "injected": True,
                    **vf,
                }
            )
        return rows

    rows.append(
        {
            "id": "alpha-probe-0",
            "status": "SCANNING",
            "side": "YES",
            "notional_usd": round(slice_usd, 2),
            "market": market_q or f"γ {POLY_EVENT_ID}",
            "yes_price": yes_px,
            "source_channel": "(fleet alpha scan)",
            "signal_kind": "ALPHA_PROBE",
            "detail": "Scanning alpha groups / OpenClaw feed for entries",
            "opened_at": now,
            "injected": True,
            **vf,
        }
    )
    return rows


async def _ensure_target_progress_initialized(redis: Any) -> dict[str, float]:
    """Profit-target progress bar in Redis: starts at ``0 / target_goal_usd``."""
    goal = float(target_goal_usd())
    budget = float(daily_budget_usd())
    default: dict[str, float] = {
        "toward_target_usd": 0.0,
        "goal_usd": goal,
        "daily_budget_usd": budget,
    }
    try:
        raw = await redis.get(POSITION_PROGRESS_KEY)
        if raw:
            data = json.loads(_decode(raw))
            if isinstance(data, dict):
                out = dict(default)
                for k in default:
                    if k in data:
                        out[k] = float(data[k])
                return out
    except Exception as exc:
        log.debug("position_progress_read_failed", error=str(exc))
    try:
        await redis.set(
            POSITION_PROGRESS_KEY,
            json.dumps(default, ensure_ascii=False),
            ex=86400 * 30,
        )
    except Exception as exc:
        log.debug("position_progress_init_failed", error=str(exc))
    return default


async def _persist_positions(
    redis: Any,
    rows: list[dict[str, Any]],
    *,
    source: str,
    ttl: int,
    vault: dict[str, Any],
    progress_tracker: dict[str, float] | None = None,
    fleet: dict[str, Any] | None = None,
) -> None:
    pt = progress_tracker or {}
    fl = fleet or {}
    meta = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "vault_sessions_total": vault.get("sessions_total"),
        "vault_sessions_green": vault.get("sessions_green"),
        "execution_mode": (
            "LIVE EXECUTION"
            if POSITION_LIVE_EXECUTION
            else "PAPER MODE (BLOCKED)"
        ),
        "mode": POSITION_OPS_MODE,
        "mode_status": POSITION_OPS_STATUS_ACTIVE,
        "forced_active_bypass": FORCE_ACTIVE_BYPASS_ZERO_NODES,
        "daily_budget_usd": daily_budget_usd(),
        "budget_usd": daily_budget_usd(),
        "target_goal_usd": target_goal_usd(),
        "toward_target_usd": float(pt.get("toward_target_usd", 0.0)),
        "goal_usd": float(pt.get("goal_usd", target_goal_usd())),
        "dashboard_display": DASHBOARD_DISPLAY_GRAPHICAL_TUI,
        "fleet_workers_online": int(fl.get("workers_online") or 0),
        "fleet_masters_online": int(fl.get("masters_online") or 0),
        "fleet_linux_anchor_seen": bool(fl.get("linux_anchor_seen")),
        "fleet_heartbeat_channel_key": HEARTBEAT_PUB_MIRROR_KEY,
    }
    payload = json.dumps(rows, ensure_ascii=False)
    await redis.set(LIVE_POSITIONS_KEY, payload, ex=ttl)
    await redis.set(_INJECT_META_KEY, json.dumps(meta, ensure_ascii=False), ex=ttl)


async def ensure_live_positions_injected(redis: Any, turbo: bool | None = None) -> list[dict[str, Any]]:
    """
    Maintain Redis ``nexus:live_trading:positions`` (dashboard Live Positions).

    - Heartbeat for UI online/offline.
    - Vault session counts from ``session_vault`` Redis index (``vault/sessions`` on disk).
    - New alpha-feed messages (unseen fingerprints) are prepended as rows.
    """
    turbo_on = turbo_mode_active() if turbo is None else turbo
    await _wait_for_fleet_workers(redis, turbo=turbo_on, interval_s=0.1 if turbo_on else 1.0)
    fleet = await _fleet_heartbeat_summary(redis)
    await touch_position_engine_heartbeat(redis, fleet=fleet, turbo=turbo_on)
    progress_tracker = await _ensure_target_progress_initialized(redis)
    vault = await _vault_sessions_summary(redis)
    budget = daily_budget_usd()
    new_alpha_rows = await _pop_new_alpha_as_position_rows(
        redis, vault, budget, turbo_boost=turbo_on
    )

    skip = os.getenv("NEXUS_LIVE_POSITION_INJECT", "1").strip().lower() in (
        "0",
        "false",
        "no",
        "off",
    )
    if skip:
        raw = await redis.get(LIVE_POSITIONS_KEY)
        base: list[dict[str, Any]] = []
        if raw:
            try:
                data = json.loads(_decode(raw))
                if isinstance(data, list):
                    base = data
            except json.JSONDecodeError:
                base = []
        merged = _merge_new_in_front(new_alpha_rows, base)
        if new_alpha_rows and merged != base:
            await _persist_positions(
                redis,
                merged,
                source="alpha_new_only",
                ttl=120,
                vault=vault,
                progress_tracker=progress_tracker,
                fleet=fleet,
            )
        return merged

    ledger = await _recent_ledger_tail(redis, 40)
    has_open = _ledger_has_open_live(ledger)

    raw_cached = await redis.get(LIVE_POSITIONS_KEY)
    cached: list[dict[str, Any]] = []
    if raw_cached:
        try:
            parsed = json.loads(_decode(raw_cached))
            if isinstance(parsed, list):
                cached = parsed
        except json.JSONDecodeError:
            cached = []

    if has_open:
        if cached and not any(c.get("injected") for c in cached):
            out = _merge_new_in_front(new_alpha_rows, cached)
            if new_alpha_rows:
                await _persist_positions(
                    redis,
                    out,
                    source="ledger+cache",
                    ttl=120,
                    vault=vault,
                    progress_tracker=progress_tracker,
                    fleet=fleet,
                )
            return out
        live_rows: list[dict[str, Any]] = []
        vf = _vault_fields(vault)
        for row in ledger:
            ev = str(row.get("event") or "")
            if ev == "open_live":
                live_rows.append(
                    {
                        "id": str(row.get("order_id") or row.get("id") or "")[:24] or "live",
                        "status": "OPEN",
                        "side": str(row.get("side") or "YES"),
                        "notional_usd": float(row.get("bet_usd") or 0.0),
                        "market": str(row.get("market") or "")[:200],
                        "yes_price": row.get("yes_price"),
                        "source_channel": str(row.get("channel_title") or "")[:120],
                        "signal_kind": "LEDGER",
                        "detail": "",
                        "opened_at": str(row.get("opened_at") or ""),
                        "injected": False,
                        **vf,
                    }
                )
        if live_rows:
            out = _merge_new_in_front(new_alpha_rows, live_rows)
            await _persist_positions(
                redis,
                out,
                source="ledger",
                ttl=120,
                vault=vault,
                progress_tracker=progress_tracker,
                fleet=fleet,
            )
            return out

    if cached and len(cached) > 0:
        out = _merge_new_in_front(new_alpha_rows, cached)
        if new_alpha_rows:
            await _persist_positions(
                redis,
                out,
                source="cache_refresh",
                ttl=90,
                vault=vault,
                progress_tracker=progress_tracker,
                fleet=fleet,
            )
        return out

    injected = await _build_alpha_scan_positions(redis, budget, vault)
    out = _merge_new_in_front(new_alpha_rows, injected)
    await _persist_positions(
        redis,
        out,
        source="alpha_scan",
        ttl=90,
        vault=vault,
        progress_tracker=progress_tracker,
        fleet=fleet,
    )
    log.info("live_positions_injected", count=len(out))
    return out


async def _cli_run(args: argparse.Namespace) -> int:
    global POSITION_LIVE_EXECUTION

    from nexus.shared import redis_util

    redis_util.apply_redis_url_to_environment()
    url = (os.getenv("REDIS_URL") or "").strip() or redis_util.default_redis_url_string()
    os.environ["NEXUS_POSITION_DAILY_BUDGET_USD"] = str(args.budget)
    os.environ["NEXUS_POSITION_TARGET_GOAL_USD"] = str(args.target)
    POSITION_LIVE_EXECUTION = bool(args.live)
    if args.turbo:
        os.environ["NEXUS_POSITION_TURBO"] = "1"
    else:
        os.environ.pop("NEXUS_POSITION_TURBO", None)

    client = await redis_util.connect_async_redis_with_fallback(url)

    watch = bool(args.watch)
    interval = 0.1 if args.turbo else 1.0

    try:
        while True:
            rows = await ensure_live_positions_injected(client, turbo=turbo_mode_active())
            hb = await position_engine_heartbeat_snapshot(client)
            if not watch:
                print(json.dumps({"ok": True, "positions": len(rows), "heartbeat": hb}, indent=2))
                return 0
            log.info(
                "position_manager_tick",
                positions=len(rows),
                heartbeat=hb.get("status"),
                interval_s=interval,
            )
            await asyncio.sleep(interval)
    except KeyboardInterrupt:
        return 0
    finally:
        try:
            await client.aclose()
        except Exception:
            pass


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Nexus position manager — sync Live Positions + heartbeat.")
    p.add_argument("--budget", type=float, default=100.0, help="Daily budget USD (default 100)")
    p.add_argument("--target", type=float, default=1000.0, help="Profit target USD (default 1000)")
    p.add_argument(
        "--live",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Live execution meta (default: on; use --no-live for paper/disabled)",
    )
    p.add_argument(
        "--turbo",
        action="store_true",
        help="Skip worker heartbeat wait, publish turbo flag; with --watch poll every 0.1s (else 1.0s)",
    )
    p.add_argument(
        "--watch",
        action="store_true",
        help="Run until Ctrl+C; refresh every 1.0s, or 0.1s with --turbo",
    )
    return p


def main() -> int:
    print("[DEBUG] Logging to logs/nexus_runtime.log is ACTIVE")
    parser = _build_arg_parser()
    args, _ = parser.parse_known_args()
    if args.budget <= 0 or args.target <= 0:
        print("budget and target must be positive", file=sys.stderr)
        return 2
    return asyncio.run(_cli_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
