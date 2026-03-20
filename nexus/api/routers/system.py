"""
nexus/api/routers/system.py — System-wide Emergency Kill-Switch (PANIC) + Black Box

Endpoints
---------
POST /api/system/panic              — Engage panic mode: sets global Redis kill-switch,
                                      broadcasts TERMINATE to all workers via Pub/Sub,
                                      fires urgent Telegram notification.
POST /api/system/panic/reset        — Clear panic state (admin recovery).
GET  /api/system/panic/state        — Read current panic flag + metadata.
GET  /api/system/blackbox/status    — Check whether a Black Box dump file exists and
                                      return the path and size of the latest dump.
GET  /api/system/blackbox/download  — Stream the latest dump file as JSON for download.

Performance target
------------------
POST /panic response time < 100 ms.  The critical path:
  1. redis.set()       ~1 ms
  2. redis.publish()   ~1 ms
  3. psutil stats      ~5 ms (non-blocking cpu_percent)
  4. redis.scan()      ~5 ms
  5. Telegram          async fire-and-forget (background task)
  Total: ~15 ms typical.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psutil
import structlog
from fastapi import APIRouter
from fastapi.responses import FileResponse, JSONResponse

from nexus.api.dependencies import RedisDep
from nexus.utils.blackbox import BLACKBOX_DIR

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/system", tags=["system"])

PANIC_KEY     = "SYSTEM_STATE:PANIC"
PANIC_META    = "SYSTEM_STATE:PANIC_META"
PANIC_CHANNEL = "nexus:system:control"


# ── Engage panic ───────────────────────────────────────────────────────────────

@router.post("/panic", summary="Engage system-wide emergency kill-switch")
async def trigger_panic(redis: RedisDep) -> dict[str, Any]:
    """
    Sub-100 ms kill-switch:

    1. Sets ``SYSTEM_STATE:PANIC = 'true'`` in Redis — workers check this key
       before executing every task.
    2. Broadcasts ``TERMINATE`` on ``nexus:system:control`` Pub/Sub channel
       so worker subscribers can stop immediately.
    3. Collects master-node CPU/RAM stats + last known trade price.
    4. Fires an urgent Telegram report in a background task (non-blocking).
    """
    t_start = time.monotonic()

    # ── 1. Set global panic flag ───────────────────────────────────────────────
    await redis.set(PANIC_KEY, "true")
    activated_at = datetime.now(timezone.utc).isoformat()

    await redis.set(
        PANIC_META,
        json.dumps({
            "activated_at": activated_at,
            "reason":       "Manual Trigger",
            "activated_by": "Dashboard",
        }),
    )

    # ── 2. Broadcast TERMINATE to all worker subscribers ──────────────────────
    await redis.publish(PANIC_CHANNEL, "TERMINATE")

    # ── 3. Collect system stats (non-blocking) ─────────────────────────────────
    cpu_percent  = psutil.cpu_percent(interval=None)
    vm           = psutil.virtual_memory()
    ram_used_mb  = round(vm.used  / 1_048_576)
    ram_total_mb = round(vm.total / 1_048_576)

    # Last known trade price (most recent paper/live trade in Redis)
    last_trade_price = "N/A"
    try:
        from nexus.trading.config import PAPER_TRADING_REDIS_KEY
        raw = await redis.lindex(PAPER_TRADING_REDIS_KEY, -1)
        if raw:
            trade = json.loads(raw)
            last_trade_price = f"${trade.get('price', 'N/A')}"
    except Exception:
        pass

    # Active worker IDs from heartbeat keys
    active_workers: list[str] = []
    try:
        cur = 0
        while True:
            cur, keys = await redis.scan(cur, match="nexus:heartbeat:*", count=100)
            active_workers.extend(k.replace("nexus:heartbeat:", "") for k in keys)
            if cur == 0:
                break
    except Exception:
        pass

    # ── 4. Fire-and-forget Telegram notification ───────────────────────────────
    asyncio.create_task(
        _send_panic_telegram(
            activated_at=activated_at,
            last_trade_price=last_trade_price,
            active_workers=active_workers,
            cpu_percent=cpu_percent,
            ram_used_mb=ram_used_mb,
            ram_total_mb=ram_total_mb,
        ),
        name="panic_telegram_notify",
    )

    elapsed_ms = round((time.monotonic() - t_start) * 1000)
    log.critical(
        "system_panic_engaged",
        elapsed_ms=elapsed_ms,
        workers_notified=len(active_workers),
        cpu_percent=cpu_percent,
        ram_used_mb=ram_used_mb,
    )

    return {
        "status":            "PANIC_ENGAGED",
        "activated_at":      activated_at,
        "workers_terminated": active_workers,
        "elapsed_ms":        elapsed_ms,
        "cpu_percent":       cpu_percent,
        "ram_used_mb":       ram_used_mb,
        "last_trade_price":  last_trade_price,
    }


# ── Reset panic ────────────────────────────────────────────────────────────────

@router.post("/panic/reset", summary="Clear panic state — admin only")
async def reset_panic(redis: RedisDep) -> dict[str, str]:
    """
    Clear the ``SYSTEM_STATE:PANIC`` flag and broadcast ``RESUME`` to all
    worker subscribers so they accept new tasks again.
    """
    await redis.delete(PANIC_KEY, PANIC_META)
    await redis.publish(PANIC_CHANNEL, "RESUME")
    log.info("system_panic_reset")
    return {
        "status":  "PANIC_CLEARED",
        "message": "System restored to normal operation",
    }


# ── Panic state probe ──────────────────────────────────────────────────────────

@router.get("/panic/state", summary="Get current panic state + metadata")
async def get_panic_state(redis: RedisDep) -> dict[str, Any]:
    """Return whether the system is in panic mode along with activation metadata."""
    is_panic = (await redis.get(PANIC_KEY)) == "true"
    meta: dict[str, Any] = {}
    if is_panic:
        raw_meta = await redis.get(PANIC_META)
        if raw_meta:
            try:
                meta = json.loads(raw_meta)
            except Exception:
                pass
    return {"panic": is_panic, **meta}


# ── Telegram notification (background) ────────────────────────────────────────

async def _send_panic_telegram(
    activated_at: str,
    last_trade_price: str,
    active_workers: list[str],
    cpu_percent: float,
    ram_used_mb: int,
    ram_total_mb: int,
) -> None:
    """
    Sends an urgent panic report to the Telegram admin chat.

    Runs as a background asyncio task so it never blocks the panic response.
    Message is formatted in MarkdownV2 and includes all requested fields:
      - Reason (Manual Trigger)
      - Last known trade price
      - Active Workers status
      - Master node CPU / RAM
    """
    try:
        from nexus.shared.notifications.providers.telegram import TelegramProvider, _esc

        provider   = TelegramProvider()
        workers_str = ", ".join(active_workers) if active_workers else "None"
        ts          = activated_at[:19].replace("T", " ")

        lines = [
            "🚨🛑 *SYSTEM PANIC ENGAGED*",
            "",
            "⚠️ _Emergency kill\\-switch triggered\\. All trading halted immediately\\._",
            "",
            f"📋 *Reason:* `Manual Trigger`",
            f"⏰ *Time:* `{_esc(ts)} UTC`",
            f"💰 *Last Trade Price:* `{_esc(last_trade_price)}`",
            f"🖥️ *Active Workers:* `{_esc(workers_str)}`",
            "",
            f"💻 *Master CPU:* `{cpu_percent:.1f}%`",
            f"🧠 *Master RAM:* `{ram_used_mb:,} MB / {ram_total_mb:,} MB`",
            "",
            "🔴 _All Worker nodes received TERMINATE signal\\._",
            "_Restore via Admin Settings → Reset System / איפוס מערכת\\._",
        ]
        await provider.send_message("\n".join(lines))
        log.info("panic_telegram_sent", workers=workers_str, cpu=cpu_percent)
    except Exception as exc:
        log.error("panic_telegram_failed", error=str(exc))


# ── Black Box endpoints ────────────────────────────────────────────────────────

def _latest_dump() -> Path | None:
    """Return the Path of the most-recently written crash dump, or None."""
    if not BLACKBOX_DIR.is_dir():
        return None
    dumps = sorted(
        BLACKBOX_DIR.glob("crash_dump_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return dumps[0] if dumps else None


@router.get("/blackbox/status", summary="Check whether a Black Box dump exists")
async def blackbox_status() -> dict[str, Any]:
    """
    Returns metadata about the latest crash dump without serving the file.

    The dashboard uses this to decide whether to show the download button.
    """
    latest = _latest_dump()
    if latest is None:
        return {"exists": False}

    stat = latest.stat()
    return {
        "exists":        True,
        "filename":      latest.name,
        "path":          str(latest),
        "size_bytes":    stat.st_size,
        "modified_utc":  datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
    }


@router.get("/blackbox/download", summary="Download the latest Black Box crash dump")
async def blackbox_download() -> FileResponse:
    """
    Stream the latest crash-dump JSON file to the caller.

    The ``Content-Disposition: attachment`` header causes browsers to trigger
    a Save-As dialog rather than rendering the JSON inline.
    """
    latest = _latest_dump()
    if latest is None:
        return JSONResponse(
            status_code=404,
            content={"detail": "No Black Box dump file found. Trigger a crash or wait for a critical failure."},
        )  # type: ignore[return-value]

    log.info("blackbox_download_served", filename=latest.name, size_bytes=latest.stat().st_size)
    return FileResponse(
        path=str(latest),
        media_type="application/json",
        filename=latest.name,
        headers={"Content-Disposition": f'attachment; filename="{latest.name}"'},
    )
