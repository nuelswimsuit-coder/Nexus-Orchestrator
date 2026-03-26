"""
Management Ahu — Bridge API Router
Reads directly from the external Management Ahu project at
C:\\Users\\Yarin\\Desktop\\Mangement Ahu without copying any of its code.

Endpoints:
  GET  /api/ahu/status           — bot process running/stopped
  POST /api/ahu/bot/start        — launch run_bot.py
  POST /api/ahu/bot/stop         — kill bot process
  GET  /api/ahu/sessions         — session counts per category
  GET  /api/ahu/stats            — DB stats (users, premium, enrollments, targets)
  GET  /api/ahu/targets          — source + target groups from DB
  GET  /api/ahu/logs             — last N lines from telefix.log
  WS   /api/ahu/logs/stream      — live log streaming via WebSocket
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any

import structlog
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/ahu", tags=["ahu"])

# ── Paths ──────────────────────────────────────────────────────────────────────
AHU_ROOT = Path(r"C:\Users\Yarin\Desktop\Mangement Ahu")
AHU_DB = AHU_ROOT / "data" / "telefix.db"
AHU_SESSIONS = AHU_ROOT / "sessions"
AHU_LOG = AHU_ROOT / "logs" / "telefix.log"
AHU_BOT = AHU_ROOT / "run_bot.py"

# Categories that exist as subdirectories under sessions/
SESSION_CATEGORIES = ["managers", "adders", "frozen", "bots", "spammers"]

# Global reference to the bot subprocess (one at a time)
_bot_process: subprocess.Popen | None = None


# ── Helpers ────────────────────────────────────────────────────────────────────

def _db_connect() -> sqlite3.Connection | None:
    """Return a read-only SQLite connection to telefix.db, or None if absent."""
    if not AHU_DB.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{AHU_DB}?mode=ro", uri=True, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as exc:
        log.warning("ahu_db_connect_failed", error=str(exc))
        return None


def _query(sql: str, params: tuple = ()) -> list[dict]:
    conn = _db_connect()
    if conn is None:
        return []
    try:
        cur = conn.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
        return rows
    except Exception as exc:
        log.warning("ahu_db_query_failed", sql=sql, error=str(exc))
        return []
    finally:
        conn.close()


def _query_one(sql: str, params: tuple = ()) -> dict | None:
    rows = _query(sql, params)
    return rows[0] if rows else None


def _scan_sessions() -> dict[str, Any]:
    """Scan the sessions directory and return counts + file lists per category."""
    result: dict[str, Any] = {}
    if not AHU_SESSIONS.exists():
        return result
    for cat in SESSION_CATEGORIES:
        cat_dir = AHU_SESSIONS / cat
        if not cat_dir.exists():
            result[cat] = {"count": 0, "sessions": []}
            continue
        sessions = []
        for item in cat_dir.iterdir():
            if item.suffix == ".session":
                sessions.append(item.stem)
            elif item.is_dir():
                # Numbered subdirs like 972523092452/
                inner = list(item.glob("*.session"))
                if inner:
                    sessions.append(item.name)
        result[cat] = {"count": len(sessions), "sessions": sorted(sessions)}
    return result


def _tail_log(n: int = 150) -> list[str]:
    """Return the last n lines from telefix.log."""
    if not AHU_LOG.exists():
        return []
    try:
        with open(AHU_LOG, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        return [l.rstrip() for l in lines[-n:]]
    except Exception as exc:
        log.warning("ahu_log_read_failed", error=str(exc))
        return []


def _bot_is_running() -> bool:
    global _bot_process
    if _bot_process is None:
        return False
    poll = _bot_process.poll()
    if poll is not None:
        _bot_process = None
        return False
    return True


# ── Status ─────────────────────────────────────────────────────────────────────

@router.get("/status")
async def get_status() -> JSONResponse:
    running = _bot_is_running()
    db_ok = AHU_DB.exists()
    sessions_ok = AHU_SESSIONS.exists()

    session_counts: dict[str, int] = {}
    if sessions_ok:
        cats = _scan_sessions()
        session_counts = {k: v["count"] for k, v in cats.items()}

    total_sessions = sum(session_counts.values())

    return JSONResponse({
        "bot_running": running,
        "bot_pid": _bot_process.pid if running and _bot_process else None,
        "db_available": db_ok,
        "sessions_available": sessions_ok,
        "total_sessions": total_sessions,
        "session_counts": session_counts,
        "ahu_root": str(AHU_ROOT),
    })


# ── Bot control ────────────────────────────────────────────────────────────────

@router.post("/bot/start")
async def start_bot() -> JSONResponse:
    global _bot_process
    if _bot_is_running():
        return JSONResponse({"ok": False, "detail": "Bot is already running", "pid": _bot_process.pid})
    if not AHU_BOT.exists():
        return JSONResponse({"ok": False, "detail": f"run_bot.py not found at {AHU_BOT}"}, status_code=404)
    try:
        _bot_process = subprocess.Popen(
            [sys.executable, str(AHU_BOT)],
            cwd=str(AHU_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        log.info("ahu_bot_started", pid=_bot_process.pid)
        return JSONResponse({"ok": True, "pid": _bot_process.pid})
    except Exception as exc:
        log.error("ahu_bot_start_failed", error=str(exc))
        return JSONResponse({"ok": False, "detail": str(exc)}, status_code=500)


@router.post("/bot/stop")
async def stop_bot() -> JSONResponse:
    global _bot_process
    if not _bot_is_running():
        return JSONResponse({"ok": False, "detail": "Bot is not running"})
    try:
        pid = _bot_process.pid
        _bot_process.terminate()
        try:
            _bot_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _bot_process.kill()
        _bot_process = None
        log.info("ahu_bot_stopped", pid=pid)
        return JSONResponse({"ok": True, "pid": pid})
    except Exception as exc:
        log.error("ahu_bot_stop_failed", error=str(exc))
        return JSONResponse({"ok": False, "detail": str(exc)}, status_code=500)


# ── Sessions ───────────────────────────────────────────────────────────────────

@router.get("/sessions")
async def get_sessions() -> JSONResponse:
    data = _scan_sessions()
    return JSONResponse(data)


# ── Stats ──────────────────────────────────────────────────────────────────────

@router.get("/stats")
async def get_stats() -> JSONResponse:
    # scraped_users
    users_row = _query_one("SELECT COUNT(*) as total, SUM(is_premium) as premium FROM scraped_users")
    total_users = int(users_row["total"]) if users_row else 0
    premium_users = int(users_row["premium"] or 0) if users_row else 0

    # distinct sources
    sources_row = _query_one("SELECT COUNT(DISTINCT source_group) as cnt FROM scraped_users")
    total_sources = int(sources_row["cnt"]) if sources_row else 0

    # targets
    targets_rows = _query("SELECT role, COUNT(*) as cnt FROM targets GROUP BY role")
    targets_by_role: dict[str, int] = {}
    for r in targets_rows:
        targets_by_role[r["role"]] = int(r["cnt"])

    # enrollments
    enroll_row = _query_one("SELECT COUNT(*) as total FROM enrollments")
    total_enrollments = int(enroll_row["total"]) if enroll_row else 0

    enroll_by_status = _query("SELECT status, COUNT(*) as cnt FROM enrollments GROUP BY status")
    enrollment_status: dict[str, int] = {r["status"]: int(r["cnt"]) for r in enroll_by_status}

    # last run metrics
    metrics = _query("SELECT key, value FROM settings WHERE key LIKE 'last_run:%'")
    last_runs: dict[str, str] = {r["key"].replace("last_run:", ""): r["value"] for r in metrics}

    return JSONResponse({
        "users": {
            "total": total_users,
            "premium": premium_users,
            "sources": total_sources,
            "premium_pct": round(premium_users * 100 / total_users, 1) if total_users > 0 else 0,
        },
        "targets": targets_by_role,
        "enrollments": {
            "total": total_enrollments,
            "by_status": enrollment_status,
        },
        "last_runs": last_runs,
    })


# ── Targets ────────────────────────────────────────────────────────────────────

@router.get("/targets")
async def get_targets() -> JSONResponse:
    rows = _query("SELECT id, title, link, role FROM targets ORDER BY role, id")
    return JSONResponse({"targets": rows, "count": len(rows)})


# ── Scraped users (paginated) ──────────────────────────────────────────────────

@router.get("/scraped-users")
async def get_scraped_users(limit: int = 50, offset: int = 0) -> JSONResponse:
    rows = _query(
        "SELECT user_id, username, source_group, is_premium, added_at "
        "FROM scraped_users ORDER BY added_at DESC LIMIT ? OFFSET ?",
        (limit, offset),
    )
    total_row = _query_one("SELECT COUNT(*) as cnt FROM scraped_users")
    total = int(total_row["cnt"]) if total_row else 0
    return JSONResponse({"users": rows, "total": total, "limit": limit, "offset": offset})


# ── Logs ───────────────────────────────────────────────────────────────────────

@router.get("/logs")
async def get_logs(lines: int = 150) -> JSONResponse:
    log_lines = _tail_log(min(lines, 500))
    return JSONResponse({"lines": log_lines, "count": len(log_lines)})


@router.websocket("/logs/stream")
async def stream_logs(ws: WebSocket) -> None:
    await ws.accept()
    if not AHU_LOG.exists():
        await ws.send_text("[ahu] telefix.log not found")
        await ws.close()
        return

    # Send last 50 lines as initial burst
    initial = _tail_log(50)
    for line in initial:
        await ws.send_text(line)

    # Then tail new lines
    try:
        with open(AHU_LOG, "r", encoding="utf-8", errors="replace") as f:
            f.seek(0, 2)  # seek to end
            while True:
                line = f.readline()
                if line:
                    await ws.send_text(line.rstrip())
                else:
                    await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.warning("ahu_log_stream_error", error=str(exc))
        try:
            await ws.close()
        except Exception:
            pass
