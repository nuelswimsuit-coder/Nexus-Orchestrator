"""
Telegram Bot — Nexus Command Center.

This bot serves as the mobile version of the React dashboard.
It handles HITL approvals AND provides live statistics via a persistent
Reply Keyboard menu.

Commands
--------
/start    — Show the persistent Reply Keyboard menu
/dashboard — Send a direct link to the dashboard
/help     — Show available commands

Menu buttons (Reply Keyboard)
------------------------------
📊 Current Stats    — Telefix DB stats (groups, users, sessions)
🖥️ Cluster Health  — Worker nodes with CPU/RAM/IP
💰 Profit Report   — Latest ROI forecast from the business audit
🛠️ Active Tasks    — Pending HITL approvals + queue depth

Architecture
------------
All data is fetched from the FastAPI Control Center (same source of truth
as the React dashboard).  The bot does NOT access Redis or the DB directly.

Data sources:
  GET /api/business/stats          → Current Stats
  GET /api/cluster/status          → Cluster Health
  GET /api/business/report         → Profit Report
  GET /api/hitl/pending            → Active Tasks
  GET /api/business/scrape-status  → Scrape status
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal as _signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

# Windows / Python 3.10+ fix: the default ProactorEventLoop does not support
# all asyncio features used by ARQ/aiogram.  Switch to SelectorEventLoop and
# ensure a loop exists in the main thread before anything else runs.
if sys.platform == "win32":
    # Required for Windows + Python 3.8+ compatibility with aiohttp/arq
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
elif os.environ.get("ENVIRONMENT", "PRODUCTION").upper() == "PRODUCTION":
    try:
        import uvloop  # type: ignore[import-not-found]

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except Exception:
        pass

try:
    asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

# Force-load .env before any nexus imports (same pattern as start_master.py)
_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"
if _ENV_FILE.exists():
    for _line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if not _line or _line.startswith("#") or "=" not in _line:
            continue
        _key, _, _val = _line.partition("=")
        _key = _key.strip()
        _val = _val.strip().split("#")[0].strip()
        if _key and _key not in os.environ:
            os.environ[_key] = _val

import httpx  # noqa: E402
import structlog  # noqa: E402
from aiogram import Bot, F  # noqa: E402
from aiogram import Dispatcher as TgDispatcher  # noqa: E402
from aiogram.client.default import DefaultBotProperties  # noqa: E402
from aiogram.enums import ParseMode  # noqa: E402
from aiogram.filters import Command, CommandStart  # noqa: E402
from aiogram.types import (  # noqa: E402
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

from nexus.shared.config import settings  # noqa: E402
from nexus.shared.logging_config import configure_logging  # noqa: E402

log = structlog.get_logger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
_host = "localhost" if settings.api_host == "0.0.0.0" else settings.api_host
API_BASE = f"http://{_host}:{settings.api_port}"

DASHBOARD_URL = (
    os.environ.get("TELEGRAM_DASHBOARD_URL", "")
    or settings.telegram_dashboard_url
    or "http://localhost:3000"
)

# ── TeleFix V2: Hebrew/English Localization ───────────────────────────────────
from nexus.shared.telegram_strings import (  # noqa: E402
    get_string, format_stats_report, format_cluster_report, format_wallet_report,
    create_main_menu, create_approval_keyboard, format_decision_request,
    format_confirmation_update, Language,
)

# Default language (Hebrew for TeleFix OS)
BOT_LANGUAGE: Language = "he"

# ── Legacy Reply Keyboard button labels (V1 — kept for handle_menu_button & /help) ──
BTN_STATS     = "📊 Current Stats"
BTN_CLUSTER   = "🖥️ Cluster Health"
BTN_PROFIT    = "💰 Profit Report"
BTN_TASKS     = "🛠️ Active Tasks"
BTN_INCUBATOR = "🧬 Evolution Engine"

# ── Inline Keyboard Menu (replaces old Reply Keyboard) ────────────────────────
def get_main_menu():
    """Get the main inline keyboard menu in the current language."""
    return create_main_menu(BOT_LANGUAGE)


def get_start_menu() -> InlineKeyboardMarkup:
    """
    Primary 2×2 inline keyboard shown on /start.

    Designed as a professional trading terminal control panel — each button
    maps directly to an orchestrator action via its callback_data.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 סטטוס מערכת",          callback_data="status"),
            InlineKeyboardButton(text="⚡ LIVE OPS - REAL-TIME EXECUTION", callback_data="live_ops"),
        ],
        [
            InlineKeyboardButton(text="🛡️ בדיקת חוסן (Sentinel)", callback_data="check_sentinel"),
            InlineKeyboardButton(text="🛑 עצירת חירום (PANIC)",   callback_data="panic_stop"),
        ],
    ])


# ── API helpers ────────────────────────────────────────────────────────────────

async def _api_get(path: str) -> dict | None:
    """Fetch JSON from the FastAPI server.  Returns None on any error."""
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.get(f"{API_BASE}{path}")
            resp.raise_for_status()
            return resp.json()
    except Exception as exc:
        log.warning("telegram_bot_api_error", path=path, error=str(exc))
        return None


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _esc(text: str) -> str:
    """Escape text for MarkdownV2."""
    import re
    return re.sub(r"([_\*\[\]\(\)~`>#+\-=|{}.!\\])", r"\\\1", str(text))


# ── Report formatters ──────────────────────────────────────────────────────────

def _fmt_current_stats(data: dict) -> str:
    """Format /api/business/stats into a Telegram message."""
    db_ok = "✅ Live" if data.get("db_available") else "❌ Offline"
    active  = data.get("active_sessions", 0)
    frozen  = data.get("frozen_sessions", 0)
    total_s = active + frozen
    health  = f"{active}/{total_s}" if total_s else "0/0"
    health_icon = "🟢" if active > 0 else "🔴"

    lines = [
        "📊 *CURRENT STATS*",
        f"🗄 Database: {_esc(db_ok)}",
        "",
        "👥 *Groups & Targets*",
        f"  • Managed groups: `{data.get('total_managed_groups', 0)}`",
        f"  • Source groups:  `{data.get('source_groups', 0)}`",
        f"  • Target groups:  `{data.get('target_groups', 0)}`",
        "",
        "👤 *Users*",
        f"  • Scraped \\(total\\): `{data.get('total_scraped_users', 0)}`",
        f"  • Pipeline:         `{data.get('total_users_pipeline', 0)}`",
        "",
        f"🤖 *Sessions* {health_icon}",
        f"  • Active:  `{active}`",
        f"  • Frozen:  `{frozen}`",
        f"  • Managers: `{data.get('manager_sessions', 0)}`",
        f"  • Health:  `{health}`",
        "",
    ]

    if data.get("last_scraper_run"):
        lines.append(f"🕐 Last scrape: `{_esc(data['last_scraper_run'])}`")
    if data.get("last_adder_run"):
        lines.append(f"🕐 Last adder:  `{_esc(data['last_adder_run'])}`")

    lines += ["", f"🔄 _Updated: {_esc(_now_utc())}_"]
    return "\n".join(lines)


def _fmt_cluster_health(data: dict) -> str:
    """Format /api/cluster/status into a Telegram message."""
    nodes = data.get("nodes", [])
    queues = data.get("queues", [])

    lines = ["🖥️ *CLUSTER HEALTH*", ""]

    if not nodes:
        lines.append("⚠️ No nodes reporting heartbeats\\.")
    else:
        for node in nodes:
            role   = node.get("role", "?").upper()
            nid    = node.get("node_id", "?")
            online = node.get("online", False)
            cpu    = node.get("cpu_percent", 0)
            ram    = node.get("ram_used_mb", 0)
            ram_t  = node.get("ram_total_mb", 0)
            ip     = node.get("local_ip", "—")
            gpu    = node.get("gpu_model", "N/A")
            jobs   = node.get("active_jobs", 0)

            status_icon = "🟢" if online else "🔴"
            role_icon   = "👑" if role == "MASTER" else "⚙️"

            cpu_bar = _cpu_bar(cpu)
            ram_pct = (ram / ram_t * 100) if ram_t > 0 else 0

            lines += [
                f"{status_icon} {role_icon} *{_esc(nid)}*",
                f"  🌐 IP:  `{_esc(ip)}`",
                f"  🖥️ CPU: {cpu_bar} `{cpu:.0f}%`",
                f"  💾 RAM: `{ram:.0f} / {ram_t:.0f} MB` \\(`{ram_pct:.0f}%`\\)",
                f"  🎮 GPU: `{_esc(gpu)}`",
                f"  📋 Jobs: `{jobs}`",
                "",
            ]

    if queues:
        lines.append("📬 *Task Queues*")
        for q in queues:
            pending = q.get("pending_jobs", 0)
            q_icon  = "🟡" if pending > 0 else "✅"
            lines.append(
                f"  {q_icon} `{_esc(q.get('queue_name', '?'))}`: `{pending}` pending"
            )
        lines.append("")

    caps = data.get("master_resource_caps", {})
    if caps:
        lines += [
            "⚙️ *Master Caps*",
            f"  CPU cap: `{caps.get('cpu_cap_percent', 0):.0f}%`",
            f"  RAM cap: `{caps.get('ram_cap_mb', 0):.0f} MB`",
            "",
        ]

    lines.append(f"🔄 _Updated: {_esc(_now_utc())}_")
    return "\n".join(lines)


def _fmt_profit_report(data: dict) -> str:
    """Format /api/business/report into a Telegram message."""
    db_ok = "✅" if data.get("db_available") else "❌ DB offline"
    roi   = data.get("estimated_roi", 0)
    roi_s = f"+{roi}%" if roi >= 0 else f"{roi}%"
    roi_icon = "📈" if roi >= 0 else "📉"

    active  = data.get("active_sessions", 0)
    frozen  = data.get("frozen_sessions", 0)
    health  = data.get("health_ratio", 0)
    health_icon = "🟢" if health >= 60 else ("🟡" if health >= 30 else "🔴")

    forecast = data.get("forecast_history", [])
    forecast_str = "  " + " · ".join(_esc(d) for d in forecast[:5]) if forecast else "  _No data_"

    lines = [
        f"💰 *PROFIT REPORT* {db_ok}",
        f"📅 Window: `{data.get('window_hours', 24)}h`",
        "",
        "📊 *Revenue Metrics*",
        f"  {roi_icon} ROI:              `{_esc(roi_s)}`",
        f"  👤 New scraped \\(24h\\): `{data.get('new_scraped_users', 0)}`",
        f"  📦 Pipeline users:    `{data.get('total_pipeline', 0)}`",
        f"  🎯 Target groups:     `{data.get('target_groups', 0)}`",
        f"  📡 Source groups:     `{data.get('source_groups', 0)}`",
        "",
        f"🤖 *Session Health* {health_icon}",
        f"  Active:   `{active}`",
        f"  Frozen:   `{frozen}`",
        f"  Managers: `{data.get('manager_sessions', 0)}`",
        f"  Ratio:    `{health:.0f}%`",
        "",
        "📅 *Forecast History*",
        forecast_str,
        "",
    ]

    if data.get("last_scraper_run"):
        lines.append(f"🕐 Last scrape: `{_esc(data['last_scraper_run'])}`")
    if data.get("last_adder_run"):
        lines.append(f"🕐 Last adder:  `{_esc(data['last_adder_run'])}`")

    gen = data.get("generated_at", "")
    if gen:
        try:
            gen_dt = datetime.fromisoformat(gen.replace("Z", "+00:00"))
            gen_str = gen_dt.strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            gen_str = gen
        lines.append(f"\n📋 _Report generated: {_esc(gen_str)}_")

    lines.append(f"🔄 _Updated: {_esc(_now_utc())}_")
    return "\n".join(lines)


def _fmt_active_tasks(hitl_data: dict, scrape_data: dict | None) -> str:
    """Format HITL pending + scrape status into a Telegram message."""
    items = hitl_data.get("items", [])
    total = hitl_data.get("total", 0)

    lines = ["🛠️ *ACTIVE TASKS*", ""]

    # ── HITL pending ──────────────────────────────────────────────────────────
    if total == 0:
        lines.append("✅ No tasks awaiting approval\\.")
    else:
        lines.append(f"⚠️ *{total} task\\(s\\) awaiting approval:*")
        lines.append("")
        for item in items[:5]:  # cap at 5 to avoid message length limit
            task_type = _esc(item.get("task_type", "?"))
            task_id   = _esc(item.get("task_id", "?")[:16])
            context   = _esc(item.get("context", "")[:120])
            req_id    = item.get("request_id", "")
            lines += [
                f"🔔 *{task_type}*",
                f"  ID: `{task_id}`",
                f"  📝 {context}\\.\\.\\."
                if len(item.get("context", "")) > 120
                else f"  📝 {context}",
                f"  ✅ /approve\\_{_esc(req_id[:8])}",
                "",
            ]
        if total > 5:
            lines.append(f"_\\.\\.\\. and {total - 5} more\\. Open the dashboard to see all\\._")

    lines.append("")

    # ── Scrape status ─────────────────────────────────────────────────────────
    if scrape_data:
        s_status = scrape_data.get("status", "idle")
        s_detail = scrape_data.get("detail", "")
        s_icon   = {
            "running":       "🔄",
            "completed":     "✅",
            "failed":        "❌",
            "low_resources": "⚠️",
            "idle":          "💤",
        }.get(s_status, "❓")
        lines += [
            "🔍 *Scraper Status*",
            f"  {s_icon} `{_esc(s_status)}`",
        ]
        if s_detail:
            lines.append(f"  {_esc(s_detail[:100])}")
        lines.append("")

    lines.append(f"🔄 _Updated: {_esc(_now_utc())}_")
    return "\n".join(lines)


def _cpu_bar(pct: float, width: int = 8) -> str:
    """Render a compact ASCII progress bar for CPU usage."""
    filled = int(round(pct / 100 * width))
    bar = "█" * filled + "░" * (width - filled)
    return f"`{bar}`"


# ── Pre-flight helpers ────────────────────────────────────────────────────────

async def _preflight_cleanup(token: str) -> None:
    """
    Pre-flight: delete any active Telegram webhook and drop pending updates
    to prevent 409 Conflict errors caused by lingering sessions.
    A temporary Bot instance is created solely for the cleanup call so the
    main bot object is always started from a clean slate.
    """
    print("[PREFLIGHT] מנקה חיבורים קיימים לפני האתחול...")
    log.info("telegram_bot_preflight_start")
    try:
        tmp = Bot(
            token=token,
            default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN_V2),
        )
        await tmp.delete_webhook(drop_pending_updates=True)
        await tmp.session.close()
        log.info("telegram_bot_preflight_webhook_cleared")
    except Exception as exc:
        log.warning("telegram_bot_preflight_webhook_failed", error=str(exc))


async def _acquire_bot_lock() -> None:
    """
    Acquire a Redis-based distributed lock ``nexus:bot:active_lock`` with a
    15-second TTL. If another instance currently holds the lock, wait
    ``_WAIT_S`` seconds for it to release before taking over.

    The function degrades gracefully — if Redis is unavailable the bot still
    starts (the lock is advisory, not mandatory).
    """
    _LOCK_KEY = "nexus:bot:active_lock"
    _LOCK_TTL = 15   # seconds
    _WAIT_S   = 5    # seconds to wait when a stale lock is detected
    try:
        import redis.asyncio as _aioredis  # type: ignore[import]
        parsed = urlparse(settings.redis_url)
        host = parsed.hostname or "127.0.0.1"
        if host == "localhost":
            host = "127.0.0.1"
        port = parsed.port or 6379
        db = 0
        if parsed.path and parsed.path != "/":
            try:
                db = int(parsed.path.lstrip("/"))
            except ValueError:
                db = 0
        password = unquote(parsed.password) if parsed.password else None
        username = unquote(parsed.username) if parsed.username else None
        use_ssl = parsed.scheme in {"rediss", "redis+ssl"}
        r = _aioredis.Redis(
            host=host,
            port=port,
            db=db,
            username=username,
            password=password,
            ssl=use_ssl,
            socket_connect_timeout=2,
            decode_responses=True,
        )
        existing = await r.get(_LOCK_KEY)
        if existing:
            log.warning(
                "telegram_bot_lock_held_waiting",
                held_by_pid=existing,
                wait_s=_WAIT_S,
                hint="Old instance detected — waiting for it to exit.",
            )
            await asyncio.sleep(_WAIT_S)
        await r.set(_LOCK_KEY, os.getpid(), ex=_LOCK_TTL)
        await r.aclose()
        log.info("telegram_bot_lock_acquired", pid=os.getpid(), ttl_s=_LOCK_TTL)
    except Exception as exc:
        log.warning(
            "telegram_bot_lock_redis_unavailable",
            error=str(exc),
            hint="Proceeding without distributed lock.",
        )


# ── Command / button handlers ─────────────────────────────────────────────────

async def cmd_start(message: Message) -> None:
    """Show the primary control-panel inline menu and welcome message (admin only)."""
    admin_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "")
    if admin_id and str(message.chat.id) != str(admin_id):
        await message.answer("⛔ גישה נדחתה — ממשק זה מוגבל למנהל המערכת בלבד\\.")
        return

    name = message.from_user.first_name if message.from_user else "מפעיל"
    welcome = (
        "🎯 *Nexus Orchestrator — מרכז פיקוד*\n\n"
        f"ברוך הבא, {_esc(name)}\\!\n\n"
        "המערכת הופעלה בהצלחה — כל המעבדים מחוברים ופעילים\\.\n"
        "בחר פעולה מממשק הפקודות \\(ChatOps\\):"
    )
    await message.answer(
        welcome,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_start_menu(),
    )

# ── /start Control-Panel Callback Handlers ────────────────────────────────────

async def handle_status(callback: CallbackQuery) -> None:
    """📊 סטטוס מערכת — aggregate live status from business + cluster APIs."""
    await callback.answer("טוען סטטוס מערכת...")

    biz_data     = await _api_get("/api/business/stats")
    cluster_data = await _api_get("/api/cluster/status")
    panic_data   = await _api_get("/api/system/panic/state")

    lines = ["📊 *סטטוס מערכת — Nexus Orchestrator*", ""]

    # ── Panic state ───────────────────────────────────────────────────────────
    if panic_data and panic_data.get("panic"):
        activated = _esc(panic_data.get("activated_at", "לא ידוע")[:19].replace("T", " "))
        lines += [
            "🚨 *מצב חירום פעיל*",
            f"  ⏱ הופעל: `{activated} UTC`",
            f"  ℹ️ סיבה: `{_esc(panic_data.get('reason', 'ידני'))}`",
            "",
        ]
    else:
        lines += ["✅ *מצב מערכת: תקין*", ""]

    # ── Business stats ────────────────────────────────────────────────────────
    if biz_data:
        active  = biz_data.get("active_sessions", 0)
        frozen  = biz_data.get("frozen_sessions", 0)
        health_icon = "🟢" if active > 0 else "🔴"
        lines += [
            "📡 *תפעול*",
            f"  {health_icon} סשנים פעילים: `{active}` \\| קפואים: `{frozen}`",
            f"  👥 קבוצות מנוהלות: `{biz_data.get('total_managed_groups', 0)}`",
            f"  👤 משתמשים בצינור: `{biz_data.get('total_users_pipeline', 0)}`",
            "",
        ]
    else:
        lines += ["⚠️ _נתוני תפעול לא זמינים_", ""]

    # ── Cluster ───────────────────────────────────────────────────────────────
    if cluster_data:
        nodes   = cluster_data.get("nodes", [])
        online  = sum(1 for n in nodes if n.get("online"))
        total_n = len(nodes)
        worker_nodes = [n for n in nodes if str(n.get("role", "")).lower() == "worker"]
        online_workers = [n for n in worker_nodes if n.get("online")]
        worker_list = ", ".join(_esc(str(n.get("node_id", "?"))) for n in online_workers[:6]) or "none"
        cluster_icon = "🟢" if online == total_n and total_n > 0 else ("🟡" if online > 0 else "🔴")
        lines += [
            "🖥️ *קלאסטר*",
            f"  {cluster_icon} צמתים מחוברים: `{online}/{total_n}`",
            f"  💓 Node Heartbeat: `{len(online_workers)}/{len(worker_nodes)}` workers",
            f"  🧩 Workers: `{worker_list}`",
            "",
        ]
    else:
        lines += ["⚠️ _נתוני קלאסטר לא זמינים_", ""]

    lines.append(f"🕐 _עודכן: {_esc(_now_utc())}_")

    await callback.message.edit_text(
        "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_start_menu(),
    )


async def handle_live_ops_status(callback: CallbackQuery) -> None:
    """⚡ LIVE OPS panel with heartbeat visibility."""
    await callback.answer("בודק LIVE OPS...")

    mode_data = await _api_get("/api/prediction/trading-mode")
    cluster_data = await _api_get("/api/cluster/status")
    panic_data = await _api_get("/api/system/panic/state")

    if mode_data is None or cluster_data is None:
        await callback.message.edit_text(
            "⚠️ *שגיאת חיבור*\n\nלא ניתן לקרוא את מצב Live Ops מהשרת\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=get_start_menu(),
        )
        return

    paper_mode = bool(mode_data.get("paper_trading", True))
    live_mode = not paper_mode
    mode_icon = "🟢" if live_mode else "🔴"
    mode_label = "LIVE EXECUTION" if live_mode else "PAPER MODE \\(BLOCKED\\)"

    nodes = cluster_data.get("nodes", [])
    worker_nodes = [n for n in nodes if str(n.get("role", "")).lower() == "worker"]
    online_workers = [n for n in worker_nodes if n.get("online")]
    worker_names = ", ".join(_esc(str(n.get("node_id", "?"))) for n in online_workers[:8]) or "none"
    heartbeat_icon = "🟢" if online_workers else "🔴"

    panic = bool(panic_data and panic_data.get("panic"))
    panic_label = "PANIC ACTIVE" if panic else "NORMAL"
    panic_icon = "🚨" if panic else "✅"

    lines = [
        "⚡ *LIVE OPS \\- REAL\\-TIME EXECUTION*",
        "",
        f"  {mode_icon} מצב ביצוע: *{mode_label}*",
        f"  {heartbeat_icon} Node Heartbeat: `{len(online_workers)}/{len(worker_nodes)}` workers online",
        f"  🧩 Workers: `{worker_names}`",
        f"  {panic_icon} System State: `{panic_label}`",
        "",
        f"🕐 _עודכן: {_esc(_now_utc())}_",
    ]

    await callback.message.edit_text(
        "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_start_menu(),
    )


async def handle_check_sentinel(callback: CallbackQuery) -> None:
    """🛡️ בדיקת חוסן — fetch Sentinel AI engine status."""
    await callback.answer("מריץ בדיקת חוסן...")

    data = await _api_get("/api/sentinel/status")
    if data is None:
        await callback.message.edit_text(
            "⚠️ *שגיאת חיבור*\n\nמנוע Sentinel אינו מגיב\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=get_start_menu(),
        )
        return

    state    = data.get("state", "unknown")
    lat_ms   = data.get("latency_ms")
    ram_pct  = data.get("ram_pct")
    switched = data.get("rpc_switched", False)

    state_icons = {
        "healthy":   "🟢",
        "degraded":  "🟡",
        "critical":  "🔴",
        "offline":   "⚫",
        "unknown":   "❓",
    }
    state_labels = {
        "healthy":   "תקין",
        "degraded":  "בעומס",
        "critical":  "קריטי",
        "offline":   "לא מחובר",
        "unknown":   "לא ידוע",
    }
    s_icon  = state_icons.get(state, "❓")
    s_label = _esc(state_labels.get(state, state))

    lat_str  = f"`{lat_ms:.0f} ms`"  if lat_ms  is not None else "`—`"
    ram_str  = f"`{ram_pct:.1f}%`"   if ram_pct is not None else "`—`"
    rpc_str  = "🔄 _הוחלף ל\\-RPC גיבוי_" if switched else "✅ _RPC ראשי פעיל_"

    bad_lat = data.get("latency_bad_cycles", 0)
    bad_ram = data.get("ram_bad_cycles", 0)

    lines = [
        "🛡️ *בדיקת חוסן — Sentinel AI*",
        "",
        f"  {s_icon} מצב: *{s_label}*",
        f"  ⚡ השהיה: {lat_str}",
        f"  🧠 שימוש בזיכרון: {ram_str}",
        f"  🔁 מחזורי השהיה חריגה: `{bad_lat}`",
        f"  💾 מחזורי זיכרון חריג: `{bad_ram}`",
        f"  📡 {rpc_str}",
        "",
        f"🕐 _עודכן: {_esc(_now_utc())}_",
    ]

    await callback.message.edit_text(
        "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_start_menu(),
    )


async def handle_panic_stop(callback: CallbackQuery) -> None:
    """🛑 עצירת חירום — show confirmation prompt before engaging PANIC."""
    await callback.answer("נדרש אישור לפני הפעלת עצירת חירום\\!", show_alert=True)

    confirm_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🚨 אישור — הפעל עצירת חירום", callback_data="panic_confirm"),
            InlineKeyboardButton(text="↩️ ביטול",                    callback_data="panic_cancel"),
        ],
    ])

    await callback.message.edit_text(
        "🚨 *עצירת חירום \\(PANIC STOP\\)*\n\n"
        "פעולה זו תבצע את הפעולות הבאות *מיידית*:\n\n"
        "  🔴 הגדרת מצב חירום גלובלי ב\\-Redis\n"
        "  📡 שידור TERMINATE לכל צמתי העבודה\n"
        "  ⏹ עצירת כל המשימות הפעילות\n\n"
        "⚠️ *האם אתה בטוח שברצונך להמשיך?*",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=confirm_keyboard,
    )


async def handle_panic_confirm(callback: CallbackQuery) -> None:
    """Execute the PANIC after user confirms."""
    admin_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "")
    if admin_id and str(callback.from_user.id) != str(admin_id):
        await callback.answer("⛔ גישה נדחתה — פעולה זו מוגבלת למנהל המערכת בלבד\\.", show_alert=True)
        return

    await callback.answer("🚨 מפעיל עצירת חירום...")

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(f"{API_BASE}/api/system/panic")
            resp.raise_for_status()
            data = resp.json()

        workers_killed = len(data.get("workers_terminated", []))
        elapsed_ms     = data.get("elapsed_ms", 0)
        activated_at   = _esc(data.get("activated_at", "")[:19].replace("T", " "))

        lines = [
            "🚨 *עצירת חירום הופעלה*",
            "",
            f"  ⏰ זמן הפעלה: `{activated_at} UTC`",
            f"  🖥️ צמתים שהופסקו: `{workers_killed}`",
            f"  ⚡ זמן תגובה: `{elapsed_ms} ms`",
            "",
            "_כל המשימות הפעילות הופסקו\\._\n_לחזרה לפעולה רגילה — השתמש ב\\-שחזור מערכת\\._",
        ]
        await callback.message.edit_text(
            "\n".join(lines),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=get_start_menu(),
        )
        log.critical("telegram_panic_stop_activated", workers_killed=workers_killed, elapsed_ms=elapsed_ms)

    except Exception as exc:
        await callback.message.edit_text(
            f"❌ *שגיאה בהפעלת עצירת חירום*\n\n`{_esc(str(exc))}`",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=get_start_menu(),
        )
        log.error("telegram_panic_stop_error", error=str(exc))


async def handle_panic_cancel(callback: CallbackQuery) -> None:
    """Dismiss the PANIC confirmation and return to the main control panel."""
    await callback.answer("הפעולה בוטלה.")
    name = callback.from_user.first_name if callback.from_user else "מפעיל"
    await callback.message.edit_text(
        "🎯 *Nexus Orchestrator — מרכז פיקוד*\n\n"
        f"ברוך הבא, {_esc(name)}\\!\n\n"
        "המערכת מוכנה לפעולה\\.\n"
        "בחר פעולה מלוח הבקרה:",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_start_menu(),
    )


# ── V2 Menu Handlers ──────────────────────────────────────────────────────────

async def handle_menu_stats(callback: CallbackQuery) -> None:
    """Handle 📊 סטטיסטיקות menu selection."""
    await callback.answer("טוען נתונים...")
    
    data = await _api_get("/api/business/stats")
    if not data:
        await callback.message.edit_text(
            get_string("api_error", BOT_LANGUAGE),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=get_main_menu(),
        )
        return

    report = format_stats_report(data, BOT_LANGUAGE)
    await callback.message.edit_text(
        report,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_main_menu(),
    )

async def handle_menu_cluster(callback: CallbackQuery) -> None:
    """Handle 🖥️ ניהול קלאסטר menu selection.""" 
    await callback.answer("בודק מצב קלאסטר...")
    
    data = await _api_get("/api/cluster/status")
    if not data:
        await callback.message.edit_text(
            get_string("api_error", BOT_LANGUAGE),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=get_main_menu(),
        )
        return

    report = format_cluster_report(data, BOT_LANGUAGE)
    
    # Add cluster control buttons
    control_menu = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🚀 סנכרן עובדים", callback_data="cluster_sync"),
            InlineKeyboardButton(text="🔄 אתחל", callback_data="cluster_restart"),
        ],
        [
            InlineKeyboardButton(text="🏠 תפריט ראשי", callback_data="main_menu"),
        ],
    ])
    
    await callback.message.edit_text(
        report,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=control_menu,
    )

async def handle_menu_wallet(callback: CallbackQuery) -> None:
    """Handle 💰 ארנק menu selection."""
    await callback.answer("טוען נתונים פיננסיים...")
    
    data = await _api_get("/api/projects/budget/widget")
    if not data:
        await callback.message.edit_text(
            get_string("api_error", BOT_LANGUAGE),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=get_main_menu(),
        )
        return

    report = format_wallet_report(data, BOT_LANGUAGE)
    await callback.message.edit_text(
        report,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_main_menu(),
    )

async def handle_main_menu(callback: CallbackQuery) -> None:
    """Return to main menu."""
    await callback.answer()
    await callback.message.edit_text(
        get_string("welcome", BOT_LANGUAGE),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_main_menu(),
    )


async def cmd_dashboard(message: Message) -> None:
    """Send a direct link to the dashboard."""
    await message.answer(
        f"🔗 *Nexus Dashboard*\n\n"
        f"Open the Control Center:\n"
        f"{_esc(DASHBOARD_URL)}\n\n"
        f"_Tip: Use Tailscale VPN to access from anywhere\\._",
    )


async def cmd_help(message: Message) -> None:
    """List available commands."""
    await message.answer(
        "🤖 *Nexus Bot Commands*\n\n"
        "/start — Show the main menu\n"
        "/dashboard — Open the dashboard link\n"
        "/killswitch — 🚨 KILL all autonomous projects instantly\n"
        "/godmode\\_on — Enable GOD MODE \\(auto\\-deploy\\)\n"
        "/godmode\\_off — Disable GOD MODE\n"
        "/incubator — Show Evolution Engine status\n"
        "/help — Show this message\n\n"
        "*Menu Buttons:*\n"
        f"  {_esc(BTN_STATS)} — DB stats\n"
        f"  {_esc(BTN_CLUSTER)} — Worker nodes\n"
        f"  {_esc(BTN_PROFIT)} — ROI report\n"
        f"  {_esc(BTN_TASKS)} — HITL queue\n"
        f"  {_esc(BTN_INCUBATOR)} — Evolution Engine\n\n"
        "_HITL Approve/Reject buttons appear automatically when a task pauses\\._",
    )


async def cmd_killswitch(message: Message) -> None:
    """Emergency Kill Switch — stops all autonomous incubator projects instantly."""
    admin_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "")
    if admin_id and str(message.chat.id) != str(admin_id):
        await message.answer("⛔ Unauthorized\\. Kill Switch is admin\\-only\\.")
        return

    await message.answer("🚨 *KILL SWITCH ACTIVATED*\n\nStopping all autonomous projects\\.\\.\\.")

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            # Get all projects and kill each live one
            resp = await client.get(f"{API_BASE}/api/incubator/projects")
            resp.raise_for_status()
            data = resp.json()
            projects = data.get("projects", [])

            killed = 0
            for p in projects:
                if p.get("status") in ("live", "pending_review"):
                    kill_resp = await client.post(
                        f"{API_BASE}/api/incubator/kill/{p['project_id']}"
                    )
                    if kill_resp.status_code == 200:
                        killed += 1

            # Also disable GOD MODE
            await client.post(
                f"{API_BASE}/api/incubator/god-mode",
                json={"enabled": False},
            )

        await message.answer(
            f"✅ *Kill Switch Complete*\n\n"
            f"  • Projects killed: `{killed}`\n"
            f"  • GOD MODE: `DISABLED`\n\n"
            f"_All autonomous projects have been stopped\\._"
        )
        log.info("telegram_killswitch_activated", killed=killed)

    except Exception as exc:
        await message.answer(f"❌ Kill Switch error: {_esc(str(exc))}")
        log.error("telegram_killswitch_error", error=str(exc))


async def cmd_godmode_on(message: Message) -> None:
    """Enable GOD MODE via Telegram."""
    admin_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "")
    if admin_id and str(message.chat.id) != str(admin_id):
        await message.answer("⛔ Unauthorized\\.")
        return
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.post(
                f"{API_BASE}/api/incubator/god-mode",
                json={"enabled": True},
            )
            resp.raise_for_status()
        await message.answer(
            "⚡ *GOD MODE ACTIVATED*\n\n"
            "_Projects will now deploy without human approval\\._\n"
            "Use /godmode\\_off or /killswitch to regain control\\."
        )
        log.info("telegram_godmode_activated")
    except Exception as exc:
        await message.answer(f"❌ Error: {_esc(str(exc))}")


async def cmd_godmode_off(message: Message) -> None:
    """Disable GOD MODE via Telegram."""
    admin_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "")
    if admin_id and str(message.chat.id) != str(admin_id):
        await message.answer("⛔ Unauthorized\\.")
        return
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.post(
                f"{API_BASE}/api/incubator/god-mode",
                json={"enabled": False},
            )
            resp.raise_for_status()
        await message.answer(
            "✅ *GOD MODE DEACTIVATED*\n\n"
            "_All new projects now require human approval\\._"
        )
        log.info("telegram_godmode_deactivated")
    except Exception as exc:
        await message.answer(f"❌ Error: {_esc(str(exc))}")


async def cmd_incubator(message: Message) -> None:
    """Show Evolution Engine / Incubator status."""
    await message.answer("⏳ Fetching incubator status\\.\\.\\.")
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            state_resp    = await client.get(f"{API_BASE}/api/incubator/state")
            projects_resp = await client.get(f"{API_BASE}/api/incubator/projects")
            niches_resp   = await client.get(f"{API_BASE}/api/incubator/niches")

            state    = state_resp.json()    if state_resp.status_code    == 200 else {}
            projects = projects_resp.json() if projects_resp.status_code == 200 else {}
            niches   = niches_resp.json()   if niches_resp.status_code   == 200 else {}

        god_mode_icon = "⚡ ON" if state.get("god_mode") else "✅ OFF"
        lines = [
            "🧬 *EVOLUTION INCUBATOR*",
            "",
            f"⚡ GOD MODE: `{god_mode_icon}`",
            f"🏗️ Architect: `{_esc(state.get('architect_state', 'idle'))}`",
            f"🔍 Scout: `{_esc(state.get('scout_state', 'idle'))}`",
            "",
            "📊 *Projects*",
            f"  Total: `{state.get('total_projects', 0)}`",
            f"  Live:  `{state.get('live_projects', 0)}`",
            "",
        ]

        # Top niches
        niche_list = niches.get("niches", [])
        if niche_list:
            lines.append("🎯 *Top Niches*")
            for i, n in enumerate(niche_list[:3], 1):
                lines.append(
                    f"  {i}\\. {_esc(n.get('name', '?'))} "
                    f"\\(conf: `{n.get('confidence', 0)}%`\\)"
                )
            lines.append("")

        # Recent projects
        proj_list = projects.get("projects", [])
        if proj_list:
            lines.append("🚀 *Recent Projects*")
            for p in proj_list[:5]:
                status_icon = {"live": "🟢", "pending_review": "🟡", "killed": "🔴"}.get(
                    p.get("status", ""), "⚪"
                )
                lines.append(
                    f"  {status_icon} {_esc(p.get('name', '?')[:40])}"
                )

        lines += ["", f"🔄 _Updated: {_esc(_now_utc())}_"]
        await message.answer("\n".join(lines))

    except Exception as exc:
        await message.answer(f"❌ Could not reach the API: {_esc(str(exc))}")


async def handle_menu_button(message: Message) -> None:
    """Dispatch menu button presses to the correct data fetch + format."""
    text = (message.text or "").strip()

    await message.answer("⏳ Fetching data\\.\\.\\.")

    if text == BTN_STATS:
        data = await _api_get("/api/business/stats")
        if data is None:
            await message.answer("❌ Could not reach the API\\. Is `start_api\\.py` running?")
            return
        await message.answer(_fmt_current_stats(data))

    elif text == BTN_CLUSTER:
        data = await _api_get("/api/cluster/status")
        if data is None:
            await message.answer("❌ Could not reach the API\\.")
            return
        await message.answer(_fmt_cluster_health(data))

    elif text == BTN_PROFIT:
        data = await _api_get("/api/business/report")
        if data is None:
            await message.answer("❌ Could not reach the API\\.")
            return
        await message.answer(_fmt_profit_report(data))

    elif text == BTN_TASKS:
        hitl_data  = await _api_get("/api/hitl/pending") or {"items": [], "total": 0}
        scrape_data = await _api_get("/api/business/scrape-status")
        await message.answer(_fmt_active_tasks(hitl_data, scrape_data))

    elif text == BTN_INCUBATOR:
        await cmd_incubator(message)


# ── Birth-approval callback handler ──────────────────────────────────────────

async def handle_birth_callback(callback: CallbackQuery) -> None:
    """Handle APPROVE / REJECT button presses from Project Birth Proposal messages."""
    if callback.data is None or callback.message is None:
        await callback.answer("Invalid callback.")
        return

    parts = callback.data.split(":", 1)
    if len(parts) != 2 or parts[0] not in ("birth_approve", "birth_reject"):
        await callback.answer("Unknown action.")
        return

    action, request_id = parts
    approved    = action == "birth_approve"
    reviewer_id = f"telegram:{callback.from_user.id}" if callback.from_user else "telegram"

    log.info("telegram_birth_callback", action=action, request_id=request_id, reviewer=reviewer_id)

    result_text: str
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{API_BASE}/api/evolution/birth-resolve",
                json={
                    "request_id": request_id,
                    "approved":   approved,
                    "reviewer_id": reviewer_id,
                    "reason": (
                        "GOD MODE ENABLED"
                        if approved
                        else f"Rejected via Telegram by {reviewer_id}"
                    ),
                },
            )
            resp.raise_for_status()
            data = resp.json()
            result_text = data.get("message", "Decision recorded.")
            log.info("telegram_birth_resolved", request_id=request_id, approved=approved)

    except httpx.HTTPStatusError as exc:
        result_text = (
            "⚠️ Request not found — already resolved or expired."
            if exc.response.status_code == 404
            else f"❌ API error {exc.response.status_code}: {exc.response.text}"
        )
        log.error("telegram_birth_api_error", error=str(exc))
    except Exception as exc:
        result_text = f"❌ Could not reach the API: {exc}"
        log.error("telegram_birth_network_error", error=str(exc))

    decision_icon = "🚀" if approved else "❌"
    decision_word = (
        "GOD MODE ENABLED — Project Deploying"
        if approved
        else "Rejected — Regenerating"
    )

    try:
        original_text = callback.message.text or callback.message.caption or ""
        await callback.message.edit_text(
            f"{original_text}\n\n"
            f"{decision_icon} *{_esc(decision_word)}*\n"
            f"_{_esc(result_text)}_",
            reply_markup=None,
        )
    except Exception as exc:
        log.warning("telegram_edit_birth_message_failed", error=str(exc))

    await callback.answer(f"{decision_icon} {decision_word}!", show_alert=True)


# ── HITL callback handler (existing) ─────────────────────────────────────────

async def handle_hitl_callback(callback: CallbackQuery) -> None:
    """Handle Approve / Reject button presses from HITL notification messages."""
    if callback.data is None or callback.message is None:
        await callback.answer("Invalid callback.")
        return

    parts = callback.data.split(":", 1)
    if len(parts) != 2 or parts[0] not in ("hitl_approve", "hitl_reject"):
        await callback.answer("Unknown action.")
        return

    action, request_id = parts
    approved    = action == "hitl_approve"
    reviewer_id = f"telegram:{callback.from_user.id}" if callback.from_user else "telegram"

    log.info("telegram_hitl_callback", action=action, request_id=request_id, reviewer=reviewer_id)

    result_text: str
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{API_BASE}/api/hitl/resolve",
                json={
                    "request_id": request_id,
                    "approved":   approved,
                    "reviewer_id": reviewer_id,
                    "reason": f"Resolved via Telegram by {reviewer_id}",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            result_text = data.get("message", "Decision recorded.")
            log.info("telegram_hitl_resolved", request_id=request_id, approved=approved)

    except httpx.HTTPStatusError as exc:
        result_text = (
            "⚠️ Request not found — already resolved or expired."
            if exc.response.status_code == 404
            else f"❌ API error {exc.response.status_code}: {exc.response.text}"
        )
        log.error("telegram_hitl_api_error", error=str(exc))
    except Exception as exc:
        result_text = f"❌ Could not reach the API: {exc}"
        log.error("telegram_hitl_network_error", error=str(exc))

    # V2: Hebrew confirmation format
    decision_state = "approved" if approved else "rejected"
    
    try:
        # Update the existing message with Hebrew confirmation
        original_text = callback.message.text or callback.message.caption or ""
        updated_text = format_confirmation_update(
            original_text,
            decision_state,
            reviewer_id,
            BOT_LANGUAGE
        )
        
        await callback.message.edit_text(
            updated_text,
            reply_markup=None,
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    except Exception as exc:
        log.warning("telegram_edit_message_failed", error=str(exc))

    # Hebrew notification
    notification = get_string("approved" if approved else "rejected", BOT_LANGUAGE)
    await callback.answer(notification, show_alert=False)


# ── Force-run callback (from STUCK alert) ────────────────────────────────────

async def handle_force_run_callback(callback: CallbackQuery) -> None:
    """
    Handle Force Run / Dismiss button presses from STUCK loop alert messages.

    force_run:<task_type>  — bypass confidence check and enqueue immediately
    stuck_dismiss          — just dismiss the alert
    """
    if callback.data is None or callback.message is None:
        await callback.answer("Invalid callback.")
        return

    reviewer_id = f"telegram:{callback.from_user.id}" if callback.from_user else "telegram"

    if callback.data == "stuck_dismiss":
        try:
            await callback.message.edit_text(
                (callback.message.text or "") + "\n\n🚫 _Dismissed by operator_",
                reply_markup=None,
            )
        except Exception:
            pass
        await callback.answer("Alert dismissed.", show_alert=False)
        return

    # force_run:<task_type>
    task_type = callback.data.split(":", 1)[1] if ":" in callback.data else ""
    if not task_type:
        await callback.answer("Invalid task type.", show_alert=True)
        return

    log.info("telegram_force_run_callback", task_type=task_type, reviewer=reviewer_id)

    result_text: str
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{API_BASE}/api/business/force-run",
                json={
                    "task_type": task_type,
                    "task_params": {"force": True},
                    "reviewer_id": reviewer_id,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            result_text = data.get("message", "Force-run enqueued.")
            log.info(
                "telegram_force_run_dispatched",
                task_type=task_type,
                task_id=data.get("task_id"),
            )
    except httpx.HTTPStatusError as exc:
        result_text = f"❌ API error {exc.response.status_code}: {exc.response.text}"
        log.error("telegram_force_run_api_error", error=str(exc))
    except Exception as exc:
        result_text = f"❌ Could not reach the API: {exc}"
        log.error("telegram_force_run_network_error", error=str(exc))

    try:
        await callback.message.edit_text(
            (callback.message.text or "") + f"\n\n⚡ *Force Run dispatched*\n_{_esc(result_text)}_",
            reply_markup=None,
        )
    except Exception as exc:
        log.warning("telegram_edit_force_run_failed", error=str(exc))

    await callback.answer("⚡ Force-run dispatched!", show_alert=True)


# ── System Recovery callback (from Autonomous Flight Mode alert) ──────────────

async def handle_system_recovery_callback(callback: CallbackQuery) -> None:
    """
    Handle the "System Recovery / שחזור מערכת" button sent by the Sentinel
    when the system enters Autonomous Flight Mode.

    Calls POST /api/flight-mode/recover with operator=telegram:<user_id>,
    then confirms the action in the original message.
    """
    if callback.data != "system_recovery" or callback.message is None:
        await callback.answer("Invalid callback.")
        return

    reviewer_id = f"telegram:{callback.from_user.id}" if callback.from_user else "telegram"
    log.info("telegram_system_recovery_callback", reviewer=reviewer_id)

    result_text: str
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{API_BASE}/api/flight-mode/recover",
                json={"operator": reviewer_id},
            )
            resp.raise_for_status()
            data       = resp.json()
            status_val = data.get("status", "unknown")
            result_text = data.get(
                "message",
                "המערכת הופעלה בהצלחה — חזרה לפעולה רגילה." if status_val == "recovered" else status_val,
            )
            log.info("telegram_system_recovery_success", reviewer=reviewer_id, status=status_val)

    except httpx.HTTPStatusError as exc:
        result_text = f"❌ API error {exc.response.status_code}: {exc.response.text}"
        log.error("telegram_system_recovery_api_error", error=str(exc))
    except Exception as exc:
        result_text = f"❌ Could not reach the API: {exc}"
        log.error("telegram_system_recovery_network_error", error=str(exc))

    try:
        await callback.message.edit_text(
            (callback.message.text or "")
            + f"\n\n✅ *שחזור מערכת הופעל בהצלחה*\n_{_esc(result_text)}_\n"
            f"_מבצע: {_esc(reviewer_id)}_",
            reply_markup=None,
        )
    except Exception as exc:
        log.warning("telegram_edit_recovery_message_failed", error=str(exc))

    await callback.answer("✅ System Recovery activated!", show_alert=True)


# ── Bot setup ─────────────────────────────────────────────────────────────────

def build_bot_dispatcher(token: str) -> tuple["Bot", "TgDispatcher"]:
    """
    Construct and wire up the aiogram Bot + Dispatcher.

    Returns (bot, dp) ready for start_polling().
    Extracted so start_master.py can embed the bot in its own event loop
    without spawning a separate process.
    """
    bot = Bot(
        token=token,
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN_V2),
    )
    dp = TgDispatcher()

    dp.message.register(cmd_start, CommandStart())
    dp.message.register(cmd_dashboard, Command("dashboard"))
    dp.message.register(cmd_help, Command("help"))
    dp.message.register(cmd_killswitch, Command("killswitch"))
    dp.message.register(cmd_godmode_on, Command("godmode_on"))
    dp.message.register(cmd_godmode_off, Command("godmode_off"))
    dp.message.register(cmd_incubator, Command("incubator"))

    # /start control-panel callbacks (4-button 2×2 grid)
    dp.callback_query.register(handle_status,             F.data == "status")
    dp.callback_query.register(handle_live_ops_status,     F.data == "live_ops")
    dp.callback_query.register(handle_check_sentinel,     F.data == "check_sentinel")
    dp.callback_query.register(handle_panic_stop,         F.data == "panic_stop")
    dp.callback_query.register(handle_panic_confirm,      F.data == "panic_confirm")
    dp.callback_query.register(handle_panic_cancel,       F.data == "panic_cancel")

    # V2 Hebrew menu handlers
    dp.callback_query.register(handle_menu_stats,  F.data == "menu_stats")
    dp.callback_query.register(handle_menu_cluster, F.data == "menu_cluster")
    dp.callback_query.register(handle_menu_wallet, F.data == "menu_wallet")
    dp.callback_query.register(handle_main_menu,   F.data == "main_menu")

    # HITL and control handlers
    dp.callback_query.register(
        handle_hitl_callback,
        F.data.startswith("hitl_approve:") | F.data.startswith("hitl_reject:"),
    )
    dp.callback_query.register(
        handle_birth_callback,
        F.data.startswith("birth_approve:") | F.data.startswith("birth_reject:"),
    )
    dp.callback_query.register(
        handle_force_run_callback,
        F.data.startswith("force_run:") | F.data.in_({"stuck_dismiss"}),
    )
    dp.callback_query.register(
        handle_system_recovery_callback,
        F.data == "system_recovery",
    )

    return bot, dp


async def start_bot_polling(token: str) -> None:
    """
    Long-running coroutine — poll Telegram for updates until cancelled.

    Designed to be launched with asyncio.create_task() from start_master.py
    so the Command Center runs in the same event loop as the Orchestrator.
    Signal handling is the Master's responsibility when running in this mode.
    """
    print("[START] המערכת מוכנה לפעולה — מאתחל את בוט הפיקוד של Nexus...")
    log.info("telegram_bot_embedded_starting")

    # Pre-flight: clear any lingering webhook / sessions before polling starts.
    await _preflight_cleanup(token)

    bot, dp = build_bot_dispatcher(token)
    try:
        await dp.start_polling(bot)
    except asyncio.CancelledError:
        pass
    finally:
        print("[SHUTDOWN] מתנתק משרתי טלגרם ומסיים את הפעילות...")
        await bot.session.close()
        log.info("telegram_bot_stopped")


async def run() -> None:
    # Production bot logs are intentionally concise.
    configure_logging(level="ERROR", node_id=f"{settings.node_id}-telegram-bot")

    print("[START] המערכת מוכנה לפעולה — מריץ בדיקות טרום-הפעלה...")
    log.info("telegram_bot_instance_check")

    token = os.environ.get("TELEGRAM_BOT_TOKEN", "") or settings.telegram_bot_token
    if not token:
        log.error("telegram_bot_no_token", hint="Set TELEGRAM_BOT_TOKEN in .env and restart.")
        return

    # ── Pre-flight: clear lingering sessions, acquire instance lock ────────────
    await _preflight_cleanup(token)
    await _acquire_bot_lock()

    bot = Bot(
        token=token,
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN_V2),
    )
    dp = TgDispatcher()

    # ── Register handlers ──────────────────────────────────────────────────────
    dp.message.register(cmd_start, CommandStart())
    dp.message.register(cmd_dashboard, Command("dashboard"))
    dp.message.register(cmd_help, Command("help"))
    dp.message.register(cmd_killswitch, Command("killswitch"))
    dp.message.register(cmd_godmode_on, Command("godmode_on"))
    dp.message.register(cmd_godmode_off, Command("godmode_off"))
    dp.message.register(cmd_incubator, Command("incubator"))

    # Legacy text menu support (kept for backwards compatibility)
    dp.message.register(
        handle_menu_button,
        F.text.in_({BTN_STATS, BTN_CLUSTER, BTN_PROFIT, BTN_TASKS, BTN_INCUBATOR}),
    )

    # /start control-panel callbacks (4-button 2×2 grid)
    dp.callback_query.register(handle_status,            F.data == "status")
    dp.callback_query.register(handle_live_ops_status, F.data == "live_ops")
    dp.callback_query.register(handle_check_sentinel,    F.data == "check_sentinel")
    dp.callback_query.register(handle_panic_stop,        F.data == "panic_stop")
    dp.callback_query.register(handle_panic_confirm,     F.data == "panic_confirm")
    dp.callback_query.register(handle_panic_cancel,      F.data == "panic_cancel")

    # V2 Hebrew menu callbacks
    dp.callback_query.register(handle_menu_stats,  F.data == "menu_stats")
    dp.callback_query.register(handle_menu_cluster, F.data == "menu_cluster")
    dp.callback_query.register(handle_menu_wallet, F.data == "menu_wallet")
    dp.callback_query.register(handle_main_menu,   F.data == "main_menu")

    # HITL inline keyboard callbacks
    dp.callback_query.register(
        handle_hitl_callback,
        F.data.startswith("hitl_approve:") | F.data.startswith("hitl_reject:"),
    )

    # Birth-approval inline keyboard callbacks
    dp.callback_query.register(
        handle_birth_callback,
        F.data.startswith("birth_approve:") | F.data.startswith("birth_reject:"),
    )

    # Force-run + stuck-dismiss callbacks
    dp.callback_query.register(
        handle_force_run_callback,
        F.data.startswith("force_run:") | F.data.in_({"stuck_dismiss"}),
    )

    # Autonomous Flight Mode — System Recovery callback
    dp.callback_query.register(
        handle_system_recovery_callback,
        F.data == "system_recovery",
    )

    log.info(
        "telegram_bot_starting",
        api_base=API_BASE,
        dashboard=DASHBOARD_URL,
        commands=["/start", "/dashboard", "/help"],
        menu_buttons=[BTN_STATS, BTN_CLUSTER, BTN_PROFIT, BTN_TASKS],
    )

    # ── Graceful shutdown via SIGINT / SIGTERM ─────────────────────────────────
    # On Unix both signals are supported; on Windows only SIGINT (Ctrl-C)
    # is reliably delivered — SIGTERM registration is attempted but may fail.
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _on_signal(signum: int, frame: object) -> None:  # noqa: ARG001
        print("[SHUTDOWN] קיבל אות עצירה — מבצע כיבוי מסודר...")
        log.warning("telegram_bot_shutdown_signal", signum=signum)
        loop.call_soon_threadsafe(stop_event.set)

    _signal.signal(_signal.SIGINT, _on_signal)
    try:
        _signal.signal(_signal.SIGTERM, _on_signal)
    except (OSError, AttributeError, ValueError):
        pass  # SIGTERM not available on Windows

    # ── Start polling as a task so we can cancel it on signal ─────────────────
    polling_task = asyncio.create_task(dp.start_polling(bot), name="bot-polling")
    stop_watcher = asyncio.create_task(stop_event.wait(),    name="bot-stop-watcher")
    try:
        await asyncio.wait(
            [polling_task, stop_watcher],
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        stop_watcher.cancel()
        if not polling_task.done():
            polling_task.cancel()
            try:
                await polling_task
            except asyncio.CancelledError:
                pass
        print("[SHUTDOWN] מתנתק משרתי טלגרם ומסיים את הפעילות...")
        await bot.session.close()
        log.info("telegram_bot_stopped")


def main() -> None:
    logging.getLogger("aiogram").setLevel(logging.WARNING)
    asyncio.run(run())


if __name__ == "__main__":
    main()
