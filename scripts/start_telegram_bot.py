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
    # Fix Unicode output on Windows terminals (Hebrew/emoji support)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
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

# ── Remote Prompt Bridge ──────────────────────────────────────────────────────
_PROMPTS_FILE = Path(__file__).resolve().parent.parent / "INCOMING_PROMPTS.md"
_JACOB_USER_ID = int(os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "7849455058"))


async def handle_remote_prompt(message: Message) -> None:
    """Intercept plain-text messages from Jacob and append them to INCOMING_PROMPTS.md."""
    if not message.text or not message.from_user:
        return
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    entry = (
        f"\n### [{ts}] - FROM IPHONE\n"
        f"**PROMPT**: {message.text}\n"
        f"**STATUS**: PENDING\n"
        f"---\n"
    )
    with _PROMPTS_FILE.open("a", encoding="utf-8") as f:
        f.write(entry)
    await message.answer("✅ Prompt recorded in Master\\. Cursor is now analyzing\\.")


# ── Inline Keyboard Menu (replaces old Reply Keyboard) ────────────────────────
def get_main_menu():
    """Get the main inline keyboard menu in the current language."""
    return create_main_menu(BOT_LANGUAGE)


def get_start_menu() -> InlineKeyboardMarkup:
    """
    Full command-center inline keyboard — all features accessible from /start.

    Layout (8 rows):
      Row 1 — Monitoring: System Status | Live Ops
      Row 2 — Monitoring: Cluster Health | Sentinel Check
      Row 3 — Trading:    Polymarket Panel | Moltbot Scrape
      Row 4 — Finance:    Stats & DB | Wallet / Profit
      Row 5 — Automation: Incubator Engine | God Mode
      Row 6 — Emergency:  PANIC Stop | Kill Switch
      Row 7 — System:     System Recovery | Terminate Nexus
      Row 8 — Info:       Dashboard Link | Help & Commands
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 סטטוס מערכת",           callback_data="status"),
            InlineKeyboardButton(text="⚡ Live Ops",               callback_data="live_ops"),
        ],
        [
            InlineKeyboardButton(text="🖥️ בריאות קלאסטר",         callback_data="menu_cluster"),
            InlineKeyboardButton(text="🛡️ Sentinel",              callback_data="check_sentinel"),
        ],
        [
            InlineKeyboardButton(text="🎯 Polymarket",             callback_data="poly_menu"),
            InlineKeyboardButton(text="🚀 Moltbot Scrape",         callback_data="launch_moltbot"),
        ],
        [
            InlineKeyboardButton(text="📈 סטטיסטיקות DB",          callback_data="menu_stats"),
            InlineKeyboardButton(text="💰 ארנק / רווח",            callback_data="menu_wallet"),
        ],
        [
            InlineKeyboardButton(text="🧬 Incubator Engine",       callback_data="incubator_menu"),
            InlineKeyboardButton(text="⚙️ God Mode",               callback_data="godmode_menu"),
        ],
        [
            InlineKeyboardButton(text="🛑 PANIC — עצירת חירום",    callback_data="panic_stop"),
            InlineKeyboardButton(text="🚨 Kill Switch",            callback_data="killswitch_btn"),
        ],
        [
            InlineKeyboardButton(text="🔄 System Recovery",        callback_data="system_recovery"),
            InlineKeyboardButton(text="🔒 Terminate Nexus",        callback_data="terminate_btn"),
        ],
        [
            InlineKeyboardButton(text="🔗 Dashboard",              callback_data="dashboard_btn"),
            InlineKeyboardButton(text="❓ Help & Commands",         callback_data="help_btn"),
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


async def _api_post(
    path: str,
    payload: dict | None = None,
    *,
    extra_headers: dict[str, str] | None = None,
) -> dict | None:
    """POST JSON to the FastAPI server. Returns None on error."""
    try:
        hdrs = {"Content-Type": "application/json", **(extra_headers or {})}
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(f"{API_BASE}{path}", json=payload or {}, headers=hdrs)
            resp.raise_for_status()
            return resp.json()
    except Exception as exc:
        log.warning("telegram_bot_api_post_error", path=path, error=str(exc))
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


async def handle_launch_moltbot(callback: CallbackQuery) -> None:
    """
    Dispatch a bot.moltbot ARQ job directly from Telegram control panel.
    """
    await callback.answer("Dispatching Moltbot scrape...")
    payload = {
        "action": "launch_scrape",
        "query": "telegram scrape via command center",
        "max_items": 120,
    }
    data = await _api_post("/api/modules/moltbot/launch", payload)
    if not data:
        await callback.message.edit_text(
            "❌ *Moltbot dispatch failed*\n\nCould not reach the API or enqueue the task\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=get_start_menu(),
        )
        return

    task_id = _esc(str(data.get("task_id", "")))
    msg = _esc(str(data.get("message", "Moltbot dispatch accepted")))
    await callback.message.edit_text(
        "🚀 *Moltbot Scrape Launched*\n\n"
        f"Task ID: `{task_id}`\n"
        f"Queue: `nexus:tasks`\n"
        f"{msg}",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_start_menu(),
    )


# ── Polymarket Control Panel ──────────────────────────────────────────────────

def get_polymarket_menu() -> InlineKeyboardMarkup:
    """Polymarket full control inline keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 תיק השקעות / Portfolio", callback_data="poly_portfolio"),
            InlineKeyboardButton(text="📈 פוזיציות / Positions",   callback_data="poly_positions"),
        ],
        [
            InlineKeyboardButton(text="📉 ספר פקודות / Orderbook", callback_data="poly_orderbook"),
            InlineKeyboardButton(text="🤖 המלצות AI / AI Recs",    callback_data="poly_ai_recs"),
        ],
        [
            InlineKeyboardButton(text="🟢 קנה YES / BUY YES",      callback_data="poly_buy_prompt"),
            InlineKeyboardButton(text="🔴 מכור / SELL",             callback_data="poly_sell_prompt"),
        ],
        [
            InlineKeyboardButton(text="📋 יומן עסקאות / Trade Log", callback_data="poly_trade_log"),
            InlineKeyboardButton(text="⚡ סקאלפר 5m / Scalper",    callback_data="poly_scalper"),
        ],
        [
            InlineKeyboardButton(text="🏠 תפריט ראשי / Main Menu", callback_data="main_menu"),
        ],
    ])


def _fmt_poly_portfolio(dash: dict) -> str:
    """Format Polymarket dashboard data into a Hebrew+English Telegram message."""
    portfolio_val    = float(dash.get("portfolio_value") or 0)
    portfolio_cash   = float(dash.get("portfolio_cash") or 0)
    portfolio_pos    = float(dash.get("portfolio_positions") or 0)
    clob_bal         = float(dash.get("clob_balance") or 0)
    signer           = str(dash.get("signer_address") or dash.get("portfolio_address") or "—")
    short_addr       = f"{signer[:6]}…{signer[-4:]}" if len(signer) > 10 else signer
    realized_pnl     = float(dash.get("realized_pnl") or 0)
    total_deposited  = float(dash.get("total_deposited") or 0)
    total_withdrawn  = float(dash.get("total_withdrawn") or 0)
    break_even_delta = float(dash.get("break_even_delta") or 0)

    pnl_icon = "📈" if realized_pnl >= 0 else "📉"
    pnl_sign = "+" if realized_pnl >= 0 else ""
    be_icon  = "🟢" if break_even_delta >= 0 else "🔴"
    be_sign  = "+" if break_even_delta >= 0 else ""

    lines = [
        "📊 *Polymarket Portfolio / תיק פולימרקט*",
        "",
        f"  👛 ארנק / Wallet: `{_esc(short_addr)}`",
        "",
        "💼 *שווי נוכחי / Current Value*",
        f"  📦 סה\"כ תיק / Total: `${portfolio_val:,.2f}`",
        f"  💵 מזומן / Cash: `${portfolio_cash:,.2f}`",
        f"  📈 פוזיציות / Positions: `${portfolio_pos:,.2f}`",
        f"  🏦 CLOB Balance: `${clob_bal:,.2f}`",
        "",
        "💰 *הפקדות ומשיכות / Deposits & Withdrawals*",
        f"  ⬇️ סה\"כ הפקדות / Total Deposited: `${total_deposited:,.2f}`",
        f"  ⬆️ סה\"כ משיכות / Total Withdrawn: `${total_withdrawn:,.2f}`",
        "",
        "📊 *ביצועים / Performance*",
        f"  {pnl_icon} רווח ממומש / Realized PnL: `{pnl_sign}${realized_pnl:,.2f}`",
        f"  {be_icon} Break\\-Even: `{be_sign}${break_even_delta:,.2f}`",
    ]

    if total_deposited > 0:
        roi = (break_even_delta / total_deposited) * 100
        roi_icon = "🟢" if roi >= 0 else "🔴"
        roi_sign = "+" if roi >= 0 else ""
        lines.append(f"  {roi_icon} ROI: `{roi_sign}{roi:.1f}%`")

    lines += [
        "",
        "_להגדיר סכום הפקדה: /set\\_deposit <amount>_",
        f"🔄 _עודכן / Updated: {_esc(_now_utc())}_",
    ]
    return "\n".join(lines)


def _fmt_poly_positions(dash: dict) -> str:
    """Format Polymarket positions list into a Telegram message."""
    positions = dash.get("portfolio_positions_list") or []
    lines = ["📈 *פוזיציות פתוחות / Open Positions*", ""]

    if not positions:
        lines.append("_אין פוזיציות פתוחות / No open positions_")
    else:
        for i, p in enumerate(positions[:6], 1):
            title    = _esc(str(p.get("title") or "?")[:38])
            outcome  = str(p.get("outcome") or "YES")
            size     = float(p.get("size") or 0)
            avg_p    = float(p.get("avg_price") or 0)
            cur_p    = float(p.get("cur_price") or 0)
            cur_val  = float(p.get("current_value") or 0)
            cash_pnl = float(p.get("cash_pnl") or 0)
            pct_pnl  = float(p.get("percent_pnl") or 0)
            end_date = str(p.get("end_date") or "")[:10]

            pnl_icon     = "🟢" if cash_pnl >= 0 else "🔴"
            pnl_sign     = "+" if cash_pnl >= 0 else ""
            outcome_icon = "✅" if outcome == "YES" else "❌"

            lines += [
                f"*{i}\\. {title}*",
                f"  {outcome_icon} `{_esc(outcome)}` · `{size:.1f}` shares",
                f"  💰 Avg→Now: `{avg_p*100:.1f}c -> {cur_p*100:.1f}c`",
                f"  💼 Value: `${cur_val:.2f}`",
                f"  {pnl_icon} PnL: `{pnl_sign}${cash_pnl:.2f}` \\(`{pnl_sign}{pct_pnl:.1f}%`\\)",
            ]
            if end_date:
                lines.append(f"  📅 Exp: `{_esc(end_date)}`")
            lines.append("")

    lines.append(f"🔄 _Updated: {_esc(_now_utc())}_")
    return "\n".join(lines)


def _fmt_poly_orderbook(ob: dict) -> str:
    """Format CLOB orderbook data into a Telegram message."""
    if ob.get("no_position"):
        return (
            "📉 *ספר פקודות / Orderbook*\n\n"
            "⚪ _אין פוזיציה פעילה — הבוט ממתין לאות_\n"
            "_No active position — bot is idle_"
        )
    if ob.get("expired"):
        mq = _esc(str(ob.get("market_question") or "?")[:60])
        return (
            f"📉 *ספר פקודות / Orderbook*\n\n"
            f"🟡 _שוק פג תוקף / Market Expired_\n`{mq}`"
        )

    best_bid = ob.get("best_bid")
    best_ask = ob.get("best_ask")
    mid      = ob.get("mid_price")
    spread   = ob.get("spread")
    bids     = ob.get("bids") or []
    asks     = ob.get("asks") or []

    def _fmt_price(v: object) -> str:
        return f"`{float(v):.4f}`" if v is not None else "`—`"

    lines = [
        "📉 *CLOB Live Orderbook / ספר פקודות חי*",
        "",
        f"  🟢 BID / קנייה: {_fmt_price(best_bid)}",
        f"  🔴 ASK / מכירה: {_fmt_price(best_ask)}",
        f"  🔵 MID / אמצע:  {_fmt_price(mid)}",
        f"  ↔️ SPREAD / פער: {_fmt_price(spread)}",
        "",
    ]

    if bids:
        lines.append("*Top Bids / קניות:*")
        for b in bids[:5]:
            lines.append(f"  `{float(b['price']):.4f}` × `{float(b['size']):.1f}`")
        lines.append("")

    if asks:
        lines.append("*Top Asks / מכירות:*")
        for a in asks[:5]:
            lines.append(f"  `{float(a['price']):.4f}` × `{float(a['size']):.1f}`")
        lines.append("")

    lines.append(f"🔄 _עודכן / Updated: {_esc(_now_utc())}_")
    return "\n".join(lines)


def _compute_ai_recs(positions: list[dict], portfolio_val: float) -> list[dict]:
    """Compute AI recommendations for a list of positions. Returns list of rec dicts."""
    recs = []
    for p in positions:
        cur_p   = float(p.get("cur_price") or 0)
        avg_p   = float(p.get("avg_price") or 0)
        pct_pnl = float(p.get("percent_pnl") or 0)
        cur_val = float(p.get("current_value") or 0)
        title   = str(p.get("title") or "?")

        implied   = cur_p
        real_prob = min(0.99, max(0.01, implied + (0.08 if pct_pnl > 0 else -0.06)))
        edge      = (real_prob - implied) * 100
        confidence = min(95.0, 60 + abs(edge) * 2)

        if edge > 5:
            action_he = f"קנה מתחת ל-{implied*100-2:.0f}c"
            action_en = f"BUY below {implied*100-2:.0f}c"
            action_type = "BUY"
        elif edge < -5:
            action_he = f"מכור מעל {implied*100+2:.0f}c"
            action_en = f"SELL above {implied*100+2:.0f}c"
            action_type = "SELL"
        else:
            action_he = "המתן"
            action_en = "HOLD"
            action_type = "HOLD"

        pct_of_portfolio = min(15.0, abs(edge) * 1.5) if portfolio_val > 0 else 0.0
        rec_amount = round(portfolio_val * pct_of_portfolio / 100, 2)

        recs.append({
            "title": title,
            "action_type": action_type,
            "action_he": action_he,
            "action_en": action_en,
            "edge": edge,
            "confidence": confidence,
            "cur_val": cur_val,
            "cur_price": cur_p,
            "avg_price": avg_p,
            "pct_of_portfolio": pct_of_portfolio,
            "rec_amount": rec_amount,
        })
    return recs


def _fmt_poly_ai_recs(dash: dict, cx_data: dict | None) -> str:
    """Format AI recommendations from cross-exchange + positions into Telegram."""
    positions    = dash.get("portfolio_positions_list") or []
    portfolio_val = float(dash.get("portfolio_value") or 0)
    cx_signal    = str((cx_data or {}).get("signal_label") or (cx_data or {}).get("signal") or "—")
    cx_conf      = bool((cx_data or {}).get("high_confidence", False))
    arb_gap      = float((cx_data or {}).get("arbitrage_gap") or 0.0)

    lines = [
        "🤖 *AI Recommendations / המלצות AI*",
        "",
        "📡 *Cross\\-Exchange Signal:*",
        f"  {'⚡' if cx_conf else '📊'} `{_esc(cx_signal)}`",
        f"  ARB Gap: `{arb_gap*100:.3f}%`",
    ]
    if cx_conf:
        lines.append("  🔥 *HIGH CONFIDENCE* — שקול כניסה")
    lines.append("")

    if not positions:
        lines.append("_No positions to analyze / אין פוזיציות לניתוח_")
    else:
        recs = _compute_ai_recs(positions, portfolio_val)
        lines.append("*Position Analysis:*")
        for rec in recs[:5]:
            title = _esc(rec["title"][:35])
            icon  = "🟢" if rec["action_type"] == "BUY" else ("🔴" if rec["action_type"] == "SELL" else "🟡")
            lines += [
                f"{icon} *{title}*",
                f"  {_esc(rec['action_he'])} / {_esc(rec['action_en'])}",
                f"  Edge: `{rec['edge']:+.1f}%` Conf: `{rec['confidence']:.0f}%` Val: `${rec['cur_val']:.2f}`",
                f"  Rec size: `${rec['rec_amount']:.2f}` \\({rec['pct_of_portfolio']:.0f}% of portfolio\\)",
                "",
            ]

    lines.append(f"🔄 _Updated: {_esc(_now_utc())}_")
    return "\n".join(lines)


def _fmt_poly_scalper(scalper: dict) -> str:
    """Format 5m scalper status."""
    wins    = scalper.get("wins", 0)
    losses  = scalper.get("losses", 0)
    halted  = scalper.get("trading_halted", False)
    paper   = scalper.get("paper_trading", True)
    decision = scalper.get("decision") or "—"
    vel     = scalper.get("velocity_pct_60s")
    btc     = scalper.get("btc_price")
    yes_p   = scalper.get("yes_price")

    mode_label = "📄 PAPER / נייר" if paper else "💰 LIVE / חי"
    status_label = "⛔ HALTED / עצור" if halted else "✅ פעיל / Active"

    lines = [
        "⚡ *5m Scalper / סקאלפר 5 דקות*",
        "",
        f"  {mode_label}",
        f"  {status_label}",
        f"  🏆 תוצאות / Results: `{wins}W / {losses}L`",
        f"  🎯 החלטה / Decision: `{_esc(decision)}`",
    ]
    if btc is not None:
        lines.append(f"  ₿ BTC: `${btc:,.0f}`")
    if yes_p is not None:
        lines.append(f"  🎲 YES Price: `{yes_p*100:.1f}¢`")
    if vel is not None:
        vel_icon = "🚀" if vel >= 0 else "📉"
        lines.append(f"  {vel_icon} VEL 60s: `{vel:+.3f}%`")

    lines += ["", f"🔄 _עודכן / Updated: {_esc(_now_utc())}_"]
    return "\n".join(lines)


def _fmt_poly_trade_log(log_data: dict) -> str:
    """Format trade log into Telegram message."""
    entries = log_data.get("entries") or []
    total   = log_data.get("total", 0)
    paper   = log_data.get("paper_trading", True)
    ks_bal  = log_data.get("kill_switch_balance_usd", 0)

    mode_label = "📄 PAPER / נייר" if paper else "💰 LIVE / חי"

    lines = [
        "📋 *יומן עסקאות / Trade Log*",
        "",
        f"  {mode_label} · סה״כ / Total: `{total}`",
        f"  🔒 Kill Switch: `${ks_bal:.2f}`",
        "",
    ]

    if not entries:
        lines.append("_אין היסטוריית מסחר עדיין / No trade history yet_")
    else:
        lines.append("*עסקאות אחרונות / Recent Trades:*")
        for e in entries[:8]:
            ts   = e.get("timestamp", "")
            time_str = ts[11:19] if len(ts) >= 19 else "—"
            mkt  = str(e.get("market_question") or e.get("log_text") or "—")[:35]
            side = str(e.get("side") or "—")
            price = float(e.get("price") or 0)
            spent = float(e.get("spent_usd") or 0)
            status = str(e.get("status") or "—")
            is_paper = e.get("paper", False)

            side_icon = "🟢" if side == "BUY" else "🔴"
            status_label = "📄 PAPER" if is_paper else _esc(status.upper())

            lines += [
                f"{side_icon} `{_esc(time_str)}` · *{_esc(mkt[:30])}*",
                f"  {_esc(side)} @ `{price:.4f}` · `${spent:.2f}` · {status_label}",
                "",
            ]

    lines.append(f"🔄 _עודכן / Updated: {_esc(_now_utc())}_")
    return "\n".join(lines)


async def handle_poly_menu(callback: CallbackQuery) -> None:
    """Show the Polymarket control panel."""
    await callback.answer("טוען Polymarket...")
    await callback.message.edit_text(
        "🎯 *Polymarket Control Center / מרכז שליטה פולימרקט*\n\n"
        "בחר פעולה / Choose an action:",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_polymarket_menu(),
    )


async def handle_poly_portfolio(callback: CallbackQuery) -> None:
    """Show Polymarket portfolio summary."""
    await callback.answer("טוען תיק השקעות...")
    dash = await _api_get("/api/polymarket/dashboard.json")
    if not dash:
        await callback.message.edit_text(
            "⚠️ *שגיאת חיבור / Connection Error*\n\nלא ניתן לטעון נתוני Polymarket\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=get_polymarket_menu(),
        )
        return
    await callback.message.edit_text(
        _fmt_poly_portfolio(dash),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_polymarket_menu(),
    )


async def handle_poly_positions(callback: CallbackQuery) -> None:
    """Show open Polymarket positions."""
    await callback.answer("טוען פוזיציות...")
    dash = await _api_get("/api/polymarket/dashboard.json")
    if not dash:
        await callback.message.edit_text(
            "⚠️ *שגיאת חיבור / Connection Error*",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=get_polymarket_menu(),
        )
        return
    await callback.message.edit_text(
        _fmt_poly_positions(dash),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_polymarket_menu(),
    )


async def handle_poly_orderbook(callback: CallbackQuery) -> None:
    """Show CLOB live orderbook."""
    await callback.answer("טוען ספר פקודות...")
    ob = await _api_get("/api/polymarket/orderbook")
    if ob is None:
        await callback.message.edit_text(
            "⚠️ *שגיאת חיבור / Connection Error*",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=get_polymarket_menu(),
        )
        return
    await callback.message.edit_text(
        _fmt_poly_orderbook(ob),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_polymarket_menu(),
    )


async def handle_poly_ai_recs(callback: CallbackQuery) -> None:
    """Show AI recommendations for Polymarket positions."""
    await callback.answer("מחשב המלצות AI...")
    dash, cx = await asyncio.gather(
        _api_get("/api/polymarket/dashboard.json"),
        _api_get("/api/prediction/cross-exchange"),
    )
    if not dash:
        await callback.message.edit_text(
            "⚠️ *שגיאת חיבור / Connection Error*",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=get_polymarket_menu(),
        )
        return
    await callback.message.edit_text(
        _fmt_poly_ai_recs(dash, cx),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_polymarket_menu(),
    )


async def handle_poly_scalper(callback: CallbackQuery) -> None:
    """Show 5m scalper status."""
    await callback.answer("טוען סקאלפר...")
    data = await _api_get("/api/prediction/poly5m-scalper")
    if not data:
        await callback.message.edit_text(
            "⚠️ *שגיאת חיבור / Connection Error*",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=get_polymarket_menu(),
        )
        return
    await callback.message.edit_text(
        _fmt_poly_scalper(data),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_polymarket_menu(),
    )


async def handle_poly_trade_log(callback: CallbackQuery) -> None:
    """Show recent trade log."""
    await callback.answer("טוען יומן עסקאות...")
    data = await _api_get("/api/prediction/trade-log")
    if not data:
        await callback.message.edit_text(
            "⚠️ *שגיאת חיבור / Connection Error*",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=get_polymarket_menu(),
        )
        return
    await callback.message.edit_text(
        _fmt_poly_trade_log(data),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_polymarket_menu(),
    )


async def handle_poly_buy_prompt(callback: CallbackQuery) -> None:
    """Prompt user to send /poly_buy <token_id> <amount> command."""
    await callback.answer()
    await callback.message.edit_text(
        "🟢 *קנה YES / BUY YES*\n\n"
        "שלח פקודה / Send command:\n"
        "`/poly\\_buy <token\\_id> <amount\\_usdc>`\n\n"
        "לדוגמה / Example:\n"
        "`/poly\\_buy 0xabc123 50`\n\n"
        "_הסכום ב\\-USDC / Amount in USDC_",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_polymarket_menu(),
    )


async def handle_poly_sell_prompt(callback: CallbackQuery) -> None:
    """Prompt user to send /poly_sell <token_id> <amount> command."""
    await callback.answer()
    await callback.message.edit_text(
        "🔴 *מכור / SELL*\n\n"
        "שלח פקודה / Send command:\n"
        "`/poly\\_sell <token\\_id> <amount\\_usdc>`\n\n"
        "לדוגמה / Example:\n"
        "`/poly\\_sell 0xabc123 50`\n\n"
        "_הסכום ב\\-USDC / Amount in USDC_",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_polymarket_menu(),
    )


async def cmd_set_deposit(message: Message) -> None:
    """Set total deposited amount: /set_deposit <amount>"""
    admin_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "")
    if admin_id and str(message.chat.id) != str(admin_id):
        await message.answer("⛔ גישה נדחתה\\.")
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("⚠️ Usage: `/set\\_deposit <amount>`\nExample: `/set\\_deposit 500`", parse_mode=ParseMode.MARKDOWN_V2)
        return
    try:
        amount = float(parts[1])
    except ValueError:
        await message.answer("❌ Invalid amount\\.")
        return
    data = await _api_post("/api/polymarket/set-deposit", {"amount": amount})
    if data:
        await message.answer(
            f"✅ *סכום הפקדה עודכן / Deposit amount set*\n\n"
            f"  Total Deposited: `${amount:,.2f}`\n\n"
            f"_Use /polymarket to see break\\-even_",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    else:
        await message.answer("❌ Failed to update\\. Is the API running?", parse_mode=ParseMode.MARKDOWN_V2)


async def cmd_set_withdrawn(message: Message) -> None:
    """Set total withdrawn amount: /set_withdrawn <amount>"""
    admin_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "")
    if admin_id and str(message.chat.id) != str(admin_id):
        await message.answer("⛔ גישה נדחתה\\.")
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("⚠️ Usage: `/set\\_withdrawn <amount>`", parse_mode=ParseMode.MARKDOWN_V2)
        return
    try:
        amount = float(parts[1])
    except ValueError:
        await message.answer("❌ Invalid amount\\.")
        return
    data = await _api_post("/api/polymarket/set-withdrawn", {"amount": amount})
    if data:
        await message.answer(
            f"✅ *סכום משיכות עודכן / Withdrawn amount set*\n\n"
            f"  Total Withdrawn: `${amount:,.2f}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    else:
        await message.answer("❌ Failed to update\\.", parse_mode=ParseMode.MARKDOWN_V2)


async def cmd_polymarket(message: Message) -> None:
    """Show the Polymarket control panel via /polymarket command."""
    admin_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "")
    if admin_id and str(message.chat.id) != str(admin_id):
        await message.answer("⛔ גישה נדחתה\\.")
        return
    await message.answer(
        "🎯 *Polymarket Control Center / מרכז שליטה פולימרקט*\n\n"
        "בחר פעולה / Choose an action:",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_polymarket_menu(),
    )


async def cmd_poly_buy(message: Message) -> None:
    """Execute a BUY YES order: /poly_buy <token_id> <amount>"""
    admin_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "")
    if admin_id and str(message.chat.id) != str(admin_id):
        await message.answer("⛔ גישה נדחתה\\.")
        return

    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.answer(
            "⚠️ שימוש / Usage: `/poly\\_buy <token\\_id> <amount\\_usdc>`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    token_id = parts[1]
    try:
        amount = float(parts[2])
    except ValueError:
        await message.answer("❌ סכום לא תקין / Invalid amount\\.")
        return

    await message.answer(f"⏳ מבצע קנייה / Executing BUY… `{_esc(token_id[:20])}` × `${amount:.2f}`", parse_mode=ParseMode.MARKDOWN_V2)

    data = await _api_post("/api/polymarket/manual-order", {
        "token_id": token_id,
        "side": "BUY",
        "amount": amount,
    })
    if data:
        order_id = _esc(str(data.get("order_id") or ""))
        paper    = data.get("paper", False)
        spent    = float(data.get("spent_usd") or 0)
        mode     = "PAPER" if paper else "LIVE"
        await message.answer(
            f"✅ *BUY executed / פקודת קנייה בוצעה*\n\n"
            f"  Mode: `{mode}`\n"
            f"  Spent: `${spent:.2f}`\n"
            f"  Order ID: `{order_id}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    else:
        await message.answer("❌ *Execution failed / שגיאה בביצוע*\n\nבדוק את חיבור ה\\-API\\.", parse_mode=ParseMode.MARKDOWN_V2)


async def cmd_poly_sell(message: Message) -> None:
    """Execute a SELL order: /poly_sell <token_id> <amount>"""
    admin_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "")
    if admin_id and str(message.chat.id) != str(admin_id):
        await message.answer("⛔ גישה נדחתה\\.")
        return

    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.answer(
            "⚠️ שימוש / Usage: `/poly\\_sell <token\\_id> <amount\\_usdc>`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    token_id = parts[1]
    try:
        amount = float(parts[2])
    except ValueError:
        await message.answer("❌ סכום לא תקין / Invalid amount\\.")
        return

    await message.answer(f"⏳ מבצע מכירה / Executing SELL… `{_esc(token_id[:20])}` × `${amount:.2f}`", parse_mode=ParseMode.MARKDOWN_V2)

    data = await _api_post("/api/polymarket/manual-order", {
        "token_id": token_id,
        "side": "SELL",
        "amount": amount,
    })
    if data:
        order_id = _esc(str(data.get("order_id") or ""))
        paper    = data.get("paper", False)
        spent    = float(data.get("spent_usd") or 0)
        mode     = "PAPER" if paper else "LIVE"
        await message.answer(
            f"✅ *SELL executed / פקודת מכירה בוצעה*\n\n"
            f"  Mode: `{mode}`\n"
            f"  Spent: `${spent:.2f}`\n"
            f"  Order ID: `{order_id}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    else:
        await message.answer("❌ *Execution failed / שגיאה בביצוע*\n\nבדוק את חיבור ה\\-API\\.", parse_mode=ParseMode.MARKDOWN_V2)


async def send_poly_ai_alert(bot: "Bot", chat_id: str) -> None:
    """
    Proactive AI alert: fetch Polymarket data and send high-confidence
    trade recommendations with BUY/SELL/DISMISS approve buttons.
    """
    try:
        dash, cx = await asyncio.gather(
            _api_get("/api/polymarket/dashboard.json"),
            _api_get("/api/prediction/cross-exchange"),
        )
        if not dash:
            return

        positions     = dash.get("portfolio_positions_list") or []
        cx_conf       = bool((cx or {}).get("high_confidence"))
        cx_signal     = str((cx or {}).get("signal_label") or (cx or {}).get("signal") or "")
        arb_gap       = float((cx or {}).get("arbitrage_gap") or 0.0)
        portfolio_val = float(dash.get("portfolio_value") or 0.0)

        recs = _compute_ai_recs(positions, portfolio_val)
        # Only alert on high-confidence recs (>= 75%)
        high_conf_recs = [r for r in recs if r["confidence"] >= 75 and r["action_type"] != "HOLD"]

        # Also include cross-exchange high-conf signal
        cx_alert_lines: list[str] = []
        cx_rec_amt: float = 0.0
        if cx_conf and cx_signal:
            pct = 10.0
            cx_rec_amt = round(portfolio_val * pct / 100, 2)
            signal_upper = cx_signal.upper()
            is_buy = "BUY" in signal_upper
            signal_icon = "📈" if is_buy else "📉"
            action_icon = "🟢" if is_buy else "🔴"
            cx_alert_lines = [
                f"⚡ *HIGH CONFIDENCE SIGNAL* / אות בביטחון גבוה",
                f"",
                f"{signal_icon} Signal: `{_esc(cx_signal)}`",
                f"📊 ARB Gap: `{arb_gap*100:.3f}%`",
                f"💰 Rec / המלצה: `${cx_rec_amt:.2f}` \\({pct:.0f}% מהתיק\\)",
                f"",
                f"_Use `/poly\\_buy` to execute_",
                f"",
            ]

        if not high_conf_recs and not cx_alert_lines:
            return

        # Build one alert message per high-conf rec (with approve/reject buttons)
        for rec in high_conf_recs[:3]:
            title_short = rec["title"][:40]
            icon = "🟢" if rec["action_type"] == "BUY" else "🔴"

            alert_text = (
                f"🚨 *AI ALERT — Polymarket*\n\n"
                f"{icon} *{_esc(title_short)}*\n\n"
                f"  Action: `{_esc(rec['action_en'])}`\n"
                f"  Edge: `{rec['edge']:+.1f}%`\n"
                f"  Confidence: `{rec['confidence']:.0f}%`\n"
                f"  Current Value: `${rec['cur_val']:.2f}`\n"
                f"  Avg Price: `{rec['avg_price']*100:.1f}c` → Now: `{rec['cur_price']*100:.1f}c`\n\n"
                f"  💡 Recommended size: `${rec['rec_amount']:.2f}` \\({rec['pct_of_portfolio']:.0f}% of portfolio\\)\n\n"
                f"_האם לבצע? / Execute?_\n"
                f"🔄 _{_esc(_now_utc())}_"
            )

            # Encode token_id + amount into callback for direct execution
            # Format: poly_exec_approve:<side>:<rec_amount>
            side = rec["action_type"]
            amt  = rec["rec_amount"]

            alert_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=f"✅ בצע {side} ${amt:.0f} / Execute",
                        callback_data=f"poly_alert_approve:{side}:{amt:.2f}",
                    ),
                    InlineKeyboardButton(
                        text="❌ דחה / Dismiss",
                        callback_data="poly_alert_dismiss",
                    ),
                ],
                [
                    InlineKeyboardButton(text="📊 Positions", callback_data="poly_positions"),
                    InlineKeyboardButton(text="🎯 Panel",     callback_data="poly_menu"),
                ],
            ])

            await bot.send_message(
                chat_id=chat_id,
                text=alert_text,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=alert_keyboard,
            )

        # Send cross-exchange alert with approve/execute buttons
        if cx_alert_lines:
            is_cx_buy = "BUY" in cx_signal.upper()
            cx_side = "BUY" if is_cx_buy else "SELL"
            cx_text = (
                "🚨 *Polymarket AI Alert / פולימרקט AI התראת*\n\n"
                + "\n".join(cx_alert_lines)
                + f"🔄 _{_esc(_now_utc())}_"
            )
            cx_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=f"✅ בצע {cx_side} ${cx_rec_amt:.0f} / Execute",
                        callback_data=f"poly_cx_approve:{cx_side}:{cx_rec_amt:.2f}",
                    ),
                    InlineKeyboardButton(text="❌ דחה / Dismiss", callback_data="poly_alert_dismiss"),
                ],
                [
                    InlineKeyboardButton(text="📊 Positions / פוזיציות", callback_data="poly_positions"),
                    InlineKeyboardButton(text="🤖 AI / AI Recs המלצות", callback_data="poly_ai_recs"),
                ],
                [
                    InlineKeyboardButton(text="🎯 Polymarket Panel", callback_data="poly_menu"),
                ],
            ])
            await bot.send_message(
                chat_id=chat_id,
                text=cx_text,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=cx_keyboard,
            )

        log.info("poly_ai_alert_sent", high_conf_count=len(high_conf_recs), cx_alert=bool(cx_alert_lines))

    except Exception as exc:
        log.warning("poly_ai_alert_error", error=str(exc))


async def handle_poly_alert_approve(callback: CallbackQuery) -> None:
    """Handle approve button on AI alert — show prompt to execute with token_id."""
    if callback.data is None:
        await callback.answer("Invalid.")
        return

    # callback_data format: poly_alert_approve:<side>:<amount>
    parts = callback.data.split(":")
    if len(parts) < 3:
        await callback.answer("Invalid format.")
        return

    side   = parts[1].upper()
    amount = parts[2]
    cmd    = "buy" if side == "BUY" else "sell"
    icon   = "📈" if side == "BUY" else "📉"

    await callback.answer("✅ אישור התקבל / Approved")
    await callback.message.edit_text(
        f"✅ *Approved / אושר* {icon}\n\n"
        f"⚡ לביצוע, שלח:\n"
        f"`/poly\\_{cmd} <token\\_id> {_esc(amount)}`\n\n"
        f"💡 _קבל token\\_id מ: /polymarket → Positions_",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="📊 Positions / פוזיציות", callback_data="poly_positions"),
                InlineKeyboardButton(text="🎯 Panel", callback_data="poly_menu"),
            ],
        ]),
    )


async def handle_poly_cx_approve(callback: CallbackQuery) -> None:
    """Handle approve button on Cross-Exchange AI alert — show execute prompt."""
    if callback.data is None:
        await callback.answer("Invalid.")
        return

    # callback_data format: poly_cx_approve:<side>:<amount>
    parts = callback.data.split(":")
    if len(parts) < 3:
        await callback.answer("Invalid format.")
        return

    side   = parts[1].upper()
    amount = parts[2]
    cmd    = "buy" if side == "BUY" else "sell"
    icon   = "📈" if side == "BUY" else "📉"

    await callback.answer("✅ אישור התקבל / Approved")
    await callback.message.edit_text(
        f"✅ *Approved / אושר* {icon}\n\n"
        f"⚡ *Cross\\-Exchange Signal — ביצוע*\n\n"
        f"שלח את הפקודה הבאה עם ה\\-token\\_id:\n"
        f"`/poly\\_{cmd} <token\\_id> {_esc(amount)}`\n\n"
        f"💡 _קבל token\\_id מ: /polymarket → Positions_\n"
        f"📊 _ARB Gap מצביע על הזדמנות ארביטראז' חוצת בורסות_",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="📊 Positions / פוזיציות", callback_data="poly_positions"),
                InlineKeyboardButton(text="🤖 AI Recs", callback_data="poly_ai_recs"),
            ],
            [
                InlineKeyboardButton(text="🎯 Polymarket Panel", callback_data="poly_menu"),
            ],
        ]),
    )


async def handle_poly_alert_dismiss(callback: CallbackQuery) -> None:
    """Dismiss an AI alert."""
    await callback.answer("נדחה / Dismissed")
    try:
        original = callback.message.text or ""
        await callback.message.edit_text(
            original + "\n\n_❌ Dismissed / נדחה_",
            reply_markup=None,
        )
    except Exception:
        pass


async def _poly_alert_loop(bot: "Bot", chat_id: str, interval_s: int = 300) -> None:
    """Background loop that sends Polymarket AI alerts every `interval_s` seconds."""
    log.info("poly_alert_loop_started", interval_s=interval_s)
    while True:
        await asyncio.sleep(interval_s)
        await send_poly_ai_alert(bot, chat_id)


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


# ── Full-menu new button handlers ─────────────────────────────────────────────

async def handle_incubator_menu(callback: CallbackQuery) -> None:
    """Show Incubator Engine status via inline button."""
    await callback.answer("טוען נתוני Incubator...")
    data = await _api_get("/api/incubator/projects")
    if not data:
        await callback.message.edit_text(
            "❌ *Incubator* — לא ניתן להתחבר ל\\-API\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=get_start_menu(),
        )
        return

    projects = data.get("projects", [])
    god_mode = "✅ פעיל" if data.get("god_mode") else "❌ כבוי"
    live = [p for p in projects if p.get("status") == "live"]
    pending = [p for p in projects if p.get("status") == "pending_review"]

    lines = [
        "🧬 *INCUBATOR ENGINE*",
        "",
        f"⚙️ God Mode: {god_mode}",
        f"📦 פרויקטים פעילים: `{len(live)}`",
        f"🔍 ממתינים לאישור: `{len(pending)}`",
        f"📊 סה\"כ פרויקטים: `{len(projects)}`",
    ]
    if live:
        lines += ["", "🟢 *פעילים:*"]
        for p in live[:5]:
            lines.append(f"  • `{_esc(p.get('project_id', '?'))}` — {_esc(p.get('niche', ''))}")
    if pending:
        lines += ["", "🟡 *ממתינים:*"]
        for p in pending[:3]:
            lines.append(f"  • `{_esc(p.get('project_id', '?'))}`")

    await callback.message.edit_text(
        "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 חזרה לתפריט", callback_data="start_menu")],
        ]),
    )


async def handle_godmode_menu(callback: CallbackQuery) -> None:
    """Show God Mode toggle panel."""
    await callback.answer()
    data = await _api_get("/api/incubator/projects")
    god_mode_on = bool(data and data.get("god_mode"))
    status_line = "✅ *GOD MODE פעיל*" if god_mode_on else "❌ *GOD MODE כבוי*"
    await callback.message.edit_text(
        f"⚙️ *GOD MODE — שליטה אוטומטית*\n\n"
        f"סטטוס נוכחי: {status_line}\n\n"
        f"כאשר פעיל, המערכת פורסת פרויקטים חדשים אוטומטית ללא אישור ידני\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ הפעל God Mode",  callback_data="godmode_on_btn"),
                InlineKeyboardButton(text="❌ כבה God Mode",   callback_data="godmode_off_btn"),
            ],
            [InlineKeyboardButton(text="🔙 חזרה לתפריט",      callback_data="start_menu")],
        ]),
    )


async def handle_godmode_on_btn(callback: CallbackQuery) -> None:
    """Enable God Mode via inline button."""
    await callback.answer("מפעיל God Mode...")
    data = await _api_post("/api/incubator/god-mode", {"enabled": True})
    if data:
        await callback.message.edit_text(
            "✅ *GOD MODE הופעל\\!*\n\nהמערכת תפרוס פרויקטים חדשים אוטומטית\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 חזרה לתפריט", callback_data="start_menu")],
            ]),
        )
    else:
        await callback.answer("❌ שגיאה — לא ניתן להפעיל God Mode", show_alert=True)


async def handle_godmode_off_btn(callback: CallbackQuery) -> None:
    """Disable God Mode via inline button."""
    await callback.answer("מכבה God Mode...")
    data = await _api_post("/api/incubator/god-mode", {"enabled": False})
    if data:
        await callback.message.edit_text(
            "❌ *GOD MODE כובה\\.*\n\nהמערכת תדרוש אישור ידני לפני פריסת פרויקטים\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 חזרה לתפריט", callback_data="start_menu")],
            ]),
        )
    else:
        await callback.answer("❌ שגיאה — לא ניתן לכבות God Mode", show_alert=True)


async def handle_killswitch_btn(callback: CallbackQuery) -> None:
    """Kill Switch confirmation panel via inline button."""
    await callback.answer()
    await callback.message.edit_text(
        "🚨 *KILL SWITCH*\n\n"
        "פעולה זו תעצור את כל הפרויקטים האוטונומיים הפעילים מיידית\\.\n\n"
        "⚠️ האם אתה בטוח?",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🚨 כן, הפעל Kill Switch", callback_data="killswitch_confirm"),
                InlineKeyboardButton(text="❌ ביטול",                callback_data="start_menu"),
            ],
        ]),
    )


async def handle_killswitch_confirm(callback: CallbackQuery) -> None:
    """Execute Kill Switch after confirmation."""
    await callback.answer("מפעיל Kill Switch...", show_alert=False)
    data = await _api_get("/api/incubator/projects")
    projects = (data or {}).get("projects", [])
    killed = 0
    for p in projects:
        if p.get("status") in ("live", "pending_review"):
            res = await _api_post(f"/api/incubator/kill/{p['project_id']}")
            if res:
                killed += 1
    await _api_post("/api/incubator/god-mode", {"enabled": False})
    await callback.message.edit_text(
        f"🚨 *KILL SWITCH הופעל*\n\n"
        f"✅ {killed} פרויקטים הופסקו\\.\n"
        f"❌ God Mode כובה\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 חזרה לתפריט", callback_data="start_menu")],
        ]),
    )


async def handle_terminate_btn(callback: CallbackQuery) -> None:
    """Terminate Nexus confirmation panel via inline button."""
    await callback.answer()
    await callback.message.edit_text(
        "🔒 *TERMINATE NEXUS*\n\n"
        "פעולה זו תבצע כיבוי מלא של כל מערכת Nexus:\n"
        "• עצירת Workers\n"
        "• שטיחת חשיפות\n"
        "• ניתוק Redis\n\n"
        "⚠️ *פעולה בלתי הפיכה\\!* האם להמשיך?",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🔒 כן, סיים הכל", callback_data="terminate_confirm"),
                InlineKeyboardButton(text="❌ ביטול",         callback_data="start_menu"),
            ],
        ]),
    )


async def handle_terminate_confirm(callback: CallbackQuery) -> None:
    """Execute full Nexus termination after confirmation."""
    await callback.answer("מבצע כיבוי מלא...", show_alert=False)
    extra: dict[str, str] = {}
    tok = (os.environ.get("NEXUS_KILL_SWITCH_API_TOKEN") or "").strip()
    if tok:
        extra["X-Nexus-Kill-Auth"] = tok
    data = await _api_post(
        "/api/system/kill-switch",
        {"confirm": "TERMINATE_NEXUS_NOW", "evacuate": False},
        extra_headers=extra or None,
    )
    status = _esc(data.get("status", "unknown")) if data else "לא זמין"
    await callback.message.edit_text(
        f"🔒 *NEXUS TERMINATED*\n\n`status`: {status}",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 חזרה לתפריט", callback_data="start_menu")],
        ]),
    )


async def handle_dashboard_btn(callback: CallbackQuery) -> None:
    """Send dashboard link via inline button."""
    await callback.answer()
    await callback.message.edit_text(
        f"🔗 *Nexus Dashboard*\n\n"
        f"פתח את מרכז השליטה:\n"
        f"{_esc(DASHBOARD_URL)}\n\n"
        f"_טיפ: השתמש ב\\-Tailscale VPN לגישה מכל מקום\\._",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 חזרה לתפריט", callback_data="start_menu")],
        ]),
    )


async def handle_help_btn(callback: CallbackQuery) -> None:
    """Show help & commands list via inline button."""
    await callback.answer()
    await callback.message.edit_text(
        "❓ *פקודות זמינות*\n\n"
        "/start — תפריט ראשי\n"
        "/dashboard — קישור לדשבורד\n"
        "/polymarket — לוח שליטה Polymarket\n"
        "/poly\\_buy \\<id\\> \\<amount\\> — קנה YES\n"
        "/poly\\_sell \\<id\\> \\<amount\\> — מכור\n"
        "/set\\_deposit \\<amount\\> — הגדר הפקדה\n"
        "/set\\_withdrawn \\<amount\\> — הגדר משיכה\n"
        "/killswitch — עצור פרויקטים אוטונומיים\n"
        "/terminate\\_nexus\\_now — כיבוי מלא\n"
        "/godmode\\_on — הפעל God Mode\n"
        "/godmode\\_off — כבה God Mode\n"
        "/incubator — סטטוס Incubator\n"
        "/help — הצג עזרה\n\n"
        "_כפתורי HITL מופיעים אוטומטית כשמשימה ממתינה לאישור\\._",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 חזרה לתפריט", callback_data="start_menu")],
        ]),
    )


async def handle_start_menu(callback: CallbackQuery) -> None:
    """Return to the full /start inline menu."""
    await callback.answer()
    name = callback.from_user.first_name if callback.from_user else "מפעיל"
    welcome = (
        "🎯 *Nexus Orchestrator — מרכז פיקוד*\n\n"
        f"ברוך הבא, {_esc(name)}\\!\n\n"
        "המערכת פעילה — בחר פעולה:"
    )
    await callback.message.edit_text(
        welcome,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_start_menu(),
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
        "/polymarket — 🎯 Polymarket control panel / לוח שליטה פולימרקט\n"
        "/poly\\_buy \\<token\\_id\\> \\<amount\\> — 🟢 BUY YES order / קנה\n"
        "/poly\\_sell \\<token\\_id\\> \\<amount\\> — 🔴 SELL order / מכור\n"
        "/set\\_deposit \\<amount\\> — 💰 Set total deposited \\(break\\-even tracking\\)\n"
        "/set\\_withdrawn \\<amount\\> — 💸 Set total withdrawn\n"
        "/killswitch — 🚨 KILL all autonomous projects instantly\n"
        "/terminate\\_nexus\\_now — 🔒 *Full* emergency kill\\-switch \\(user ID + API/Redis\\)\n"
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


async def cmd_terminate_nexus_now(message: Message) -> None:
    """
    Full NEXUS kill-switch: trading halt, workers (TERMINATE+FORCE_STOP), exposure flatten, env wipe.

    Only the Telegram user id in TELEGRAM_ADMIN_USER_ID may invoke this (not chat id).
    """
    allowed = (os.environ.get("TELEGRAM_ADMIN_USER_ID") or "").strip()
    uid = str(message.from_user.id) if message.from_user else ""
    if not allowed or uid != allowed:
        await message.answer("⛔ Unauthorized\\. Set `TELEGRAM_ADMIN_USER_ID` to your numeric user id\\.")
        return

    await message.answer(
        "🔒 NEXUS KILL-SWITCH\n\n"
        "Phase 1: Redis + worker TERMINATE/FORCE_STOP. Then flatten exposure + secure report.",
    )

    extra: dict[str, str] = {}
    tok = (os.environ.get("NEXUS_KILL_SWITCH_API_TOKEN") or "").strip()
    if tok:
        extra["X-Nexus-Kill-Auth"] = tok

    data = await _api_post(
        "/api/system/kill-switch",
        {"confirm": "TERMINATE_NEXUS_NOW", "evacuate": False},
        extra_headers=extra or None,
    )
    if data:
        await message.answer(
            "✅ *Kill\\-switch engaged*\n\n"
            f"`status`: {_esc(data.get('status', ''))}\n"
            f"`elapsed_ms`: {_esc(str(data.get('elapsed_ms', '')))}\n\n"
            "_Background: close exposure, env wipe, Telegram secure report\\._",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        log.critical("telegram_full_kill_switch_api_ok", user_id=uid)
        return

    redis_url = (os.environ.get("REDIS_URL") or "").strip()
    if not redis_url:
        await message.answer(
            "❌ API request failed and `REDIS_URL` is not set\\.\n"
            "Start the Control Center API or set `REDIS_URL` for direct Redis mode\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    try:
        from redis.asyncio import from_url as redis_from_url

        from nexus.shared.kill_switch import run_full_kill_switch

        rcli = redis_from_url(redis_url, decode_responses=True)
        try:
            rep = await run_full_kill_switch(
                rcli,
                reason="telegram_direct_redis",
                source=f"tg:{uid}",
                evacuate=False,
            )
        finally:
            await rcli.aclose()
        await message.answer(
            "✅ *Kill\\-switch via direct Redis*\n\n"
            f"`activated`: {_esc(str(rep.get('activated_at', ''))[:24])}",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        log.critical("telegram_full_kill_switch_redis_ok", user_id=uid)
    except Exception as exc:
        await message.answer(f"❌ Direct Redis kill\\-switch failed: {_esc(str(exc))}", parse_mode=ParseMode.MARKDOWN_V2)
        log.error("telegram_full_kill_switch_redis_error", error=str(exc))


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
    dp.message.register(cmd_terminate_nexus_now, Command("terminate_nexus_now"))
    dp.message.register(cmd_godmode_on, Command("godmode_on"))
    dp.message.register(cmd_godmode_off, Command("godmode_off"))
    dp.message.register(cmd_incubator, Command("incubator"))
    dp.message.register(cmd_polymarket,    Command("polymarket"))
    dp.message.register(cmd_poly_buy,      Command("poly_buy"))
    dp.message.register(cmd_poly_sell,     Command("poly_sell"))
    dp.message.register(cmd_set_deposit,   Command("set_deposit"))
    dp.message.register(cmd_set_withdrawn, Command("set_withdrawn"))

    # /start control-panel callbacks (4-button 2×2 grid)
    dp.callback_query.register(handle_status,             F.data == "status")
    dp.callback_query.register(handle_live_ops_status,     F.data == "live_ops")
    dp.callback_query.register(handle_check_sentinel,     F.data == "check_sentinel")
    dp.callback_query.register(handle_panic_stop,         F.data == "panic_stop")
    dp.callback_query.register(handle_panic_confirm,      F.data == "panic_confirm")
    dp.callback_query.register(handle_panic_cancel,       F.data == "panic_cancel")
    dp.callback_query.register(handle_launch_moltbot,     F.data == "launch_moltbot")

    # V2 Hebrew menu handlers
    dp.callback_query.register(handle_menu_stats,  F.data == "menu_stats")
    dp.callback_query.register(handle_menu_cluster, F.data == "menu_cluster")
    dp.callback_query.register(handle_menu_wallet, F.data == "menu_wallet")
    dp.callback_query.register(handle_main_menu,   F.data == "main_menu")

    # Full-menu new button handlers
    dp.callback_query.register(handle_incubator_menu,    F.data == "incubator_menu")
    dp.callback_query.register(handle_godmode_menu,      F.data == "godmode_menu")
    dp.callback_query.register(handle_godmode_on_btn,    F.data == "godmode_on_btn")
    dp.callback_query.register(handle_godmode_off_btn,   F.data == "godmode_off_btn")
    dp.callback_query.register(handle_killswitch_btn,    F.data == "killswitch_btn")
    dp.callback_query.register(handle_killswitch_confirm, F.data == "killswitch_confirm")
    dp.callback_query.register(handle_terminate_btn,     F.data == "terminate_btn")
    dp.callback_query.register(handle_terminate_confirm, F.data == "terminate_confirm")
    dp.callback_query.register(handle_dashboard_btn,     F.data == "dashboard_btn")
    dp.callback_query.register(handle_help_btn,          F.data == "help_btn")
    dp.callback_query.register(handle_start_menu,        F.data == "start_menu")

    # Polymarket control panel callbacks
    dp.callback_query.register(handle_poly_menu,         F.data == "poly_menu")
    dp.callback_query.register(handle_poly_portfolio,    F.data == "poly_portfolio")
    dp.callback_query.register(handle_poly_positions,    F.data == "poly_positions")
    dp.callback_query.register(handle_poly_orderbook,    F.data == "poly_orderbook")
    dp.callback_query.register(handle_poly_ai_recs,      F.data == "poly_ai_recs")
    dp.callback_query.register(handle_poly_scalper,      F.data == "poly_scalper")
    dp.callback_query.register(handle_poly_trade_log,    F.data == "poly_trade_log")
    dp.callback_query.register(handle_poly_buy_prompt,   F.data == "poly_buy_prompt")
    dp.callback_query.register(handle_poly_sell_prompt,  F.data == "poly_sell_prompt")
    dp.callback_query.register(handle_poly_alert_approve, F.data.startswith("poly_alert_approve:"))
    dp.callback_query.register(handle_poly_cx_approve,    F.data.startswith("poly_cx_approve:"))
    dp.callback_query.register(handle_poly_alert_dismiss, F.data == "poly_alert_dismiss")

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

    # Remote Prompt Bridge — registered last so all Command filters take priority
    dp.message.register(
        handle_remote_prompt,
        F.from_user.id == _JACOB_USER_ID,
        ~F.text.startswith("/"),
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
    dp.message.register(cmd_terminate_nexus_now, Command("terminate_nexus_now"))
    dp.message.register(cmd_godmode_on, Command("godmode_on"))
    dp.message.register(cmd_godmode_off, Command("godmode_off"))
    dp.message.register(cmd_incubator, Command("incubator"))
    dp.message.register(cmd_polymarket,    Command("polymarket"))
    dp.message.register(cmd_poly_buy,      Command("poly_buy"))
    dp.message.register(cmd_poly_sell,     Command("poly_sell"))
    dp.message.register(cmd_set_deposit,   Command("set_deposit"))
    dp.message.register(cmd_set_withdrawn, Command("set_withdrawn"))

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
    dp.callback_query.register(handle_launch_moltbot,    F.data == "launch_moltbot")

    # V2 Hebrew menu callbacks
    dp.callback_query.register(handle_menu_stats,  F.data == "menu_stats")
    dp.callback_query.register(handle_menu_cluster, F.data == "menu_cluster")
    dp.callback_query.register(handle_menu_wallet, F.data == "menu_wallet")
    dp.callback_query.register(handle_main_menu,   F.data == "main_menu")

    # Full-menu new button handlers
    dp.callback_query.register(handle_incubator_menu,    F.data == "incubator_menu")
    dp.callback_query.register(handle_godmode_menu,      F.data == "godmode_menu")
    dp.callback_query.register(handle_godmode_on_btn,    F.data == "godmode_on_btn")
    dp.callback_query.register(handle_godmode_off_btn,   F.data == "godmode_off_btn")
    dp.callback_query.register(handle_killswitch_btn,    F.data == "killswitch_btn")
    dp.callback_query.register(handle_killswitch_confirm, F.data == "killswitch_confirm")
    dp.callback_query.register(handle_terminate_btn,     F.data == "terminate_btn")
    dp.callback_query.register(handle_terminate_confirm, F.data == "terminate_confirm")
    dp.callback_query.register(handle_dashboard_btn,     F.data == "dashboard_btn")
    dp.callback_query.register(handle_help_btn,          F.data == "help_btn")
    dp.callback_query.register(handle_start_menu,        F.data == "start_menu")

    # Polymarket control panel callbacks
    dp.callback_query.register(handle_poly_menu,          F.data == "poly_menu")
    dp.callback_query.register(handle_poly_portfolio,     F.data == "poly_portfolio")
    dp.callback_query.register(handle_poly_positions,     F.data == "poly_positions")
    dp.callback_query.register(handle_poly_orderbook,     F.data == "poly_orderbook")
    dp.callback_query.register(handle_poly_ai_recs,       F.data == "poly_ai_recs")
    dp.callback_query.register(handle_poly_scalper,       F.data == "poly_scalper")
    dp.callback_query.register(handle_poly_trade_log,     F.data == "poly_trade_log")
    dp.callback_query.register(handle_poly_buy_prompt,    F.data == "poly_buy_prompt")
    dp.callback_query.register(handle_poly_sell_prompt,   F.data == "poly_sell_prompt")
    dp.callback_query.register(handle_poly_alert_approve, F.data.startswith("poly_alert_approve:"))
    dp.callback_query.register(handle_poly_cx_approve,    F.data.startswith("poly_cx_approve:"))
    dp.callback_query.register(handle_poly_alert_dismiss, F.data == "poly_alert_dismiss")

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

    # Remote Prompt Bridge — registered last so all Command filters take priority
    dp.message.register(
        handle_remote_prompt,
        F.from_user.id == _JACOB_USER_ID,
        ~F.text.startswith("/"),
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

    # ── Start Polymarket AI alert loop (every 5 minutes) ──────────────────────
    admin_chat_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "").strip()
    poly_alert_task: asyncio.Task | None = None
    if admin_chat_id:
        poly_alert_task = asyncio.create_task(
            _poly_alert_loop(bot, admin_chat_id, interval_s=300),
            name="poly-alert-loop",
        )
        log.info("poly_alert_loop_started", chat_id=admin_chat_id, interval_s=300)

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
        if poly_alert_task and not poly_alert_task.done():
            poly_alert_task.cancel()
            try:
                await poly_alert_task
            except asyncio.CancelledError:
                pass
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
