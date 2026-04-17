"""
Nexus Supreme — Utility Tools Telegram Handler Registration
Registers all 27 tool commands into an aiogram 3.x Dispatcher.

Inject into your bot with:
    from nexus_supreme.core.tools.telegram_handlers import register_all_tools
    register_all_tools(dp, bot, owner_id, api_id, api_hash)
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import structlog

log = structlog.get_logger(__name__)
ROOT = Path(__file__).resolve().parents[3]

# ── Helpers ───────────────────────────────────────────────────────────────────

def _esc(t: str) -> str:
    for ch in r"\_*[]()~`>#+-=|{}.!":
        t = t.replace(ch, f"\\{ch}")
    return t


def _owner_id() -> int:
    try:
        return int(os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "0").strip())
    except ValueError:
        return 0


def _session_dir() -> Path:
    return (
        Path(os.environ.get("TELEFIX_SESSIONS_DIR", "")).expanduser()
        or Path(os.environ.get("TELEFIX_ROOT", str(Path.home() / "Desktop" / "Mangement Ahu")))
        / "sessions"
    )


def _first_session() -> str:
    """Return the first live .session file path string, or empty string."""
    d = _session_dir()
    files = list(d.rglob("*.session")) if d.exists() else []
    return str(files[0]) if files else ""


def _all_sessions() -> list[str]:
    d = _session_dir()
    return [str(f) for f in d.rglob("*.session")] if d.exists() else []


def _api() -> tuple[int, str]:
    return (
        int(os.environ.get("TELEGRAM_API_ID",  "0")),
        os.environ.get("TELEGRAM_API_HASH", ""),
    )


# ── Guard decorator factory ────────────────────────────────────────────────────

def _make_guard(owner_id: int):
    async def guard(msg) -> bool:
        uid = getattr(getattr(msg, "from_user", None), "id", None)
        if uid != owner_id:
            await msg.answer("⛔ Unauthorized\\.", parse_mode="MarkdownV2")
            return False
        return True
    return guard


# ── Reply helpers ─────────────────────────────────────────────────────────────

async def _reply(msg, text: str) -> None:
    """Send with MarkdownV2; fall back to plain text on parse error."""
    try:
        await msg.answer(text, parse_mode="MarkdownV2",
                         disable_web_page_preview=True)
    except Exception:
        await msg.answer(text[:4000])


# ── Registration ──────────────────────────────────────────────────────────────

def register_all_tools(dp, bot, owner_id: int | None = None,
                       api_id: int = 0, api_hash: str = "") -> None:
    """
    Register all 27 utility tool commands on `dp` (aiogram Dispatcher).
    Call once after creating the dispatcher, before polling starts.
    """
    from aiogram.filters import Command

    oid = owner_id or _owner_id()
    aid, ahash = api_id or _api()[0], api_hash or _api()[1]
    guard = _make_guard(oid)

    # ── DevOps / Monitor (tools 1–6) ──────────────────────────────────────────

    @dp.message.register(Command("sysmon"))
    async def cmd_sysmon(msg):
        if not await guard(msg):
            return
        from .monitor import get_system_metrics, format_sysmon
        await _reply(msg, format_sysmon(get_system_metrics()))

    @dp.message.register(Command("logs"))
    async def cmd_logs(msg):
        if not await guard(msg):
            return
        from .monitor import tail_logs
        parts = (msg.text or "").split(maxsplit=1)
        n     = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 40
        lines = tail_logs(n)
        await _reply(msg, f"```\n{lines[:3500]}\n```")

    @dp.message.register(Command("sessions_check"))
    async def cmd_sessions_check(msg):
        if not await guard(msg):
            return
        await msg.answer("⏳ בודק סשנים…")
        from .monitor import check_sessions_health, format_sessions
        r = await check_sessions_health(aid, ahash)
        await _reply(msg, format_sessions(r))

    @dp.message.register(Command("watchdog"))
    async def cmd_watchdog(msg):
        if not await guard(msg):
            return
        from .monitor import watchdog_status
        await _reply(msg, watchdog_status())

    @dp.message.register(Command("watchdog_on"))
    async def cmd_watchdog_on(msg):
        if not await guard(msg):
            return
        from .monitor import watchdog_enable
        await _reply(msg, watchdog_enable())

    @dp.message.register(Command("watchdog_off"))
    async def cmd_watchdog_off(msg):
        if not await guard(msg):
            return
        from .monitor import watchdog_disable
        await _reply(msg, watchdog_disable())

    # ── Intelligence (tools 7–12) ─────────────────────────────────────────────

    @dp.message.register(Command("seo"))
    async def cmd_seo(msg):
        if not await guard(msg):
            return
        parts = (msg.text or "").split(maxsplit=1)
        if len(parts) < 2:
            await _reply(msg, "שימוש: `/seo @username`")
            return
        await msg.answer("⏳ בודק SEO…")
        from .intelligence import check_seo_visibility, format_seo
        obs = _first_session()
        if not obs:
            await _reply(msg, "❌ אין סשן Observer זמין")
            return
        r = await check_seo_visibility(parts[1], obs, aid, ahash)
        await _reply(msg, format_seo(r))

    @dp.message.register(Command("trends"))
    async def cmd_trends(msg):
        if not await guard(msg):
            return
        parts = (msg.text or "").split(maxsplit=1)
        if len(parts) < 2:
            await _reply(msg, "שימוש: `/trends <channel\\_id>`")
            return
        await msg.answer("⏳ סורק טרנדים…")
        from .intelligence import scan_channel_trends, format_trends
        sess = _first_session()
        r = await scan_channel_trends(parts[1], sess, aid, ahash)
        await _reply(msg, format_trends(r))

    @dp.message.register(Command("stats"))
    async def cmd_stats(msg):
        if not await guard(msg):
            return
        from .intelligence import build_stats_report
        await _reply(msg, build_stats_report())

    @dp.message.register(Command("roi"))
    async def cmd_roi(msg):
        if not await guard(msg):
            return
        parts = (msg.text or "").split()
        if len(parts) < 4:
            await _reply(msg, "שימוש: `/roi <TON\\-spent> <conversions> <value\\-per\\-conversion>`")
            return
        from .intelligence import calculate_roi
        try:
            await _reply(msg, calculate_roi(float(parts[1]), int(parts[2]), float(parts[3])))
        except ValueError:
            await _reply(msg, "❌ ערכים לא חוקיים")

    @dp.message.register(Command("hostile_scan"))
    async def cmd_hostile(msg):
        if not await guard(msg):
            return
        parts = (msg.text or "").split(maxsplit=1)
        if len(parts) < 2:
            await _reply(msg, "שימוש: `/hostile\\_scan <group\\_id>`")
            return
        await msg.answer("⏳ סורק קבוצה…")
        from .intelligence import detect_hostile_bots, format_hostile
        sess = _first_session()
        try:
            gid = int(parts[1])
        except ValueError:
            gid = parts[1]
        r = await detect_hostile_bots(gid, sess, aid, ahash)
        await _reply(msg, format_hostile(r))

    @dp.message.register(Command("analyze"))
    async def cmd_analyze(msg):
        if not await guard(msg):
            return
        parts = (msg.text or "").split()
        # /analyze <chat_id> [leads|tasks|sentiment|summary]
        if len(parts) < 2:
            await _reply(msg, "שימוש: `/analyze <chat\\_id> [leads|tasks|sentiment|summary]`")
            return
        await msg.answer("⏳ מנתח ארכיב…")
        from .intelligence import analyze_archive
        chat_id = int(parts[1])
        mode    = parts[2] if len(parts) > 2 else "summary"
        base    = os.environ.get("NEXUS_API_BASE", "http://localhost:8000")
        key     = os.environ.get("NEXUS_API_KEY", "")
        result  = await analyze_archive(chat_id, mode, base, key)
        await _reply(msg, f"🧠 *ניתוח AI — {_esc(mode)}*\n\n{_esc(str(result)[:3000])}")

    # ── Branding / Media (tools 13–18) ────────────────────────────────────────

    @dp.message.register(Command("emoji_gen"))
    async def cmd_emoji_gen(msg):
        if not await guard(msg):
            return
        # Expects a photo or a label arg; photo upload handled via separate media handler
        parts = (msg.text or "").split(maxsplit=1)
        label = parts[1].strip() if len(parts) > 1 else "brand"
        # Check for replied-to photo
        reply = msg.reply_to_message
        if not (reply and reply.photo):
            await _reply(msg,
                "📎 שלח תמונה עם `/emoji\\_gen <שם>` או ענה על תמונה קיימת"
            )
            return
        await msg.answer("⏳ מייצר ספריית אימוג'י…")
        from .media_tools import generate_emoji_set, format_emoji_gen
        photo   = reply.photo[-1]
        finfo   = await bot.get_file(photo.file_id)
        fbytes  = await bot.download_file(finfo.file_path)
        r       = await generate_emoji_set(fbytes.read(), label=label)
        await _reply(msg, format_emoji_gen(r))

    @dp.message.register(Command("sticker"))
    async def cmd_sticker(msg):
        if not await guard(msg):
            return
        reply = msg.reply_to_message
        if not (reply and reply.video):
            await _reply(msg, "📎 ענה על סרטון עם `/sticker` להמרה ל\\-WebM")
            return
        await msg.answer("⏳ ממיר ל\\-WebM…")
        import tempfile
        from .media_tools import convert_to_webm_sticker, format_sticker
        v     = reply.video
        finfo = await bot.get_file(v.file_id)
        fbytes = await bot.download_file(finfo.file_path)
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
            tmp.write(fbytes.read())
            inp_path = tmp.name
        r = await convert_to_webm_sticker(inp_path)
        await _reply(msg, format_sticker(r))
        if not r.get("error") and Path(r["output"]).exists():
            await bot.send_document(msg.chat.id, open(r["output"], "rb"),
                                    caption="🎬 WebM Sticker")

    @dp.message.register(Command("watermark"))
    async def cmd_watermark(msg):
        if not await guard(msg):
            return
        parts = (msg.text or "").split(maxsplit=1)
        text  = parts[1].strip() if len(parts) > 1 else "© Nexus"
        reply = msg.reply_to_message
        if not (reply and reply.photo):
            await _reply(msg, "📎 ענה על תמונה עם `/watermark <טקסט>`")
            return
        from .media_tools import apply_watermark
        import io as _io
        photo  = reply.photo[-1]
        finfo  = await bot.get_file(photo.file_id)
        fbytes = await bot.download_file(finfo.file_path)
        result = apply_watermark(fbytes.read(), text=text)
        await bot.send_photo(msg.chat.id, _io.BytesIO(result),
                             caption=f"🖋 ווטרמרק: {text}")

    @dp.message.register(Command("compress"))
    async def cmd_compress(msg):
        if not await guard(msg):
            return
        parts = (msg.text or "").split()
        target_kb = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 500
        reply = msg.reply_to_message
        if not reply or not (reply.photo or reply.video or reply.document):
            await _reply(msg, "📎 ענה על מדיה עם `/compress [target\\_kb]`")
            return
        await msg.answer(f"⏳ מכווץ ל\\-{target_kb} KB…", parse_mode="MarkdownV2")
        import tempfile, io as _io
        from .media_tools import compress_media, format_compress

        # Determine file
        if reply.photo:
            obj = reply.photo[-1]
            ext = ".jpg"
        elif reply.video:
            obj = reply.video
            ext = ".mp4"
        else:
            obj = reply.document
            ext = Path(reply.document.file_name or "file.bin").suffix

        finfo  = await bot.get_file(obj.file_id)
        fbytes = await bot.download_file(finfo.file_path)
        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
            tmp.write(fbytes.read())
            inp_path = tmp.name

        r = await compress_media(inp_path, target_kb=target_kb)
        await _reply(msg, format_compress(r))
        if not r.get("error") and Path(r["output"]).exists():
            await bot.send_document(msg.chat.id, open(r["output"], "rb"),
                                    caption="🗜 קובץ מכווץ")

    @dp.message.register(Command("grid"))
    async def cmd_grid(msg):
        if not await guard(msg):
            return
        parts = (msg.text or "").split(maxsplit=1)
        label = parts[1].strip() if len(parts) > 1 else "grid"
        reply = msg.reply_to_message
        if not (reply and reply.photo):
            await _reply(msg, "📎 ענה על תמונה עם `/grid <שם>`")
            return
        await msg.answer("⏳ מפצל לגריד 3×3…")
        from .media_tools import split_instagram_grid, format_grid
        photo  = reply.photo[-1]
        finfo  = await bot.get_file(photo.file_id)
        fbytes = await bot.download_file(finfo.file_path)
        r = split_instagram_grid(fbytes.read(), label=label)
        await _reply(msg, format_grid(r))
        # Send the 9 tiles
        import io as _io
        for tile_path in r.get("post_order", []):
            p = Path(tile_path)
            if p.exists():
                await bot.send_photo(msg.chat.id, open(p, "rb"), caption=p.name)

    # ── Marketing (tools 19–27) ───────────────────────────────────────────────

    @dp.message.register(Command("broadcast"))
    async def cmd_broadcast(msg):
        if not await guard(msg):
            return
        # /broadcast <message text>  — sends to all users in DB
        parts = (msg.text or "").split(maxsplit=1)
        if len(parts) < 2:
            await _reply(msg, "שימוש: `/broadcast <הודעה>`")
            return
        text = parts[1].strip()
        await msg.answer("⏳ שולח…")
        from .marketing import mass_broadcast, format_broadcast
        from ..db.models import ScrapedUser, get_session
        sess  = get_session()
        uids  = [u.telegram_id for u in sess.query(ScrapedUser).all() if u.telegram_id]
        sess.close()
        if not uids:
            await _reply(msg, "❌ אין משתמשים בבסיס הנתונים")
            return
        r = await mass_broadcast(uids, text, _all_sessions(), aid, ahash)
        await _reply(msg, format_broadcast(r))

    @dp.message.register(Command("export"))
    async def cmd_export(msg):
        if not await guard(msg):
            return
        parts = (msg.text or "").split(maxsplit=1)
        fmt   = (parts[1].strip().lower() if len(parts) > 1 else "csv")
        from .marketing import export_panel_links
        import io as _io
        data = export_panel_links(fmt=fmt)
        ext  = fmt if fmt in {"csv", "json"} else "csv"
        await bot.send_document(
            msg.chat.id,
            (_io.BytesIO(data), f"nexus_links.{ext}"),
            caption=f"📊 ייצוא קישורים ({ext.upper()})",
        )

    @dp.message.register(Command("warmup"))
    async def cmd_warmup(msg):
        if not await guard(msg):
            return
        parts   = (msg.text or "").split()
        actions = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 20
        sess    = _first_session()
        if not sess:
            await _reply(msg, "❌ אין סשן זמין")
            return
        await msg.answer("⏳ מחמם חשבון…")
        from .marketing import warmup_account, format_warmup
        r = await warmup_account(sess, aid, ahash, actions=actions)
        await _reply(msg, format_warmup(r))

    @dp.message.register(Command("menu_update"))
    async def cmd_menu_update(msg):
        if not await guard(msg):
            return
        # /menu_update <bot_token>
        # Commands are passed as following lines or hardcoded
        parts = (msg.text or "").split(maxsplit=1)
        if len(parts) < 2:
            await _reply(msg, "שימוש: `/menu\\_update <bot\\_token>`")
            return
        token = parts[1].strip()
        commands = [
            {"command": "start",   "description": "הפעל את הבוט"},
            {"command": "help",    "description": "עזרה ותמיכה"},
            {"command": "stats",   "description": "סטטיסטיקות"},
            {"command": "referral","description": "קישור שיתוף"},
        ]
        from .marketing import update_bot_menu, format_menu_update
        r = await update_bot_menu(token, commands)
        await _reply(msg, format_menu_update(r))

    @dp.message.register(Command("fragment"))
    async def cmd_fragment(msg):
        if not await guard(msg):
            return
        parts = (msg.text or "").split(maxsplit=1)
        if len(parts) < 2:
            await _reply(msg, "שימוש: `/fragment @username`")
            return
        await msg.answer("⏳ בודק ב\\-Fragment…", parse_mode="MarkdownV2")
        from .marketing import check_fragment_username, format_fragment
        r = await check_fragment_username(parts[1])
        await _reply(msg, format_fragment(r))

    @dp.message.register(Command("ab_test"))
    async def cmd_ab_test(msg):
        if not await guard(msg):
            return
        # /ab_test <variant_a> | <variant_b>
        body = (msg.text or "").split(maxsplit=1)
        if len(body) < 2 or "|" not in body[1]:
            await _reply(msg, "שימוש: `/ab\\_test <variant A> | <variant B>`")
            return
        va, vb = body[1].split("|", 1)
        await msg.answer("⏳ מריץ A/B test…")
        from .marketing import run_ab_test, format_ab_test
        from ..db.models import ScrapedUser, get_session
        sess = get_session()
        uids = [u.telegram_id for u in sess.query(ScrapedUser).all() if u.telegram_id]
        sess.close()
        if not uids:
            await _reply(msg, "❌ אין משתמשים")
            return
        r = await run_ab_test(uids[:200], va.strip(), vb.strip(), _all_sessions(), aid, ahash)
        await _reply(msg, format_ab_test(r))

    @dp.message.register(Command("schedule"))
    async def cmd_schedule(msg):
        if not await guard(msg):
            return
        # /schedule <ISO-datetime> <message>
        parts = (msg.text or "").split(maxsplit=2)
        if len(parts) < 3:
            await _reply(msg, "שימוש: `/schedule 2025\\-12\\-31T18:00:00 <הודעה>`")
            return
        from .marketing import schedule_broadcast, format_schedule
        from ..db.models import ScrapedUser, get_session
        sess = get_session()
        uids = [u.telegram_id for u in sess.query(ScrapedUser).all() if u.telegram_id]
        sess.close()
        r = schedule_broadcast(uids, parts[2], parts[1], _all_sessions(), aid, ahash)
        await _reply(msg, format_schedule(r))

    @dp.message.register(Command("schedule_list"))
    async def cmd_schedule_list(msg):
        if not await guard(msg):
            return
        from .marketing import list_scheduled_jobs
        await _reply(msg, list_scheduled_jobs())

    @dp.message.register(Command("cleanup"))
    async def cmd_cleanup(msg):
        if not await guard(msg):
            return
        # /cleanup <group_id> [days] [execute]
        parts = (msg.text or "").split()
        if len(parts) < 2:
            await _reply(msg, "שימוש: `/cleanup <group\\_id> [days] [execute]`")
            return
        gid      = parts[1]
        days     = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 30
        dry_run  = not (len(parts) > 3 and parts[3].lower() == "execute")
        await msg.answer("⏳ סורק…")
        from .marketing import cleanup_inactive_users, format_cleanup
        sess = _first_session()
        try:
            gid = int(gid)
        except ValueError:
            pass
        r = await cleanup_inactive_users(gid, sess, aid, ahash,
                                         inactive_days=days, dry_run=dry_run)
        await _reply(msg, format_cleanup(r))

    log.info("nexus_tools_registered", commands=27)
