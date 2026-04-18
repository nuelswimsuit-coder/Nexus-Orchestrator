"""
Intelligence & AI Tools
Covers tools 7–12 of the Intelligence category:
  /seo @username    — global search visibility + shadowban check via observer sessions
  /trends           — scan a competing channel and surface hot topics
  /stats            — daily growth report for managed bots/channels
  /roi              — campaign ROI calculator (TON cost → conversion value)
  /hostile_scan     — detect competitor bots scraping your groups
  /analyze [chat_id]— AI Historian: Claude analysis of archived chat (leads, tasks, sentiment)
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

log = structlog.get_logger(__name__)
ROOT = Path(__file__).resolve().parents[3]


# ── SEO / Search Visibility ───────────────────────────────────────────────────

async def check_seo_visibility(username: str, observer_session: str,
                                api_id: int, api_hash: str) -> dict:
    """
    Use an observer Telethon session to search for `username` in the global
    contact search and report whether it appears (visible) or is shadowbanned.
    """
    username = username.lstrip("@").strip()
    result   = {
        "username":  username,
        "visible":   None,
        "rank":      None,
        "error":     None,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        from telethon import TelegramClient
        from telethon.tl.functions.contacts import SearchRequest

        client = TelegramClient(
            observer_session.replace(".session", ""), api_id, api_hash
        )
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            result["error"] = "Observer session לא מורשה"
            return result

        search_result = await client(SearchRequest(q=username, limit=20))
        await client.disconnect()

        found = False
        rank  = None
        for i, u in enumerate(search_result.users, 1):
            uname = getattr(u, "username", "") or ""
            if uname.lower() == username.lower():
                found = True
                rank  = i
                break

        result["visible"] = found
        result["rank"]    = rank
    except Exception as exc:
        result["error"] = str(exc)[:100]
    return result


def format_seo(r: dict) -> str:
    user = r.get("username", "?")
    err  = r.get("error")
    if err:
        return f"❌ *SEO Check — שגיאה*\n`@{user}`\n_{err}_"

    visible = r.get("visible")
    rank    = r.get("rank")
    if visible:
        icon = "✅"
        status = f"מופיע בחיפוש גלובלי — דירוג #{rank}"
    else:
        icon = "🚫"
        status = "לא מופיע בחיפוש — ייתכן Shadowban"

    return (
        f"🔍 *SEO Visibility — @{user}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{icon} {status}\n"
        f"⏱ בדיקה: {r.get('checked_at','')[:16]}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"_בדיקה בוצעה ע\"י סשן Observer_"
    )


# ── Trend Finder ──────────────────────────────────────────────────────────────

async def scan_channel_trends(channel_id: int | str, session_path: str,
                               api_id: int, api_hash: str,
                               limit: int = 200) -> dict:
    """
    Read the last `limit` messages from a channel and return top keywords/topics.
    """
    from collections import Counter
    import re

    words: list[str] = []
    result = {"channel_id": channel_id, "total_msgs": 0, "top_words": [], "error": None}

    try:
        from telethon import TelegramClient

        client = TelegramClient(
            session_path.replace(".session", ""), api_id, api_hash
        )
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            result["error"] = "Session לא מורשה"
            return result

        entity = await client.get_entity(int(channel_id) if str(channel_id).lstrip("-").isdigit() else channel_id)
        msgs   = []
        async for msg in client.iter_messages(entity, limit=limit):
            if msg.text:
                msgs.append(msg.text)
        await client.disconnect()

        result["total_msgs"] = len(msgs)

        # Simple word frequency (Hebrew + English words > 3 chars)
        stop = {"של", "הם", "הן", "זה", "זו", "כי", "אם", "לא",
                "from", "with", "this", "that", "have", "will", "your", "the"}
        for text in msgs:
            for w in re.findall(r"[\u0590-\u05ff\w]{4,}", text.lower()):
                if w not in stop:
                    words.append(w)

        top = Counter(words).most_common(15)
        result["top_words"] = top

    except Exception as exc:
        result["error"] = str(exc)[:100]

    return result


def format_trends(r: dict) -> str:
    err = r.get("error")
    if err:
        return f"❌ *Trend Finder — שגיאה*\n_{err}_"

    top    = r.get("top_words", [])
    total  = r.get("total_msgs", 0)
    lines  = [
        f"📈 *Trend Finder — ערוץ {r.get('channel_id')}*",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"נסרקו: {total} הודעות",
        "",
        "*🔥 מילים חמות:*",
    ]
    for i, (word, cnt) in enumerate(top, 1):
        bar = "█" * min(cnt // 2, 12)
        lines.append(f"{i:2}. `{word:<20}` {bar} ({cnt})")
    return "\n".join(lines)


# ── Stats Report ──────────────────────────────────────────────────────────────

def build_stats_report(db_path: str = "data/nexus_supreme.db") -> str:
    """Pull bot /start stats and enrollment counts from the unified DB."""
    try:
        from ..db.models import BotStartEvent, ManagedBot, ScrapedUser, get_session
        from sqlalchemy import func
        from datetime import timedelta

        sess = get_session(db_path)

        # Total bots
        total_bots  = sess.query(ManagedBot).count()
        active_bots = sess.query(ManagedBot).filter_by(is_active=True).count()

        # /start events last 24 h
        since_24h = datetime.now(timezone.utc) - timedelta(hours=24)
        starts_24h = sess.query(BotStartEvent).filter(
            BotStartEvent.timestamp >= since_24h
        ).count()
        new_24h = sess.query(BotStartEvent).filter(
            BotStartEvent.timestamp >= since_24h,
            BotStartEvent.is_new_user.is_(True),
        ).count()

        # Total scraped users
        total_users = sess.query(ScrapedUser).count()
        premium_u   = sess.query(ScrapedUser).filter(ScrapedUser.is_premium == 1).count()
        prem_pct    = round(premium_u * 100 / total_users, 1) if total_users else 0

        # Top bot by starts
        top = (
            sess.query(ManagedBot.name, ManagedBot.start_count)
            .order_by(ManagedBot.start_count.desc())
            .limit(3)
            .all()
        )
        sess.close()

        top_lines = "\n".join(
            f"  {i+1}. {n} — {c:,} starts" for i, (n, c) in enumerate(top)
        )

        return (
            f"📊 *דוח סטטיסטיקה — Nexus Supreme*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🤖 בוטים: {active_bots}/{total_bots} פעילים\n"
            f"⚡ /start ב-24h: *{starts_24h:,}*  ({new_24h} חדשים)\n"
            f"👥 משתמשים שנאספו: *{total_users:,}*\n"
            f"💎 פרימיום: *{prem_pct}%*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"*🏆 Top 3 בוטים:*\n{top_lines or '—'}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"_עדכן: {datetime.now().strftime('%d/%m %H:%M')}_"
        )
    except Exception as exc:
        return f"❌ שגיאה בהפקת הדוח: {exc}"


# ── ROI Calculator ────────────────────────────────────────────────────────────

def calculate_roi(ton_spent: float, conversions: int,
                  value_per_conversion: float) -> str:
    """
    Simple ROI: revenue = conversions × value_per_conversion.
    TON price pulled from CoinGecko if available; else uses $2.5 fallback.
    """
    # Try to get live TON price
    ton_usd = 2.5
    try:
        import requests
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=the-open-network&vs_currencies=usd",
            timeout=4,
        )
        ton_usd = r.json()["the-open-network"]["usd"]
    except Exception:
        pass

    cost_usd    = ton_spent * ton_usd
    revenue_usd = conversions * value_per_conversion
    profit_usd  = revenue_usd - cost_usd
    roi_pct     = (profit_usd / cost_usd * 100) if cost_usd > 0 else 0
    icon        = "✅" if roi_pct > 0 else "❌"

    return (
        f"💰 *ROI Calculator*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💎 עלות: {ton_spent} TON ≈ ${cost_usd:.2f}\n"
        f"📈 המרות: {conversions} × ${value_per_conversion} = ${revenue_usd:.2f}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{icon} *רווח: ${profit_usd:.2f}*\n"
        f"📊 ROI: *{roi_pct:.1f}%*\n"
        f"💱 TON/USD: ${ton_usd:.3f}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"_שימוש: /roi <ton> <המרות> <ערך\\_להמרה>_"
    )


# ── Hostile Bot Detector ──────────────────────────────────────────────────────

async def detect_hostile_bots(group_id: int, session_path: str,
                               api_id: int, api_hash: str) -> dict:
    """
    Scan a group's member list for bots (is_bot=True) that are NOT owned by us.
    Reports suspicious bots that might be harvesting leads.
    """
    result = {"group_id": group_id, "our_bots": [], "hostile": [], "error": None}
    try:
        from telethon import TelegramClient
        from telethon.tl.functions.channels import GetParticipantsRequest
        from telethon.tl.types import ChannelParticipantsSearch

        client = TelegramClient(
            session_path.replace(".session", ""), api_id, api_hash
        )
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            result["error"] = "Session לא מורשה"
            return result

        # Load our known bots from DB
        known_usernames: set[str] = set()
        try:
            from ..db.models import ManagedBot, get_session as _gs
            s = _gs()
            for b in s.query(ManagedBot).filter(ManagedBot.username.isnot(None)).all():
                if b.username:
                    known_usernames.add(b.username.lower().lstrip("@"))
            s.close()
        except Exception:
            pass

        entity = await client.get_entity(group_id)
        participants = await client(GetParticipantsRequest(
            entity, ChannelParticipantsSearch(""), offset=0, limit=200, hash=0
        ))
        await client.disconnect()

        for user in participants.users:
            if not getattr(user, "bot", False):
                continue
            uname = (getattr(user, "username", "") or "").lower()
            if uname in known_usernames:
                result["our_bots"].append(uname)
            else:
                result["hostile"].append({
                    "username": uname or str(user.id),
                    "id":       user.id,
                    "name":     getattr(user, "first_name", ""),
                })

    except Exception as exc:
        result["error"] = str(exc)[:100]
    return result


def format_hostile(r: dict) -> str:
    err = r.get("error")
    if err:
        return f"❌ *Hostile Bot Scan — שגיאה*\n_{err}_"

    hostile = r.get("hostile", [])
    ours    = r.get("our_bots", [])

    lines = [
        f"🕵️ *Hostile Bot Detector — קבוצה {r['group_id']}*",
        f"━━━━━━━━━━━━━━━━━━━━━━━━",
        f"🤖 הבוטים שלנו: {len(ours)}",
        f"🚨 בוטים זרים: {len(hostile)}",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    if hostile:
        lines.append("⚠️ *בוטים חשודים שגרים בקבוצה:*")
        for b in hostile[:10]:
            lines.append(f"  • @{b['username']} (ID: {b['id']}) — {b['name']}")
    else:
        lines.append("✅ לא נמצאו בוטים זרים חשודים")
    return "\n".join(lines)


# ── AI Historian ──────────────────────────────────────────────────────────────

async def analyze_archive(chat_id: int, mode: str,
                           nexus_api_base: str, nexus_api_key: str) -> str:
    """
    Read from the local archive DB and send to the Nexus /api/analyze endpoint.
    Modes: leads | tasks | sentiment | summary
    """
    try:
        from ..db.models import ArchivedChat, ArchivedMessage, get_session
        sess = get_session()

        chat = sess.query(ArchivedChat).filter_by(chat_id=chat_id).first()
        if not chat:
            sess.close()
            return f"❌ לא נמצא ארכיב לצ'אט {chat_id}\\. הרץ ארכיון תחילה\\."

        msgs = (
            sess.query(ArchivedMessage)
            .filter_by(chat_id=chat_id)
            .order_by(ArchivedMessage.timestamp.desc())
            .limit(300)
            .all()
        )
        sess.close()

        texts = [m.text for m in msgs if m.text and len(m.text) > 5]
        if not texts:
            return "❌ הארכיב ריק\\."

        sample = "\n".join(texts[:200])
        prompts = {
            "leads":     "Identify all sales leads, contacts, or business opportunities. Reply in Hebrew.",
            "tasks":     "List all open action items and tasks mentioned. Reply in Hebrew as a numbered list.",
            "sentiment": "Analyze the overall communication sentiment and tone. Reply in Hebrew with examples.",
            "summary":   "Write an executive summary of the main topics discussed. Reply in Hebrew.",
        }
        instruction = prompts.get(mode, prompts["summary"])

        import httpx
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                f"{nexus_api_base}/api/analyze",
                json    = {"text": sample[:12_000], "instruction": instruction},
                headers = {"X-Nexus-Api-Key": nexus_api_key},
            )
            data = resp.json()
            return data.get("analysis", resp.text)[:3500]

    except Exception as exc:
        return f"❌ שגיאה בניתוח: {str(exc)[:200]}"
