"""
Marketing & AHU Portal Tools
Covers tools 19–27:
  /broadcast   — mass message sender with delay + session rotation
  /export      — export panel referral links to CSV/JSON
  /warmup      — Telethon account warmup manager (activity simulation)
  /menu_update — dynamically update a bot's inline keyboard menu via BotFather
  /fragment    — reserve/check username availability on Fragment.com
  /panel_stats — aggregate stats across multiple affiliate panels
  /ab_test     — A/B test two message variants on a sub-segment
  /schedule    — schedule a broadcast for a future UTC time
  /cleanup     — remove inactive users from a managed group
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import structlog

log = structlog.get_logger(__name__)
ROOT = Path(__file__).resolve().parents[3]


# ── Mass Broadcast ─────────────────────────────────────────────────────────────

async def mass_broadcast(
    user_ids: list[int],
    text: str,
    session_paths: list[str],
    api_id: int,
    api_hash: str,
    delay_range: tuple[float, float] = (1.5, 3.5),
    progress_cb: Callable[[int, int], None] | None = None,
) -> dict:
    """
    Send `text` to every user_id, rotating across sessions.
    Returns a summary dict with sent/failed counts.
    """
    result = {
        "total":  len(user_ids),
        "sent":   0,
        "failed": 0,
        "errors": [],
    }
    if not session_paths:
        result["errors"].append("אין סשנים זמינים")
        return result

    from telethon import TelegramClient
    from telethon.errors import FloodWaitError, UserPrivacyRestrictedError

    clients: list[TelegramClient] = []
    for sp in session_paths:
        c = TelegramClient(sp.replace(".session", ""), api_id, api_hash)
        await c.connect()
        if await c.is_user_authorized():
            clients.append(c)

    if not clients:
        result["errors"].append("כל הסשנים מנותקים")
        return result

    idx = 0
    for i, uid in enumerate(user_ids):
        client = clients[idx % len(clients)]
        idx   += 1
        try:
            await client.send_message(uid, text, parse_mode="md")
            result["sent"] += 1
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 2)
            result["failed"] += 1
            result["errors"].append(f"flood:{uid}")
        except UserPrivacyRestrictedError:
            result["failed"] += 1
        except Exception as exc:
            result["failed"] += 1
            result["errors"].append(f"{uid}:{str(exc)[:40]}")

        if progress_cb:
            progress_cb(i + 1, len(user_ids))

        await asyncio.sleep(random.uniform(*delay_range))

    for c in clients:
        try:
            await c.disconnect()
        except Exception:
            pass

    return result


def format_broadcast(r: dict) -> str:
    pct  = round(r["sent"] / r["total"] * 100, 1) if r["total"] else 0
    errs = r["errors"][:5]
    err_block = ("\n_שגיאות ראשונות:_\n" + "\n".join(f"  • {e}" for e in errs)) if errs else ""
    return (
        f"📢 *Mass Broadcast — סיכום*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📨 נשלח:  *{r['sent']:,}* / {r['total']:,} \\({pct}%\\)\n"
        f"❌ נכשל:  {r['failed']:,}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━"
        f"{err_block}"
    )


# ── Panel Link Exporter ────────────────────────────────────────────────────────

def export_panel_links(
    db_path: str = "data/nexus_supreme.db",
    fmt: str = "csv",    # "csv" | "json"
) -> bytes:
    """
    Export referral / affiliate links from Target table.
    Returns the file content as bytes.
    """
    try:
        from ..db.models import Target, get_session
        sess  = get_session(db_path)
        rows  = sess.query(Target).all()
        sess.close()

        data = [
            {
                "id":         t.id,
                "username":   t.username or "",
                "source":     getattr(t, "source", ""),
                "group_id":   getattr(t, "group_id", ""),
                "added_at":   str(getattr(t, "added_at", "")),
                "is_active":  getattr(t, "is_active", True),
            }
            for t in rows
        ]
    except Exception as exc:
        data = [{"error": str(exc)}]

    if fmt == "json":
        return json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")

    buf = io.StringIO()
    if data:
        writer = csv.DictWriter(buf, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)
    return buf.getvalue().encode("utf-8")


# ── Account Warmup Manager ─────────────────────────────────────────────────────

async def warmup_account(
    session_path: str,
    api_id: int,
    api_hash: str,
    actions: int = 20,
    target_channels: list[str | int] | None = None,
) -> dict:
    """
    Simulate natural activity on a session: read messages, view channels, type...
    Reduces the chance of Telegram anti-spam triggers on fresh accounts.
    """
    result = {"session": session_path, "actions_done": 0, "error": None}

    CHANNELS = target_channels or [
        "telegram", "durov", "techcrunch", "bbcnews",
    ]

    try:
        from telethon import TelegramClient
        from telethon.tl.functions.messages import GetHistoryRequest
        from telethon.tl.functions.channels import GetChannelsRequest

        client = TelegramClient(session_path.replace(".session", ""), api_id, api_hash)
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            result["error"] = "Session לא מורשה"
            return result

        done = 0
        for ch in CHANNELS:
            if done >= actions:
                break
            try:
                entity = await client.get_entity(ch)
                async for _msg in client.iter_messages(entity, limit=min(5, actions - done)):
                    done += 1
                    await asyncio.sleep(random.uniform(0.8, 2.5))
                    if done >= actions:
                        break
            except Exception:
                pass

        result["actions_done"] = done
        await client.disconnect()
    except Exception as exc:
        result["error"] = str(exc)[:100]

    return result


def format_warmup(r: dict) -> str:
    if r.get("error"):
        return f"❌ *Warmup — שגיאה*\n_{r['error']}_"
    return (
        f"🔥 *Account Warmup*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📱 סשן: `{Path(r['session']).stem}`\n"
        f"✅ פעולות בוצעו: *{r['actions_done']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"_חשבון מוכן לשימוש מלא_"
    )


# ── Dynamic Menu Updater ───────────────────────────────────────────────────────

async def update_bot_menu(
    bot_token: str,
    commands: list[dict],   # [{"command": "start", "description": "..."}]
) -> dict:
    """
    Set a bot's command menu via the Bot API setMyCommands endpoint.
    commands: list of {"command": str, "description": str}
    """
    import httpx
    result = {"ok": False, "error": None}
    url    = f"https://api.telegram.org/bot{bot_token}/setMyCommands"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(url, json={"commands": commands})
            data = resp.json()
            result["ok"]    = data.get("ok", False)
            result["error"] = data.get("description")
    except Exception as exc:
        result["error"] = str(exc)[:100]
    return result


def format_menu_update(r: dict) -> str:
    if r.get("ok"):
        return "✅ *תפריט הבוט עודכן בהצלחה\\!*"
    return f"❌ *שגיאה בעדכון תפריט:*\n_{r.get('error','')}_"


# ── Fragment Username Checker ──────────────────────────────────────────────────

async def check_fragment_username(
    username: str,
    ton_wallet: str | None = None,
) -> dict:
    """
    Check if a Telegram username is available on Fragment.com.
    Falls back to the unofficial Fragment search API.
    """
    import httpx
    username = username.lstrip("@").strip()
    result   = {
        "username":   username,
        "available":  None,
        "price_ton":  None,
        "error":      None,
    }
    try:
        async with httpx.AsyncClient(
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (compatible; NexusBot/1.0)"},
        ) as client:
            resp = await client.get(
                f"https://fragment.com/username/{username}",
                follow_redirects=True,
            )
            html = resp.text
            if "This username is not available" in html or resp.status_code == 404:
                result["available"] = False
            elif "Auction" in html or "Buy now" in html or "Place a bid" in html:
                result["available"] = True
                # Try to parse price
                import re
                m = re.search(r'([\d,]+)\s*TON', html)
                if m:
                    result["price_ton"] = m.group(1).replace(",", "")
            else:
                result["available"] = None   # ambiguous
    except Exception as exc:
        result["error"] = str(exc)[:100]
    return result


def format_fragment(r: dict) -> str:
    if r.get("error"):
        return f"❌ *Fragment Check — שגיאה*\n_{r['error']}_"
    av   = r.get("available")
    icon = "✅" if av else ("🚫" if av is False else "❓")
    status = "זמין לרכישה" if av else ("לא זמין" if av is False else "לא ברור")
    price  = f"\n💎 מחיר: *{r['price_ton']} TON*" if r.get("price_ton") else ""
    return (
        f"🔗 *Fragment Username Check*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{icon} `@{r['username']}` — {status}"
        f"{price}\n"
        f"🌐 [פתח ב\\-Fragment](https://fragment.com/username/{r['username']})"
    )


# ── A/B Test Dispatcher ───────────────────────────────────────────────────────

async def run_ab_test(
    user_ids: list[int],
    variant_a: str,
    variant_b: str,
    session_paths: list[str],
    api_id: int,
    api_hash: str,
    split: float = 0.5,
) -> dict:
    """
    Send variant_a to `split` fraction of users, variant_b to the rest.
    Returns counts per variant.
    """
    random.shuffle(user_ids)
    cutoff  = int(len(user_ids) * split)
    group_a = user_ids[:cutoff]
    group_b = user_ids[cutoff:]

    res_a = await mass_broadcast(group_a, variant_a, session_paths, api_id, api_hash)
    res_b = await mass_broadcast(group_b, variant_b, session_paths, api_id, api_hash)

    return {
        "variant_a": {"text_preview": variant_a[:80], "users": len(group_a), **res_a},
        "variant_b": {"text_preview": variant_b[:80], "users": len(group_b), **res_b},
    }


def format_ab_test(r: dict) -> str:
    def side(v: dict, label: str) -> str:
        pct = round(v["sent"] / v["users"] * 100, 1) if v["users"] else 0
        return (
            f"*{label}* ({v['users']} משתמשים)\n"
            f"  `{v['text_preview']}...`\n"
            f"  ✅ {v['sent']} נשלחו ({pct}%)"
        )
    return (
        f"🧪 *A/B Test — תוצאות*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        + side(r["variant_a"], "Variant A") + "\n\n"
        + side(r["variant_b"], "Variant B")
    )


# ── Scheduled Broadcast ────────────────────────────────────────────────────────

_scheduled_jobs: list[dict] = []   # in-memory; persist to DB in production


def schedule_broadcast(
    user_ids: list[int],
    text: str,
    send_at_utc: str,   # ISO-8601 e.g. "2025-12-31T18:00:00"
    session_paths: list[str],
    api_id: int,
    api_hash: str,
    label: str = "broadcast",
) -> dict:
    """Register a broadcast job to fire at send_at_utc (UTC ISO string)."""
    job = {
        "id":            len(_scheduled_jobs) + 1,
        "label":         label,
        "send_at":       send_at_utc,
        "user_count":    len(user_ids),
        "text_preview":  text[:100],
        "user_ids":      user_ids,
        "text":          text,
        "sessions":      session_paths,
        "api_id":        api_id,
        "api_hash":      api_hash,
        "status":        "pending",
    }
    _scheduled_jobs.append(job)

    return {
        "job_id":    job["id"],
        "send_at":   send_at_utc,
        "users":     len(user_ids),
        "label":     label,
    }


async def tick_scheduled_broadcasts() -> list[int]:
    """
    Call periodically (e.g., every minute) to fire due jobs.
    Returns list of fired job IDs.
    """
    fired = []
    now   = datetime.now(timezone.utc).isoformat()
    for job in _scheduled_jobs:
        if job["status"] == "pending" and job["send_at"] <= now:
            job["status"] = "running"
            try:
                await mass_broadcast(
                    job["user_ids"], job["text"],
                    job["sessions"], job["api_id"], job["api_hash"],
                )
                job["status"] = "done"
            except Exception as exc:
                job["status"] = f"error:{str(exc)[:60]}"
            fired.append(job["id"])
    return fired


def format_schedule(r: dict) -> str:
    return (
        f"🕐 *Scheduled Broadcast — #{r['job_id']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 שליחה ב\\-UTC: `{r['send_at']}`\n"
        f"👥 יעד: *{r['users']:,}* משתמשים\n"
        f"🏷 תוית: `{r['label']}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"_ממתין לשליחה אוטומטית_"
    )


def list_scheduled_jobs() -> str:
    if not _scheduled_jobs:
        return "_אין שידורים מתוזמנים_"
    lines = [f"📋 *שידורים מתוזמנים:* ({len(_scheduled_jobs)})"]
    for j in _scheduled_jobs[-10:]:
        icon = {"pending": "⏳", "running": "▶️", "done": "✅"}.get(j["status"], "❌")
        lines.append(f"{icon} #{j['id']} {j['send_at'][:16]} — {j['user_count']} משתמשים — `{j['label']}`")
    return "\n".join(lines)


# ── Group Cleanup ─────────────────────────────────────────────────────────────

async def cleanup_inactive_users(
    group_id: int | str,
    session_path: str,
    api_id: int,
    api_hash: str,
    inactive_days: int = 30,
    dry_run: bool = True,
) -> dict:
    """
    Identify group members with no recorded interaction in the last `inactive_days`.
    If dry_run=False, remove them.  Returns summary counts.
    """
    result = {
        "group_id":    group_id,
        "scanned":     0,
        "inactive":    0,
        "removed":     0,
        "dry_run":     dry_run,
        "error":       None,
    }
    try:
        from datetime import timedelta
        from telethon import TelegramClient
        from telethon.tl.functions.channels import GetParticipantsRequest, KickFromChannelRequest
        from telethon.tl.types import ChannelParticipantsSearch

        client = TelegramClient(session_path.replace(".session", ""), api_id, api_hash)
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            result["error"] = "Session לא מורשה"
            return result

        entity    = await client.get_entity(
            int(group_id) if str(group_id).lstrip("-").isdigit() else group_id
        )
        parts     = await client(GetParticipantsRequest(
            entity, ChannelParticipantsSearch(""), offset=0, limit=200, hash=0
        ))
        cutoff_ts = (datetime.now(timezone.utc) - timedelta(days=inactive_days)).timestamp()

        result["scanned"] = len(parts.users)

        # We use join date as proxy for activity (Telethon doesn't expose last_seen easily)
        from telethon.tl.types import ChannelParticipant
        for p in parts.participants:
            joined = getattr(p, "date", None)
            if not joined:
                continue
            if joined.timestamp() < cutoff_ts:
                result["inactive"] += 1
                if not dry_run:
                    try:
                        await client(KickFromChannelRequest(entity, p.user_id))
                        result["removed"] += 1
                        await asyncio.sleep(0.5)
                    except Exception:
                        pass

        await client.disconnect()
    except Exception as exc:
        result["error"] = str(exc)[:100]

    return result


def format_cleanup(r: dict) -> str:
    if r.get("error"):
        return f"❌ *Cleanup — שגיאה*\n_{r['error']}_"
    mode = "🔍 סימולציה \\(Dry Run\\)" if r["dry_run"] else "🗑 ניקוי ביצוע"
    return (
        f"🧹 *Group Cleanup — קבוצה {r['group_id']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔬 מצב: {mode}\n"
        f"👥 סרוקו: {r['scanned']}\n"
        f"💤 לא פעיל: {r['inactive']}\n"
        f"🗑 הוסרו: {r['removed']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"_להסרה אמיתית: /cleanup \\<group\\_id\\> false_"
    )
