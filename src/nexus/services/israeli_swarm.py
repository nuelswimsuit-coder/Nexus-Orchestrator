"""
Israeli Swarm Engine — Hatan Industries
========================================

Orchestrates a full Israeli-Hebrew Telegram swarm:

1. SESSION HARVESTER  — Scans vault/incoming for ZIP/RAR archives, extracts
                        .session files into vault/sessions automatically.

2. COMMUNITY ENGINE   — Drives bots to join a target group and generate
                        contextual Israeli Hebrew chat (news-community tone) via Gemini.
                        Reads recent messages so bots reply like humans; no hashtag spam.
                        Optional: SWARM_UPDATE_PROFILES sets Israeli-style display names, bios, usernames, and non-face photos (picsum or cleared avatar).

3. DASHBOARD STATS    — Reads telefix.db and Redis to expose live metrics
                        for the "Live AI Swarm" tab:
                        Total Sessions, Group Link, Conversation Status,
                        Verified/Written stats.

Environment variables
---------------------
GEMINI_API_KEY          — Google Gemini API key (required for dialogue gen)
SWARM_GROUP_LINK        — Telegram group invite link / username
TELEFIX_DB_PATH         — Override path to telefix.db
REDIS_URL               — Redis connection string (default: redis://127.0.0.1:6379/0)
VAULT_INCOMING_DIR      — Override path to vault/incoming (default: auto-detect)
VAULT_SESSIONS_DIR      — Override path to vault/sessions (default: auto-detect)
SWARM_SESSIONS_PER_CYCLE — Telethon send attempts per engine cycle (default 3, max 12)
SWARM_TELETHON_TIMEOUT_S — Max seconds per Telethon connect/send attempt (default 90, max 300)
SWARM_UPDATE_PROFILES   — If not 0/false/off, set each session's Telegram first/last name
                          and a distinct avatar once (writes *.swarm_identity.json).

A daemon thread publishes ``nexus:swarm:israeli:heartbeat`` every 15s so the dashboard
sees a pulse even while ``CommunityEngine`` is blocked inside a long Telethon cycle.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import pathlib
import random
import re
import shutil
import sqlite3
import time
import threading
import zipfile
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Literal

from nexus.services.anti_parrot_shield import MAX_REGENERATION_RETRIES, is_too_similar_to_recent
from nexus.services.recent_news_digest import append_article_link_to_text, telegram_image_filename_from_bytes
from nexus.services.tg_message_text import strip_trailing_israeli_news_outlet

log = logging.getLogger("hatan.israeli_swarm")
_HEARTBEAT_REDIS_WARNED = False

# ── Path resolution ────────────────────────────────────────────────────────────

def _project_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parent.parent.parent.parent


_ROOT = _project_root()


def _resolve_redis_dsn() -> str:
    """
    Use the same Redis URL coercion as the API (start_api.py): load .env,
    then apply_redis_url_to_environment() so Windows [::1] matches the broker
    the API writes to (avoids 127.0.0.1 vs IPv6 loopback split-brain).
    """
    try:
        from dotenv import load_dotenv

        load_dotenv(_ROOT / ".env", override=False)
    except Exception:
        pass
    try:
        from nexus.shared.redis_util import apply_redis_url_to_environment, default_redis_url_string

        apply_redis_url_to_environment()
        return (os.environ.get("REDIS_URL") or "").strip() or default_redis_url_string()
    except Exception:
        try:
            from nexus.shared.redis_util import default_redis_url_string

            return (os.getenv("REDIS_URL") or "").strip() or default_redis_url_string()
        except Exception:
            return (os.getenv("REDIS_URL") or "").strip() or "redis://127.0.0.1:6379/0"


_VAULT_INCOMING = pathlib.Path(
    os.getenv("VAULT_INCOMING_DIR", str(_ROOT / "vault" / "incoming"))
)
_VAULT_SESSIONS = pathlib.Path(
    os.getenv("VAULT_SESSIONS_DIR", str(_ROOT / "vault" / "sessions"))
)
_TELEFIX_DB = pathlib.Path(
    os.getenv("TELEFIX_DB_PATH", str(_ROOT / "telefix.db"))
)
_REDIS_URL = _resolve_redis_dsn()
_GEMINI_KEY = os.getenv("GEMINI_API_KEY", "")
_GROUP_LINK = os.getenv("SWARM_GROUP_LINK", "")
# Written by POST /api/swarm/start — must be readable here so UI link works without env restart.
_REDIS_SWARM_STATUS_KEY = "nexus:swarm:israeli:status"
_REDIS_SWARM_TARGET_KEY = "nexus:swarm:israeli:target_group"
_REDIS_LAST_ENGINE_ERROR_KEY = "nexus:swarm:israeli:last_engine_error"
_ISRAELI_EVENTS_KEY = "nexus:swarm:israeli:events"
_REDIS_POKE_KEY = "nexus:swarm:israeli:poke"
_ISRAELI_HEARTBEAT_KEY = "nexus:swarm:israeli:heartbeat"
_ISRAELI_ENGINE_PID_KEY = "nexus:swarm:israeli:engine_pid"
_ISRAELI_SCHEDULE_KEY = "nexus:swarm:israeli:schedule"

# Per-target-group last sent Hebrew lines (Redis list; local fallback if broker fails).
_israeli_group_recent_sent_local: dict[str, list[str]] = {}


def _israeli_group_recent_sent_redis_key(group_link: str) -> str:
    d = hashlib.sha256(group_link.strip().encode("utf-8")).hexdigest()[:16]
    return f"nexus:swarm:israeli:group_recent_sent:{d}"


async def _israeli_group_recent_sent_fetch(group_link: str) -> list[str]:
    from nexus.services.anti_parrot_shield import RECENT_SENT_CAP

    rk = _israeli_group_recent_sent_redis_key(group_link)
    try:
        import redis.asyncio as aioredis  # type: ignore[import]

        r = await aioredis.from_url(_REDIS_URL, decode_responses=True)
        try:
            raw = await r.lrange(rk, 0, RECENT_SENT_CAP - 1)
        finally:
            await r.aclose()
        out: list[str] = []
        for x in raw or []:
            s = str(x).strip()
            if s:
                out.append(s)
        return out
    except Exception:
        return list(_israeli_group_recent_sent_local.get(rk, []))


async def _israeli_group_recent_sent_push(group_link: str, fragment: str) -> None:
    from nexus.services.anti_parrot_shield import RECENT_SENT_CAP

    frag = (fragment or "").strip()[:600]
    if not frag:
        return
    rk = _israeli_group_recent_sent_redis_key(group_link)
    try:
        import redis.asyncio as aioredis  # type: ignore[import]

        r = await aioredis.from_url(_REDIS_URL, decode_responses=True)
        try:
            await r.lpush(rk, frag)
            await r.ltrim(rk, 0, RECENT_SENT_CAP - 1)
            await r.expire(rk, 1209600)
        finally:
            await r.aclose()
    except Exception:
        cur = _israeli_group_recent_sent_local.setdefault(rk, [])
        cur.insert(0, frag)
        del cur[RECENT_SENT_CAP:]


def _write_israeli_schedule_sync(payload: dict[str, Any]) -> None:
    """Dashboard: phase cycle|waiting, next_cycle_at, delay_total_s, cycle_started_at."""
    try:
        import redis as redis_sync

        body = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            **payload,
        }
        r = redis_sync.Redis.from_url(_REDIS_URL, decode_responses=True)
        try:
            r.set(
                _ISRAELI_SCHEDULE_KEY,
                json.dumps(body, ensure_ascii=False),
                ex=7200,
            )
        finally:
            r.close()
    except Exception:
        pass


def _rpush_israeli_attempt_sync(phone: str, display_name: str) -> None:
    try:
        import redis as redis_sync

        payload = json.dumps(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "phone": phone,
                "topic": "attempt",
                "message": "מנסה לשלוח הודעה…",
                "display_name": display_name,
                "engine": "israeli_swarm",
            },
            ensure_ascii=False,
        )
        r = redis_sync.Redis.from_url(_REDIS_URL, decode_responses=True)
        try:
            r.rpush(_ISRAELI_EVENTS_KEY, payload)
            r.ltrim(_ISRAELI_EVENTS_KEY, -500, -1)
            r.publish("nexus:swarm:events", payload)
        finally:
            r.close()
    except Exception:
        pass


def _touch_engine_heartbeat_sync() -> None:
    """So GET /live-feed can tell the community thread is alive (ISO UTC)."""
    ts = datetime.now(timezone.utc).isoformat()
    _redis_sync_set(_ISRAELI_HEARTBEAT_KEY, ts, ex=600)
    _redis_sync_set(_ISRAELI_ENGINE_PID_KEY, str(os.getpid()), ex=400)


def _rpush_feed_line(message: str, topic: str = "engine") -> None:
    """Append a dashboard-visible line to the same list as Telethon posts (phone empty → no fake bot row)."""
    try:
        import redis as redis_sync

        r = redis_sync.Redis.from_url(_REDIS_URL, decode_responses=True)
        try:
            payload = json.dumps(
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "phone": "",
                    "message": message,
                    "topic": topic,
                    "engine": "israeli_swarm",
                },
                ensure_ascii=False,
            )
            r.rpush(_ISRAELI_EVENTS_KEY, payload)
            r.ltrim(_ISRAELI_EVENTS_KEY, -500, -1)
        finally:
            r.close()
    except Exception:
        pass


def _redis_sync_get(key: str) -> str | None:
    try:
        import redis as redis_sync

        r = redis_sync.Redis.from_url(_REDIS_URL, decode_responses=True)
        try:
            v = r.get(key)
            return str(v) if v is not None else None
        finally:
            r.close()
    except Exception:
        return None


def _redis_sync_set(key: str, value: str, ex: int = 7200) -> None:
    global _HEARTBEAT_REDIS_WARNED
    try:
        import redis as redis_sync

        r = redis_sync.Redis.from_url(_REDIS_URL, decode_responses=True)
        try:
            r.set(key, value[:2000], ex=ex)
        finally:
            r.close()
    except Exception as exc:
        if key == _ISRAELI_HEARTBEAT_KEY and not _HEARTBEAT_REDIS_WARNED:
            log.warning(
                "[COMMUNITY] Redis heartbeat write failed — broker unreachable or wrong REDIS_URL "
                "(engine uses coerced DSN like the API). Error: %s",
                exc,
            )
            _HEARTBEAT_REDIS_WARNED = True


def _consume_poke_sync() -> bool:
    """True if dashboard/API requested an immediate engine cycle (SET by POST /swarm/start)."""
    try:
        import redis as redis_sync

        r = redis_sync.Redis.from_url(_REDIS_URL, decode_responses=True)
        try:
            if r.exists(_REDIS_POKE_KEY):
                r.delete(_REDIS_POKE_KEY)
                return True
            return False
        finally:
            r.close()
    except Exception:
        return False


def _publish_engine_error(detail: str) -> None:
    """Surface failures to GET /api/swarm/live-feed (dashboard banner)."""
    ts = datetime.now(timezone.utc).isoformat()
    _redis_sync_set(_REDIS_LAST_ENGINE_ERROR_KEY, f"{ts} | {detail}")


def _vault_session_inventory_hint() -> str:
    """Explain common mismatch: JSON metadata without Telethon .session files."""
    try:
        n_sess = len(list(_VAULT_SESSIONS.glob("*.session")))
        n_json = len(list(_VAULT_SESSIONS.glob("*.json")))
    except OSError:
        return "לא ניתן לקרוא את vault/sessions"
    if n_sess == 0 and n_json > 0:
        return (
            f"נמצאו {n_json} קבצי JSON אבל 0 קבצי .session — "
            "Telethon דורש קובץ session בינארי (למשל 9725….session) באותה תיקייה"
        )
    if n_sess == 0:
        return "אין קבצי *.session ב-vault/sessions (הוסף סשנים או ZIP ל-vault/incoming)"
    return ""


def _redis_swarm_status_allows_send() -> bool:
    """Legacy: if status key unset, keep sending. Dashboard stop sets 'stopped'."""
    raw = _redis_sync_get(_REDIS_SWARM_STATUS_KEY)
    if raw is None or not str(raw).strip():
        return True
    return str(raw).strip().lower() == "running"


def effective_swarm_group_link() -> str:
    """Prefer target from API/UI (Redis); fall back to SWARM_GROUP_LINK env."""
    r = _redis_sync_get(_REDIS_SWARM_TARGET_KEY)
    if r and r.strip():
        return r.strip()
    return (_GROUP_LINK or "").strip()


def _swarm_private_invite_hash(link: str) -> str | None:
    """Invite hash for ``t.me/+…`` / ``joinchat/…`` / leading ``+`` only."""
    s = (link or "").strip()
    if "/+" in s:
        h = s.split("/+")[-1].split("?")[0].strip()
        return h or None
    low = s.lower()
    if "joinchat/" in low:
        h = s.split("joinchat/")[-1].split("?")[0].strip()
        return h or None
    if s.startswith("+"):
        h = s[1:].strip()
        return h or None
    return None


def _swarm_public_username_from_link(link: str) -> str | None:
    """Public supergroup username from ``https://t.me/name`` (not ``+`` invites)."""
    s = link.strip()
    for prefix in (
        "https://t.me/",
        "http://t.me/",
        "https://telegram.me/",
        "http://telegram.me/",
    ):
        if len(s) >= len(prefix) and s[: len(prefix)].lower() == prefix.lower():
            rest = s[len(prefix) :].split("/")[0].split("?")[0].strip()
            if not rest or rest.startswith("+") or "joinchat" in rest.lower():
                return None
            return rest.lstrip("@") or None
    if s.startswith("@"):
        tail = s[1:].split("/")[0].split("?")[0].strip()
        if tail.startswith("+") or not tail:
            return None
        return tail
    if s and "/" not in s and not s.lower().startswith("http"):
        return s.lstrip("@") or None
    return None


async def _ensure_swarm_target_entity(client: Any, group_link: str) -> Any:
    """
    Join the target supergroup/channel when required and return an entity for
    ``send_message``. The previous code passed a raw URL into ``JoinChannelRequest``
    (expects InputChannel) and often skipped a real join → *You can't write in this chat*.
    """
    from telethon import errors
    from telethon.tl.functions.channels import JoinChannelRequest
    from telethon.tl.functions.messages import ImportChatInviteRequest
    from telethon.tl.types import Channel

    t = group_link.strip()
    priv = _swarm_private_invite_hash(t)
    if priv:
        try:
            res = await client(ImportChatInviteRequest(priv))
            chats = getattr(res, "chats", None) or []
            if chats:
                return chats[0]
        except errors.UserAlreadyParticipantError:
            pass
        return await client.get_entity(t)

    uname = _swarm_public_username_from_link(t)
    if uname:
        ent = await client.get_entity(uname)
        if isinstance(ent, Channel):
            try:
                await client(JoinChannelRequest(await client.get_input_entity(ent)))
            except errors.UserAlreadyParticipantError:
                pass
        return ent

    ent = await client.get_entity(t)
    if isinstance(ent, Channel) and (
        getattr(ent, "megagroup", False) or not getattr(ent, "broadcast", False)
    ):
        try:
            await client(JoinChannelRequest(await client.get_input_entity(ent)))
        except errors.UserAlreadyParticipantError:
            pass
    return ent


# ── Israeli display names (deterministic per session stem) ──────────────────

_ISRAELI_FIRST_NAMES = [
    "יוסי", "דני", "אורי", "נועם", "איתי", "רועי", "עומר", "גיא", "תומר", "אלון",
    "מיכאל", "אדם", "עידו", "ליאור", "שי", "רן", "עמית", "אביב", "הדר", "גל",
    "מיכל", "נועה", "שירה", "מאיה", "תמר", "יעל", "רות", "דנה", "ליאת", "ענת",
    "הילה", "קרן", "שקד", "מור", "אור", "ספיר", "לילך", "רוני", "מיטל", "עדי",
]

_ISRAELI_LAST_NAMES = [
    "כהן", "לוי", "מזרחי", "דהן", "אביב", "שפירא", "גולן", "ברק", "אדרי", "ביטון",
    "פרידמן", "גרין", "רוזן", "קליין", "אשכנזי", "סגל", "טל", "נחום", "אורבך", "חיים",
    "דוד", "משה", "יוסף", "אליהו", "רפאל", "עמר", "זיו", "שמש", "אילן", "נבו",
]

_NEWS_ANGLES = [
    "ביטחון וסביב הגדרה",
    "מחירים ויוקר המחיה",
    "פוליטיקה מקומית",
    "חדשות מהאזור",
    "משפט ותקשורת",
    "כלכלה ושוק העבודה",
    "בריאות ומערכות ציבוריות",
]

_FALLBACK_CHAT_LINES = [
    "וואלה הזייה",
    "אמאלה רצח",
    "אין מצב אחי",
    "פיגוע פלילי",
    "מטורף מה שקורה",
    "תכלס וואלה",
    "זה לא נורמלי",
    "אני בלי מילים",
    "הם באמת רציניים?",
    "נו באמת",
]

ISRAELI_NEWS_SYSTEM_PROMPT = (
    "You are a restless Israeli in a Telegram news group — impatient, slangy, opinionated. "
    "You are NOT a journalist, NOT a news anchor, and NOT summarizing articles.\n"
    "STRICT OUTPUT RULES (Hebrew only in JSON \"text\"):\n"
    "1. LENGTH: exactly 2–10 words. Not one word; not a sentence; bursts like real chat.\n"
    "2. NO COPY-PASTE: Never output the raw headline or any long phrase from real_news / preferred_anchor. "
    "Read internally, then react in your own words (reaction, swear, joke, cynicism).\n"
    "3. NO SOURCES: Never print outlet names or patterns like \"- Ynet\", \"- מעריב\", \"- calcalist\", \"- N12\", "
    "or \"[ynet]\". The digest has no outlet labels — do not invent them.\n"
    "4. NO LAZY OPENERS: Forbidden starts include \"שמעתם כבר\", \"שמעתם על\", \"דיווח:\", \"לפי כותרות\", "
    "\"ראיתם ש\", \"חדשות:\" — jump straight into the vibe.\n"
    "5. ROLE=replier: Do NOT repeat or paraphrase facts from message_you_reply_to. "
    "Only opinion, joke, complaint, or disagreement matching your persona — zero recap.\n"
    "6. NO hashtags (#). Minor typos OK. Casual slang (אחי, וואלה, תכלס, אמאלה, הזייה).\n"
    "Grounding: pick ONE event implied by the digest, but your line must sound like a person texting, not citing news."
)

ISRAELI_NEWS_PRIVILEGED_REPLY_PROMPT = (
    "You are a casual Israeli Telegram user. The message you reply to was sent by a group OWNER or ADMIN.\n"
    "Write 2–10 words in polite casual Hebrew. Do not repeat what they said; brief agreement, thanks, or light follow-up only.\n"
    "No arguing, insults, or profanity. No hashtags. Never paste headlines or outlet names."
)

_REDIS_SWARM_PROFILE_GATE = "nexus:swarm:israeli:profile_gate"
_THREAD_ID_CAP = 5
_THREAD_REACTION_EMOJIS = ["👍", "🤦‍♂️", "🤬"]


def _swarm_identity_path(stem: str) -> pathlib.Path:
    return _VAULT_SESSIONS / f"{stem}.swarm_identity.json"


def _identity_from_stem(stem: str) -> dict[str, Any]:
    """Match ``roll_israeli_profile(stem)`` naming so local JSON matches Telegram."""
    try:
        from nexus.worker.services.israeli_telegram_profile import (  # type: ignore[import]
            roll_display_name_for_session,
        )

        fn, ln = roll_display_name_for_session(stem)
    except Exception:
        h = hashlib.sha256(stem.encode("utf-8")).digest()
        fi = h[0] % len(_ISRAELI_FIRST_NAMES)
        li = h[1] % len(_ISRAELI_LAST_NAMES)
        fn, ln = _ISRAELI_FIRST_NAMES[fi], _ISRAELI_LAST_NAMES[li]
    return {
        "first_name": fn,
        "last_name": ln,
        "avatar_seed": stem,
        "profile_applied": False,
    }


def _load_or_create_swarm_identity(stem: str) -> dict[str, Any]:
    p = _swarm_identity_path(stem)
    if p.exists():
        try:
            with open(p, encoding="utf-8") as f:
                d = json.load(f)
            if isinstance(d, dict) and d.get("first_name"):
                d.setdefault("profile_applied", False)
                d.setdefault("last_name", "")
                return d
        except Exception:
            pass
    base = _identity_from_stem(stem)
    try:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(base, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    return base


def _save_swarm_identity(stem: str, data: dict[str, Any]) -> None:
    try:
        with open(_swarm_identity_path(stem), "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _swarm_profiles_enabled() -> bool:
    v = (os.getenv("SWARM_UPDATE_PROFILES", "1") or "").strip().lower()
    return v not in ("0", "false", "no", "off", "")


def _strip_hashtags_and_cleanup(text: str) -> str:
    s = re.sub(r"#\S+", "", text or "")
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def _extract_json_object(text: str) -> dict[str, Any] | None:
    t = (text or "").strip()
    if not t:
        return None
    try:
        i = t.index("{")
        j = t.rindex("}") + 1
        return json.loads(t[i:j])
    except Exception:
        return None


def _redis_thread_key(group_link: str) -> str:
    d = hashlib.sha256(group_link.strip().encode("utf-8")).hexdigest()[:16]
    return f"nexus:swarm:israeli:thread:{d}"


def _finalize_swarm_llm_line(text: str) -> str:
    s = _strip_hashtags_and_cleanup(text)
    s = strip_trailing_israeli_news_outlet(s)
    parts = s.split()
    if len(parts) > 10:
        parts = parts[:10]
    if len(parts) == 1 and parts[0]:
        parts.append(random.choice(["אחי", "תכלס", "וואלה", "נו"]))
    return " ".join(parts).strip()


def _display_name_is_non_israeli(first: str, last: str) -> bool:
    combined = f"{first or ''} {last or ''}".strip()
    if not combined:
        return True
    if re.search(r"[\u0590-\u05FF]", combined):
        return False
    latin = re.sub(r"[^A-Za-z]", "", combined)
    if len(latin) < 2:
        return False
    return True


def _anchor_from_transcript(transcript: str, msg_id: int) -> str | None:
    prefix = f"[{int(msg_id)}]"
    for line in (transcript or "").splitlines():
        s = line.strip()
        if s.startswith(prefix):
            return s[len(prefix) :].strip()[:500]
    return None


def _roll_thread_role(has_thread: bool) -> Literal["lurk", "opener", "replier", "reactor"]:
    if random.random() < 0.10:
        return "lurk"
    if not has_thread:
        return "opener"
    u = random.random()
    if u < 0.35:
        return "opener"
    if u < 0.75:
        return "replier"
    return "reactor"


async def _async_thread_ids_read(group_link: str) -> list[int]:
    try:
        import redis.asyncio as aioredis  # type: ignore[import]
    except Exception:
        return []
    r = await aioredis.from_url(_REDIS_URL, decode_responses=True)
    try:
        raw = await r.get(_redis_thread_key(group_link))
        if not raw:
            return []
        data = json.loads(raw)
        if not isinstance(data, list):
            return []
        out: list[int] = []
        for x in data:
            try:
                out.append(int(x))
            except (TypeError, ValueError):
                continue
        return out
    except Exception:
        return []
    finally:
        await r.aclose()


async def _async_thread_ids_push(group_link: str, msg_id: int) -> None:
    try:
        import redis.asyncio as aioredis  # type: ignore[import]
    except Exception:
        return
    r = await aioredis.from_url(_REDIS_URL, decode_responses=True)
    try:
        key = _redis_thread_key(group_link)
        raw = await r.get(key)
        cur: list[int] = []
        if raw:
            try:
                data = json.loads(raw)
                if isinstance(data, list):
                    for x in data:
                        try:
                            cur.append(int(x))
                        except (TypeError, ValueError):
                            pass
            except Exception:
                pass
        cur.append(int(msg_id))
        cur = cur[-_THREAD_ID_CAP:]
        await r.set(key, json.dumps(cur, ensure_ascii=False))
    except Exception:
        pass
    finally:
        await r.aclose()


async def _sadd_swarm_profile_gate(stem: str) -> None:
    try:
        import redis.asyncio as aioredis  # type: ignore[import]

        r = await aioredis.from_url(_REDIS_URL, decode_responses=True)
        try:
            await r.sadd(_REDIS_SWARM_PROFILE_GATE, stem)
        finally:
            await r.aclose()
    except Exception:
        pass


async def _ensure_swarm_profile_ascii_fix(client: Any, stem: str) -> None:
    ident = _load_or_create_swarm_identity(stem)
    if ident.get("profile_applied"):
        await _sadd_swarm_profile_gate(stem)
        return
    try:
        import redis.asyncio as aioredis  # type: ignore[import]

        r = await aioredis.from_url(_REDIS_URL, decode_responses=True)
        try:
            if await r.sismember(_REDIS_SWARM_PROFILE_GATE, stem):
                return
        finally:
            await r.aclose()
    except Exception:
        pass
    try:
        from nexus.worker.services.israeli_telegram_profile import (  # type: ignore[import]
            roll_display_name_for_session,
        )

        me = await client.get_me()
        fn = str(getattr(me, "first_name", None) or "")
        ln = str(getattr(me, "last_name", None) or "")
        if _display_name_is_non_israeli(fn, ln):
            from telethon.tl.functions.account import UpdateProfileRequest  # type: ignore[import]

            nfn, nln = roll_display_name_for_session(stem)
            await client(UpdateProfileRequest(first_name=nfn, last_name=nln))
    except Exception as exc:
        log.debug("[COMMUNITY] On-the-fly profile fix skipped: %s", exc)
    await _sadd_swarm_profile_gate(stem)


async def _apply_swarm_identity(client: Any, stem: str) -> None:
    if not _swarm_profiles_enabled():
        return
    ident = _load_or_create_swarm_identity(stem)
    if ident.get("profile_applied"):
        return
    try:
        from nexus.worker.services.israeli_telegram_profile import (  # type: ignore[import]
            apply_israeli_profile_roll,
            roll_israeli_profile,
        )
    except Exception as exc:
        log.warning("[COMMUNITY] Israeli profile module unavailable for %s: %s", stem, exc)
        return

    roll = roll_israeli_profile(stem)
    try:
        await apply_israeli_profile_roll(client, roll)
    except Exception as exc:
        log.warning("[COMMUNITY] Profile apply failed for %s: %s", stem, exc)
        return

    ident["first_name"] = roll.first_name
    ident["last_name"] = roll.last_name
    ident["profile_applied"] = True
    ident.pop("avatar_seed", None)
    _save_swarm_identity(stem, ident)
    await _sadd_swarm_profile_gate(stem)


async def _fetch_group_transcript_and_meta(
    session_file: pathlib.Path,
    api_id: int,
    api_hash: str,
    group_link: str,
    limit: int = 22,
) -> tuple[str, list[dict[str, Any]]]:
    """Return (chronological transcript for the prompt, newest-first meta for reply IDs)."""
    from telethon import TelegramClient  # type: ignore[import]

    session_path = str(session_file.with_suffix(""))
    client = TelegramClient(session_path, api_id, api_hash)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            return "", []
        target = await _ensure_swarm_target_entity(client, group_link)
        msgs = await client.get_messages(target, limit=limit)
        lines: list[str] = []
        meta_newest_first: list[dict[str, Any]] = []
        try:
            from nexus.services.tg_message_text import telethon_display_text
        except Exception:
            telethon_display_text = None  # type: ignore[assignment,misc]

        for m in reversed([x for x in msgs if x]):
            if telethon_display_text is not None:
                text = telethon_display_text(m).strip()
            else:
                raw = (getattr(m, "message", None) or getattr(m, "raw_text", None) or "") or ""
                text = str(raw).strip()
            if not text:
                continue
            sid = int(getattr(m, "id", 0) or 0)
            if not sid:
                continue
            name = "משתמש"
            try:
                sdr = await m.get_sender()
                if sdr is not None:
                    parts = [
                        str(getattr(sdr, "first_name", "") or "").strip(),
                        str(getattr(sdr, "last_name", "") or "").strip(),
                    ]
                    name = " ".join(p for p in parts if p).strip() or name
            except Exception:
                pass
            lines.append(f"[{sid}] {name}: {text}")
            meta_newest_first.insert(0, {"id": sid, "sender": name})
        return "\n".join(lines), meta_newest_first
    except Exception as exc:
        log.debug("[COMMUNITY] Fetch transcript failed: %s", exc)
        return "", []
    finally:
        await client.disconnect()


async def _anchor_sender_is_privileged(
    session_file: pathlib.Path,
    api_id: int,
    api_hash: str,
    group_link: str,
    message_id: int | None,
) -> bool:
    if not message_id or not api_id or not (api_hash or "").strip():
        return False
    from telethon import TelegramClient  # type: ignore[import-untyped]

    session_path = str(session_file.with_suffix(""))
    client = TelegramClient(session_path, api_id, api_hash)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            return False
        target = await _ensure_swarm_target_entity(client, group_link)
        from nexus.services.tg_participant_privilege import sender_of_message_is_owner_or_admin

        return await sender_of_message_is_owner_or_admin(client, target, int(message_id))
    except Exception:
        return False
    finally:
        await client.disconnect()


async def _generate_community_message(
    transcript: str,
    meta_newest_first: list[dict[str, Any]],
    speaker_first: str,
    speaker_last: str,
    *,
    role: Literal["opener", "replier"] = "opener",
    anchor_preview: str | None = None,
    forced_reply_to: int | None = None,
    news_digest: str = "",
    anchor_headline: str = "",
    privileged_reply_target: bool = False,
    regeneration_attempt: int = 0,
) -> tuple[str, int | None]:
    """Short colloquial Hebrew line; reply_to is forced for replier role (Redis thread)."""
    angle = random.choice(_NEWS_ANGLES)
    display = f"{speaker_first} {speaker_last}".strip()
    reply_out: int | None = forced_reply_to if role == "replier" and forced_reply_to else None

    if not _GEMINI_KEY:
        text = random.choice(_FALLBACK_CHAT_LINES)
        return _finalize_swarm_llm_line(text), reply_out

    nd = (news_digest or "").strip()
    ah = (anchor_headline or "").strip()

    if role == "opener":
        user_obj: dict[str, Any] = {
            "role": "opener",
            "angle_hint": angle,
            "your_display_name": display,
            "recent_chat_chronological": (
                transcript or "(אין הודעות אחרונות — תפתח בניחוש חדשותי קצר)"
            )[-5500:],
            "task": "פותח: תגובה רגשית קצרה (2–10 מילים) לאירוע אחד מהדיגסט — בלי לצטט כותרת, בלי מקור, בלי קידומת חדשות.",
            "output_contract": 'החזר אך ורק JSON: {"text":"..."}',
        }
    else:
        ap = (anchor_preview or "").strip()[:800] or "(אין טקסט — תגיב בקצרה)"
        user_obj = {
            "role": "replier",
            "angle_hint": angle,
            "your_display_name": display,
            "message_you_reply_to": ap,
            "recent_chat_chronological": (transcript or "")[-5500:],
            "task": "משיב בשרשור: רק עמדה/בדיחה/תלונה/התנגדות — 2–10 מילים. אסור לחזור על עובדות מההודעה שאתה משיב לה.",
            "output_contract": 'החזר אך ורק JSON: {"text":"..."}',
        }
    if nd:
        user_obj["real_news_last_24h"] = nd[:6000]
        user_obj["news_grounding_rule"] = (
            "בחר אירוע אחד מהרשימה כבסיס פנימי בלבד; בטקסט החוצה רק תגובה אנושית קצרה — "
            "לא להעתיק ניסוח מהכותרות ולא לציין אתר/עיתון."
        )
    if ah:
        user_obj["preferred_anchor_headline"] = ah[:400]
    if privileged_reply_target and role == "replier":
        user_obj["admin_reply_constraint"] = (
            "ההודעה שאתה משיב לה נשלחה על ידי מנהל או בעלים של הקבוצה. "
            "היה מנומס, קצר, בלי ויכוח, בלי קללות ובלי התנגדות אגרסיבית."
        )
    if regeneration_attempt == 1:
        user_obj["anti_parrot_regen"] = (
            "דחייה טכנית: שורה קודמת דומה מדי להודעות שכבר נשלחו בקבוצה. "
            "חובה: משפט אחר לגמרי — מילים ומבנה שונים, בלי לשכפל ניסוח קודם."
        )
    elif regeneration_attempt >= 2:
        user_obj["anti_parrot_regen"] = (
            "ניסיון אחרון לפני דילוג: אסור דמיון לשורות שנשלחו לאחרונה בקבוצה. "
            "זווית חדשה לגמרי — בדיחה, רגש אחר, או נושא אחר."
        )
    user_payload = json.dumps(user_obj, ensure_ascii=False)

    try:
        import urllib.request

        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-pro:generateContent?key={_GEMINI_KEY}"
        )
        base_sys = (
            ISRAELI_NEWS_PRIVILEGED_REPLY_PROMPT
            if privileged_reply_target and role == "replier"
            else ISRAELI_NEWS_SYSTEM_PROMPT
        )
        prompt = f"{base_sys}\n\nהקשר JSON:\n{user_payload}"
        temp = min(1.0, 0.9 + 0.05 * max(0, regeneration_attempt))
        body = json.dumps(
            {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": 120, "temperature": temp},
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        loop = asyncio.get_event_loop()
        response_bytes = await loop.run_in_executor(
            None,
            lambda: urllib.request.urlopen(req, timeout=22).read(),
        )
        data = json.loads(response_bytes)
        raw = (
            data.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
            .strip()
        )
        parsed = _extract_json_object(raw) or {}
        text = _finalize_swarm_llm_line(str(parsed.get("text") or "").strip())
        if text:
            return text, reply_out
    except Exception as exc:
        log.debug("[GEMINI] Community message failed (%s) — fallback", exc)

    text_fb = random.choice(_FALLBACK_CHAT_LINES)
    return _finalize_swarm_llm_line(text_fb), reply_out


async def _generate_unique_community_message(
    group_link: str,
    transcript: str,
    meta_newest_first: list[dict[str, Any]],
    speaker_first: str,
    speaker_last: str,
    *,
    role: Literal["opener", "replier"] = "opener",
    anchor_preview: str | None = None,
    forced_reply_to: int | None = None,
    news_digest: str = "",
    anchor_headline: str = "",
    privileged_reply_target: bool = False,
) -> tuple[str, int | None] | None:
    """
    Up to 1 + MAX_REGENERATION_RETRIES Gemini attempts vs last 15 lines sent in this group.
    None => skip this bot's turn (do not send a parrot line).
    """
    recent = await _israeli_group_recent_sent_fetch(group_link)
    for attempt in range(1 + MAX_REGENERATION_RETRIES):
        text, reply_out = await _generate_community_message(
            transcript,
            meta_newest_first,
            speaker_first,
            speaker_last,
            role=role,
            anchor_preview=anchor_preview,
            forced_reply_to=forced_reply_to,
            news_digest=news_digest,
            anchor_headline=anchor_headline,
            privileged_reply_target=privileged_reply_target,
            regeneration_attempt=attempt,
        )
        if not is_too_similar_to_recent(text, recent):
            return text, reply_out
        log.debug(
            "[COMMUNITY] anti-parrot reject attempt=%s sample=%s",
            attempt + 1,
            (text or "")[:72],
        )
    return None


# ── Session Harvester ─────────────────────────────────────────────────────────

class SessionHarvester:
    """
    Watches vault/incoming for ZIP/RAR archives and extracts .session files
    into vault/sessions. Runs in a background thread.
    """

    POLL_INTERVAL_S = 30

    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.extracted_total = 0

    def start(self) -> None:
        _VAULT_INCOMING.mkdir(parents=True, exist_ok=True)
        _VAULT_SESSIONS.mkdir(parents=True, exist_ok=True)
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="israeli-swarm-harvester", daemon=True
        )
        self._thread.start()
        log.info("[HARVESTER] Session harvester started — watching %s", _VAULT_INCOMING)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._scan_once()
            except Exception as exc:
                log.warning("[HARVESTER] Scan error: %s", exc)
            self._stop.wait(timeout=self.POLL_INTERVAL_S)

    def _scan_once(self) -> None:
        if not _VAULT_INCOMING.is_dir():
            return
        for archive in _VAULT_INCOMING.iterdir():
            if archive.suffix.lower() not in (".zip", ".rar"):
                continue
            try:
                count = self._extract_archive(archive)
                if count:
                    log.info(
                        "[HARVESTER] Extracted %d session(s) from %s",
                        count, archive.name,
                    )
                    self.extracted_total += count
                    # Move processed archive to vault/processed/ (sibling of vault/sessions)
                    done_dir = _ROOT / "vault" / "processed"
                    done_dir.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(archive), str(done_dir / archive.name))
            except Exception as exc:
                log.warning("[HARVESTER] Failed to extract %s: %s", archive.name, exc)

    def _extract_archive(self, archive: pathlib.Path) -> int:
        extracted = 0
        if archive.suffix.lower() == ".zip":
            with zipfile.ZipFile(archive, "r") as zf:
                for member in zf.namelist():
                    if member.endswith(".session"):
                        filename = pathlib.Path(member).name
                        dest = _VAULT_SESSIONS / filename
                        if not dest.exists():
                            with zf.open(member) as src, open(dest, "wb") as dst:
                                shutil.copyfileobj(src, dst)
                            extracted += 1
        elif archive.suffix.lower() == ".rar":
            try:
                import rarfile  # type: ignore[import]
                with rarfile.RarFile(str(archive)) as rf:
                    for member in rf.namelist():
                        if member.endswith(".session"):
                            filename = pathlib.Path(member).name
                            dest = _VAULT_SESSIONS / filename
                            if not dest.exists():
                                rf.extract(member, str(_VAULT_SESSIONS))
                                extracted += 1
            except ImportError:
                log.warning("[HARVESTER] rarfile not installed — cannot extract .rar archives")
        return extracted


# ── Community Engine ──────────────────────────────────────────────────────────

class CommunityEngine:
    """
    Drives bots to join the target group and post natural Hebrew dialogue.
    Each active session gets a random delay to simulate organic behaviour.
    """

    MIN_DELAY_S = 60
    MAX_DELAY_S = 600

    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.messages_sent = 0
        self.bots_joined = 0

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="israeli-swarm-community", daemon=True
        )
        self._thread.start()
        log.info(
            "[COMMUNITY] Community engine started — group: %s",
            effective_swarm_group_link() or "(not set — use UI Start or SWARM_GROUP_LINK)",
        )

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)

    def _loop(self) -> None:
        while not self._stop.is_set():
            now_iso = datetime.now(timezone.utc).isoformat()
            _write_israeli_schedule_sync(
                {
                    "phase": "cycle",
                    "cycle_started_at": now_iso,
                    "next_cycle_at": None,
                    "delay_total_s": None,
                }
            )
            try:
                asyncio.run(self._cycle())
            except Exception as exc:
                log.warning("[COMMUNITY] Cycle error: %s", exc)
            delay = random.randint(self.MIN_DELAY_S, self.MAX_DELAY_S)
            next_ts = datetime.now(timezone.utc).timestamp() + delay
            next_iso = datetime.fromtimestamp(next_ts, tz=timezone.utc).isoformat()
            _write_israeli_schedule_sync(
                {
                    "phase": "waiting",
                    "cycle_started_at": None,
                    "next_cycle_at": next_iso,
                    "delay_total_s": delay,
                }
            )
            remaining = delay
            while remaining > 0 and not self._stop.is_set():
                chunk = min(remaining, 5)
                if self._stop.wait(timeout=chunk):
                    break
                remaining -= chunk
                if _consume_poke_sync():
                    break

    async def _cycle(self) -> None:
        try:
            if not _redis_swarm_status_allows_send():
                log.debug("[COMMUNITY] Swarm paused — Redis status is not 'running'")
                return

            # Pulse early so the dashboard sees a heartbeat during long Telethon work, not only at cycle end.
            _touch_engine_heartbeat_sync()

            group_link = effective_swarm_group_link()
            if not group_link:
                msg = "חסר קישור קבוצה — לחץ Start Swarm בדשבורד או הגדר SWARM_GROUP_LINK"
                log.warning("[COMMUNITY] %s", msg)
                _publish_engine_error(msg)
                return

            sessions = list(_VAULT_SESSIONS.glob("*.session"))
            if not sessions:
                log.info("[COMMUNITY] No sessions in vault/sessions — triggering immediate harvester scan")
                if _harvester:
                    try:
                        _harvester._scan_once()
                        sessions = list(_VAULT_SESSIONS.glob("*.session"))
                    except Exception as exc:
                        log.warning("[COMMUNITY] Harvester scan error: %s", exc)
            if not sessions:
                hint = _vault_session_inventory_hint()
                log.warning("[COMMUNITY] %s", hint or "אין סשנים זמינים")
                _publish_engine_error(hint or "אין קבצי .session ב-vault/sessions")
                return

            gl_short = (group_link[:56] + "…") if len(group_link) > 56 else group_link
            _rpush_feed_line(
                f"[מנוע] מחזור התחיל — יעד: {gl_short} · סשנים זמינים: {len(sessions)}",
                "engine",
            )

            try:
                per = int(os.getenv("SWARM_SESSIONS_PER_CYCLE", "3") or "3")
            except ValueError:
                per = 3
            per = max(1, min(per, 12, len(sessions)))

            api_id = int(os.getenv("TELEGRAM_API_ID", "0") or "0")
            if not api_id:
                api_id = int(os.getenv("TELEFIX_API_ID", "0") or "0")
            api_hash = (
                os.getenv("TELEGRAM_API_HASH", "")
                or os.getenv("TELEFIX_API_HASH", "")
                or ""
            ).strip()

            transcript = ""
            meta_nf: list[dict[str, Any]] = []
            if api_id and api_hash:
                fetch_order = list(sessions)
                random.shuffle(fetch_order)
                for probe in fetch_order:
                    transcript, meta_nf = await _fetch_group_transcript_and_meta(
                        probe, api_id, api_hash, group_link
                    )
                    if transcript or meta_nf:
                        break

            news_digest_str = ""
            news_anchor_str = ""
            news_anchor_link = ""
            news_image_url: str | None = None
            try:
                import redis.asyncio as aioredis  # type: ignore[import]
                from nexus.services.recent_news_digest import get_tick_news_bundle_for_consumer

                r_news = await aioredis.from_url(_REDIS_URL, decode_responses=True)
                try:
                    _nb = await get_tick_news_bundle_for_consumer(r_news)
                finally:
                    await r_news.aclose()
                news_digest_str = _nb.digest_text
                news_anchor_str = _nb.anchor_title
                news_anchor_link = (_nb.anchor_link or "").strip()
                news_image_url = _nb.image_url
            except Exception as exc:
                log.debug("[COMMUNITY] news bundle skipped: %s", exc)

            shared_photo: bytes | None = None
            if news_image_url:
                try:
                    from nexus.services.recent_news_digest import download_image_bytes

                    shared_photo = await download_image_bytes(news_image_url)
                except Exception:
                    shared_photo = None
                if shared_photo is None:
                    log.debug(
                        "[COMMUNITY] headline image download failed or empty (url=%s)",
                        (news_image_url[:80] + "…") if len(news_image_url) > 80 else news_image_url,
                    )

            used_stems: set[str] = set()
            any_sent = False
            has_api = bool(
                int(os.getenv("TELEGRAM_API_ID", "0") or os.getenv("TELEFIX_API_ID", "0") or "0")
            )
            thread_ids = await _async_thread_ids_read(group_link)
            has_thread = len(thread_ids) > 0
            for _attempt in range(per):
                pool = [s for s in sessions if s.stem not in used_stems] or sessions
                session_file = random.choice(pool)
                used_stems.add(session_file.stem)
                phone = session_file.stem
                ident = _load_or_create_swarm_identity(phone)
                sf = str(ident.get("first_name") or "")
                sl = str(ident.get("last_name") or "")
                role = _roll_thread_role(has_thread)
                if role == "replier" and not has_thread:
                    role = "opener"
                if role == "reactor" and not has_thread:
                    role = "opener"

                if role == "lurk":
                    continue

                if role == "reactor":
                    anchor_id = thread_ids[-1]
                    who_try = f"{sf} {sl}".strip() or phone
                    _rpush_israeli_attempt_sync(phone, who_try)
                    ok_r = await self._try_react_telethon(session_file, group_link, anchor_id)
                    if ok_r:
                        any_sent = True
                        self.messages_sent += 1
                        await self._push_redis_event(phone, "חדשות", f"[reaction] → msg {anchor_id}")
                        await self._mark_verified_written(phone)
                    continue

                forced_reply = thread_ids[-1] if role == "replier" and thread_ids else None
                anchor_txt = (
                    _anchor_from_transcript(transcript, forced_reply) if forced_reply else None
                )
                privileged_reply = False
                if forced_reply and api_id and api_hash:
                    privileged_reply = await _anchor_sender_is_privileged(
                        session_file, api_id, api_hash, group_link, forced_reply
                    )
                gen_pair = await _generate_unique_community_message(
                    group_link,
                    transcript,
                    meta_nf,
                    sf,
                    sl,
                    role="replier" if role == "replier" else "opener",
                    anchor_preview=anchor_txt,
                    forced_reply_to=forced_reply,
                    news_digest=news_digest_str,
                    anchor_headline=news_anchor_str,
                    privileged_reply_target=privileged_reply,
                )
                if gen_pair is None:
                    continue
                core_line, reply_to = gen_pair
                message, msg_parse_mode = append_article_link_to_text(
                    core_line,
                    news_anchor_link,
                    title=news_anchor_str or None,
                )
                log.info(
                    "[COMMUNITY] Bot %s role=%s → msg=%s reply_to=%s",
                    phone,
                    role,
                    message[:60],
                    reply_to,
                )
                who_try = f"{sf} {sl}".strip() or phone
                _rpush_israeli_attempt_sync(phone, who_try)
                photo_bytes: bytes | None = None
                if shared_photo and random.random() < 0.82:
                    photo_bytes = shared_photo
                sent, new_mid = await self._try_send_telethon(
                    session_file,
                    message,
                    group_link,
                    reply_to=reply_to,
                    photo_bytes=photo_bytes,
                    parse_mode=msg_parse_mode,
                )
                if sent:
                    any_sent = True
                    self.messages_sent += 1
                    await _israeli_group_recent_sent_push(group_link, core_line)
                    if new_mid is not None:
                        await _async_thread_ids_push(group_link, int(new_mid))
                        thread_ids = await _async_thread_ids_read(group_link)
                        has_thread = len(thread_ids) > 0
                    topic_tag = "חדשות"
                    await self._push_redis_event(phone, topic_tag, message)
                    await self._mark_verified_written(phone)
                    who = f"{sf} {sl}".strip() or phone
                    chunk = f"[הודעה חדשה במחזור זה] {who}: {message}"
                    transcript = (transcript + "\n" + chunk)[-5500:] if transcript else chunk

            if not any_sent and has_api:
                _rpush_feed_line(
                    "[מנוע] מחזור הסתיים ללא שליחה מוצלחת — בדוק last_engine_error / לוגי Telethon",
                    "engine",
                )
        finally:
            _touch_engine_heartbeat_sync()

    async def _try_send_telethon(
        self,
        session_file: pathlib.Path,
        message: str,
        group_link: str,
        *,
        reply_to: int | None = None,
        photo_bytes: bytes | None = None,
        parse_mode: str | None = None,
    ) -> tuple[bool, int | None]:
        if not group_link:
            return False, None
        try:
            timeout_s = float(os.getenv("SWARM_TELETHON_TIMEOUT_S", "90") or "90")
        except ValueError:
            timeout_s = 90.0
        timeout_s = max(15.0, min(timeout_s, 300.0))

        try:
            from telethon import TelegramClient  # type: ignore[import]

            api_id = int(os.getenv("TELEGRAM_API_ID", "0") or "0")
            if not api_id:
                api_id = int(os.getenv("TELEFIX_API_ID", "0") or "0")
            api_hash = (
                os.getenv("TELEGRAM_API_HASH", "")
                or os.getenv("TELEFIX_API_HASH", "")
                or ""
            ).strip()
            if not api_id or not api_hash:
                msg = "חסר TELEGRAM_API_ID/HASH או TELEFIX_API_ID/HASH ב-.env"
                log.warning("[COMMUNITY] %s", msg)
                _publish_engine_error(msg)
                return False, None

            session_path = str(session_file.with_suffix(""))

            async def _run_client() -> tuple[bool, int | None]:
                # Telethon's ``async with TelegramClient`` calls ``start()``, which prompts
                # for phone/OTP via ``input()`` — headless israeli-swarm has no TTY → EOFError.
                from telethon import errors

                client = TelegramClient(session_path, api_id, api_hash)
                await client.connect()
                try:
                    if not await client.is_user_authorized():
                        detail = (
                            f"{session_file.stem}: סשן טלגרם לא מחובר או פג תוקף — "
                            "לא ניתן לבקש קוד SMS/סיסמה ברקע (אין טרמינל). "
                            "התחבר מחדש אינטראקטיבית (למשל דרך כלי סשן) והחלף את ‎.session ב־vault."
                        )
                        log.warning("[COMMUNITY] %s", detail)
                        _publish_engine_error(detail)
                        return False, None
                    try:
                        await _apply_swarm_identity(client, session_file.stem)
                        await _ensure_swarm_profile_ascii_fix(client, session_file.stem)
                        target = await _ensure_swarm_target_entity(client, group_link)
                        async with client.action(target, "typing"):
                            await asyncio.sleep(random.uniform(2.0, 8.0))
                        if photo_bytes:
                            from nexus.services.media_opsec import (
                                make_image_upload_salt_seed,
                                prepare_jpeg_png_for_telegram_upload,
                            )

                            _salt = make_image_upload_salt_seed(session_file.stem)
                            _pb, _ = prepare_jpeg_png_for_telegram_upload(
                                photo_bytes, salt_seed=_salt
                            )
                            fname = telegram_image_filename_from_bytes(_pb)
                            bio = BytesIO(_pb)
                            try:
                                sent = await client.send_file(
                                    target,
                                    file=(fname, bio),
                                    caption=message[:1024],
                                    reply_to=reply_to if reply_to else None,
                                    force_document=False,
                                    parse_mode=parse_mode,
                                )
                            except Exception as photo_exc:
                                log.warning(
                                    "[COMMUNITY] send_file failed (%s) — text only",
                                    photo_exc,
                                )
                                sent = await client.send_message(
                                    target,
                                    message,
                                    reply_to=reply_to if reply_to else None,
                                    parse_mode=parse_mode,
                                )
                        else:
                            sent = await client.send_message(
                                target,
                                message,
                                reply_to=reply_to if reply_to else None,
                                parse_mode=parse_mode,
                            )
                        mid_raw = getattr(sent, "id", None)
                        mid = int(mid_raw) if mid_raw is not None else None
                        return True, mid
                    except errors.ChatWriteForbiddenError as cw_exc:
                        detail = (
                            f"{session_file.stem}: אין הרשאת כתיבה בצ'אט (אולי ערוץ הכרזות או חסימת משתמשים). "
                            f"({cw_exc})"
                        )
                        log.warning("[COMMUNITY] %s", detail)
                        _publish_engine_error(detail[:500])
                        return False, None
                finally:
                    await client.disconnect()

            try:
                return await asyncio.wait_for(_run_client(), timeout=timeout_s)
            except asyncio.TimeoutError:
                detail = f"{session_file.stem}: Telethon timeout {timeout_s:.0f}s"
                log.warning("[COMMUNITY] %s", detail)
                _publish_engine_error(detail)
                _rpush_feed_line(f"[מנוע] {detail}", "engine")
                return False, None

        except ImportError:
            msg = "חבילת telethon לא מותקנת בסביבת israeli-swarm"
            log.warning("[COMMUNITY] %s", msg)
            _publish_engine_error(msg)
        except Exception as exc:
            err = f"{session_file.stem}: {exc}"
            log.warning("[COMMUNITY] Send failed — %s", err)
            _publish_engine_error(err[:500])
        return False, None

    async def _try_react_telethon(
        self,
        session_file: pathlib.Path,
        group_link: str,
        msg_id: int,
    ) -> bool:
        if not group_link or not msg_id:
            return False
        try:
            timeout_s = float(os.getenv("SWARM_TELETHON_TIMEOUT_S", "90") or "90")
        except ValueError:
            timeout_s = 90.0
        timeout_s = max(15.0, min(timeout_s, 300.0))

        try:
            from telethon import TelegramClient  # type: ignore[import]

            api_id = int(os.getenv("TELEGRAM_API_ID", "0") or "0")
            if not api_id:
                api_id = int(os.getenv("TELEFIX_API_ID", "0") or "0")
            api_hash = (
                os.getenv("TELEGRAM_API_HASH", "")
                or os.getenv("TELEFIX_API_HASH", "")
                or ""
            ).strip()
            if not api_id or not api_hash:
                return False

            session_path = str(session_file.with_suffix(""))

            async def _run_client() -> bool:
                from telethon import errors
                from telethon.tl.functions.messages import SendReactionRequest  # type: ignore[import]
                from telethon.tl.types import ReactionEmoji  # type: ignore[import]

                client = TelegramClient(session_path, api_id, api_hash)
                await client.connect()
                try:
                    if not await client.is_user_authorized():
                        return False
                    try:
                        await _apply_swarm_identity(client, session_file.stem)
                        await _ensure_swarm_profile_ascii_fix(client, session_file.stem)
                        target = await _ensure_swarm_target_entity(client, group_link)
                        emojis = list(_THREAD_REACTION_EMOJIS)
                        random.shuffle(emojis)
                        for emo in emojis:
                            try:
                                await client(
                                    SendReactionRequest(
                                        peer=target,
                                        msg_id=int(msg_id),
                                        reaction=[ReactionEmoji(emoticon=emo)],
                                    )
                                )
                                return True
                            except Exception:
                                continue
                        return False
                    except errors.ChatWriteForbiddenError:
                        return False
                finally:
                    await client.disconnect()

            return await asyncio.wait_for(_run_client(), timeout=timeout_s)
        except Exception as exc:
            log.debug("[COMMUNITY] Reaction failed — %s", exc)
            return False

    async def _push_join_event(self, phone: str, group_link: str) -> None:
        try:
            import redis.asyncio as aioredis  # type: ignore[import]
            r = await aioredis.from_url(_REDIS_URL, decode_responses=True)
            payload = json.dumps({
                "event": "swarm_join_success",
                "ts": datetime.now(timezone.utc).isoformat(),
                "phone": phone,
                "group": group_link,
                "engine": "israeli_swarm",
            }, ensure_ascii=False)
            await r.rpush("nexus:swarm:israeli:events", payload)
            await r.ltrim("nexus:swarm:israeli:events", -500, -1)
            await r.publish("nexus:swarm:events", payload)
            await r.set("nexus:swarm:israeli:last_join", payload)
            await r.aclose()
            log.info("[COMMUNITY] swarm_join_success emitted for %s", phone)
        except Exception as exc:
            log.debug("[COMMUNITY] Redis join event push failed: %s", exc)

    async def _mark_verified_written(self, phone: str) -> None:
        """Update verified=1 and written=1 in DB + broadcast to UI via Redis."""
        # DB update
        try:
            if _TELEFIX_DB.exists():
                conn = sqlite3.connect(str(_TELEFIX_DB), timeout=5, check_same_thread=False)
                cur = conn.cursor()
                try:
                    cur.execute(
                        "UPDATE groups SET verified=1, written=1 WHERE phone=?", (phone,)
                    )
                    if cur.rowcount == 0:
                        cur.execute(
                            "INSERT OR IGNORE INTO groups (phone, verified, written) VALUES (?,1,1)",
                            (phone,),
                        )
                    conn.commit()
                except Exception:
                    pass
                finally:
                    conn.close()
        except Exception as exc:
            log.debug("[COMMUNITY] DB verified/written update failed: %s", exc)

        # Redis broadcast
        try:
            import redis.asyncio as aioredis  # type: ignore[import]
            r = await aioredis.from_url(_REDIS_URL, decode_responses=True)
            payload = json.dumps({
                "event": "swarm_post_verified",
                "ts": datetime.now(timezone.utc).isoformat(),
                "phone": phone,
                "verified": 1,
                "written": 1,
                "engine": "israeli_swarm",
            }, ensure_ascii=False)
            await r.publish("nexus:swarm:events", payload)
            await r.set(f"nexus:swarm:verified:{phone}", "1")
            await r.set(f"nexus:swarm:written:{phone}", "1")
            await r.aclose()
            log.info("[COMMUNITY] verified+written broadcast for %s", phone)
        except Exception as exc:
            log.debug("[COMMUNITY] Redis verified/written broadcast failed: %s", exc)

    async def _push_redis_event(self, phone: str, topic: str, message: str) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        ident_ev = _load_or_create_swarm_identity(phone)
        fn = str(ident_ev.get("first_name") or "")
        ln = str(ident_ev.get("last_name") or "")
        display_name = f"{fn} {ln}".strip() or phone
        payload = json.dumps({
            "ts": ts,
            "phone": phone,
            "topic": topic,
            "message": message,
            "verified": 1,
            "written": 1,
            "engine": "israeli_swarm",
            "display_name": display_name,
            "first_name": fn,
            "last_name": ln,
        }, ensure_ascii=False)
        # Update DB: mark this phone as verified=1 and written=1
        self._update_db_verification(phone)
        # Broadcast to Redis + UI
        try:
            import redis.asyncio as aioredis  # type: ignore[import]
            r = await aioredis.from_url(_REDIS_URL, decode_responses=True)
            await r.rpush("nexus:swarm:israeli:events", payload)
            await r.ltrim("nexus:swarm:israeli:events", -500, -1)
            await r.set("nexus:swarm:israeli:last_message", payload)
            # Broadcast to UI via pub/sub so dashboard updates instantly
            await r.publish("nexus:swarm:events", payload)
            # Increment running counters for the live stats panel
            await r.incr("nexus:swarm:israeli:verified_count")
            await r.incr("nexus:swarm:israeli:written_count")
            await r.delete(_REDIS_LAST_ENGINE_ERROR_KEY)
            await r.aclose()
            log.info("[COMMUNITY] Verification broadcast: phone=%s verified=1 written=1", phone)
        except Exception as exc:
            log.debug("[COMMUNITY] Redis push failed: %s", exc)

    def _update_db_verification(self, phone: str) -> None:
        """Mark phone as verified=1 and written=1 in telefix.db after a successful post."""
        if not _TELEFIX_DB.exists():
            return
        try:
            conn = sqlite3.connect(str(_TELEFIX_DB), timeout=5, check_same_thread=False)
            cur = conn.cursor()
            try:
                # Try telefix table first
                cur.execute(
                    "UPDATE telefix SET status='verified' WHERE phone=?",
                    (phone,),
                )
                if cur.rowcount == 0:
                    cur.execute(
                        "INSERT OR IGNORE INTO telefix (phone, status) VALUES (?, 'verified')",
                        (phone,),
                    )
            except sqlite3.OperationalError:
                pass
            try:
                # Try groups/sessions table
                cur.execute(
                    "UPDATE groups SET verified=1, written=1 WHERE phone=?",
                    (phone,),
                )
            except sqlite3.OperationalError:
                pass
            conn.commit()
            conn.close()
        except Exception as exc:
            log.debug("[COMMUNITY] DB verification update failed: %s", exc)


# ── Dashboard Stats ───────────────────────────────────────────────────────────

def get_swarm_stats() -> dict[str, Any]:
    """
    Return live stats for the 'Live AI Swarm' dashboard tab.

    Fields:
        total_sessions      — count of .session files in vault/sessions
        group_link          — SWARM_GROUP_LINK env value
        conversation_status — "ACTIVE" | "IDLE" | "NO_SESSIONS"
        verified            — count of verified rows from telefix.db
        written             — count of written/sent messages from telefix.db
        last_message_ts     — ISO timestamp of last swarm message
        extracted_total     — sessions extracted by harvester this run
        bots_joined         — bots that joined the group this run
        messages_sent       — messages sent this run
    """
    try:
        from nexus.services.tg_session_disk import count_live_telethon_session_files

        total_sessions = count_live_telethon_session_files()
    except Exception:
        sessions = list(_VAULT_SESSIONS.glob("*.session")) if _VAULT_SESSIONS.is_dir() else []
        total_sessions = len(sessions)

    verified = 0
    written = 0
    if _TELEFIX_DB.exists():
        try:
            conn = sqlite3.connect(str(_TELEFIX_DB), timeout=5, check_same_thread=False)
            cur = conn.cursor()
            try:
                cur.execute("SELECT COUNT(*) FROM telefix WHERE status='verified'")
                row = cur.fetchone()
                verified = int(row[0]) if row else 0
            except sqlite3.OperationalError:
                try:
                    cur.execute("SELECT COUNT(*) FROM groups WHERE verified=1")
                    row = cur.fetchone()
                    verified = int(row[0]) if row else 0
                except Exception:
                    pass
            try:
                cur.execute("SELECT COUNT(*) FROM telefix WHERE status='written'")
                row = cur.fetchone()
                written = int(row[0]) if row else 0
            except sqlite3.OperationalError:
                try:
                    cur.execute("SELECT COUNT(*) FROM messages")
                    row = cur.fetchone()
                    written = int(row[0]) if row else 0
                except Exception:
                    pass
            conn.close()
        except Exception as exc:
            log.debug("[STATS] telefix.db read error: %s", exc)

    gl = effective_swarm_group_link()
    status = "NO_SESSIONS" if total_sessions == 0 else "IDLE"
    if total_sessions > 0 and gl:
        status = "ACTIVE"

    return {
        "total_sessions": total_sessions,
        "group_link": gl or "—",
        "conversation_status": status,
        "verified": verified,
        "written": written,
        "last_message_ts": None,
        "extracted_total": _harvester.extracted_total if _harvester else 0,
        "bots_joined": _community.bots_joined if _community else 0,
        "messages_sent": _community.messages_sent if _community else 0,
    }


# ── IsraeliSwarmEngine — top-level service ────────────────────────────────────

_harvester: SessionHarvester | None = None
_community: CommunityEngine | None = None


class IsraeliSwarmEngine:
    """
    Top-level service that starts the harvester and community engine together.

    Usage
    -----
        engine = IsraeliSwarmEngine()
        engine.start()
        stats = engine.stats()
        engine.stop()
    """

    SERVICE_NAME = "IsraeliSwarmEngine"

    def __init__(self) -> None:
        global _harvester, _community
        self._hb_stop: threading.Event | None = None
        self._hb_thread: threading.Thread | None = None
        _harvester = SessionHarvester()
        _community = CommunityEngine()

    def _ensure_periodic_heartbeat(self) -> None:
        """Pulse Redis while Telethon may block the community thread for minutes."""
        if self._hb_thread is not None and self._hb_thread.is_alive():
            return
        self._hb_stop = threading.Event()

        def _run() -> None:
            assert self._hb_stop is not None
            while not self._hb_stop.wait(15.0):
                _touch_engine_heartbeat_sync()

        self._hb_thread = threading.Thread(
            target=_run, name="israeli-swarm-heartbeat", daemon=True
        )
        self._hb_thread.start()

    def start(self) -> None:
        global _harvester, _community
        _harvester.start()  # type: ignore[union-attr]
        _community.start()  # type: ignore[union-attr]
        _touch_engine_heartbeat_sync()
        self._ensure_periodic_heartbeat()
        log.info(
            "[%s] ✅ HATAN INDUSTRIES — ISRAELI SWARM ACTIVE — 5,000 MEMBER SIMULATION RUNNING",
            self.SERVICE_NAME,
        )
        print(
            "✅ [HATAN INDUSTRIES] ISRAELI SWARM ACTIVE — 5,000 MEMBER SIMULATION RUNNING",
            flush=True,
        )

    def stop(self) -> None:
        global _harvester, _community
        if self._hb_stop is not None:
            self._hb_stop.set()
        if self._hb_thread is not None:
            self._hb_thread.join(timeout=5.0)
            self._hb_thread = None
            self._hb_stop = None
        if _harvester:
            _harvester.stop()
        if _community:
            _community.stop()
        log.info("[%s] Stopped.", self.SERVICE_NAME)

    def stats(self) -> dict[str, Any]:
        return get_swarm_stats()


# ── Standalone entry ──────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="| HATAN INDUSTRIES | %(asctime)s | %(levelname)s | %(message)s",
    )
    engine = IsraeliSwarmEngine()
    engine.start()
    try:
        while True:
            time.sleep(60)
            stats = engine.stats()
            print(
                f"[SWARM] sessions={stats['total_sessions']}  "
                f"sent={stats['messages_sent']}  "
                f"joined={stats['bots_joined']}  "
                f"verified={stats['verified']}  "
                f"status={stats['conversation_status']}",
                flush=True,
            )
    except (KeyboardInterrupt, SystemExit):
        engine.stop()


if __name__ == "__main__":
    main()
