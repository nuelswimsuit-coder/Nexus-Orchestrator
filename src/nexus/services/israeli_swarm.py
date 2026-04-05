"""
Israeli Swarm Engine — Hatan Industries
========================================

Orchestrates a full Israeli-Hebrew Telegram swarm:

1. SESSION HARVESTER  — Scans vault/incoming for ZIP/RAR archives, extracts
                        .session files into vault/sessions automatically.

2. COMMUNITY ENGINE   — Drives bots to join a target group and generate
                        contextual Israeli Hebrew chat (news-community tone) via Gemini.
                        Reads recent messages so bots reply like humans; no hashtag spam.
                        Optional: SWARM_UPDATE_PROFILES sets Israeli display names + avatars.

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
import tempfile
import time
import threading
import zipfile
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("hatan.israeli_swarm")
_HEARTBEAT_REDIS_WARNED = False
_AGENT_FD2E46_HB_OK = False
_AGENT_FD2E46_HB_FAIL = False

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
    global _HEARTBEAT_REDIS_WARNED, _AGENT_FD2E46_HB_OK, _AGENT_FD2E46_HB_FAIL
    try:
        import redis as redis_sync

        r = redis_sync.Redis.from_url(_REDIS_URL, decode_responses=True)
        try:
            r.set(key, value[:2000], ex=ex)
            if key == _ISRAELI_HEARTBEAT_KEY and not _AGENT_FD2E46_HB_OK:
                _AGENT_FD2E46_HB_OK = True
                # #region agent log
                try:
                    import json as _json
                    from pathlib import Path as _Path
                    from urllib.parse import urlparse as _urlparse

                    _root = _Path(__file__).resolve().parents[3]
                    _host = _urlparse(_REDIS_URL).hostname or "?"
                    _line = (
                        _json.dumps(
                            {
                                "sessionId": "fd2e46",
                                "timestamp": int(time.time() * 1000),
                                "location": "israeli_swarm.py:_redis_sync_set",
                                "message": "heartbeat_redis_set_ok",
                                "hypothesisId": "H1-H2",
                                "data": {"redis_host": _host, "key": key},
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    with open(_root / "debug-fd2e46.log", "a", encoding="utf-8") as _df:
                        _df.write(_line)
                except Exception:
                    pass
                # #endregion
        finally:
            r.close()
    except Exception as exc:
        if key == _ISRAELI_HEARTBEAT_KEY and not _AGENT_FD2E46_HB_FAIL:
            _AGENT_FD2E46_HB_FAIL = True
            # #region agent log
            try:
                import json as _json
                from pathlib import Path as _Path
                from urllib.parse import urlparse as _urlparse

                _root = _Path(__file__).resolve().parents[3]
                _host = _urlparse(_REDIS_URL).hostname or "?"
                _line = (
                    _json.dumps(
                        {
                            "sessionId": "fd2e46",
                            "timestamp": int(time.time() * 1000),
                            "location": "israeli_swarm.py:_redis_sync_set",
                            "message": "heartbeat_redis_set_fail",
                            "hypothesisId": "H2",
                            "data": {
                                "redis_host": _host,
                                "exc_type": type(exc).__name__,
                                "exc_detail": str(exc)[:240],
                            },
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                with open(_root / "debug-fd2e46.log", "a", encoding="utf-8") as _df:
                    _df.write(_line)
            except Exception:
                pass
            # #endregion
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
    "נשמע לי מוגזם קצת, יש לכם מקור על זה?",
    "אני חושב שצריך לחכות לעוד פרטים לפני שמספקים.",
    "מסכים חלקית — תלוי איך זה יתפתח בשבוע הקרוב.",
    "מישהו ראה את זה גם בכתבה אחרת או שזה רק כאן?",
    "לא בטוח שזה משקף את המציאות, נראה לי שחסר קונטקסט.",
    "וואי, אם זה נכון זה משנה את התמונה לגמרי.",
    "בעיקרון זה מה שדיברו עליו אתמול בתכנית, לא?",
    "אני פחות מכיר את הנושא — מישהו יכול לפרק את זה בקצרה?",
    "יש פה כמה נקודות טובות, אבל גם נקודה שמפריעה לי.",
    "תכל'ס, מה זה אומר בשבילנו בפועל?",
]


def _swarm_identity_path(stem: str) -> pathlib.Path:
    return _VAULT_SESSIONS / f"{stem}.swarm_identity.json"


def _identity_from_stem(stem: str) -> dict[str, Any]:
    h = hashlib.sha256(stem.encode("utf-8")).digest()
    fi = h[0] % len(_ISRAELI_FIRST_NAMES)
    li = h[1] % len(_ISRAELI_LAST_NAMES)
    return {
        "first_name": _ISRAELI_FIRST_NAMES[fi],
        "last_name": _ISRAELI_LAST_NAMES[li],
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


def _download_file_sync(url: str, dest: pathlib.Path) -> bool:
    try:
        import urllib.request

        urllib.request.urlretrieve(url, str(dest))
        return dest.exists() and dest.stat().st_size > 256
    except Exception:
        return False


async def _apply_swarm_identity(client: Any, stem: str) -> None:
    if not _swarm_profiles_enabled():
        return
    ident = _load_or_create_swarm_identity(stem)
    if ident.get("profile_applied"):
        return
    fn = str(ident.get("first_name") or "").strip() or "חבר"
    ln = str(ident.get("last_name") or "").strip()
    seed = str(ident.get("avatar_seed") or stem).strip() or stem
    from urllib.parse import quote

    avatar_url = f"https://i.pravatar.cc/512?u={quote(seed, safe='')}"

    try:
        from telethon.tl.functions.account import UpdateProfileRequest  # type: ignore[import]
        from telethon.tl.functions.photos import UploadProfilePhotoRequest  # type: ignore[import]

        await client(UpdateProfileRequest(first_name=fn[:64], last_name=ln[:64]))
    except Exception as exc:
        log.warning("[COMMUNITY] Profile name update failed for %s: %s", stem, exc)
        return

    tmp_path: pathlib.Path | None = None
    try:
        fd, tmp = tempfile.mkstemp(suffix=".jpg")
        os.close(fd)
        tmp_path = pathlib.Path(tmp)
        loop = asyncio.get_event_loop()
        ok = await loop.run_in_executor(
            None,
            lambda: _download_file_sync(avatar_url, tmp_path),  # type: ignore[misc]
        )
        if ok and tmp_path is not None:
            from telethon.tl.functions.photos import UploadProfilePhotoRequest  # type: ignore[import]

            file = await client.upload_file(str(tmp_path))
            await client(UploadProfilePhotoRequest(file=file))
        else:
            log.debug("[COMMUNITY] Avatar download skipped/failed for %s", stem)
    except Exception as exc:
        log.warning("[COMMUNITY] Profile photo update failed for %s: %s", stem, exc)
    finally:
        if tmp_path is not None:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass

    ident["profile_applied"] = True
    _save_swarm_identity(stem, ident)


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
        for m in reversed([x for x in msgs if x]):
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


async def _generate_community_message(
    transcript: str,
    meta_newest_first: list[dict[str, Any]],
    speaker_first: str,
    speaker_last: str,
) -> tuple[str, int | None]:
    """
    One short Hebrew group line; optional reply_to_id must appear in meta.
    """
    allowed_ids = {int(x["id"]) for x in meta_newest_first if x.get("id") is not None}
    angle = random.choice(_NEWS_ANGLES)
    display = f"{speaker_first} {speaker_last}".strip()

    if not _GEMINI_KEY:
        text = random.choice(_FALLBACK_CHAT_LINES)
        reply_id: int | None = None
        if meta_newest_first and random.random() < 0.65:
            reply_id = int(meta_newest_first[0]["id"])
        return _strip_hashtags_and_cleanup(text), reply_id

    sys_prompt = (
        "אתה משתתף בקבוצת טלגרם ישראלית על חדשות ואקטואליה. "
        "כתוב הודעה אחת קצרה (עד 2–3 משפטים) בעברית מדוברת, טבעית, כמו בני אדם — "
        "אפשר להסכים, להתווכח, לשאול שאלת המשך, או להוסיף פרט/ספק. "
        "חובה: להתייחס לתוכן מהצ'אט האחרון כשיש (לא לדבר לריק). "
        "אסור: האשטגים (#), קישורים מומצאים, 'כבוט', או ניסוח שיווקי. "
        "אימוג'י — לכל היותר אחד, רק אם זה באמת מתאים. "
        'החזר אך ורק JSON תקין: {"text":"...","reply_to_id":null או מספר שלם} '
        "כאשר reply_to_id הוא מזהה הודעה מהרשימה בלבד אם אתה משיב ישירות למישהו, אחרת null."
    )
    user_obj = {
        "group_theme": "קהילת חדשות ישראל — דיון אקטואלי",
        "angle_hint": angle,
        "your_display_name": display,
        "recent_chat_chronological": (transcript or "(אין הודעות אחרונות — תפתח נושא אקטואלי עדין)")[
            -5500:
        ],
        "message_ids_newest_first": meta_newest_first[:20],
    }
    user_payload = json.dumps(user_obj, ensure_ascii=False)

    try:
        import urllib.request

        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-pro:generateContent?key={_GEMINI_KEY}"
        )
        prompt = f"{sys_prompt}\n\nהקשר JSON:\n{user_payload}"
        body = json.dumps(
            {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": 200, "temperature": 0.88},
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
        text = _strip_hashtags_and_cleanup(str(parsed.get("text") or "").strip())
        rid = parsed.get("reply_to_id")
        reply_to: int | None = None
        if rid is not None and str(rid).strip().lstrip("-").isdigit():
            cand = int(rid)
            if cand in allowed_ids:
                reply_to = cand
        if text:
            return text, reply_to
    except Exception as exc:
        log.debug("[GEMINI] Community message failed (%s) — fallback", exc)

    text = random.choice(_FALLBACK_CHAT_LINES)
    reply_id_fb = int(meta_newest_first[0]["id"]) if meta_newest_first else None
    return _strip_hashtags_and_cleanup(text), reply_id_fb


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

    MIN_DELAY_S = 45
    MAX_DELAY_S = 180

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
            try:
                asyncio.run(self._cycle())
            except Exception as exc:
                log.warning("[COMMUNITY] Cycle error: %s", exc)
            delay = random.randint(self.MIN_DELAY_S, self.MAX_DELAY_S)
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

            used_stems: set[str] = set()
            any_sent = False
            has_api = bool(
                int(os.getenv("TELEGRAM_API_ID", "0") or os.getenv("TELEFIX_API_ID", "0") or "0")
            )
            for _attempt in range(per):
                pool = [s for s in sessions if s.stem not in used_stems] or sessions
                session_file = random.choice(pool)
                used_stems.add(session_file.stem)
                phone = session_file.stem
                ident = _load_or_create_swarm_identity(phone)
                sf = str(ident.get("first_name") or "")
                sl = str(ident.get("last_name") or "")
                message, reply_to = await _generate_community_message(
                    transcript, meta_nf, sf, sl
                )
                log.info(
                    "[COMMUNITY] Bot %s → msg=%s reply_to=%s",
                    phone, message[:60], reply_to,
                )
                sent = await self._try_send_telethon(
                    session_file, message, group_link, reply_to=reply_to
                )
                if sent:
                    any_sent = True
                    self.messages_sent += 1
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
    ) -> bool:
        if not group_link:
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
                msg = "חסר TELEGRAM_API_ID/HASH או TELEFIX_API_ID/HASH ב-.env"
                log.warning("[COMMUNITY] %s", msg)
                _publish_engine_error(msg)
                return False

            session_path = str(session_file.with_suffix(""))

            async def _run_client() -> bool:
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
                        return False
                    try:
                        await _apply_swarm_identity(client, session_file.stem)
                        target = await _ensure_swarm_target_entity(client, group_link)
                        await client.send_message(
                            target, message, reply_to=reply_to if reply_to else None
                        )
                        return True
                    except errors.ChatWriteForbiddenError as cw_exc:
                        detail = (
                            f"{session_file.stem}: אין הרשאת כתיבה בצ'אט (אולי ערוץ הכרזות או חסימת משתמשים). "
                            f"({cw_exc})"
                        )
                        log.warning("[COMMUNITY] %s", detail)
                        _publish_engine_error(detail[:500])
                        return False
                finally:
                    await client.disconnect()

            try:
                return await asyncio.wait_for(_run_client(), timeout=timeout_s)
            except asyncio.TimeoutError:
                detail = f"{session_file.stem}: Telethon timeout {timeout_s:.0f}s"
                log.warning("[COMMUNITY] %s", detail)
                _publish_engine_error(detail)
                _rpush_feed_line(f"[מנוע] {detail}", "engine")
                return False

        except ImportError:
            msg = "חבילת telethon לא מותקנת בסביבת israeli-swarm"
            log.warning("[COMMUNITY] %s", msg)
            _publish_engine_error(msg)
        except Exception as exc:
            err = f"{session_file.stem}: {exc}"
            log.warning("[COMMUNITY] Send failed — %s", err)
            _publish_engine_error(err[:500])
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
        payload = json.dumps({
            "ts": ts,
            "phone": phone,
            "topic": topic,
            "message": message,
            "verified": 1,
            "written": 1,
            "engine": "israeli_swarm",
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
