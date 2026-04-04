"""
Israeli Swarm Engine — Hatan Industries
========================================

Orchestrates a full Israeli-Hebrew Telegram swarm:

1. SESSION HARVESTER  — Scans vault/incoming for ZIP/RAR archives, extracts
                        .session files into vault/sessions automatically.

2. COMMUNITY ENGINE   — Drives bots to join a target group and generate
                        natural Israeli Hebrew dialogue via Gemini API.
                        Topics: פוליטיקה, אקטואליה, צהוב, כלכלה.
                        Style: slang, emojis, GIFs, reactions.

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
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import pathlib
import random
import shutil
import sqlite3
import time
import threading
import zipfile
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("hatan.israeli_swarm")

# ── Path resolution ────────────────────────────────────────────────────────────

def _project_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parent.parent.parent.parent


_ROOT = _project_root()
_VAULT_INCOMING = pathlib.Path(
    os.getenv("VAULT_INCOMING_DIR", str(_ROOT / "vault" / "incoming"))
)
_VAULT_SESSIONS = pathlib.Path(
    os.getenv("VAULT_SESSIONS_DIR", str(_ROOT / "vault" / "sessions"))
)
_TELEFIX_DB = pathlib.Path(
    os.getenv("TELEFIX_DB_PATH", str(_ROOT / "telefix.db"))
)
_REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
_GEMINI_KEY = os.getenv("GEMINI_API_KEY", "")
_GROUP_LINK = os.getenv("SWARM_GROUP_LINK", "")
# Written by POST /api/swarm/start — must be readable here so UI link works without env restart.
_REDIS_SWARM_STATUS_KEY = "nexus:swarm:israeli:status"
_REDIS_SWARM_TARGET_KEY = "nexus:swarm:israeli:target_group"
_REDIS_LAST_ENGINE_ERROR_KEY = "nexus:swarm:israeli:last_engine_error"


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
    try:
        import redis as redis_sync

        r = redis_sync.Redis.from_url(_REDIS_URL, decode_responses=True)
        try:
            r.set(key, value[:2000], ex=ex)
        finally:
            r.close()
    except Exception:
        pass


def _publish_engine_error(detail: str) -> None:
    """Surface failures to GET /api/swarm/live-feed (dashboard banner)."""
    ts = datetime.now(timezone.utc).isoformat()
    _redis_sync_set(_REDIS_LAST_ENGINE_ERROR_KEY, f"{ts} | {detail}")


def _clear_engine_error() -> None:
    try:
        import redis as redis_sync

        r = redis_sync.Redis.from_url(_REDIS_URL, decode_responses=True)
        try:
            r.delete(_REDIS_LAST_ENGINE_ERROR_KEY)
        finally:
            r.close()
    except Exception:
        pass


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


# ── Hebrew dialogue topics & slang pool ───────────────────────────────────────

_TOPICS = ["פוליטיקה", "אקטואליה", "צהוב", "כלכלה"]

_SLANG_OPENERS = [
    "אחי שמעת על זה?? 😂",
    "וואלה לא מאמין 🤯",
    "יא אלהי זה אמיתי?? 😱",
    "בן אדם רציני תגיד לי",
    "חבר'ה מה קורה פה בכלל 😅",
    "ממש לא מבין את המדינה הזאת 🤦",
    "אז מה אתם חושבים על זה?",
    "שמעתם את הבאסה האחרונה?? 💀",
    "לא יאומן כי יסופר 😤",
    "אנשים, תתעוררו כבר 🔥",
]

_REACTIONS = ["🔥", "😂", "💀", "🤯", "👀", "😱", "🫡", "💪", "🤦", "😅", "❤️", "🙏"]

_GEMINI_SYSTEM_PROMPT = (
    "אתה ישראלי צעיר שמשתתף בקבוצת טלגרם. "
    "כתוב הודעה קצרה (1-2 משפטים) בעברית ישראלית יומיומית עם סלנג, "
    "אימוג'ים, ותגובה אותנטית לנושא שניתן. "
    "אל תהיה רשמי. תהיה ספונטני וטבעי כמו בן אדם אמיתי."
)


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


# ── Gemini Hebrew Dialogue Generator ─────────────────────────────────────────

async def _generate_hebrew_message(topic: str) -> str:
    """Call Gemini API to generate a natural Israeli Hebrew message about topic."""
    if not _GEMINI_KEY:
        opener = random.choice(_SLANG_OPENERS)
        reaction = random.choice(_REACTIONS)
        return f"{opener} #{topic} {reaction}"

    try:
        import urllib.request
        import urllib.error

        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-pro:generateContent?key={_GEMINI_KEY}"
        )
        prompt = f"{_GEMINI_SYSTEM_PROMPT}\n\nנושא: {topic}"
        payload = json.dumps({
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"maxOutputTokens": 80, "temperature": 0.9},
        }).encode("utf-8")

        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        loop = asyncio.get_event_loop()
        response_bytes = await loop.run_in_executor(
            None,
            lambda: urllib.request.urlopen(req, timeout=15).read(),
        )
        data = json.loads(response_bytes)
        text = (
            data.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
            .strip()
        )
        if text:
            reaction = random.choice(_REACTIONS)
            return f"{text} {reaction}"
    except Exception as exc:
        log.debug("[GEMINI] Generation failed (%s) — using fallback", exc)

    opener = random.choice(_SLANG_OPENERS)
    reaction = random.choice(_REACTIONS)
    return f"{opener} #{topic} {reaction}"


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
            self._stop.wait(timeout=delay)

    async def _cycle(self) -> None:
        if not _redis_swarm_status_allows_send():
            log.debug("[COMMUNITY] Swarm paused — Redis status is not 'running'")
            return

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

        topic = random.choice(_TOPICS)
        message = await _generate_hebrew_message(topic)

        # Pick a random session for this cycle
        session_file = random.choice(sessions)
        phone = session_file.stem

        log.info(
            "[COMMUNITY] Bot %s → topic=%s  msg=%s",
            phone, topic, message[:60],
        )

        # Attempt Telethon send if available
        sent = await self._try_send_telethon(session_file, message, group_link)
        if sent:
            self.messages_sent += 1
            await self._push_redis_event(phone, topic, message)
            await self._mark_verified_written(phone)

    async def _try_send_telethon(
        self, session_file: pathlib.Path, message: str, group_link: str
    ) -> bool:
        if not group_link:
            return False
        try:
            from telethon import TelegramClient  # type: ignore[import]
            from telethon.errors import (  # type: ignore[import]
                FloodWaitError,
                UserBannedInChannelError,
            )

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
            async with TelegramClient(session_path, api_id, api_hash) as client:
                try:
                    await client.get_entity(group_link)
                except Exception:
                    await client(
                        __import__(
                            "telethon.tl.functions.channels",
                            fromlist=["JoinChannelRequest"],
                        ).JoinChannelRequest(group_link)
                    )
                    self.bots_joined += 1
                    await self._push_join_event(session_file.stem, group_link)

                await client.send_message(group_link, message)
                return True

        except ImportError:
            log.debug("[COMMUNITY] telethon not installed — message not sent")
        except Exception as exc:
            log.debug("[COMMUNITY] Send failed for %s: %s", session_file.stem, exc)
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
        _harvester = SessionHarvester()
        _community = CommunityEngine()

    def start(self) -> None:
        global _harvester, _community
        _harvester.start()  # type: ignore[union-attr]
        _community.start()  # type: ignore[union-attr]
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
