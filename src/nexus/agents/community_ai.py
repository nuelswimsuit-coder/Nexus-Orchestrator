"""
Israeli Swarm Community Engine — IsraeliSwarm
=============================================

Manages a swarm of Telegram bots that join a target group and generate
natural Hebrew content using Gemini/LLM.  Designed to mimic organic
Israeli community activity with randomised delays, slang, emojis, and GIFs.

Usage
-----
    from src.nexus.agents.community_ai import IsraeliSwarm

    swarm = IsraeliSwarm(
        target_group="https://t.me/some_group",
        session_dir="vault/sessions",
    )
    swarm.start()   # non-blocking daemon thread
    swarm.stop()    # graceful shutdown

Environment variables
---------------------
GEMINI_API_KEY          — Google Gemini API key (required for LLM generation)
SWARM_MIN_DELAY_S       — minimum delay between messages in seconds (default 120)
SWARM_MAX_DELAY_S       — maximum delay between messages in seconds (default 900)
SWARM_MAX_ACTIVE_BOTS   — max concurrent active talkers (default 5)
SWARM_TARGET_GROUP      — default target group link
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import shutil
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── Hebrew content topics ─────────────────────────────────────────────────────

_TOPICS = ["חדשות", "פוליטיקה", "צהוב", "כלכלה"]

_TOPIC_PROMPTS: dict[str, str] = {
    "חדשות": (
        "כתוב הודעה קצרה (1-3 משפטים) בעברית ישראלית יומיומית על חדשות עדכניות. "
        "השתמש בסלנג ישראלי, אמוג'י, ותגובות טבעיות. אל תציין שאתה בוט."
    ),
    "פוליטיקה": (
        "כתוב הודעה קצרה (1-3 משפטים) בעברית ישראלית יומיומית על פוליטיקה ישראלית. "
        "הבע דעה אישית חזקה עם סלנג ישראלי ואמוג'י. אל תציין שאתה בוט."
    ),
    "צהוב": (
        "כתוב הודעה קצרה (1-3 משפטים) בעברית ישראלית יומיומית על גוסיפ, "
        "סלבריטאים, או ידיעות צהובות. השתמש בסלנג ישראלי ואמוג'י. אל תציין שאתה בוט."
    ),
    "כלכלה": (
        "כתוב הודעה קצרה (1-3 משפטים) בעברית ישראלית יומיומית על כלכלה, "
        "מחירים, או שוק ההון. השתמש בסלנג ישראלי ואמוג'י. אל תציין שאתה בוט."
    ),
}

_FALLBACK_MESSAGES: dict[str, list[str]] = {
    "חדשות": [
        "שמעתם מה קרה היום? 😱 הכל הפוך פה",
        "אחי, החדשות היום מטורפות לגמרי 🤯",
        "מישהו עוקב אחרי מה שקורה? 👀 מטורף",
    ],
    "פוליטיקה": [
        "הממשלה שוב עם השטויות שלה 🙄 מתי זה ייגמר",
        "אחי הפוליטיקאים האלה... אין מילים 😤",
        "מה אתם חושבים על מה שקרה היום בכנסת? 🏛️",
    ],
    "צהוב": [
        "ראיתם מה הסלב הזה עשה?? 😂 מת",
        "הגוסיפ של היום 🔥 תשמעו את זה...",
        "אחי זה ממש לא נורמלי מה שהם עושים 🤣",
    ],
    "כלכלה": [
        "המחירים עולים שוב 😭 אי אפשר לחיות",
        "הדולר היום... מישהו רואה? 📈",
        "שוק ההון מטורף היום, מישהו מבין מה קורה? 💸",
    ],
}

_EMOJIS = ["😂", "🔥", "👀", "😱", "🤯", "💪", "🙄", "😤", "✅", "❤️", "🇮🇱", "💸", "📈"]

_REACTIONS = ["👍", "❤️", "🔥", "🎉", "😂", "👏", "💯"]


# ── Session extractor (ZIP / RAR from vault/incoming) ─────────────────────────

def _find_project_root() -> Path:
    here = Path(os.path.dirname(os.path.abspath(__file__)))
    root = here
    for _ in range(8):
        if (root / "vault").exists() or (root / ".git").exists():
            return root
        root = root.parent
    return here


def extract_incoming_sessions(
    incoming_dir: Path | None = None,
    session_dir: Path | None = None,
) -> int:
    """
    Extract ``.session`` files from any ZIP or RAR archives found in
    ``vault/incoming`` and copy them to ``vault/sessions``.

    Returns the number of session files extracted.
    """
    root = _find_project_root()
    if incoming_dir is None:
        incoming_dir = root / "vault" / "incoming"
    if session_dir is None:
        session_dir = root / "vault" / "sessions"

    incoming_dir.mkdir(parents=True, exist_ok=True)
    session_dir.mkdir(parents=True, exist_ok=True)

    extracted = 0
    archives = list(incoming_dir.glob("*.zip")) + list(incoming_dir.glob("*.rar"))

    for archive in archives:
        try:
            if archive.suffix.lower() == ".zip":
                import zipfile
                with zipfile.ZipFile(archive, "r") as zf:
                    for member in zf.namelist():
                        if member.endswith(".session"):
                            dest = session_dir / Path(member).name
                            if not dest.exists():
                                with zf.open(member) as src, open(dest, "wb") as dst:
                                    shutil.copyfileobj(src, dst)
                                extracted += 1
                                logger.info("Extracted session from ZIP: %s → %s", member, dest)
            elif archive.suffix.lower() == ".rar":
                try:
                    import rarfile  # type: ignore[import]
                    with rarfile.RarFile(str(archive)) as rf:
                        for member in rf.namelist():
                            if member.endswith(".session"):
                                dest = session_dir / Path(member).name
                                if not dest.exists():
                                    rf.extract(member, path=str(session_dir))
                                    extracted_path = session_dir / member
                                    if extracted_path != dest and extracted_path.exists():
                                        shutil.move(str(extracted_path), str(dest))
                                    extracted += 1
                                    logger.info("Extracted session from RAR: %s → %s", member, dest)
                except ImportError:
                    logger.warning("rarfile not installed — skipping %s (pip install rarfile)", archive.name)
        except Exception as exc:
            logger.warning("Failed to extract %s: %s", archive.name, exc)

    if extracted:
        logger.info("[IsraeliSwarm] Extracted %d new session(s) from vault/incoming", extracted)
    return extracted


# ── Telefix DB stats ───────────────────────────────────────────────────────────

def get_telefix_swarm_stats(db_path: Path | None = None) -> dict[str, Any]:
    """
    Read ``telefix.db`` and return live swarm stats for the Dashboard.

    Returns a dict with:
      - ``verified``: count of rows with verified=1 (or status='verified')
      - ``written``: count of rows with a non-empty message/content column
      - ``total``: total row count
      - ``db_found``: bool
    """
    root = _find_project_root()
    if db_path is None:
        for candidate in (
            root / "telefix.db",
            Path(os.environ.get("TELEFIX_DB_PATH", "")) if os.environ.get("TELEFIX_DB_PATH") else None,
        ):
            if candidate is not None and candidate.exists():
                db_path = candidate
                break

    if db_path is None or not db_path.exists():
        return {"verified": 0, "written": 0, "total": 0, "db_found": False}

    try:
        conn = sqlite3.connect(str(db_path), timeout=5, check_same_thread=False)
        cur = conn.cursor()
        stats: dict[str, Any] = {"db_found": True}

        # Try swarm_messages table first (written by IsraeliSwarm)
        try:
            cur.execute("SELECT COUNT(*) FROM swarm_messages")
            stats["total"] = cur.fetchone()[0] or 0
            try:
                cur.execute("SELECT COUNT(*) FROM swarm_messages WHERE verified=1 OR status='verified'")
                stats["verified"] = cur.fetchone()[0] or 0
            except sqlite3.OperationalError:
                stats["verified"] = stats["total"]
            try:
                cur.execute(
                    "SELECT COUNT(*) FROM swarm_messages WHERE message IS NOT NULL AND message != ''"
                )
                stats["written"] = cur.fetchone()[0] or 0
            except sqlite3.OperationalError:
                stats["written"] = stats["total"]
        except sqlite3.OperationalError:
            # Fall back to generic groups / telefix table
            try:
                cur.execute("SELECT COUNT(*) FROM groups")
                stats["total"] = cur.fetchone()[0] or 0
                stats["verified"] = stats["total"]
                stats["written"] = 0
            except sqlite3.OperationalError:
                try:
                    cur.execute("SELECT COUNT(*) FROM telefix")
                    stats["total"] = cur.fetchone()[0] or 0
                    stats["verified"] = stats["total"]
                    stats["written"] = 0
                except sqlite3.OperationalError:
                    stats.update({"total": 0, "verified": 0, "written": 0})
        conn.close()
        return stats
    except Exception as exc:
        logger.debug("telefix stats read error: %s", exc)
        return {"verified": 0, "written": 0, "total": 0, "db_found": False}


# ── Swarm state ───────────────────────────────────────────────────────────────

@dataclass
class SwarmBotState:
    session_file: str
    phone: str
    machine_id: str
    is_active: bool = False
    last_message_ts: float = 0.0
    last_message_text: str = ""
    messages_sent: int = 0
    joined_group: bool = False
    is_king: bool = False


@dataclass
class SwarmLiveFeed:
    """Live feed data exposed to the UI."""
    total_in_group: int = 0
    active_talkers: int = 0
    last_message: str = ""
    last_message_ts: float = 0.0
    last_sender_phone: str = ""
    is_running: bool = False
    bots: list[dict[str, Any]] = field(default_factory=list)


# ── LLM content generator ─────────────────────────────────────────────────────

def _generate_hebrew_message_gemini(topic: str, api_key: str) -> str | None:
    """Call Gemini API to generate a Hebrew message about the given topic."""
    try:
        import urllib.request

        prompt = _TOPIC_PROMPTS.get(topic, _TOPIC_PROMPTS["חדשות"])
        payload = json.dumps({
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.9,
                "maxOutputTokens": 150,
            },
        }).encode("utf-8")

        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-pro:generateContent?key={api_key}"
        )
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        candidates = data.get("candidates", [])
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [])
            if parts:
                text = parts[0].get("text", "").strip()
                if text:
                    return text
    except Exception as exc:
        logger.debug("Gemini generation failed: %s", exc)
    return None


def _pick_fallback_message(topic: str) -> str:
    msgs = _FALLBACK_MESSAGES.get(topic, _FALLBACK_MESSAGES["חדשות"])
    return random.choice(msgs)


def generate_hebrew_message(topic: str | None = None) -> str:
    """Generate a Hebrew message for the given topic (or a random one)."""
    if topic is None:
        topic = random.choice(_TOPICS)
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if api_key:
        msg = _generate_hebrew_message_gemini(topic, api_key)
        if msg:
            return msg
    return _pick_fallback_message(topic)


# ── IsraeliSwarm ──────────────────────────────────────────────────────────────

class IsraeliSwarm:
    """
    Swarm engine that manages Telegram bots joining a target group and
    generating natural Hebrew content.

    Parameters
    ----------
    target_group:
        Telegram group invite link or username (e.g. ``https://t.me/+abc123``).
    session_dir:
        Path to the directory containing ``.session`` files.
    min_delay_s:
        Minimum delay between messages per bot (seconds).
    max_delay_s:
        Maximum delay between messages per bot (seconds).
    max_active_bots:
        Maximum number of bots talking concurrently.
    king_machine_id:
        Machine ID that receives the KING badge (default: ``Jacob-PC``).
    redis_url:
        Redis URL for publishing live feed data.
    """

    SERVICE_NAME = "IsraeliSwarm"

    def __init__(
        self,
        target_group: str | None = None,
        session_dir: str | Path | None = None,
        min_delay_s: int | None = None,
        max_delay_s: int | None = None,
        max_active_bots: int | None = None,
        king_machine_id: str = "Jacob-PC",
        redis_url: str | None = None,
    ) -> None:
        self.target_group = (
            target_group
            or os.getenv("SWARM_TARGET_GROUP", "").strip()
            or ""
        )
        if session_dir is None:
            _here = Path(os.path.dirname(os.path.abspath(__file__)))
            _root = _here
            for _ in range(6):
                if (_root / "vault").exists() or (_root / ".git").exists():
                    break
                _root = _root.parent
            session_dir = _root / "vault" / "sessions"
        self.session_dir = Path(session_dir).resolve()
        self.min_delay_s = int(
            min_delay_s or os.getenv("SWARM_MIN_DELAY_S", "120")
        )
        self.max_delay_s = int(
            max_delay_s or os.getenv("SWARM_MAX_DELAY_S", "900")
        )
        self.max_active_bots = int(
            max_active_bots or os.getenv("SWARM_MAX_ACTIVE_BOTS", "5")
        )
        self.king_machine_id = king_machine_id
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self.live_feed = SwarmLiveFeed()
        self._bots: list[SwarmBotState] = []

        # Auto-extract sessions from vault/incoming on init
        _root = _find_project_root()
        self._incoming_dir = _root / "vault" / "incoming"
        self._telefix_db: Path | None = None
        for _candidate in (
            _root / "telefix.db",
            Path(os.environ.get("TELEFIX_DB_PATH", "")) if os.environ.get("TELEFIX_DB_PATH") else None,
        ):
            if _candidate is not None and _candidate.exists():
                self._telefix_db = _candidate
                break

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the swarm loop in a background daemon thread."""
        if self._thread and self._thread.is_alive():
            logger.info("[%s] Already running.", self.SERVICE_NAME)
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name=self.SERVICE_NAME,
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "[%s] Started — target=%s session_dir=%s min_delay=%ds max_delay=%ds",
            self.SERVICE_NAME,
            self.target_group or "(not set)",
            self.session_dir,
            self.min_delay_s,
            self.max_delay_s,
        )
        print(
            f"[{self.SERVICE_NAME}] Started — target={self.target_group or '(not set)'} "
            f"session_dir={self.session_dir}",
            flush=True,
        )

    def stop(self) -> None:
        """Signal the swarm to stop and wait for the thread to exit."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=15)
        with self._lock:
            self.live_feed.is_running = False
        print(f"[{self.SERVICE_NAME}] Stopped.", flush=True)

    def get_live_feed(self) -> dict[str, Any]:
        """Return the current live feed snapshot as a JSON-serialisable dict."""
        with self._lock:
            return {
                "total_in_group": self.live_feed.total_in_group,
                "active_talkers": self.live_feed.active_talkers,
                "last_message": self.live_feed.last_message,
                "last_message_ts": self.live_feed.last_message_ts,
                "last_sender_phone": self.live_feed.last_sender_phone,
                "is_running": self.live_feed.is_running,
                "bots": [
                    {
                        "phone": b.phone,
                        "machine_id": b.machine_id,
                        "is_active": b.is_active,
                        "messages_sent": b.messages_sent,
                        "last_message": b.last_message_text,
                        "is_king": b.is_king,
                    }
                    for b in self._bots
                ],
            }

    # ── Internal loop ─────────────────────────────────────────────────────────

    def _load_sessions(self) -> list[SwarmBotState]:
        """Discover .session files and build SwarmBotState list."""
        bots: list[SwarmBotState] = []
        if not self.session_dir.exists():
            return bots
        for sf in sorted(self.session_dir.glob("*.session")):
            phone = sf.stem
            machine_id = self._infer_machine_id(sf)
            is_king = machine_id == self.king_machine_id
            bots.append(
                SwarmBotState(
                    session_file=str(sf),
                    phone=phone,
                    machine_id=machine_id,
                    is_king=is_king,
                )
            )
        return bots

    def _infer_machine_id(self, session_path: Path) -> str:
        """Try to read machine_id from companion JSON; fallback to hostname."""
        companion = session_path.with_suffix(".json")
        if companion.exists():
            try:
                data = json.loads(companion.read_text(encoding="utf-8"))
                mid = (
                    data.get("machine_id")
                    or data.get("origin_machine")
                    or data.get("node_id")
                    or ""
                )
                if mid:
                    return str(mid)
            except Exception:
                pass
        import socket
        return socket.gethostname()

    def _publish_redis(self) -> None:
        """Push live feed snapshot to Redis for the UI."""
        try:
            import redis as _redis  # type: ignore[import]
            client = _redis.from_url(
                self.redis_url, decode_responses=True, socket_connect_timeout=2
            )
            payload = json.dumps(self.get_live_feed(), ensure_ascii=False)
            client.set("nexus:swarm:live_feed", payload, ex=60)
            client.close()
        except Exception:
            pass

    def _send_message(self, bot: SwarmBotState, message: str) -> bool:
        """
        Attempt to send a message via Telethon using the bot's session file.
        Returns True on success.  Falls back gracefully if Telethon is not
        installed or the session is invalid.
        """
        if not self.target_group:
            logger.debug("[%s] No target group set — skipping send.", self.SERVICE_NAME)
            return False
        try:
            from telethon.sync import TelegramClient  # type: ignore[import]
            from telethon.sessions import StringSession  # type: ignore[import]

            api_id_str = os.getenv("TELEGRAM_API_ID", "").strip()
            api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
            if not api_id_str or not api_hash:
                logger.debug("[%s] TELEGRAM_API_ID/HASH not set.", self.SERVICE_NAME)
                return False
            api_id = int(api_id_str)

            with TelegramClient(bot.session_file, api_id, api_hash) as client:
                if not bot.joined_group:
                    try:
                        client.join_chat(self.target_group)
                        bot.joined_group = True
                        logger.info(
                            "[%s] %s joined group %s",
                            self.SERVICE_NAME, bot.phone, self.target_group,
                        )
                        # Immediately refresh verified/written counters on join
                        self._publish_db_stats()
                    except Exception as join_exc:
                        logger.debug(
                            "[%s] Join failed for %s: %s",
                            self.SERVICE_NAME, bot.phone, join_exc,
                        )
                client.send_message(self.target_group, message)
            return True
        except ImportError:
            logger.debug("[%s] Telethon not installed — simulating send.", self.SERVICE_NAME)
            return True
        except Exception as exc:
            logger.debug(
                "[%s] Send failed for %s: %s", self.SERVICE_NAME, bot.phone, exc
            )
            return False

    def _publish_db_stats(self) -> None:
        """Push telefix.db swarm stats (verified/written counts) to Redis."""
        stats = get_telefix_swarm_stats(self._telefix_db)
        try:
            import redis as _redis  # type: ignore[import]
            client = _redis.from_url(
                self.redis_url, decode_responses=True, socket_connect_timeout=2
            )
            client.set("nexus:swarm:db_stats", json.dumps(stats, ensure_ascii=False), ex=120)
            client.set("nexus:swarm:verified_count", str(stats.get("verified", 0)), ex=120)
            client.set("nexus:swarm:written_count", str(stats.get("written", 0)), ex=120)
            client.close()
        except Exception:
            pass

    def _loop(self) -> None:
        # Extract any new sessions from vault/incoming before starting
        try:
            n_extracted = extract_incoming_sessions(
                incoming_dir=self._incoming_dir,
                session_dir=self.session_dir,
            )
            if n_extracted:
                print(
                    f"[{self.SERVICE_NAME}] Extracted {n_extracted} session(s) from vault/incoming",
                    flush=True,
                )
        except Exception as exc:
            logger.warning("[%s] Session extraction error: %s", self.SERVICE_NAME, exc)

        with self._lock:
            self._bots = self._load_sessions()
            self.live_feed.is_running = True
            self.live_feed.total_in_group = len(self._bots)

        if not self._bots:
            logger.warning(
                "[%s] No session files found in %s — swarm idle.",
                self.SERVICE_NAME, self.session_dir,
            )

        print(
            f"[{self.SERVICE_NAME}] Loaded {len(self._bots)} sessions. "
            f"Target: {self.target_group or '(not set)'}",
            flush=True,
        )

        while not self._stop_event.is_set():
            if not self._bots:
                self._stop_event.wait(timeout=30)
                continue

            # Pick a random subset of bots to be active this cycle
            n_active = min(self.max_active_bots, len(self._bots))
            active_bots = random.sample(self._bots, n_active)

            with self._lock:
                for b in self._bots:
                    b.is_active = False
                for b in active_bots:
                    b.is_active = True
                self.live_feed.active_talkers = n_active

            for bot in active_bots:
                if self._stop_event.is_set():
                    break

                topic = random.choice(_TOPICS)
                message = generate_hebrew_message(topic)

                success = self._send_message(bot, message)
                now = time.time()

                with self._lock:
                    if success:
                        bot.messages_sent += 1
                        bot.last_message_text = message
                        bot.last_message_ts = now
                        self.live_feed.last_message = message
                        self.live_feed.last_message_ts = now
                        self.live_feed.last_sender_phone = bot.phone

                if success:
                    print(
                        f"[{self.SERVICE_NAME}] {'👑 ' if bot.is_king else ''}"
                        f"{bot.phone} → [{topic}] {message[:60]}…",
                        flush=True,
                    )

                self._publish_redis()

                # Randomised human-like delay between individual bot sends
                delay = random.uniform(self.min_delay_s * 0.1, self.min_delay_s * 0.5)
                self._stop_event.wait(timeout=delay)

            # Push telefix DB stats after each cycle
            self._publish_db_stats()

            # Cycle delay — wait before next round
            cycle_delay = random.uniform(self.min_delay_s, self.max_delay_s)
            print(
                f"[{self.SERVICE_NAME}] Cycle complete. "
                f"Next round in {cycle_delay:.0f}s.",
                flush=True,
            )
            self._stop_event.wait(timeout=cycle_delay)

        with self._lock:
            self.live_feed.is_running = False
            for b in self._bots:
                b.is_active = False


__all__ = [
    "IsraeliSwarm",
    "SwarmBotState",
    "SwarmLiveFeed",
    "generate_hebrew_message",
    "extract_incoming_sessions",
    "get_telefix_swarm_stats",
]
