"""
Israeli Ghostwriter Community — telefix ``groups`` rows flagged ``is_israeli``,
join-rate limits, and Hebrew vibe lines via the ghostwriter LLM stack.
"""

from __future__ import annotations

import importlib.util
import json
import os
import random
import sqlite3
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Awaitable

ISRAELI_TELEGRAM_PERSONA = "IsraeliTelegramUser"
_JOIN_REDIS_PREFIX = "nexus:ghostwriter:israeli:join_budget:"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _ghostwriter_ai_reply_path() -> Path:
    return _repo_root() / "src" / "nexus" / "agents" / "ghostwriter" / "ai_reply.py"


def _load_generate_reply() -> Callable[..., Awaitable[str]]:
    p = _ghostwriter_ai_reply_path()
    if not p.is_file():
        raise ImportError(f"ghostwriter ai_reply not found at {p}")
    spec = importlib.util.spec_from_file_location("ghostwriter_ai_reply", p)
    if spec is None or spec.loader is None:
        raise ImportError("could not load ghostwriter ai_reply")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.generate_reply  # type: ignore[no-any-return]


def resolve_telefix_db_path() -> Path:
    explicit = os.environ.get("TELEFIX_DB_PATH", "").strip()
    if explicit:
        return Path(explicit)
    try:
        from nexus.api.services.telefix_bridge import DB_PATH

        return Path(DB_PATH)
    except Exception:
        return _repo_root() / "telefix.db"


def ensure_is_israeli_column(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(groups)")
    cols = {str(r[1]) for r in cur.fetchall()}
    if "is_israeli" not in cols:
        conn.execute("ALTER TABLE groups ADD COLUMN is_israeli INTEGER DEFAULT 0")
        conn.commit()


def fetch_israeli_groups(db_path: Path | None = None) -> list[dict[str, Any]]:
    """
    Return group rows where ``is_israeli`` is truthy (1 / non-zero).
    Ensures the ``is_israeli`` column exists when opening the DB.
    """
    path = db_path or resolve_telefix_db_path()
    if not path.is_file():
        return []
    conn = sqlite3.connect(str(path), timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        ensure_is_israeli_column(conn)
        cur = conn.execute(
            "SELECT * FROM groups WHERE COALESCE(is_israeli, 0) != 0 ORDER BY id ASC"
        )
        return [dict(row) for row in cur.fetchall()]
    except sqlite3.Error:
        return []
    finally:
        conn.close()


class JoinHourLimiter:
    """
    Caps automated joins per rolling hour per session (default: 2) to reduce ban risk.
    Optional Redis backing shares counts across processes.
    """

    def __init__(self, max_per_hour: int = 2) -> None:
        self.max_per_hour = max_per_hour
        self._timestamps: dict[str, list[float]] = defaultdict(list)

    def is_allowed(self, session_key: str) -> bool:
        now = time.monotonic()
        window = self._timestamps[session_key]
        window[:] = [t for t in window if now - t < 3600.0]
        if len(window) >= self.max_per_hour:
            return False
        window.append(now)
        return True

    async def is_allowed_redis(self, redis: Any, session_key: str) -> bool:
        if redis is None:
            return self.is_allowed(session_key)
        key = f"{_JOIN_REDIS_PREFIX}{session_key}"
        raw = await redis.get(key)
        now = time.time()
        ts: list[float] = []
        if raw:
            try:
                ts = [float(x) for x in json.loads(raw) if isinstance(x, (int, float))]
            except Exception:
                ts = []
        ts = [t for t in ts if now - t < 3600.0]
        if len(ts) >= self.max_per_hour:
            return False
        ts.append(now)
        await redis.set(key, json.dumps(ts), ex=7200)
        return True


def _invite_hash(link_or_hash: str) -> str:
    s = (link_or_hash or "").strip()
    if "/+" in s:
        return s.split("/+")[-1].split("?")[0].strip()
    if "joinchat/" in s:
        return s.split("joinchat/")[-1].split("?")[0].strip()
    return s.lstrip("+")


async def join_group_auto(
    client: Any,
    target: str,
    session_key: str,
    limiter: JoinHourLimiter,
    *,
    redis: Any | None = None,
) -> bool:
    """
    Join a public username/supergroup or private invite (``t.me/+`` / ``joinchat``).
    Respects JoinHourLimiter (Redis-aware when ``redis`` is set).
    """
    allowed = (
        await limiter.is_allowed_redis(redis, session_key)
        if redis is not None
        else limiter.is_allowed(session_key)
    )
    if not allowed:
        return False

    from telethon.errors import RPCError  # type: ignore[import-untyped]
    from telethon.tl.functions.channels import JoinChannelRequest  # type: ignore[import-untyped]
    from telethon.tl.functions.messages import ImportChatInviteRequest  # type: ignore[import-untyped]

    t = target.strip()
    try:
        if "t.me/" in t or t.startswith("+") or "joinchat/" in t.lower():
            h = _invite_hash(t)
            if not h:
                return False
            await client(ImportChatInviteRequest(h))
            return True
        uname = t.lstrip("@")
        ent = await client.get_entity(uname)
        await client(JoinChannelRequest(await client.get_input_entity(ent)))
        return True
    except RPCError:
        return False
    except Exception:
        return False


async def generate_israeli_ghost_message(
    context_messages: list[str],
    *,
    group_title: str = "",
    topic_hint: str = "",
    provider: str = "gemini",
    gemini_api_key: str = "",
    openai_api_key: str = "",
    anthropic_api_key: str = "",
    model_gemini: str = "gemini-1.5-flash",
    model_openai: str = "gpt-4o-mini",
    model_anthropic: str = "claude-3-haiku-20240307",
    max_tokens: int = 200,
    temperature: float = 0.88,
) -> str:
    """
    One short Hebrew line in the *Israeli Telegram User* persona (slang allowed).
    """
    generate_reply = _load_generate_reply()
    trigger = (topic_hint or "").strip() or "מה הולך בקבוצה"
    extra = f" שם הקבוצה (אם רלוונטי): {group_title}" if group_title else ""
    return await generate_reply(
        trigger_word=f"{trigger}{extra}",
        context_messages=context_messages,
        personality=ISRAELI_TELEGRAM_PERSONA,
        provider=provider,  # type: ignore[arg-type]
        gemini_api_key=gemini_api_key,
        openai_api_key=openai_api_key,
        anthropic_api_key=anthropic_api_key,
        model_gemini=model_gemini,
        model_openai=model_openai,
        model_anthropic=model_anthropic,
        max_tokens=max_tokens,
        temperature=temperature,
    )


def _row_telegram_entity_id(row: dict[str, Any]) -> int | None:
    for k in ("telegram_id", "tg_id", "chat_id", "telegram_group_id", "group_id"):
        v = row.get(k)
        if v is None:
            continue
        try:
            return int(v)
        except (TypeError, ValueError):
            continue
    return None


def merge_israeli_db_rows_into_groups_config(
    existing: dict[str, Any],
    db_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Merge ``fetch_israeli_groups`` results into the Redis ``groups`` map.
    Preserves ``sessions`` / ``enabled`` when already present; fills ids / titles from DB.
    """
    out = dict(existing) if isinstance(existing, dict) else {}
    for row in db_rows:
        rid = row.get("id")
        if rid is None:
            continue
        key = str(rid)
        cur = out.get(key) if isinstance(out.get(key), dict) else {}
        invite = row.get("invite_link") or row.get("invite") or ""
        username = row.get("username") or ""
        title = row.get("title") or ""
        tg_id = _row_telegram_entity_id(row)
        prior_gid = cur.get("group_id")
        try:
            prior_int = int(prior_gid) if prior_gid is not None else None
        except (TypeError, ValueError):
            prior_int = None
        resolved_gid = prior_int if prior_int is not None else tg_id
        merged = {
            "enabled": bool(cur.get("enabled", True)),
            "group_id": resolved_gid,
            "invite_link": str(cur.get("invite_link") or invite or ""),
            "username": str(cur.get("username") or username or ""),
            "group_title": str(cur.get("group_title") or title or ""),
            "sessions": list(cur.get("sessions") or []),
            "timezone": str(cur.get("timezone", "Asia/Jerusalem") or "Asia/Jerusalem"),
        }
        out[key] = merged
    return out
