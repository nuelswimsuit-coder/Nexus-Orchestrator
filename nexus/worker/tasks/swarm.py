"""
swarm.community_factory — Israeli Community Factory: role split, group creation,
distributed joins with FloodWait / ban handling, and LLM-driven Hebrew chatter.

Redis namespace: nexus:swarm:factory:*
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote

import structlog

from nexus.modules.community_vibe import parse_json_object
from nexus.worker.services.israeli_telegram_profile import ensure_israeli_factory_profile
from nexus.worker.services.tg_session import (
    async_telegram_client,
    classify_telethon_account_error,
    flood_wait_seconds,
    resolve_telethon_creds,
)
from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

KEY_ROLES = "nexus:swarm:factory:roles"
KEY_GROUPS = "nexus:swarm:factory:groups"
KEY_STATE = "nexus:swarm:factory:state"
KEY_BANNED = "nexus:swarm:factory:banned"
KEY_COOLDOWNS = "nexus:swarm:factory:cooldowns"
KEY_METRICS = "nexus:swarm:factory:metrics"
KEY_PROFILE_GATE = "nexus:swarm:factory:profile_gate"
THREAD_KEY_PREFIX = "nexus:swarm:factory:thread:"
ACTIVE_TOPIC_KEY_PREFIX = "nexus:swarm:factory:active_topic:"
THREAD_ID_CAP = 5
RECENT_GROUP_MSG_CAP = 15
RECENT_GROUP_MSG_MAX_CHARS = 180

# When Redis is unavailable, avoid re-running profile checks every tick (Latin names stay "non_israeli" per heuristic).
_factory_profile_verified_local: set[str] = set()

GROUPS_TARGET_PER_OWNER = 20
REACTION_EMOJIS = ["🔥", "😂", "💀", "🤯", "👀", "😱", "💪", "🤦", "😅", "❤️", "🙏"]
THREAD_REACTION_EMOJIS = ["👍", "🤦‍♂️", "🤬"]

AMCHA_ISRAEL_SYSTEM_PROMPT = (
    "You are an everyday Israeli ('עמך ישראל') in a local Telegram group. \n"
    "RULES:\n"
    "- Read the last 5 messages and react to them uniquely. Argue, laugh, teach, or suddenly try to sell a second-hand item (e.g., 'מישהו פה מוכר אופניים?').\n"
    "- Tone: Casual, slightly cynical, authentic. Use slang ('אחי', 'וואלה', 'בדוק', 'הזייה').\n"
    "- Length: Variable. Sometimes 2 words, sometimes a 15-word rant.\n"
    "- NEVER be polite or formal. Act like you are chatting with friends on WhatsApp."
)

AMCHA_STANCES_HE = [
    "עמדה לתגובה שלך: הסכם בעוצמה, ממש תתלהב.",
    "עמדה לתגובה שלך: לא מסכים בחום, תתנגד אגרסיבית.",
    "עמדה לתגובה שלך: תעשה בדיחה על זה.",
    "עמדה לתגובה שלך: תשנה נושא בפתאומיות (אבל עדיין טבעי לקבוצה).",
]

FACTORY_TOPICS = [
    "קריפטו ומטבעות דיגיטליים — ביטקוין, אלטקוינים, בורסות",
    "ישראל–איראן — מתח אזורי וגיאופוליטיקה",
    "פוליטיקה ישראלית — קואליציה, משפט, מחאות",
    "כלכלה בישראל — דיור, מחירים, ריבית",
    "קניות אונליין — משלוחים, מבצעים, אתרים",
    "יד שנייה — יד2, מרקטפלייס, טיפים לקנייה",
]

def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve_sessions_dir(explicit: str | None) -> Path:
    if explicit and explicit.strip():
        p = Path(explicit).expanduser()
        return p.resolve()
    env = os.getenv("VAULT_SESSIONS_DIR", "").strip()
    if env:
        return Path(env).expanduser().resolve()
    return (_project_root() / "vault" / "sessions").resolve()


def _discover_session_bases(sessions_dir: Path) -> list[str]:
    if not sessions_dir.is_dir():
        return []
    files = sorted(sessions_dir.glob("*.session"), key=lambda p: p.as_posix().lower())
    return [str(p.with_suffix("").resolve()) for p in files]


def _split_roles(bases: list[str]) -> tuple[list[str], list[str]]:
    """
    ~3% owners, remainder members. Uses rounding so large pools track nominal 3%;
    with very few sessions, at least one owner is kept (may exceed 3% until n grows).
    """
    n = len(bases)
    if n == 0:
        return [], []
    owner_count = max(1, round(n * 0.03))
    owner_count = min(owner_count, n)
    owners = bases[:owner_count]
    members = bases[owner_count:]
    return owners, members


def _default_metrics() -> dict[str, Any]:
    return {
        "messages_sent": 0,
        "flood_waits": 0,
        "bans": 0,
        "joins_ok": 0,
        "joins_failed": 0,
        "join_attempts": 0,
        "groups_total": 0,
        "active_sessions": 0,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _default_state(sessions_dir: str) -> dict[str, Any]:
    return {
        "phase": "idle",
        "sessions_dir": sessions_dir,
        "creation_index": 0,
        "join_flat_idx": 0,
        "converse_idx": 0,
        "groups_per_owner_target": GROUPS_TARGET_PER_OWNER,
        "init_phases": [],
        "chat_enabled": False,
    }


async def _redis_json_get(redis: Any, key: str) -> Any:
    if redis is None:
        return None
    raw = await redis.get(key)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


async def _redis_json_set(redis: Any, key: str, data: Any) -> None:
    if redis is None:
        return
    await redis.set(key, json.dumps(data, ensure_ascii=False))


def _resolve_api_key(parameters: dict[str, Any]) -> str:
    secrets = parameters.get("__secrets__", {})
    return (
        str(parameters.get("gemini_api_key", "")).strip()
        or secrets.get("GEMINI_API_KEY", "")
        or os.getenv("GEMINI_API_KEY", "")
    )


def _resolve_openai_key(parameters: dict[str, Any]) -> str:
    secrets = parameters.get("__secrets__", {})
    return (
        str(parameters.get("openai_api_key", "")).strip()
        or secrets.get("OPENAI_API_KEY", "")
        or os.getenv("OPENAI_API_KEY", "")
    )


def _invite_hash(link_or_hash: str) -> str:
    s = (link_or_hash or "").strip()
    if "/+" in s:
        return s.split("/+")[-1].split("?")[0].strip()
    if "joinchat/" in s.lower():
        return s.split("joinchat/")[-1].split("?")[0].strip()
    return s.lstrip("+")


def _strip_hashtags_and_cleanup(text: str) -> str:
    s = re.sub(r"#\S+", "", text or "")
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def _cap_hebrew_words(text: str, max_words: int = 10) -> str:
    parts = (text or "").split()
    if len(parts) <= max_words:
        return (text or "").strip()
    return " ".join(parts[:max_words])


def _thread_redis_key(group_id: int | str) -> str:
    return f"{THREAD_KEY_PREFIX}{int(group_id)}"


async def _thread_ids_read(redis: Any, group_id: int | str) -> list[int]:
    if redis is None:
        return []
    raw = await redis.get(_thread_redis_key(group_id))
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    out: list[int] = []
    for x in data:
        try:
            out.append(int(x))
        except (TypeError, ValueError):
            continue
    return out


async def _thread_ids_push(redis: Any, group_id: int | str, msg_id: int) -> None:
    if redis is None:
        return
    cur = await _thread_ids_read(redis, group_id)
    cur.append(int(msg_id))
    cur = cur[-THREAD_ID_CAP:]
    await redis.set(_thread_redis_key(group_id), json.dumps(cur, ensure_ascii=False))


def _active_topic_redis_key(group_id: int | str) -> str:
    return f"{ACTIVE_TOPIC_KEY_PREFIX}{int(group_id)}"


async def _active_topic_read(redis: Any, group_id: int | str) -> dict[str, Any] | None:
    raw = await _redis_json_get(redis, _active_topic_redis_key(group_id))
    if isinstance(raw, dict) and str(raw.get("text") or "").strip():
        return raw
    return None


async def _active_topic_write(redis: Any, group_id: int | str, text: str) -> None:
    await _redis_json_set(
        redis,
        _active_topic_redis_key(group_id),
        {
            "text": (text or "").strip()[:2000],
            "event_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def _active_topic_should_refresh(record: dict[str, Any] | None, threshold_sec: float) -> bool:
    if not record:
        return True
    iso = record.get("event_at")
    if not iso:
        return True
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - dt).total_seconds()
        return age >= threshold_sec
    except Exception:
        return True


def _recent_texts_from_telethon_messages(
    messages: list[Any], *, max_items: int = RECENT_GROUP_MSG_CAP, max_chars: int = RECENT_GROUP_MSG_MAX_CHARS
) -> list[str]:
    out: list[str] = []
    for m in messages:
        if m is None:
            continue
        raw = getattr(m, "message", None)
        if raw is None:
            continue
        t = str(raw).strip()
        if not t:
            continue
        if len(t) > max_chars:
            t = t[: max_chars - 1] + "…"
        out.append(t)
        if len(out) >= max_items:
            break
    return out


def _anti_duplication_prompt_suffix(recent_texts: list[str]) -> str:
    if not recent_texts:
        return ""
    lines = "\n".join(f"- {t}" for t in recent_texts)
    return (
        "\n\nחובה מוחלטת: אל תחזור, אל תפרפרז ואל תשכפל מבנים דומה להודעות האחרונות בקבוצה. "
        "משהו אחר לגמרי.\n"
        "הודעות אחרונות בקבוצה (אסור לחקות):\n"
        f"{lines}\n"
    )


def _message_refs_newest_first(messages_oldest_first: list[Any]) -> list[tuple[int, str]]:
    """Telethon history reversed to chronological oldest-first; collect newest text messages first."""
    out: list[tuple[int, str]] = []
    for m in reversed(messages_oldest_first):
        if m is None:
            continue
        mid = getattr(m, "id", None)
        if mid is None:
            continue
        raw = getattr(m, "message", None)
        if raw is None:
            continue
        t = str(raw).strip()
        if not t:
            continue
        if len(t) > RECENT_GROUP_MSG_MAX_CHARS:
            t = t[: RECENT_GROUP_MSG_MAX_CHARS - 1] + "…"
        out.append((int(mid), t))
        if len(out) >= RECENT_GROUP_MSG_CAP:
            break
    return out


def _last_five_prompt_block(refs_newest_first: list[tuple[int, str]]) -> str:
    block = refs_newest_first[:5]
    if not block:
        return "אין הודעות אחרונות בקבוצה — תאלתר טבעי."
    chronological = list(reversed(block))
    lines = "\n".join(f"{i + 1}. {txt}" for i, (_, txt) in enumerate(chronological))
    return f"5 ההודעות האחרונות בקבוצה (לפי סדר כרונולוגי, מהישנה לחדשה):\n{lines}"


def _finalize_primary_message(text: str) -> str:
    s = _strip_hashtags_and_cleanup(text)
    s = _cap_hebrew_words(s, 25)
    parts = s.split()
    if len(parts) <= 8 or len(s) <= 40:
        s = re.sub(r"[.!?]+\s*$", "", s).strip()
    return s


def _finalize_correction_message(text: str) -> str:
    return _cap_hebrew_words(_strip_hashtags_and_cleanup(text), 20)


def _safe_md_link_label(label: str) -> str:
    s = (label or "").replace("[", "(").replace("]", ")").strip()
    return s or "קראו כאן"


async def _maybe_tinyurl_shorten(url: str) -> str:
    u = (url or "").strip()
    if not u.startswith(("http://", "https://")):
        return u
    flag = (os.getenv("COMMUNITY_FACTORY_USE_TINYURL", "") or "").strip().lower()
    if flag not in ("1", "true", "yes", "on"):
        return u
    try:
        import httpx

        api = f"https://tinyurl.com/api-create.php?url={quote(u, safe='')}"
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(api)
            if r.status_code == 200:
                short = (r.text or "").strip()
                if short.startswith("http://") or short.startswith("https://"):
                    return short
    except Exception as exc:
        log.debug("factory_tinyurl_failed", error=str(exc))
    return u


def _format_opener_with_md_link(primary: str, url: str, label: str) -> tuple[str, bool]:
    base = _finalize_primary_message(primary)
    u = (url or "").strip()
    if not u.startswith(("http://", "https://")):
        return base, False
    lab = _safe_md_link_label(label)
    link_line = f"[{lab}]({u})"
    if not base:
        return link_line, True
    return f"{base}\n{link_line}", True


def _coerce_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    return s in ("true", "1", "yes", "on")


def _normalize_amcha_dict(obj: dict[str, Any]) -> dict[str, Any]:
    pm = str(obj.get("primary_message") or obj.get("text") or "").strip()
    cm = str(obj.get("correction_message") or "").strip()
    return {
        "primary_message": pm,
        "needs_correction": _coerce_bool(obj.get("needs_correction")),
        "correction_message": cm,
        "article_url": str(obj.get("article_url") or "").strip(),
        "link_label": str(obj.get("link_label") or "").strip(),
    }


def _parse_llm_json_object(raw: str) -> dict[str, Any] | None:
    t = (raw or "").strip()
    if not t:
        return None
    try:
        obj = parse_json_object(t)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


async def _send_amcha_messages(
    client: Any,
    entity: Any,
    *,
    primary: str,
    needs_correction: bool,
    correction: str,
    reply_to_id: int | None,
    parse_mode: str | None,
) -> list[Any]:
    sent: list[Any] = []
    text = primary[:4096]
    try:
        msg = await client.send_message(entity, text, reply_to=reply_to_id, parse_mode=parse_mode)
        sent.append(msg)
    except Exception as exc:
        if parse_mode:
            log.debug("factory_send_md_fallback", error=str(exc))
            msg = await client.send_message(entity, text, reply_to=reply_to_id, parse_mode=None)
            sent.append(msg)
        else:
            raise
    if needs_correction and (correction or "").strip():
        await asyncio.sleep(random.uniform(2.0, 4.0))
        fix = _finalize_correction_message(correction)[:4096]
        if fix:
            msg2 = await client.send_message(entity, fix, reply_to=None, parse_mode=None)
            sent.append(msg2)
    return sent


async def _generate_amcha_turn(
    api_key: str,
    topic: str,
    openai_key: str,
    *,
    role: Literal["opener", "replier"],
    stance_he: str,
    last_five_block: str,
    typo_must_correct: bool,
    anchor_preview: str | None = None,
    recent_texts: list[str] | None = None,
    active_topic_line: str | None = None,
    opener_fresh_event: bool = False,
    news_opener: bool = False,
) -> dict[str, Any]:
    anti = _anti_duplication_prompt_suffix(list(recent_texts or []))
    json_schema = (
        "החזר אך ורק JSON תקף (בלי טקסט נוסף) עם המפתחות: "
        '"primary_message","needs_correction","correction_message","article_url","link_label". '
        "article_url ו-link_label — מחרוזות; כשאין קישור חדשותי השאר ריק."
    )
    if typo_must_correct:
        typo_rule = (
            "חובה: needs_correction=true — שים ב-primary_message טעות הקלדה עברית נפוצה "
            "(למשל בוט במקום טוב, או ניראה לי במקום נראה לי), "
            "וב-correction_message תיקון אותנטי קצר כמו 'טוב*' או 'סליחה טוב*' או 'איזה אהבל אני, טוב*'."
        )
    else:
        typo_rule = "needs_correction חייב להיות false; correction_message יכול להיות מחרוזת ריקה."

    opener_news_clause = ""
    if news_opener:
        opener_news_clause = (
            "תפקיד: פותח חדשות. primary_message — שורות קצרות כמו בווטסאפ על הכתבה/פלאש (בלי URL גולמי בגוף ההודעה). "
            "חובה למלא article_url עם קישור https plausibly לאתר חדשות ישראלי, "
            "ול-link_label משפט עברית לכפתור הקישור (למשל: קראו פה את הכתבה המלאה)."
        )

    if role == "opener" and not news_opener:
        ctx = (active_topic_line or "").strip()[:400] or topic
        user_he = (
            f"{last_five_block}\n\n{stance_he}\n\n"
            f'הקבוצה כבר רותחת סביב: "{ctx}". '
            "עוד משפט או שניים — זווית אחרת, לא פורמלי.\n"
            f"{opener_news_clause}\n{typo_rule}\n{anti}\n{json_schema}"
        )
    elif role == "opener" and news_opener:
        user_he = (
            f"{last_five_block}\n\n{stance_he}\n\n"
            f"הנחיה: שמועה/פלאש/מה זה עכשיו — קבוצת חדשות בטלגרם. "
            f'רקע רחב בלבד: "{topic}".\n'
            f"{opener_news_clause}\n{typo_rule}\n{anti}\n{json_schema}"
        )
    else:
        ap = (anchor_preview or "").strip()[:800] or "(אין טקסט — תגיב בקצרה)"
        if (active_topic_line or "").strip():
            head = f'הנושא הפעיל בקבוצה: "{(active_topic_line or "").strip()[:400]}". '
        else:
            head = ""
        user_he = (
            f"{last_five_block}\n\n{stance_he}\n\n"
            f"{head}"
            f'אתה משיב להודעה/שורה הזו (או לקונטקסט שלה): "{ap}"\n'
            f"{typo_rule}\n{anti}\n{json_schema}"
        )

    temperature = random.uniform(0.85, 0.95)
    frequency_penalty = random.uniform(0.35, 0.5)
    presence_penalty = random.uniform(0.35, 0.5)

    if api_key:
        try:
            from nexus.modules.community_vibe import _gemini_json  # type: ignore[attr-defined]

            out = await _gemini_json(
                api_key,
                AMCHA_ISRAEL_SYSTEM_PROMPT,
                user_he,
                temperature=temperature,
                max_tokens=320,
                frequency_penalty=frequency_penalty,
                presence_penalty=presence_penalty,
            )
            if isinstance(out, dict) and (out.get("primary_message") or out.get("text")):
                return _normalize_amcha_dict(out)
        except Exception as exc:
            log.warning("factory_gemini_failed", error=str(exc))

    if openai_key:
        try:
            import httpx

            url = "https://api.openai.com/v1/chat/completions"
            headers = {"Authorization": f"Bearer {openai_key}"}
            payload = {
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": AMCHA_ISRAEL_SYSTEM_PROMPT},
                    {"role": "user", "content": user_he},
                ],
                "temperature": temperature,
                "max_tokens": 320,
                "frequency_penalty": frequency_penalty,
                "presence_penalty": presence_penalty,
            }
            async with httpx.AsyncClient(timeout=60.0) as client:
                r = await client.post(url, json=payload, headers=headers)
                r.raise_for_status()
                data = r.json()
                choice = (data.get("choices") or [{}])[0]
                raw_msg = (choice.get("message") or {}).get("content") or ""
                obj = _parse_llm_json_object(raw_msg) if raw_msg.strip() else None
                if obj:
                    return _normalize_amcha_dict(obj)
        except Exception as exc:
            log.warning("factory_openai_failed", error=str(exc))

    fb_pm = "וואלה הזייה אחי" if role == "opener" else "אין מצב"
    return {
        "primary_message": fb_pm,
        "needs_correction": False,
        "correction_message": "",
        "article_url": "",
        "link_label": "",
    }


async def _redis_delete_keys_with_prefix(redis: Any, prefix: str) -> None:
    if redis is None:
        return
    keys: list[Any] = []
    try:
        async for key in redis.scan_iter(match=f"{prefix}*"):
            keys.append(key)
    except Exception as exc:
        log.warning("factory_redis_scan_failed", prefix=prefix, error=str(exc))
        return
    if not keys:
        return
    try:
        await redis.delete(*keys)
    except Exception as exc:
        log.warning("factory_redis_delete_failed", prefix=prefix, error=str(exc))


async def _ensure_factory_profile(client: Any, redis: Any, session_base: str) -> None:
    await ensure_israeli_factory_profile(
        client,
        redis,
        session_base,
        gate_key=KEY_PROFILE_GATE,
        local_verified=_factory_profile_verified_local,
    )


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


async def _send_thread_reaction(client: Any, entity: Any, msg_id: int) -> bool:
    from telethon.tl.functions.messages import SendReactionRequest  # type: ignore[import-untyped]
    from telethon.tl.types import ReactionEmoji  # type: ignore[import-untyped]

    emojis = list(THREAD_REACTION_EMOJIS)
    random.shuffle(emojis)
    for emo in emojis:
        try:
            await client(
                SendReactionRequest(
                    peer=entity,
                    msg_id=int(msg_id),
                    reaction=[ReactionEmoji(emoticon=emo)],
                )
            )
            return True
        except Exception:
            continue
    return False


async def _try_send_pack_sticker(client: Any, entity: Any) -> bool:
    from telethon.tl.functions.messages import GetStickerSetRequest  # type: ignore[import-untyped]
    from telethon.tl.types import InputStickerSetShortName  # type: ignore[import-untyped]

    short = (os.getenv("COMMUNITY_FACTORY_STICKER_SET", "AnimatedEmojies") or "").strip()
    if not short:
        return False
    try:
        res = await client(
            GetStickerSetRequest(
                stickerset=InputStickerSetShortName(short_name=short),
                hash=0,
            )
        )
        docs = [d for d in (getattr(res, "documents", None) or []) if d]
        if not docs:
            return False
        await client.send_file(entity, random.choice(docs))
        return True
    except Exception as exc:
        log.debug("factory_sticker_skipped", error=str(exc))
        return False


async def _mark_banned(redis: Any, session_base: str) -> None:
    if redis is None:
        return
    raw = await _redis_json_get(redis, KEY_BANNED)
    banned: list[str] = list(raw) if isinstance(raw, list) else []
    stem = session_base
    if stem not in banned:
        banned.append(stem)
    await _redis_json_set(redis, KEY_BANNED, banned)


async def _is_session_banned(redis: Any, session_base: str) -> bool:
    raw = await _redis_json_get(redis, KEY_BANNED)
    if not isinstance(raw, list):
        return False
    return session_base in raw


async def _cooldown_until(redis: Any, session_base: str) -> datetime | None:
    raw = await _redis_json_get(redis, KEY_COOLDOWNS)
    if not isinstance(raw, dict):
        return None
    iso = raw.get(session_base)
    if not iso:
        return None
    try:
        return datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
    except Exception:
        return None


async def _set_cooldown(redis: Any, session_base: str, seconds: int) -> None:
    if redis is None:
        return
    raw = await _redis_json_get(redis, KEY_COOLDOWNS)
    cd: dict[str, str] = dict(raw) if isinstance(raw, dict) else {}
    until = datetime.now(timezone.utc).timestamp() + seconds
    cd[session_base] = datetime.fromtimestamp(until, tz=timezone.utc).isoformat()
    await _redis_json_set(redis, KEY_COOLDOWNS, cd)


async def _bump_metric(redis: Any, field: str, delta: int = 1) -> None:
    if redis is None:
        return
    m = await _redis_json_get(redis, KEY_METRICS)
    if not isinstance(m, dict):
        m = _default_metrics()
    m[field] = int(m.get(field, 0)) + delta
    m["updated_at"] = datetime.now(timezone.utc).isoformat()
    await _redis_json_set(redis, KEY_METRICS, m)


async def _sync_active_sessions(redis: Any, all_bases: list[str]) -> None:
    banned_raw = await _redis_json_get(redis, KEY_BANNED)
    banned_n = len(banned_raw) if isinstance(banned_raw, list) else 0
    active = max(0, len(all_bases) - banned_n)
    m = await _redis_json_get(redis, KEY_METRICS)
    if not isinstance(m, dict):
        m = _default_metrics()
    m["active_sessions"] = active
    m["updated_at"] = datetime.now(timezone.utc).isoformat()
    await _redis_json_set(redis, KEY_METRICS, m)


async def _enqueue_task(task_type: str, parameters: dict[str, Any]) -> bool:
    try:
        import arq
        from arq.connections import RedisSettings

        from nexus.shared.config import settings
        from nexus.shared.schemas import TaskPayload

        task = TaskPayload(
            task_type=task_type,
            parameters=parameters,
            project_id="community-factory",
            priority=3,
            job_expires_seconds=600,
        )
        job_ttl = int(task.job_expires_seconds or int(os.getenv("TASK_DEFAULT_TIMEOUT", "300")))
        arq_pool = await arq.create_pool(
            RedisSettings.from_dsn(settings.redis_url),
            default_queue_name="nexus:tasks",
        )
        await arq_pool.enqueue_job(
            "execute_task",
            task_payload=task.model_dump_for_wire(),
            _job_id=str(uuid.uuid4()),
            _queue_name="nexus:tasks",
            _expires=job_ttl,
        )
        await arq_pool.aclose()
        return True
    except Exception as exc:
        log.error("community_factory_enqueue_failed", task_type=task_type, error=str(exc))
        return False


@registry.register("swarm.community_factory.bootstrap")
async def community_factory_bootstrap(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Scan sessions_dir, compute 3% owners / 97% members, persist roles, init state/metrics,
    enqueue create/join/chat ticks per ``phases``.
    """
    redis = parameters.get("__redis__")
    sessions_dir = _resolve_sessions_dir(str(parameters.get("sessions_dir", "") or ""))
    phases = [str(p).lower() for p in (parameters.get("phases") or ["allocate", "create"])]
    dry_run = bool(parameters.get("dry_run", False))
    reset = bool(parameters.get("reset", False))

    bases = _discover_session_bases(sessions_dir)
    owners, members = _split_roles(bases)

    if reset and redis and not dry_run:
        await redis.delete(
            KEY_ROLES, KEY_GROUPS, KEY_STATE, KEY_BANNED, KEY_COOLDOWNS, KEY_METRICS, KEY_PROFILE_GATE
        )
        await _redis_delete_keys_with_prefix(redis, THREAD_KEY_PREFIX)
        await _redis_delete_keys_with_prefix(redis, ACTIVE_TOPIC_KEY_PREFIX)

    roles_payload = {"owners": owners, "members": members}

    if not dry_run and redis:
        await _redis_json_set(redis, KEY_ROLES, roles_payload)
        roles_path = os.getenv("COMMUNITY_FACTORY_ROLES_PATH", "").strip()
        if roles_path:
            try:
                Path(roles_path).parent.mkdir(parents=True, exist_ok=True)
                Path(roles_path).write_text(
                    json.dumps(roles_payload, ensure_ascii=False, indent=2), encoding="utf-8"
                )
            except OSError as exc:
                log.warning("factory_roles_file_write_failed", path=roles_path, error=str(exc))

        state = await _redis_json_get(redis, KEY_STATE)
        if not isinstance(state, dict):
            state = _default_state(str(sessions_dir))
        else:
            state["sessions_dir"] = str(sessions_dir)
        state["phase"] = "allocating"
        state["init_phases"] = phases
        state["chat_enabled"] = "chat" in phases
        state["creation_index"] = 0
        state["join_flat_idx"] = 0
        state["converse_idx"] = 0
        state["max_joins_per_tick"] = int(parameters.get("max_joins_per_tick") or 1)
        state["converse_chain_limit"] = int(
            parameters.get("converse_chain_limit")
            or os.getenv("COMMUNITY_FACTORY_CONVERSE_CHAIN", "5000")
        )
        await _redis_json_set(redis, KEY_STATE, state)

        m = await _redis_json_get(redis, KEY_METRICS)
        if not isinstance(m, dict):
            await _redis_json_set(redis, KEY_METRICS, _default_metrics())
        await _sync_active_sessions(redis, bases)

    carry = {
        "sessions_dir": str(sessions_dir),
        "phases": phases,
    }

    if dry_run:
        return {
            "status": "completed",
            "dry_run": True,
            "sessions_dir": str(sessions_dir),
            "total_sessions": len(bases),
            "owners": len(owners),
            "members": len(members),
            "roles": roles_payload,
        }

    if "create" in phases and owners:
        state = await _redis_json_get(redis, KEY_STATE)
        if isinstance(state, dict):
            state["phase"] = "creating"
            await _redis_json_set(redis, KEY_STATE, state)
        await _enqueue_task("swarm.community_factory.create_groups_tick", carry)
    elif "join" in phases and not ("create" in phases):
        await _enqueue_task("swarm.community_factory.join_tick", carry)
    if "chat" in phases and "create" not in phases and "join" not in phases:
        await _enqueue_task("swarm.community_factory.converse_tick", carry)

    return {
        "status": "completed",
        "total_sessions": len(bases),
        "owners": len(owners),
        "members": len(members),
        "phases": phases,
        "enqueued": True,
    }


@registry.register("swarm.community_factory.create_groups_tick")
async def community_factory_create_groups_tick(parameters: dict[str, Any]) -> dict[str, Any]:
    redis = parameters.get("__redis__")

    roles = await _redis_json_get(redis, KEY_ROLES)
    if not isinstance(roles, dict):
        return {"status": "failed", "error": "roles not allocated — run bootstrap"}
    owners: list[str] = list(roles.get("owners") or [])
    if not owners:
        return {"status": "failed", "error": "no owners"}

    aid, ahash = resolve_telethon_creds(owners[0], parameters)
    if not aid or not ahash:
        return {
            "status": "failed",
            "error": "Telethon api_id/api_hash missing: set TELEFIX_* or add .json next to the first owner session",
        }

    state = await _redis_json_get(redis, KEY_STATE)
    if not isinstance(state, dict):
        return {"status": "failed", "error": "state missing"}
    target = int(state.get("groups_per_owner_target") or GROUPS_TARGET_PER_OWNER)
    idx = int(state.get("creation_index", 0))
    max_idx = target * len(owners) - 1
    if idx > max_idx:
        iphases = list(state.get("init_phases") or [])
        if "join" in iphases:
            state["phase"] = "joining"
        elif "chat" in iphases:
            state["phase"] = "chatting"
        else:
            state["phase"] = "complete"
        await _redis_json_set(redis, KEY_STATE, state)
        if "join" in iphases:
            await _enqueue_task(
                "swarm.community_factory.join_tick",
                {"sessions_dir": state.get("sessions_dir", "")},
            )
        elif "chat" in iphases:
            await _enqueue_task(
                "swarm.community_factory.converse_tick",
                {"sessions_dir": state.get("sessions_dir", "")},
            )
        return {"status": "completed", "phase": "create_done", "groups_created_total": idx}

    try:
        from telethon.tl.functions.channels import CreateChannelRequest  # type: ignore[import-untyped]
        from telethon.tl.types import Channel  # type: ignore[import-untyped]
    except ImportError:
        return {"status": "failed", "error": "telethon not installed"}

    await asyncio.sleep(random.uniform(30.0, 120.0))

    owner_idx = idx % len(owners)
    owner_base = owners[owner_idx]

    title = f"CF {owner_idx}-{idx // len(owners)}-{random.randint(1000, 9999)}"
    group_id: int | None = None
    invite_link = ""

    try:
        async with async_telegram_client(owner_base, parameters) as client:
            if not await client.is_user_authorized():
                await _mark_banned(redis, owner_base)
                await _bump_metric(redis, "bans", 1)
                state["creation_index"] = idx + 1
                await _redis_json_set(redis, KEY_STATE, state)
                await _enqueue_task("swarm.community_factory.create_groups_tick", parameters)
                return {"status": "skipped", "reason": "owner_unauthorized"}

            await _ensure_factory_profile(client, redis, owner_base)

            created = await client(
                CreateChannelRequest(title=title[:128], about="", megagroup=True, broadcast=False)
            )
            chats = list(getattr(created, "chats", None) or [])
            ch = next((c for c in chats if isinstance(c, Channel)), None)
            if ch is None and chats:
                ch = chats[0]
            if ch is None:
                raise RuntimeError("CreateChannelRequest returned no channel")
            invite_link = await client.export_chat_invite_link(ch)
            group_id = int(ch.id)
    except ValueError as exc:
        log.warning("factory_create_creds_missing", owner=owner_base[:48], error=str(exc))
        state["creation_index"] = idx + 1
        await _redis_json_set(redis, KEY_STATE, state)
        await _enqueue_task("swarm.community_factory.create_groups_tick", parameters)
        return {"status": "failed", "error": str(exc), "continuing": True}
    except Exception as exc:
        kind = classify_telethon_account_error(exc)
        if kind == "ban":
            await _mark_banned(redis, owner_base)
            await _bump_metric(redis, "bans", 1)
        elif kind == "flood":
            sec = int(flood_wait_seconds(exc) * 1.1) + 1
            await _set_cooldown(redis, owner_base, sec)
            await _bump_metric(redis, "flood_waits", 1)
            await _enqueue_task("swarm.community_factory.create_groups_tick", parameters)
            return {"status": "deferred", "reason": "flood_wait", "seconds": sec}
        log.warning("factory_create_failed", error=str(exc))
        state["creation_index"] = idx + 1
        await _redis_json_set(redis, KEY_STATE, state)
        await _enqueue_task("swarm.community_factory.create_groups_tick", parameters)
        return {"status": "failed", "error": str(exc), "continuing": True}

    groups = await _redis_json_get(redis, KEY_GROUPS)
    glist: list[dict[str, Any]] = list(groups) if isinstance(groups, list) else []
    glist.append(
        {
            "group_id": group_id,
            "owner_session": owner_base,
            "invite_link": invite_link,
            "invite_hash": _invite_hash(invite_link),
            "title": title,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    await _redis_json_set(redis, KEY_GROUPS, glist)
    await _bump_metric(redis, "groups_total", 1)

    state["creation_index"] = idx + 1
    await _redis_json_set(redis, KEY_STATE, state)
    await _enqueue_task("swarm.community_factory.create_groups_tick", parameters)

    return {
        "status": "completed",
        "group_id": group_id,
        "invite_link": invite_link,
        "creation_index": idx + 1,
    }


@registry.register("swarm.community_factory.join_tick")
async def community_factory_join_tick(parameters: dict[str, Any]) -> dict[str, Any]:
    redis = parameters.get("__redis__")

    roles = await _redis_json_get(redis, KEY_ROLES)
    groups = await _redis_json_get(redis, KEY_GROUPS)
    state = await _redis_json_get(redis, KEY_STATE)
    if not isinstance(roles, dict) or not isinstance(state, dict):
        return {"status": "failed", "error": "roles or state missing"}
    if not isinstance(groups, list) or not groups:
        return {"status": "failed", "error": "no groups to join"}

    owners = list(roles.get("owners") or [])
    members = list(roles.get("members") or [])
    if not members:
        return {
            "status": "failed",
            "error": "no member sessions to join groups — need non-owner accounts in the pool",
        }

    aid, ahash = resolve_telethon_creds(members[0], parameters)
    if not aid or not ahash:
        return {
            "status": "failed",
            "error": "Telethon api_id/api_hash missing: set TELEFIX_* or add .json next to member sessions",
        }

    all_sessions = owners + members

    G = len(groups)
    S = len(members)
    flat_max = S * G
    j = int(state.get("join_flat_idx", 0))

    max_joins = int(
        state.get("max_joins_per_tick")
        or parameters.get("max_joins_per_tick")
        or os.getenv("COMMUNITY_FACTORY_MAX_JOINS_PER_TICK", "1")
    )

    try:
        from telethon.tl.functions.messages import ImportChatInviteRequest  # type: ignore[import-untyped]
    except ImportError:
        return {"status": "failed", "error": "telethon not installed"}

    attempts = 0
    while attempts < max(20, max_joins * 5) and j < flat_max:
        session_i = j % S
        group_i = j // S
        session_base = members[session_i]
        grp = groups[group_i] if group_i < len(groups) else {}
        link = str(grp.get("invite_link") or "")

        j += 1
        attempts += 1

        if await _is_session_banned(redis, session_base):
            continue
        until = await _cooldown_until(redis, session_base)
        if until and datetime.now(timezone.utc) < until:
            continue

        if not link:
            continue

        await _bump_metric(redis, "join_attempts", 1)
        h = _invite_hash(link)
        if not h:
            await _bump_metric(redis, "joins_failed", 1)
            continue

        try:
            async with async_telegram_client(session_base, parameters) as client:
                if not await client.is_user_authorized():
                    await _mark_banned(redis, session_base)
                    await _bump_metric(redis, "bans", 1)
                    continue
                await _ensure_factory_profile(client, redis, session_base)
                await client(ImportChatInviteRequest(h))
            await _bump_metric(redis, "joins_ok", 1)
            state["join_flat_idx"] = j
            await _redis_json_set(redis, KEY_STATE, state)
            all_bases = _discover_session_bases(_resolve_sessions_dir(str(state.get("sessions_dir", ""))))
            await _sync_active_sessions(redis, all_bases or all_sessions)
            carry = dict(parameters)
            carry.pop("__redis__", None)
            if j < flat_max:
                await _enqueue_task("swarm.community_factory.join_tick", carry)
            else:
                state["phase"] = "chatting" if state.get("chat_enabled") else "complete"
                await _redis_json_set(redis, KEY_STATE, state)
                if state.get("chat_enabled"):
                    await _enqueue_task("swarm.community_factory.converse_tick", carry)
            return {"status": "completed", "joined": True, "join_flat_idx": j}

        except ValueError as exc:
            log.warning("factory_join_creds_missing", session=session_base[:32], error=str(exc))
            await _bump_metric(redis, "joins_failed", 1)
            continue
        except Exception as exc:
            kind = classify_telethon_account_error(exc)
            if kind == "ban":
                await _mark_banned(redis, session_base)
                await _bump_metric(redis, "bans", 1)
                continue
            if kind == "flood":
                sec = int(flood_wait_seconds(exc) * 1.1) + 1
                await _set_cooldown(redis, session_base, sec)
                await _bump_metric(redis, "flood_waits", 1)
                state["join_flat_idx"] = max(0, j - 1)
                await _redis_json_set(redis, KEY_STATE, state)
                carry = dict(parameters)
                carry.pop("__redis__", None)
                await _enqueue_task("swarm.community_factory.join_tick", carry)
                return {"status": "deferred", "reason": "flood_wait"}
            await _bump_metric(redis, "joins_failed", 1)
            log.debug("factory_join_failed", session=session_base[:32], error=str(exc))

    state["join_flat_idx"] = j
    await _redis_json_set(redis, KEY_STATE, state)
    carry = dict(parameters)
    carry.pop("__redis__", None)
    if j < flat_max:
        await _enqueue_task("swarm.community_factory.join_tick", carry)
    else:
        state["phase"] = "chatting" if state.get("chat_enabled") else "complete"
        await _redis_json_set(redis, KEY_STATE, state)
        if state.get("chat_enabled"):
            await _enqueue_task("swarm.community_factory.converse_tick", carry)

    return {"status": "completed", "joined": False, "join_flat_idx": j, "exhausted": j >= flat_max}


@registry.register("swarm.community_factory.burst_reply_chain")
async def community_factory_burst_reply_chain(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    After a news opener, send N quick replies (2–15s apart) as random pool accounts, all reply_to the opener.
    """
    redis = parameters.get("__redis__")
    api_key = _resolve_api_key(parameters)
    openai_key = _resolve_openai_key(parameters)
    gid_int = int(parameters.get("burst_group_id") or 0)
    reply_mid = int(parameters.get("burst_reply_to_msg_id") or 0)
    count = int(parameters.get("burst_count") or 5)
    if gid_int <= 0 or reply_mid <= 0 or count <= 0:
        return {"status": "failed", "error": "invalid burst parameters"}

    roles = await _redis_json_get(redis, KEY_ROLES)
    if not isinstance(roles, dict):
        return {"status": "failed", "error": "roles missing"}
    owners = list(roles.get("owners") or [])
    members = list(roles.get("members") or [])
    pool = owners + members
    if not pool:
        return {"status": "failed", "error": "no sessions"}

    aid, ahash = resolve_telethon_creds(pool[0], parameters)
    if not aid or not ahash:
        return {
            "status": "failed",
            "error": "Telethon api_id/api_hash missing: set TELEFIX_* or add .json next to sessions",
        }

    sent_n = 0
    for burst_i in range(count):
        if burst_i > 0:
            await asyncio.sleep(random.uniform(2.0, 15.0))
        random.shuffle(pool)
        picked = False
        for session_base in pool:
            if await _is_session_banned(redis, session_base):
                continue
            until = await _cooldown_until(redis, session_base)
            if until and datetime.now(timezone.utc) < until:
                continue
            try:
                async with async_telegram_client(session_base, parameters) as client:
                    if not await client.is_user_authorized():
                        await _mark_banned(redis, session_base)
                        await _bump_metric(redis, "bans", 1)
                        continue
                    await _ensure_factory_profile(client, redis, session_base)
                    ent = await client.get_entity(gid_int)
                    hist = await client.get_messages(ent, limit=RECENT_GROUP_MSG_CAP)
                    chronological = list(hist)
                    chronological.reverse()
                    refs = _message_refs_newest_first(chronological)
                    last_five = _last_five_prompt_block(refs)
                    recent_texts = [t for _, t in reversed(refs)] if refs else []
                    anchor_preview = ""
                    try:
                        ams = await client.get_messages(ent, ids=reply_mid)
                        m0 = ams[0] if ams else None
                        if m0 is not None:
                            anchor_preview = str(getattr(m0, "message", None) or "")[:800]
                    except Exception:
                        anchor_preview = ""
                    active_record = await _active_topic_read(redis, gid_int)
                    active_topic_line = (
                        str(active_record.get("text") or "").strip() if isinstance(active_record, dict) else None
                    ) or None
                    topic = random.choice(FACTORY_TOPICS)
                    stance = random.choice(AMCHA_STANCES_HE)
                    typo_must = random.random() < 0.15
                    turn = await _generate_amcha_turn(
                        api_key,
                        topic,
                        openai_key,
                        role="replier",
                        stance_he=stance,
                        last_five_block=last_five,
                        typo_must_correct=typo_must,
                        anchor_preview=anchor_preview or None,
                        recent_texts=recent_texts,
                        active_topic_line=active_topic_line,
                        opener_fresh_event=False,
                        news_opener=False,
                    )
                    body = _finalize_primary_message(turn["primary_message"])
                    async with client.action(ent, "typing"):
                        await asyncio.sleep(random.uniform(1.0, 4.0))
                    msgs = await _send_amcha_messages(
                        client,
                        ent,
                        primary=body,
                        needs_correction=bool(turn.get("needs_correction")),
                        correction=str(turn.get("correction_message") or ""),
                        reply_to_id=reply_mid,
                        parse_mode=None,
                    )
                    sent_n += len(msgs)
                    await _bump_metric(redis, "messages_sent", len(msgs))
                    picked = True
                    break
            except ValueError as exc:
                log.warning("factory_burst_creds_missing", error=str(exc))
            except Exception as exc:
                kind = classify_telethon_account_error(exc)
                if kind == "ban":
                    await _mark_banned(redis, session_base)
                    await _bump_metric(redis, "bans", 1)
                elif kind == "flood":
                    sec = int(flood_wait_seconds(exc) * 1.1) + 1
                    await _set_cooldown(redis, session_base, sec)
                    await _bump_metric(redis, "flood_waits", 1)
                else:
                    log.debug("factory_burst_send_failed", error=str(exc))
        if not picked:
            log.debug("factory_burst_skipped_no_session")

    return {"status": "completed", "burst_replies_attempted": count, "messages_sent": sent_n}


@registry.register("swarm.community_factory.converse_tick")
async def community_factory_converse_tick(parameters: dict[str, Any]) -> dict[str, Any]:
    redis = parameters.get("__redis__")
    api_key = _resolve_api_key(parameters)
    openai_key = _resolve_openai_key(parameters)

    roles = await _redis_json_get(redis, KEY_ROLES)
    groups = await _redis_json_get(redis, KEY_GROUPS)
    state = await _redis_json_get(redis, KEY_STATE)
    if not isinstance(roles, dict) or not isinstance(groups, list) or not groups:
        return {"status": "failed", "error": "missing roles or groups"}
    if not isinstance(state, dict):
        return {"status": "failed", "error": "state missing"}

    owners = list(roles.get("owners") or [])
    members = list(roles.get("members") or [])
    all_sessions = owners + members
    if not all_sessions:
        return {"status": "failed", "error": "no sessions"}

    aid, ahash = resolve_telethon_creds(all_sessions[0], parameters)
    if not aid or not ahash:
        return {
            "status": "failed",
            "error": "Telethon api_id/api_hash missing: set TELEFIX_* or add .json next to sessions",
        }

    stop_after = int(
        state.get("converse_chain_limit")
        or parameters.get("converse_ticks")
        or os.getenv("COMMUNITY_FACTORY_CONVERSE_CHAIN", "5000")
    )
    cidx = int(state.get("converse_idx", 0))
    if cidx >= stop_after:
        state["phase"] = "complete"
        await _redis_json_set(redis, KEY_STATE, state)
        return {"status": "completed", "phase": "chat_cap"}

    gi = cidx % len(groups)
    si = cidx % len(all_sessions)
    session_base = all_sessions[si]
    grp = groups[gi]
    group_id = grp.get("group_id")
    carry = dict(parameters)
    carry.pop("__redis__", None)
    if group_id is None:
        await _enqueue_task("swarm.community_factory.converse_tick", carry)
        return {"status": "failed", "error": "group_id missing"}

    state["converse_idx"] = cidx + 1
    await _redis_json_set(redis, KEY_STATE, state)

    async def _enqueue_next() -> None:
        await _enqueue_task("swarm.community_factory.converse_tick", carry)

    await asyncio.sleep(random.uniform(900.0, 2700.0))

    if await _is_session_banned(redis, session_base):
        await _enqueue_next()
        return {"status": "skipped", "reason": "banned"}

    until = await _cooldown_until(redis, session_base)
    if until and datetime.now(timezone.utc) < until:
        await _enqueue_next()
        return {"status": "deferred", "reason": "cooldown"}

    try:
        import telethon  # noqa: F401
    except ImportError:
        return {"status": "failed", "error": "telethon not installed"}

    gid_int = int(group_id)
    thread_ids = await _thread_ids_read(redis, gid_int)
    has_thread = len(thread_ids) > 0
    role = _roll_thread_role(has_thread)
    if role == "replier" and not has_thread:
        role = "opener"
    if role == "reactor" and not has_thread:
        role = "opener"

    topic = random.choice(FACTORY_TOPICS)
    active_record = await _active_topic_read(redis, gid_int)
    event_threshold_sec = random.uniform(7200.0, 10800.0)
    opener_fresh_event = False
    active_topic_line: str | None = None
    if role == "opener":
        opener_fresh_event = _active_topic_should_refresh(active_record, event_threshold_sec)
        if not opener_fresh_event and active_record:
            active_topic_line = str(active_record.get("text") or "").strip() or None
    elif role == "replier" and active_record:
        active_topic_line = str(active_record.get("text") or "").strip() or None

    if role == "lurk":
        await _enqueue_next()
        return {"status": "completed", "action": "lurk"}

    if role == "reactor":
        anchor_id = thread_ids[-1]
        try:
            async with async_telegram_client(session_base, parameters) as client:
                if not await client.is_user_authorized():
                    await _mark_banned(redis, session_base)
                    await _bump_metric(redis, "bans", 1)
                    await _enqueue_next()
                    return {"status": "skipped", "reason": "unauthorized"}
                ent = await client.get_entity(gid_int)
                await _ensure_factory_profile(client, redis, session_base)
                ok = await _send_thread_reaction(client, ent, anchor_id)
                if ok:
                    await _bump_metric(redis, "messages_sent", 1)
        except ValueError as exc:
            log.warning("factory_converse_creds_missing", error=str(exc))
        except Exception as exc:
            kind = classify_telethon_account_error(exc)
            if kind == "ban":
                await _mark_banned(redis, session_base)
                await _bump_metric(redis, "bans", 1)
            elif kind == "flood":
                sec = int(flood_wait_seconds(exc) * 1.1) + 1
                await _set_cooldown(redis, session_base, sec)
                await _bump_metric(redis, "flood_waits", 1)
            else:
                log.debug("factory_converse_reactor_failed", error=str(exc))
        await _enqueue_next()
        return {"status": "completed", "action": "reactor"}

    reply_to_id: int | None = None
    stance = random.choice(AMCHA_STANCES_HE)
    typo_must_correct = random.random() < 0.15

    sent_list: list[Any] = []
    try:
        async with async_telegram_client(session_base, parameters) as client:
            if not await client.is_user_authorized():
                await _mark_banned(redis, session_base)
                await _bump_metric(redis, "bans", 1)
                await _enqueue_next()
                return {"status": "skipped", "reason": "unauthorized"}
            await _ensure_factory_profile(client, redis, session_base)
            ent = await client.get_entity(gid_int)
            refs_newest_first: list[tuple[int, str]] = []
            recent_texts: list[str] = []
            try:
                hist = await client.get_messages(ent, limit=RECENT_GROUP_MSG_CAP)
                if hist:
                    chronological = list(hist)
                    chronological.reverse()
                    refs_newest_first = _message_refs_newest_first(chronological)
                    recent_texts = [t for _, t in reversed(refs_newest_first)]
            except Exception as exc:
                log.debug("factory_recent_messages_failed", group_id=gid_int, error=str(exc))
            last_five_block = _last_five_prompt_block(refs_newest_first)
            pick_pool = refs_newest_first[:5]
            if pick_pool and random.random() < 0.6:
                reply_to_id = random.choice(pick_pool)[0]
            ref_by_id = {mid: txt for mid, txt in refs_newest_first}
            anchor_preview: str | None = ref_by_id.get(reply_to_id) if reply_to_id is not None else None
            if reply_to_id is not None and anchor_preview is None:
                try:
                    msgs = await client.get_messages(ent, ids=reply_to_id)
                    m0 = msgs[0] if msgs else None
                    if m0 is not None:
                        anchor_preview = (getattr(m0, "message", None) or "")[:500]
                except Exception:
                    anchor_preview = None
            news_opener = role == "opener" and opener_fresh_event
            turn = await _generate_amcha_turn(
                api_key,
                topic,
                openai_key,
                role="opener" if role == "opener" else "replier",
                stance_he=stance,
                last_five_block=last_five_block,
                typo_must_correct=typo_must_correct,
                anchor_preview=anchor_preview,
                recent_texts=recent_texts,
                active_topic_line=active_topic_line,
                opener_fresh_event=opener_fresh_event,
                news_opener=news_opener,
            )
            use_md = False
            primary_out = _finalize_primary_message(turn["primary_message"])
            if news_opener:
                url = await _maybe_tinyurl_shorten(str(turn.get("article_url") or ""))
                primary_out, use_md = _format_opener_with_md_link(
                    turn["primary_message"],
                    url,
                    str(turn.get("link_label") or ""),
                )
            async with client.action(ent, "typing"):
                await asyncio.sleep(random.uniform(2.0, 8.0))
            sent_list = await _send_amcha_messages(
                client,
                ent,
                primary=primary_out,
                needs_correction=bool(turn.get("needs_correction")),
                correction=str(turn.get("correction_message") or ""),
                reply_to_id=reply_to_id,
                parse_mode="md" if use_md else None,
            )
            sent = sent_list[0] if sent_list else None
            mid = getattr(sent, "id", None) if sent is not None else None
            if mid is not None and role == "opener":
                await _thread_ids_push(redis, gid_int, int(mid))
            if role == "opener" and opener_fresh_event:
                await _active_topic_write(redis, gid_int, primary_out)
            if mid is not None and news_opener:
                burst_carry = dict(carry)
                burst_carry["burst_group_id"] = gid_int
                burst_carry["burst_reply_to_msg_id"] = int(mid)
                burst_carry["burst_count"] = random.randint(4, 8)
                await _enqueue_task("swarm.community_factory.burst_reply_chain", burst_carry)
        await _bump_metric(redis, "messages_sent", len(sent_list))
    except ValueError as exc:
        log.warning("factory_converse_creds_missing", error=str(exc))
    except Exception as exc:
        kind = classify_telethon_account_error(exc)
        if kind == "ban":
            await _mark_banned(redis, session_base)
            await _bump_metric(redis, "bans", 1)
        elif kind == "flood":
            sec = int(flood_wait_seconds(exc) * 1.1) + 1
            await _set_cooldown(redis, session_base, sec)
            await _bump_metric(redis, "flood_waits", 1)
        else:
            log.warning("factory_converse_send_failed", error=str(exc))

    await _enqueue_next()
    return {"status": "completed", "action": role}
