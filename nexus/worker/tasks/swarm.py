"""
swarm.community_factory — Israeli Community Factory: role split, group creation,
distributed joins with FloodWait / ban handling, and LLM-driven Hebrew chatter.

Redis namespace: nexus:swarm:factory:*, nexus:swarm:lore_facts
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import time
import re
import uuid
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote

import structlog

from nexus.modules.community_vibe import parse_json_object
from nexus.services.anti_parrot_shield import MAX_REGENERATION_RETRIES, is_too_similar_to_recent
from nexus.services.media_opsec import make_image_upload_salt_seed, prepare_jpeg_png_for_telegram_upload
from nexus.services.tg_message_text import (
    llm_media_prefix_for_message,
    purge_absolute_news_source_blacklist,
    strip_trailing_israeli_news_outlet,
    telethon_display_text,
)
from nexus.services.tg_participant_privilege import sender_of_message_is_owner_or_admin
from nexus.shared.personas import (
    PERSONA_ARCHETYPE_PROMPTS,
    deterministic_archetype_index,
    effective_persona_prompt_for_archetype_index,
    session_is_asleep_jerusalem,
)
from nexus.shared.swarm_logs_redis import ISSUE_HALLUCINATION, ISSUE_PARROT_BUG, publish_swarm_log_event
from nexus.worker.services.israeli_telegram_profile import ensure_israeli_factory_profile
from nexus.shared.telethon_human_hesitation import schedule_post_send_human_hesitation
from nexus.worker.services.tg_session import (
    async_telegram_client,
    classify_telethon_account_error,
    flood_wait_seconds,
    resolve_telethon_creds,
)
from nexus.worker.task_registry import registry
from nexus.worker.tasks.reactions import send_passive_group_reaction

log = structlog.get_logger(__name__)

KEY_ROLES = "nexus:swarm:factory:roles"
KEY_GROUPS = "nexus:swarm:factory:groups"
KEY_STATE = "nexus:swarm:factory:state"
KEY_BANNED = "nexus:swarm:factory:banned"
KEY_COOLDOWNS = "nexus:swarm:factory:cooldowns"
KEY_METRICS = "nexus:swarm:factory:metrics"
KEY_PROFILE_GATE = "nexus:swarm:factory:profile_gate"
KEY_RECENT_OUTGOING = "nexus:swarm:factory:recent_outgoing"
GROUP_RECENT_SENT_PREFIX = "nexus:swarm:factory:group_recent_sent:"
KEY_FACTORY_POOL_UIDS = "nexus:swarm:factory:pool_user_ids"
KEY_MEDIA_SLOT_PREFIX = "nexus:swarm:factory:media_slot:"
THREAD_KEY_PREFIX = "nexus:swarm:factory:thread:"
ACTIVE_TOPIC_KEY_PREFIX = "nexus:swarm:factory:active_topic:"
# Global ADHD derailment: count every factory bot Telegram send, arm at 40, seed + 3 contagion replies.
KEY_GLOBAL_SWARM_MESSAGE_COUNT = "nexus:swarm:global_message_count"
KEY_DERAIL_STATE = "nexus:swarm:derail_state"
KEY_DERAIL_CONTAGION_LEFT = "nexus:swarm:derail_contagion_left"
KEY_DERAIL_ANCHOR = "nexus:swarm:derail_anchor_text"
GLOBAL_SWARM_DERAIL_THRESHOLD = 40
DERAIL_SEED_SYSTEM_OVERRIDE = (
    "IGNORE the news. You just remembered something else. Ask the group a random, everyday Israeli question. "
    "Examples: traffic on Ayalon highway, a recommendation for a Netflix show, complaining about the heat, "
    "or asking if anyone knows a good plumber in Petah Tikva."
)
DERAIL_CONTAGION_SYSTEM_SUFFIX = (
    "DERAILMENT (contagion): IGNORE the news digest. The group is on an everyday side-topic question below. "
    "Reply to that question in casual Israeli Hebrew — not politics, not headlines, no news parroting."
)
_DERAIL_CLAIM_LUA = """
local st = redis.call('GET', KEYS[1])
if st == false then st = 'idle' end
if st == 'need_seed' then
  redis.call('SET', KEYS[1], 'awaiting_anchor')
  return 'seed'
end
if st == 'contagion' then
  local rem = tonumber(redis.call('GET', KEYS[2]) or '0')
  if rem > 0 then
    local after = redis.call('DECR', KEYS[2])
    return 'contagion:' .. tostring(after)
  end
end
return 'none'
"""
THREAD_ID_CAP = 5
RECENT_GROUP_MSG_CAP = 15
RECENT_GROUP_MSG_MAX_CHARS = 180
RECENT_OUTGOING_CAP = 200
RECENT_OUTGOING_PROMPT_LINES = 40

# Hebrew swarm LLM decoding: stable temperature + top_p (avoid collapsed/repetitive local output).
AMCHA_LLM_TEMPERATURE = 0.85
AMCHA_LLM_TOP_P = 0.9
# Anti-duplication shield vs recent group + cross-account lines (word Jaccard, same spirit as content_factory).
AMCHA_ANTI_DUP_JACCARD_THRESHOLD = 0.85
AMCHA_OLLAMA_ANTI_DUP_MAX_TRIES = 3

_RICH_ACTION_TYPES = frozenset({"text", "text_with_emoji", "sticker", "gif", "image"})
_DEFAULT_STICKER_PACKS = ["AnimatedEmojies", "HotCherry"]

# When Redis is unavailable, avoid re-running profile checks every tick (Latin names stay "non_israeli" per heuristic).
_factory_profile_verified_local: set[str] = set()
# Per-group last sent lines when Redis is missing (best-effort within this process only).
_factory_group_recent_sent_local: dict[int, list[str]] = {}
# When Redis is None: monotonic deadline until another factory bot may send sticker/gif/image in this group.
_factory_media_slot_local: dict[int, float] = {}

GROUPS_TARGET_PER_OWNER = 20
_RANKSEO_REPORT_PATH = (
    Path(__file__).resolve().parents[3] / "vault" / "data" / "group_factory_rankseo_report.json"
)
REACTION_EMOJIS = ["🔥", "😂", "💀", "🤯", "👀", "😱", "💪", "🤦", "😅", "❤️", "🙏"]
THREAD_REACTION_EMOJIS = ["👍", "🤦‍♂️", "🤬"]

AMCHA_ISRAEL_SYSTEM_PROMPT = (
    "You are an everyday Israeli ('עמך ישראל') in a local Telegram group — impatient, slangy, not a news reader.\n"
    "RULES:\n"
    "- LENGTH: Every primary line is 2–10 Hebrew words. Bursts only; no mini-articles.\n"
    "- NO HEADLINE PARROTING: Never paste or lightly reword RSS/news titles. Internalize, then react in your own words.\n"
    "- NO OUTLETS: Never output '- Ynet', '- מעריב', '- calcalist', '- N12', site names, or '[source]' tags.\n"
    "- NO LAZY OPENERS: Forbidden starts include 'שמעתם כבר', 'דיווח:', 'לפי כותרות', 'חדשות:' — jump into the take.\n"
    "- When your task is to REPLY in-thread: do not restate facts from the quoted message; only opinion, joke, gripe, or disagreement (persona-true).\n"
    "- Read the last 5 messages and react uniquely. Argue, laugh, or derail (e.g. second-hand sale) without sounding like a bot.\n"
    "- Tone: casual, cynical, authentic slang ('אחי', 'וואלה', 'בדוק', 'הזייה'). Not formal.\n"
    "- Do NOT sound like an AI: avoid perfect essay Hebrew and textbook phrasing."
)

# Few-shot alignment block (verbatim requirement for Israeli Swarm prompting).
AMCHA_FEW_SHOT_BLOCK = """EXAMPLES OF BAD OUTPUTS (DO NOT DO THIS):
'שמעתם כבר על X - מעריב'
'דיווח: רה"מ אמר ש... (כותרת מועתקת)'
'אני חושב שצריך לחכות לעוד פרטים לפני שמספקים.'
'וואי, אם זה נכון זה משנה את התמונה לגמרי.'  (as a reply that only repeats the previous message)

EXAMPLES OF GOOD, AUTHENTIC OUTPUTS (DO THIS):
'חארטה רצח'
'אמאלה איזה פחד'
'תכלס נו'
'בדוק עובדים עלינו חחח'
'הזייה מה שהולך פה'
'אין מצב אחי'"""

# Twelve fixed archetypes — prompts + sleep windows: ``nexus.shared.personas``.
PERSONA_ARCHETYPES: list[str] = list(PERSONA_ARCHETYPE_PROMPTS)

# Twelve geo anchors — second index from same MD5 digest (different byte range).
GEO_ANCHORS: list[str] = [
    "GEO-ANCHOR פתח תקווה: פרברים, פקקים, חניה, חיי יום-יום.",
    "GEO-ANCHOR אילת: חום, תיירים, 'שם זה אחרת'.",
    "GEO-ANCHOR תל אביב: קצב מהיר, ציניזם עירוני, 'תכלס'.",
    "GEO-ANCHOR ירושלים: מתח דתי-חילוני ברקע, שפה חמה.",
    "GEO-ANCHOR חיפה: גרים על הגבעה/מורדות, רוח צפון.",
    "GEO-ANCHOR באר שבע: דרום, קצת מרוחק מהמרכז, ישירות.",
    "GEO-ANCHOR אשדוד: נמל, עבודה, משפחות.",
    "GEO-ANCHOR נתניה: שרון, קרוב לים, טון בינוני.",
    "GEO-ANCHOR רמת גן: ליד גוש דן, פרגודי בניין.",
    "GEO-ANCHOR חולון/בת ים: גוש דן, לא 'מרכז העולם' אבל מגניב.",
    "GEO-ANCHOR קריית שמונה/צפון: קרוב לגבול, טון מקומי.",
    "GEO-ANCHOR רעננה/השרון: פרבר ירוק, קצת 'בועה' אבל עדיין עממי.",
]

# Often-weak final letters in casual Hebrew (for optional typo-like trimming on very short lines).
_HEBREW_SILENT_FINAL_LETTERS = frozenset("אהי")

AMCHA_STANCES_HE = [
    "עמדה לתגובה שלך: הסכם בעוצמה, ממש תתלהב.",
    "עמדה לתגובה שלך: לא מסכים בחום, תתנגד אגרסיבית.",
    "עמדה לתגובה שלך: תעשה בדיחה על זה.",
    "עמדה לתגובה שלך: תשנה נושא בפתאומיות (אבל עדיין טבעי לקבוצה).",
]

AMCHA_STANCES_PRIVILEGED_HE = [
    "עמדה לתגובה שלך: היה מנומס וקצר; אל תתעמת ואל תיכנס לוויכוח.",
    "עמדה לתגובה שלך: הסכם בעדינות או הוסף הערה חיובית קטנה.",
    "עמדה לתגובה שלך: שאל שאלה נחמדה או הודה על השיתוף בלי להתנגד.",
]

AMCHA_PRIVILEGED_SYSTEM_PROMPT = (
    "You are a casual Israeli Telegram group member. The line you reply to is from a group OWNER or ADMIN.\n"
    "RULES:\n"
    "- 2–10 words. Brief, friendly, respectful. No arguing, insults, profanity, or slurs.\n"
    "- Do not repeat their facts; short agreement, thanks, or gentle follow-up only.\n"
    "- No headline paste, no outlet names ('ynet', 'N12', etc.).\n"
    "- Informal Hebrew OK ('וואלה', 'אחי'); never confrontational. Do NOT sound like an AI."
)

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


def _split_roles_rankseo(bases: list[str]) -> tuple[list[str], list[str]]:
    """RANKSEO / Group Factory: every session creates (round-robin) and joins others' groups."""
    if not bases:
        return [], []
    return list(bases), list(bases)


def _peer_channel_id(raw: int) -> int:
    """Normalize stored Telegram channel id to a positive channel id for PeerChannel."""
    x = abs(int(raw))
    s = str(x)
    if len(s) > 6 and s.startswith("100"):
        return int(s[3:])
    return int(raw) if int(raw) > 0 else x


def _write_rankseo_report_file(groups: list[dict[str, Any]]) -> None:
    try:
        _RANKSEO_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        links = [g.get("invite_link") for g in groups if isinstance(g, dict) and g.get("invite_link")]
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "private_groups_total": len(groups),
            "private_invite_links_total": len(links),
            "groups": groups,
            "links_text": "\n".join(str(u) for u in links if u),
        }
        _RANKSEO_REPORT_PATH.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except OSError as exc:
        log.warning("rankseo_report_write_failed", error=str(exc))


async def _factory_after_joins_done(
    redis: Any, state: dict[str, Any], carry: dict[str, Any]
) -> None:
    """After the join matrix is done: optional invite export, then chat or complete."""
    iphases = [str(p).lower() for p in (state.get("init_phases") or [])]
    if state.get("rankseo_mode") and "export_invites" in iphases:
        state["phase"] = "exporting_invites"
        state["export_invite_idx"] = 0
        await _redis_json_set(redis, KEY_STATE, state)
        await _enqueue_task("swarm.community_factory.export_private_invites_tick", carry)
        return
    state["phase"] = "chatting" if state.get("chat_enabled") else "complete"
    await _redis_json_set(redis, KEY_STATE, state)
    if state.get("chat_enabled"):
        await _enqueue_task("swarm.community_factory.converse_tick", carry)


def _default_metrics() -> dict[str, Any]:
    return {
        "messages_sent": 0,
        "flood_waits": 0,
        "bans": 0,
        "joins_ok": 0,
        "joins_failed": 0,
        "join_attempts": 0,
        "groups_total": 0,
        "private_links_exported": 0,
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


async def _redis_sample_lore_fact(redis: Any) -> str | None:
    """Return one random stored lore line (uniform over the Redis list)."""
    if redis is None:
        return None
    try:
        facts = await redis.lrange(KEY_LORE_FACTS, 0, -1)
    except Exception as exc:
        log.debug("lore_redis_lrange_failed", error=str(exc))
        return None
    decoded: list[str] = []
    for raw in facts or []:
        s = raw.decode() if isinstance(raw, (bytes, bytearray)) else str(raw)
        s = s.strip()
        if s:
            decoded.append(s)
    if not decoded:
        return None
    return random.choice(decoded)


async def _maybe_sample_lore_for_amcha(redis: Any, *, privileged_anchor: bool) -> str | None:
    if privileged_anchor or redis is None:
        return None
    raw_prob = (os.getenv("NEXUS_SWARM_LORE_INJECT_PROB") or "1").strip()
    try:
        prob = float(raw_prob)
    except ValueError:
        prob = 1.0
    prob = max(0.0, min(1.0, prob))
    if prob < 1.0 and random.random() >= prob:
        return None
    return await _redis_sample_lore_fact(redis)


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


def _is_hebrew_char(ch: str) -> bool:
    return len(ch) == 1 and "\u0590" <= ch <= "\u05FF"


# Israeli Hebrew keyboard proximity (Qwerty-style layout) — one adjacent swap, post-LLM only.
_HEB_KEYBOARD_PROXIMITY: dict[str, tuple[str, ...]] = {
    "א": ("ע", "ט", "ר"),
    "ע": ("א", "י", "ח"),
    "ח": ("כ", "י", "ל", "ע"),
    "כ": ("ח", "ק", "ל", "ף"),
    "ך": ("ל", "כ", "י"),
    "ט": ("ת", "ו", "א", "ר"),
    "ת": ("ט", "מ", "צ", "ד"),
    "ק": ("כ", "ר", "ש"),
    "ש": ("ד", "ח", "ק"),
    "ר": ("א", "ט", "ק", "ד"),
    "ד": ("ש", "ג", "ת", "ר"),
    "י": ("ח", "ע", "ל", "ך"),
    "ל": ("כ", "ח", "ך", "י"),
    "ו": ("ט", "ה", "ן"),
    "ה": ("ו", "ב", "נ"),
    "ב": ("ה", "ס", "מ"),
    "מ": ("נ", "ת", "ב", "צ"),
    "נ": ("מ", "ה", "צ"),
    "צ": ("ת", "מ", "נ"),
    "ס": ("ב", "ש", "ז"),
    "ז": ("ס", "ג"),
    "ג": ("ד", "ז", "כ"),
}


def _inject_hebrew_typo(text: str) -> tuple[str, str | None]:
    """
    With probability 5%, corrupt one Hebrew token by swapping a letter to a keyboard neighbor.
    Returns (possibly_modified_text, original_correct_word) for a follow-up '*word' reply, or (text, None).
    """
    s = text or ""
    if not s.strip() or random.random() >= 0.05:
        return s, None
    spans = list(re.finditer(r"\S+", s))
    if not spans:
        return s, None
    candidates: list[re.Match[str]] = []
    for m in spans:
        w = m.group(0)
        if not any(_is_hebrew_char(c) for c in w):
            continue
        if not any(c in _HEB_KEYBOARD_PROXIMITY for c in w):
            continue
        candidates.append(m)
    if not candidates:
        return s, None
    m = random.choice(candidates)
    orig_word = m.group(0)
    chars = list(orig_word)
    positions = [i for i, c in enumerate(chars) if c in _HEB_KEYBOARD_PROXIMITY]
    if not positions:
        return s, None
    i = random.choice(positions)
    old_c = chars[i]
    alts = tuple(x for x in _HEB_KEYBOARD_PROXIMITY[old_c] if x != old_c)
    if not alts:
        return s, None
    chars[i] = random.choice(alts)
    new_word = "".join(chars)
    if new_word == orig_word:
        return s, None
    new_text = s[: m.start()] + new_word + s[m.end() :]
    return new_text, orig_word


async def _telethon_send_text_schedule_hesitation(
    client: Any, entity: Any, text: str, **kwargs: Any
) -> Any:
    msg = await client.send_message(entity, text, **kwargs)
    schedule_post_send_human_hesitation(client, entity, msg)
    return msg


async def _send_factory_plain_text_with_hebrew_typo(
    client: Any,
    entity: Any,
    body: str,
    *,
    reply_to_id: int | None,
    sent: list[Any],
) -> None:
    """Plain send_message with optional post-LLM Hebrew typo + delayed '*correct_word' reply."""
    chunk = (body or "")[:4096]
    out_text, correct_word = _inject_hebrew_typo(chunk)
    msg = await _telethon_send_text_schedule_hesitation(
        client, entity, out_text, reply_to=reply_to_id, parse_mode=None
    )
    sent.append(msg)
    if correct_word:
        original_msg_id = getattr(msg, "id", None)
        await asyncio.sleep(random.uniform(2.0, 5.0))
        fix = f"*{correct_word.strip()}"[:4096]
        if fix:
            msg2 = await _telethon_send_text_schedule_hesitation(
                client,
                entity,
                fix,
                reply_to=int(original_msg_id) if original_msg_id is not None else None,
                parse_mode=None,
            )
            sent.append(msg2)


def _anti_robot_message_text(text: str, *, short_mutations: bool = True) -> str:
    """
    Post-LLM anti-formal filter for outgoing chat lines: drop '#', trim trailing '.',
    and optionally mess with very short messages like fast Telegram typing.
    """
    s = (text or "").replace("#", "")
    s = re.sub(r"\s{2,}", " ", s).strip()
    s = _strip_trailing_periods_hebrew(s)
    if not short_mutations or not s:
        return s
    words = s.split()
    if len(words) >= 4 or random.random() >= 0.3:
        return s
    last = words[-1]
    if len(last) < 2:
        return s
    last_ch = last[-1]
    if random.random() < 0.5 and last_ch in _HEBREW_SILENT_FINAL_LETTERS and _is_hebrew_char(last_ch):
        words[-1] = last[:-1]
        return " ".join(words).strip()
    words[-1] = last + last_ch + last_ch
    return " ".join(words).strip()


def _apply_anti_robot_to_turn_dict(out: dict[str, Any], *, rich_media_mode: bool) -> dict[str, Any]:
    """Apply programmatic post-processing to primary / message_text / correction after JSON normalize."""
    if rich_media_mode:
        mt = str(out.get("message_text") or "").strip()
        pm = str(out.get("primary_message") or "").strip()
        base = mt or pm
        if base:
            filtered = _anti_robot_message_text(base, short_mutations=True)
            out["message_text"] = filtered
            out["primary_message"] = filtered
    else:
        pm = str(out.get("primary_message") or "").strip()
        if pm:
            out["primary_message"] = _anti_robot_message_text(pm, short_mutations=True)
    cm = str(out.get("correction_message") or "").strip()
    if cm:
        out["correction_message"] = _anti_robot_message_text(cm, short_mutations=False)
    return out


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


def _jaccard_word_similarity(a: str, b: str) -> float:
    """Word-level Jaccard similarity (0–1), Unicode-word friendly."""
    set_a = set(re.sub(r"[^\w\s]", "", (a or "").lower()).split())
    set_b = set(re.sub(r"[^\w\s]", "", (b or "").lower()).split())
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / len(set_a | set_b)


def _anti_duplication_shield_rejects_turn(
    turn: dict[str, Any],
    *,
    recent_texts: list[str] | None,
    global_recent_outgoing: list[str] | None,
    rich_media_mode: bool,
) -> bool:
    """
    Reject a parsed turn when the primary line is too close to recent group text
    or recent cross-account outgoing lines (local models often parrot despite prompt).
    """
    if rich_media_mode:
        at = str(turn.get("action_type") or "text").strip().lower()
        if at in ("sticker", "gif", "image"):
            mt = str(turn.get("message_text") or turn.get("primary_message") or "").strip()
            if len(mt) < 1:
                return True
    primary = _finalize_primary_message(
        str(turn.get("primary_message") or turn.get("message_text") or "")
    ).strip()
    if len(primary) < 2:
        return False
    refs: list[str] = []
    for src in list(recent_texts or []) + list(global_recent_outgoing or []):
        s = (src or "").strip()
        if s:
            refs.append(s)
    for ref in refs:
        if _jaccard_word_similarity(primary, ref) >= AMCHA_ANTI_DUP_JACCARD_THRESHOLD:
            return True
    return False


def _session_persona_seed(session_base: str) -> str:
    raw = (session_base or "").encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()[:24]


def _deterministic_persona_axes(session_base: str) -> tuple[str, str]:
    """MD5(session id/path) → fixed archetype + geo (stable per Telethon session file)."""
    raw = (session_base or "default").encode("utf-8", errors="ignore")
    d = hashlib.md5(raw).digest()
    ai = int.from_bytes(d[0:2], "big") % len(PERSONA_ARCHETYPES)
    gi = int.from_bytes(d[2:4], "big") % len(GEO_ANCHORS)
    return effective_persona_prompt_for_archetype_index(ai), GEO_ANCHORS[gi]


def _resolve_ollama_base_url() -> str:
    return (os.getenv("NEXUS_OLLAMA_BASE_URL") or os.getenv("OLLAMA_HOST") or "").strip().rstrip("/")


def _resolve_ollama_model() -> str:
    return (os.getenv("NEXUS_OLLAMA_MODEL") or "llama3").strip() or "llama3"


def _resolve_ollama_gatekeeper_model() -> str:
    """Tiny classifier on the Mac Mini (e.g. Llama-3-8B); defaults to main Ollama model."""
    return (
        (os.getenv("NEXUS_OLLAMA_GATEKEEPER_MODEL") or "").strip()
        or _resolve_ollama_model()
    )


def _ollama_quality_gate_enabled() -> bool:
    raw = (os.getenv("NEXUS_OLLAMA_QUALITY_GATE") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _turn_blob_for_quality_gate(turn: dict[str, Any]) -> str:
    parts: list[str] = []
    for key in (
        "primary_message",
        "message_text",
        "text",
        "link_label",
        "correction_message",
        "correction",
    ):
        v = str(turn.get(key) or "").strip()
        if v:
            parts.append(v)
    return "\n".join(parts).strip()


def _parse_gatekeeper_yes_no(raw: str | None) -> bool | None:
    """
    Interpret gatekeeper output. True = YES (gibberish or contains '- Ynet' — reject).
    False = NO (ok). None = unclear — treat as ok to avoid mangling good lines.
    """
    s = (raw or "").strip().upper()
    if not s:
        return None
    line = s.split("\n", 1)[0].strip()
    if line.startswith("YES"):
        return True
    if line.startswith("NO"):
        return False
    m = re.search(r"\b(YES|NO)\b", line)
    if m:
        return m.group(1) == "YES"
    return None


def _scrub_turn_news_leakage(turn: dict[str, Any]) -> dict[str, Any]:
    """Deterministic cleanup when local repair JSON fails."""
    out = dict(turn)
    for k in (
        "primary_message",
        "message_text",
        "text",
        "link_label",
        "correction_message",
        "correction",
    ):
        if k not in out:
            continue
        v = str(out.get(k) or "").strip()
        if not v:
            continue
        v2 = purge_absolute_news_source_blacklist(v)
        v2 = strip_trailing_israeli_news_outlet(v2)
        out[k] = v2
    return out


def _merge_ollama_text_fix_into_turn(
    turn: dict[str, Any], fix: dict[str, Any]
) -> dict[str, Any]:
    out = dict(turn)
    pm = str(fix.get("primary_message") or fix.get("text") or "").strip()
    if pm:
        mt = str(fix.get("message_text") or pm).strip() or pm
        out["primary_message"] = pm
        out["message_text"] = mt
    ll = str(fix.get("link_label") or "").strip()
    if ll or "link_label" in fix:
        out["link_label"] = ll
    return out


async def _ollama_quality_gate_classify_bad(
    base_url: str, model: str, hebrew_blob: str
) -> bool | None:
    user = (
        "Is this Hebrew message gibberish or does it contain '- Ynet'? Answer YES/NO.\n\n"
        f"{hebrew_blob[:4000]}"
    )
    raw = await _ollama_chat_completion_content(
        base_url,
        model,
        "",
        user,
        temperature=0.0,
        top_p=0.2,
        max_tokens=8,
        timeout_sec=35.0,
    )
    return _parse_gatekeeper_yes_no(raw)


async def _ollama_repair_cloud_turn_locally(
    base_url: str,
    model: str,
    blob: str,
) -> dict[str, Any] | None:
    sys_h = (
        "You repair Hebrew Telegram lines for a news-discussion group. "
        "Remove gibberish, fix broken Hebrew, and remove '- Ynet' and other news-site attribution. "
        "Output ONLY one JSON object, no markdown."
    )
    user = (
        "Fix these field lines (same intent, natural Israeli Hebrew, short like chat).\n\n"
        f"{blob[:3500]}\n\n"
        'Return only JSON: {"primary_message":"...","message_text":"...","link_label":"..."} '
        "Use the same keys that had content; message_text may equal primary_message; "
        "link_label must be generic Hebrew without site names or empty string."
    )
    raw = await _ollama_chat_completion_content(
        base_url,
        model,
        sys_h,
        user,
        temperature=0.35,
        top_p=0.85,
        max_tokens=256,
        timeout_sec=90.0,
    )
    if not raw:
        return None
    obj = parse_json_object(raw)
    return obj if isinstance(obj, dict) else None


async def _apply_cloud_llm_quality_gate(
    turn: dict[str, Any],
    *,
    rich_media_mode: bool,
    ollama_base: str,
) -> dict[str, Any]:
    """
    After Gemini/OpenAI: Mac Mini Ollama classifies the Hebrew blob; if bad, repair locally
    (no second paid call).
    """
    if not _ollama_quality_gate_enabled():
        return turn
    base = (ollama_base or "").strip().rstrip("/")
    if not base:
        return turn
    blob = _turn_blob_for_quality_gate(turn)
    if len(blob) < 2:
        return turn
    gk_model = _resolve_ollama_gatekeeper_model()
    is_bad = await _ollama_quality_gate_classify_bad(base, gk_model, blob)
    if is_bad is not True:
        return turn
    log.info("factory_ollama_quality_gate_reject", preview=blob[:100])
    fix_model = (os.getenv("NEXUS_OLLAMA_FIX_MODEL") or "").strip() or _resolve_ollama_model()
    fixed_obj = await _ollama_repair_cloud_turn_locally(base, fix_model, blob)
    if isinstance(fixed_obj, dict):
        merged = _merge_ollama_text_fix_into_turn(turn, fixed_obj)
        pm = str(merged.get("primary_message") or merged.get("message_text") or "").strip()
        if pm:
            return _apply_anti_robot_to_turn_dict(merged, rich_media_mode=rich_media_mode)
    scrubbed = _scrub_turn_news_leakage(turn)
    return _apply_anti_robot_to_turn_dict(scrubbed, rich_media_mode=rich_media_mode)


def _reading_delay_before_typing_seconds(*context_parts: str, wpm: int = 250) -> float:
    """Simulate reading incoming chat before showing the typing indicator (250 WPM default)."""
    combined = " ".join((p or "").strip() for p in context_parts if (p or "").strip())
    if not combined:
        return 0.35
    n_words = max(1, len(combined.split()))
    sec = (n_words / float(max(1, wpm))) * 60.0
    return max(0.25, min(sec, 60.0))


def _strip_trailing_periods_hebrew(text: str) -> str:
    """Human-flaw filter: drop trailing '.' on Hebrew-heavy lines (LLMs over-punctuate)."""
    s = text or ""
    if not any(_is_hebrew_char(c) for c in s):
        return s
    while s.endswith("."):
        s = s[:-1].rstrip()
    return s


def _global_outgoing_prompt_suffix(lines: list[str]) -> str:
    if not lines:
        return ""
    clipped = [ln[:160] for ln in lines[:RECENT_OUTGOING_PROMPT_LINES]]
    block = "\n".join(f"- {t}" for t in clipped)
    return (
        "\n\nחובה: אל תשכפל ואל תדמה לתוכן שהמערכת כבר שלחה לאחרונה מהחשבונות האחרים — שוני מוחלט.\n"
        "טקסטים שכבר נשלחו (אסור לחקות):\n"
        f"{block}\n"
    )


async def _redis_recent_outgoing_fetch(redis: Any) -> list[str]:
    if redis is None:
        return []
    try:
        raw = await redis.lrange(KEY_RECENT_OUTGOING, 0, RECENT_OUTGOING_PROMPT_LINES - 1)
    except Exception:
        return []
    out: list[str] = []
    for x in raw or []:
        if isinstance(x, (bytes, bytearray)):
            s = bytes(x).decode("utf-8", errors="ignore")
        else:
            s = str(x)
        s = s.strip()
        if s:
            out.append(s)
    return out


async def _redis_recent_outgoing_push(redis: Any, fragment: str) -> None:
    if redis is None:
        return
    frag = (fragment or "").strip()[:400]
    if not frag:
        return
    try:
        await redis.lpush(KEY_RECENT_OUTGOING, frag)
        await redis.ltrim(KEY_RECENT_OUTGOING, 0, RECENT_OUTGOING_CAP - 1)
    except Exception as exc:
        log.debug("factory_recent_outgoing_push_failed", error=str(exc))


def _factory_group_recent_sent_key(group_id: int) -> str:
    return f"{GROUP_RECENT_SENT_PREFIX}{int(group_id)}"


def _local_factory_group_recent_fetch(group_id: int) -> list[str]:
    return list(_factory_group_recent_sent_local.get(int(group_id), []))


def _local_factory_group_recent_push(group_id: int, fragment: str) -> None:
    from nexus.services.anti_parrot_shield import RECENT_SENT_CAP

    frag = (fragment or "").strip()[:600]
    if not frag:
        return
    gid = int(group_id)
    cur = _factory_group_recent_sent_local.setdefault(gid, [])
    cur.insert(0, frag)
    del cur[RECENT_SENT_CAP:]


async def _factory_group_recent_sent_fetch(redis: Any, group_id: int) -> list[str]:
    from nexus.services.anti_parrot_shield import RECENT_SENT_CAP

    if redis is None:
        return _local_factory_group_recent_fetch(group_id)
    try:
        raw = await redis.lrange(_factory_group_recent_sent_key(group_id), 0, RECENT_SENT_CAP - 1)
    except Exception:
        return []
    out: list[str] = []
    for x in raw or []:
        if isinstance(x, (bytes, bytearray)):
            s = bytes(x).decode("utf-8", errors="ignore")
        else:
            s = str(x)
        s = s.strip()
        if s:
            out.append(s)
    return out


async def _factory_group_recent_sent_push(redis: Any, group_id: int, fragment: str) -> None:
    from nexus.services.anti_parrot_shield import RECENT_SENT_CAP

    frag = (fragment or "").strip()[:600]
    if not frag:
        return
    if redis is None:
        _local_factory_group_recent_push(group_id, frag)
        return
    try:
        await redis.lpush(_factory_group_recent_sent_key(group_id), frag)
        await redis.ltrim(_factory_group_recent_sent_key(group_id), 0, RECENT_SENT_CAP - 1)
    except Exception as exc:
        log.debug("factory_group_recent_sent_push_failed", group_id=group_id, error=str(exc))


def _default_hebrew_media_companion(action: str, image_query: str) -> str:
    q = (image_query or "").lower()
    funny_kw = ("funny", "lol", "meme", "laugh", "comedy", "joke", "haha", "rofl", "lmao")
    if action == "sticker" or any(k in q for k in funny_kw):
        return random.choice(
            (
                "חחח חזק",
                "מדויק",
                "וואלה מצחיק",
                "הרגת אותי",
                "די חזק",
                "נכון לגמרי חח",
                "אמאלה",
            )
        )
    if any(k in q for k in ("wow", "crazy", "wild", "insane")):
        return random.choice(("וואלה הזייה", "לא ציפיתי", "חזק מדי"))
    if any(k in q for k in ("sad", "cry", "rip")):
        return random.choice(("איכס", "חבל", "מבאס"))
    return random.choice(("וואלה", "חזק", "אש", "מדויק"))


def _ensure_rich_media_message_text(turn: dict[str, Any]) -> dict[str, Any]:
    at = str(turn.get("action_type") or "text").strip().lower()
    if at not in ("sticker", "gif", "image"):
        return turn
    mt = str(turn.get("message_text") or turn.get("primary_message") or "").strip()
    if mt:
        return {**turn, "message_text": mt, "primary_message": mt}
    companion = _default_hebrew_media_companion(at, str(turn.get("image_query") or ""))
    return {**turn, "message_text": companion, "primary_message": companion}


async def _try_acquire_factory_media_slot(redis: Any, group_id: int) -> bool:
    ttl = int(os.getenv("COMMUNITY_FACTORY_MEDIA_LOCK_SEC", "900") or "900")
    ttl = max(120, min(ttl, 7200))
    now_m = time.monotonic()
    gid = int(group_id)
    if redis is None:
        until = _factory_media_slot_local.get(gid, 0.0)
        if now_m < until:
            return False
        _factory_media_slot_local[gid] = now_m + float(ttl)
        return True
    key = f"{KEY_MEDIA_SLOT_PREFIX}{gid}"
    try:
        ok = await redis.set(key, "1", nx=True, ex=int(ttl))
        return bool(ok)
    except Exception as exc:
        log.debug("factory_media_slot_redis_failed", group_id=gid, error=str(exc))
        return False


def _factory_turn_antiparrot_compare_text(
    turn: dict[str, Any],
    *,
    news_opener: bool,
    rich_media_mode: bool,
) -> str | None:
    """
    Comparable primary line for de-duplication. None => skip text similarity (e.g. empty opener).
    """
    if news_opener:
        pm = str(turn.get("primary_message") or turn.get("message_text") or "").strip()
        return _finalize_primary_message(pm) if pm else None
    if rich_media_mode:
        at = str(turn.get("action_type") or "").strip().lower()
        mt = _finalize_primary_message(str(turn.get("message_text") or turn.get("primary_message") or ""))
        if mt.strip():
            return mt.strip()
        if at in ("sticker", "gif", "image"):
            return mt.strip() or None
        return mt.strip() or None
    pm = str(turn.get("primary_message") or turn.get("message_text") or "").strip()
    out = _finalize_primary_message(pm)
    return out if out.strip() else None


def _parse_derail_claim_result(raw: Any) -> tuple[str | None, int | None]:
    s = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw or "")
    s = s.strip()
    if s == "seed":
        return "seed", None
    if s.startswith("contagion:"):
        tail = s.split(":", 1)[1].strip()
        try:
            return "contagion", int(tail)
        except ValueError:
            return "contagion", None
    return None, None


async def _derail_claim_for_turn(redis: Any) -> tuple[str | None, int | None]:
    if redis is None:
        return None, None
    try:
        raw = await redis.eval(_DERAIL_CLAIM_LUA, 2, KEY_DERAIL_STATE, KEY_DERAIL_CONTAGION_LEFT)
        return _parse_derail_claim_result(raw)
    except Exception as exc:
        log.debug("derail_claim_failed", error=str(exc))
        return None, None


async def _derail_restore_claim(
    redis: Any, kind: str | None, contagion_after: int | None
) -> None:
    if redis is None or not kind:
        return
    try:
        if kind == "seed":
            st = await redis.get(KEY_DERAIL_STATE)
            if st == "awaiting_anchor":
                await redis.set(KEY_DERAIL_STATE, "need_seed")
        elif kind == "contagion":
            await redis.incr(KEY_DERAIL_CONTAGION_LEFT)
    except Exception as exc:
        log.debug("derail_restore_failed", error=str(exc))


async def _derail_maybe_arm(redis: Any, new_count: int) -> None:
    if redis is None or new_count < GLOBAL_SWARM_DERAIL_THRESHOLD:
        return
    try:
        st = await redis.get(KEY_DERAIL_STATE)
        if st in (None, "", "idle"):
            await redis.set(KEY_DERAIL_STATE, "need_seed")
    except Exception as exc:
        log.debug("derail_arm_failed", error=str(exc))


async def _global_swarm_bump_after_send(
    redis: Any,
    *,
    n_bot_messages: int,
    derail_kind: str | None,
    contagion_after: int | None,
    seed_anchor_text: str | None,
) -> None:
    if redis is None or n_bot_messages <= 0:
        return
    try:
        latest = 0
        for _ in range(n_bot_messages):
            latest = int(await redis.incr(KEY_GLOBAL_SWARM_MESSAGE_COUNT))
            await _derail_maybe_arm(redis, latest)
        if derail_kind == "seed":
            anchor = (seed_anchor_text or "").strip()[:900] or "שאלה יומיומית"
            await redis.set(KEY_DERAIL_STATE, "contagion")
            await redis.set(KEY_DERAIL_CONTAGION_LEFT, 3)
            await redis.set(KEY_DERAIL_ANCHOR, anchor)
            log.info("swarm_derail_seed_sent", anchor_preview=anchor[:120])
        if derail_kind == "contagion" and contagion_after == 0:
            await redis.set(KEY_DERAIL_STATE, "idle")
            await redis.delete(KEY_DERAIL_ANCHOR, KEY_DERAIL_CONTAGION_LEFT)
            await redis.set(KEY_GLOBAL_SWARM_MESSAGE_COUNT, 0)
            log.info("swarm_derail_cycle_complete_reset")
    except Exception as exc:
        log.debug("global_swarm_bump_failed", error=str(exc))


async def _generate_unique_amcha_turn(redis: Any, group_id: int, **kwargs: Any) -> dict[str, Any] | None:
    """Up to 1 + MAX_REGENERATION_RETRIES generations; None => skip send (too similar to recent group lines)."""
    kwargs.pop("regeneration_attempt", None)
    kwargs["redis_for_news_digest"] = redis
    news_opener = bool(kwargs.get("news_opener"))
    rich_media_mode = bool(kwargs.get("rich_media_mode"))
    session_base = str(kwargs.get("session_base") or "")
    recent = await _factory_group_recent_sent_fetch(redis, group_id)
    last_compare = ""
    for attempt in range(1 + MAX_REGENERATION_RETRIES):
        turn = await _generate_amcha_turn(**kwargs, regeneration_attempt=attempt)
        if news_opener and turn is not None and redis is not None:
            oc = await read_openclaw_digest_overlay(redis)
            img = _first_http_url_from_mapping(oc or {}, _OPENCLAW_PUBSUB_IMAGE_KEYS)
            if img:
                turn = {**turn, "openclaw_image_url": img}
        compare = _factory_turn_antiparrot_compare_text(
            turn, news_opener=news_opener, rich_media_mode=rich_media_mode
        )
        if compare is None:
            return turn
        if not is_too_similar_to_recent(compare, recent):
            return turn
        last_compare = compare or last_compare
        log.debug(
            "factory_antiparrot_rejected",
            group_id=int(group_id),
            attempt=attempt,
            sample=compare[:80],
        )
    if redis is not None:
        ai = deterministic_archetype_index(session_base or "default")
        await publish_swarm_log_event(
            redis,
            {
                "issue": ISSUE_PARROT_BUG,
                "session_base": session_base,
                "archetype_index": ai,
                "sample": (last_compare or "")[:400],
                "group_id": int(group_id),
                "engine": "swarm_factory",
            },
        )
    return None


def _build_amcha_system_prompt(
    session_base: str,
    persona_seed: str | None,
    *,
    privileged_anchor: bool = False,
    community_lore_line: str | None = None,
) -> str:
    """
    System prompt: base Israeli swarm rules, few-shot BAD/GOOD examples,
    deterministic archetype + geo from MD5(session), and optional uniqueness seed.
    """
    arch, geo = _deterministic_persona_axes(session_base)
    if privileged_anchor:
        parts = [
            AMCHA_PRIVILEGED_SYSTEM_PROMPT,
            f"PERSONA (קבוע לחשבון — קול עקבי, אבל מכבד מול מנהלים):\n{arch}\n{geo}",
        ]
        if (persona_seed or "").strip():
            parts.append(
                f"Unique seed {persona_seed.strip()}: stay respectful to admins; still vary wording."
            )
        return "\n\n".join(parts)
    parts = [
        AMCHA_ISRAEL_SYSTEM_PROMPT,
        AMCHA_FEW_SHOT_BLOCK,
        f"PERSONA (קבוע לחשבון — תמיד אותו קול ומיקום מנטלי):\n{arch}\n{geo}",
    ]
    if (persona_seed or "").strip():
        parts.append(
            f"Your unique persona seed is {persona_seed.strip()}. You MUST output a completely unique response "
            "never seen before. NEVER use generic templates."
        )
    out = "\n\n".join(parts)
    lore = (community_lore_line or "").strip()
    if lore:
        out += (
            "\n\nCommunity inside joke: "
            + lore
            + "\nIf relevant, casually mock the user involved or bring this up naturally."
        )
    return out


def _message_text_for_factory_prompt(m: Any) -> str | None:
    raw = getattr(m, "message", None)
    t = str(raw).strip() if raw else ""
    if not t:
        t = telethon_display_text(m).strip()
    prefix = llm_media_prefix_for_message(m).strip()
    if t:
        line = f"{prefix} {t}".strip() if prefix else t
    elif prefix:
        line = prefix
    else:
        return None
    if len(line) > RECENT_GROUP_MSG_MAX_CHARS:
        line = line[: RECENT_GROUP_MSG_MAX_CHARS - 1] + "…"
    return line


def _consecutive_pool_message_tail(messages_newest_first: list[Any], pool_ids: set[int]) -> int:
    """How many newest messages in a row are from accounts in ``pool_ids`` (swarm factory users)."""
    n = 0
    for m in messages_newest_first:
        if m is None:
            continue
        sid = getattr(m, "sender_id", None)
        if sid is None:
            break
        if int(sid) in pool_ids:
            n += 1
        else:
            break
    return n


def _message_refs_newest_first(messages_oldest_first: list[Any]) -> list[tuple[int, str]]:
    """Telethon history reversed to chronological oldest-first; collect newest text messages first."""
    out: list[tuple[int, str]] = []
    for m in reversed(messages_oldest_first):
        if m is None:
            continue
        mid = getattr(m, "id", None)
        if mid is None:
            continue
        t = _message_text_for_factory_prompt(m)
        if not t:
            continue
        out.append((int(mid), t))
        if len(out) >= RECENT_GROUP_MSG_CAP:
            break
    return out


def _raw_had_trailing_news_outlet_echo(raw: str) -> bool:
    r = (raw or "").strip()
    if not r:
        return False
    return strip_trailing_israeli_news_outlet(r) != r


async def _maybe_publish_outlet_echo_issue(redis: Any, session_base: str, raw_llm: str) -> None:
    if redis is None or not (session_base or "").strip():
        return
    if not _raw_had_trailing_news_outlet_echo(raw_llm):
        return
    ai = deterministic_archetype_index(session_base)
    await publish_swarm_log_event(
        redis,
        {
            "issue": ISSUE_HALLUCINATION,
            "session_base": session_base,
            "archetype_index": ai,
            "sample": (raw_llm or "")[:400],
            "engine": "swarm_factory",
        },
    )


def _last_five_prompt_block(refs_newest_first: list[tuple[int, str]]) -> str:
    block = refs_newest_first[:5]
    if not block:
        return "אין הודעות אחרונות בקבוצה — תאלתר טבעי."
    chronological = list(reversed(block))
    lines = "\n".join(f"{i + 1}. {txt}" for i, (_, txt) in enumerate(chronological))
    return f"5 ההודעות האחרונות בקבוצה (לפי סדר כרונולוגי, מהישנה לחדשה):\n{lines}"


def _finalize_primary_message(text: str) -> str:
    s = _strip_hashtags_and_cleanup(text)
    s = strip_trailing_israeli_news_outlet(s)
    parts = s.split()
    if len(parts) > 10:
        parts = parts[:10]
    if len(parts) == 1 and parts[0]:
        parts.append(random.choice(["אחי", "תכלס", "וואלה", "נו"]))
    s = " ".join(parts)
    if (len(parts) <= 8 or len(s) <= 40) and any(_is_hebrew_char(c) for c in s):
        s = re.sub(r"[.!?]+\s*$", "", s).strip()
    return _strip_trailing_periods_hebrew(s)


def _finalize_correction_message(text: str) -> str:
    s = _strip_hashtags_and_cleanup(text)
    s = strip_trailing_israeli_news_outlet(s)
    return _strip_trailing_periods_hebrew(_cap_hebrew_words(s, 20))


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
    cm = str(obj.get("correction_message") or obj.get("correction") or "").strip()
    nc_raw = obj.get("needs_correction")
    if nc_raw is None:
        nc_raw = bool(cm)
    return {
        "primary_message": pm,
        "needs_correction": _coerce_bool(nc_raw),
        "correction_message": cm,
        "article_url": str(obj.get("article_url") or "").strip(),
        "link_label": str(obj.get("link_label") or "").strip(),
    }


def _normalize_rich_turn(obj: dict[str, Any]) -> dict[str, Any]:
    n = _normalize_amcha_dict(obj)
    raw_at = str(obj.get("action_type") or "text").strip().strip('"').strip("'")
    if "|" in raw_at:
        raw_at = raw_at.split("|")[0].strip()
    at = raw_at.lower().replace(" ", "_").replace("-", "_")
    if at not in _RICH_ACTION_TYPES:
        at = "text"
    mt = str(obj.get("message_text") or n["primary_message"] or "").strip()
    if not mt and n["primary_message"]:
        mt = str(n["primary_message"]).strip()
    if mt:
        n["primary_message"] = mt
    n["action_type"] = at
    n["message_text"] = mt
    n["image_query"] = str(obj.get("image_query") or "").strip()[:120]
    return n


def _parse_llm_json_object(raw: str) -> dict[str, Any] | None:
    t = (raw or "").strip()
    if not t:
        return None
    normalized = (
        t.replace("\u201c", '"')
        .replace("\u201d", '"')
        .replace("\u2018", "'")
        .replace("\u2019", "'")
        .replace("\ufeff", "")
    )
    for candidate in (t, normalized):
        try:
            obj = parse_json_object(candidate)
            if isinstance(obj, dict):
                return obj
        except Exception:
            continue
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
    text, typo_correct_word = _inject_hebrew_typo(text)
    try:
        msg = await _telethon_send_text_schedule_hesitation(
            client, entity, text, reply_to=reply_to_id, parse_mode=parse_mode
        )
        sent.append(msg)
    except Exception as exc:
        if parse_mode:
            log.debug("factory_send_md_fallback", error=str(exc))
            msg = await _telethon_send_text_schedule_hesitation(
                client, entity, text, reply_to=reply_to_id, parse_mode=None
            )
            sent.append(msg)
        else:
            raise
    if typo_correct_word:
        original_msg_id = getattr(msg, "id", None)
        await asyncio.sleep(random.uniform(2.0, 5.0))
        star_fix = f"*{typo_correct_word.strip()}"[:4096]
        if star_fix:
            sent.append(
                await _telethon_send_text_schedule_hesitation(
                    client,
                    entity,
                    star_fix,
                    reply_to=int(original_msg_id) if original_msg_id is not None else None,
                    parse_mode=None,
                )
            )
    if needs_correction and (correction or "").strip():
        await asyncio.sleep(random.uniform(2.0, 4.0))
        fix = _finalize_correction_message(correction)[:4096]
        if fix:
            msg2 = await _telethon_send_text_schedule_hesitation(
                client, entity, fix, reply_to=None, parse_mode=None
            )
            sent.append(msg2)
    return sent


async def _ollama_chat_completion_content(
    base_url: str,
    model: str,
    system_prompt: str,
    user_he: str,
    *,
    temperature: float,
    top_p: float,
    max_tokens: int,
    timeout_sec: float = 120.0,
) -> str | None:
    """Ollama OpenAI-compatible /api/chat — returns assistant message text."""
    import httpx

    url = f"{base_url.rstrip('/')}/api/chat"
    payload: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_he},
        ],
        "stream": False,
        "options": {
            "temperature": float(temperature),
            "top_p": float(top_p),
            "num_predict": int(max_tokens),
        },
    }
    try:
        async with httpx.AsyncClient(timeout=float(timeout_sec)) as client:
            r = await client.post(url, json=payload)
            r.raise_for_status()
            data = r.json()
    except Exception as exc:
        log.warning("factory_ollama_failed", error=str(exc))
        return None
    if not isinstance(data, dict):
        return None
    msg = data.get("message")
    if not isinstance(msg, dict):
        return None
    raw = str(msg.get("content") or "").strip()
    return raw or None


async def _generate_amcha_turn(
    api_key: str,
    topic: str,
    openai_key: str,
    *,
    session_base: str,
    role: Literal["opener", "replier"],
    stance_he: str,
    last_five_block: str,
    anchor_preview: str | None = None,
    recent_texts: list[str] | None = None,
    active_topic_line: str | None = None,
    opener_fresh_event: bool = False,
    news_opener: bool = False,
    persona_seed: str | None = None,
    rich_media_mode: bool = False,
    global_recent_outgoing: list[str] | None = None,
    privileged_anchor: bool = False,
    community_lore_line: str | None = None,
    regeneration_attempt: int = 0,
    derail_mode: str | None = None,
    derail_anchor_text: str | None = None,
    redis_for_news_digest: Any | None = None,
) -> dict[str, Any]:
    anti = _anti_duplication_prompt_suffix(list(recent_texts or []))
    anti += _global_outgoing_prompt_suffix(list(global_recent_outgoing or []))
    system_prompt = _build_amcha_system_prompt(
        session_base,
        persona_seed,
        privileged_anchor=privileged_anchor,
        community_lore_line=community_lore_line,
    )
    effective_stance = (
        random.choice(AMCHA_STANCES_PRIVILEGED_HE) if privileged_anchor else stance_he
    )

    core_analysis: dict[str, Any] | None = None
    nb_cached: Any = None
    overlay_early: dict[str, Any] | None = None
    if news_opener and redis_for_news_digest is not None:
        try:
            from nexus.services.recent_news_digest import get_tick_news_bundle_for_consumer
            from nexus.shared.intelligence_cache import compute_news_id, ensure_core_analysis

            overlay_early = await read_openclaw_digest_overlay(redis_for_news_digest)
            nb_cached = await get_tick_news_bundle_for_consumer(redis_for_news_digest)
            nt = (
                str((overlay_early or {}).get("headline") or "").strip()
                or (str(getattr(nb_cached, "anchor_title", "") or "").strip())
            )
            nd = (
                str((overlay_early or {}).get("content") or "").strip()
                or (str(getattr(nb_cached, "digest_text", "") or "").strip())
            )
            nl = (
                str((overlay_early or {}).get("article_url") or "").strip()
                or (str(getattr(nb_cached, "anchor_link", "") or "").strip())
            )
            if nt or nd or nl:
                nid = compute_news_id(nt, nl, nd)
                core_analysis = await ensure_core_analysis(
                    redis_for_news_digest,
                    news_id=nid,
                    anchor_title=nt,
                    anchor_link=nl,
                    digest_snippet=nd[:4000],
                    gemini_api_key=api_key,
                    holder_id=(session_base or "")[:80] or "session",
                )
        except Exception as exc:
            log.debug("factory_master_thought_prefetch_failed", error=str(exc))

    if rich_media_mode:
        json_schema = (
            "החזר אך ורק JSON תקף (בלי טקסט נוסף) עם המפתחות: "
            '"action_type","message_text","image_query",'
            '"primary_message" או "text","needs_correction","correction_message" או "correction","article_url","link_label". '
            "action_type חייב להיות אחד מ: text | text_with_emoji | sticker | gif | image. "
            "בערך לאורך זמן: ~70% text או text_with_emoji, ~10% sticker, ~10% gif, ~10% image — אל תציף מדיה. "
            "חובה ב-sticker/gif/image: message_text הוא תגובה קצרה בעברית (2–8 מילים) שמלווה את המדיה — "
            "למשל לסטיקר מצחיק: 'חחח חזק', 'מדויק', 'וואלה מצחיק'; לא להשאיר ריק. "
            "image_query — מילת מפתח באנגלית לחיפוש (למשל funny, coffee, traffic, shawarma). "
            "ב-text/text_with_emoji: מלא message_text (ועדיף גם primary_message באותו תוכן). "
            "article_url ו-link_label — מחרוזות; כשאין קישור חדשותי השאר ריק."
        )
        if privileged_anchor:
            json_schema += (
                " חובה: action_type חייב להיות text או text_with_emoji בלבד (בלי sticker/gif/image)."
            )
        if derail_mode in ("seed", "contagion"):
            json_schema += (
                " חובה (שוליים): action_type חייב להיות text או text_with_emoji בלבד; article_url ו-link_label ריקים."
            )
    else:
        json_schema = (
            "החזר אך ורק JSON תקף (בלי טקסט נוסף) במבנה: "
            '{"text":"...","needs_correction":true/false,"correction":"..."} — מותר גם primary_message/correction_message במקום text/correction. '
            "article_url ו-link_label — מחרוזות; כשאין קישור חדשותי השאר ריק."
        )
    typo_rule = (
        "needs_correction חייב להיות false; correction/correction_message ריקים. "
        "כתוב עברית תקנית וברורה — בלי טעויות כתיב מכוונות בפלט."
    )

    opener_news_clause = ""
    if news_opener:
        opener_news_clause = (
            "תפקיד: פותח חדשות. primary_message — 2–10 מילים בלבד: תגובה רגשית/צינית, לא כותרת מועתקת ולא שם אתר. "
            "בלי URL בגוף ההודעה. חובה article_url (https לאתר חדשות). "
            "link_label: עברית קצרה בלי שם עיתון/אתר (למשל: 'לכתבה המלאה', 'פה הרחבה')."
        )
        if rich_media_mode:
            opener_news_clause += (
                " חובה: action_type חייב להיות text או text_with_emoji בלבד (לא sticker/gif/image)."
            )

    if derail_mode == "seed":
        system_prompt = f"{DERAIL_SEED_SYSTEM_OVERRIDE}\n\n---\n\n{system_prompt}"
        user_he = (
            f"{last_five_block}\n\n{effective_stance}\n\n"
            "שכח מהחדשות ומהדאיג'סט. שאל את הקבוצה שאלה יומיומית אחת בעברית — קצר, עממי, לא פוליטיקה.\n"
            f"{typo_rule}\n{anti}\n{json_schema}"
        )
    elif derail_mode == "contagion":
        system_prompt = f"{system_prompt}\n\n{DERAIL_CONTAGION_SYSTEM_SUFFIX}"
        aq = (derail_anchor_text or "").strip()[:800] or "(שאלה יומיומית בקבוצה)"
        replier_media_ban = ""
        if rich_media_mode:
            replier_media_ban = (
                "חובה: אסור sticker/gif/image; רק text או text_with_emoji.\n"
            )
        user_he = (
            f"{last_five_block}\n\n{effective_stance}\n\n"
            f"{replier_media_ban}"
            f'מישהו שאל בקבוצה (שוליים, לא חדשות): "{aq}"\n'
            "תגיב לזה בקצרה — 2–10 מילים, טבעי, בלי כותרות ולא חדשות.\n"
            f"{typo_rule}\n{anti}\n{json_schema}"
        )
    elif role == "opener" and not news_opener:
        ctx = (active_topic_line or "").strip()[:400] or topic
        user_he = (
            f"{last_five_block}\n\n{effective_stance}\n\n"
            f'הקבוצה כבר רותחת סביב: "{ctx}". '
            "עוד משפט או שניים — זווית אחרת, לא פורמלי.\n"
            f"{opener_news_clause}\n{typo_rule}\n{anti}\n{json_schema}"
        )
    elif role == "opener" and news_opener:
        digest_hint = ""
        nb = nb_cached
        if nb is None and redis_for_news_digest is not None:
            try:
                from nexus.services.recent_news_digest import get_tick_news_bundle_for_consumer

                nb = await get_tick_news_bundle_for_consumer(redis_for_news_digest)
            except Exception as exc:
                log.debug("factory_news_opener_digest_bundle_failed", error=str(exc))
                nb = None
        parts: list[str] = []
        ov = overlay_early
        if ov:
            oh = str(ov.get("headline") or "").strip()
            oc = str(ov.get("content") or "").strip()
            if oh:
                parts.append(f"כותרת OpenClaw (רקע): {oh[:500]}")
            if oc:
                parts.append(f"תוכן OpenClaw (רקע): {oc[:1200]}")
            ex = ov.get("extra")
            if isinstance(ex, dict):
                for ek, ev in list(ex.items())[:16]:
                    if isinstance(ev, (str, int, float, bool)):
                        parts.append(f"מטא־דאטה {ek}: {str(ev)[:400]}")
        if nb is not None:
            if (nb.digest_text or "").strip():
                parts.append(f"שורות עדכון (רקע בלבד, לא להעתיק מילה במילה): {nb.digest_text[:1200]}")
            if (nb.anchor_title or "").strip():
                parts.append(f"כותרת מובילה (רקע): {nb.anchor_title[:400]}")
            if (nb.anchor_link or "").strip():
                parts.append(f"קישור מוביל (לשימוש ב-article_url אם מתאים): {nb.anchor_link[:2000]}")
        digest_hint = "\n".join(parts)
        digest_block = f"\n{digest_hint}\n" if digest_hint else "\n"
        user_he = (
            f"{last_five_block}\n\n{effective_stance}\n\n"
            f"הנחיה: שמועה/פלאש/מה זה עכשיו — קבוצת חדשות בטלגרם. "
            f'רקע רחב בלבד: "{topic}".{digest_block}'
            f"{opener_news_clause}\n{typo_rule}\n{anti}\n{json_schema}"
        )
    else:
        ap = (anchor_preview or "").strip()[:800] or "(אין טקסט — תגיב בקצרה)"
        if (active_topic_line or "").strip():
            head = f'הנושא הפעיל בקבוצה: "{(active_topic_line or "").strip()[:400]}". '
        else:
            head = ""
        replier_media_ban = ""
        if rich_media_mode:
            replier_media_ban = (
                "חובה: אתה משיב בשרשור — אסור sticker/gif/image; רק text או text_with_emoji. "
                "אם מישהו שלח מדיה, תגיב במילים בלבד (למשל 'חחח חזק', 'מדויק').\n"
            )
        user_he = (
            f"{last_five_block}\n\n{effective_stance}\n\n"
            f"{head}"
            f"{replier_media_ban}"
            f'אתה משיב להודעה/שורה הזו (או לקונטקסט שלה): "{ap}"\n'
            "אסור לחזור על עובדות או ניסוח מהציטוט — רק תגובה אישית קצרה (2–10 מילים).\n"
            f"{typo_rule}\n{anti}\n{json_schema}"
        )

    if core_analysis is not None and news_opener:
        from nexus.shared.intelligence_cache import augment_user_prompt_with_core_analysis

        user_he = augment_user_prompt_with_core_analysis(user_he, core_analysis)

    if regeneration_attempt == 1:
        user_he += (
            "\n\n⚠️ דחייה טכנית: הפלט דומה מדי להודעות שכבר נשלחו בקבוצה זו (מערכת בקרה). "
            "חובה מוחלטת: משפט אחר לגמרי — מילים, מבנה וגוון שונים. אסור לשכפל ניסוח קודם."
        )
    elif regeneration_attempt >= 2:
        user_he += (
            "\n\n🚨 ניסיון אחרון לפני דילוג: אסור שום דמיון לשורות שנשלחו לאחרונה בקבוצה. "
            "זווית חדשה לגמרי (בדיחה / רגש אחר / נושא אחר) — בלי אותן מילות מפתח."
        )

    temperature = AMCHA_LLM_TEMPERATURE
    top_p = AMCHA_LLM_TOP_P
    if rich_media_mode:
        frequency_penalty = 0.8
        presence_penalty = 0.5
        max_tokens = 384
    else:
        frequency_penalty = random.uniform(0.35, 0.5)
        presence_penalty = random.uniform(0.35, 0.5)
        max_tokens = 320

    if regeneration_attempt > 0:
        temperature = min(1.0, float(temperature) + 0.06 * regeneration_attempt)
        top_p = min(1.0, float(top_p) + 0.02 * regeneration_attempt)
        frequency_penalty = min(2.0, float(frequency_penalty) + 0.1 * regeneration_attempt)
        presence_penalty = min(2.0, float(presence_penalty) + 0.1 * regeneration_attempt)

    def _coerce_rich_respectful_media(d: dict[str, Any]) -> dict[str, Any]:
        if not privileged_anchor:
            return d
        at = str(d.get("action_type") or "").strip().lower()
        if at not in ("sticker", "gif", "image"):
            return d
        mt = str(d.get("message_text") or d.get("primary_message") or "וואלה").strip() or "וואלה"
        return {
            **d,
            "action_type": "text",
            "message_text": mt,
            "primary_message": mt,
        }

    def _postprocess_llm_dict(out: dict[str, Any]) -> dict[str, Any] | None:
        if not isinstance(out, dict):
            return None
        if rich_media_mode:
            at = str(out.get("action_type") or "").strip().lower().strip('"').strip("'")
            if at in ("sticker", "gif", "image"):
                normalized = _normalize_rich_turn(out)
                normalized = _ensure_rich_media_message_text(normalized)
                return _coerce_rich_respectful_media(
                    _apply_anti_robot_to_turn_dict(normalized, rich_media_mode=True)
                )
            if out.get("primary_message") or out.get("text") or str(out.get("message_text") or "").strip():
                normalized = _normalize_rich_turn(out)
                return _coerce_rich_respectful_media(
                    _apply_anti_robot_to_turn_dict(normalized, rich_media_mode=True)
                )
            return None
        if out.get("primary_message") or out.get("text"):
            normalized = _normalize_amcha_dict(out)
            return _apply_anti_robot_to_turn_dict(normalized, rich_media_mode=False)
        return None

    ollama_base = _resolve_ollama_base_url()
    ollama_model = _resolve_ollama_model()
    ollama_dup_strikes = 0
    if ollama_base:
        for attempt in range(AMCHA_OLLAMA_ANTI_DUP_MAX_TRIES):
            user_for_attempt = user_he
            if attempt > 0:
                user_for_attempt = (
                    user_he
                    + "\n\n(חובה: ניסוח שונה לגמרי מכל ניסיון קודם — לא לשכפל אותה הודעה.)"
                )
            raw_ol = await _ollama_chat_completion_content(
                ollama_base,
                ollama_model,
                system_prompt,
                user_for_attempt,
                temperature=temperature,
                top_p=top_p,
                max_tokens=max_tokens,
            )
            if not raw_ol:
                break
            obj_ol = _parse_llm_json_object(raw_ol)
            if not obj_ol:
                break
            processed_ol = _postprocess_llm_dict(obj_ol)
            if processed_ol is None:
                break
            if _anti_duplication_shield_rejects_turn(
                processed_ol,
                recent_texts=recent_texts,
                global_recent_outgoing=global_recent_outgoing,
                rich_media_mode=rich_media_mode,
            ):
                ollama_dup_strikes += 1
                log.warning(
                    "factory_ollama_anti_duplication_reject",
                    attempt=attempt + 1,
                    strikes=ollama_dup_strikes,
                )
                continue
            return processed_ol
        if ollama_dup_strikes >= AMCHA_OLLAMA_ANTI_DUP_MAX_TRIES:
            log.info(
                "factory_ollama_fallback_cloud_after_anti_dup",
                strikes=ollama_dup_strikes,
            )

    if api_key:
        try:
            from nexus.modules.community_vibe import _gemini_json  # type: ignore[attr-defined]

            out = await _gemini_json(
                api_key,
                system_prompt,
                user_he,
                temperature=temperature,
                max_tokens=max_tokens,
                frequency_penalty=frequency_penalty,
                presence_penalty=presence_penalty,
                top_p=top_p,
            )
            processed = _postprocess_llm_dict(out) if isinstance(out, dict) else None
            if processed is not None:
                return await _apply_cloud_llm_quality_gate(
                    processed,
                    rich_media_mode=rich_media_mode,
                    ollama_base=ollama_base,
                )
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
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_he},
                ],
                "temperature": temperature,
                "top_p": top_p,
                "max_tokens": max_tokens,
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
                    processed = _postprocess_llm_dict(obj)
                    if processed is not None:
                        return await _apply_cloud_llm_quality_gate(
                            processed,
                            rich_media_mode=rich_media_mode,
                            ollama_base=ollama_base,
                        )
        except Exception as exc:
            log.warning("factory_openai_failed", error=str(exc))

    fb_pm = "וואלה הזייה אחי" if role == "opener" else "אין מצב"
    if privileged_anchor:
        fb_pm = "מעניין, תודה על השיתוף" if role == "replier" else "וואלה מעניין"
    if rich_media_mode:
        return _normalize_rich_turn(
            {
                "action_type": "text",
                "message_text": fb_pm,
                "primary_message": fb_pm,
                "needs_correction": False,
                "correction_message": "",
                "article_url": "",
                "link_label": "",
            }
        )
    return {
        "primary_message": fb_pm,
        "needs_correction": False,
        "correction_message": "",
        "article_url": "",
        "link_label": "",
    }


def _converse_batch_size() -> int:
    raw = (os.getenv("COMMUNITY_FACTORY_CONVERSE_BATCH", "15") or "15").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 15
    return max(1, min(20, n))


async def _http_get_bytes(url: str) -> bytes | None:
    u = (url or "").strip()
    if not u.startswith(("http://", "https://")):
        return None
    try:
        import httpx

        async with httpx.AsyncClient(timeout=45.0, follow_redirects=True) as hc:
            r = await hc.get(u)
            if r.status_code == 200 and r.content:
                return bytes(r.content)
    except Exception as exc:
        log.debug("factory_http_get_bytes_failed", url=u[:120], error=str(exc))
    return None


def _picsum_image_url(image_query: str, persona_seed: str) -> str:
    h = hashlib.sha256(f"{image_query}|{persona_seed}".encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"https://picsum.photos/seed/{h}/800/600"


async def _unsplash_random_image_url(query: str) -> str | None:
    key = (os.getenv("UNSPLASH_ACCESS_KEY") or "").strip()
    if not key:
        return None
    try:
        import httpx

        q = quote((query or "landscape").strip() or "landscape", safe="")
        u = f"https://api.unsplash.com/photos/random?query={q}&client_id={key}"
        async with httpx.AsyncClient(timeout=25.0) as hc:
            r = await hc.get(u, headers={"Accept-Version": "v1"})
            if r.status_code != 200:
                return None
            data = r.json()
            if isinstance(data, list) and data:
                data = data[0]
            urls = (data or {}).get("urls") or {}
            return str(urls.get("regular") or urls.get("full") or "").strip() or None
    except Exception as exc:
        log.debug("factory_unsplash_random_failed", error=str(exc))
    return None


async def _resolve_gif_media_url(query: str) -> str | None:
    q = (query or "fun").strip() or "fun"
    try:
        import httpx

        async with httpx.AsyncClient(timeout=25.0) as hc:
            tenor = (os.getenv("TENOR_API_KEY") or os.getenv("TENOR_KEY") or "").strip()
            if tenor:
                u = (
                    "https://tenor.googleapis.com/v2/search?"
                    f"q={quote(q, safe='')}&key={quote(tenor, safe='')}&limit=8&random=true"
                )
                r = await hc.get(u)
                if r.status_code == 200:
                    data = r.json()
                    results = data.get("results") or []
                    if results:
                        pick = random.choice(results)
                        mf = pick.get("media_formats") or {}
                        for key in ("gif", "tinygif", "nanogif", "mediumgif"):
                            g = mf.get(key) or {}
                            url = str(g.get("url") or "").strip()
                            if url:
                                return url
            giphy = (os.getenv("GIPHY_API_KEY") or "").strip()
            if giphy:
                u = (
                    "https://api.giphy.com/v1/gifs/random?"
                    f"api_key={quote(giphy, safe='')}&tag={quote(q, safe='')}"
                )
                r = await hc.get(u)
                if r.status_code == 200:
                    dj = r.json()
                    d0 = (dj.get("data") or {}) if isinstance(dj.get("data"), dict) else {}
                    imgs = d0.get("images") or {}
                    orig = imgs.get("original") or {}
                    url = str(orig.get("url") or "").strip()
                    if url:
                        return url
    except Exception as exc:
        log.debug("factory_gif_resolve_failed", error=str(exc))
    return None


async def _fetch_image_bytes_for_query(image_query: str, persona_seed: str) -> tuple[bytes | None, str]:
    unsplash = await _unsplash_random_image_url(image_query)
    if unsplash:
        b = await _http_get_bytes(unsplash)
        if b:
            return b, "photo.jpg"
    url = _picsum_image_url(image_query, persona_seed)
    b = await _http_get_bytes(url)
    return b, "photo.jpg"


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


def _sticker_pack_short_names() -> list[str]:
    multi = (os.getenv("COMMUNITY_FACTORY_STICKER_SETS") or "").strip()
    legacy = (os.getenv("COMMUNITY_FACTORY_STICKER_SET") or "").strip()
    names: list[str] = []
    if multi:
        names.extend(x.strip() for x in multi.split(",") if x.strip())
    elif legacy:
        names.append(legacy)
    return names or list(_DEFAULT_STICKER_PACKS)


async def _try_send_random_sticker_from_packs(
    client: Any, entity: Any, reply_to: int | None = None
) -> Any:
    from telethon.tl.functions.messages import GetStickerSetRequest  # type: ignore[import-untyped]
    from telethon.tl.types import InputStickerSetShortName  # type: ignore[import-untyped]

    packs = list(_sticker_pack_short_names())
    random.shuffle(packs)
    for short in packs[:8]:
        try:
            res = await client(
                GetStickerSetRequest(
                    stickerset=InputStickerSetShortName(short_name=short),
                    hash=0,
                )
            )
            docs = [d for d in (getattr(res, "documents", None) or []) if d]
            if not docs:
                continue
            return await client.send_file(entity, random.choice(docs), reply_to=reply_to)
        except Exception as exc:
            log.debug("factory_sticker_pack_skipped", pack=short, error=str(exc))
            continue
    return None


async def _try_send_inline_gif(client: Any, entity: Any, query: str) -> Any:
    from telethon.tl.functions.messages import (  # type: ignore[import-untyped]
        GetInlineBotResultsRequest,
        SendInlineBotResultRequest,
    )

    q = (query or "").strip()[:64] or random.choice(["funny", "lol", "wow", "mood", "cat"])
    try:
        bot = await client.get_input_entity("gif")
        results = await client(GetInlineBotResultsRequest(bot=bot, peer=entity, query=q, offset=""))
    except Exception as exc:
        log.debug("factory_gif_inline_query_failed", error=str(exc))
        return None
    rlist = [r for r in (getattr(results, "results", None) or []) if r is not None]
    if not rlist:
        return None
    chosen = random.choice(rlist)
    rid = getattr(chosen, "id", None)
    if rid is None:
        return None
    try:
        return await client(
            SendInlineBotResultRequest(
                peer=entity,
                query_id=int(results.query_id),
                id=str(rid),
                random_id=random.randint(1, 2**63 - 1),
                hide_via=True,
            )
        )
    except Exception as exc:
        log.debug("factory_gif_inline_send_failed", error=str(exc))
        return None


async def _append_rich_correction_messages(
    client: Any, entity: Any, turn: dict[str, Any], sent: list[Any]
) -> None:
    if not turn.get("needs_correction"):
        return
    cm = str(turn.get("correction_message") or "").strip()
    if not cm:
        return
    await asyncio.sleep(random.uniform(2.0, 4.0))
    fix = _finalize_correction_message(cm)[:4096]
    if fix:
        msg2 = await _telethon_send_text_schedule_hesitation(
            client, entity, fix, reply_to=None, parse_mode=None
        )
        sent.append(msg2)


async def _send_rich_factory_messages(
    client: Any,
    entity: Any,
    turn: dict[str, Any],
    *,
    reply_to_id: int | None,
    news_opener: bool,
    use_md: bool = False,
    redis: Any | None = None,
    session_base: str = "",
) -> list[Any]:
    """Jitter before Telegram, then text / sticker / Tenor·Giphy·inline GIF / image bytes per action_type."""
    await asyncio.sleep(random.uniform(0.5, 4.0))
    sent: list[Any] = []
    action = str(turn.get("action_type") or "text").strip().lower()
    raw_primary = str(turn.get("primary_message") or turn.get("message_text") or "")

    if news_opener:
        primary_out = _finalize_primary_message(str(turn.get("primary_message") or turn.get("message_text") or ""))
        await _maybe_publish_outlet_echo_issue(redis, session_base, raw_primary)
        url = await _maybe_tinyurl_shorten(str(turn.get("article_url") or ""))
        primary_out, use_md2 = _format_opener_with_md_link(
            str(turn.get("primary_message") or primary_out),
            url,
            str(turn.get("link_label") or ""),
        )
        use_md = use_md or use_md2
        msgs = await _send_amcha_messages(
            client,
            entity,
            primary=primary_out,
            needs_correction=bool(turn.get("needs_correction")),
            correction=str(turn.get("correction_message") or ""),
            reply_to_id=reply_to_id,
            parse_mode="md" if use_md else None,
        )
        img_u = str(turn.get("openclaw_image_url") or "").strip()
        if img_u.startswith(("http://", "https://")):
            try:
                from nexus.services.recent_news_digest import (
                    download_image_bytes,
                    telegram_image_filename_from_bytes,
                )

                data = await download_image_bytes(img_u)
                if data:
                    fname = telegram_image_filename_from_bytes(data)
                    bio = BytesIO(data)
                    rpl: int | None = None
                    if msgs:
                        rpl = getattr(msgs[-1], "id", None)
                        if rpl is not None:
                            rpl = int(rpl)
                    if rpl is None:
                        rpl = reply_to_id
                    try:
                        photo_msg = await client.send_file(
                            entity,
                            file=(fname, bio),
                            reply_to=rpl,
                            force_document=False,
                        )
                        msgs.append(photo_msg)
                    except Exception as exc:
                        log.debug("factory_news_opener_photo_failed", error=str(exc))
            except Exception as exc:
                log.debug("factory_news_opener_image_branch_failed", error=str(exc))
        return msgs

    mt = _finalize_primary_message(str(turn.get("message_text") or turn.get("primary_message") or ""))
    await _maybe_publish_outlet_echo_issue(redis, session_base, raw_primary)
    iq = str(turn.get("image_query") or "").strip()
    persona_seed = str(turn.get("_persona_seed") or "")

    try:
        if action in ("text", "text_with_emoji"):
            body = (mt[:4096] if mt else "וואלה") or "וואלה"
            await _send_factory_plain_text_with_hebrew_typo(
                client, entity, body, reply_to_id=reply_to_id, sent=sent
            )
        elif action == "sticker":
            msg = await _try_send_random_sticker_from_packs(client, entity, reply_to=reply_to_id)
            if msg is not None:
                sent.append(msg)
                line = (mt[:4096] if (mt or "").strip() else _default_hebrew_media_companion("sticker", iq))
                line = (line or "חחח").strip()
                try:
                    smid = getattr(msg, "id", None)
                    await _send_factory_plain_text_with_hebrew_typo(
                        client,
                        entity,
                        line,
                        reply_to_id=int(smid) if smid is not None else reply_to_id,
                        sent=sent,
                    )
                except Exception as exc:
                    log.debug("factory_sticker_text_followup_failed", error=str(exc))
            else:
                fb = (mt[:4096] if mt else "😂") or "😂"
                await _send_factory_plain_text_with_hebrew_typo(
                    client, entity, fb, reply_to_id=reply_to_id, sent=sent
                )
        elif action == "gif":
            gif_url = await _resolve_gif_media_url(iq or "funny")
            msg_obj = None
            used_url_send = False
            if gif_url:
                try:
                    msg_obj = await client.send_file(
                        entity,
                        gif_url,
                        caption=mt[:1024] if mt else None,
                        reply_to=reply_to_id,
                    )
                    used_url_send = True
                except Exception:
                    msg_obj = None
            if msg_obj is None:
                msg_obj = await _try_send_inline_gif(client, entity, iq or "funny")
            if msg_obj is not None:
                sent.append(msg_obj)
                need_followup = (not used_url_send) or not (mt or "").strip()
                if need_followup:
                    line = (mt[:4096] if (mt or "").strip() else _default_hebrew_media_companion("gif", iq))
                    line = (line or "וואלה").strip()
                    gmid = getattr(msg_obj, "id", None)
                    try:
                        await _send_factory_plain_text_with_hebrew_typo(
                            client,
                            entity,
                            line,
                            reply_to_id=int(gmid) if gmid is not None else reply_to_id,
                            sent=sent,
                        )
                    except Exception as exc:
                        log.debug("factory_gif_text_followup_failed", error=str(exc))
            else:
                fb_msg = await _try_send_random_sticker_from_packs(client, entity, reply_to=reply_to_id)
                if fb_msg is not None:
                    sent.append(fb_msg)
                    line = (mt[:4096] if (mt or "").strip() else _default_hebrew_media_companion("sticker", iq))
                    line = (line or "חחח").strip()
                    try:
                        fmid = getattr(fb_msg, "id", None)
                        if fmid is not None:
                            await _send_factory_plain_text_with_hebrew_typo(
                                client,
                                entity,
                                line,
                                reply_to_id=int(fmid),
                                sent=sent,
                            )
                    except Exception as exc:
                        log.debug("factory_gif_fallback_sticker_followup_failed", error=str(exc))
                else:
                    await _send_factory_plain_text_with_hebrew_typo(
                        client,
                        entity,
                        (mt or "וואלה")[:4096],
                        reply_to_id=reply_to_id,
                        sent=sent,
                    )
        elif action == "image":
            data, fname = await _fetch_image_bytes_for_query(iq or "city", persona_seed)
            msg_obj = None
            if data:
                try:
                    salt_raw = turn.get("_media_salt_seed")
                    salt_seed = salt_raw if isinstance(salt_raw, (bytes, bytearray)) else None
                    if salt_seed is None:
                        salt_seed = make_image_upload_salt_seed(persona_seed or "factory")
                    data, fname = prepare_jpeg_png_for_telegram_upload(
                        data, salt_seed=bytes(salt_seed)
                    )
                    bio = BytesIO(data)
                    msg_obj = await client.send_file(
                        entity,
                        file=(fname, bio),
                        caption=mt[:1024] if mt else None,
                        reply_to=reply_to_id,
                        force_document=False,
                    )
                except Exception:
                    msg_obj = None
            if msg_obj is not None:
                sent.append(msg_obj)
                if not (mt or "").strip():
                    line = _default_hebrew_media_companion("image", iq)
                    try:
                        imid = getattr(msg_obj, "id", None)
                        if imid is not None:
                            await _send_factory_plain_text_with_hebrew_typo(
                                client,
                                entity,
                                line[:4096],
                                reply_to_id=int(imid),
                                sent=sent,
                            )
                    except Exception as exc:
                        log.debug("factory_image_text_followup_failed", error=str(exc))
            else:
                await _send_factory_plain_text_with_hebrew_typo(
                    client,
                    entity,
                    (mt or "וואלה")[:4096],
                    reply_to_id=reply_to_id,
                    sent=sent,
                )
        else:
            body = (mt[:4096] if mt else "וואלה") or "וואלה"
            await _send_factory_plain_text_with_hebrew_typo(
                client, entity, body, reply_to_id=reply_to_id, sent=sent
            )
    except Exception as exc:
        log.debug("factory_rich_send_failed", action=action, error=str(exc))
        try:
            await _send_factory_plain_text_with_hebrew_typo(
                client,
                entity,
                (mt or "וואלה")[:4096],
                reply_to_id=reply_to_id,
                sent=sent,
            )
        except Exception as exc2:
            log.warning("factory_rich_fallback_failed", error=str(exc2))
            return sent

    await _append_rich_correction_messages(client, entity, turn, sent)
    return sent


async def _try_send_pack_sticker(client: Any, entity: Any, reply_to: int | None = None) -> bool:
    msg = await _try_send_random_sticker_from_packs(client, entity, reply_to=reply_to)
    return msg is not None


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


def _normalize_extracted_lore_sentence(raw: str | None) -> str:
    if not raw:
        return ""
    s = str(raw).strip()
    s = s.split("\n")[0].strip()
    low = s.lower()
    for pref in ("משפט:", "תשובה:", "סיכום:", "sentence:", "output:"):
        if low.startswith(pref):
            s = s[len(pref) :].strip()
            low = s.lower()
    if len(s) >= 2 and s[0] in ('"', "'") and s[-1] == s[0]:
        s = s[1:-1].strip()
    return s[:500]


def _lore_history_block_from_messages(messages_newest_first: list[Any]) -> str:
    chronological = list(reversed(messages_newest_first))
    lines: list[str] = []
    for m in chronological:
        t = _message_text_for_factory_prompt(m)
        if not t:
            continue
        sid = getattr(m, "sender_id", None)
        lab = str(sid) if sid is not None else "?"
        lines.append(f"{lab}: {t}")
    return "\n".join(lines)


def _resolve_lore_target_group_id(parameters: dict[str, Any], groups: list[Any]) -> int | None:
    raw = parameters.get("group_id")
    if raw is not None and str(raw).strip() != "":
        try:
            return int(raw)
        except (TypeError, ValueError):
            pass
    env_g = (os.getenv("NEXUS_SWARM_LORE_GROUP_ID") or "").strip()
    if env_g:
        try:
            return int(env_g)
        except ValueError:
            pass
    for g in groups:
        if isinstance(g, dict) and g.get("group_id") is not None:
            try:
                return int(g["group_id"])
            except (TypeError, ValueError):
                continue
    return None


@registry.register("swarm.lore_nightly")
async def swarm_lore_nightly(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Nightly lore: last N messages from the factory group → Ollama (Mac Mini) → Redis list
    ``nexus:swarm:lore_facts`` (newest-first, capped at LORE_FACTS_LIST_MAX).
    """
    redis = parameters.get("__redis__")
    if redis is None:
        return {"status": "failed", "error": "redis unavailable"}

    roles = await _redis_json_get(redis, KEY_ROLES)
    groups = await _redis_json_get(redis, KEY_GROUPS)
    if not isinstance(roles, dict) or not isinstance(groups, list) or not groups:
        return {"status": "skipped", "reason": "missing roles or groups"}

    target_gid = _resolve_lore_target_group_id(parameters, groups)
    if target_gid is None:
        return {"status": "skipped", "reason": "no group_id"}

    owners = list(roles.get("owners") or [])
    members = list(roles.get("members") or [])
    pool = owners + members
    if not pool:
        return {"status": "skipped", "reason": "no sessions"}

    ollama_base = _resolve_ollama_base_url()
    ollama_model = _resolve_ollama_model()
    if not ollama_base:
        return {"status": "failed", "error": "NEXUS_OLLAMA_BASE_URL / OLLAMA_HOST not set"}

    random.shuffle(pool)
    last_exc: str | None = None
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
                    continue
                await _ensure_factory_profile(client, redis, session_base)
                ent = await client.get_entity(target_gid)
                hist = await client.get_messages(ent, limit=LORE_NIGHTLY_MESSAGE_LIMIT)
                block = _lore_history_block_from_messages(list(hist or []))
                if not block.strip():
                    return {
                        "status": "skipped",
                        "reason": "no_text_in_history",
                        "group_id": target_gid,
                    }

                user_he = LORE_EXTRACTION_USER_PREFIX + block
                raw_ol = await _ollama_chat_completion_content(
                    ollama_base,
                    ollama_model,
                    LORE_EXTRACTION_SYSTEM,
                    user_he,
                    temperature=0.45,
                    top_p=0.9,
                    max_tokens=160,
                )
                fact = _normalize_extracted_lore_sentence(raw_ol)
                if not fact:
                    log.warning("lore_nightly_empty_llm", group_id=target_gid)
                    return {
                        "status": "failed",
                        "error": "ollama_empty_output",
                        "group_id": target_gid,
                    }

                await redis.lpush(KEY_LORE_FACTS, fact)
                await redis.ltrim(KEY_LORE_FACTS, 0, LORE_FACTS_LIST_MAX - 1)
                log.info("lore_nightly_stored", group_id=target_gid, fact_len=len(fact))
                return {"status": "completed", "group_id": target_gid, "fact_len": len(fact)}
        except ValueError as exc:
            last_exc = str(exc)
            log.warning("lore_nightly_creds", error=last_exc)
        except Exception as exc:
            last_exc = str(exc)
            kind = classify_telethon_account_error(exc)
            if kind == "ban":
                await _mark_banned(redis, session_base)
            elif kind == "flood":
                sec = int(flood_wait_seconds(exc) * 1.1) + 1
                await _set_cooldown(redis, session_base, sec)
            log.warning("lore_nightly_failed", error=last_exc[:300], group_id=target_gid)

    return {
        "status": "failed",
        "error": last_exc or "no_session_succeeded",
        "group_id": target_gid,
    }


# ── OpenClaw / pubsub: flexible JSON + optional image metadata for news openers ──

OPENCLAW_DIGEST_OVERLAY_KEY = "nexus:swarm:factory:openclaw_digest_overlay"
OPENCLAW_DIGEST_OVERLAY_TTL_SEC = 900
_OPENCLAW_PUBSUB_IMAGE_KEYS = (
    "image_url",
    "image",
    "thumb_url",
    "thumbnail",
    "photo_url",
    "og_image",
    "hero_image",
)
_OPENCLAW_OVERLAY_EXTRA_SKIP: frozenset[str] = frozenset(
    {
        "headline",
        "title",
        "subject",
        "head_line",
        "anchor_title",
        "name",
        "content",
        "body",
        "text",
        "summary",
        "article",
        "article_text",
        "digest_text",
        "excerpt",
        "description",
        "message",
        "timestamp",
        "ts",
        "time",
        "schema",
        "event",
        "engine",
        "digest_preview",
        "anchor_link",
        "article_url",
        "url",
        "link",
        "article_link",
        *_OPENCLAW_PUBSUB_IMAGE_KEYS,
    }
)


def parse_swarm_news_digest_pubsub_payload(raw: str) -> dict[str, Any] | None:
    """Best-effort JSON object from a pub/sub payload; never raises."""
    s = (raw or "").strip()
    if not s:
        return None
    try:
        obj = json.loads(s)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    if isinstance(obj, list):
        for it in obj:
            if isinstance(it, dict):
                return it
        return None
    if isinstance(obj, dict):
        return obj
    return None


def _is_internal_nexus_digest_pubsub_event(obj: dict[str, Any]) -> bool:
    if str(obj.get("event") or "").strip() == "news_digest_updated":
        return True
    sch = str(obj.get("schema") or "").strip()
    return bool(sch.startswith("nexus."))


def _first_http_url_from_mapping(obj: dict[str, Any], keys: tuple[str, ...]) -> str:
    for k in keys:
        v = obj.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s.startswith(("http://", "https://")):
            return s[:4000]
    return ""


async def read_openclaw_digest_overlay(redis: Any) -> dict[str, Any] | None:
    if redis is None:
        return None
    try:
        raw = await redis.get(OPENCLAW_DIGEST_OVERLAY_KEY)
        if not raw:
            return None
        if isinstance(raw, (bytes, bytearray)):
            raw = bytes(raw).decode("utf-8", errors="ignore")
        data = json.loads(str(raw))
        return data if isinstance(data, dict) else None
    except Exception as exc:
        log.debug("openclaw_digest_overlay_read_failed", error=str(exc))
    return None


async def persist_openclaw_digest_overlay_from_pubsub(redis: Any, obj: dict[str, Any]) -> None:
    """
    Store headline/body/image URL and unknown scalar metadata from a non-internal
    pub/sub JSON object so factory news openers can use it on the next tick.
    """
    if redis is None or not isinstance(obj, dict):
        return
    if _is_internal_nexus_digest_pubsub_event(obj):
        return
    headline, content = "", ""
    try:
        from nexus.services.openclaw_bridge import extract_headline_content

        headline, content = extract_headline_content(obj)
    except Exception:
        headline = str(obj.get("headline") or obj.get("title") or "").strip()
        content = str(obj.get("content") or obj.get("body") or obj.get("text") or "").strip()
    img = _first_http_url_from_mapping(obj, _OPENCLAW_PUBSUB_IMAGE_KEYS)
    article = _first_http_url_from_mapping(
        obj,
        ("article_url", "url", "link", "article_link", "anchor_link"),
    )
    extra: dict[str, Any] = {}
    for k, v in obj.items():
        ks = str(k)
        if ks in _OPENCLAW_OVERLAY_EXTRA_SKIP:
            continue
        if isinstance(v, (str, int, float, bool)) or v is None:
            if isinstance(v, str) and not v.strip():
                continue
            extra[ks[:80]] = v if not isinstance(v, str) else v[:2000]
    if not (headline or content or img or article or extra):
        return
    record = {
        "headline": headline[:2000],
        "content": content[:8000],
        "image_url": img or None,
        "article_url": article or None,
        "extra": extra,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        await redis.set(
            OPENCLAW_DIGEST_OVERLAY_KEY,
            json.dumps(record, ensure_ascii=False),
            ex=int(OPENCLAW_DIGEST_OVERLAY_TTL_SEC),
        )
    except Exception as exc:
        log.warning("openclaw_digest_overlay_persist_failed", error=str(exc))


async def run_swarm_news_digest_subscriber(ctx: dict[str, Any]) -> None:
    """
    Dedicated Redis pub/sub connection for ``nexus:swarm:news_digest``.
    Try-parses each message; overlay + converse wake are handled in
    ``schedule_converse_tick_on_swarm_news_digest_message``.
    """
    from redis.asyncio import from_url as _redis_from_url

    from nexus.services.recent_news_digest import SWARM_NEWS_DIGEST_CHANNEL
    from nexus.shared import redis_util

    raw_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")
    redis_url = redis_util.coerce_redis_url_for_platform(raw_url)
    retry_s = 1.0
    attempt = 0
    while True:
        pubsub_client = None
        try:
            attempt += 1
            pubsub_client = _redis_from_url(redis_url, decode_responses=True)
            pubsub = pubsub_client.pubsub()
            await pubsub.subscribe(SWARM_NEWS_DIGEST_CHANNEL)
            if attempt > 1:
                log.info(
                    "worker_swarm_news_digest_subscriber_reconnected",
                    attempts=attempt,
                    **{"Source": "OpenClaw"},
                )
            else:
                log.info(
                    "worker_swarm_news_digest_subscriber_started",
                    channel=SWARM_NEWS_DIGEST_CHANNEL,
                    **{"Source": "OpenClaw"},
                )
            retry_s = 1.0

            async for message in pubsub.listen():
                if message.get("type") != "message":
                    continue
                data = message.get("data") or ""
                if not isinstance(data, str) or not data.strip():
                    continue
                parsed = parse_swarm_news_digest_pubsub_payload(data)
                log.info(
                    "swarm_news_digest_redis_received",
                    **{"Source": "OpenClaw"},
                    event=(parsed or {}).get("event") if parsed else None,
                    schema=(parsed or {}).get("schema") if parsed else None,
                    digest_engine=(parsed or {}).get("engine") if parsed else None,
                    parse_ok=parsed is not None,
                )
                shared_redis = ctx.get("redis")
                if shared_redis is None:
                    log.warning(
                        "swarm_news_digest_wake_skipped_no_redis",
                        **{"Source": "OpenClaw"},
                    )
                    continue
                out = await schedule_converse_tick_on_swarm_news_digest_message(
                    shared_redis, raw_payload=data
                )
                log.info(
                    "swarm_news_digest_wake_enqueue_result",
                    **{"Source": "OpenClaw"},
                    **out,
                )
        except asyncio.CancelledError:
            break
        except Exception as exc:
            if attempt <= 2 or attempt % 5 == 0:
                log.warning(
                    "worker_swarm_news_digest_subscriber_retry",
                    attempt=attempt,
                    retry_in_s=round(retry_s, 2),
                    error=str(exc),
                    **{"Source": "OpenClaw"},
                )
            await asyncio.sleep(retry_s)
            retry_s = min(retry_s * 1.7, 10.0)
        finally:
            if pubsub_client is not None:
                try:
                    await pubsub_client.aclose()
                except Exception:
                    pass


DIGEST_WAKE_LOCK_PREFIX = "nexus:swarm:factory:digest_wake_lock:"


async def schedule_converse_tick_on_swarm_news_digest_message(
    redis: Any, *, raw_payload: str
) -> dict[str, Any]:
    """
    Invoked when a message is received on ``nexus:swarm:news_digest`` (e.g. OpenClaw
    after updating the central digest in Redis). Enqueues a one-shot
    ``converse_tick`` with ``news_digest_wake`` so opener slots run the news/LLM
    path immediately without waiting for the active-topic TTL.
    """
    if redis is None:
        return {"ok": False, "reason": "no_redis"}
    raw = raw_payload or ""
    h = hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()[:32]
    lock_key = f"{DIGEST_WAKE_LOCK_PREFIX}{h}"
    try:
        got = await redis.set(lock_key, "1", nx=True, ex=120)
    except Exception as exc:
        log.warning("swarm_digest_wake_lock_failed", error=str(exc))
        return {"ok": False, "reason": "lock_error"}
    if not got:
        return {"ok": False, "reason": "deduped"}

    parsed = parse_swarm_news_digest_pubsub_payload(raw)
    if isinstance(parsed, dict):
        try:
            await persist_openclaw_digest_overlay_from_pubsub(redis, parsed)
        except Exception as exc:
            log.debug("swarm_news_digest_overlay_apply_failed", error=str(exc))

    state = await _redis_json_get(redis, KEY_STATE)
    groups = await _redis_json_get(redis, KEY_GROUPS)
    if not isinstance(state, dict) or state.get("phase") != "chatting":
        return {"ok": False, "reason": "not_chatting"}
    if not isinstance(groups, list) or not groups:
        return {"ok": False, "reason": "no_groups"}

    sd = str(state.get("sessions_dir") or "").strip()
    if not sd:
        sd = str(_resolve_sessions_dir(None))
    params: dict[str, Any] = {"sessions_dir": sd, "news_digest_wake": True}
    enq = await _enqueue_task("swarm.community_factory.converse_tick", params)
    return {"ok": bool(enq), "reason": "enqueued" if enq else "enqueue_failed"}


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
    rankseo_mode = bool(parameters.get("rankseo_mode", False))
    gpt_override = parameters.get("groups_per_owner_target")
    groups_per_owner_target = (
        int(gpt_override)
        if gpt_override is not None and str(gpt_override).strip() != ""
        else GROUPS_TARGET_PER_OWNER
    )
    groups_per_owner_target = max(1, min(groups_per_owner_target, 200))

    bases = _discover_session_bases(sessions_dir)
    owners, members = (
        _split_roles_rankseo(bases) if rankseo_mode else _split_roles(bases)
    )

    if reset and redis and not dry_run:
        await redis.delete(
            KEY_ROLES,
            KEY_GROUPS,
            KEY_STATE,
            KEY_BANNED,
            KEY_COOLDOWNS,
            KEY_METRICS,
            KEY_PROFILE_GATE,
            KEY_RECENT_OUTGOING,
        )
        await _redis_delete_keys_with_prefix(redis, THREAD_KEY_PREFIX)
        await _redis_delete_keys_with_prefix(redis, ACTIVE_TOPIC_KEY_PREFIX)
        await _redis_delete_keys_with_prefix(redis, GROUP_RECENT_SENT_PREFIX)

    if reset and not dry_run:
        _factory_group_recent_sent_local.clear()

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
        state["rankseo_mode"] = rankseo_mode
        state["groups_per_owner_target"] = groups_per_owner_target
        state["creation_index"] = 0
        state["join_flat_idx"] = 0
        state["converse_idx"] = 0
        state["export_invite_idx"] = 0
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
            "rankseo_mode": rankseo_mode,
            "groups_per_owner_target": groups_per_owner_target,
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
        "rankseo_mode": rankseo_mode,
        "groups_per_owner_target": groups_per_owner_target,
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

    if bool(state.get("rankseo_mode")):
        title = f"RANKSEO {owner_idx}-{idx // len(owners)}-{random.randint(1000, 9999)}"
    else:
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

        owner_sess = str(grp.get("owner_session") or "").strip()
        if owner_sess and session_base.strip() == owner_sess:
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
                await _factory_after_joins_done(redis, state, carry)
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
        await _factory_after_joins_done(redis, state, carry)

    return {"status": "completed", "joined": False, "join_flat_idx": j, "exhausted": j >= flat_max}


@registry.register("swarm.community_factory.export_private_invites_tick")
async def community_factory_export_private_invites_tick(
    parameters: dict[str, Any],
) -> dict[str, Any]:
    """
    For each factory group, refresh primary invite link as the owner (private t.me/+ link).
    Writes ``vault/data/group_factory_rankseo_report.json`` when all rows are processed.
    """
    redis = parameters.get("__redis__")
    groups = await _redis_json_get(redis, KEY_GROUPS)
    state = await _redis_json_get(redis, KEY_STATE)
    if not isinstance(groups, list) or not groups:
        return {"status": "failed", "error": "no groups"}
    if not isinstance(state, dict):
        return {"status": "failed", "error": "state missing"}

    idx = int(state.get("export_invite_idx", 0))
    if idx >= len(groups):
        _write_rankseo_report_file(groups)
        state["phase"] = "complete"
        await _redis_json_set(redis, KEY_STATE, state)
        return {"status": "completed", "phase": "export_done", "groups_total": len(groups)}

    try:
        from telethon.tl.types import PeerChannel  # type: ignore[import-untyped]
    except ImportError:
        return {"status": "failed", "error": "telethon not installed"}

    grp = groups[idx]
    owner = str(grp.get("owner_session") or "").strip()
    raw_gid = grp.get("group_id")
    carry = dict(parameters)
    carry.pop("__redis__", None)

    async def _continue_next() -> None:
        state["export_invite_idx"] = idx + 1
        await _redis_json_set(redis, KEY_GROUPS, groups)
        await _redis_json_set(redis, KEY_STATE, state)
        await _enqueue_task("swarm.community_factory.export_private_invites_tick", carry)

    if not owner or raw_gid is None:
        groups[idx]["invite_export_error"] = "missing owner or group_id"
        await _continue_next()
        return {"status": "skipped", "idx": idx, "reason": "missing_meta"}

    await asyncio.sleep(random.uniform(5.0, 20.0))
    ch_id = _peer_channel_id(int(raw_gid))

    try:
        async with async_telegram_client(owner, parameters) as client:
            if not await client.is_user_authorized():
                await _mark_banned(redis, owner)
                groups[idx]["invite_export_error"] = "owner_unauthorized"
                await _continue_next()
                return {"status": "skipped", "idx": idx, "reason": "owner_unauthorized"}

            await _ensure_factory_profile(client, redis, owner)
            entity = await client.get_entity(PeerChannel(ch_id))
            new_link = await client.export_chat_invite_link(entity)
            groups[idx]["invite_link"] = new_link
            groups[idx]["private_invite_exported_at"] = datetime.now(timezone.utc).isoformat()
            groups[idx].pop("invite_export_error", None)
            await _bump_metric(redis, "private_links_exported", 1)
    except ValueError as exc:
        log.warning("factory_export_invite_creds", idx=idx, error=str(exc))
        groups[idx]["invite_export_error"] = str(exc)[:500]
    except Exception as exc:
        log.warning("factory_export_invite_failed", idx=idx, error=str(exc))
        groups[idx]["invite_export_error"] = str(exc)[:500]

    await _continue_next()
    return {
        "status": "completed",
        "exported_index": idx,
        "invite_link": groups[idx].get("invite_link"),
    }


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
    global_recent_burst = await _redis_recent_outgoing_fetch(redis)
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
            if session_is_asleep_jerusalem(session_base):
                continue
            derail_kind_b: str | None = None
            derail_cont_after_b: int | None = None
            burst_derail_committed = False
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
                            if not anchor_preview.strip():
                                anchor_preview = telethon_display_text(m0)[:800]
                    except Exception:
                        anchor_preview = ""
                    active_record = await _active_topic_read(redis, gid_int)
                    active_topic_line = (
                        str(active_record.get("text") or "").strip() if isinstance(active_record, dict) else None
                    ) or None
                    topic = random.choice(FACTORY_TOPICS)
                    stance = random.choice(AMCHA_STANCES_HE)
                    privileged_burst = False
                    try:
                        privileged_burst = await sender_of_message_is_owner_or_admin(
                            client, ent, reply_mid
                        )
                    except Exception:
                        privileged_burst = False
                    derail_kind_b, derail_cont_after_b = await _derail_claim_for_turn(redis)
                    derail_anchor_b = ""
                    if derail_kind_b == "contagion" and redis is not None:
                        derail_anchor_b = str(await redis.get(KEY_DERAIL_ANCHOR) or "")
                    lore_line = await _maybe_sample_lore_for_amcha(
                        redis, privileged_anchor=privileged_burst
                    )
                    turn = await _generate_unique_amcha_turn(
                        redis,
                        gid_int,
                        api_key=api_key,
                        topic=topic,
                        openai_key=openai_key,
                        session_base=session_base,
                        role="replier",
                        stance_he=stance,
                        last_five_block=last_five,
                        anchor_preview=anchor_preview or None,
                        recent_texts=recent_texts,
                        active_topic_line=active_topic_line,
                        opener_fresh_event=False,
                        news_opener=False,
                        persona_seed=_session_persona_seed(session_base),
                        privileged_anchor=privileged_burst,
                        community_lore_line=lore_line,
                        global_recent_outgoing=global_recent_burst,
                        derail_mode=derail_kind_b,
                        derail_anchor_text=derail_anchor_b or None,
                    )
                    if turn is None:
                        await _derail_restore_claim(redis, derail_kind_b, derail_cont_after_b)
                        log.debug("factory_burst_antiparrot_skip", group_id=gid_int)
                        continue
                    if derail_kind_b == "seed":
                        mtb = str(turn.get("primary_message") or turn.get("text") or "").strip() or (
                            "מישהו יודע סדרה טובה בנטפליקס"
                        )
                        turn = {
                            **turn,
                            "primary_message": mtb,
                            "text": mtb,
                            "needs_correction": False,
                            "correction_message": "",
                            "correction": "",
                        }
                    body = _finalize_primary_message(turn["primary_message"])
                    await asyncio.sleep(
                        _reading_delay_before_typing_seconds(last_five, anchor_preview or "", active_topic_line or "")
                    )
                    async with client.action(ent, "typing"):
                        await asyncio.sleep(random.uniform(0.4, 1.8))
                    msgs = await _send_amcha_messages(
                        client,
                        ent,
                        primary=body,
                        needs_correction=bool(turn.get("needs_correction")),
                        correction=str(turn.get("correction_message") or ""),
                        reply_to_id=reply_mid,
                        parse_mode=None,
                    )
                    if derail_kind_b and not msgs:
                        await _derail_restore_claim(redis, derail_kind_b, derail_cont_after_b)
                    elif msgs:
                        burst_derail_committed = True
                        await _global_swarm_bump_after_send(
                            redis,
                            n_bot_messages=len(msgs),
                            derail_kind=derail_kind_b,
                            contagion_after=derail_cont_after_b,
                            seed_anchor_text=body if derail_kind_b == "seed" else None,
                        )
                    sent_n += len(msgs)
                    await _bump_metric(redis, "messages_sent", len(msgs))
                    ap_frag = _factory_turn_antiparrot_compare_text(
                        turn, news_opener=False, rich_media_mode=False
                    )
                    push_body = (ap_frag or body).strip()[:600]
                    if push_body:
                        await _factory_group_recent_sent_push(redis, gid_int, push_body)
                    picked = True
                    break
            except ValueError as exc:
                log.warning("factory_burst_creds_missing", error=str(exc))
                if derail_kind_b and not burst_derail_committed:
                    await _derail_restore_claim(redis, derail_kind_b, derail_cont_after_b)
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
                if derail_kind_b and not burst_derail_committed:
                    await _derail_restore_claim(redis, derail_kind_b, derail_cont_after_b)
        if not picked:
            log.debug("factory_burst_skipped_no_session")

    return {"status": "completed", "burst_replies_attempted": count, "messages_sent": sent_n}


async def _factory_converse_slot(
    *,
    redis: Any,
    parameters: dict[str, Any],
    api_key: str,
    openai_key: str,
    slot_index: int,
    groups: list[dict[str, Any]],
    all_sessions: list[str],
    carry: dict[str, Any],
    news_digest_wake: bool = False,
) -> dict[str, Any]:
    """One session×group converse attempt; exceptions should be caught by the gather wrapper."""
    gi = slot_index % len(groups)
    si = slot_index % len(all_sessions)
    session_base = all_sessions[si]
    grp = groups[gi]
    group_id = grp.get("group_id")
    base_out: dict[str, Any] = {"slot": slot_index, "session": session_base[:48], "group_id": group_id}

    if group_id is None:
        return {**base_out, "status": "skipped", "reason": "group_id missing"}

    gid_int = int(group_id)

    try:
        import telethon  # noqa: F401
    except ImportError:
        return {**base_out, "status": "failed", "error": "telethon not installed"}

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
        if news_digest_wake:
            opener_fresh_event = True
            active_topic_line = None
    elif role == "replier" and active_record:
        active_topic_line = str(active_record.get("text") or "").strip() or None

    news_opener = role == "opener" and opener_fresh_event

    if news_opener and news_digest_wake:
        log.info(
            "factory_converse_news_digest_wake_slot",
            group_id=gid_int,
            **{"Source": "OpenClaw"},
        )

    if role == "lurk":
        if await _is_session_banned(redis, session_base):
            return {**base_out, "status": "skipped", "reason": "banned"}
        until = await _cooldown_until(redis, session_base)
        if until and datetime.now(timezone.utc) < until:
            return {**base_out, "status": "deferred", "reason": "cooldown"}
        return {**base_out, "status": "completed", "action": "lurk"}

    if news_opener:
        rotated = all_sessions[si:] + all_sessions[:si]
        picked_news: str | None = None
        for cand in rotated:
            if await _is_session_banned(redis, cand):
                continue
            until_c = await _cooldown_until(redis, cand)
            if until_c and datetime.now(timezone.utc) < until_c:
                continue
            if session_is_asleep_jerusalem(cand):
                continue
            picked_news = cand
            break
        if picked_news is None:
            return {
                **base_out,
                "status": "skipped",
                "reason": "news_opener_all_sleeping_or_busy",
            }
        session_base = picked_news
        base_out = {**base_out, "session": session_base[:48]}
    else:
        if await _is_session_banned(redis, session_base):
            return {**base_out, "status": "skipped", "reason": "banned"}
        until = await _cooldown_until(redis, session_base)
        if until and datetime.now(timezone.utc) < until:
            return {**base_out, "status": "deferred", "reason": "cooldown"}

    persona = _session_persona_seed(session_base)
    global_recent = await _redis_recent_outgoing_fetch(redis)

    if role == "reactor":
        anchor_id = thread_ids[-1]
        try:
            async with async_telegram_client(session_base, parameters) as client:
                if not await client.is_user_authorized():
                    await _mark_banned(redis, session_base)
                    await _bump_metric(redis, "bans", 1)
                    return {**base_out, "status": "skipped", "reason": "unauthorized"}
                ent = await client.get_entity(gid_int)
                await _ensure_factory_profile(client, redis, session_base)
                await asyncio.sleep(random.uniform(0.5, 4.0))
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
        return {**base_out, "status": "completed", "action": "reactor"}

    reply_to_id: int | None = None
    stance = random.choice(AMCHA_STANCES_HE)

    derail_kind: str | None = None
    derail_cont_after: int | None = None
    derail_send_done = False
    sent_list: list[Any] = []
    try:
        async with async_telegram_client(session_base, parameters) as client:
            if not await client.is_user_authorized():
                await _mark_banned(redis, session_base)
                await _bump_metric(redis, "bans", 1)
                return {**base_out, "status": "skipped", "reason": "unauthorized"}
            await _ensure_factory_profile(client, redis, session_base)
            ent = await client.get_entity(gid_int)
            if news_opener and random.random() < 0.7:
                try:
                    ok = await send_passive_group_reaction(client, ent, session_base)
                except Exception as exc:
                    log.debug("news_wake_passive_reaction_failed", error=str(exc))
                    ok = False
                if ok:
                    await _bump_metric(redis, "messages_sent", 1)
                return {
                    **base_out,
                    "status": "completed",
                    "action": "news_reaction_only",
                    "reaction_sent": ok,
                }
            refs_newest_first: list[tuple[int, str]] = []
            recent_texts: list[str] = []
            hist: list[Any] = []
            try:
                hist = await client.get_messages(ent, limit=RECENT_GROUP_MSG_CAP)
                if hist:
                    chronological = list(hist)
                    chronological.reverse()
                    refs_newest_first = _message_refs_newest_first(chronological)
                    recent_texts = [t for _, t in reversed(refs_newest_first)]
            except Exception as exc:
                log.debug("factory_recent_messages_failed", group_id=gid_int, error=str(exc))
            max_chain = int(os.getenv("COMMUNITY_FACTORY_BOT_CHAIN_MAX", "4"))
            if redis is not None and max_chain > 0:
                try:
                    me = await client.get_me()
                    if me and getattr(me, "id", None) is not None:
                        await redis.sadd(KEY_FACTORY_POOL_UIDS, str(int(me.id)))
                    raw_p = await redis.smembers(KEY_FACTORY_POOL_UIDS)
                    pool_ids = {int(x) for x in raw_p if str(x).isdigit()}
                    consec = _consecutive_pool_message_tail(hist or [], pool_ids)
                    if consec >= max_chain:
                        return {
                            **base_out,
                            "status": "skipped",
                            "reason": "bot_chain_cap",
                            "consecutive_swarm_tail": consec,
                        }
                except Exception as exc:
                    log.debug("factory_bot_chain_check_failed", error=str(exc))
            derail_kind, derail_cont_after = await _derail_claim_for_turn(redis)
            if derail_kind:
                news_opener = False
                opener_fresh_event = False
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
                        ap = _message_text_for_factory_prompt(m0)
                        anchor_preview = (ap or (getattr(m0, "message", None) or ""))[:500]
                except Exception:
                    anchor_preview = None

            privileged_anchor = False
            if reply_to_id is not None:
                try:
                    privileged_anchor = await sender_of_message_is_owner_or_admin(
                        client, ent, reply_to_id
                    )
                except Exception:
                    privileged_anchor = False

            lore_line = await _maybe_sample_lore_for_amcha(
                redis, privileged_anchor=privileged_anchor
            )
            derail_anchor = ""
            if derail_kind == "contagion" and redis is not None:
                derail_anchor = str(await redis.get(KEY_DERAIL_ANCHOR) or "")
            turn = await _generate_unique_amcha_turn(
                redis,
                gid_int,
                api_key=api_key,
                topic=topic,
                openai_key=openai_key,
                session_base=session_base,
                role="opener" if role == "opener" else "replier",
                stance_he=stance,
                last_five_block=last_five_block,
                anchor_preview=anchor_preview,
                recent_texts=recent_texts,
                active_topic_line=active_topic_line,
                opener_fresh_event=opener_fresh_event,
                news_opener=news_opener,
                persona_seed=persona,
                rich_media_mode=True,
                community_lore_line=lore_line,
                global_recent_outgoing=global_recent,
                privileged_anchor=privileged_anchor,
                derail_mode=derail_kind,
                derail_anchor_text=derail_anchor or None,
            )
            if turn is None:
                await _derail_restore_claim(redis, derail_kind, derail_cont_after)
                return {**base_out, "status": "skipped", "reason": "antiparrot_group_recent"}
            if derail_kind == "seed":
                mt_seed = str(turn.get("primary_message") or turn.get("message_text") or "").strip()
                if not mt_seed:
                    mt_seed = "מישהו מכיר שרברב טוב בפתח תקווה בלי עקיצות"
                turn = {
                    **turn,
                    "action_type": "text",
                    "message_text": mt_seed,
                    "primary_message": mt_seed,
                    "article_url": "",
                    "link_label": "",
                    "needs_correction": False,
                    "correction_message": "",
                }
            original_action = str(turn.get("action_type") or "text").strip().lower()
            media_slot_acquired = False
            if original_action in ("sticker", "gif", "image"):
                # Only thread openers post rich media; repliers stay text (burst replies anchor on the media msg).
                if role != "opener":
                    mt_coerce = str(turn.get("message_text") or turn.get("primary_message") or "").strip()
                    if not mt_coerce:
                        mt_coerce = _default_hebrew_media_companion(
                            original_action, str(turn.get("image_query") or "")
                        )
                    turn = {
                        **turn,
                        "action_type": random.choice(("text", "text_with_emoji")),
                        "message_text": mt_coerce,
                        "primary_message": mt_coerce,
                    }
                else:
                    media_slot_acquired = await _try_acquire_factory_media_slot(redis, gid_int)
                    if not media_slot_acquired:
                        mt_coerce = str(turn.get("message_text") or turn.get("primary_message") or "").strip()
                        if not mt_coerce:
                            mt_coerce = _default_hebrew_media_companion(
                                original_action, str(turn.get("image_query") or "")
                            )
                        turn = {
                            **turn,
                            "action_type": random.choice(("text", "text_with_emoji")),
                            "message_text": mt_coerce,
                            "primary_message": mt_coerce,
                        }
            turn["_persona_seed"] = persona
            turn["_media_salt_seed"] = make_image_upload_salt_seed(session_base)

            use_md = False
            summary_line = _finalize_primary_message(
                str(turn.get("primary_message") or turn.get("message_text") or "")
            )
            if news_opener:
                url = await _maybe_tinyurl_shorten(str(turn.get("article_url") or ""))
                summary_line, use_md = _format_opener_with_md_link(
                    str(turn.get("primary_message") or ""),
                    url,
                    str(turn.get("link_label") or ""),
                )
            elif not summary_line:
                at = str(turn.get("action_type") or "text")
                summary_line = {"sticker": "🎭 סטיקר", "gif": "🎞 גיף", "image": "🖼 תמונה"}.get(
                    at, "הודעה"
                )

            await asyncio.sleep(
                _reading_delay_before_typing_seconds(
                    last_five_block,
                    str(anchor_preview or ""),
                    str(active_topic_line or ""),
                )
            )
            async with client.action(ent, "typing"):
                await asyncio.sleep(random.uniform(0.3, 1.5))

            sent_list = await _send_rich_factory_messages(
                client,
                ent,
                turn,
                reply_to_id=reply_to_id,
                news_opener=news_opener,
                use_md=use_md,
                redis=redis,
                session_base=session_base,
            )
            if derail_kind and not sent_list:
                await _derail_restore_claim(redis, derail_kind, derail_cont_after)
            elif sent_list:
                derail_send_done = True
                await _global_swarm_bump_after_send(
                    redis,
                    n_bot_messages=len(sent_list),
                    derail_kind=derail_kind,
                    contagion_after=derail_cont_after,
                    seed_anchor_text=summary_line if derail_kind == "seed" else None,
                )
            sent = sent_list[0] if sent_list else None
            mid = getattr(sent, "id", None) if sent is not None else None
            if mid is not None and role == "opener":
                await _thread_ids_push(redis, gid_int, int(mid))
            if role == "opener" and opener_fresh_event and not derail_kind:
                await _active_topic_write(redis, gid_int, summary_line)
            if mid is not None and news_opener:
                burst_carry = dict(carry)
                burst_carry["burst_group_id"] = gid_int
                burst_carry["burst_reply_to_msg_id"] = int(mid)
                burst_carry["burst_count"] = random.randint(4, 8)
                await _enqueue_task("swarm.community_factory.burst_reply_chain", burst_carry)
            if (
                mid is not None
                and original_action in ("sticker", "gif", "image")
                and media_slot_acquired
                and not news_opener
            ):
                mburst = dict(carry)
                mburst["burst_group_id"] = gid_int
                mburst["burst_reply_to_msg_id"] = int(mid)
                try:
                    bn = int(os.getenv("COMMUNITY_FACTORY_MEDIA_BURST_REPLIES", "5") or "5")
                except ValueError:
                    bn = 5
                mburst["burst_count"] = max(1, min(12, bn))
                await _enqueue_task("swarm.community_factory.burst_reply_chain", mburst)

            recent_frag = summary_line or str(turn.get("message_text") or turn.get("primary_message") or "")
            await _redis_recent_outgoing_push(redis, recent_frag)
            if sent_list:
                ap_frag = _factory_turn_antiparrot_compare_text(
                    turn, news_opener=news_opener, rich_media_mode=True
                )
                push_body = (ap_frag or summary_line or recent_frag).strip()[:600]
                if push_body:
                    await _factory_group_recent_sent_push(redis, gid_int, push_body)

        await _bump_metric(redis, "messages_sent", len(sent_list))
    except ValueError as exc:
        log.warning("factory_converse_creds_missing", error=str(exc))
        if derail_kind and not derail_send_done:
            await _derail_restore_claim(redis, derail_kind, derail_cont_after)
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
        if derail_kind and not derail_send_done:
            await _derail_restore_claim(redis, derail_kind, derail_cont_after)

    return {**base_out, "status": "completed", "action": role}


@registry.register("swarm.community_factory.converse_tick")
async def community_factory_converse_tick(parameters: dict[str, Any]) -> dict[str, Any]:
    redis = parameters.get("__redis__")
    news_digest_wake_once = bool(parameters.pop("news_digest_wake", False))
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

    carry = dict(parameters)
    carry.pop("__redis__", None)

    remaining = stop_after - cidx
    batch_size = min(_converse_batch_size(), remaining)
    if parameters.get("converse_batch_size") is not None:
        try:
            batch_size = min(max(1, min(20, int(parameters.get("converse_batch_size")))), remaining)
        except (TypeError, ValueError):
            batch_size = min(_converse_batch_size(), remaining)

    state["converse_idx"] = cidx + batch_size
    await _redis_json_set(redis, KEY_STATE, state)

    async def _enqueue_next() -> None:
        await _enqueue_task("swarm.community_factory.converse_tick", carry)

    async def _safe_slot(k: int) -> None:
        try:
            await _factory_converse_slot(
                redis=redis,
                parameters=parameters,
                api_key=api_key,
                openai_key=openai_key,
                slot_index=cidx + k,
                groups=groups,
                all_sessions=all_sessions,
                carry=carry,
                news_digest_wake=news_digest_wake_once,
            )
        except Exception as exc:
            log.warning("factory_converse_slot_failed", slot=cidx + k, error=str(exc))

    # Slots may share a group_id; Redis thread/active_topic updates are best-effort under concurrency.
    await asyncio.gather(*[_safe_slot(k) for k in range(batch_size)])
    await _enqueue_next()
    return {"status": "completed", "batch_size": batch_size, "parallel_slots": batch_size}
