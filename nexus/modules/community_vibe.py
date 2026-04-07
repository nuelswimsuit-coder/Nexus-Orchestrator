"""
Community vibe + chatter planning — Gemini 1.5 Flash JSON helpers.

Used by ``swarm.group_warmer`` for persona assignment, topic selection,
message composition, and periodic community classification.

Performance: shared ``httpx`` client (pooling), ``ujson`` for bodies/parsing,
tight ``maxOutputTokens``; paid chatter calls use a 3-line local summary + news only.
(Redis batching belongs in callers; this module does not touch Redis.)
"""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any

import httpx
import structlog
import ujson

from nexus.services.conversation_summary_context import (
    reply_target_ids_newest_first,
    speaker_persona_for_paid_api,
    summarize_transcript_for_paid_api,
)
from nexus.services.tg_message_text import (
    purge_absolute_news_source_blacklist,
    strip_trailing_israeli_news_outlet,
)

log = structlog.get_logger(__name__)

GEMINI_MODEL = "gemini-1.5-flash"
GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_MODEL}:generateContent"
)

# Smaller context → faster inference (Ollama path mirrors prompts upstream).
_TRANSCRIPT_REFRESH_MAX = 4000
_TRANSCRIPT_CLASSIFY_MAX = 6000
_NEWS_DIGEST_MAX = 4000
_ANCHOR_HEADLINE_MAX = 300
_HOOKS_MAX = 12
_MSG_INDEX_MAP_MAX = 20

# Strict generation caps (Gemini maxOutputTokens).
_TOK_DEFAULT = 512
_TOK_PERSONAS = 320
_TOK_TOPIC = 240
_TOK_CHATTER = 64
_TOK_CLASSIFY = 280

_JSON_HEADERS = {"Content-Type": "application/json; charset=utf-8"}

_RE_HASHTAG = re.compile(r"#\S+")
_RE_SPACES = re.compile(r"\s{2,}")
_RE_AT_HANDLE = re.compile(r"@[\w\d_]+")
_RE_MD_FENCE_OPEN = re.compile(r"^```(?:json)?\s*", re.IGNORECASE)
_RE_MD_FENCE_CLOSE = re.compile(r"\s*```$")

_LAZY_NEWS_PREFIXES_HE: tuple[str, ...] = (
    "שמעתם כבר על ",
    "שמעתם כבר ",
    "שמעת על ",
    "שמעת ",
    "דיווח: ",
    "דיווח ",
    "לפי דיווח ",
    "לפי הדיווח ",
    "ראיתם מה ",
    "ראית ",
    "חדשות: ",
    "פלאש: ",
    "עכשיו ב",
    "מתפרסם ש",
    "פורסם ש",
)
_BANNED_SLACK_OPENERS_HE: tuple[str, ...] = (
    "תכלס וואלה",
    "תכלס, וואלה",
    "אמאלה רצח",
    "אמאלה, רצח",
)
_CHATTER_OUTLET_TAIL_RE = re.compile(
    r"\s*[-–—]\s*("
    r"\[[^\]]+\]"
    r"|(?i)ynet|n12|n13|mako|calcalist|walla|themarker|timesofisrael|times\s+of\s+israel|haaretz|kan\s*11|google[\s-]*news"
    r"|מעריב|הארץ|גלובס|כלכליסט|וואלה|חדשות\s*13|ני12|מאקו|גלי\s*צהל"
    r")\s*$",
    re.UNICODE,
)

_gemini_client: httpx.AsyncClient | None = None
_gemini_client_lock = asyncio.Lock()


def _dumps_bytes(obj: Any) -> bytes:
    return ujson.dumps(obj, ensure_ascii=False).encode("utf-8")


def _loads_dict(data: str | bytes) -> Any:
    try:
        return ujson.loads(data)
    except (ValueError, TypeError):
        return json.loads(data)


async def _gemini_http_client() -> httpx.AsyncClient:
    global _gemini_client
    if _gemini_client is None:
        async with _gemini_client_lock:
            if _gemini_client is None:
                _gemini_client = httpx.AsyncClient(
                    timeout=httpx.Timeout(60.0, connect=10.0),
                    limits=httpx.Limits(max_keepalive_connections=32, max_connections=64),
                )
    return _gemini_client


def _strip_hashtags_and_cleanup(text: str) -> str:
    s = _RE_HASHTAG.sub("", text or "")
    s = _RE_SPACES.sub(" ", s).strip()
    return s


def _strip_lazy_news_openers_he(text: str) -> str:
    s = (text or "").strip()
    if not s:
        return s
    t = s
    for _ in range(6):
        hit = False
        for p in _LAZY_NEWS_PREFIXES_HE:
            if t.startswith(p):
                t = t[len(p) :].lstrip(" -–—:?!")
                hit = True
                break
        if not hit:
            break
    return (t.strip() or s).strip()


def _strip_banned_slack_openers_he(text: str) -> str:
    s = (text or "").strip()
    if not s:
        return s
    t = s
    for _ in range(4):
        hit = False
        for p in _BANNED_SLACK_OPENERS_HE:
            if t.startswith(p):
                t = t[len(p) :].lstrip(" ,.:;!?")
                hit = True
                break
        if not hit:
            break
    return (t.strip() or s).strip()


def _strip_at_mentions(text: str) -> str:
    s = _RE_AT_HANDLE.sub("", text or "")
    return _RE_SPACES.sub(" ", s).strip()


def _strip_trailing_news_attribution(text: str) -> str:
    s = (text or "").rstrip()
    for _ in range(5):
        m = _CHATTER_OUTLET_TAIL_RE.search(s)
        if not m:
            break
        s = s[: m.start()].rstrip()
    return s


def _cap_words(text: str, max_words: int = 10) -> str:
    raw = text or ""
    parts = raw.split()
    if len(parts) <= max_words:
        return raw.strip()
    return " ".join(parts[:max_words])


def _finalize_chatter_line(text: str) -> str:
    s = _strip_hashtags_and_cleanup(text)
    s = _strip_lazy_news_openers_he(s)
    s = _strip_banned_slack_openers_he(s)
    s = strip_trailing_israeli_news_outlet(s)
    s = _strip_trailing_news_attribution(s)
    s = purge_absolute_news_source_blacklist(s)
    s = _strip_at_mentions(s)
    s = _cap_words(s, 10)
    if s and " " not in s:
        s = f"{s} אחי"
    return s.strip()


def parse_json_object(raw: str) -> dict[str, Any]:
    """Strip optional markdown fences and parse a single JSON object."""
    text = raw.strip()
    if text.startswith("```"):
        text = _RE_MD_FENCE_OPEN.sub("", text)
        text = _RE_MD_FENCE_CLOSE.sub("", text)
        text = text.strip()
    if "{" in text:
        start = text.index("{")
        depth = 0
        for i, ch in enumerate(text[start:], start=start):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return _loads_dict(text[start : i + 1])
    return _loads_dict(text)


async def _gemini_json(
    api_key: str,
    system_instruction: str,
    user_text: str,
    *,
    temperature: float = 0.85,
    max_tokens: int = _TOK_DEFAULT,
    frequency_penalty: float | None = None,
    presence_penalty: float | None = None,
    top_p: float | None = None,
) -> dict[str, Any]:
    url = f"{GEMINI_URL}?key={api_key}"
    combined = f"{system_instruction}\n\n---\n\n{user_text}"
    gen_cfg: dict[str, Any] = {
        "temperature": temperature,
        "maxOutputTokens": max_tokens,
    }
    if top_p is not None:
        gen_cfg["topP"] = float(top_p)
    if frequency_penalty is not None:
        gen_cfg["frequencyPenalty"] = frequency_penalty
    if presence_penalty is not None:
        gen_cfg["presencePenalty"] = presence_penalty
    payload_primary = {
        "systemInstruction": {"parts": [{"text": system_instruction}]},
        "contents": [{"role": "user", "parts": [{"text": user_text}]}],
        "generationConfig": gen_cfg,
    }
    payload_fallback = {
        "contents": [{"role": "user", "parts": [{"text": combined}]}],
        "generationConfig": gen_cfg,
    }
    client = await _gemini_http_client()
    resp = await client.post(url, content=_dumps_bytes(payload_primary), headers=_JSON_HEADERS)
    if resp.status_code >= 400:
        resp = await client.post(url, content=_dumps_bytes(payload_fallback), headers=_JSON_HEADERS)
    resp.raise_for_status()
    data = _loads_dict(resp.content)
    cands = data.get("candidates") or []
    if not cands:
        raise ValueError("gemini_empty_candidates")
    parts = cands[0].get("content", {}).get("parts") or []
    if not parts or "text" not in parts[0]:
        raise ValueError("gemini_no_text")
    raw = parts[0]["text"]
    return parse_json_object(raw)


async def assign_personas(
    api_key: str,
    accounts: list[dict[str, Any]],
    group_hint: str,
) -> list[dict[str, Any]]:
    """
    Assign a distinct persona archetype to each account (AI-generated).

    ``accounts`` items: ``session_path``, ``username`` (optional).
    """
    if not api_key or not accounts:
        return []
    sys_prompt = (
        "Invent distinct chat personas. Reply with JSON only: "
        '{"personas":[{"session_path":"","username":"","archetype":"short label",'
        '"voice":"typing style","slang_notes":"terms"}]}. '
        "Unique archetypes; empty usernames OK."
    )
    user = ujson.dumps({"accounts": accounts, "group_context_hint": group_hint}, ensure_ascii=False)
    try:
        out = await _gemini_json(
            api_key, sys_prompt, user, temperature=0.9, max_tokens=_TOK_PERSONAS
        )
        personas = out.get("personas") or []
        if isinstance(personas, list) and len(personas) >= len(accounts):
            trimmed = personas[: len(accounts)]
            for i, acc in enumerate(accounts):
                if i < len(trimmed) and isinstance(trimmed[i], dict):
                    trimmed[i]["session_path"] = acc.get("session_path", "")
                    trimmed[i]["username"] = acc.get("username") or trimmed[i].get("username", "")
            return trimmed
        merged: list[dict[str, Any]] = []
        for i, acc in enumerate(accounts):
            p = personas[i] if i < len(personas) else {}
            merged.append(
                {
                    "session_path": acc.get("session_path", ""),
                    "username": acc.get("username") or p.get("username", ""),
                    "archetype": p.get("archetype", f"Persona {i + 1}"),
                    "voice": p.get("voice", "casual, concise"),
                    "slang_notes": p.get("slang_notes", ""),
                }
            )
        return merged
    except Exception as exc:
        log.warning("assign_personas_failed", error=str(exc))
        return [
            {
                "session_path": a.get("session_path", ""),
                "username": a.get("username", ""),
                "archetype": f"Voice {i + 1}",
                "voice": "natural human chat",
                "slang_notes": "",
            }
            for i, a in enumerate(accounts)
        ]


async def refresh_emerging_topic(
    api_key: str,
    transcript: str,
    prior_identity: str,
    group_title: str,
) -> dict[str, Any]:
    """Infer emerging identity and a fresh on-topic discussion thread."""
    sys_prompt = (
        "From chat, infer group identity. JSON only: "
        '{"emerging_identity":"2-4 sentences","discussion_topic":"next-message angle",'
        '"in_universe_hooks":["niche memes/rumors"]}'
    )
    user = ujson.dumps(
        {
            "group_title": group_title,
            "prior_emerging_identity": prior_identity,
            "recent_transcript": transcript[-_TRANSCRIPT_REFRESH_MAX:],
        },
        ensure_ascii=False,
    )
    try:
        return await _gemini_json(
            api_key, sys_prompt, user, temperature=0.88, max_tokens=_TOK_TOPIC
        )
    except Exception as exc:
        log.warning("refresh_emerging_topic_failed", error=str(exc))
        return {
            "emerging_identity": prior_identity or "General community chat",
            "discussion_topic": "what people are already talking about",
            "in_universe_hooks": [],
        }


async def compose_chatter_line(
    api_key: str,
    *,
    emerging_identity: str,
    topic: str,
    hooks: list[str],
    transcript: str,
    speaker: dict[str, Any],
    other_handles: list[str],
    message_index_map: list[dict[str, Any]],
    news_digest: str = "",
    anchor_headline: str = "",
    privileged_reply_target: bool = False,
    forced_reply_to_id: int | None = None,
    drama_directive: str | None = None,
    require_peer_mention: bool = False,
) -> dict[str, Any]:
    """
    Produce one chat line; threading uses Telegram reply_to only (no @ in text).

    ``message_index_map``: [{\"id\": telegram int, \"sender\": str}, ...] newest first.
    """
    sys_prompt = (
        "One Telegram line: impatient Israeli, not newsreader. 2–10 words. "
        "NEVER use the @ symbol. NEVER type a username or handle manually. "
        "Just write your conversational response. To engage someone, set reply_to_id to "
        "a Telegram message id from reply_target_ids_newest_first (newest first); leave mention_usernames always [].\n"
        "Context is state_of_conversation only (3-line summary of recent chat) plus optional news_from_last_24h — "
        "not full raw history.\n"
        "If news_from_last_24h set: one casual reaction, own words — no headline paste, "
        "no [source], no '- ynet' / '- מעריב' / outlet names (גלובס, וואלה, N12, ערוץ 12, etc.).\n"
        "Forbidden openers: שמעתם כבר, דיווח:, ראיתם מה, לפי דיווח, תכלס וואלה, אמאלה רצח — "
        "start mid-thought like a real human (no generic filler prefix).\n"
        "If reply_to_id set: no repeating their facts — reaction only.\n"
        'JSON only: {"text":"...","reply_to_id":null|int,"mention_usernames":[]}'
    )
    if require_peer_mention and other_handles and not privileged_reply_target:
        sys_prompt += (
            " MANDATORY: set reply_to_id to one of the ids in reply_target_ids_newest_first "
            "(prefer engaging another participant over talking into the void); still no @ in text."
        )
    if drama_directive and not privileged_reply_target:
        sys_prompt += f"\nScene: {drama_directive.strip()}"
    if privileged_reply_target:
        sys_prompt += (
            " Replying to owner/admin: neutral, brief, respectful — no insults/profanity."
        )
    if forced_reply_to_id is not None:
        sys_prompt += f" reply_to_id must be {int(forced_reply_to_id)}."

    state_of_conversation = await summarize_transcript_for_paid_api(transcript)
    target_ids = reply_target_ids_newest_first(
        message_index_map, cap=_MSG_INDEX_MAP_MAX
    )
    user_obj: dict[str, Any] = {
        "emerging_identity": emerging_identity,
        "discussion_topic": topic,
        "in_universe_hooks": hooks[:_HOOKS_MAX],
        "state_of_conversation": state_of_conversation,
        "speaker_persona": speaker_persona_for_paid_api(speaker),
        "reply_target_ids_newest_first": target_ids,
    }
    if forced_reply_to_id is not None:
        user_obj["required_reply_to_id"] = int(forced_reply_to_id)
        user_obj["forced_reply_rule"] = (
            "Reply to that id: 2–10 words, reaction only, no restating their wording."
        )
    nd = (news_digest or "").strip()
    if nd:
        user_obj["news_from_last_24h"] = nd[:_NEWS_DIGEST_MAX]
    ah = (anchor_headline or "").strip()
    if ah:
        user_obj["preferred_anchor_headline"] = ah[:_ANCHOR_HEADLINE_MAX]
    user = ujson.dumps(user_obj, ensure_ascii=False)
    base_instruction = sys_prompt
    try:
        out: dict[str, Any] = {"text": "", "reply_to_id": None, "mention_usernames": []}
        for attempt in range(3):
            regen_note = ""
            if attempt:
                regen_note = (
                    "\nREGEN: Previous line was rejected (empty after safety filter). "
                    "Again: no @, no outlets, mid-thought opener only."
                )
            raw_out = await _gemini_json(
                api_key,
                base_instruction + regen_note,
                user,
                temperature=min(1.0, 0.88 + 0.05 * attempt),
                max_tokens=_TOK_CHATTER,
            )
            if not isinstance(raw_out, dict):
                raw_out = {}
            finalized = _finalize_chatter_line(str(raw_out.get("text") or ""))
            out = {
                **raw_out,
                "text": finalized,
                "mention_usernames": [],
            }
            if finalized or attempt == 2:
                return out
        return out
    except Exception as exc:
        log.warning("compose_chatter_failed", error=str(exc))
        return {"text": "", "reply_to_id": None, "mention_usernames": []}


async def classify_community(
    api_key: str,
    transcript: str,
    group_title: str,
) -> dict[str, Any]:
    """24h vibe scan — short dashboard labels + description text."""
    sys_prompt = (
        "Classify chat vibe for ops dashboard. JSON only: "
        '{"community_identity":"2-5 words",'
        '"group_description":"<=220 chars Telegram about",'
        '"emerging_identity":"2-3 sentences"}'
    )
    user = ujson.dumps(
        {
            "group_title": group_title,
            "transcript": transcript[-_TRANSCRIPT_CLASSIFY_MAX:],
        },
        ensure_ascii=False,
    )
    try:
        return await _gemini_json(
            api_key, sys_prompt, user, temperature=0.55, max_tokens=_TOK_CLASSIFY
        )
    except Exception as exc:
        log.warning("classify_community_failed", error=str(exc))
        return {
            "community_identity": "Community Hub",
            "group_description": "",
            "emerging_identity": "",
        }
