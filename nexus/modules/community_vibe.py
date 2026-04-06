"""
Community vibe + chatter planning — Gemini 1.5 Flash JSON helpers.

Used by ``swarm.group_warmer`` for persona assignment, topic selection,
message composition, and periodic community classification.
"""

from __future__ import annotations

import json
import re
from typing import Any

import structlog

log = structlog.get_logger(__name__)

GEMINI_MODEL = "gemini-1.5-flash"
GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_MODEL}:generateContent"
)

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
_CHATTER_OUTLET_TAIL_RE = re.compile(
    r"\s*[-–—]\s*("
    r"\[[^\]]+\]"
    r"|(?i)ynet|n12|n13|mako|calcalist|walla|themarker|timesofisrael|times\s+of\s+israel|haaretz|kan\s*11|google[\s-]*news"
    r"|מעריב|הארץ|גלובס|כלכליסט|וואלה|חדשות\s*13|ני12|מאקו|גלי\s*צהל"
    r")\s*$",
    re.UNICODE,
)


def _strip_hashtags_and_cleanup(text: str) -> str:
    s = re.sub(r"#\S+", "", text or "")
    s = re.sub(r"\s{2,}", " ", s).strip()
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


def _strip_trailing_news_attribution(text: str) -> str:
    s = (text or "").rstrip()
    for _ in range(5):
        m = _CHATTER_OUTLET_TAIL_RE.search(s)
        if not m:
            break
        s = s[: m.start()].rstrip()
    return s


def _cap_words(text: str, max_words: int = 10) -> str:
    parts = (text or "").split()
    if len(parts) <= max_words:
        return (text or "").strip()
    return " ".join(parts[:max_words])


def _finalize_chatter_line(text: str) -> str:
    s = _strip_hashtags_and_cleanup(text)
    s = _strip_lazy_news_openers_he(s)
    s = _strip_trailing_news_attribution(s)
    s = _cap_words(s, 10)
    parts = s.split()
    if len(parts) == 1 and parts[0]:
        s = f"{parts[0]} אחי"
    return s.strip()


def parse_json_object(raw: str) -> dict[str, Any]:
    """Strip optional markdown fences and parse a single JSON object."""
    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    # Some models wrap with extra prose; grab outermost {...}
    if "{" in text:
        start = text.index("{")
        depth = 0
        for i, ch in enumerate(text[start:], start=start):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return json.loads(text[start : i + 1])
    return json.loads(text)


async def _gemini_json(
    api_key: str,
    system_instruction: str,
    user_text: str,
    *,
    temperature: float = 0.85,
    max_tokens: int = 1024,
    frequency_penalty: float | None = None,
    presence_penalty: float | None = None,
) -> dict[str, Any]:
    import httpx

    url = f"{GEMINI_URL}?key={api_key}"
    combined = f"{system_instruction}\n\n---\n\n{user_text}"
    gen_cfg: dict[str, Any] = {
        "temperature": temperature,
        "maxOutputTokens": max_tokens,
    }
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
        "generationConfig": dict(gen_cfg),
    }
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(url, json=payload_primary)
        if resp.status_code >= 400:
            resp = await client.post(url, json=payload_fallback)
        resp.raise_for_status()
        data = resp.json()
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
        "You invent realistic chat personas for organic group discussion. "
        "Output ONLY valid JSON: {\"personas\":[{\"session_path\":\"\",\"username\":\"\","
        "\"archetype\":\"short label e.g. The Skeptic\",\"voice\":\"how they type\","
        "\"slang_notes\":\"terms they favor\"}]}. "
        "Each archetype must be unique. Usernames may be empty — still assign personas."
    )
    user = json.dumps(
        {"accounts": accounts, "group_context_hint": group_hint},
        ensure_ascii=False,
    )
    try:
        out = await _gemini_json(api_key, sys_prompt, user, temperature=0.9)
        personas = out.get("personas") or []
        if isinstance(personas, list) and len(personas) >= len(accounts):
            trimmed = personas[: len(accounts)]
            for i, acc in enumerate(accounts):
                if i < len(trimmed) and isinstance(trimmed[i], dict):
                    trimmed[i]["session_path"] = acc.get("session_path", "")
                    trimmed[i]["username"] = acc.get("username") or trimmed[i].get("username", "")
            return trimmed
        # Pad / merge by index
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
        "You shape a group's 'emerging identity' from chat. "
        "Output ONLY JSON: {\"emerging_identity\":\"2-4 sentences\","
        "\"discussion_topic\":\"specific angle for next messages\","
        "\"in_universe_hooks\":[\"optional rumor or meme names fitting the niche\"]}"
    )
    user = json.dumps(
        {
            "group_title": group_title,
            "prior_emerging_identity": prior_identity,
            "recent_transcript": transcript[-8000:],
        },
        ensure_ascii=False,
    )
    try:
        return await _gemini_json(api_key, sys_prompt, user, temperature=0.88)
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
) -> dict[str, Any]:
    """
    Produce one chat line with optional reply and @mentions.

    ``message_index_map``: [{\"id\": telegram int, \"sender\": str}, ...] newest first.
    """
    sys_prompt = (
        "You write one authentic Telegram group line as an impatient Israeli — NOT a newsreader.\n"
        "Rules: 2–10 words only (count them). Natural emoji/slang matching the speaker. "
        "@mention from the list when it fits (with @).\n"
        "If news_from_last_24h is non-empty, internalize ONE real item and output a fresh casual reaction "
        "in your own words — NEVER copy/paste the headline, NEVER print [source] tags or '- ynet' / '- מעריב' / outlet names.\n"
        "FORBIDDEN openers: 'שמעתם כבר', 'דיווח:', 'ראיתם מה', 'לפי דיווח'.\n"
        "If you reply to another message (reply_to_id set): do NOT repeat their facts — only opinion, joke, complaint, or disagreement.\n"
        "Do not invent stories not implied by the digest. "
        "Output ONLY JSON: {\"text\":\"message\",\"reply_to_id\":null or integer,"
        "\"mention_usernames\":[\"without@\"]}"
    )
    if privileged_reply_target:
        sys_prompt += (
            " The message you reply to is from a group owner or admin: never argue, insult, "
            "or use profanity; stay neutral, brief, and respectful."
        )
    if forced_reply_to_id is not None:
        sys_prompt += (
            f" You MUST set reply_to_id in your JSON to exactly {int(forced_reply_to_id)}."
        )
    user_obj: dict[str, Any] = {
        "emerging_identity": emerging_identity,
        "discussion_topic": topic,
        "in_universe_hooks": hooks,
        "recent_transcript": transcript[-6000:],
        "speaker_persona": speaker,
        "other_participant_handles": other_handles,
        "message_ids_newest_first": message_index_map[:25],
    }
    if forced_reply_to_id is not None:
        user_obj["required_reply_to_id"] = int(forced_reply_to_id)
        user_obj["forced_reply_rule"] = (
            "You MUST reply to that message: 2–10 words, do NOT restate their facts or wording — reaction only."
        )
    nd = (news_digest or "").strip()
    if nd:
        user_obj["news_from_last_24h"] = nd[:8000]
    ah = (anchor_headline or "").strip()
    if ah:
        user_obj["preferred_anchor_headline"] = ah[:500]
    user = json.dumps(user_obj, ensure_ascii=False)
    try:
        out = await _gemini_json(api_key, sys_prompt, user, temperature=0.92, max_tokens=96)
        if isinstance(out, dict):
            out["text"] = _finalize_chatter_line(str(out.get("text") or ""))
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
        "Classify the vibe of this chat for an operator dashboard. "
        "Output ONLY JSON: {\"community_identity\":\"2-5 words e.g. Active Trading Floor\","
        "\"group_description\":\"<=220 chars suitable for Telegram about text\","
        "\"emerging_identity\":\"2-3 sentences internal summary\"}"
    )
    user = json.dumps(
        {"group_title": group_title, "transcript": transcript[-12000:]},
        ensure_ascii=False,
    )
    try:
        return await _gemini_json(api_key, sys_prompt, user, temperature=0.55, max_tokens=512)
    except Exception as exc:
        log.warning("classify_community_failed", error=str(exc))
        return {
            "community_identity": "Community Hub",
            "group_description": "",
            "emerging_identity": "",
        }
