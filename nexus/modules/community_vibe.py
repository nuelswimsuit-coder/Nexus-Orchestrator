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
        "You write one authentic Telegram group message. "
        "Use natural emoji and slang matching the speaker's voice. "
        "Respond to others when it fits; @mention usernames from the list (with @). "
        "If news_from_last_24h is non-empty, you MUST react to one specific real headline "
        "from that list (paraphrase OK, stay grounded in what is written there). "
        "Do not invent fake breaking stories that are not implied by the digest. "
        "Keep it short like a real chat line (often <= 280 chars). "
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
    nd = (news_digest or "").strip()
    if nd:
        user_obj["news_from_last_24h"] = nd[:8000]
    ah = (anchor_headline or "").strip()
    if ah:
        user_obj["preferred_anchor_headline"] = ah[:500]
    user = json.dumps(user_obj, ensure_ascii=False)
    try:
        return await _gemini_json(api_key, sys_prompt, user, temperature=0.92, max_tokens=256)
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
