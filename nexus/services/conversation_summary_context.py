"""
Summary-as-context for paid LLM calls (Gemini).

Takes the last N plain chat lines (metadata stripped), summarizes via a local
Ollama instance (e.g. Mac Mini), and returns exactly three lines:
"State of the Conversation" for the paid API payload.

Environment
-----------
NEXUS_CONVERSATION_SUMMARY_OLLAMA_URL — optional; overrides base URL below
NEXUS_OLLAMA_BASE_URL / OLLAMA_HOST — Ollama base (no /api/chat suffix)
NEXUS_CONVERSATION_SUMMARY_OLLAMA_MODEL — optional; else NEXUS_OLLAMA_MODEL or llama3
NEXUS_CONVERSATION_SUMMARY_MESSAGES — max raw lines to feed the local model (default 20)
"""

from __future__ import annotations

import os
import re
from typing import Any

import httpx
import structlog

log = structlog.get_logger(__name__)

_RE_BRACKET_MSG = re.compile(r"^\[\d+\]\s*[^:]+:\s*(.*)$")
_RE_USER_NUMERIC = re.compile(r"^user:\d+:\s*(.*)$", re.IGNORECASE)
_RE_AT_USER = re.compile(r"^@\S+\s*:\s*(.*)$")

_DEFAULT_SUMMARY_MESSAGES = 20
_MAX_PLAIN_BLOCK_CHARS = 4500
_MAX_LINE_CHARS = 320


def _summary_message_limit() -> int:
    raw = (os.getenv("NEXUS_CONVERSATION_SUMMARY_MESSAGES") or "").strip()
    if not raw:
        return _DEFAULT_SUMMARY_MESSAGES
    try:
        return max(5, min(50, int(raw)))
    except ValueError:
        return _DEFAULT_SUMMARY_MESSAGES


def resolve_conversation_summary_ollama_url() -> str:
    return (
        (os.getenv("NEXUS_CONVERSATION_SUMMARY_OLLAMA_URL") or "")
        .strip()
        .rstrip("/")
        or (os.getenv("NEXUS_OLLAMA_BASE_URL") or os.getenv("OLLAMA_HOST") or "")
        .strip()
        .rstrip("/")
    )


def resolve_conversation_summary_ollama_model() -> str:
    return (
        (os.getenv("NEXUS_CONVERSATION_SUMMARY_OLLAMA_MODEL") or "").strip()
        or (os.getenv("NEXUS_OLLAMA_MODEL") or "").strip()
        or "llama3"
    )


def strip_transcript_line_to_plain(line: str) -> str:
    """Remove Telegram message id, user id, @username, and leading name labels."""
    s = (line or "").strip()
    if not s:
        return ""
    for pat in (_RE_BRACKET_MSG, _RE_USER_NUMERIC, _RE_AT_USER):
        m = pat.match(s)
        if m:
            return (m.group(1) or "").strip()
    if ":" in s:
        head, tail = s.split(":", 1)
        if head.strip().startswith("@") or head.strip().lower().startswith("user:"):
            return tail.strip()
    return s


def plain_message_lines_from_transcript(transcript: str, *, max_messages: int | None = None) -> list[str]:
    n = max_messages if max_messages is not None else _summary_message_limit()
    out: list[str] = []
    for raw in (transcript or "").splitlines():
        plain = strip_transcript_line_to_plain(raw)
        plain = plain[:_MAX_LINE_CHARS].strip()
        if plain:
            out.append(plain)
    return out[-n:] if len(out) > n else out


def _fallback_three_line_state(lines: list[str]) -> str:
    if not lines:
        return "אין הודעות טקסטואליות לאחרונה.\nהשיחה שקטה.\nאין נושא בולט."
    blob = " ".join(lines)
    blob = blob[:900]
    parts = blob.split()
    if len(parts) < 12:
        a, b, c = lines[0], lines[len(lines) // 2] if len(lines) > 1 else "—", lines[-1] if len(lines) > 1 else "—"
        return "\n".join(x[:200] for x in (a, b, c))
    third = max(1, len(parts) // 3)
    chunks = [parts[i : i + third] for i in range(0, len(parts), third)][:3]
    while len(chunks) < 3:
        chunks.append(chunks[-1] if chunks else ["—"])
    return "\n".join(" ".join(c)[:220] for c in chunks[:3])


def _normalize_three_lines(text: str) -> str:
    raw_lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if not raw_lines:
        return ""
    picked: list[str] = []
    for ln in raw_lines:
        if len(picked) >= 3:
            picked[2] = (picked[2] + " " + ln).strip()[:400]
        else:
            picked.append(ln[:400])
    while len(picked) < 3:
        picked.append("—")
    return "\n".join(picked[:3])


async def _ollama_three_line_summary(plain_block: str) -> str | None:
    base = resolve_conversation_summary_ollama_url()
    if not base:
        return None
    model = resolve_conversation_summary_ollama_model()
    sys_prompt = (
        "You compress group chat into exactly 3 lines in Hebrew titled implicitly "
        '"מצב השיחה" (state of the conversation). '
        "Rules: no timestamps, no user IDs, no @handles, no real names unless unavoidable; "
        "focus topics, mood, and open threads. "
        "Output only those 3 lines, no numbering, no markdown."
    )
    url = f"{base}/api/chat"
    payload: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": plain_block[:_MAX_PLAIN_BLOCK_CHARS]},
        ],
        "stream": False,
        "options": {"temperature": 0.2, "top_p": 0.9, "num_predict": 220},
    }
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=8.0)) as client:
            r = await client.post(url, json=payload)
            r.raise_for_status()
            data = r.json()
    except Exception as exc:
        log.warning("conversation_summary_ollama_failed", error=str(exc))
        return None
    if not isinstance(data, dict):
        return None
    msg = data.get("message")
    if not isinstance(msg, dict):
        return None
    raw = str(msg.get("content") or "").strip()
    norm = _normalize_three_lines(raw)
    return norm or None


async def summarize_transcript_for_paid_api(transcript: str) -> str:
    """
    Last N plain lines → local model → 3-line state; Ollama missing/failure → compact fallback.
    """
    lines = plain_message_lines_from_transcript(transcript)
    block = "\n".join(lines)
    if not block.strip():
        return "אין הודעות אחרונות.\nשיחה ריקה או ללא טקסט.\n—"

    ollama_out = await _ollama_three_line_summary(block)
    if ollama_out:
        return ollama_out
    return _fallback_three_line_state(lines)


def speaker_persona_for_paid_api(speaker: dict[str, Any]) -> dict[str, Any]:
    """Drop session_path, username, and other routing metadata from Gemini payload."""
    return {
        "archetype": str(speaker.get("archetype", "") or ""),
        "voice": str(speaker.get("voice", "") or ""),
        "slang_notes": str(speaker.get("slang_notes", "") or ""),
    }


def reply_target_ids_newest_first(message_index_map: list[dict[str, Any]], *, cap: int) -> list[int]:
    out: list[int] = []
    for m in message_index_map[:cap]:
        if not isinstance(m, dict):
            continue
        try:
            out.append(int(m["id"]))
        except (KeyError, TypeError, ValueError):
            continue
    return out
