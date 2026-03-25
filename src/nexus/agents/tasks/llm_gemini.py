"""
nexus.llm.gemini_terminal — run Gemini (or fallback) on a worker node.

Used when the Master offloads heavy LLM work to the ARQ queue so laptops
pick up CPU-bound generateContent calls. Secrets arrive via Vault injection
(``GEMINI_API_KEY`` in ``__secrets__``).
"""

from __future__ import annotations

import os
from typing import Any

import httpx
import structlog

from nexus.agents.task_registry import registry

log = structlog.get_logger(__name__)

GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")
GEMINI_TIMEOUT_S = float(os.environ.get("GEMINI_HTTP_TIMEOUT", "90"))


def _api_key(parameters: dict[str, Any]) -> str:
    sec = parameters.get("__secrets__") or {}
    return (
        str(sec.get("GEMINI_API_KEY", "")).strip()
        or str(os.environ.get("GEMINI_API_KEY", "")).strip()
    )


def _fallback_reply(user: str) -> tuple[str, list[str]]:
    low = user.lower()
    steps = [
        "ניתוח כוונה מהודעת המפעיל",
        "בדיקת מצב מערכת (ללא מפתח Gemini)",
        "הכנת תשובה היוריסטית",
    ]
    if "deployer" in low or "פריס" in user:
        return (
            "מומלץ לוודא ש־WORKER_IP מצביע על היעד הנכון. עבור מכונה מקומית השתמש ב־127.0.0.1.",
            steps + ["התמקדות בתיקון deployer / נתיב worker"],
        )
    if "report" in low or "דוח" in user:
        return (
            "דוח שבועי: פעילות הקלאסטר, מצב Redis, משימות worker — פתח את לוח הבקרה לנתונים חיים.",
            steps + ["הפניה לווידג'טים קיימים בדשבורד"],
        )
    return (
        "הפקודה התקבלה. הגדר GEMINI_API_KEY כדי לקבל ניתוח מלא מ־Gemini.",
        steps,
    )


def _build_prompt(
    message: str,
    *,
    analysis_mode: str,
    context_messages: list[Any],
) -> str:
    msg = (message or "").strip()
    if analysis_mode == "turbo_shard":
        lines: list[str] = []
        for i, m in enumerate(context_messages[:80]):
            if isinstance(m, dict):
                t = str(m.get("text", m))
            else:
                t = str(m)
            lines.append(f"{i + 1}. {t}")
        blob = "\n".join(lines) if lines else "(אין קטע טקסט)"
        return (
            "You are a behavioral analyst working in parallel with other analysts on the same target. "
            "Focus ONLY on the angle described in the operator instruction below. "
            "Write a concise Hebrew subsection (bullet points, under 280 words). "
            "Do not repeat a full generic profile — stay specific to your lens.\n\n"
            f"Operator instruction:\n{msg or '—'}\n\n"
            f"Assigned text chunk (may be partial):\n{blob}"
        )
    if analysis_mode == "personality":
        lines: list[str] = []
        for i, m in enumerate(context_messages[:40]):
            if isinstance(m, dict):
                t = str(m.get("text", m))
            else:
                t = str(m)
            lines.append(f"{i + 1}. {t}")
        blob = "\n".join(lines) if lines else "(אין דוגמאות — הסתמך על ההקשר הכללי)"
        return (
            "You are a behavioral analyst. Read the chat samples below and write a concise "
            "Hebrew personality profile (ניתוח אופי): communication style, risk appetite, "
            "trust signals, negotiation tendencies, and red flags. "
            "Use bullet points, stay under 350 words, bilingual terms OK.\n\n"
            f"Chat samples:\n{blob}\n\n"
            f"Operator note: {msg or '—'}"
        )
    return (
        "You are Nexus OS — a concise bilingual (Hebrew + English) operations copilot. "
        "Answer the operator's command in plain language. Be actionable and under 400 words.\n\n"
        f"Operator message:\n{msg}"
    )


async def _gemini_http(prompt: str, api_key: str) -> str | None:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    payload: dict[str, Any] = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.35,
            "maxOutputTokens": 1024,
        },
    }
    try:
        async with httpx.AsyncClient(timeout=GEMINI_TIMEOUT_S) as client:
            r = await client.post(url, params={"key": api_key}, json=payload)
            r.raise_for_status()
            data = r.json()
        cands = data.get("candidates") or []
        if not cands:
            return None
        parts = (cands[0].get("content") or {}).get("parts") or []
        texts = [str(p.get("text", "")).strip() for p in parts if isinstance(p, dict)]
        out = "\n".join(t for t in texts if t).strip()
        return out or None
    except Exception as exc:
        log.warning("llm_gemini_http_error", error=str(exc))
        return None


@registry.register("nexus.llm.gemini_terminal")
async def gemini_terminal(parameters: dict[str, Any]) -> dict[str, Any]:
    message = str(parameters.get("message", "")).strip()
    analysis_mode = str(parameters.get("analysis_mode", "chat") or "chat").strip().lower()
    ctx = parameters.get("context_messages")
    context_messages: list[Any] = list(ctx) if isinstance(ctx, list) else []

    thinking: list[str] = [
        "קבלת פקודה מהמפעיל (worker)",
        "מיפוי הקשר (מסחר / פריסה / דוחות)",
    ]

    prompt = _build_prompt(message, analysis_mode=analysis_mode, context_messages=context_messages)
    key = _api_key(parameters)

    if key:
        thinking.append("קריאת Gemini דרך HTTP מה-worker")
        text = await _gemini_http(prompt, key)
        if text:
            thinking.extend(
                [
                    "קבלת טקסט מהמודל",
                    "עיצוב תשובה סופית למפעיל",
                ]
            )
            return {"reply": text, "thinking_steps": thinking, "source": "gemini"}

    thinking.append("Gemini לא זמין — מצב fallback")
    reply, extra = _fallback_reply(message or prompt[:200])
    thinking.extend(extra)
    return {"reply": reply, "thinking_steps": thinking, "source": "fallback"}
