"""
AI Command Terminal — Gemini-backed chat with explicit thinking steps for the dashboard.

Heavy generation can run on laptop workers via the ARQ queue (``nexus:tasks``) when
at least one worker heartbeat is present; otherwise the API falls back to local
Gemini or heuristic replies so the Master never hard-depends on workers.
"""

from __future__ import annotations

import asyncio
import os

import structlog
from fastapi import APIRouter
from pydantic import BaseModel, Field

from nexus.services.api.dependencies import RedisDep
from nexus.services.api.services.worker_arq_client import (
    build_llm_task_payload,
    count_online_workers,
    enqueue_execute_task_and_wait,
)
from nexus.shared.config import settings

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/ai-terminal", tags=["ai-terminal"])

GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")
GEMINI_TIMEOUT = 45.0
_WORKER_LLM_TIMEOUT = float(os.environ.get("NEXUS_WORKER_LLM_TIMEOUT", "120"))


def _offload_llm_enabled() -> bool:
    return os.getenv("NEXUS_OFFLOAD_LLM_TO_WORKERS", "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _master_personality_fallback_prefix(reply: str) -> str:
    t = (reply or "").strip()
    if not t:
        return t
    if t.startswith("⚠️"):
        return t
    return f"⚠️ {t}"


class TerminalChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=8000)
    analysis_mode: str | None = Field(
        default=None,
        description="Optional: 'personality' for ניתוח אופי-style prompts; default chat.",
    )
    context_messages: list[dict[str, str]] = Field(
        default_factory=list,
        description="Optional chat samples for personality / shadow analysis.",
    )


class TerminalChatResponse(BaseModel):
    reply: str
    thinking_steps: list[str]
    source: str  # "gemini" | "fallback" | "worker_gemini" | "worker_fallback"


class PersonalityRequest(BaseModel):
    """Body for ניתוח אופי — samples plus optional operator note."""

    messages: list[dict[str, str]] = Field(
        default_factory=list,
        description='e.g. [{"text": "..."}, ...]',
    )
    note: str = Field(default="", max_length=4000)


def _fallback_reply(user: str) -> tuple[str, list[str]]:
    low = user.lower()
    steps = [
        "ניתוח כוונה מהודעת המפעיל",
        "בדיקת מצב מערכת (ללא מפתח Gemini)",
        "הכנת תשובה היוריסטית",
    ]
    if "deployer" in low or "פריס" in user:
        return (
            "מומלץ לוודא ש־WORKER_IP מצביע על היעד הנכון. עבור מכונה מקומית השתמש ב־127.0.0.1 — "
            "המערכת מדלגת על SSH ומסנכרנת קבצים מקומית.",
            steps + ["התמקדות בתיקון deployer / נתיב worker"],
        )
    if "report" in low or "דוח" in user:
        return (
            "דוח שבועי (סיכום): פעילות הקלאסטר, מצב Redis, משימות worker וסטטוס scalper — "
            "פתח את לוח הבקרה או Treasury לנתונים חיים.",
            steps + ["הפניה לווידג'טים קיימים בדשבורד"],
        )
    if "exposure" in low or "חשיפה" in user or "%" in user:
        return (
            "הגדלת חשיפה דורשת אישור סיכון. בדוק מצב paper-trading, יעדי scalper ו־manual override לפני שינוי sizing.",
            steps + ["הערכת סיכון לפני שינוי גודל פוזיציה"],
        )
    return (
        "הפקודה התקבלה. הגדר GEMINI_API_KEY כדי לקבל ניתוח מלא מ־Gemini. "
        "בינתיים ניתן להשתמש בלוח הבקרה, Treasury וב־Market Intelligence.",
        steps,
    )


async def _gemini_reply_local(user: str) -> str | None:
    key = (settings.gemini_api_key or os.environ.get("GEMINI_API_KEY", "")).strip()
    if not key:
        return None
    try:
        from google import genai  # type: ignore[import-untyped]
    except ImportError:
        log.warning("ai_terminal_genai_missing")
        return None

    client = genai.Client(api_key=key)
    prompt = (
        "You are Nexus OS — a concise bilingual (Hebrew + English) operations copilot. "
        "Answer the operator's command in plain language. Be actionable and under 400 words.\n\n"
        f"Operator message:\n{user}"
    )
    try:
        response = await asyncio.wait_for(
            client.aio.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=genai.types.GenerateContentConfig(
                    temperature=0.35,
                    max_output_tokens=1024,
                ),
            ),
            timeout=GEMINI_TIMEOUT,
        )
        text = (getattr(response, "text", None) or "").strip()
        return text or None
    except Exception as exc:
        log.warning("ai_terminal_gemini_error", error=str(exc))
        return None


def _terminal_from_worker_payload(
    out: dict,
    *,
    thinking_extra: list[str] | None = None,
) -> TerminalChatResponse | None:
    reply = out.get("reply")
    if not isinstance(reply, str) or not reply.strip():
        return None
    steps = out.get("thinking_steps")
    if not isinstance(steps, list):
        steps = []
    steps = [str(s) for s in steps]
    if thinking_extra:
        steps = thinking_extra + steps
    src = str(out.get("source", "gemini"))
    if src == "gemini":
        mapped = "worker_gemini"
    elif src == "fallback":
        mapped = "worker_fallback"
    else:
        mapped = src
    return TerminalChatResponse(
        reply=reply.strip(),
        thinking_steps=steps,
        source=mapped,
    )


async def _try_worker_llm(
    redis: object,
    *,
    message: str,
    analysis_mode: str,
    context_messages: list[dict[str, str]],
    force_worker_queue: bool = False,
) -> TerminalChatResponse | None:
    # ניתוח אופי always prefers ``nexus:tasks`` when workers exist, regardless of
    # NEXUS_OFFLOAD_LLM_TO_WORKERS (chat-only offload toggle).
    if not force_worker_queue and not _offload_llm_enabled():
        return None
    try:
        n_workers = await count_online_workers(redis)
    except Exception as exc:
        log.warning("ai_terminal_worker_count_failed", error=str(exc))
        return None
    if n_workers <= 0:
        log.info("ai_terminal_llm_master_only", reason="no_worker_heartbeats")
        return None

    task = build_llm_task_payload(
        message=message,
        analysis_mode=analysis_mode,
        context_messages=context_messages,
    )
    raw = await enqueue_execute_task_and_wait(task, timeout_s=_WORKER_LLM_TIMEOUT)
    if not isinstance(raw, dict):
        return None
    if raw.get("error"):
        log.warning(
            "ai_terminal_worker_llm_error",
            error=str(raw.get("error"))[:200],
        )
        return None
    out = raw.get("output")
    if not isinstance(out, dict):
        return None
    extra = [
        f"בוצע על worker ({n_workers} צמדים מחוברים)",
    ]
    parsed = _terminal_from_worker_payload(out, thinking_extra=extra)
    if parsed:
        log.info("ai_terminal_llm_worker_ok", task_id=task.task_id)
    return parsed


@router.post("/chat", response_model=TerminalChatResponse)
async def post_terminal_chat(
    body: TerminalChatRequest,
    redis: RedisDep,
) -> TerminalChatResponse:
    msg = body.message.strip()
    mode = (body.analysis_mode or "chat").strip().lower()
    if mode not in ("chat", "personality", "turbo_shard"):
        mode = "chat"
    ctx = body.context_messages or []

    thinking: list[str] = [
        "קבלת פקודה מהמפעיל",
        "מיפוי הקשר (מסחר / פריסה / דוחות)",
    ]

    worker_first = await _try_worker_llm(
        redis,
        message=msg,
        analysis_mode=mode,
        context_messages=ctx,
        force_worker_queue=(mode == "personality"),
    )
    if worker_first is not None:
        return worker_first

    thinking.append("מצב מאסטר מקומי — אין worker זמין או תור נכשל")
    text = await _gemini_reply_local(msg)
    if text:
        thinking.extend(
            [
                "קריאה ל־Gemini והמתנה לתשובה (מקומי)",
                "עיצוב תשובה סופית למפעיל",
            ]
        )
        out = text
        if mode == "personality":
            out = _master_personality_fallback_prefix(out)
        return TerminalChatResponse(
            reply=out, thinking_steps=thinking, source="gemini"
        )

    thinking.append("Gemini לא זמין — מצב fallback")
    reply, extra = _fallback_reply(msg)
    thinking.extend(extra)
    out = reply
    if mode == "personality":
        out = _master_personality_fallback_prefix(out)
    return TerminalChatResponse(
        reply=out, thinking_steps=thinking, source="fallback"
    )


@router.post("/personality", response_model=TerminalChatResponse)
async def post_personality_analysis(
    body: PersonalityRequest,
    redis: RedisDep,
) -> TerminalChatResponse:
    """
    ניתוח אופי — prefers worker queue; falls back to master-only Gemini/fallback.
    """
    note = body.note.strip()
    samples = body.messages or []
    synthetic_message = note or "נתח את אופי הכותב/ת על בסיס הדוגמאות."

    thinking: list[str] = [
        "בקשת ניתוח אופי (personality)",
        "הכנת הקשר מדגימות צ'אט",
    ]

    worker_first = await _try_worker_llm(
        redis,
        message=synthetic_message,
        analysis_mode="personality",
        context_messages=samples,
        force_worker_queue=True,
    )
    if worker_first is not None:
        return worker_first

    thinking.append("מצב מאסטר מקומי — אין worker זמין או תור נכשל")
    # Local path: fold samples into one prompt for single generate call
    lines = [
        str(m.get("text", m)) if isinstance(m, dict) else str(m)
        for m in samples[:40]
    ]
    blob = "\n".join(f"{i + 1}. {t}" for i, t in enumerate(lines)) or "(אין דוגמאות)"
    combined = (
        "You are a behavioral analyst. Write a concise Hebrew personality profile (ניתוח אופי) "
        "from these samples. Use bullets, under 350 words.\n\n"
        f"Samples:\n{blob}\n\nOperator note: {note or '—'}"
    )

    key = (settings.gemini_api_key or os.environ.get("GEMINI_API_KEY", "")).strip()
    if key:
        try:
            from google import genai  # type: ignore[import-untyped]

            client = genai.Client(api_key=key)
            response = await asyncio.wait_for(
                client.aio.models.generate_content(
                    model=GEMINI_MODEL,
                    contents=combined,
                    config=genai.types.GenerateContentConfig(
                        temperature=0.35,
                        max_output_tokens=1024,
                    ),
                ),
                timeout=GEMINI_TIMEOUT,
            )
            text = (getattr(response, "text", None) or "").strip()
            if text:
                thinking.extend(
                    ["קריאה ל־Gemini מקומי", "סיום ניתוח אופי"]
                )
                return TerminalChatResponse(
                    reply=_master_personality_fallback_prefix(text),
                    thinking_steps=thinking,
                    source="gemini",
                )
        except Exception as exc:
            log.warning("ai_terminal_personality_local_error", error=str(exc))

    thinking.append("fallback היוריסטי")
    reply, extra = _fallback_reply(synthetic_message)
    thinking.extend(extra)
    return TerminalChatResponse(
        reply=_master_personality_fallback_prefix(reply),
        thinking_steps=thinking,
        source="fallback",
    )


class StrategyMutationResponse(BaseModel):
    ok: bool
    message: str
    logged_hint: str


@router.post("/strategy-mutation", response_model=StrategyMutationResponse)
async def post_strategy_mutation() -> StrategyMutationResponse:
    """Queue hint for autonomous strategy pivot (logged; workers may pick up via Redis)."""
    hint = "strategy_mutation_requested_from_market_intel"
    log.info("ai_terminal_strategy_mutation", hint=hint)
    return StrategyMutationResponse(
        ok=True,
        message="בקשת מוטציית אסטרטגיה נרשמה. המערכת תטען פרמטרים מחדש במחזור הבא.",
        logged_hint=hint,
    )
