"""
Dynamic LLM router and daily API budget guard (Redis).

Routes work to Gemini 1.5 Pro (critical), Mac Mini Ollama Llama/Aya (reactions),
or local regex / tiny Ollama Phi-3 (cleanup). When the Redis daily spend hard cap
is reached, cloud (Gemini) is skipped and everything uses Ollama on the Mac Mini.
"""

from __future__ import annotations

import asyncio
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import IntEnum
from typing import Any, Literal

import httpx
import structlog
import ujson

from nexus.modules.community_vibe import parse_json_object

log = structlog.get_logger(__name__)

GEMINI_PRO_MODEL = "gemini-1.5-pro"
GEMINI_PRO_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_PRO_MODEL}:generateContent"
)

REDIS_KEY_DAILY_USD = "nexus:brain:llm:daily_usd"

_JSON_HEADERS = {"Content-Type": "application/json; charset=utf-8"}

_httpx_client: httpx.AsyncClient | None = None
_httpx_lock = asyncio.Lock()


class LLMRoutingLevel(IntEnum):
    CRITICAL = 1
    REACTION = 2
    CLEANUP = 3


@dataclass(frozen=True)
class RoutingSignals:
    """Boolean hints from the caller; used to pick the cheapest adequate tier."""

    major_news: bool = False
    complex_argument: bool = False
    first_message_in_thread: bool = False
    replying_to_bot: bool = False
    small_talk: bool = False
    cynical_remark: bool = False
    cleanup_task: bool = False


@dataclass
class RoutedCompletion:
    """Result of :func:`complete_with_router`."""

    parsed: dict[str, Any] | None
    text: str | None
    level: LLMRoutingLevel
    backend: Literal["gemini-1.5-pro", "ollama-main", "ollama-phi", "regex_cleanup", "none"]
    budget_capped_cloud: bool


def classify_route(signals: RoutingSignals) -> LLMRoutingLevel:
    """
    Level 1 — critical: major news, complex arguments, first reply in a thread.
    Level 2 — reaction: bot replies, small talk, cynical asides.
    Level 3 — cleanup: formatting, typos, emojis (prefer regex / smallest local model).
    """
    if signals.cleanup_task:
        return LLMRoutingLevel.CLEANUP
    if signals.major_news or signals.complex_argument or signals.first_message_in_thread:
        return LLMRoutingLevel.CRITICAL
    if signals.replying_to_bot or signals.small_talk or signals.cynical_remark:
        return LLMRoutingLevel.REACTION
    return LLMRoutingLevel.REACTION


def _effective_level(level: LLMRoutingLevel, *, budget_allows_cloud: bool) -> LLMRoutingLevel:
    if budget_allows_cloud:
        return level
    if level == LLMRoutingLevel.CRITICAL:
        return LLMRoutingLevel.REACTION
    return level


def _utc_today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _daily_spend_key() -> str:
    return f"{REDIS_KEY_DAILY_USD}:{_utc_today_iso()}"


def _end_of_utc_day_epoch() -> int:
    now = datetime.now(timezone.utc)
    nxt = (now.date() + timedelta(days=1))
    midnight = datetime(nxt.year, nxt.month, nxt.day, tzinfo=timezone.utc)
    return int(midnight.timestamp())


def _budget_cap_usd() -> float:
    raw = (os.getenv("NEXUS_LLM_DAILY_BUDGET_CAP_USD") or "").strip()
    if not raw:
        return 10.0
    try:
        return float(raw)
    except ValueError:
        return 10.0


def _gemini_pro_estimate_usd() -> float:
    raw = (os.getenv("NEXUS_GEMINI_PRO_ESTIMATED_COST_USD") or "").strip()
    if not raw:
        return 0.01
    try:
        return float(raw)
    except ValueError:
        return 0.01


async def get_daily_spend_usd(redis: Any) -> float:
    key = _daily_spend_key()
    raw = await redis.get(key)
    try:
        return float(raw or 0.0)
    except (TypeError, ValueError):
        return 0.0


async def cloud_budget_allows(
    redis: Any | None,
    *,
    estimated_next_call_usd: float | None = None,
) -> bool:
    """
    False when the next estimated Gemini Pro call would push today's total over the hard cap.
    If Redis is missing, the cap cannot be enforced (returns True).
    A cap <= 0 disables budgeting (always True).
    """
    cap = _budget_cap_usd()
    if cap <= 0:
        return True
    if redis is None:
        log.debug("llm_budget_no_redis_cap_not_enforced")
        return True
    est = float(estimated_next_call_usd if estimated_next_call_usd is not None else _gemini_pro_estimate_usd())
    spent = await get_daily_spend_usd(redis)
    ok = (spent + est) <= cap
    if not ok:
        log.info(
            "llm_daily_budget_hard_cap",
            spent_usd=spent,
            cap_usd=cap,
            estimated_next_usd=est,
        )
    return ok


async def record_gemini_spend(redis: Any | None, usd: float | None = None) -> None:
    """Increment today's running total after a billable Gemini Pro call."""
    if redis is None:
        return
    amt = float(usd if usd is not None else _gemini_pro_estimate_usd())
    key = _daily_spend_key()
    try:
        await redis.incrbyfloat(key, amt)
        await redis.expireat(key, _end_of_utc_day_epoch() + 7200)
    except Exception as exc:
        log.warning("llm_redis_spend_record_failed", error=str(exc))


def _resolve_ollama_base_url() -> str:
    return (os.getenv("NEXUS_OLLAMA_BASE_URL") or os.getenv("OLLAMA_HOST") or "").strip().rstrip("/")


def _resolve_ollama_main_model() -> str:
    return (os.getenv("NEXUS_OLLAMA_MODEL") or "llama3").strip() or "llama3"


def _resolve_ollama_phi_model() -> str:
    return (os.getenv("NEXUS_OLLAMA_PHI_MODEL") or "phi3").strip() or "phi3"


_RE_MULTI_SPACE = re.compile(r"[ \t\r\f\v]+")
_RE_MULTIPUNCT = re.compile(r"([!?.,])\1{2,}")


def cleanup_text_local(text: str) -> str:
    """
    Level-3 local path: whitespace, repeated punctuation, trim.
    Emoji are preserved; no cloud call.
    """
    s = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    s = _RE_MULTI_SPACE.sub(" ", s)
    s = _RE_MULTIPUNCT.sub(r"\1\1", s)
    return s.strip()


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


def _dumps_bytes(obj: Any) -> bytes:
    return ujson.dumps(obj, ensure_ascii=False).encode("utf-8")


def _loads_dict(data: str | bytes) -> Any:
    try:
        return ujson.loads(data)
    except (ValueError, TypeError):
        import json

        return json.loads(data)


async def _shared_httpx() -> httpx.AsyncClient:
    global _httpx_client
    if _httpx_client is None:
        async with _httpx_lock:
            if _httpx_client is None:
                _httpx_client = httpx.AsyncClient(
                    timeout=httpx.Timeout(90.0, connect=15.0),
                    limits=httpx.Limits(max_keepalive_connections=16, max_connections=32),
                )
    return _httpx_client


async def gemini_pro_json(
    api_key: str,
    system_instruction: str,
    user_text: str,
    *,
    temperature: float = 0.85,
    max_tokens: int = 512,
    frequency_penalty: float | None = None,
    presence_penalty: float | None = None,
    top_p: float | None = None,
) -> dict[str, Any]:
    """Gemini 1.5 Pro JSON object response (same contract as community_vibe._gemini_json)."""
    url = f"{GEMINI_PRO_URL}?key={api_key}"
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
    combined = f"{system_instruction}\n\n---\n\n{user_text}"
    payload_fallback = {
        "contents": [{"role": "user", "parts": [{"text": combined}]}],
        "generationConfig": gen_cfg,
    }
    client = await _shared_httpx()
    resp = await client.post(url, content=_dumps_bytes(payload_primary), headers=_JSON_HEADERS)
    if resp.status_code >= 400:
        resp = await client.post(url, content=_dumps_bytes(payload_fallback), headers=_JSON_HEADERS)
    resp.raise_for_status()
    data = _loads_dict(resp.content)
    cands = data.get("candidates") or []
    if not cands:
        raise ValueError("gemini_pro_empty_candidates")
    parts = cands[0].get("content", {}).get("parts") or []
    if not parts or "text" not in parts[0]:
        raise ValueError("gemini_pro_no_text")
    raw = parts[0]["text"]
    return parse_json_object(raw)


async def _ollama_chat_completion_content(
    base_url: str,
    model: str,
    system_prompt: str,
    user_text: str,
    *,
    temperature: float,
    top_p: float,
    max_tokens: int,
) -> str | None:
    url = f"{base_url.rstrip('/')}/api/chat"
    payload: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_text},
        ],
        "stream": False,
        "options": {
            "temperature": float(temperature),
            "top_p": float(top_p),
            "num_predict": int(max_tokens),
        },
    }
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            r = await client.post(url, json=payload)
            r.raise_for_status()
            data = r.json()
    except Exception as exc:
        log.warning("llm_ollama_chat_failed", model=model, error=str(exc))
        return None
    if not isinstance(data, dict):
        return None
    msg = data.get("message")
    if not isinstance(msg, dict):
        return None
    raw = str(msg.get("content") or "").strip()
    return raw or None


async def complete_with_router(
    redis: Any | None,
    api_key: str,
    *,
    system_prompt: str,
    user_prompt: str,
    signals: RoutingSignals,
    temperature: float = 0.85,
    max_tokens: int = 512,
    top_p: float | None = None,
    frequency_penalty: float | None = None,
    presence_penalty: float | None = None,
    cleanup_source_text: str | None = None,
    json_only: bool = True,
) -> RoutedCompletion:
    """
    Run generation on the cheapest backend that matches ``signals``, respecting Redis cap.

    * Level 3 + ``json_only=False`` + ``cleanup_source_text``: only :func:`cleanup_text_local`.
    * Level 3 + JSON: Phi-3 (or ``NEXUS_OLLAMA_PHI_MODEL``) on Ollama.
    * Level 2: main Ollama model (Llama 3 / Aya via ``NEXUS_OLLAMA_MODEL``).
    * Level 1: Gemini 1.5 Pro if ``api_key`` and budget allow; else main Ollama.
    """
    base = classify_route(signals)
    budget_ok = await cloud_budget_allows(redis)
    effective = _effective_level(base, budget_allows_cloud=budget_ok)
    capped = base == LLMRoutingLevel.CRITICAL and effective != LLMRoutingLevel.CRITICAL

    if effective == LLMRoutingLevel.CLEANUP and not json_only and cleanup_source_text is not None:
        return RoutedCompletion(
            parsed=None,
            text=cleanup_text_local(cleanup_source_text),
            level=effective,
            backend="regex_cleanup",
            budget_capped_cloud=capped,
        )

    ollama_base = _resolve_ollama_base_url()

    async def _try_ollama(model: str, label: Literal["ollama-main", "ollama-phi"]) -> RoutedCompletion | None:
        if not ollama_base:
            return None
        raw = await _ollama_chat_completion_content(
            ollama_base,
            model,
            system_prompt,
            user_prompt,
            temperature=temperature,
            top_p=float(top_p if top_p is not None else 0.9),
            max_tokens=max_tokens,
        )
        if not raw:
            return None
        if json_only:
            obj = _parse_llm_json_object(raw)
            if obj is None:
                return None
            return RoutedCompletion(
                parsed=obj,
                text=None,
                level=effective,
                backend=label,
                budget_capped_cloud=capped,
            )
        return RoutedCompletion(
            parsed=None,
            text=raw.strip(),
            level=effective,
            backend=label,
            budget_capped_cloud=capped,
        )

    if effective == LLMRoutingLevel.CLEANUP:
        phi = _resolve_ollama_phi_model()
        hit = await _try_ollama(phi, "ollama-phi")
        if hit:
            return hit
        if not json_only and cleanup_source_text is not None:
            return RoutedCompletion(
                parsed=None,
                text=cleanup_text_local(cleanup_source_text),
                level=effective,
                backend="regex_cleanup",
                budget_capped_cloud=capped,
            )
        return RoutedCompletion(
            parsed=None,
            text=None,
            level=effective,
            backend="none",
            budget_capped_cloud=capped,
        )

    if effective == LLMRoutingLevel.REACTION:
        main = _resolve_ollama_main_model()
        hit = await _try_ollama(main, "ollama-main")
        if hit:
            return hit
        return RoutedCompletion(
            parsed=None,
            text=None,
            level=effective,
            backend="none",
            budget_capped_cloud=capped,
        )

    # CRITICAL → Gemini 1.5 Pro when key + budget (JSON-shaped completions only).
    if json_only and api_key.strip() and budget_ok:
        try:
            out = await gemini_pro_json(
                api_key.strip(),
                system_prompt,
                user_prompt,
                temperature=temperature,
                max_tokens=max_tokens,
                frequency_penalty=frequency_penalty,
                presence_penalty=presence_penalty,
                top_p=top_p,
            )
            await record_gemini_spend(redis)
            return RoutedCompletion(
                parsed=out,
                text=None,
                level=effective,
                backend="gemini-1.5-pro",
                budget_capped_cloud=False,
            )
        except Exception as exc:
            log.warning("llm_gemini_pro_failed", error=str(exc))

    main = _resolve_ollama_main_model()
    hit = await _try_ollama(main, "ollama-main")
    if hit:
        return RoutedCompletion(
            parsed=hit.parsed,
            text=hit.text,
            level=effective,
            backend="ollama-main",
            budget_capped_cloud=capped or not budget_ok,
        )
    return RoutedCompletion(
        parsed=None,
        text=None,
        level=effective,
        backend="none",
        budget_capped_cloud=capped or not budget_ok,
    )
