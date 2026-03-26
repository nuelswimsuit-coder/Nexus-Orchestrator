"""
AI Terminal endpoints — chat, personality analysis, strategy mutation.
Uses Gemini (google-generativeai) when available, falls back to a stub.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(tags=["ai"])

GEMINI_MODEL = "gemini-2.0-flash"
GEMINI_TIMEOUT = 20


async def _gemini_ask(prompt: str) -> str:
    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        return "GEMINI_API_KEY not configured."
    try:
        import google.generativeai as genai  # type: ignore[import]

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(GEMINI_MODEL)
        loop = asyncio.get_event_loop()
        response = await asyncio.wait_for(
            loop.run_in_executor(None, lambda: model.generate_content(prompt)),
            timeout=GEMINI_TIMEOUT,
        )
        return response.text.strip()
    except ImportError:
        return "google-generativeai not installed."
    except asyncio.TimeoutError:
        return "Gemini timeout."
    except Exception as exc:  # noqa: BLE001
        return f"Gemini error: {exc}"


# ── /api/ai/chat ──────────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    prompt: str


class AiReply(BaseModel):
    reply: str
    source: str


@router.post("/ai/chat", response_model=AiReply)
async def ai_chat(body: ChatRequest) -> AiReply:
    reply = await _gemini_ask(body.prompt)
    source = "gemini" if os.getenv("GEMINI_API_KEY") else "stub"
    return AiReply(reply=reply, source=source)


# ── /api/ai/personality ───────────────────────────────────────────────────────

class PersonalityMessage(BaseModel):
    text: str


class PersonalityRequest(BaseModel):
    messages: list[PersonalityMessage]
    note: str | None = None


@router.post("/ai/personality", response_model=AiReply)
async def ai_personality(body: PersonalityRequest) -> AiReply:
    samples = "\n".join(f"- {m.text}" for m in body.messages)
    note = body.note or "Analyze the personality and communication style."
    prompt = (
        f"You are a personality analyst. Given these message samples:\n{samples}\n\n"
        f"Task: {note}\n"
        "Provide a concise personality profile (2-3 sentences)."
    )
    reply = await _gemini_ask(prompt)
    source = "gemini" if os.getenv("GEMINI_API_KEY") else "stub"
    return AiReply(reply=reply, source=source)


# ── /api/ai/strategy-mutation ─────────────────────────────────────────────────

class MutationResponse(BaseModel):
    status: str
    message: str


@router.post("/ai/strategy-mutation", response_model=MutationResponse)
async def ai_strategy_mutation() -> MutationResponse:
    prompt = (
        "You are a trading strategy AI. Analyze current market conditions "
        "(BTC vs Polymarket prediction markets) and suggest one concrete "
        "strategy mutation to improve performance. Be brief (2-3 sentences)."
    )
    reply = await _gemini_ask(prompt)
    return MutationResponse(status="ok", message=reply)
