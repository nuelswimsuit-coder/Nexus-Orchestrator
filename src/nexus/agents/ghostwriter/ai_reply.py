"""
AI Reply Generator — builds context-aware Hebrew responses via OpenAI or Anthropic.
"""

from __future__ import annotations

import os
from typing import Literal, Optional

PERSONALITY_PROMPTS: dict[str, str] = {
    "Expert": (
        "אתה מומחה בינה מלאכותית ישראלי עם ניסיון של שנים. "
        "אתה מדבר בביטחון, מציין עובדות טכניות, ומשתף תובנות מעשיות. "
        "הסגנון שלך: ישיר, מקצועי, לפעמים ציני בצורה חיובית."
    ),
    "Skeptic": (
        "אתה ספקן מנוסה בקהילת ה-AI הישראלית. "
        "אתה שואל שאלות קשות, מבקש הוכחות, ומזהיר מהייפ. "
        "הסגנון שלך: ביקורתי אך הוגן, לא שולל — רק בודק."
    ),
    "Hype-Man": (
        "אתה נלהב מטכנולוגיית AI ומשתף את ההתרגשות שלך עם כולם. "
        "אתה משתמש באמוג'י, מגביר את האנרגיה, ומעודד אחרים לנסות. "
        "הסגנון שלך: אנרגטי, חיובי, מדבק."
    ),
    "Beginner": (
        "אתה חדש בעולם ה-AI ומתלהב ללמוד. "
        "אתה שואל שאלות תמימות, מביע פליאה, ומשתף את חוויות הלמידה שלך. "
        "הסגנון שלך: סקרן, ענוותן, אותנטי."
    ),
    "IsraeliTelegramUser": (
        "אתה משתמש טלגרם ישראלי אמיתי בקבוצה — לא מדריך ולא בוט. "
        "דבר בגובה העיניים, עברית מדוברת ולא ספרותית. "
        "אפשר סלנג מדי פעם: אחי, מטורף, מה הולך, בקטנה, וואלה, סבבה — רק כשזה טבעי. "
        "קצר, חמים, אנושי; בלי סימני קריאה מוגזמים ובלי להסביר מי אתה."
    ),
}

SYSTEM_BASE = (
    "אתה חבר אמיתי בקבוצת טלגרם ישראלית על בינה מלאכותית. "
    "כתוב תמיד בעברית טבעית, לא מתורגמת. "
    "התשובה חייבת להיות קצרה (1-3 משפטים), ספונטנית, ולא להיראות כמו בוט. "
    "אל תתחיל עם 'שלום' או 'היי' אלא אם זה מתאים להקשר. "
    "אל תשתמש בביטויים גנריים כמו 'שאלה מצוינת'. "
    "הכתיבה היא RTL — עברית לוגית תקינה."
)


def _build_system_prompt(personality: str) -> str:
    persona = PERSONALITY_PROMPTS.get(personality, PERSONALITY_PROMPTS["Expert"])
    return f"{SYSTEM_BASE}\n\n{persona}"


def _build_user_prompt(trigger_word: str, context_messages: list[str]) -> str:
    context_block = "\n".join(f"- {m}" for m in context_messages[-10:])
    return (
        f"ההקשר האחרון בקבוצה:\n{context_block}\n\n"
        f"המילה/הנושא שהפעיל את התגובה: \"{trigger_word}\"\n\n"
        "כתוב תגובה קצרה וטבעית שמשתלבת בשיחה."
    )


async def generate_reply_openai(
    trigger_word: str,
    context_messages: list[str],
    personality: str,
    api_key: str,
    model: str,
    max_tokens: int,
    temperature: float,
) -> str:
    from openai import AsyncOpenAI

    key = api_key or os.getenv("OPENAI_API_KEY", "")
    client = AsyncOpenAI(api_key=key)

    response = await client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": _build_system_prompt(personality)},
            {"role": "user", "content": _build_user_prompt(trigger_word, context_messages)},
        ],
        max_tokens=max_tokens,
        temperature=temperature,
    )
    return response.choices[0].message.content.strip()


async def generate_reply_anthropic(
    trigger_word: str,
    context_messages: list[str],
    personality: str,
    api_key: str,
    model: str,
    max_tokens: int,
    temperature: float,
) -> str:
    import anthropic

    key = api_key or os.getenv("ANTHROPIC_API_KEY", "")
    client = anthropic.AsyncAnthropic(api_key=key)

    message = await client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=_build_system_prompt(personality),
        messages=[
            {"role": "user", "content": _build_user_prompt(trigger_word, context_messages)},
        ],
        temperature=temperature,
    )
    return message.content[0].text.strip()


async def generate_reply_gemini(
    trigger_word: str,
    context_messages: list[str],
    personality: str,
    api_key: str,
    model: str,
    max_tokens: int,
    temperature: float,
) -> str:
    import google.generativeai as genai

    key = api_key or os.getenv("GEMINI_API_KEY", "")
    genai.configure(api_key=key)

    system_prompt = _build_system_prompt(personality)
    user_prompt = _build_user_prompt(trigger_word, context_messages)
    full_prompt = f"{system_prompt}\n\n{user_prompt}"

    gemini_model = genai.GenerativeModel(
        model_name=model,
        generation_config=genai.GenerationConfig(
            max_output_tokens=max_tokens,
            temperature=temperature,
        ),
    )
    response = await gemini_model.generate_content_async(full_prompt)
    return response.text.strip()


async def generate_reply(
    trigger_word: str,
    context_messages: list[str],
    personality: str,
    provider: Literal["gemini", "openai", "anthropic"] = "gemini",
    gemini_api_key: str = "",
    openai_api_key: str = "",
    anthropic_api_key: str = "",
    model_gemini: str = "gemini-1.5-flash",
    model_openai: str = "gpt-4o-mini",
    model_anthropic: str = "claude-3-haiku-20240307",
    max_tokens: int = 200,
    temperature: float = 0.85,
) -> str:
    if provider == "gemini":
        return await generate_reply_gemini(
            trigger_word, context_messages, personality,
            gemini_api_key, model_gemini, max_tokens, temperature,
        )
    if provider == "anthropic":
        return await generate_reply_anthropic(
            trigger_word, context_messages, personality,
            anthropic_api_key, model_anthropic, max_tokens, temperature,
        )
    return await generate_reply_openai(
        trigger_word, context_messages, personality,
        openai_api_key, model_openai, max_tokens, temperature,
    )
