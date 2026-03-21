"""Lightweight readiness / configuration analysis (no side effects)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class ReadinessReport:
    """Flags describing which optional subsystems appear configured."""

    telegram_configured: bool
    gemini_configured: bool
    openai_configured: bool

    @property
    def summary(self) -> str:
        parts: list[str] = []
        if self.telegram_configured:
            parts.append("Telegram token present")
        if self.gemini_configured:
            parts.append("Gemini key present")
        if self.openai_configured:
            parts.append("OpenAI key present")
        return ", ".join(parts) if parts else "minimal config (no AI keys detected)"


def readiness_from_flags(flags: Mapping[str, bool]) -> ReadinessReport:
    """Build a report from a simple name → bool map."""
    return ReadinessReport(
        telegram_configured=bool(flags.get("telegram")),
        gemini_configured=bool(flags.get("gemini")),
        openai_configured=bool(flags.get("openai")),
    )
