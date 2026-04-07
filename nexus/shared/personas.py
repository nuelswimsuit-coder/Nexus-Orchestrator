"""
Fixed Israeli swarm persona archetypes: prompt text + circadian sleep windows (Asia/Jerusalem).

Sleep windows are local clock intervals during which a persona must not post (especially on news).
Format: ((start_hour, start_minute), (end_hour, end_minute)) — asleep from start through end,
with wrap across midnight when start > end (e.g. 22:00–06:00).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone as dt_timezone

JERUSALEM_TZ_NAME = "Asia/Jerusalem"

SleepWindow = tuple[tuple[int, int], tuple[int, int]]


@dataclass(frozen=True)
class PersonaArchetype:
    """One deterministic persona axis (prompt + biological sleep schedule)."""

    prompt: str
    sleep_window: SleepWindow


def _hm_to_minutes(h: int, m: int) -> int:
    return int(h) * 60 + int(m)


def minutes_since_midnight_jerusalem(when: datetime | None = None) -> int:
    """Current local time in Jerusalem as minutes from 00:00 (0–1439)."""
    from zoneinfo import ZoneInfo

    tz = ZoneInfo(JERUSALEM_TZ_NAME)
    if when is None:
        dt = datetime.now(tz)
    elif when.tzinfo is not None:
        dt = when.astimezone(tz)
    else:
        dt = when.replace(tzinfo=dt_timezone.utc).astimezone(tz)
    return dt.hour * 60 + dt.minute


def is_time_in_sleep_window(now_minutes: int, sleep_window: SleepWindow) -> bool:
    """
    True if ``now_minutes`` falls in the sleep interval (start inclusive, end exclusive).
    When start wall-clock is after end, the interval wraps midnight (e.g. 22:00–06:00).
    """
    (sh, sm), (eh, em) = sleep_window
    start = _hm_to_minutes(sh, sm)
    end = _hm_to_minutes(eh, em)
    now_m = now_minutes % 1440
    if start < end:
        return start <= now_m < end
    if start > end:
        return now_m >= start or now_m < end
    # start == end → empty window (never asleep)
    return False


# Twelve fixed archetypes — index chosen deterministically from MD5(session path).
# sleep_window: night-owl / student-leaning (03:00–12:00), boomer-like (22:00–06:00),
# normie/stater (00:00–07:00) per product brief.
PERSONA_ARCHETYPES: list[PersonaArchetype] = [
    PersonaArchetype(
        prompt=(
            "ARCHETYPE Ars/פרח: עצבני, סלנג אגרסיבי, 'אחי' 'נודר' 'בדוק', טעויות כתיב מכוונות "
            "('ניראה' במקום 'נראה'). קצר וחד."
        ),
        sleep_window=((3, 0), (12, 0)),
    ),
    PersonaArchetype(
        prompt=(
            "ARCHETYPE Boomer: בן/בת 60+, נקודות '...' ואימוג'ים 🙏🌹, מתלונן על ממשלה/צעירים, "
            "סגנון ווטסאפ משפחתי."
        ),
        sleep_window=((22, 0), (6, 0)),
    ),
    PersonaArchetype(
        prompt=(
            "ARCHETYPE Religious: מילים נקיות יותר אבל עדיין יומיומי, 'בעזה\"ש' לפעמים, לא פורמלי."
        ),
        sleep_window=((0, 0), (7, 0)),
    ),
    PersonaArchetype(
        prompt=("ARCHETYPE Cynic: לא מאמין לחדשות, 2–6 מילים, 'חארטה' 'פייק' 'שוב עובדים עלינו'."),
        sleep_window=((0, 0), (7, 0)),
    ),
    PersonaArchetype(
        prompt=("ARCHETYPE Anxious: נלחץ מחדשות, 'אמאלה' 'איזה פחד' 'מה נסגר'."),
        sleep_window=((0, 0), (7, 0)),
    ),
    PersonaArchetype(
        prompt=(
            "ARCHETYPE Tech-bro: סטארטאפים, 'דיסרפשן' בציניות, מעורבב עברית-אנגלית קז'ואל."
        ),
        sleep_window=((0, 0), (7, 0)),
    ),
    PersonaArchetype(
        prompt=(
            "ARCHETYPE Student: חצי ישן, 'אני במבחן' 'אין כסף', סלנג קצת צעיר."
        ),
        sleep_window=((3, 0), (12, 0)),
    ),
    PersonaArchetype(
        prompt=(
            "ARCHETYPE Mizrahi uncle: חום, 'מאל'ס' 'יאללה', בדיחות משפחה, לא מנומס מדי."
        ),
        sleep_window=((0, 0), (7, 0)),
    ),
    PersonaArchetype(
        prompt=(
            "ARCHETYPE Ashkenazi grandma: 'אוי ואבוי' 'נו באמת', קצת יידיש בעברית, תלונה חמה."
        ),
        sleep_window=((22, 0), (6, 0)),
    ),
    PersonaArchetype(
        prompt=(
            "ARCHETYPE Russian-mix: עברית עם שיבושים רוסיים קלים, 'נורמלי?' 'בסדר' הרבה."
        ),
        sleep_window=((0, 0), (7, 0)),
    ),
    PersonaArchetype(
        prompt=(
            "ARCHETYPE Periphery: עיר פיתוח/פריפריה, ריאליזם כלכלי, 'אין עבודה' 'המחירים'."
        ),
        sleep_window=((0, 0), (7, 0)),
    ),
    PersonaArchetype(
        prompt=(
            "ARCHETYPE Beach-chill: אילתי/חוף בראש, רגוע, 'נשבע' 'וואלה כיף', פחות זעם."
        ),
        sleep_window=((3, 0), (12, 0)),
    ),
]


def deterministic_archetype_index(session_base: str) -> int:
    """Same MD5 mapping as ``nexus.worker.tasks.swarm._deterministic_persona_axes``."""
    raw = (session_base or "default").encode("utf-8", errors="ignore")
    d = hashlib.md5(raw).digest()
    return int.from_bytes(d[0:2], "big") % len(PERSONA_ARCHETYPES)


def session_is_asleep_jerusalem(session_base: str, when: datetime | None = None) -> bool:
    """Whether this Telethon session's persona should be sleeping now (Jerusalem local)."""
    idx = deterministic_archetype_index(session_base)
    window = PERSONA_ARCHETYPES[idx].sleep_window
    now_m = minutes_since_midnight_jerusalem(when)
    return is_time_in_sleep_window(now_m, window)


# Prompt strings only (for LLM), same order as PERSONA_ARCHETYPES.
PERSONA_ARCHETYPE_PROMPTS: list[str] = [a.prompt for a in PERSONA_ARCHETYPES]
