"""
Standalone maintenance: set Hebrew Israeli display names on vault Telethon sessions
whose current profile contains Latin letters (A–Z).

Unlike swarm's ``_display_name_is_non_israeli`` (Hebrew presence / short-Latin rules),
this script localizes whenever ``[A-Za-z]`` appears in first+last name.

OPSEC: at most 20 successful ``UpdateProfileRequest`` calls per rolling hour, and
``asyncio.sleep(random.randint(60, 300))`` after each successful update.

Run from repo root (PYTHONPATH=. or editable install)::

    python -m nexus.worker.tasks.localize_profiles --dry-run
    python -m nexus.worker.tasks.localize_profiles --limit 3
"""

from __future__ import annotations

import argparse
import asyncio
import random
import re
import time
from collections import deque
from pathlib import Path
from typing import Any

import structlog

from nexus.services.session_vault import discover_meta_paths_from_session_sqlite
from nexus.worker.services.tg_session import (
    async_telegram_client,
    flood_wait_seconds,
)

log = structlog.get_logger(__name__)

_TELEGRAM_NAME_MAX = 64
_MAX_UPDATES_PER_ROLLING_HOUR = 20
_ROLLING_WINDOW_S = 3600
_POST_UPDATE_SLEEP_MIN_S = 60
_POST_UPDATE_SLEEP_MAX_S = 300
_FLOOD_WAIT_RETRIES = 1

# --- Large Hebrew-only pools (authentic Israeli given names + surnames) ---

ISRAELI_FIRST_NAMES_MALE: list[str] = [
    "יוסי",
    "דני",
    "אורי",
    "נועם",
    "איתי",
    "רועי",
    "עומר",
    "גיא",
    "תומר",
    "אלון",
    "מיכאל",
    "אדם",
    "עידו",
    "ליאור",
    "שי",
    "רן",
    "עמית",
    "אביב",
    "הדר",
    "גל",
    "אבי",
    "רונן",
    "בן",
    "אמיר",
    "נדב",
    "אייל",
    "שלומי",
    "יונתן",
    "דור",
    "מאור",
    "אליהו",
    "משה",
    "דוד",
    "יעקב",
    "אהרן",
    "שמואל",
    "איתמר",
    "גלעד",
    "זיו",
    "קובי",
    "רמי",
    "ירון",
    "עופר",
    "אילן",
    "ניר",
    "דקל",
    "אורן",
    "עידן",
    "לירן",
    "אופק",
    "ניתאי",
    "אסף",
    "אמנון",
    "יגאל",
    "ראובן",
    "שמעון",
    "יהודה",
    "אפרים",
    "מנחם",
    "יואב",
    "ברק",
    "דורון",
    "אלכס",
    "גיל",
    "חיים",
    "יוחאי",
    "יוסף",
    "מאיר",
    "מוטי",
    "סרגיי",
    "פיני",
    "צחי",
    "קורן",
    "רפאל",
    "שחר",
    "תבור",
    "ארז",
    "בועז",
    "גדעון",
    "דניאל",
    "הראל",
    "זהר",
    "חגי",
    "טל",
    "יובל",
    "כרמל",
    "ליאם",
    "מורן",
    "נבו",
    "סהר",
    "עומרי",
    "פלג",
    "צבי",
    "קייס",
    "רום",
    "שגיא",
    "תאו",
]

ISRAELI_FIRST_NAMES_FEMALE: list[str] = [
    "מיכל",
    "נועה",
    "שירה",
    "מאיה",
    "תמר",
    "יעל",
    "רות",
    "דנה",
    "ליאת",
    "ענת",
    "הילה",
    "קרן",
    "שקד",
    "מור",
    "אור",
    "ספיר",
    "לילך",
    "רוני",
    "מיטל",
    "עדי",
    "סיגל",
    "אורית",
    "טלי",
    "עדיה",
    "נוגה",
    "גילי",
    "דפנה",
    "מרב",
    "עופרה",
    "רונית",
    "יפעת",
    "אילנה",
    "נטע",
    "שרה",
    "רחל",
    "מרים",
    "לאה",
    "אסתר",
    "חנה",
    "רויטל",
    "שרון",
    "סיון",
    "ניצה",
    "גלית",
    "אפרת",
    "נעמה",
    "הדר",
    "שי-לי",
    "מאי",
    "נוי",
    "פז",
    "ציפי",
    "קארין",
    "רינת",
    "תהילה",
    "אגם",
    "בת-שבע",
    "גפן",
    "דורית",
    "הילי",
    "ויקי",
    "זוהר",
    "חגית",
    "טובה",
    "כרמית",
    "ליאורה",
    "מוריה",
    "נורית",
    "סימה",
    "עלמה",
    "פנינה",
    "צליל",
    "קורל",
    "רעות",
    "תכלת",
    "אבישג",
    "בר",
    "דניאלה",
    "ורד",
    "חן",
    "כנרת",
    "ענבר",
    "ציפורה",
    "קלרה",
    "שיר",
]

ISRAELI_LAST_NAMES: list[str] = [
    "כהן",
    "לוי",
    "מזרחי",
    "אברהם",
    "דהן",
    "ביטון",
    "פרידמן",
    "שפירא",
    "גולן",
    "ברק",
    "אדרי",
    "גרין",
    "רוזן",
    "קליין",
    "אשכנזי",
    "סגל",
    "טל",
    "נחום",
    "אורבך",
    "חיים",
    "דוד",
    "משה",
    "יוסף",
    "זיו",
    "שמש",
    "אילן",
    "נבו",
    "עזריה",
    "מלכה",
    "פרץ",
    "אזולאי",
    "אוחיון",
    "אלבז",
    "אלון",
    "אלחדד",
    "אמסלם",
    "ארביב",
    "בן-דוד",
    "בן-עמי",
    "ברוך",
    "גבאי",
    "גולדשטיין",
    "גורן",
    "דנינו",
    "הרשקוביץ",
    "ויזל",
    "וינברג",
    "זלצר",
    "חזן",
    "טובי",
    "טורגמן",
    "יאיר",
    "כהנא",
    "לביא",
    "לוין",
    "מאיר",
    "מורד",
    "מילר",
    "מלכה",
    "נחמני",
    "סויסה",
    "סלומון",
    "עוז",
    "עידן",
    "פוגל",
    "צדקיהו",
    "צמח",
    "קדוש",
    "קורן",
    "קפלן",
    "רבינוביץ",
    "שטרן",
    "שטרית",
    "שמעוני",
    "תורגמן",
    "אביטל",
    "אבנר",
    "אהרוני",
    "אוחנה",
    "אלמוג",
    "בן-ארי",
    "בן-חיים",
    "בן-שושן",
    "גוטמן",
    "גרוס",
    "דוידי",
    "הלפרין",
    "חדד",
    "טויטו",
    "יוגב",
    "כץ",
    "לנדאו",
    "מנדלסון",
    "נתן",
    "סבן",
    "סלע",
    "עמר",
    "פלד",
    "צוקר",
    "קימחי",
    "רוזנברג",
    "שאול",
    "שביט",
    "שושני",
    "תמם",
    "אדר",
    "אריאל",
    "בוסקילה",
    "גמליאל",
    "דניאל",
    "חכמון",
    "טביב",
    "ישראלי",
    "כהן-צדק",
    "מזוז",
    "משעלי",
    "עוזרי",
    "פנחס",
    "צור",
    "קוריאט",
    "רוטשטיין",
    "שאולוב",
    "שטרק",
    "שמחה",
    "גינזבורג",
    "וינוקור",
    "זקס",
    "חביב",
    "טולידנו",
    "יושע",
    "כהן-אלון",
    "לבנון",
    "מימון",
    "נחשון",
    "סורוקה",
    "עוזר",
    "פנקס",
    "צרפתי",
    "קופר",
    "רוזנטל",
    "שבתאי",
    "שפיגל",
    "תבורי",
]

_LATIN_IN_NAME = re.compile(r"[A-Za-z]")


def _truncate_field(s: str) -> str:
    t = (s or "").strip()
    if len(t) <= _TELEGRAM_NAME_MAX:
        return t
    return t[:_TELEGRAM_NAME_MAX]


def display_name_contains_latin(first: str | None, last: str | None) -> bool:
    combined = f"{first or ''} {last or ''}".strip()
    if not combined:
        return False
    return _LATIN_IN_NAME.search(combined) is not None


def roll_hebrew_display_name(rng: random.Random) -> tuple[str, str]:
    pool = ISRAELI_FIRST_NAMES_MALE if rng.random() < 0.5 else ISRAELI_FIRST_NAMES_FEMALE
    first = _truncate_field(rng.choice(pool))
    last = _truncate_field(rng.choice(ISRAELI_LAST_NAMES))
    return first, last


async def _wait_rate_slot(update_times: deque[float]) -> None:
    while True:
        now = time.time()
        while update_times and update_times[0] < now - _ROLLING_WINDOW_S:
            update_times.popleft()
        if len(update_times) < _MAX_UPDATES_PER_ROLLING_HOUR:
            return
        oldest = update_times[0]
        wait_s = oldest + _ROLLING_WINDOW_S - now + random.uniform(1.0, 8.0)
        log.info(
            "localize_profiles_rate_limit_wait",
            wait_s=round(wait_s, 1),
            in_window=len(update_times),
        )
        await asyncio.sleep(max(wait_s, 1.0))


async def _apply_profile_update(
    client: Any,
    first: str,
    last: str,
    *,
    update_times: deque[float],
) -> bool:
    from telethon.errors import FloodWaitError  # type: ignore[import-untyped]
    from telethon.tl.functions.account import UpdateProfileRequest  # type: ignore[import-untyped]

    await _wait_rate_slot(update_times)

    attempt = 0
    while True:
        try:
            await client(UpdateProfileRequest(first_name=first, last_name=last))
            update_times.append(time.time())
            return True
        except FloodWaitError as exc:
            wait = flood_wait_seconds(exc)
            log.warning("localize_profiles_flood_wait", seconds=wait, attempt=attempt)
            await asyncio.sleep(wait)
            attempt += 1
            if attempt > _FLOOD_WAIT_RETRIES:
                log.error("localize_profiles_flood_wait_give_up", attempts=attempt)
                return False


async def _process_session(
    meta_json: Path,
    parameters: dict[str, Any],
    *,
    rng: random.Random,
    dry_run: bool,
    update_times: deque[float],
) -> None:
    session_base = str(meta_json.with_suffix(""))
    stem = Path(session_base).name

    try:
        async with async_telegram_client(session_base, parameters) as client:
            if not await client.is_user_authorized():
                log.warning("localize_profiles_skip_unauthorized", stem=stem)
                return

            me = await client.get_me()
            uid = getattr(me, "id", None)
            fn_old = str(getattr(me, "first_name", None) or "")
            ln_old = str(getattr(me, "last_name", None) or "")

            if not display_name_contains_latin(fn_old, ln_old):
                log.info(
                    "localize_profiles_skip_no_latin",
                    stem=stem,
                    user_id=uid,
                )
                return

            fn_new, ln_new = roll_hebrew_display_name(rng)

            if dry_run:
                log.info(
                    "localize_profiles_dry_run",
                    stem=stem,
                    user_id=uid,
                    old_first=fn_old,
                    old_last=ln_old,
                    new_first=fn_new,
                    new_last=ln_new,
                )
                return

            ok = await _apply_profile_update(
                client,
                fn_new,
                ln_new,
                update_times=update_times,
            )
            if ok:
                log.info(
                    "localize_profiles_updated",
                    stem=stem,
                    user_id=uid,
                    new_first=fn_new,
                    new_last=ln_new,
                )
                delay = random.randint(_POST_UPDATE_SLEEP_MIN_S, _POST_UPDATE_SLEEP_MAX_S)
                log.info("localize_profiles_post_update_sleep", seconds=delay)
                await asyncio.sleep(delay)
            else:
                log.error("localize_profiles_update_failed", stem=stem, user_id=uid)

    except ValueError as exc:
        log.warning("localize_profiles_skip_no_creds", stem=stem, error=str(exc))
    except Exception as exc:
        log.exception("localize_profiles_session_error", stem=stem, error=str(exc))


async def main_async(*, dry_run: bool, limit: int | None) -> int:
    meta_paths = discover_meta_paths_from_session_sqlite()
    if limit is not None:
        meta_paths = meta_paths[: max(0, limit)]

    log.info(
        "localize_profiles_start",
        sessions=len(meta_paths),
        dry_run=dry_run,
    )

    parameters: dict[str, Any] = {}
    rng = random.Random()
    update_times: deque[float] = deque()

    for meta in meta_paths:
        await _process_session(meta, parameters, rng=rng, dry_run=dry_run, update_times=update_times)

    log.info("localize_profiles_done")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Localize Telegram profile names to Hebrew (vault sessions).")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log proposed changes without calling UpdateProfileRequest.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process at most N sessions (discovery order).",
    )
    args = parser.parse_args()
    try:
        code = asyncio.run(main_async(dry_run=args.dry_run, limit=args.limit))
    except KeyboardInterrupt:
        log.warning("localize_profiles_interrupted")
        raise SystemExit(130) from None
    raise SystemExit(code)


if __name__ == "__main__":
    main()
