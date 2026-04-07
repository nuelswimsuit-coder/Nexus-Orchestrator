"""
Standalone maintenance: set Telethon user display names to Hebrew (Israeli-style)
when the display name is empty or contains Latin letters (e.g. English names).

Run (from repo root)::

    python -m nexus.worker.tasks.localize_profiles

**Vault mode (default)** — scans Nexus session vault for ``*.session`` + ``*.json``
meta (``api_id`` / ``api_hash``). Override vault root with ``NEXUS_SESSION_VAULT_DIR``.

**Flat directory mode (plan)** — set ``TELEGRAM_API_ID``, ``TELEGRAM_API_HASH``, and
``TELEGRAM_SESSIONS_DIR`` (or pass ``--sessions-dir``). Non-recursive scan for
``*.session`` in that folder; same API credentials for every file.

OPSEC: random 60–300s pause after each successful update and at most 20 updates
per rolling hour.

Environment
-----------
NEXUS_SESSION_VAULT_DIR — vault root for default mode
TELEGRAM_API_ID / TELEGRAM_API_HASH — required for flat directory mode
TELEGRAM_SESSIONS_DIR — directory of ``*.session`` files for flat mode
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import re
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import structlog

from nexus.services.session_vault import (
    discover_meta_paths_from_session_sqlite,
    vault_meta_resolve_api_credentials,
)
from nexus.shared.logging_config import configure_logging
from nexus.shared.tg_connection import (
    telegram_network_slot,
    telethon_connect_kwargs_for_session_base,
)

log = structlog.get_logger(__name__)

# Telegram display-name limits (Telethon / MTProto)
_NAME_MAX_LEN = 64


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        t = (x or "").strip()
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


# ── Authentic Israeli name pools (Hebrew script; male/female given + surnames) ─

_ISRAELI_FIRST_NAMES_MALE_RAW: list[str] = [
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
    "איתן",
    "בני",
    "דוד",
    "משה",
    "יואב",
    "אילן",
    "עוז",
    "גלעד",
    "אופיר",
    "דורון",
    "יובל",
    "קובי",
    "רמי",
    "אהרון",
    "יעקב",
    "שמואל",
    "אריאל",
    "רפאל",
    "אסף",
    "עומרי",
    "ניר",
    "גדעון",
    "אביחי",
    "מתן",
    "אלעד",
    "רז",
    "עמוס",
    "צחי",
    "רום",
    "אורן",
    "פלג",
    "אליהו",
    "מאיר",
    "חיים",
    "שלמה",
    "מנחם",
    "יצחק",
    "מור",
    "אבירם",
    "אלכסנדר",
    "מקס",
    "טל",
    "עומרי",
    "אופק",
    "נבו",
    "אילי",
    "רועי",
    "אמנון",
    "זיו",
    "ליאם",
    "ניתאי",
    "אור",
    "אליאב",
    "איתמר",
    "אבישי",
    "יהונתן",
    "אליה",
    "יואל",
    "אבינועם",
    "רביד",
    "עידן",
    "שחר",
    "אלמוג",
    "אוריאל",
    "אביאל",
    "נתן",
    "שגיא",
    "רון",
    "עדי",
    "לירן",
    "אמית",
    "אביב",
    "אלרואי",
    "אבנר",
    "אלחנן",
    "יוגב",
]

_ISRAELI_FIRST_NAMES_FEMALE_RAW: list[str] = [
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
    "שני",
    "סתיו",
    "הדר",
    "גל",
    "מעיין",
    "רונית",
    "אילנה",
    "מירב",
    "שושנה",
    "רחל",
    "שרה",
    "לאה",
    "אסתר",
    "נעמה",
    "תהילה",
    "אורית",
    "דפנה",
    "מורן",
    "שירלי",
    "לימור",
    "ענבר",
    "נגה",
    "אפרת",
    "טלי",
    "מאי",
    "נוי",
    "אגם",
    "שי-לי",
    "גילי",
    "שירן",
    "ליאורה",
    "אורנה",
    "חנה",
    "מרים",
    "רבקה",
    "אילנית",
    "שולמית",
    "יובל",
    "נועם",
    "אלה",
    "דורית",
    "יעלה",
    "מיטל",
    "ניצן",
    "פנינה",
    "ציפי",
    "קורל",
    "רינה",
    "שי",
    "תכלת",
    "אביגיל",
    "אופירה",
    "איילת",
    "אילת",
    "אלונה",
    "אריאלה",
    "בת-שבע",
    "גאיה",
    "גיל",
    "דיאנה",
    "הודיה",
    "זהבה",
    "חגית",
    "טובה",
    "יאסמין",
]

_ISRAELI_LAST_NAMES_RAW: list[str] = [
    "כהן",
    "לוי",
    "מזרחי",
    "דהן",
    "אברהם",
    "ביטון",
    "פרידמן",
    "שפירא",
    "אביב",
    "גולן",
    "ברק",
    "אדרי",
    "חדד",
    "אזולאי",
    "בן דוד",
    "פרץ",
    "בוסקילה",
    "אוחנה",
    "זיו",
    "מלכה",
    "אלון",
    "בן חמו",
    "לוין",
    "שטרן",
    "רוזן",
    "גרין",
    "קליין",
    "סגל",
    "טל",
    "נחום",
    "אורבך",
    "חיים",
    "יוסף",
    "דוד",
    "משה",
    "אשכנזי",
    "רוזנברג",
    "גולדשטיין",
    "וייס",
    "שלום",
    "מזרחי",
    "פרידמן",
    "ביטון",
    "כהן",
    "לוי",
    "אברהם",
    "דהן",
    "חזן",
    "אלבז",
    "אמסלם",
    "אוחיון",
    "בן סימון",
    "בן עמי",
    "גבאי",
    "גמליאל",
    "גרוסמן",
    "דנינו",
    "הרשקוביץ",
    "זלצר",
    "חנוכה",
    "טובי",
    "יאיר",
    "כרמלי",
    "לביא",
    "מלכה",
    "נחמני",
    "סבג",
    "עזריה",
    "פנחס",
    "צדוק",
    "קורן",
    "רבינוביץ",
    "שמש",
    "תורגמן",
    "אילן",
    "נבו",
    "עזרא",
    "בן ארי",
    "בן גל",
    "בן חיים",
    "בן יוסף",
    "בן שושן",
    "ברמן",
    "גוטמן",
    "גרטנר",
    "הלפרין",
    "זוהר",
    "חביב",
    "טביב",
    "כץ",
    "לנדאו",
    "מנדלסון",
    "נחשון",
    "סויסה",
    "עמיר",
    "פוגל",
    "צור",
    "קפלן",
    "רוזנטל",
    "שטיין",
    "תמיר",
    "אדר",
    "בן אליעזר",
    "גביש",
    "דניאל",
    "הכהן",
    "זינגר",
    "חמו",
    "טולידנו",
    "כרמל",
    "לנצט",
    "מזוז",
    "נחשון",
    "סורוקין",
    "פלד",
    "צוקר",
    "קדוש",
    "רוט",
    "שוהם",
    "תורני",
]

ISRAELI_FIRST_NAMES_MALE = _dedupe_preserve_order(_ISRAELI_FIRST_NAMES_MALE_RAW)
ISRAELI_FIRST_NAMES_FEMALE = _dedupe_preserve_order(_ISRAELI_FIRST_NAMES_FEMALE_RAW)
ISRAELI_LAST_NAMES = _dedupe_preserve_order(_ISRAELI_LAST_NAMES_RAW)

_LATIN_LETTER = re.compile(r"[A-Za-z]")


def _truncate(s: str) -> str:
    t = (s or "").strip()
    if len(t) <= _NAME_MAX_LEN:
        return t
    return t[:_NAME_MAX_LEN]


def profile_has_english(first_name: str | None, last_name: str | None) -> bool:
    """True if either name field contains ASCII Latin letters."""
    for part in (first_name or "", last_name or ""):
        if _LATIN_LETTER.search(part):
            return True
    return False


def needs_profile_localization(first_name: str | None, last_name: str | None) -> bool:
    """Empty combined name or any Latin letter → localize (per maintenance plan)."""
    fn = first_name or ""
    ln = last_name or ""
    if not f"{fn} {ln}".strip():
        return True
    return profile_has_english(fn, ln)


def pick_hebrew_name_pair(rng: random.Random) -> tuple[str, str]:
    """Random Hebrew first (gendered pool) + Hebrew surname."""
    if rng.random() < 0.5:
        first = rng.choice(ISRAELI_FIRST_NAMES_MALE)
    else:
        first = rng.choice(ISRAELI_FIRST_NAMES_FEMALE)
    last = rng.choice(ISRAELI_LAST_NAMES)
    return _truncate(first), _truncate(last)


def _prune_hour_window(q: deque[float], now: float) -> None:
    while q and now - q[0] >= 3600.0:
        q.popleft()


async def _wait_for_hourly_slot(update_times: deque[float]) -> None:
    """Block until fewer than 20 updates occurred in the last rolling hour."""
    while True:
        now = time.monotonic()
        _prune_hour_window(update_times, now)
        if len(update_times) < 20:
            return
        wait_s = max(1.0, 3600.0 - (now - update_times[0]) + random.uniform(1.0, 15.0))
        log.info("localize_profiles_hourly_cap_wait", seconds=round(wait_s, 1))
        await asyncio.sleep(wait_s)


@dataclass(frozen=True)
class _SessionWorkItem:
    stem: str
    session_base: str
    api_id: int
    api_hash: str


def _scan_flat_session_dir(root: Path) -> list[Path]:
    if not root.is_dir():
        return []
    return sorted(p for p in root.glob("*.session") if p.is_file())


def _env_flat_api_credentials() -> tuple[int, str] | None:
    raw_id = (os.environ.get("TELEGRAM_API_ID") or "").strip()
    api_hash = (os.environ.get("TELEGRAM_API_HASH") or "").strip()
    if not raw_id or not api_hash:
        return None
    try:
        return int(raw_id), api_hash
    except ValueError:
        return None


async def _run_localize_session(
    item: _SessionWorkItem,
    *,
    dry_run: bool,
    rng: random.Random,
    update_times: deque[float],
) -> dict[str, Any]:
    from telethon import TelegramClient  # type: ignore[import-untyped]
    from telethon.errors import FloodWaitError  # type: ignore[import-untyped]
    from telethon.tl.functions.account import UpdateProfileRequest  # type: ignore[import-untyped]

    stem = item.stem
    session_base = item.session_base
    extra = telethon_connect_kwargs_for_session_base(session_base, stem)

    async with telegram_network_slot(task_name="localize_profiles"):
        async with TelegramClient(
            session_base, item.api_id, item.api_hash, **extra
        ) as client:
            if not await client.is_user_authorized():
                return {"stem": stem, "ok": False, "action": "skip", "detail": "not authorized"}

            me = await client.get_me()
            uid = getattr(me, "id", None)
            fn = me.first_name or ""
            ln = me.last_name or ""

            if not needs_profile_localization(me.first_name, me.last_name):
                return {
                    "stem": stem,
                    "ok": True,
                    "action": "unchanged",
                    "detail": "already Hebrew-only non-empty name",
                    "user_id": uid,
                    "first_name": fn,
                    "last_name": ln,
                }

            new_fn, new_ln = pick_hebrew_name_pair(rng)

            if dry_run:
                return {
                    "stem": stem,
                    "ok": True,
                    "action": "dry_run",
                    "detail": "would update",
                    "user_id": uid,
                    "old_first": fn,
                    "old_last": ln,
                    "new_first": new_fn,
                    "new_last": new_ln,
                }

            await _wait_for_hourly_slot(update_times)

            async def _do_update() -> None:
                await client(
                    UpdateProfileRequest(
                        first_name=new_fn,
                        last_name=new_ln,
                    )
                )

            try:
                await _do_update()
            except FloodWaitError as exc:
                sec = min(int(getattr(exc, "seconds", 300) or 300), 3600)
                log.warning("localize_profiles_flood_wait", stem=stem, seconds=sec)
                await asyncio.sleep(sec)
                try:
                    await _do_update()
                except Exception as exc2:
                    log.warning("localize_profiles_update_failed", stem=stem, error=str(exc2))
                    return {"stem": stem, "ok": False, "action": "error", "detail": str(exc2)}
            except Exception as exc:
                log.warning("localize_profiles_update_failed", stem=stem, error=str(exc))
                return {"stem": stem, "ok": False, "action": "error", "detail": str(exc)}

            update_times.append(time.monotonic())
            log.info(
                "localize_profiles_updated",
                stem=stem,
                user_id=uid,
                new_first=new_fn,
                new_last=new_ln,
            )
            return {
                "stem": stem,
                "ok": True,
                "action": "updated",
                "user_id": uid,
                "old_first": fn,
                "old_last": ln,
                "new_first": new_fn,
                "new_last": new_ln,
            }


async def _process_vault_meta(
    meta_json: Path,
    *,
    dry_run: bool,
    rng: random.Random,
    update_times: deque[float],
) -> dict[str, Any]:
    stem = meta_json.stem
    session_base = str(meta_json.with_suffix(""))
    try:
        raw = json.loads(meta_json.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"stem": stem, "ok": False, "action": "error", "detail": f"meta read: {exc}"}

    if not isinstance(raw, dict):
        return {"stem": stem, "ok": False, "action": "error", "detail": "meta not an object"}

    creds = vault_meta_resolve_api_credentials(raw)
    if not creds:
        return {"stem": stem, "ok": False, "action": "skip", "detail": "no api_id/api_hash in meta"}

    api_id, api_hash = creds
    item = _SessionWorkItem(
        stem=stem, session_base=session_base, api_id=api_id, api_hash=api_hash
    )
    return await _run_localize_session(item, dry_run=dry_run, rng=rng, update_times=update_times)


async def _process_flat_session_file(
    session_file: Path,
    *,
    api_id: int,
    api_hash: str,
    dry_run: bool,
    rng: random.Random,
    update_times: deque[float],
) -> dict[str, Any]:
    stem = session_file.stem
    session_base = str(session_file.resolve().with_suffix(""))
    item = _SessionWorkItem(
        stem=stem, session_base=session_base, api_id=api_id, api_hash=api_hash
    )
    return await _run_localize_session(item, dry_run=dry_run, rng=rng, update_times=update_times)


async def run_localize_profiles(
    *,
    dry_run: bool = False,
    limit: int | None = None,
    shuffle: bool = True,
    flat_sessions_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """
    Vault mode: scan session vault metas (per-session API credentials).

    Flat mode: ``flat_sessions_dir`` set → non-recursive ``*.session`` scan;
    requires ``TELEGRAM_API_ID`` / ``TELEGRAM_API_HASH`` in the environment.

    For each authorized account that needs localization (empty name or Latin
    letters), apply a Hebrew name pair unless ``dry_run``.
    """
    rng = random.Random()
    update_times: deque[float] = deque()
    results: list[dict[str, Any]] = []

    if flat_sessions_dir is not None:
        creds = _env_flat_api_credentials()
        if not creds:
            log.error(
                "localize_profiles_flat_mode_missing_api",
                hint="set TELEGRAM_API_ID and TELEGRAM_API_HASH",
            )
            return [
                {
                    "stem": "",
                    "ok": False,
                    "action": "error",
                    "detail": "flat mode requires TELEGRAM_API_ID and TELEGRAM_API_HASH",
                }
            ]
        api_id, api_hash = creds
        paths = _scan_flat_session_dir(flat_sessions_dir.resolve())
        if shuffle:
            random.shuffle(paths)
        if limit is not None and limit > 0:
            paths = paths[:limit]
        if not flat_sessions_dir.is_dir():
            log.warning(
                "localize_profiles_flat_dir_missing",
                dir=str(flat_sessions_dir.resolve()),
            )
        log.info(
            "localize_profiles_flat_scan",
            dir=str(flat_sessions_dir.resolve()),
            count=len(paths),
        )
        for sess_path in paths:
            res = await _process_flat_session_file(
                sess_path,
                api_id=api_id,
                api_hash=api_hash,
                dry_run=dry_run,
                rng=rng,
                update_times=update_times,
            )
            results.append(res)
            if res.get("action") == "updated" and not dry_run:
                delay = random.randint(60, 300)
                log.info("localize_profiles_opsec_sleep", seconds=delay, stem=res.get("stem"))
                await asyncio.sleep(delay)
            elif res.get("action") == "error":
                await asyncio.sleep(random.randint(5, 15))
            else:
                await asyncio.sleep(random.randint(2, 8))
        return results

    metas = discover_meta_paths_from_session_sqlite()
    if shuffle:
        random.shuffle(metas)
    if limit is not None and limit > 0:
        metas = metas[:limit]

    for meta in metas:
        res = await _process_vault_meta(
            meta,
            dry_run=dry_run,
            rng=rng,
            update_times=update_times,
        )
        results.append(res)

        if res.get("action") == "updated" and not dry_run:
            delay = random.randint(60, 300)
            log.info("localize_profiles_opsec_sleep", seconds=delay, stem=res.get("stem"))
            await asyncio.sleep(delay)
        elif res.get("action") == "error":
            await asyncio.sleep(random.randint(5, 15))
        else:
            await asyncio.sleep(random.randint(2, 8))

    return results


def _summarize(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for r in rows:
        a = str(r.get("action") or "unknown")
        counts[a] = counts.get(a, 0) + 1
    return counts


def _default_telegram_sessions_dir() -> Path | None:
    raw = (os.environ.get("TELEGRAM_SESSIONS_DIR") or "").strip()
    if not raw:
        return None
    return Path(raw).expanduser().resolve()


async def _async_main() -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="Localize Telegram profiles to Hebrew names.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print what would change; no Telegram writes.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process at most N sessions (0 = all).",
    )
    parser.add_argument(
        "--no-shuffle",
        action="store_true",
        help="Process sessions in sorted path order instead of random.",
    )
    parser.add_argument(
        "--sessions-dir",
        type=Path,
        default=None,
        help=(
            "Non-recursive scan for *.session (flat mode). "
            "Default: TELEGRAM_SESSIONS_DIR if set, else Nexus vault discovery."
        ),
    )
    args = parser.parse_args()
    lim = args.limit if args.limit and args.limit > 0 else None

    flat_dir: Path | None = None
    if args.sessions_dir is not None:
        flat_dir = args.sessions_dir.expanduser().resolve()
    else:
        flat_dir = _default_telegram_sessions_dir()

    rows = await run_localize_profiles(
        dry_run=args.dry_run,
        limit=lim,
        shuffle=not args.no_shuffle,
        flat_sessions_dir=flat_dir,
    )
    summary = _summarize(rows)
    log.info("localize_profiles_done", summary=summary, total=len(rows))
    print(json.dumps({"summary": summary, "total": len(rows)}, ensure_ascii=False, indent=2))
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(_async_main()))


if __name__ == "__main__":
    main()
