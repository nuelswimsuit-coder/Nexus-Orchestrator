"""
Authentic-ish Israeli Telegram profile rolls for swarm workers: mixed Hebrew/English
names, mostly empty bios, optional messy usernames, and non-face photos (delete or picsum).
All Telethon calls are wrapped for FloodWaitError (sleep, no crash) and username collisions.
"""

from __future__ import annotations

import asyncio
import os
import random
import re
import secrets
import string
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, TypeVar

import httpx
import structlog

log = structlog.get_logger(__name__)

FLOOD_WAIT_MAX_S = 3600
_TELEGRAM_NAME_MAX = 64

# Hebrew given names (popular / common)
_HEBREW_FIRST = [
    "יוסי", "דני", "אורי", "נועם", "איתי", "רועי", "עומר", "גיא", "תומר", "אלון",
    "מיכאל", "אדם", "עידו", "ליאור", "שי", "רן", "עמית", "אביב", "הדר", "גל",
    "מיכל", "נועה", "שירה", "מאיה", "תמר", "יעל", "רות", "דנה", "ליאת", "ענת",
    "הילה", "קרן", "שקד", "מור", "אור", "ספיר", "לילך", "רוני", "מיטל", "עדי",
    "אבי", "רונן", "בן", "אמיר", "נדב", "אייל", "שלומי", "יונתן", "דור", "מאור",
]

# English / international spellings common in Israel
_ENGLISH_FIRST = [
    "Yossi", "Dana", "Avi", "Omer", "Noam", "Tomer", "Guy", "Rotem", "Shir", "Maya",
    "David", "Daniel", "Michael", "Adam", "Lior", "Itai", "Nir", "Gal", "Amit", "Erez",
    "Tal", "Bar", "Yuval", "Ido", "Roni", "Shani", "Michal", "Yael", "Tamar", "Stav",
    "Alex", "Max", "Ben", "Jonathan", "Dor", "Maor", "Eitan", "Kobi", "Rami", "Hila",
]

_HEBREW_LAST = [
    "כהן", "לוי", "מזרחי", "דהן", "אביב", "שפירא", "גולן", "ברק", "אדרי", "ביטון",
    "פרידמן", "גרין", "רוזן", "קליין", "אשכנזי", "סגל", "טל", "נחום", "אורבך", "חיים",
    "דוד", "משה", "יוסף", "זיו", "שמש", "אילן", "נבו", "עזריה", "מלכה", "פרץ",
]

_ENGLISH_LAST = [
    "Cohen", "Levy", "Mizrahi", "Aviv", "Barak", "Friedman", "Green", "Rosen", "Klein",
    "Segal", "Tal", "David", "Joseph", "Peretz", "Azulay", "Biton", "Shapiro", "Gold",
]

_INITIALS_HE = ["מ.", "ד.", "א.", "נ.", "י.", "ל.", "ר.", "ש.", "ע.", "ג.", "ת.", "ה."]
_INITIALS_EN = ["C.", "L.", "D.", "M.", "A.", "B.", "S.", "R.", "T.", "K.", "N.", "G."]


_ISRAELI_BIOS = [
    "",
    "Live and let live",
    "Tel Aviv",
    "...",
    "🙏",
    "רק בריאות",
    "Carpe Diem",
    "תל אביב",
    "לא פעיל פה הרבה",
    "✌️",
    "שלווה",
    "Busy",
    "🇮🇱",
    "יום יום",
    "no drama",
    "🙂",
    "בשקט",
]

T = TypeVar("T")


@dataclass(frozen=True)
class IsraeliProfileRoll:
    first_name: str
    last_name: str
    about: str
    clear_username: bool
    username_candidate: str | None
    delete_profile_photos: bool
    picsum_seed: str


def _truncate_telegram_field(s: str) -> str:
    t = (s or "").strip()
    if len(t) <= _TELEGRAM_NAME_MAX:
        return t
    return t[:_TELEGRAM_NAME_MAX]


def _pick_hebrew_pair() -> tuple[str, str]:
    u = random.random()
    first = random.choice(_HEBREW_FIRST)
    if u < 0.55:
        last = random.choice(_HEBREW_LAST)
    else:
        last = random.choice(_INITIALS_HE)
    return first, last


def _pick_english_pair() -> tuple[str, str]:
    u = random.random()
    first = random.choice(_ENGLISH_FIRST)
    if u < 0.55:
        last = random.choice(_ENGLISH_LAST)
    else:
        last = random.choice(_INITIALS_EN)
    return first, last


def roll_display_name() -> tuple[str, str]:
    """40% Hebrew, 40% English, 10% first-only, 10% first + digits."""
    r = random.random()
    if r < 0.40:
        f, l = _pick_hebrew_pair()
        return _truncate_telegram_field(f), _truncate_telegram_field(l)
    if r < 0.80:
        f, l = _pick_english_pair()
        return _truncate_telegram_field(f), _truncate_telegram_field(l)
    if r < 0.90:
        pool = random.choice((_HEBREW_FIRST, _ENGLISH_FIRST))
        f = random.choice(pool)
        return _truncate_telegram_field(f), ""
    base = random.choice(_ENGLISH_FIRST)
    digits = str(random.randint(1, 9999))
    return _truncate_telegram_field(f"{base}{digits}"), ""


def roll_about() -> str:
    if random.random() < 0.80:
        return ""
    choices = [b for b in _ISRAELI_BIOS if b]
    return random.choice(choices) if choices else ""


def _slug_ascii_token(s: str) -> str:
    t = re.sub(r"[^a-z0-9]+", "_", s.lower().strip())
    t = re.sub(r"_+", "_", t).strip("_")
    return t[:24] if t else ""


def roll_username_plan(first_name: str, last_name: str) -> tuple[bool, str | None]:
    """
    70% clear username (private-style); 30% messy ascii username.
    Returns (clear_username, candidate_or_none).
    """
    if random.random() < 0.70:
        return True, None
    base = _slug_ascii_token(first_name) or "user"
    frag = _slug_ascii_token(last_name)
    sep = random.choice(["_", ".", "_", ""])
    mid = f"{sep}{frag}" if frag and sep else (f"_{frag}" if frag else "")
    digits = "".join(random.choices(string.digits, k=random.randint(3, 5)))
    raw = f"{base}{mid}_{digits}" if mid else f"{base}_{digits}"
    raw = re.sub(r"[^a-z0-9_]", "", raw.lower())
    raw = raw.strip("_")
    if len(raw) < 5:
        raw = f"{base}_{digits}".strip("_")
    if len(raw) < 5:
        raw = f"user_{digits}"
    if len(raw) > 32:
        raw = raw[:32].rstrip("_")
    if len(raw) < 5:
        raw = (raw + "x" * 5)[:5]
    return False, raw


def roll_photo_plan(seed_material: str) -> tuple[bool, str]:
    """Returns (delete_all_photos, picsum_seed). If delete True, ignore picsum except for logging."""
    delete = random.random() < 0.50
    h = secrets.token_hex(8)
    picsum_seed = f"{abs(hash(seed_material)) % (2**32):x}-{h}"
    return delete, picsum_seed


def roll_israeli_profile(seed_material: str) -> IsraeliProfileRoll:
    fn, ln = roll_display_name()
    about = roll_about()
    clear_u, u_cand = roll_username_plan(fn, ln)
    del_ph, pseed = roll_photo_plan(seed_material)
    return IsraeliProfileRoll(
        first_name=fn,
        last_name=ln,
        about=about,
        clear_username=clear_u,
        username_candidate=u_cand,
        delete_profile_photos=del_ph,
        picsum_seed=pseed,
    )


async def _sleep_flood_wait(exc: BaseException) -> None:
    try:
        from telethon.errors import FloodWaitError  # type: ignore[import-untyped]
    except ImportError:
        return
    if isinstance(exc, FloodWaitError):
        sec = min(int(getattr(exc, "seconds", 60) or 60), FLOOD_WAIT_MAX_S)
        log.warning("israeli_profile_flood_wait", seconds=sec)
        await asyncio.sleep(sec)


async def _safe_call(coro: Awaitable[T], *, context: str) -> T | None:
    try:
        return await coro
    except Exception as exc:
        try:
            from telethon.errors import FloodWaitError  # type: ignore[import-untyped]

            if isinstance(exc, FloodWaitError):
                await _sleep_flood_wait(exc)
                return None
        except ImportError:
            pass
        log.debug("israeli_profile_rpc_skipped", context=context, error=str(exc))
        return None


async def _update_profile_name_bio(client: Any, fn: str, ln: str, about: str) -> None:
    from telethon.tl.functions.account import UpdateProfileRequest  # type: ignore[import-untyped]

    await _safe_call(
        client(
            UpdateProfileRequest(
                first_name=fn,
                last_name=ln,
                about=about,
            )
        ),
        context="update_profile",
    )


async def _update_username(client: Any, username: str) -> None:
    from telethon.errors import (  # type: ignore[import-untyped]
        UsernameInvalidError,
        UsernameOccupiedError,
    )
    from telethon.tl.functions.account import UpdateUsernameRequest  # type: ignore[import-untyped]

    try:
        await client(UpdateUsernameRequest(username=username))
    except UsernameOccupiedError:
        log.debug("israeli_profile_username_occupied", username=username[:32])
    except UsernameInvalidError:
        log.debug("israeli_profile_username_invalid", username=username[:32])
    except Exception as exc:
        try:
            from telethon.errors import FloodWaitError  # type: ignore[import-untyped]

            if isinstance(exc, FloodWaitError):
                await _sleep_flood_wait(exc)
                return
        except ImportError:
            pass
        log.debug("israeli_profile_username_failed", error=str(exc))


def _is_flood(exc: BaseException) -> bool:
    try:
        from telethon.errors import FloodWaitError  # type: ignore[import-untyped]

        return isinstance(exc, FloodWaitError)
    except ImportError:
        return False


async def _delete_all_profile_photos(client: Any) -> None:
    from telethon.tl.functions.photos import DeletePhotosRequest  # type: ignore[import-untyped]
    from telethon.utils import get_input_photo  # type: ignore[import-untyped]

    try:
        ph_list = await client.get_profile_photos("me", limit=100).collect()
    except Exception as exc:
        if _is_flood(exc):
            await _sleep_flood_wait(exc)
        else:
            log.debug("israeli_profile_list_photos_failed", error=str(exc))
        return

    if not ph_list:
        return

    try:
        ids = [get_input_photo(p) for p in ph_list]
        await _safe_call(client(DeletePhotosRequest(id=ids)), context="delete_photos")
    except Exception as exc:
        if _is_flood(exc):
            await _sleep_flood_wait(exc)
        log.debug("israeli_profile_delete_photos_failed", error=str(exc))


async def _upload_picsum_profile_photo(client: Any, picsum_seed: str) -> None:
    from telethon.tl.functions.photos import UploadProfilePhotoRequest  # type: ignore[import-untyped]

    url = f"https://picsum.photos/seed/{picsum_seed}/400/400"
    tmp_path: Path | None = None
    try:
        async with httpx.AsyncClient(timeout=45.0, follow_redirects=True) as http:
            r = await http.get(url)
            r.raise_for_status()
            body = r.content
        if len(body) < 256:
            log.debug("israeli_profile_picsum_too_small")
            return
        fd, name = tempfile.mkstemp(suffix=".jpg")
        os.close(fd)
        tmp_path = Path(name)
        tmp_path.write_bytes(body)
        uploaded = await client.upload_file(str(tmp_path))
        await _safe_call(
            client(UploadProfilePhotoRequest(file=uploaded)),
            context="upload_profile_photo",
        )
    except Exception as exc:
        if _is_flood(exc):
            await _sleep_flood_wait(exc)
        log.debug("israeli_profile_picsum_failed", error=str(exc))
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except TypeError:
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                pass


async def apply_israeli_profile_roll(client: Any, roll: IsraeliProfileRoll) -> None:
    """Execute Telethon updates for a pre-computed roll (no Redis)."""
    await _update_profile_name_bio(client, roll.first_name, roll.last_name, roll.about)

    if roll.clear_username:
        await _update_username(client, "")
    elif roll.username_candidate:
        await _update_username(client, roll.username_candidate)

    if roll.delete_profile_photos:
        await _delete_all_profile_photos(client)
    else:
        await _delete_all_profile_photos(client)
        await _upload_picsum_profile_photo(client, roll.picsum_seed)


async def ensure_israeli_factory_profile(
    client: Any,
    redis: Any,
    session_base: str,
    *,
    gate_key: str,
    local_verified: set[str] | None = None,
) -> None:
    """
    Once per session (Redis set ``gate_key``, or ``local_verified`` when Redis missing).
    Always applies a fresh roll on first factory touch after gate miss.
    """
    if redis is not None:
        try:
            if await redis.sismember(gate_key, session_base):
                return
        except Exception:
            pass
    elif local_verified is not None and session_base in local_verified:
        return

    roll = roll_israeli_profile(session_base)
    try:
        await apply_israeli_profile_roll(client, roll)
    except Exception as exc:
        log.debug("israeli_factory_profile_apply_failed", error=str(exc))

    if redis is not None:
        try:
            await redis.sadd(gate_key, session_base)
        except Exception:
            pass
    elif local_verified is not None:
        local_verified.add(session_base)
