"""
Outbound media hygiene before Telegram upload (swarm / warmer / community engine).

- Raster: strip EXIF (JPEG) and textual PNG chunks; apply a one-pixel LSB salt so SHA-256
  differs per upload while staying visually imperceptible.
- Ogg Opus/Vorbis: strip comment headers; helpers for voice-note duration + Telethon kwargs.

New code that sends ``.ogg`` bytes must use ``strip_ogg_metadata`` + ``telethon_voice_kwargs``.
"""

from __future__ import annotations

import hashlib
import math
import secrets
from io import BytesIO
from typing import Any

from PIL import Image

from nexus.services.recent_news_digest import telegram_image_filename_from_bytes


def make_image_upload_salt_seed(session_label: str) -> bytes:
    """Session-scoped entropy; each call is unique (includes fresh random bytes)."""
    h = hashlib.sha256()
    h.update((session_label or "").encode("utf-8", errors="ignore"))
    h.update(secrets.token_bytes(16))
    return h.digest()


def _is_jpeg_magic(data: bytes) -> bool:
    return len(data) >= 3 and data[:3] == b"\xff\xd8\xff"


def _is_png_magic(data: bytes) -> bool:
    return len(data) >= 8 and data[:8] == b"\x89PNG\r\n\x1a\n"


def _lsb_tweak_pixel(img: Image.Image, salt_seed: bytes) -> None:
    w, h = img.size
    if w < 1 or h < 1:
        return
    x, y = (w - 1) % w, (h - 1) % h
    digest = hashlib.sha256(salt_seed).digest()
    raw_mode = img.mode
    if raw_mode == "RGB":
        r, g, b = img.getpixel((x, y))[:3]
        r = (r & ~1) | (digest[0] & 1)
        g = (g & ~1) | ((digest[0] >> 1) & 1)
        b = (b & ~1) | ((digest[0] >> 2) & 1)
        img.putpixel((x, y), (r, g, b))
    elif raw_mode == "RGBA":
        r, g, b, a = img.getpixel((x, y))[:4]
        r = (r & ~1) | (digest[0] & 1)
        g = (g & ~1) | ((digest[0] >> 1) & 1)
        b = (b & ~1) | ((digest[0] >> 2) & 1)
        a = (a & ~1) | ((digest[0] >> 3) & 1)
        img.putpixel((x, y), (r, g, b, a))


def prepare_jpeg_png_for_telegram_upload(raw: bytes, *, salt_seed: bytes) -> tuple[bytes, str]:
    """
    Strip metadata and apply one-pixel LSB salt for JPEG/PNG; pass-through others.
    Returns ``(bytes, suggested_filename)``.
    """
    if not raw:
        return raw, "photo.jpg"
    if not (_is_jpeg_magic(raw) or _is_png_magic(raw)):
        return raw, telegram_image_filename_from_bytes(raw)

    try:
        src = Image.open(BytesIO(raw))
    except Exception:
        return raw, telegram_image_filename_from_bytes(raw)

    try:
        if _is_jpeg_magic(raw):
            im = src.convert("RGB")
            _lsb_tweak_pixel(im, salt_seed)
            out = BytesIO()
            im.save(
                out,
                format="JPEG",
                quality=88,
                optimize=True,
                subsampling=2,
                exif=b"",
            )
            data = out.getvalue()
            return data, telegram_image_filename_from_bytes(data)

        # PNG → drop ancillary metadata by copying pixels only, then RGBA + salt
        im = src.convert("RGBA")
        clean = Image.new("RGBA", im.size)
        clean.putdata(list(im.getdata()))
        _lsb_tweak_pixel(clean, salt_seed)
        out = BytesIO()
        clean.save(out, format="PNG", optimize=True)
        data = out.getvalue()
        return data, telegram_image_filename_from_bytes(data)
    finally:
        try:
            src.close()
        except Exception:
            pass


def ogg_voice_duration_seconds(data: bytes) -> int:
    """Length in whole seconds for Telethon ``DocumentAttributeAudio`` (minimum 1)."""
    if not data:
        return 1
    try:
        from mutagen import File as MutagenFile  # type: ignore[import-untyped]
    except ImportError:
        return 1
    try:
        audio = MutagenFile(BytesIO(data))
        if audio is None or not hasattr(audio, "info") or audio.info is None:
            return 1
        length = getattr(audio.info, "length", None)
        if length is None:
            return 1
        return max(1, int(math.ceil(float(length))))
    except Exception:
        return 1


def strip_ogg_metadata(data: bytes) -> bytes:
    """Remove Vorbis/Opus comment packets; returns original bytes on failure."""
    if not data or data[:4] != b"OggS":
        return data
    try:
        from mutagen.oggopus import OggOpus  # type: ignore[import-untyped]
        from mutagen.oggvorbis import OggVorbis  # type: ignore[import-untyped]
    except ImportError:
        return data
    bio = BytesIO(data)
    for cls in (OggOpus, OggVorbis):
        try:
            bio.seek(0)
            f = cls(bio)
            f.delete()
            out = BytesIO()
            f.save(out)
            return out.getvalue()
        except Exception:
            continue
    return data


def telethon_voice_kwargs(duration_seconds: int) -> dict[str, Any]:
    """Kwargs for ``TelegramClient.send_file`` so OGG is shown as a voice note."""
    from telethon.tl.types import DocumentAttributeAudio  # type: ignore[import-untyped]

    d = max(1, int(duration_seconds))
    return {
        "voice_note": True,
        "attributes": [DocumentAttributeAudio(duration=d, voice=True)],
    }
