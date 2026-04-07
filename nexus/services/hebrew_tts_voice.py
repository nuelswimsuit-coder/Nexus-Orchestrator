"""
Hebrew TTS for Telegram voice notes: Edge-TTS (free) → OGG Opus via ffmpeg.

Requires ``ffmpeg`` on PATH (same as other media pipelines in this repo).
"""

from __future__ import annotations

import asyncio
import os
import random
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import structlog

log = structlog.get_logger(__name__)

# Israeli Hebrew neural voices (Edge TTS)
_HE_VOICES = (
    "he-IL-HilaNeural",
    "he-IL-AvriNeural",
)

_MAX_TTS_CHARS = 900


def tts_suitable_text(text: str) -> str | None:
    """Return cleaned text suitable for TTS, or None if we should skip voice."""
    raw = (text or "").strip()
    if len(raw) < 2:
        return None
    if len(raw) > _MAX_TTS_CHARS:
        raw = raw[:_MAX_TTS_CHARS].rstrip()
    # Need something speakable (Hebrew or Latin letters)
    if not re.search(r"[\u0590-\u05FFa-zA-Z]", raw):
        return None
    return re.sub(r"\s+", " ", raw)


async def build_hebrew_voice_ogg_path(text: str) -> str | None:
    """
    Synthesize ``text`` to a temporary ``.ogg`` (Opus) file.

    Caller must ``os.remove`` the path when finished (e.g. after send).
    Returns None if synthesis failed.
    """
    tts_text = tts_suitable_text(text)
    if not tts_text:
        return None
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        log.debug("hebrew_tts_no_ffmpeg")
        return None
    try:
        import edge_tts  # type: ignore[import-untyped]
    except ImportError:
        log.debug("hebrew_tts_edge_import_failed")
        return None

    voice = random.choice(_HE_VOICES)
    mp3_path: str | None = None
    ogg_path: str | None = None
    try:
        fd_m, mp3_path = tempfile.mkstemp(suffix=".mp3")
        os.close(fd_m)
        fd_o, ogg_path = tempfile.mkstemp(suffix=".ogg")
        os.close(fd_o)

        communicate = edge_tts.Communicate(tts_text, voice)
        await communicate.save(mp3_path)

        cmd = [
            ffmpeg,
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            mp3_path,
            "-c:a",
            "libopus",
            "-b:a",
            "48k",
            "-ar",
            "48000",
            "-ac",
            "1",
            ogg_path,
        ]

        def _run_ffmpeg() -> None:
            subprocess.run(cmd, check=True, capture_output=True)

        await asyncio.to_thread(_run_ffmpeg)
        if not Path(ogg_path).is_file() or Path(ogg_path).stat().st_size < 64:
            return None
        return ogg_path
    except Exception as exc:
        log.debug("hebrew_tts_build_failed", error=str(exc))
        if ogg_path and os.path.isfile(ogg_path):
            try:
                os.remove(ogg_path)
            except OSError:
                pass
        return None
    finally:
        if mp3_path and os.path.isfile(mp3_path):
            try:
                os.remove(mp3_path)
            except OSError:
                pass
