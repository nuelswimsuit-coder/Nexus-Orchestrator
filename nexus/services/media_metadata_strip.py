"""
Strip embedded metadata from media on disk (ingestion / meme DB).

Images: re-encode via Pillow so EXIF / PNG text chunks / WebP XMP are dropped.
Video: optional ffmpeg ``-map_metadata -1`` copy when ``ffmpeg`` is on PATH.
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Final

from PIL import Image

_IMAGE_EXT: Final = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"}
_VIDEO_EXT: Final = {".mp4", ".mov", ".webm", ".mkv", ".m4v"}


def _ffmpeg_bin() -> str | None:
    return shutil.which("ffmpeg")


def strip_image_metadata_inplace(path: Path) -> bool:
    """
    Rewrite ``path`` in place with no EXIF / ancillary PNG chunks (pixels preserved
    where possible). Returns True if the file was processed as a raster image.
    """
    suf = path.suffix.lower()
    if suf not in _IMAGE_EXT:
        return False
    tmp: Path | None = None
    try:
        with Image.open(path) as src:
            src.load()
            if suf in {".jpg", ".jpeg"}:
                im = src.convert("RGB")
                tmp = path.with_suffix(path.suffix + ".tmp")
                im.save(tmp, format="JPEG", quality=90, optimize=True, subsampling=0, exif=b"")
            elif suf == ".png":
                im = src.convert("RGBA")
                clean = Image.new("RGBA", im.size)
                clean.putdata(list(im.getdata()))
                tmp = path.with_suffix(path.suffix + ".tmp")
                clean.save(tmp, format="PNG", optimize=True)
            elif suf == ".webp":
                im = src.convert("RGBA")
                tmp = path.with_suffix(path.suffix + ".tmp")
                im.save(tmp, format="WEBP", quality=85, method=6)
            elif suf == ".gif":
                im = src.convert("RGBA")
                tmp = path.with_suffix(path.suffix + ".tmp")
                im.save(tmp, format="GIF", save_all=False)
            elif suf == ".bmp":
                im = src.convert("RGB")
                tmp = path.with_suffix(path.suffix + ".tmp")
                im.save(tmp, format="BMP")
            else:
                im = src.convert("RGB")
                tmp = path.with_suffix(path.suffix + ".tmp")
                im.save(tmp, format="PNG")
        tmp.replace(path)
        return True
    except Exception:
        try:
            if tmp is not None:
                tmp.unlink(missing_ok=True)
        except Exception:
            pass
        return False


def strip_video_metadata_inplace(path: Path) -> bool:
    """
    Remux video without container metadata using ffmpeg. Returns False if ffmpeg
    is missing or the transcode fails (file left unchanged).
    """
    suf = path.suffix.lower()
    if suf not in _VIDEO_EXT:
        return False
    ffmpeg = _ffmpeg_bin()
    if not ffmpeg:
        return False
    out: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            suffix=suf,
            delete=False,
        ) as tmp:
            out = Path(tmp.name)
        cmd = [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            str(path),
            "-map_metadata",
            "-1",
            "-c",
            "copy",
            str(out),
        ]
        subprocess.run(cmd, check=True, capture_output=True, timeout=600)
        if out is not None:
            out.replace(path)
        return True
    except Exception:
        try:
            if out is not None:
                out.unlink(missing_ok=True)
        except Exception:
            pass
        return False


def strip_media_metadata_inplace(path: Path) -> None:
    """Best-effort metadata wipe for common Telegram media types."""
    if strip_image_metadata_inplace(path):
        return
    strip_video_metadata_inplace(path)
