"""
Branding Studio & Media Tools
Covers tools 13–18 of the Branding/Media category:
  /emoji_gen  — convert uploaded image to a Telegram premium emoji sticker set
  /sticker    — convert MP4 → WebM animated sticker (ffmpeg)
  /watermark  — overlay logo/text watermark on an image
  /compress   — smart media compressor (Pillow + ffmpeg)
  /grid       — split image into 3×3 Instagram grid (9 tiles)
  /resize     — bulk-resize images to a target resolution
"""
from __future__ import annotations

import asyncio
import io
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Callable

import structlog

log = structlog.get_logger(__name__)
ROOT = Path(__file__).resolve().parents[3]


# ── Helpers ────────────────────────────────────────────────────────────────────

def _pil():
    """Lazy Pillow import with a friendly error."""
    try:
        from PIL import Image, ImageDraw, ImageFont
        return Image, ImageDraw, ImageFont
    except ImportError as exc:
        raise RuntimeError("Pillow לא מותקן. הרץ: pip install Pillow") from exc


def _ffmpeg_path() -> str:
    """Return ffmpeg path (env override or system PATH)."""
    return os.environ.get("FFMPEG_PATH", "ffmpeg")


async def _run_ffmpeg(*args: str, timeout: int = 120) -> tuple[int, str]:
    """Run ffmpeg asynchronously and return (returncode, stderr)."""
    proc = await asyncio.create_subprocess_exec(
        _ffmpeg_path(), *args,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return proc.returncode or 0, (stderr or b"").decode("utf-8", errors="replace")
    except asyncio.TimeoutError:
        proc.kill()
        return -1, "timeout"


# ── Emoji Set Generator ────────────────────────────────────────────────────────

async def generate_emoji_set(
    image_bytes: bytes,
    label: str = "brand",
    out_dir: str | None = None,
) -> dict:
    """
    Resize a source image to 8 standard Telegram emoji sizes and save as PNGs.
    Returns a dict with file paths and any errors.

    Telegram custom emoji must be exactly 100×100 px (static) or a WebM ≤256×256.
    We produce a grid of variants at [32, 64, 100, 128, 160, 256, 512] px.
    """
    Image, _, _ = _pil()
    sizes   = [32, 64, 100, 128, 160, 256, 512]
    out_dir = Path(out_dir or (ROOT / "data" / "emoji_sets" / label))
    out_dir.mkdir(parents=True, exist_ok=True)
    result  = {"label": label, "files": [], "error": None}

    try:
        src = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
        for sz in sizes:
            img  = src.resize((sz, sz), Image.LANCZOS)
            dest = out_dir / f"{label}_{sz}.png"
            img.save(dest, "PNG", optimize=True)
            result["files"].append(str(dest))
        result["main_100"] = str(out_dir / f"{label}_100.png")
    except Exception as exc:
        result["error"] = str(exc)[:200]
    return result


def format_emoji_gen(r: dict) -> str:
    if r.get("error"):
        return f"❌ *Emoji Generator — שגיאה*\n_{r['error']}_"
    n = len(r.get("files", []))
    return (
        f"🎨 *Emoji Set — {r.get('label')}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ נוצרו {n} גדלים\n"
        f"📁 `{Path(r.get('main_100','')).parent}`\n"
        f"🔑 קובץ ראשי \\(100px\\): `{Path(r.get('main_100','')).name}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"_העלה את ה\\-PNG ל\\-BotFather → /newemojipack_"
    )


# ── MP4 → WebM Sticker Converter ──────────────────────────────────────────────

async def convert_to_webm_sticker(
    input_path: str,
    out_path: str | None = None,
    max_side: int = 512,
) -> dict:
    """
    Convert MP4/GIF/MOV to a Telegram-compliant WebM animated sticker.
    Specs: VP9, ≤512×512, ≤3 seconds, ≤256 KB, 30 fps, no audio.
    """
    inp  = Path(input_path)
    out  = Path(out_path or inp.with_suffix(".webm"))
    result = {"input": str(inp), "output": str(out), "error": None, "size_kb": None}

    vf = (
        f"scale='if(gt(iw,ih),{max_side},-2)':'if(gt(ih,iw),{max_side},-2)',"
        f"fps=30,format=yuva420p"
    )
    code, err = await _run_ffmpeg(
        "-y", "-i", str(inp),
        "-t", "3",            # max 3 s
        "-vf", vf,
        "-c:v", "libvpx-vp9",
        "-b:v", "200k",
        "-crf", "30",
        "-an",                # no audio
        "-fs", "262144",      # 256 KB hard cap
        str(out),
    )
    if code != 0:
        result["error"] = err[:200]
    elif out.exists():
        result["size_kb"] = round(out.stat().st_size / 1024, 1)
    return result


def format_sticker(r: dict) -> str:
    if r.get("error"):
        return f"❌ *WebM Sticker — שגיאה*\n`{r['error']}`"
    return (
        f"🎬 *WebM Sticker — הצלחה*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📂 קובץ: `{Path(r['output']).name}`\n"
        f"📦 גודל: *{r['size_kb']} KB*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"_העלה ל\\-BotFather → /newsticker_"
    )


# ── Watermark ─────────────────────────────────────────────────────────────────

def apply_watermark(
    image_bytes: bytes,
    text: str | None = None,
    logo_path: str | None = None,
    opacity: float = 0.4,
    position: str = "bottom_right",   # topleft / topright / bottomleft / bottomright / center
) -> bytes:
    """
    Overlay a text or logo watermark on the image.
    Returns the watermarked image as PNG bytes.
    """
    Image, ImageDraw, ImageFont = _pil()
    base = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
    W, H = base.size

    overlay = Image.new("RGBA", base.size, (0, 0, 0, 0))

    if text:
        draw   = ImageDraw.Draw(overlay)
        font_s = max(20, W // 25)
        try:
            font = ImageFont.truetype("arial.ttf", font_s)
        except Exception:
            font = ImageFont.load_default()

        bbox = draw.textbbox((0, 0), text, font=font)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        pad = 15
        positions = {
            "topleft":     (pad, pad),
            "topright":    (W - tw - pad, pad),
            "bottomleft":  (pad, H - th - pad),
            "bottomright": (W - tw - pad, H - th - pad),
            "center":      ((W - tw) // 2, (H - th) // 2),
        }
        x, y = positions.get(position, positions["bottomright"])
        alpha = int(255 * opacity)
        draw.text((x + 2, y + 2), text, font=font, fill=(0,   0,   0,   alpha))
        draw.text((x,     y    ), text, font=font, fill=(255, 255, 255, alpha))

    elif logo_path:
        logo = Image.open(logo_path).convert("RGBA")
        max_logo = W // 5
        logo.thumbnail((max_logo, max_logo), Image.LANCZOS)
        lw, lh = logo.size
        pad = 15
        positions = {
            "topleft":     (pad, pad),
            "topright":    (W - lw - pad, pad),
            "bottomleft":  (pad, H - lh - pad),
            "bottomright": (W - lw - pad, H - lh - pad),
            "center":      ((W - lw) // 2, (H - lh) // 2),
        }
        x, y = positions.get(position, positions["bottomright"])
        r, g, b, a = logo.split()
        a = a.point(lambda p: int(p * opacity))
        logo.putalpha(a)
        overlay.paste(logo, (x, y), logo)

    composite = Image.alpha_composite(base, overlay).convert("RGB")
    buf = io.BytesIO()
    composite.save(buf, "JPEG", quality=92)
    return buf.getvalue()


# ── Smart Compressor ──────────────────────────────────────────────────────────

async def compress_media(
    input_path: str,
    target_kb: int = 500,
    out_path: str | None = None,
) -> dict:
    """
    Smart compress: uses Pillow for images, ffmpeg for video.
    Iteratively reduces quality/bitrate until target_kb is met.
    """
    inp = Path(input_path)
    if not inp.exists():
        return {"error": f"קובץ לא נמצא: {inp}"}

    suffix = inp.suffix.lower()
    out    = Path(out_path or inp.parent / f"{inp.stem}_compressed{suffix}")
    result = {"input": str(inp), "output": str(out), "error": None,
              "original_kb": round(inp.stat().st_size / 1024, 1), "output_kb": None}

    if suffix in {".jpg", ".jpeg", ".png", ".webp"}:
        # Pillow iterative quality reduction
        Image, _, _ = _pil()
        img = Image.open(str(inp)).convert("RGB")
        for q in range(88, 30, -5):
            buf = io.BytesIO()
            img.save(buf, "JPEG", quality=q, optimize=True)
            if buf.tell() / 1024 <= target_kb:
                out.write_bytes(buf.getvalue())
                result["output_kb"] = round(out.stat().st_size / 1024, 1)
                return result
        # Last-resort: resize to 70%
        w, h = img.size
        img   = img.resize((int(w * 0.7), int(h * 0.7)), Image.LANCZOS)
        buf   = io.BytesIO()
        img.save(buf, "JPEG", quality=55, optimize=True)
        out.write_bytes(buf.getvalue())

    elif suffix in {".mp4", ".mov", ".avi", ".mkv"}:
        # ffmpeg 2-pass CRF reduction
        for crf in (28, 32, 36, 40):
            code, err = await _run_ffmpeg(
                "-y", "-i", str(inp),
                "-c:v", "libx264", "-crf", str(crf),
                "-preset", "fast", "-c:a", "aac", "-b:a", "96k",
                str(out),
            )
            if code == 0 and out.exists():
                if out.stat().st_size / 1024 <= target_kb:
                    break
        if code != 0:
            result["error"] = err[:200]
            return result
    else:
        result["error"] = f"סוג קובץ לא נתמך: {suffix}"
        return result

    if out.exists():
        result["output_kb"] = round(out.stat().st_size / 1024, 1)
    return result


def format_compress(r: dict) -> str:
    if r.get("error"):
        return f"❌ *Compressor — שגיאה*\n_{r['error']}_"
    saved = round((r["original_kb"] - r["output_kb"]) / r["original_kb"] * 100, 1)
    return (
        f"🗜 *Smart Compressor*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📥 מקור:  {r['original_kb']} KB\n"
        f"📤 פלט:   *{r['output_kb']} KB*\n"
        f"💾 חסכון: *{saved}%*\n"
        f"📂 `{Path(r['output']).name}`"
    )


# ── Instagram 3×3 Grid Splitter ───────────────────────────────────────────────

def split_instagram_grid(
    image_bytes: bytes,
    label: str = "grid",
    out_dir: str | None = None,
) -> dict:
    """
    Split an image into a 3×3 grid of 9 equal tiles (left→right, top→bottom).
    Each tile is saved as {label}_R{row}C{col}.jpg.
    Instagram posts in reverse order: tile 9 first, tile 1 last.
    """
    Image, _, _ = _pil()
    out_dir = Path(out_dir or (ROOT / "data" / "grids" / label))
    out_dir.mkdir(parents=True, exist_ok=True)
    result  = {"label": label, "tiles": [], "post_order": [], "error": None}

    try:
        src  = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        W, H = src.size
        # Make it square by cropping the longer side
        side  = min(W, H)
        left  = (W - side) // 2
        top   = (H - side) // 2
        src   = src.crop((left, top, left + side, top + side))

        tile  = side // 3
        files = []
        for row in range(3):
            for col in range(3):
                box   = (col * tile, row * tile, (col + 1) * tile, (row + 1) * tile)
                piece = src.crop(box)
                name  = f"{label}_R{row+1}C{col+1}.jpg"
                dest  = out_dir / name
                piece.save(dest, "JPEG", quality=95)
                files.append(str(dest))

        result["tiles"]      = files
        result["post_order"] = list(reversed(files))   # IG posts bottom-right first
        result["tile_px"]    = tile
    except Exception as exc:
        result["error"] = str(exc)[:200]

    return result


def format_grid(r: dict) -> str:
    if r.get("error"):
        return f"❌ *Grid Splitter — שגיאה*\n_{r['error']}_"
    return (
        f"🖼 *Instagram Grid — {r.get('label')}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ 9 טייל ✓ · {r.get('tile_px')}px כ/א\n"
        f"📁 `{Path(r['tiles'][0]).parent}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📋 *סדר העלאה לאינסטגרם \\(מהאחרון לראשון\\):*\n"
        + "\n".join(
            f"  {i+1}\\. `{Path(p).name}`"
            for i, p in enumerate(r.get("post_order", []))
        )
    )


# ── Bulk Resize ───────────────────────────────────────────────────────────────

def bulk_resize(
    image_bytes_list: list[bytes],
    width: int,
    height: int,
    labels: list[str] | None = None,
    out_dir: str | None = None,
) -> dict:
    """Resize a list of images to WxH. Returns list of output paths."""
    Image, _, _ = _pil()
    out_dir = Path(out_dir or (ROOT / "data" / "resized"))
    out_dir.mkdir(parents=True, exist_ok=True)
    result  = {"count": len(image_bytes_list), "files": [], "error": None}

    for i, data in enumerate(image_bytes_list):
        label = (labels[i] if labels and i < len(labels) else f"img_{i+1:03d}")
        try:
            img  = Image.open(io.BytesIO(data)).convert("RGB")
            img  = img.resize((width, height), Image.LANCZOS)
            dest = out_dir / f"{label}_{width}x{height}.jpg"
            img.save(dest, "JPEG", quality=92)
            result["files"].append(str(dest))
        except Exception as exc:
            result["files"].append(f"ERROR:{str(exc)[:60]}")

    return result
