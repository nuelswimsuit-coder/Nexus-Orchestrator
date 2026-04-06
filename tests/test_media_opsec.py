"""Tests for outbound image metadata strip + per-upload byte salt."""

from __future__ import annotations

import hashlib
from io import BytesIO

from PIL import Image
from PIL.PngImagePlugin import PngInfo

from nexus.services.media_opsec import (
    make_image_upload_salt_seed,
    prepare_jpeg_png_for_telegram_upload,
    telethon_voice_kwargs,
)


def test_make_image_upload_salt_seed_unique_per_call() -> None:
    a = make_image_upload_salt_seed("session_x")
    b = make_image_upload_salt_seed("session_x")
    assert a != b
    assert len(a) == 32


def test_jpeg_lsb_salt_changes_sha256() -> None:
    buf = BytesIO()
    im = Image.new("RGB", (64, 64))
    px = im.load()
    for row in range(64):
        for col in range(64):
            px[col, row] = (
                (row + col * 3) % 256,
                (row * 2 + col) % 256,
                (row + col * 2) % 256,
            )
    im.save(buf, format="JPEG", quality=92)
    raw = buf.getvalue()
    out_a, _ = prepare_jpeg_png_for_telegram_upload(raw, salt_seed=b"seed-one")
    out_b, _ = prepare_jpeg_png_for_telegram_upload(raw, salt_seed=b"seed-two")
    assert hashlib.sha256(out_a).hexdigest() != hashlib.sha256(out_b).hexdigest()


def test_jpeg_output_has_no_exif() -> None:
    buf = BytesIO()
    Image.new("RGB", (5, 5), (1, 2, 3)).save(buf, format="JPEG", quality=88)
    raw = buf.getvalue()
    out, _ = prepare_jpeg_png_for_telegram_upload(raw, salt_seed=b"x")
    im = Image.open(BytesIO(out))
    try:
        ex = im.getexif()
        assert len(ex) == 0
    finally:
        im.close()


def test_png_strips_text_chunk_and_salt_changes_hash() -> None:
    pnginfo = PngInfo()
    pnginfo.add_text("Software", "sensitive-marker")
    buf = BytesIO()
    Image.new("RGBA", (4, 4), (10, 20, 30, 255)).save(
        buf, format="PNG", pnginfo=pnginfo, optimize=False
    )
    raw = buf.getvalue()
    assert b"sensitive-marker" in raw
    out, _ = prepare_jpeg_png_for_telegram_upload(raw, salt_seed=b"a")
    assert b"sensitive-marker" not in out
    out2, _ = prepare_jpeg_png_for_telegram_upload(raw, salt_seed=b"b")
    assert hashlib.sha256(out).digest() != hashlib.sha256(out2).digest()
    im = Image.open(BytesIO(out))
    try:
        assert "Software" not in im.info
    finally:
        im.close()


def test_telethon_voice_kwargs() -> None:
    kw = telethon_voice_kwargs(12)
    assert kw["voice_note"] is True
    attrs = kw["attributes"]
    assert len(attrs) == 1
    assert attrs[0].voice is True
    assert attrs[0].duration == 12


def test_ogg_helpers_import_mutagen() -> None:
    from nexus.services.media_opsec import ogg_voice_duration_seconds, strip_ogg_metadata

    assert ogg_voice_duration_seconds(b"") == 1
    assert strip_ogg_metadata(b"") == b""
