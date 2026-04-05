"""
Telegram (Telethon) message → human-readable text for UI transcripts.

Media-only messages get a concrete Hebrew label instead of a generic placeholder.
"""

from __future__ import annotations

from typing import Any


def telethon_media_kind_and_hint(m: Any) -> tuple[str | None, str]:
    """
    Return (kind_for_api, hebrew_hint).

    ``kind_for_api`` is a short machine token for the frontend; ``hebrew_hint`` is shown as text.
    """
    media = getattr(m, "media", None)
    if media is None:
        return None, ""

    try:
        from telethon.tl.types import (  # type: ignore[import-untyped]
            DocumentAttributeAudio,
            DocumentAttributeFilename,
            DocumentAttributeVideo,
            MessageMediaContact,
            MessageMediaGeo,
            MessageMediaPhoto,
            MessageMediaPoll,
            MessageMediaUnsupported,
            MessageMediaWebPage,
        )
    except ImportError:
        return "unknown", "[מדיה]"

    if isinstance(media, MessageMediaUnsupported):
        return "unsupported", "[מדיה לא נתמכת]"

    if isinstance(media, MessageMediaPhoto):
        return "photo", "📷 תמונה"

    if isinstance(media, MessageMediaGeo):
        return "geo", "📍 מיקום"

    if isinstance(media, MessageMediaContact):
        fn = str(getattr(media, "first_name", "") or "").strip()
        return "contact", f"👤 איש קשר{f' ({fn})' if fn else ''}"

    if isinstance(media, MessageMediaPoll):
        return "poll", "📊 סקר"

    if isinstance(media, MessageMediaWebPage):
        wp = media.webpage
        title = ""
        if wp is not None:
            title = str(getattr(wp, "title", "") or getattr(wp, "site_name", "") or "").strip()
        if title:
            return "webpage", f"🔗 קישור: {title[:120]}"
        return "webpage", "🔗 תצוגת קישור"

    doc = getattr(media, "document", None)
    if doc is not None:
        fname = ""
        for attr in getattr(doc, "attributes", None) or []:
            if isinstance(attr, DocumentAttributeFilename):
                fname = str(attr.file_name or "").strip()
                break
        for attr in getattr(doc, "attributes", None) or []:
            if isinstance(attr, DocumentAttributeVideo):
                sec = int(getattr(attr, "duration", 0) or 0)
                label = f"🎥 סרטון ({sec}s)" if sec else "🎥 סרטון"
                if fname:
                    return "video", f"{label} — {fname[:80]}"
                return "video", label
            if isinstance(attr, DocumentAttributeAudio):
                if getattr(attr, "voice", False):
                    return "voice", "🎤 הודעת קול"
                return "audio", f"🎵 אודיו{f' — {fname[:80]}' if fname else ''}"
        if fname:
            return "file", f"📎 {fname[:120]}"
        mime = str(getattr(doc, "mime_type", "") or "").strip()
        if mime.startswith("image/"):
            return "image", f"📷 תמונה ({mime})"
        if mime.startswith("video/"):
            return "video", f"🎥 וידאו ({mime})"
        return "document", f"📎 קובץ{f' ({mime})' if mime else ''}"

    return "unknown", "[מדיה]"


def telethon_display_text(m: Any) -> str:
    """Caption if present, else media description, else generic placeholder."""
    raw = (getattr(m, "message", None) or getattr(m, "raw_text", None) or "") or ""
    text = str(raw).strip()
    if text:
        return text
    _kind, hint = telethon_media_kind_and_hint(m)
    return hint if hint else "[מדיה / ללא טקסט]"
