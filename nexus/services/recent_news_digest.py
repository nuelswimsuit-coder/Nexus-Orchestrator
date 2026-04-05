"""
Recent real headlines (last ~24h) for swarm warmer — RSS + optional GNews with images.

No extra dependencies beyond httpx + stdlib XML.
"""

from __future__ import annotations

import html as html_module
import os
import random
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
import structlog

log = structlog.get_logger(__name__)

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
}

# Hebrew / Israel–oriented feeds (public RSS)
DEFAULT_RSS_FEEDS: tuple[tuple[str, str], ...] = (
    ("https://feeds.ynet.co.il/rss/home", "ynet"),
    (
        "https://news.google.com/rss/search?q=%D7%99%D7%A9%D7%A8%D7%90%D7%9C&hl=iw&gl=IL&ceid=IL:iw",
        "google-news",
    ),
)


@dataclass
class NewsItem:
    title: str
    source: str
    link: str
    published: datetime | None
    image_url: str | None = None


def _parse_pub_date(raw: str) -> datetime | None:
    t = (raw or "").strip()
    if not t:
        return None
    try:
        dt = parsedate_to_datetime(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    try:
        return datetime.fromisoformat(t.replace("Z", "+00:00"))
    except Exception:
        return None


def _strip_ns(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _child_by_local(parent: Any, *local_names: str) -> Any:
    want = {n.lower() for n in local_names}
    for ch in list(parent):
        if _strip_ns(ch.tag).lower() in want:
            return ch
    return None


def _rss_items_from_xml(xml_text: str, source_label: str) -> list[NewsItem]:
    out: list[NewsItem] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return out

    items: list[Any] = []
    if _strip_ns(root.tag).lower() == "rss":
        channel = None
        for ch in root:
            if _strip_ns(ch.tag).lower() == "channel":
                channel = ch
                break
        if channel is not None:
            for el in channel.iter():
                if _strip_ns(el.tag).lower() == "item":
                    items.append(el)
    else:
        for el in root.iter():
            if _strip_ns(el.tag).lower() == "item":
                items.append(el)

    for item in items:
        title_el = _child_by_local(item, "title")
        link_el = _child_by_local(item, "link")
        pub_el = _child_by_local(item, "pubDate", "published", "updated")
        title = (title_el.text or "").strip() if title_el is not None and title_el.text else ""
        link = ""
        if link_el is not None:
            link = (link_el.text or "").strip()
            if not link:
                link = (link_el.get("href") or "").strip()
        pub_raw = (pub_el.text or "").strip() if pub_el is not None and pub_el.text else ""
        if not title:
            continue
        img: str | None = None
        for child in list(item):
            tag = _strip_ns(child.tag).lower()
            if tag == "enclosure" and (child.get("type") or "").startswith("image"):
                u = (child.get("url") or "").strip()
                if u:
                    img = u
                    break
            # Namespaced MRSS tags become local names ``content`` / ``thumbnail`` after NS strip.
            if tag in ("content", "thumbnail", "media:content", "media:thumbnail"):
                u = (child.get("url") or "").strip()
                if u:
                    img = u
                    break
        out.append(
            NewsItem(
                title=title[:500],
                source=source_label,
                link=link[:2000],
                published=_parse_pub_date(pub_raw),
                image_url=img,
            )
        )
    return out


async def _fetch_rss_digest(
    client: httpx.AsyncClient,
    *,
    max_age_hours: int = 24,
    max_lines: int = 12,
) -> list[NewsItem]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    merged: list[NewsItem] = []
    for url, label in DEFAULT_RSS_FEEDS:
        try:
            r = await client.get(url, follow_redirects=True, timeout=15.0)
            r.raise_for_status()
            merged.extend(_rss_items_from_xml(r.text, label))
        except Exception as exc:
            log.debug("rss_fetch_failed", url=url, error=str(exc))

    fresh: list[NewsItem] = []
    for it in merged:
        if it.published is None or it.published >= cutoff:
            fresh.append(it)
    if not fresh:
        fresh = merged[: max_lines * 2]
    # de-dupe by title lower
    seen: set[str] = set()
    uniq: list[NewsItem] = []
    for it in fresh:
        k = it.title.lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(it)
    return uniq[: max_lines * 2]


async def _fetch_gnews_items(
    client: httpx.AsyncClient,
    *,
    max_items: int = 8,
) -> list[NewsItem]:
    key = (os.getenv("GNEWS_API_KEY") or "").strip()
    if not key:
        return []
    try:
        r = await client.get(
            "https://gnews.io/api/v4/top-headlines",
            params={
                "apikey": key,
                "lang": "he",
                "country": "il",
                "max": max_items,
            },
            timeout=15.0,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        log.debug("gnews_fetch_failed", error=str(exc))
        return []

    arts = data.get("articles") if isinstance(data, dict) else None
    if not isinstance(arts, list):
        return []
    out: list[NewsItem] = []
    for a in arts:
        if not isinstance(a, dict):
            continue
        title = str(a.get("title") or "").strip()
        if not title:
            continue
        pub = _parse_pub_date(str(a.get("publishedAt") or ""))
        img = str(a.get("image") or "").strip() or None
        out.append(
            NewsItem(
                title=title[:500],
                source="gnews",
                link=str(a.get("url") or "")[:2000],
                published=pub,
                image_url=img,
            )
        )
    return out


def _format_digest_lines(items: list[NewsItem], *, max_lines: int = 10) -> str:
    lines: list[str] = []
    for it in items[:max_lines]:
        src = it.source
        lines.append(f"- [{src}] {it.title}")
    return "\n".join(lines)


@dataclass
class TickNewsBundle:
    """What the warmer passes to Gemini + optional photo send."""

    digest_text: str
    anchor_title: str
    anchor_link: str
    image_url: str | None


async def build_tick_news_bundle(
    *,
    max_age_hours: int = 24,
    digest_lines: int = 10,
) -> TickNewsBundle:
    """
    Pull recent headlines; prefer GNews rows (often include ``image``) when key is set.
    """
    async with httpx.AsyncClient(headers=_BROWSER_HEADERS) as client:
        g_items = await _fetch_gnews_items(client, max_items=10)
        r_items = await _fetch_rss_digest(client, max_age_hours=max_age_hours, max_lines=digest_lines)

    pool: list[NewsItem] = []
    if g_items:
        pool.extend(g_items)
    pool.extend(r_items)

    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    fresh = [x for x in pool if x.published is None or x.published >= cutoff]
    if not fresh:
        fresh = pool

    digest_src = fresh[:digest_lines] if fresh else []
    digest_text = _format_digest_lines(digest_src, max_lines=digest_lines)

    with_image = [x for x in fresh if x.image_url]
    pick_pool = with_image if with_image else fresh
    if not pick_pool:
        return TickNewsBundle(
            digest_text="",
            anchor_title="",
            anchor_link="",
            image_url=None,
        )

    anchor = random.choice(pick_pool)
    img_url = anchor.image_url
    if not img_url and (anchor.link or "").strip().startswith(("http://", "https://")):
        try:
            async with httpx.AsyncClient(
                timeout=14.0, follow_redirects=True, headers=_BROWSER_HEADERS
            ) as og_client:
                img_url = await _try_resolve_og_image_url(og_client, anchor.link.strip())
        except Exception as exc:
            log.debug("og_image_resolve_failed", error=str(exc))

    return TickNewsBundle(
        digest_text=digest_text,
        anchor_title=anchor.title,
        anchor_link=anchor.link,
        image_url=img_url,
    )


def telegram_image_filename_from_bytes(data: bytes) -> str:
    """Filename hint for Telethon ``send_file`` when uploading from memory."""
    if len(data) >= 3 and data[:3] == b"\xff\xd8\xff":
        return "photo.jpg"
    if len(data) >= 8 and data[:8] == b"\x89PNG\r\n\x1a\n":
        return "photo.png"
    if len(data) >= 6 and data[:6] in (b"GIF87a", b"GIF89a"):
        return "photo.gif"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "photo.webp"
    return "photo.jpg"


def append_article_link_to_text(
    text: str,
    link: str,
    *,
    title: str | None = None,
    max_total: int = 1024,
) -> tuple[str, str | None]:
    """
    Append article under the chat line (Telegram message/caption limit).

    When ``title`` is non-empty, formats the link as Telegram HTML so the group
    sees a short clickable headline instead of a raw URL. Returns
    ``(message, parse_mode)`` where ``parse_mode`` is ``\"html\"`` or ``None``.
    """
    u = (link or "").strip()
    if not u or not u.startswith(("http://", "https://")):
        return (text or "").strip()[:max_total], None
    base = (text or "").strip()
    if u in base:
        return base[:max_total], None

    label = (title or "").strip()
    if label:
        href_esc = html_module.escape(u, quote=True)
        label_esc = html_module.escape(label)
        suffix = f'\n<a href="{href_esc}">{label_esc}</a>'
        b = base
        while True:
            safe_b = html_module.escape(b)
            piece = f"{safe_b}{suffix}" if safe_b else suffix.lstrip("\n")
            if len(piece) <= max_total:
                return piece[:max_total], "html"
            if not b:
                break
            b = b[:-1]

    suffix = f"\n{u}"
    if len(base) + len(suffix) <= max_total:
        return (base + suffix)[:max_total], None
    room = max_total - len(suffix)
    if room < 12:
        return u[:max_total], None
    return (base[:room].rstrip() + suffix)[:max_total], None


_OG_IMAGE_PATTERNS = (
    r'property=["\']og:image["\'][^>]*content=["\']([^"\']+)["\']',
    r'content=["\']([^"\']+)["\'][^>]*property=["\']og:image["\']',
    r'name=["\']twitter:image["\'][^>]*content=["\']([^"\']+)["\']',
)


async def _try_resolve_og_image_url(client: httpx.AsyncClient, page_url: str) -> str | None:
    """Best-effort og/twitter image from article HTML (RSS often has no per-item image)."""
    try:
        r = await client.get(page_url)
        r.raise_for_status()
    except Exception:
        return None
    ct = (r.headers.get("content-type") or "").lower()
    if "html" not in ct and "xml" not in ct:
        return None
    body = r.text[:500_000]
    for pat in _OG_IMAGE_PATTERNS:
        m = re.search(pat, body, flags=re.I)
        if m:
            u = html_module.unescape(m.group(1)).strip()
            if u.startswith(("http://", "https://")):
                return u
    return None


async def download_image_bytes(url: str, *, max_bytes: int = 3_500_000) -> bytes | None:
    if not url or not url.startswith(("http://", "https://")):
        return None
    try:
        async with httpx.AsyncClient(
            timeout=20.0, follow_redirects=True, headers=_BROWSER_HEADERS
        ) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.content
            if len(data) > max_bytes:
                return None
            if len(data) < 256:
                return None
            # reject obvious HTML error pages
            head = data[:64].lower()
            if b"<html" in head or b"<!doctype" in head:
                return None
            return data
    except Exception as exc:
        log.debug("news_image_download_failed", error=str(exc))
        return None
