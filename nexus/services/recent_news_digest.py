"""
Recent real headlines (last ~24h) for swarm warmer — RSS + optional GNews with images.

No extra dependencies beyond httpx + stdlib XML.
"""

from __future__ import annotations

import hashlib
import html as html_module
import json
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

# Hebrew / Israel–oriented feeds (public RSS). Scraped only from the central
# refresh job (``swarm.news_digest.refresh``) to avoid per-bot/per-worker bans.
DEFAULT_RSS_FEEDS: tuple[tuple[str, str], ...] = (
    ("https://feeds.ynet.co.il/rss/home", "ynet"),
    ("https://www.mako.co.il/rss/news-flash", "n12"),
    (
        "https://news.google.com/rss/search?q=%D7%99%D7%A9%D7%A8%D7%90%D7%9C&hl=iw&gl=IL&ceid=IL:iw",
        "google-news",
    ),
)

# Redis: single source of truth for swarm/news consumers (see ``refresh_central_news_digest_cache``).
NEWS_DIGEST_CACHE_KEY = "nexus:news:digest:bundle"
NEWS_DIGEST_UPDATED_AT_KEY = "nexus:news:digest:updated_at"
NEWS_DIGEST_HASH_KEY = "nexus:news:digest:content_hash"
NEWS_DIGEST_REFRESH_LOCK_KEY = "nexus:news:digest:refresh_lock"
SWARM_NEWS_DIGEST_CHANNEL = "nexus:swarm:news_digest"
NEWS_DIGEST_CACHE_TTL_SEC = 900


def _resolved_rss_feeds() -> tuple[tuple[str, str], ...]:
    """Ynet / N12 / Google News plus optional Telegram-news RSS (public mirror or aggregator)."""
    extra = (os.getenv("TELEGRAM_NEWS_RSS_URL") or "").strip()
    if not extra.startswith(("http://", "https://")):
        return DEFAULT_RSS_FEEDS
    label = (os.getenv("TELEGRAM_NEWS_RSS_LABEL") or "telegram-news").strip() or "telegram-news"
    return (*DEFAULT_RSS_FEEDS, (extra, label[:40]))


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
    feeds: tuple[tuple[str, str], ...] | None = None,
) -> list[NewsItem]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    merged: list[NewsItem] = []
    use_feeds = feeds if feeds is not None else _resolved_rss_feeds()
    for url, label in use_feeds:
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


# Trailing " - outlet" / " | outlet" stripped only when the tail matches a known
# attribution (avoids eating legitimate hyphenated headlines).
_DIGEST_OUTLET_TAILS: frozenset[str] = frozenset(
    {
        "ynet",
        "ynet.co.il",
        "ynetnews",
        "mako",
        "n12",
        "n12 news",
        "n12news",
        "channel 12",
        "calcalist",
        "globes",
        "haaretz",
        "walla",
        "walla news",
        "themarker",
        "the marker",
        "marker",
        "reuters",
        "bbc",
        "bbc news",
        "cnn",
        "cnn news",
        "bloomberg",
        "ap",
        "ap news",
        "associated press",
        "google news",
        "google-news",
        "googlenews",
        "gnews",
        "telegram-news",
        "telegram news",
        "israel hayom",
        "israelhayom",
        "כלכליסט",
        "גלובס",
        "הארץ",
        "וואלה",
        "דה מרקר",
        "ידיעות אחרונות",
        "ישראל היום",
        "חדשות 12",
        "ערוץ 12",
        "כאן",
        "כאן 11",
    }
)


def _normalize_outlet_tail_for_match(tail: str) -> str:
    t = " ".join((tail or "").split()).strip()
    if not t:
        return ""
    if any("\u0590" <= c <= "\u05FF" for c in t):
        return t
    return t.lower()


def _tail_looks_like_outlet_attribution(tail: str) -> bool:
    key = _normalize_outlet_tail_for_match(tail)
    if not key:
        return False
    if key in _DIGEST_OUTLET_TAILS:
        return True
    kl = key.lower()
    if kl in _DIGEST_OUTLET_TAILS:
        return True
    if re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,48}", kl) and re.search(
        r"\.(co\.il|com|net|org)\b", kl
    ):
        return True
    return False


_DIGEST_TRAILING_ATTRIB_RE = re.compile(
    r"\s*[-–—|]\s*(?P<tail>[^\n]+?)\s*$",
    re.UNICODE,
)


def _sanitize_event_headline(title: str) -> str:
    """
    Remove source brackets/tags and common trailing outlet suffixes from a single
    headline so digest lines read as plain events (no outlet labels).
    """
    s = html_module.unescape((title or "").strip())
    if not s:
        return ""
    for _ in range(10):
        prev = s
        s = re.sub(r"^\s*[-–—*•]+\s*", "", s)
        s = re.sub(r"^\s*\[[^\]]{1,120}\]\s*", "", s)
        s = re.sub(r"^\s*\([^)]{1,120}\)\s*", "", s)
        if s == prev:
            break
    for _ in range(4):
        m = _DIGEST_TRAILING_ATTRIB_RE.search(s)
        if not m:
            break
        tail = (m.group("tail") or "").strip()
        if not tail or len(tail) > 100:
            break
        if not _tail_looks_like_outlet_attribution(tail):
            break
        s = s[: m.start()].rstrip()
    s = re.sub(r"\s+", " ", s).strip()
    return s[:500] if s else ""


def _sanitize_digest_text_block(text: str) -> str:
    """Normalize multi-line digest (including legacy '- [src] title' cache rows)."""
    out: list[str] = []
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        line = re.sub(r"^[-–—*•]+\s*", "", line)
        clean = _sanitize_event_headline(line)
        if clean:
            out.append(clean)
    return "\n".join(out)


def _format_digest_lines(items: list[NewsItem], *, max_lines: int = 10) -> str:
    lines: list[str] = []
    for it in items[:max_lines]:
        clean = _sanitize_event_headline(it.title)
        if clean:
            lines.append(clean)
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
    Live HTTP fetch (RSS + optional GNews). Prefer ``get_tick_news_bundle_for_consumer``
    from swarm ticks so sources are hit only from ``swarm.news_digest.refresh``.
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

    anchor_headline = _sanitize_event_headline(anchor.title)

    return TickNewsBundle(
        digest_text=digest_text,
        anchor_title=anchor_headline,
        anchor_link=anchor.link,
        image_url=img_url,
    )


def tick_news_bundle_to_dict(bundle: TickNewsBundle) -> dict[str, str | None]:
    return {
        "digest_text": bundle.digest_text,
        "anchor_title": bundle.anchor_title,
        "anchor_link": bundle.anchor_link,
        "image_url": bundle.image_url,
    }


def tick_news_bundle_from_dict(data: dict[str, Any]) -> TickNewsBundle:
    raw_img = data.get("image_url")
    img: str | None = None
    if raw_img is not None:
        s = str(raw_img).strip()
        img = s or None
    digest_raw = str(data.get("digest_text") or "")
    anchor_raw = str(data.get("anchor_title") or "")
    return TickNewsBundle(
        digest_text=_sanitize_digest_text_block(digest_raw),
        anchor_title=_sanitize_event_headline(anchor_raw),
        anchor_link=str(data.get("anchor_link") or ""),
        image_url=img,
    )


async def refresh_central_news_digest_cache(redis: Any) -> dict[str, Any]:
    """
    One HTTP pass (Ynet/N12/Google + optional Telegram RSS + GNews), write Redis,
    publish ``nexus:swarm:news_digest`` when the digest payload changes.

    Uses a short distributed lock so overlapping workers do not hammer sources.
    """
    if redis is None:
        return {"status": "failed", "error": "redis_unavailable"}
    got = await redis.set(NEWS_DIGEST_REFRESH_LOCK_KEY, "1", nx=True, ex=55)
    if not got:
        return {"status": "skipped", "reason": "lock_held"}
    changed = False
    try:
        bundle = await build_tick_news_bundle()
        payload_dict = tick_news_bundle_to_dict(bundle)
        payload_json = json.dumps(payload_dict, ensure_ascii=False)
        digest_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()[:40]
        prev = await redis.get(NEWS_DIGEST_HASH_KEY)
        ts = datetime.now(timezone.utc).isoformat()
        await redis.set(NEWS_DIGEST_CACHE_KEY, payload_json, ex=NEWS_DIGEST_CACHE_TTL_SEC)
        await redis.set(NEWS_DIGEST_UPDATED_AT_KEY, ts, ex=NEWS_DIGEST_CACHE_TTL_SEC)
        await redis.set(NEWS_DIGEST_HASH_KEY, digest_hash, ex=NEWS_DIGEST_CACHE_TTL_SEC)
        changed = prev != digest_hash
        if changed:
            pub = {
                "schema": "nexus.swarm.news_digest.v1",
                "event": "news_digest_updated",
                "ts": ts,
                "digest_preview": (bundle.digest_text or "")[:500],
                "anchor_title": (bundle.anchor_title or "")[:500],
                "anchor_link": (bundle.anchor_link or "")[:2000],
            }
            msg = json.dumps(pub, ensure_ascii=False)
            await redis.publish(SWARM_NEWS_DIGEST_CHANNEL, msg)
            await redis.publish(
                "nexus:swarm:events",
                json.dumps({**pub, "engine": "news_digest"}, ensure_ascii=False),
            )
            try:
                from nexus.shared.swarm_signals import ingest_text_for_swarm

                blob = f"{bundle.anchor_title}\n{bundle.digest_text}"
                fp = digest_hash[:20]
                await ingest_text_for_swarm(redis, blob, fp)
            except Exception as exc:
                log.debug("news_digest_swarm_signal_failed", error=str(exc))
        return {"status": "ok", "changed": changed, "updated_at": ts}
    except Exception as exc:
        log.warning("news_digest_refresh_failed", error=str(exc))
        return {"status": "failed", "error": str(exc)}
    finally:
        try:
            await redis.delete(NEWS_DIGEST_REFRESH_LOCK_KEY)
        except Exception:
            pass


async def get_tick_news_bundle_for_consumer(redis: Any | None) -> TickNewsBundle:
    """
    Read cached digest (written by ``refresh_central_news_digest_cache``).
    On cache miss, tries one refresh (if Redis is available); never does a
    standalone live scrape when Redis is absent (caller may use ``build_tick_news_bundle``).
    """
    if redis is None:
        log.debug("news_digest_consumer_no_redis_fallback_live")
        return await build_tick_news_bundle()
    raw = await redis.get(NEWS_DIGEST_CACHE_KEY)
    if raw:
        try:
            return tick_news_bundle_from_dict(json.loads(raw))
        except Exception as exc:
            log.debug("news_digest_cache_parse_failed", error=str(exc))
    await refresh_central_news_digest_cache(redis)
    raw2 = await redis.get(NEWS_DIGEST_CACHE_KEY)
    if raw2:
        try:
            return tick_news_bundle_from_dict(json.loads(raw2))
        except Exception:
            pass
    return TickNewsBundle(digest_text="", anchor_title="", anchor_link="", image_url=None)


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
