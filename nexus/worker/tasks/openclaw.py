"""
openclaw.browser_scrape — Browser-based lead extraction for the Linux Worker.

OpenClaw is the Nexus browser scraping engine.  It is dispatched by the Scout
when a Telegram-only search cycle returns low-yield results (< LOW_YIELD_THRESHOLD
unique users).  It runs exclusively on Linux workers (requires_capabilities=["linux-only"])
because it uses Playwright headless Chromium.

Supported scrape modes
----------------------
google_maps
    Extract business leads (name, phone, address, website, rating) from a
    Google Maps search query.  Intended for local-business niches such as
    "House of Exhaust" — car repair shops, garages, exhaust specialists.

social_forums
    Scrape complaint threads from Reddit, Stack Overflow, and similar forums
    for users describing a specific software problem.  Intended for niches
    like "Management Ahu" — users frustrated with a specific tool are warm
    leads for an alternative.

Lead enrichment
---------------
After raw leads are collected, OpenClaw checks each lead for an active
Telegram account before writing it to telefix.db.  This is done via the
Telethon `ResolveUsernameRequest` / phone-lookup approach:

  1. If the lead has a phone number → try Telethon `ImportContacts` to
     resolve it to a Telegram user_id.
  2. If the lead has a username (e.g. from a forum profile) → try
     `ResolveUsernameRequest`.
  3. Leads that resolve successfully are written to telefix.db with
     source="openclaw" and a `telegram_id` field.
  4. Leads that do not resolve are stored in a separate
     `nexus:openclaw:unverified:<project_id>` Redis list for later retry.

Task parameters
---------------
{
    "mode":       "google_maps" | "social_forums",
    "query":      str,          # e.g. "exhaust repair near me" or "management ahu bug"
    "project_id": str,          # telefix project to write leads into
    "max_leads":  int,          # default 50
    "location":   str,          # optional, for google_maps (e.g. "Tel Aviv")
}

Output
------
{
    "status":           "completed" | "failed" | "low_resources",
    "mode":             str,
    "leads_found":      int,
    "leads_verified":   int,   # have active Telegram accounts
    "leads_written":    int,   # written to telefix.db
    "leads_unverified": int,   # stored in Redis for retry
    "duration_s":       float,
}
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import time
from itertools import cycle
from datetime import datetime, timezone
from typing import Any

import psutil
import redis.asyncio as redis
import structlog

from nexus.shared.config import settings
from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

TELEFIX_PROJECT  = os.getenv("TELEFIX_PROJECT", r"C:\Users\Yarin\Desktop\Mangement Ahu")
TELEFIX_DB       = os.path.join(TELEFIX_PROJECT, "data", "telefix.db")

CPU_THRESHOLD    = float(os.getenv("OPENCLAW_CPU_THRESHOLD", "60"))
MAX_LEADS_CAP    = int(os.getenv("OPENCLAW_MAX_LEADS", "100"))

# Redis keys
OPENCLAW_STATUS_KEY     = "nexus:openclaw:status"
OPENCLAW_UNVERIFIED_KEY = "nexus:openclaw:unverified:{project_id}"
OPENCLAW_STATUS_TTL     = 3600  # 1 hour

# Telegram / news pipeline → 5m scalper (score 0–10, higher = more bullish)
OPENCLAW_NEWS_SENTIMENT_KEY = "nexus:openclaw:news_sentiment"
OPENCLAW_NEWS_SENTIMENT_TTL = 900  # 15 minutes

# Playwright timeout (ms)
PW_TIMEOUT = 30_000

# Google Maps selectors (as of 2025 — may need updating)
GMAPS_RESULT_SELECTOR  = 'div[role="feed"] > div[jsaction]'
GMAPS_NAME_SELECTOR    = "h3.fontHeadlineSmall"
GMAPS_PHONE_SELECTOR   = 'button[data-tooltip="Copy phone number"] span'
GMAPS_ADDR_SELECTOR    = 'button[data-tooltip="Copy address"] span'
GMAPS_RATING_SELECTOR  = 'span[aria-label*="stars"]'

# Forum search templates
FORUM_SOURCES = {
    "reddit":     "https://www.reddit.com/search/?q={query}&sort=new",
    "stackoverflow": "https://stackoverflow.com/search?q={query}",
}


async def _set_node_intent(intent: str) -> None:
    """Best-effort intent broadcast to Redis for live node dashboards."""
    node_id = settings.node_id or os.getenv("NODE_ID", "master")
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        await client.set(f"node:{node_id}:intent", intent)
        await client.set("node:intent", intent)
    except Exception as exc:
        log.debug("openclaw_intent_publish_failed", error=str(exc))
    finally:
        await client.aclose()


async def _set_node_vision(vision: str) -> None:
    """Best-effort near-term vision broadcast to Redis for worker dashboards."""
    node_id = settings.node_id or os.getenv("NODE_ID", "master")
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        await client.set(f"node:{node_id}:vision", vision)
        await client.set("node:vision", vision)
    except Exception as exc:
        log.debug("openclaw_vision_publish_failed", error=str(exc))
    finally:
        await client.aclose()


def _predict_openclaw_vision(mode: str, leads_found: int = 0, verified: int = 0) -> str:
    """Heuristic forecast of what OpenClaw will focus on in the next 5 minutes."""
    if mode == "google_maps":
        if leads_found <= 0:
            return "Next 5m: expand map crawl depth to discover fresh business leads."
        if verified <= 0:
            return "Next 5m: verify scraped map contacts against Telegram identity graph."
        return "Next 5m: persist verified map leads and queue unverified retries."

    if mode == "social_forums":
        if leads_found <= 0:
            return "Next 5m: deepen forum search to capture complaint-driven prospects."
        if verified <= 0:
            return "Next 5m: resolve forum usernames and map them to Telegram identities."
        return "Next 5m: enrich verified forum prospects and write into TeleFix datastore."

    return "Next 5m: coordinate scraping, verification, and persistence pipelines."


async def _publish_openclaw_status(
    *,
    active: bool,
    stage: str,
    detail: str,
    mode: str = "",
    query: str = "",
) -> None:
    """
    Broadcast OpenClaw runtime health to Redis for dashboard/Telegram surfaces.
    """
    node_id = settings.node_id or os.getenv("NODE_ID", "master")
    client = redis.from_url(settings.redis_url, decode_responses=True)
    proc = psutil.Process()
    try:
        payload = {
            "module": "openclaw",
            "active": active,
            "stage": stage,
            "detail": detail,
            "mode": mode,
            "query": query,
            "node_id": node_id,
            "cpu_percent": round(proc.cpu_percent(interval=None), 2),
            "rss_mb": round(proc.memory_info().rss / (1024 * 1024), 2),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        await client.set(OPENCLAW_STATUS_KEY, json.dumps(payload), ex=OPENCLAW_STATUS_TTL)
    except Exception as exc:
        log.debug("openclaw_status_publish_failed", error=str(exc))
    finally:
        await client.aclose()


async def _push_node_history(task_line: str) -> None:
    """Keep a rolling node-local history list for terminal monitors."""
    node_id = settings.node_id or os.getenv("NODE_ID", "master")
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        await client.lpush(f"node:{node_id}:history", task_line)
        await client.ltrim(f"node:{node_id}:history", 0, 4)
        await client.lpush("node:history", task_line)
        await client.ltrim("node:history", 0, 4)
    except Exception as exc:
        log.debug("openclaw_history_publish_failed", error=str(exc))
    finally:
        await client.aclose()


async def publish_openclaw_news_sentiment(
    *,
    score: float,
    channel_title: str,
    excerpt: str = "",
    source: str = "telegram",
    agent_fingerprint: str | None = None,
) -> None:
    """
    Persist the latest Telegram / news sentiment for the Ultimate Scalper
    and Nexus dashboards. ``score`` is 0–10 (breaking positive → high values).

    Optional ``agent_fingerprint`` (channel hash, session id, etc.) feeds the
    Strategy Brain swarm consensus grid when ``excerpt`` matches salient keywords.
    """
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        payload = {
            "score":          float(score),
            "channel_title":  channel_title[:500],
            "excerpt":        excerpt[:1200],
            "source":         source[:120],
            "updated_at":     datetime.now(timezone.utc).isoformat(),
        }
        await client.set(
            OPENCLAW_NEWS_SENTIMENT_KEY,
            json.dumps(payload),
            ex=OPENCLAW_NEWS_SENTIMENT_TTL,
        )
        fp = agent_fingerprint or hashlib.sha256(
            f"{channel_title}|{source}".encode("utf-8", errors="ignore")
        ).hexdigest()[:20]
        from nexus.shared.swarm_signals import ingest_text_for_swarm

        blob = f"{channel_title}\n{excerpt}"
        counts = await ingest_text_for_swarm(client, blob, fp)
        if counts:
            log.info("openclaw_swarm_keywords", matches=list(counts.keys()))
    except Exception as exc:
        log.debug("openclaw_news_sentiment_publish_failed", error=str(exc))
    finally:
        await client.aclose()


async def _intent_heartbeat(stop_event: asyncio.Event) -> None:
    """Push rotating node intent updates every 30 seconds while task runs."""
    statuses = cycle(
        (
            "OpenClaw heartbeat: Scanning...",
            "OpenClaw heartbeat: Optimizing...",
            "OpenClaw heartbeat: Resting...",
        )
    )
    while not stop_event.is_set():
        await _set_node_intent(next(statuses))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            continue


async def _stop_intent_heartbeat(stop_event: asyncio.Event, heartbeat_task: asyncio.Task[Any]) -> None:
    """Stop heartbeat task without surfacing cancellation noise."""
    stop_event.set()
    heartbeat_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await heartbeat_task


# ── Data models ────────────────────────────────────────────────────────────────

class Lead:
    """A single scraped lead before Telegram verification."""

    __slots__ = ("name", "phone", "username", "source_url", "extra", "telegram_id")

    def __init__(
        self,
        name: str = "",
        phone: str = "",
        username: str = "",
        source_url: str = "",
        extra: dict | None = None,
    ) -> None:
        self.name       = name
        self.phone      = phone.strip().replace(" ", "").replace("-", "")
        self.username   = username.lstrip("@")
        self.source_url = source_url
        self.extra      = extra or {}
        self.telegram_id: int | None = None

    def has_contact(self) -> bool:
        return bool(self.phone or self.username)

    def to_dict(self) -> dict:
        return {
            "name":        self.name,
            "phone":       self.phone,
            "username":    self.username,
            "source_url":  self.source_url,
            "telegram_id": self.telegram_id,
            "extra":       self.extra,
        }


# ── Google Maps scraper ────────────────────────────────────────────────────────

async def _scrape_google_maps(
    query: str,
    location: str,
    max_leads: int,
) -> list[Lead]:
    """
    Use Playwright to search Google Maps and extract business leads.
    Returns a list of Lead objects (phone numbers not yet verified).
    """
    try:
        from playwright.async_api import async_playwright  # type: ignore[import-untyped]
    except ImportError:
        log.warning("openclaw_playwright_missing",
                    hint="pip install playwright && playwright install chromium")
        return []

    full_query = f"{query} {location}".strip()
    search_url = f"https://www.google.com/maps/search/{full_query.replace(' ', '+')}"
    leads: list[Lead] = []

    log.info("openclaw_gmaps_start", query=full_query, max_leads=max_leads)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx     = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        page = await ctx.new_page()

        try:
            await page.goto(search_url, timeout=PW_TIMEOUT, wait_until="networkidle")
            await page.wait_for_timeout(2000)

            # Scroll the results feed to load more listings
            for _ in range(max(1, max_leads // 10)):
                await page.keyboard.press("End")
                await page.wait_for_timeout(1200)

            # Extract each result card
            cards = await page.query_selector_all(GMAPS_RESULT_SELECTOR)
            log.info("openclaw_gmaps_cards", count=len(cards))

            for card in cards[:max_leads]:
                try:
                    name_el   = await card.query_selector(GMAPS_NAME_SELECTOR)
                    phone_el  = await card.query_selector(GMAPS_PHONE_SELECTOR)
                    addr_el   = await card.query_selector(GMAPS_ADDR_SELECTOR)
                    rating_el = await card.query_selector(GMAPS_RATING_SELECTOR)

                    name    = (await name_el.inner_text()).strip()   if name_el   else ""
                    phone   = (await phone_el.inner_text()).strip()  if phone_el  else ""
                    address = (await addr_el.inner_text()).strip()   if addr_el   else ""
                    rating  = (await rating_el.get_attribute("aria-label") or "").strip() \
                              if rating_el else ""

                    if name:
                        leads.append(Lead(
                            name=name,
                            phone=phone,
                            source_url=search_url,
                            extra={"address": address, "rating": rating,
                                   "query": full_query},
                        ))
                except Exception as exc:
                    log.debug("openclaw_gmaps_card_error", error=str(exc))

        finally:
            await browser.close()

    log.info("openclaw_gmaps_done", leads=len(leads))
    return leads


# ── Social forums scraper ──────────────────────────────────────────────────────

async def _scrape_social_forums(
    query: str,
    max_leads: int,
) -> list[Lead]:
    """
    Search Reddit and Stack Overflow for users complaining about a software
    issue.  Extracts usernames from post authors and top commenters.
    """
    try:
        from playwright.async_api import async_playwright  # type: ignore[import-untyped]
    except ImportError:
        log.warning("openclaw_playwright_missing",
                    hint="pip install playwright && playwright install chromium")
        return []

    leads: list[Lead] = []
    log.info("openclaw_forums_start", query=query, max_leads=max_leads)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx     = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )

        # ── Reddit ────────────────────────────────────────────────────────────
        try:
            page = await ctx.new_page()
            url  = FORUM_SOURCES["reddit"].format(query=query.replace(" ", "+"))
            await page.goto(url, timeout=PW_TIMEOUT, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)

            # Reddit post author links
            author_els = await page.query_selector_all(
                'a[href*="/user/"][data-testid="post_author_link"],'
                'span[data-testid="comment_author_icon"] + a'
            )
            for el in author_els[:max_leads // 2]:
                try:
                    username = (await el.inner_text()).strip().lstrip("u/")
                    if username and username not in ("deleted", "[deleted]"):
                        leads.append(Lead(
                            username=username,
                            source_url=url,
                            extra={"platform": "reddit", "query": query},
                        ))
                except Exception:
                    pass
            await page.close()
            log.info("openclaw_reddit_done", leads_so_far=len(leads))
        except Exception as exc:
            log.warning("openclaw_reddit_error", error=str(exc))

        # ── Stack Overflow ────────────────────────────────────────────────────
        try:
            page = await ctx.new_page()
            url  = FORUM_SOURCES["stackoverflow"].format(
                query=query.replace(" ", "+")
            )
            await page.goto(url, timeout=PW_TIMEOUT, wait_until="domcontentloaded")
            await page.wait_for_timeout(1500)

            # SO question links → open each and grab the asker's display name
            question_links = await page.query_selector_all(
                ".s-post-summary--content-title a"
            )
            for link_el in question_links[: max_leads // 4]:
                try:
                    href = await link_el.get_attribute("href")
                    if not href:
                        continue
                    q_page = await ctx.new_page()
                    await q_page.goto(
                        f"https://stackoverflow.com{href}",
                        timeout=PW_TIMEOUT,
                        wait_until="domcontentloaded",
                    )
                    # Asker's display name
                    asker_el = await q_page.query_selector(
                        ".post-signature .user-details a"
                    )
                    if asker_el:
                        display_name = (await asker_el.inner_text()).strip()
                        if display_name:
                            leads.append(Lead(
                                name=display_name,
                                source_url=f"https://stackoverflow.com{href}",
                                extra={"platform": "stackoverflow", "query": query},
                            ))
                    await q_page.close()
                except Exception:
                    pass
            await page.close()
            log.info("openclaw_so_done", leads_so_far=len(leads))
        except Exception as exc:
            log.warning("openclaw_so_error", error=str(exc))

        await browser.close()

    log.info("openclaw_forums_done", leads=len(leads))
    return leads[:max_leads]


# ── Telegram verification ──────────────────────────────────────────────────────

async def _verify_telegram(
    leads: list[Lead],
    project_path: str,
) -> tuple[list[Lead], list[Lead]]:
    """
    Check each lead for an active Telegram account.

    Uses a subprocess that imports Telethon from the Mangement Ahu venv
    (same pattern as auto_scrape) to avoid dependency conflicts.

    Returns (verified_leads, unverified_leads).
    """
    if not leads:
        return [], []

    # Write leads to a temp JSON file for the subprocess
    import tempfile
    import sys
    from pathlib import Path

    leads_data = [l.to_dict() for l in leads]
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as f:
        json.dump(leads_data, f)
        tmp_in = f.name

    tmp_out = tmp_in.replace(".json", "_verified.json")

    helper = str(
        Path(__file__).parent / "_openclaw_verify_helper.py"
    )

    cmd = [sys.executable, helper,
           "--project", project_path,
           "--leads",   tmp_in,
           "--output",  tmp_out]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
        if proc.returncode != 0:
            log.warning("openclaw_verify_subprocess_error",
                        stderr=stderr.decode(errors="replace")[-500:])
            return [], leads  # treat all as unverified on error

        # Read results
        with open(tmp_out, encoding="utf-8") as f:
            results = json.load(f)

        verified: list[Lead] = []
        unverified: list[Lead] = []
        for orig, res in zip(leads, results):
            if res.get("telegram_id"):
                orig.telegram_id = res["telegram_id"]
                verified.append(orig)
            else:
                unverified.append(orig)

        log.info("openclaw_verify_done",
                 verified=len(verified), unverified=len(unverified))
        return verified, unverified

    except asyncio.TimeoutError:
        log.warning("openclaw_verify_timeout")
        return [], leads
    except Exception as exc:
        log.warning("openclaw_verify_error", error=str(exc))
        return [], leads
    finally:
        for p in (tmp_in, tmp_out):
            try:
                os.unlink(p)
            except Exception:
                pass


# ── telefix.db writer ──────────────────────────────────────────────────────────

async def _write_to_telefix(
    leads: list[Lead],
    project_id: str,
    db_path: str,
) -> int:
    """
    Write verified leads into telefix.db users table.
    Returns the count of rows actually inserted (skips duplicates).
    """
    if not leads or not os.path.exists(db_path):
        if not os.path.exists(db_path):
            log.warning("openclaw_db_missing", path=db_path)
        return 0

    written = 0
    try:
        import aiosqlite
        async with aiosqlite.connect(db_path) as db:
            for lead in leads:
                try:
                    await db.execute(
                        """
                        INSERT OR IGNORE INTO users
                            (telegram_id, username, first_name, source, project_id, added_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            lead.telegram_id,
                            lead.username or None,
                            lead.name or None,
                            "openclaw",
                            project_id,
                            datetime.now(timezone.utc).isoformat(),
                        ),
                    )
                    if db.total_changes > written:
                        written += 1
                except Exception as exc:
                    log.debug("openclaw_db_insert_skip", error=str(exc))
            await db.commit()
    except Exception as exc:
        log.error("openclaw_db_error", error=str(exc))

    log.info("openclaw_db_written", written=written, project_id=project_id)
    return written


# ── Main task handler ──────────────────────────────────────────────────────────

@registry.register("openclaw.browser_scrape")
@registry.register("scraper.openclaw")
async def browser_scrape(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    OpenClaw browser scraping task.

    Required parameters
    -------------------
    mode       : "google_maps" | "social_forums"
    query      : search query string
    project_id : telefix project to write verified leads into

    Optional parameters
    -------------------
    max_leads  : int  (default 50, capped at OPENCLAW_MAX_LEADS env var)
    location   : str  (for google_maps mode, e.g. "Tel Aviv")
    """
    t0 = time.monotonic()

    mode       = parameters.get("mode", "google_maps")
    query      = parameters.get("query", "")
    project_id = parameters.get("project_id", "default")
    max_leads  = min(int(parameters.get("max_leads", 50)), MAX_LEADS_CAP)
    location   = parameters.get("location", "")
    await _set_node_intent("OpenClaw validating task parameters")

    if not query:
        await _publish_openclaw_status(
            active=False,
            stage="failed",
            detail="query parameter is required",
            mode=mode,
        )
        return {"status": "failed", "error": "query parameter is required"}

    await _publish_openclaw_status(
        active=True,
        stage="boot",
        detail="OpenClaw boot sequence initiated",
        mode=mode,
        query=query,
    )
    await _set_node_intent(f"OpenClaw boot sequence: preparing {mode} scrape for '{query[:60]}'")
    await _set_node_vision(_predict_openclaw_vision(mode))
    heartbeat_stop = asyncio.Event()
    heartbeat_task = asyncio.create_task(_intent_heartbeat(heartbeat_stop))

    # ── Pre-flight: CPU check ──────────────────────────────────────────────────
    cpu = psutil.cpu_percent(interval=1)
    if cpu > CPU_THRESHOLD:
        await _set_node_intent("OpenClaw paused: CPU pressure above safe threshold")
        await _set_node_vision("Next 5m: waiting for CPU headroom before resuming scrape cycle.")
        await _publish_openclaw_status(
            active=False,
            stage="low_resources",
            detail=f"CPU pressure too high ({cpu:.1f}%)",
            mode=mode,
            query=query,
        )
        log.warning("openclaw_low_resources", cpu=cpu, threshold=CPU_THRESHOLD)
        await _stop_intent_heartbeat(heartbeat_stop, heartbeat_task)
        return {
            "status": "low_resources",
            "mode": mode,
            "cpu_percent": cpu,
            "leads_found": 0,
            "leads_verified": 0,
            "leads_written": 0,
            "leads_unverified": 0,
            "duration_s": round(time.monotonic() - t0, 2),
        }

    # ── Status broadcast ───────────────────────────────────────────────────────
    # (redis is not directly available in the task handler — use the ARQ context)
    log.info("openclaw_task_start", mode=mode, query=query, max_leads=max_leads)

    # ── Scrape ─────────────────────────────────────────────────────────────────
    leads: list[Lead] = []
    if mode == "google_maps":
        await _set_node_intent(f"OpenClaw scanning Google Maps: '{query[:60]}'")
        await _set_node_vision("Next 5m: harvesting map listings and extracting contact metadata.")
        await _publish_openclaw_status(
            active=True,
            stage="scraping",
            detail="Scanning Google Maps",
            mode=mode,
            query=query,
        )
        leads = await _scrape_google_maps(query, location, max_leads)
    elif mode == "social_forums":
        await _set_node_intent(f"OpenClaw mining social forums: '{query[:60]}'")
        await _set_node_vision("Next 5m: mining forum threads for high-intent complaint signals.")
        await _publish_openclaw_status(
            active=True,
            stage="scraping",
            detail="Scanning social forums",
            mode=mode,
            query=query,
        )
        leads = await _scrape_social_forums(query, max_leads)
    else:
        await _publish_openclaw_status(
            active=False,
            stage="failed",
            detail=f"Unknown mode: {mode!r}",
            mode=mode,
            query=query,
        )
        await _stop_intent_heartbeat(heartbeat_stop, heartbeat_task)
        return {"status": "failed", "error": f"Unknown mode: {mode!r}"}

    leads_found = len(leads)
    log.info("openclaw_scrape_complete", mode=mode, leads_found=leads_found)

    # ── Filter leads that have some contact info ───────────────────────────────
    await _set_node_intent("OpenClaw filtering scraped leads for contactable entries")
    contactable = [l for l in leads if l.has_contact()]
    log.info("openclaw_contactable", count=len(contactable))

    # ── Telegram verification ──────────────────────────────────────────────────
    await _set_node_intent("OpenClaw verifying Telegram reachability for scraped leads")
    await _set_node_vision(_predict_openclaw_vision(mode, leads_found=leads_found, verified=0))
    await _publish_openclaw_status(
        active=True,
        stage="verifying",
        detail="Verifying Telegram reachability",
        mode=mode,
        query=query,
    )
    verified, unverified = await _verify_telegram(contactable, TELEFIX_PROJECT)

    # ── Write verified leads to telefix.db ────────────────────────────────────
    await _set_node_intent("OpenClaw writing verified leads into TeleFix datastore")
    await _set_node_vision(
        _predict_openclaw_vision(mode, leads_found=leads_found, verified=len(verified))
    )
    await _publish_openclaw_status(
        active=True,
        stage="persisting",
        detail="Persisting verified leads",
        mode=mode,
        query=query,
    )
    written = await _write_to_telefix(verified, project_id, TELEFIX_DB)

    # ── Store unverified leads in Redis for later retry ───────────────────────
    # (We don't have direct Redis access here; store in a file the master can pick up)
    if unverified:
        await _set_node_intent("OpenClaw archiving unverified leads for retry pipeline")
        unverified_path = os.path.join(
            os.path.dirname(TELEFIX_DB),
            f"openclaw_unverified_{project_id}_{int(time.time())}.json",
        )
        try:
            with open(unverified_path, "w", encoding="utf-8") as f:
                json.dump([l.to_dict() for l in unverified], f, indent=2)
            log.info("openclaw_unverified_saved", path=unverified_path,
                     count=len(unverified))
        except Exception as exc:
            log.warning("openclaw_unverified_save_error", error=str(exc))

    duration = round(time.monotonic() - t0, 2)
    await _set_node_intent(f"OpenClaw complete: {written} verified leads persisted")
    await _set_node_vision(
        "Next 5m: cool down browser workload, then launch the next lead-acquisition pass."
    )
    await _publish_openclaw_status(
        active=False,
        stage="completed",
        detail=f"Completed with {written} verified leads persisted",
        mode=mode,
        query=query,
    )
    await _push_node_history(
        f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}Z] OpenClaw ({mode}) "
        f"completed | verified={len(verified)} written={written}"
    )
    log.info("openclaw_task_done",
             mode=mode, leads_found=leads_found,
             leads_verified=len(verified), leads_written=written,
             leads_unverified=len(unverified), duration_s=duration)
    await _stop_intent_heartbeat(heartbeat_stop, heartbeat_task)

    return {
        "status":           "completed",
        "mode":             mode,
        "query":            query,
        "leads_found":      leads_found,
        "leads_contactable": len(contactable),
        "leads_verified":   len(verified),
        "leads_written":    written,
        "leads_unverified": len(unverified),
        "duration_s":       duration,
    }
