"""
Scout Service — Autonomous Niche Discovery Engine.

Pulls live data from open APIs (Google Trends via pytrends, CryptoCurrency
market data via CoinGecko, and news headlines via NewsAPI / GNews) and
applies a multi-factor scoring model to surface the top 3 high-ROI Telegram
niches every week.

Architecture
------------
ScoutService runs as a background asyncio loop (default: every 7 days).
Each cycle:
  1. Fetches trending topics from Google Trends (pytrends)
  2. Fetches top-gaining crypto coins (CoinGecko public API — no key needed)
  3. Fetches hot news headlines (GNews free tier — GNEWS_API_KEY in .env)
  4. Scores each candidate on: search_volume, growth_velocity, monetisation_potential
  5. Writes the top 3 niches to Redis (nexus:scout:niches) with full metadata
  6. Logs the reasoning to the agent log (nexus:agent:log)

OpenClaw Integration (Phase 19)
--------------------------------
After a Telegram scrape cycle, the Scout checks the yield from telefix.db.
If the number of new users scraped in the last cycle is below
LOW_YIELD_THRESHOLD (default: 5), the Scout triggers an OpenClaw
browser-scraping task on the Linux worker for each known project:

  "House of Exhaust"   → google_maps  mode, query="exhaust repair"
  "Management Ahu"     → social_forums mode, query="management ahu bug"

The OpenClaw task runs on the Linux worker (requires_capabilities=["linux-only"])
and writes verified Telegram leads directly into telefix.db.

Redis Keys
----------
nexus:scout:niches       — JSON list of top 3 NicheCandidate dicts (TTL: 8 days)
nexus:scout:state        — "idle" | "scanning" | "complete" | "error"
nexus:scout:last_run     — ISO timestamp of last successful run
nexus:scout:last_yield   — int, users scraped in last Telegram cycle
nexus:scout:openclaw_log — JSON list of recent OpenClaw dispatch records
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx
import structlog

log = structlog.get_logger(__name__)

# ── Redis keys ────────────────────────────────────────────────────────────────
SCOUT_NICHES_KEY      = "nexus:scout:niches"
SCOUT_STATE_KEY       = "nexus:scout:state"
SCOUT_LAST_RUN_KEY    = "nexus:scout:last_run"
SCOUT_LAST_YIELD_KEY  = "nexus:scout:last_yield"
SCOUT_OPENCLAW_LOG    = "nexus:scout:openclaw_log"
SCOUT_NICHES_TTL      = 8 * 24 * 3600   # 8 days
SCOUT_STATE_TTL       = 3600             # 1 hour

# ── OpenClaw integration ───────────────────────────────────────────────────────
# If a Telegram scrape cycle yields fewer than this many new users, the Scout
# triggers OpenClaw browser-scraping tasks as a fallback lead source.
LOW_YIELD_THRESHOLD = int(os.environ.get("SCOUT_LOW_YIELD_THRESHOLD", "5"))

# Per-project OpenClaw configuration.
# Each entry maps a project_id to the OpenClaw task parameters that should
# be dispatched when that project's Telegram yield is low.
OPENCLAW_PROJECT_CONFIG: list[dict[str, Any]] = [
    {
        "project_id": "house_of_exhaust",
        "display_name": "House of Exhaust",
        "mode": "google_maps",
        "query": "exhaust repair shop",
        "location": "Israel",
        "max_leads": 50,
    },
    {
        "project_id": "management_ahu",
        "display_name": "Management Ahu",
        "mode": "social_forums",
        "query": "management ahu telegram bot problem",
        "max_leads": 60,
    },
]

# ── Scoring weights ────────────────────────────────────────────────────────────
W_VOLUME      = 0.35   # raw search / market interest
W_VELOCITY    = 0.40   # rate of growth (trending up = high value)
W_MONETISE    = 0.25   # how easily this niche converts to Telegram revenue

# ── CoinGecko public endpoint (no API key) ────────────────────────────────────
COINGECKO_TOP_GAINERS = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd&order=price_change_percentage_24h_desc"
    "&per_page=20&page=1&sparkline=false"
    "&price_change_percentage=24h"
)

# ── GNews free endpoint (requires GNEWS_API_KEY) ──────────────────────────────
GNEWS_TOP_HEADLINES = (
    "https://gnews.io/api/v4/top-headlines"
    "?lang=en&country=us&max=20&apikey={key}"
)

# ── Known high-monetisation Telegram niche keywords ──────────────────────────
MONETISATION_KEYWORDS: dict[str, float] = {
    # Finance / crypto
    "crypto": 0.95, "bitcoin": 0.95, "trading": 0.90, "forex": 0.90,
    "nft": 0.85, "defi": 0.88, "altcoin": 0.85, "ethereum": 0.88,
    "investment": 0.80, "stocks": 0.80, "finance": 0.75,
    # High-engagement niches
    "ai": 0.85, "artificial intelligence": 0.85, "chatgpt": 0.82,
    "tech": 0.70, "startup": 0.72, "saas": 0.75,
    # Entertainment / viral
    "meme": 0.65, "viral": 0.60, "gaming": 0.70, "esports": 0.72,
    # Health / wellness
    "fitness": 0.65, "weight loss": 0.68, "nutrition": 0.62,
    # Default
    "_default": 0.40,
}


# ── Data models ───────────────────────────────────────────────────────────────

@dataclass
class NicheCandidate:
    """A single niche candidate with scoring metadata."""
    name: str
    source: str                         # "crypto" | "trends" | "news"
    keywords: list[str] = field(default_factory=list)
    volume_score: float = 0.0           # 0–100
    velocity_score: float = 0.0         # 0–100
    monetisation_score: float = 0.0     # 0–100
    raw_data: dict[str, Any] = field(default_factory=dict)

    @property
    def composite(self) -> float:
        return (
            self.volume_score    * W_VOLUME
            + self.velocity_score  * W_VELOCITY
            + self.monetisation_score * W_MONETISE
        )

    @property
    def confidence(self) -> int:
        return max(0, min(100, int(self.composite)))

    @property
    def roi_estimate(self) -> str:
        c = self.confidence
        if c >= 80:
            return f"High ROI potential (~{c * 3}% above baseline)"
        if c >= 60:
            return f"Medium ROI potential (~{c * 2}% above baseline)"
        return f"Speculative (~{c}% above baseline)"

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["composite"] = round(self.composite, 1)
        d["confidence"] = self.confidence
        d["roi_estimate"] = self.roi_estimate
        d["discovered_at"] = datetime.now(timezone.utc).isoformat()
        return d


# ── Scoring helpers ───────────────────────────────────────────────────────────

def _monetisation_score(text: str) -> float:
    """Score 0–100 based on known high-monetisation keywords."""
    text_lower = text.lower()
    best = MONETISATION_KEYWORDS["_default"]
    for kw, score in MONETISATION_KEYWORDS.items():
        if kw != "_default" and kw in text_lower:
            best = max(best, score)
    return best * 100


# ── Data fetchers ─────────────────────────────────────────────────────────────

async def _fetch_crypto_niches(client: httpx.AsyncClient) -> list[NicheCandidate]:
    """
    Pull top 24h gainers from CoinGecko and convert to niche candidates.
    Each coin's 24h price change drives the velocity score.
    """
    candidates: list[NicheCandidate] = []
    try:
        resp = await client.get(COINGECKO_TOP_GAINERS, timeout=15)
        resp.raise_for_status()
        coins = resp.json()

        for coin in coins[:10]:
            name   = coin.get("name", "Unknown")
            symbol = coin.get("symbol", "").upper()
            change_24h = float(coin.get("price_change_percentage_24h") or 0)
            market_cap = float(coin.get("market_cap") or 0)
            volume_24h = float(coin.get("total_volume") or 0)

            # Velocity: normalise 24h change (cap at 200% for scoring)
            velocity = min(100, max(0, change_24h / 2))

            # Volume: log-normalise market cap (0–100)
            import math
            volume = min(100, math.log10(max(market_cap, 1)) / 12 * 100) if market_cap > 0 else 0

            # Monetisation: crypto always scores high
            monetise = _monetisation_score(f"crypto {name} {symbol} trading")

            candidates.append(NicheCandidate(
                name=f"{name} ({symbol}) Crypto",
                source="crypto",
                keywords=[name.lower(), symbol.lower(), "crypto", "trading", "investment"],
                volume_score=volume,
                velocity_score=velocity,
                monetisation_score=monetise,
                raw_data={
                    "coin_id": coin.get("id"),
                    "symbol": symbol,
                    "change_24h_pct": round(change_24h, 2),
                    "market_cap_usd": market_cap,
                    "volume_24h_usd": volume_24h,
                },
            ))

        log.info("scout_crypto_fetched", count=len(candidates))
    except Exception as exc:
        log.warning("scout_crypto_error", error=str(exc))

    return candidates


async def _fetch_news_niches(client: httpx.AsyncClient) -> list[NicheCandidate]:
    """
    Pull top headlines from GNews (free tier).
    Falls back to a curated trending list if no API key is configured.
    """
    candidates: list[NicheCandidate] = []
    gnews_key = os.environ.get("GNEWS_API_KEY", "")

    if gnews_key:
        try:
            url = GNEWS_TOP_HEADLINES.format(key=gnews_key)
            resp = await client.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            articles = data.get("articles", [])

            # Cluster articles by keyword frequency
            keyword_counts: dict[str, int] = {}
            keyword_articles: dict[str, list[str]] = {}

            for article in articles:
                title = article.get("title", "") + " " + article.get("description", "")
                for kw in MONETISATION_KEYWORDS:
                    if kw != "_default" and kw in title.lower():
                        keyword_counts[kw] = keyword_counts.get(kw, 0) + 1
                        keyword_articles.setdefault(kw, []).append(title[:80])

            for kw, count in sorted(keyword_counts.items(), key=lambda x: -x[1])[:8]:
                velocity = min(100, count * 15)
                monetise = _monetisation_score(kw)
                candidates.append(NicheCandidate(
                    name=f"{kw.title()} News Wave",
                    source="news",
                    keywords=[kw, "news", "trending"],
                    volume_score=min(100, count * 20),
                    velocity_score=velocity,
                    monetisation_score=monetise,
                    raw_data={
                        "article_count": count,
                        "sample_titles": keyword_articles[kw][:3],
                    },
                ))

            log.info("scout_news_fetched", count=len(candidates))
        except Exception as exc:
            log.warning("scout_news_error", error=str(exc))
    else:
        log.info("scout_news_skipped", reason="GNEWS_API_KEY not set — using curated fallback")
        # Curated evergreen high-ROI niches as fallback
        fallback = [
            ("AI & Automation Tools", ["ai", "chatgpt", "automation"], 78, 85, 85),
            ("Crypto Trading Signals", ["crypto", "trading", "signals"], 82, 80, 95),
            ("Passive Income Methods", ["passive income", "investment", "finance"], 70, 72, 80),
            ("Tech Startup News", ["startup", "tech", "saas"], 65, 68, 72),
        ]
        for name, kws, vol, vel, mon in fallback:
            candidates.append(NicheCandidate(
                name=name,
                source="news",
                keywords=kws,
                volume_score=vol,
                velocity_score=vel,
                monetisation_score=mon,
                raw_data={"type": "curated_fallback"},
            ))

    return candidates


async def _fetch_trends_niches(client: httpx.AsyncClient) -> list[NicheCandidate]:
    """
    Pull Google Trends data via pytrends (no API key needed).
    Uses the daily trending searches endpoint.
    Falls back gracefully if pytrends is not installed.
    """
    candidates: list[NicheCandidate] = []
    try:
        from pytrends.request import TrendReq  # type: ignore[import]
        import asyncio

        def _blocking_trends() -> list[tuple[str, int]]:
            pt = TrendReq(hl="en-US", tz=360, timeout=(10, 25))
            df = pt.trending_searches(pn="united_states")
            topics = df[0].tolist()[:15]
            return [(str(t), i) for i, t in enumerate(topics)]

        topics = await asyncio.get_event_loop().run_in_executor(None, _blocking_trends)

        for topic, rank in topics:
            # Higher rank (lower index) = more trending
            velocity = max(0, 100 - rank * 6)
            monetise = _monetisation_score(topic)
            volume   = max(20, 90 - rank * 4)

            candidates.append(NicheCandidate(
                name=f"{topic} (Trending)",
                source="trends",
                keywords=[topic.lower(), "trending"],
                volume_score=volume,
                velocity_score=velocity,
                monetisation_score=monetise,
                raw_data={"trend_rank": rank + 1},
            ))

        log.info("scout_trends_fetched", count=len(candidates))
    except ImportError:
        log.info("scout_trends_skipped", reason="pytrends not installed — skipping Google Trends")
    except Exception as exc:
        log.warning("scout_trends_error", error=str(exc))

    return candidates


# ── Main Scout cycle ──────────────────────────────────────────────────────────

async def _run_scout_cycle(redis: Any) -> list[NicheCandidate]:
    """
    Full discovery cycle: fetch all sources, score, deduplicate, return top 3.
    """
    await redis.set(SCOUT_STATE_KEY, "scanning", ex=SCOUT_STATE_TTL)

    async with httpx.AsyncClient(
        headers={"User-Agent": "NexusScout/1.0"},
        follow_redirects=True,
    ) as client:
        crypto_task  = _fetch_crypto_niches(client)
        news_task    = _fetch_news_niches(client)
        trends_task  = _fetch_trends_niches(client)

        crypto_niches, news_niches, trends_niches = await asyncio.gather(
            crypto_task, news_task, trends_task, return_exceptions=False
        )

    all_candidates = crypto_niches + news_niches + trends_niches

    if not all_candidates:
        log.warning("scout_no_candidates")
        await redis.set(SCOUT_STATE_KEY, "error", ex=SCOUT_STATE_TTL)
        return []

    # Sort by composite score descending
    all_candidates.sort(key=lambda c: c.composite, reverse=True)

    # Deduplicate by name similarity (simple prefix check)
    seen_names: set[str] = set()
    deduped: list[NicheCandidate] = []
    for c in all_candidates:
        key = c.name[:20].lower()
        if key not in seen_names:
            seen_names.add(key)
            deduped.append(c)

    top3 = deduped[:3]

    # Persist to Redis
    payload = json.dumps([c.to_dict() for c in top3])
    await redis.set(SCOUT_NICHES_KEY, payload, ex=SCOUT_NICHES_TTL)
    await redis.set(SCOUT_LAST_RUN_KEY, datetime.now(timezone.utc).isoformat())
    await redis.set(SCOUT_STATE_KEY, "complete", ex=SCOUT_STATE_TTL)

    log.info("scout_cycle_complete", top_niches=[c.name for c in top3])
    return top3


# ── ScoutService class ────────────────────────────────────────────────────────

class ScoutService:
    """
    Background service that discovers high-ROI Telegram niches weekly.

    Usage
    -----
        scout = ScoutService(redis)
        asyncio.create_task(scout.run_loop(interval_days=7))
    """

    def __init__(
        self,
        redis: Any,
        gemini_api_key: str = "",   # accepted for compat, not used by Scout
        interval_hours: int = 168,  # 7 days default
    ) -> None:
        self._redis = redis
        self._running = False
        self._interval_hours = interval_hours

    async def run_loop(self, interval_days: int | None = None) -> None:
        self._running = True
        interval_s = (interval_days * 24 * 3600) if interval_days else (self._interval_hours * 3600)
        log.info("scout_service_started", interval_hours=interval_s // 3600)

        while self._running:
            try:
                niches = await _run_scout_cycle(self._redis)
                await self._log_niches(niches)
            except Exception as exc:
                log.error("scout_cycle_error", error=str(exc))

            await asyncio.sleep(interval_s)

    async def run_once(self) -> list[dict[str, Any]]:
        """Run a single discovery cycle and return the top 3 niches as dicts."""
        niches = await _run_scout_cycle(self._redis)
        return [c.to_dict() for c in niches]

    async def get_latest_report(self) -> dict[str, Any] | None:
        """
        Return the latest Scout report as a dict compatible with the master startup.
        Returns None if no scan has run yet.
        """
        raw = await self._redis.get(SCOUT_NICHES_KEY)
        if not raw:
            return None
        try:
            niches = json.loads(raw)
            return {
                "opportunities": [
                    {
                        "niche": n.get("name", ""),
                        "confidence": n.get("confidence", 0),
                        "keywords": n.get("keywords", []),
                        "roi_estimate": n.get("roi_estimate", ""),
                        "source": n.get("source", ""),
                        "auto_start": n.get("confidence", 0) >= 80,
                    }
                    for n in niches
                ],
                "generated_at": await self._redis.get(SCOUT_LAST_RUN_KEY) or "",
            }
        except Exception:
            return None

    def stop(self) -> None:
        self._running = False

    # ── OpenClaw integration ───────────────────────────────────────────────────

    async def check_yield_and_dispatch_openclaw(
        self,
        dispatcher: Any | None = None,
    ) -> list[str]:
        """
        Read the last Telegram scrape yield from Redis.
        If yield < LOW_YIELD_THRESHOLD, dispatch OpenClaw browser-scraping tasks
        for every configured project.

        Parameters
        ----------
        dispatcher : Dispatcher instance (optional).  If provided, tasks are
                     dispatched via the normal ARQ pipeline.  If None, the
                     dispatch records are logged but not sent (dry-run mode).

        Returns a list of task_ids that were dispatched (empty if yield is OK).
        """
        raw_yield = await self._redis.get(SCOUT_LAST_YIELD_KEY)
        last_yield = int(raw_yield) if raw_yield and raw_yield.isdigit() else None

        if last_yield is None:
            log.info("scout_openclaw_skip",
                     reason="No yield data yet — skipping OpenClaw check")
            return []

        if last_yield >= LOW_YIELD_THRESHOLD:
            log.info("scout_openclaw_skip",
                     last_yield=last_yield,
                     threshold=LOW_YIELD_THRESHOLD,
                     reason="Yield is healthy — no OpenClaw needed")
            return []

        log.warning(
            "scout_low_yield_detected",
            last_yield=last_yield,
            threshold=LOW_YIELD_THRESHOLD,
            action="Triggering OpenClaw browser scraping",
        )

        dispatched_ids: list[str] = []

        for cfg in OPENCLAW_PROJECT_CONFIG:
            task_id = await self._dispatch_openclaw_task(cfg, dispatcher)
            if task_id:
                dispatched_ids.append(task_id)

        # Log the dispatch batch to Redis
        if dispatched_ids:
            record = json.dumps({
                "ts": datetime.now(timezone.utc).isoformat(),
                "trigger": "low_yield",
                "last_yield": last_yield,
                "threshold": LOW_YIELD_THRESHOLD,
                "task_ids": dispatched_ids,
                "projects": [c["display_name"] for c in OPENCLAW_PROJECT_CONFIG],
            })
            await self._redis.lpush(SCOUT_OPENCLAW_LOG, record)
            await self._redis.ltrim(SCOUT_OPENCLAW_LOG, 0, 49)  # keep last 50

        return dispatched_ids

    async def _dispatch_openclaw_task(
        self,
        cfg: dict[str, Any],
        dispatcher: Any | None,
    ) -> str | None:
        """
        Build and dispatch a single openclaw.browser_scrape task.
        Returns the task_id on success, None on failure.
        """
        from nexus.shared.schemas import TaskPayload, WorkerCapability

        parameters = {
            "mode":       cfg["mode"],
            "query":      cfg["query"],
            "project_id": cfg["project_id"],
            "max_leads":  cfg.get("max_leads", 50),
        }
        if cfg.get("location"):
            parameters["location"] = cfg["location"]

        task = TaskPayload(
            task_type="openclaw.browser_scrape",
            parameters=parameters,
            project_id=cfg["project_id"],
            priority=3,
            # Must run on the Linux worker — Playwright + headless Chromium
            required_capabilities=[WorkerCapability.LINUX],
            approval_context=(
                f"OpenClaw triggered by Scout (low yield < {LOW_YIELD_THRESHOLD}). "
                f"Project: {cfg['display_name']} | Mode: {cfg['mode']} | "
                f"Query: {cfg['query']!r}"
            ),
        )

        log.info(
            "scout_openclaw_dispatch",
            project=cfg["display_name"],
            mode=cfg["mode"],
            query=cfg["query"],
            task_id=task.task_id,
            dry_run=dispatcher is None,
        )

        if dispatcher is not None:
            try:
                await dispatcher.dispatch(task)
                return task.task_id
            except Exception as exc:
                log.error("scout_openclaw_dispatch_error",
                          project=cfg["display_name"], error=str(exc))
                return None
        else:
            # Dry-run: log the intent but don't actually dispatch
            await self._log_agent_entry(
                f"[Scout→OpenClaw] Would dispatch {cfg['mode']} scrape for "
                f"{cfg['display_name']} (query={cfg['query']!r}) — no dispatcher"
            )
            return task.task_id  # return the id even in dry-run for testing

    async def record_scrape_yield(self, users_scraped: int) -> None:
        """
        Called by auto_scrape / super_scrape after a Telegram cycle completes.
        Stores the yield count so the Scout can detect low-yield situations.
        """
        await self._redis.set(SCOUT_LAST_YIELD_KEY, str(users_scraped), ex=86400)
        log.debug("scout_yield_recorded", users_scraped=users_scraped)

        # Immediately check if OpenClaw should be triggered
        if users_scraped < LOW_YIELD_THRESHOLD:
            await self.check_yield_and_dispatch_openclaw()

    async def _log_agent_entry(self, message: str) -> None:
        """Write a single entry to the shared agent log."""
        from nexus.master.services.decision_engine import AGENT_LOG_KEY, AGENT_LOG_MAX

        entry = json.dumps({
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": "info",
            "message": message,
            "metadata": {},
        })
        await self._redis.lpush(AGENT_LOG_KEY, entry)
        await self._redis.ltrim(AGENT_LOG_KEY, 0, AGENT_LOG_MAX - 1)

    async def _log_niches(self, niches: list[NicheCandidate]) -> None:
        """Write discovery results to the shared agent log."""
        from nexus.master.services.decision_engine import AGENT_LOG_KEY, AGENT_LOG_MAX

        for i, niche in enumerate(niches, 1):
            entry = json.dumps({
                "ts": datetime.now(timezone.utc).isoformat(),
                "level": "decision",
                "message": (
                    f"[Scout] #{i} niche discovered: {niche.name} "
                    f"(confidence={niche.confidence}, source={niche.source})"
                ),
                "metadata": {
                    "niche_name": niche.name,
                    "confidence": niche.confidence,
                    "roi_estimate": niche.roi_estimate,
                    "source": niche.source,
                    "keywords": niche.keywords,
                },
            })
            await self._redis.lpush(AGENT_LOG_KEY, entry)

        await self._redis.ltrim(AGENT_LOG_KEY, 0, AGENT_LOG_MAX - 1)


# ── Standalone function (used by API) ─────────────────────────────────────────

async def get_current_niches(redis: Any) -> list[dict[str, Any]]:
    """
    Return the cached top-3 niches from Redis.
    If cache is empty, triggers a fresh scan and waits for it.
    """
    raw = await redis.get(SCOUT_NICHES_KEY)
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass

    # Cache miss — run a fresh cycle
    niches = await _run_scout_cycle(redis)
    return [c.to_dict() for c in niches]
