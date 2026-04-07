"""
Nexus Control Center — FastAPI application factory.

Production features
-------------------
- Rate limiting via slowapi (100 req/min per IP by default)
- Global exception handler with structured JSON error responses
- Request ID injection for distributed tracing
- CORS configured for dashboard + Tailscale VPN access
- Redis connection pooling via decode_responses=True
- Background arbitrage time-series collector (Binance vs Polymarket, 2 s cadence)
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from urllib.parse import unquote, urlparse

import structlog
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from redis.asyncio import Redis
from slowapi import Limiter, _rate_limit_exceeded_handler  # type: ignore[import-untyped]
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

from nexus.api.hitl_store import HitlStore
from nexus.api.routers import (
    ahu,
    ai,
    business,
    cluster,
    config,
    content,
    deploy,
    evolution,
    factory,
    flight_mode,
    group_infiltration,
    hitl,
    incubator,
    management_dashboard,
    modules,
    notifications,
    openclaw_control,
    polymarket,
    prediction,
    projects,
    proxy,
    scalper,
    scan,
    seo,
    sentinel,
    sessions,
    swarm,
    system,
    telefix,
)
from src.nexus.services.api.routers import factory_dashboard, telefix_dashboard
from nexus.shared import redis_util
from nexus.shared.config import log_polymarket_wallet_mismatch_at_startup, settings
from nexus.shared.logging_config import configure_logging

log = structlog.get_logger(__name__)

# ── Rate limiter (shared across all routes) ────────────────────────────────────
limiter = Limiter(key_func=get_remote_address, default_limits=["100/minute"])


def _build_redis_client(redis_url: str) -> Redis:
    """
    Build a Redis client with an explicit IPv4 host for localhost URLs.
    """
    parsed = urlparse(redis_url)
    host = parsed.hostname or "127.0.0.1"
    if host == "localhost":
        host = "127.0.0.1"
    port = parsed.port or 6379
    db = 0
    if parsed.path and parsed.path != "/":
        try:
            db = int(parsed.path.lstrip("/"))
        except ValueError:
            db = 0
    password = unquote(parsed.password) if parsed.password else None
    username = unquote(parsed.username) if parsed.username else None
    use_ssl = parsed.scheme in {"rediss", "redis+ssl"}
    return Redis(
        host=host,
        port=port,
        db=db,
        username=username,
        password=password,
        ssl=use_ssl,
        decode_responses=True,
    )


async def _connect_redis_with_retry(redis_url: str) -> tuple[Redis, bool]:
    """
    Connect to Redis with backoff, Windows WSL auto-start, then optional degraded mode.

    Returns ``(client, degraded)`` where ``degraded`` means an in-memory fakeredis
    broker (``NEXUS_ALLOW_DEGRADED=1``) after real Redis stays unreachable.
    """
    url = redis_util.coerce_redis_url_for_platform(redis_url)
    settings.redis_url = url

    attempt = 0
    delay_s = 1.0
    wsl_tried = False
    max_attempts = 14

    while attempt < max_attempts:
        attempt += 1
        client = _build_redis_client(url)
        try:
            await client.ping()
            if attempt > 1:
                log.info("api_redis_recovered", attempts=attempt)
            return client, False
        except Exception as exc:
            await client.aclose()
            if sys.platform == "win32" and not wsl_tried and attempt >= 2:
                wsl_tried = True
                log.warning(
                    "api_redis_wsl_autofix",
                    cmd="wsl redis-server start",
                )
                redis_util.try_start_redis_via_wsl_windows()
                await asyncio.sleep(3.0)
                delay_s = 1.0
                continue
            if attempt == 1 or attempt == 2 or attempt % 5 == 0:
                log.warning(
                    "api_redis_connect_retry",
                    attempt=attempt,
                    retry_in_s=round(delay_s, 2),
                    error=str(exc),
                )
            await asyncio.sleep(delay_s)
            delay_s = min(delay_s * 1.7, 10.0)

    redis_util.mark_degraded_mode()
    log.error(
        "api_redis_degraded",
        detail="in-memory fakeredis; start real broker to recover",
    )
    fake = redis_util.create_degraded_async_redis()
    return fake, True


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage startup and shutdown of shared resources."""
    configure_logging(level=settings.log_level.upper() if hasattr(settings, "log_level") else "INFO", node_id=f"{settings.node_id}-api")

    redis, redis_degraded = await _connect_redis_with_retry(settings.redis_url)
    app.state.redis = redis
    app.state.redis_degraded = redis_degraded
    log.info(
        "api_redis_connected",
        url=settings.redis_url,
        degraded=redis_degraded,
    )

    hitl_store = HitlStore(redis)
    app.state.hitl_store = hitl_store
    await hitl_store.start()

    # Evolution engine is not started inside the API process — it runs in the
    # master node.  We set app.state.evolution_engine = None so the router
    # falls back to direct Redis manipulation when the master is not co-located.
    app.state.evolution_engine = None

    # Start the background arbitrage price collector (Binance + Polymarket @ 2 s).
    from nexus.worker.tasks.prediction import run_arbitrage_collector
    collector_task = asyncio.create_task(
        run_arbitrage_collector(redis),
        name="arbitrage_collector",
    )

    from nexus.master.sentinel import run_stability_monitor
    stability_task = asyncio.create_task(
        run_stability_monitor(redis),
        name="stability_monitor",
    )

    openclaw_self_improve_task: asyncio.Task[None] | None = None
    if os.getenv("OPENCLAW_SELF_IMPROVE_ENABLED", "").strip().lower() in ("1", "true", "yes", "on"):
        from nexus.services.openclaw_self_improve import run_openclaw_self_improve_loop

        openclaw_self_improve_task = asyncio.create_task(
            run_openclaw_self_improve_loop(redis),
            name="openclaw_self_improve",
        )
        log.info("openclaw_self_improve_task_started", channel="nexus:swarm:logs")

    scalper_task: asyncio.Task[None] | None = None
    if os.getenv("NEXUS_POLY_SCALPER_ENABLED", "").strip().lower() in ("1", "true", "yes", "on"):
        from nexus.master.services.poly_5m_scalper import run_poly_scalper_loop

        scalper_task = asyncio.create_task(
            run_poly_scalper_loop(redis),
            name="poly_5m_scalper",
        )
        log.info("poly_scalper_background_task_started")

    log_polymarket_wallet_mismatch_at_startup()

    log.info("nexus_api_started", docs="/docs", rate_limit="100/min")

    # Push live system lines to the Redis log stream so the Live Terminal shows data.
    # Writes to both the canonical node_id key and the UI alias "master-hybrid-node".
    async def _master_log_streamer() -> None:
        import socket as _sock  # noqa: PLC0415
        _keys = [f"nexus:log_stream:{settings.node_id}", "nexus:log_stream:master-hybrid-node"]
        _hostname = _sock.gethostname()
        _max_lines = 200
        while True:
            try:
                await asyncio.sleep(3.0)
                _ts = __import__("datetime").datetime.now().strftime("%H:%M:%S")
                _cpu = __import__("psutil").cpu_percent(interval=None)
                _ram = __import__("psutil").virtual_memory()
                _line = (
                    f"[{_ts}] [{_hostname}] CPU {_cpu:.1f}% | "
                    f"RAM {_ram.used // 1024 // 1024}MB/{_ram.total // 1024 // 1024}MB"
                )
                for _key in _keys:
                    await redis.rpush(_key, _line)
                    await redis.ltrim(_key, -_max_lines, -1)
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    log_stream_task = asyncio.create_task(_master_log_streamer(), name="master_log_streamer")

    yield

    log_stream_task.cancel()
    try:
        await log_stream_task
    except asyncio.CancelledError:
        pass

    stability_task.cancel()
    try:
        await stability_task
    except asyncio.CancelledError:
        pass

    collector_task.cancel()
    try:
        await collector_task
    except asyncio.CancelledError:
        pass

    if scalper_task is not None:
        scalper_task.cancel()
        try:
            await scalper_task
        except asyncio.CancelledError:
            pass

    if openclaw_self_improve_task is not None:
        openclaw_self_improve_task.cancel()
        try:
            await openclaw_self_improve_task
        except asyncio.CancelledError:
            pass

    await hitl_store.stop()
    await redis.aclose()
    log.info("nexus_api_stopped")


_OPENAPI_TAGS = [
    {
        "name": "meta",
        "description": "🟢 **Liveness & Readiness probes** — בדיקות חיות ומוכנות של ה-API.",
    },
    {
        "name": "cluster",
        "description": (
            "🖥️ **Cluster Topology** — מצב כל הצמתים ב-cluster בזמן אמת.\n\n"
            "מחזיר heartbeats, CPU/RAM, תורי ARQ, ו-SSE streams לסריקת הפלוטה."
        ),
    },
    {
        "name": "business",
        "description": (
            "📊 **Business Intelligence** — אינטליגנציה עסקית מ-Telefix.\n\n"
            "סטטיסטיקות גרידה, החלטות AI, לוג מחשבות, דוחות רווח, ו-War Room."
        ),
    },
    {
        "name": "hitl",
        "description": (
            "🧑‍⚖️ **Human-in-the-Loop** — אישור אנושי למשימות רגישות.\n\n"
            "משימות עם `requires_approval=True` נעצרות כאן עד לאישור ידני."
        ),
    },
    {
        "name": "system",
        "description": (
            "🚨 **System Control** — בקרת חירום ומערכת.\n\n"
            "**PANIC** (< 100ms), **Kill-Switch** מלא, Black Box dumps, Power Profile, Retention Health."
        ),
    },
    {
        "name": "sentinel",
        "description": (
            "🛡️ **Sentinel** — ניטור יציבות המערכת.\n\n"
            "מזהה בעיות לפני שהן הופכות לקריסות, מדדי latency ו-error rates."
        ),
    },
    {
        "name": "incubator",
        "description": (
            "🧪 **Incubator** — מנוע זיהוי נישות ויצירת פרויקטים אוטונומיים.\n\n"
            "God Mode, Kill-Switch, אישור/הריגת פרויקטים."
        ),
    },
    {
        "name": "evolution",
        "description": (
            "🧬 **Evolution Engine** — אבולוציה אוטונומית של הפרויקטים.\n\n"
            "Scout לחיפוש הזדמנויות, Birth-Resolve ללידת פרויקטים חדשים."
        ),
    },
    {
        "name": "projects",
        "description": (
            "📁 **Projects** — ניהול פרויקטים פעילים.\n\n"
            "Telefix, OpenClaw ועוד — start/stop/restart, Architect audit, budget widget."
        ),
    },
    {
        "name": "content",
        "description": (
            "✍️ **Content Factory** — מפעל תוכן AI לקבוצות Telegram.\n\n"
            "יצירה, תצוגה מקדימה, ואישור תוכן שנוצר ע\"י Gemini."
        ),
    },
    {
        "name": "telegram-sessions",
        "description": (
            "🔑 **Telegram sessions (Telethon)** — חשבונות טלגרם, לא חיבורי Redis.\n\n"
            "יצירת סשנים חדשים, רשימת סשנים פעילים, Vault Commander."
        ),
    },
    {
        "name": "swarm",
        "description": (
            "🐝 **Swarm** — ניהול נחיל חשבונות Telegram.\n\n"
            "קבוצות, מלאי סשני טלגרם (Telethon), סריקות מלאות."
        ),
    },
    {
        "name": "modules",
        "description": (
            "🔌 **Modules** — מודולים חיצוניים: OpenClaw ו-Moltbot.\n\n"
            "Fuel Gauge, Financial Pulse, Module Health widgets."
        ),
    },
    {
        "name": "polymarket",
        "description": (
            "📈 **Polymarket** — מסחר בשוק ניבויים מבוסס בלוקצ'יין.\n\n"
            "Dashboard, Orderbook, Manual Orders."
        ),
    },
    {
        "name": "prediction",
        "description": (
            "🔮 **Prediction Engine** — מנוע ניבוי וארביטראז'.\n\n"
            "Poly5M Scalper, Cross-Exchange Arbitrage, Paper/Live Trading, Manual Override."
        ),
    },
    {
        "name": "scalper",
        "description": (
            "⚡ **Scalper** — סקאלפר מהיר ל-Polymarket.\n\n"
            "Simulation Mode, Ledger, News Sentiment Ingestion."
        ),
    },
    {
        "name": "deploy",
        "description": (
            "🚀 **Deploy** — פריסת קוד ל-Workers מרוחקים דרך SSH.\n\n"
            "SSE progress streams, sync, status per node."
        ),
    },
    {
        "name": "config",
        "description": (
            "⚙️ **Config** — שינוי הגדרות בזמן ריצה.\n\n"
            "כותב ל-.env ומבצע hot-reload ללא הפעלה מחדש."
        ),
    },
    {
        "name": "flight_mode",
        "description": (
            "✈️ **Flight Mode** — עצירה מבוקרת של כל הפעולות.\n\n"
            "כמו מצב טיסה בטלפון — עוצר הכל בצורה נקייה."
        ),
    },
    {
        "name": "scan",
        "description": (
            "🔍 **Scan** — סריקת פלוטה מלאה.\n\n"
            "SSE real-time updates, היסטוריית סריקות."
        ),
    },
    {
        "name": "proxy",
        "description": (
            "🌐 **Proxy** — ניהול פרוקסי לפעולות Telegram.\n\n"
            "רוטציה אוטומטית וידנית, היסטוריית רוטציות."
        ),
    },
    {
        "name": "notifications",
        "description": (
            "🔔 **Notifications** — סטטוס שירותי התראות.\n\n"
            "Telegram Bot, Super Scraper status."
        ),
    },
]

_DESCRIPTION = """
## 🤖 Nexus Orchestrator — Control Center API

מערכת אוטומציה מבוזרת ואוטונומית לניהול פעולות Telegram, מסחר ב-Polymarket, וייצור תוכן AI.

---

### 🏗️ ארכיטקטורה

```
Master Node  ──►  Redis Broker  ──►  Worker Nodes (Linux + Windows)
     │                                      │
     └──► FastAPI REST API (port 8001)       └──► ARQ Task Queue
```

### ⚡ יכולות עיקריות

| תחום | תיאור |
|------|--------|
| **Telegram Automation** | גרידה, הוספת משתמשים, ניהול נחיל חשבונות |
| **Polymarket Trading** | מסחר אוטומטי, סקאלפינג, ארביטראז' |
| **Content Factory** | ייצור תוכן AI ב-Gemini לקבוצות Telegram |
| **Incubator** | זיהוי נישות ויצירת פרויקטים אוטונומית |
| **Cluster Management** | ניהול Master + Workers מרוחקים |

### 🔒 אבטחה

- **Rate Limit:** 100 req/min per IP
- **CORS:** localhost:3000 + Tailscale VPN (100.x.x.x)
- **Kill-Switch:** דורש phrase + header auth
- **Request ID:** כל בקשה מקבלת `X-Request-ID` לצורך tracing

### 📡 Real-Time Streams (SSE)

- `GET /api/cluster/fleet/scan/stream` — סריקת פלוטה
- `GET /api/deploy/progress/{node_id}` — התקדמות פריסה
- `GET /api/scan/stream` — סריקה כללית
"""


def _get_swagger_ui_html(openapi_url: str, title: str) -> str:  # noqa: PLR0915
    return f"""<!DOCTYPE html>
<html lang="he" dir="ltr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{title}</title>
  <link rel="icon" type="image/svg+xml" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>🤖</text></svg>">
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css">
  <style>
    :root {{
      --nexus-bg:        #0a0e1a;
      --nexus-surface:   #111827;
      --nexus-surface2:  #1a2235;
      --nexus-border:    #1e2d45;
      --nexus-accent:    #3b82f6;
      --nexus-accent2:   #6366f1;
      --nexus-green:     #10b981;
      --nexus-yellow:    #f59e0b;
      --nexus-red:       #ef4444;
      --nexus-purple:    #8b5cf6;
      --nexus-cyan:      #06b6d4;
      --nexus-text:      #e2e8f0;
      --nexus-muted:     #64748b;
      --nexus-font:      'Inter', 'Segoe UI', system-ui, sans-serif;
      --nexus-mono:      'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace;
    }}

    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    * {{ box-sizing: border-box; margin: 0; padding: 0; }}

    body {{
      background: var(--nexus-bg);
      color: var(--nexus-text);
      font-family: var(--nexus-font);
      min-height: 100vh;
    }}

    /* ── Top Banner ─────────────────────────────────────────────── */
    #nexus-banner {{
      background: linear-gradient(135deg, #0f172a 0%, #1e1b4b 50%, #0f172a 100%);
      border-bottom: 1px solid var(--nexus-border);
      padding: 20px 32px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      position: sticky;
      top: 0;
      z-index: 1000;
      backdrop-filter: blur(12px);
    }}

    #nexus-banner .logo {{
      display: flex;
      align-items: center;
      gap: 14px;
    }}

    #nexus-banner .logo-icon {{
      width: 42px;
      height: 42px;
      background: linear-gradient(135deg, var(--nexus-accent), var(--nexus-purple));
      border-radius: 10px;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 22px;
      box-shadow: 0 0 20px rgba(99,102,241,0.4);
    }}

    #nexus-banner .logo-text h1 {{
      font-size: 18px;
      font-weight: 700;
      background: linear-gradient(90deg, #e2e8f0, #a5b4fc);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      letter-spacing: -0.3px;
    }}

    #nexus-banner .logo-text p {{
      font-size: 11px;
      color: var(--nexus-muted);
      margin-top: 1px;
      letter-spacing: 0.5px;
      text-transform: uppercase;
    }}

    #nexus-banner .badges {{
      display: flex;
      gap: 8px;
      align-items: center;
    }}

    .badge {{
      padding: 4px 10px;
      border-radius: 20px;
      font-size: 11px;
      font-weight: 600;
      letter-spacing: 0.3px;
    }}

    .badge-version {{
      background: rgba(59,130,246,0.15);
      color: var(--nexus-accent);
      border: 1px solid rgba(59,130,246,0.3);
    }}

    .badge-live {{
      background: rgba(16,185,129,0.15);
      color: var(--nexus-green);
      border: 1px solid rgba(16,185,129,0.3);
      animation: pulse-green 2s infinite;
    }}

    .badge-rate {{
      background: rgba(245,158,11,0.12);
      color: var(--nexus-yellow);
      border: 1px solid rgba(245,158,11,0.25);
    }}

    /* ── Language toggle button ──────────────────────────────────── */
    .lang-btn {{
      display: flex;
      align-items: center;
      gap: 6px;
      padding: 5px 12px;
      border-radius: 20px;
      background: rgba(139,92,246,0.12);
      border: 1px solid rgba(139,92,246,0.35);
      color: #a78bfa;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      font-family: var(--nexus-font);
      letter-spacing: 0.3px;
      transition: all 0.2s;
      user-select: none;
    }}

    .lang-btn:hover {{
      background: rgba(139,92,246,0.25);
      border-color: rgba(139,92,246,0.6);
      color: #c4b5fd;
      transform: translateY(-1px);
      box-shadow: 0 3px 10px rgba(139,92,246,0.25);
    }}

    .lang-btn:active {{
      transform: translateY(0);
    }}

    #lang-flag {{
      font-size: 14px;
      line-height: 1;
    }}

    @keyframes pulse-green {{
      0%, 100% {{ box-shadow: 0 0 0 0 rgba(16,185,129,0.4); }}
      50% {{ box-shadow: 0 0 0 4px rgba(16,185,129,0); }}
    }}

    /* ── Swagger UI overrides ────────────────────────────────────── */
    .swagger-ui {{
      font-family: var(--nexus-font) !important;
    }}

    .swagger-ui .wrapper {{
      max-width: 1280px;
      padding: 0 24px;
    }}

    /* Hide default topbar */
    .swagger-ui .topbar {{ display: none !important; }}

    /* Info section */
    .swagger-ui .info {{
      margin: 28px 0 20px;
      padding: 24px 28px;
      background: var(--nexus-surface);
      border: 1px solid var(--nexus-border);
      border-radius: 12px;
      border-left: 3px solid var(--nexus-accent2);
    }}

    .swagger-ui .info .title {{
      font-size: 26px !important;
      font-weight: 700 !important;
      color: var(--nexus-text) !important;
      letter-spacing: -0.5px;
    }}

    .swagger-ui .info .title small {{
      background: linear-gradient(90deg, var(--nexus-accent), var(--nexus-purple));
      color: white !important;
      padding: 3px 10px;
      border-radius: 6px;
      font-size: 12px !important;
      font-weight: 600;
      margin-left: 10px;
      vertical-align: middle;
    }}

    .swagger-ui .info p,
    .swagger-ui .info li,
    .swagger-ui .info td,
    .swagger-ui .info th {{
      color: #94a3b8 !important;
      font-size: 13.5px !important;
      line-height: 1.7;
    }}

    .swagger-ui .info h2, .swagger-ui .info h3 {{
      color: var(--nexus-text) !important;
      font-weight: 600;
      margin: 16px 0 8px;
    }}

    .swagger-ui .info table {{
      border-collapse: collapse;
      width: 100%;
      margin: 12px 0;
    }}

    .swagger-ui .info th {{
      background: var(--nexus-surface2) !important;
      color: var(--nexus-accent) !important;
      font-weight: 600;
      padding: 8px 12px;
      border: 1px solid var(--nexus-border);
      font-size: 12px !important;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }}

    .swagger-ui .info td {{
      padding: 8px 12px;
      border: 1px solid var(--nexus-border);
      background: rgba(17,24,39,0.5);
    }}

    .swagger-ui .info code {{
      background: var(--nexus-surface2) !important;
      color: var(--nexus-cyan) !important;
      padding: 2px 6px;
      border-radius: 4px;
      font-family: var(--nexus-mono);
      font-size: 12px !important;
    }}

    .swagger-ui .info pre {{
      background: #0d1117 !important;
      border: 1px solid var(--nexus-border);
      border-radius: 8px;
      padding: 14px 16px;
      overflow-x: auto;
    }}

    .swagger-ui .info pre code {{
      background: transparent !important;
      color: #7dd3fc !important;
      font-size: 12.5px !important;
      line-height: 1.6;
    }}

    /* Scheme container (servers) */
    .swagger-ui .scheme-container {{
      background: var(--nexus-surface) !important;
      border: 1px solid var(--nexus-border) !important;
      border-radius: 10px;
      padding: 16px 20px !important;
      margin: 0 0 20px;
      box-shadow: none !important;
    }}

    .swagger-ui .schemes > label {{
      color: var(--nexus-muted) !important;
      font-size: 12px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }}

    /* Tag sections */
    .swagger-ui .opblock-tag {{
      background: var(--nexus-surface) !important;
      border: 1px solid var(--nexus-border) !important;
      border-radius: 10px !important;
      margin: 8px 0 !important;
      padding: 0 !important;
      transition: border-color 0.2s;
    }}

    .swagger-ui .opblock-tag:hover {{
      border-color: var(--nexus-accent) !important;
    }}

    .swagger-ui .opblock-tag-section h3 {{
      color: var(--nexus-text) !important;
      font-size: 15px !important;
      font-weight: 600 !important;
      padding: 14px 18px !important;
    }}

    .swagger-ui .opblock-tag small {{
      color: #94a3b8 !important;
      font-size: 12.5px !important;
      font-weight: 400 !important;
    }}

    /* Operation blocks */
    .swagger-ui .opblock {{
      border-radius: 8px !important;
      margin: 5px 0 !important;
      border: 1px solid transparent !important;
      box-shadow: none !important;
      transition: all 0.15s;
    }}

    .swagger-ui .opblock:hover {{
      transform: translateX(2px);
    }}

    /* GET */
    .swagger-ui .opblock.opblock-get {{
      background: rgba(16,185,129,0.06) !important;
      border-color: rgba(16,185,129,0.2) !important;
    }}
    .swagger-ui .opblock.opblock-get .opblock-summary-method {{
      background: var(--nexus-green) !important;
    }}
    .swagger-ui .opblock.opblock-get.is-open {{
      background: rgba(16,185,129,0.1) !important;
      border-color: rgba(16,185,129,0.4) !important;
    }}

    /* POST */
    .swagger-ui .opblock.opblock-post {{
      background: rgba(59,130,246,0.06) !important;
      border-color: rgba(59,130,246,0.2) !important;
    }}
    .swagger-ui .opblock.opblock-post .opblock-summary-method {{
      background: var(--nexus-accent) !important;
    }}
    .swagger-ui .opblock.opblock-post.is-open {{
      background: rgba(59,130,246,0.1) !important;
      border-color: rgba(59,130,246,0.4) !important;
    }}

    /* PATCH */
    .swagger-ui .opblock.opblock-patch {{
      background: rgba(245,158,11,0.06) !important;
      border-color: rgba(245,158,11,0.2) !important;
    }}
    .swagger-ui .opblock.opblock-patch .opblock-summary-method {{
      background: var(--nexus-yellow) !important;
    }}

    /* DELETE */
    .swagger-ui .opblock.opblock-delete {{
      background: rgba(239,68,68,0.06) !important;
      border-color: rgba(239,68,68,0.2) !important;
    }}
    .swagger-ui .opblock.opblock-delete .opblock-summary-method {{
      background: var(--nexus-red) !important;
    }}

    /* Method badge */
    .swagger-ui .opblock-summary-method {{
      border-radius: 5px !important;
      font-family: var(--nexus-mono) !important;
      font-size: 11px !important;
      font-weight: 700 !important;
      letter-spacing: 0.5px;
      min-width: 62px !important;
      text-align: center;
    }}

    /* Summary path */
    .swagger-ui .opblock-summary-path {{
      font-family: var(--nexus-mono) !important;
      font-size: 13px !important;
      color: #cbd5e1 !important;
      font-weight: 500;
    }}

    .swagger-ui .opblock-summary-path span {{
      color: #7dd3fc !important;
    }}

    /* Summary description */
    .swagger-ui .opblock-summary-description {{
      color: var(--nexus-muted) !important;
      font-size: 12.5px !important;
    }}

    /* Expanded block body */
    .swagger-ui .opblock-body {{
      background: #0d1117 !important;
      border-top: 1px solid var(--nexus-border) !important;
    }}

    .swagger-ui .opblock-description-wrapper p,
    .swagger-ui .opblock-external-docs-wrapper p {{
      color: #94a3b8 !important;
      font-size: 13px !important;
      padding: 12px 16px;
    }}

    /* Parameters */
    .swagger-ui .parameters-col_description p {{
      color: #94a3b8 !important;
      font-size: 12.5px !important;
    }}

    .swagger-ui table.parameters {{
      background: transparent !important;
    }}

    .swagger-ui .parameter__name {{
      font-family: var(--nexus-mono) !important;
      color: #7dd3fc !important;
      font-size: 13px !important;
    }}

    .swagger-ui .parameter__type {{
      color: var(--nexus-purple) !important;
      font-family: var(--nexus-mono) !important;
      font-size: 11px !important;
    }}

    .swagger-ui .parameter__in {{
      color: var(--nexus-muted) !important;
      font-size: 11px !important;
    }}

    /* Inputs */
    .swagger-ui input[type=text],
    .swagger-ui input[type=password],
    .swagger-ui textarea,
    .swagger-ui select {{
      background: var(--nexus-surface2) !important;
      border: 1px solid var(--nexus-border) !important;
      color: var(--nexus-text) !important;
      border-radius: 6px !important;
      font-family: var(--nexus-mono) !important;
      font-size: 12.5px !important;
      padding: 8px 10px !important;
      transition: border-color 0.2s;
    }}

    .swagger-ui input[type=text]:focus,
    .swagger-ui textarea:focus {{
      border-color: var(--nexus-accent) !important;
      outline: none !important;
      box-shadow: 0 0 0 3px rgba(59,130,246,0.15) !important;
    }}

    /* Buttons */
    .swagger-ui .btn {{
      border-radius: 6px !important;
      font-size: 12.5px !important;
      font-weight: 600 !important;
      letter-spacing: 0.2px;
      transition: all 0.15s !important;
    }}

    .swagger-ui .btn.execute {{
      background: linear-gradient(135deg, var(--nexus-accent), var(--nexus-accent2)) !important;
      border: none !important;
      color: white !important;
      padding: 8px 20px !important;
      box-shadow: 0 2px 8px rgba(99,102,241,0.35) !important;
    }}

    .swagger-ui .btn.execute:hover {{
      transform: translateY(-1px);
      box-shadow: 0 4px 14px rgba(99,102,241,0.5) !important;
    }}

    .swagger-ui .btn.authorize {{
      background: rgba(16,185,129,0.1) !important;
      border: 1px solid var(--nexus-green) !important;
      color: var(--nexus-green) !important;
    }}

    .swagger-ui .btn.cancel {{
      background: rgba(239,68,68,0.1) !important;
      border: 1px solid var(--nexus-red) !important;
      color: var(--nexus-red) !important;
    }}

    /* Response section */
    .swagger-ui .responses-inner {{
      background: #0d1117 !important;
      border-radius: 0 0 8px 8px;
    }}

    .swagger-ui .response-col_status {{
      color: var(--nexus-green) !important;
      font-family: var(--nexus-mono) !important;
      font-weight: 700;
    }}

    .swagger-ui .response-col_description p {{
      color: #94a3b8 !important;
      font-size: 12.5px !important;
    }}

    /* Code / JSON */
    .swagger-ui .microlight,
    .swagger-ui .highlight-code {{
      background: #0d1117 !important;
      border: 1px solid var(--nexus-border) !important;
      border-radius: 6px !important;
      font-family: var(--nexus-mono) !important;
      font-size: 12px !important;
      line-height: 1.6;
      color: #e2e8f0 !important;
    }}

    .swagger-ui .copy-to-clipboard {{
      background: var(--nexus-surface2) !important;
      border: 1px solid var(--nexus-border) !important;
      border-radius: 4px !important;
    }}

    /* Model schemas */
    .swagger-ui .model-box {{
      background: var(--nexus-surface2) !important;
      border: 1px solid var(--nexus-border) !important;
      border-radius: 8px !important;
    }}

    .swagger-ui .model-title {{
      color: var(--nexus-text) !important;
      font-weight: 600;
    }}

    .swagger-ui .prop-type {{
      color: var(--nexus-purple) !important;
      font-family: var(--nexus-mono) !important;
    }}

    .swagger-ui .prop-format {{
      color: var(--nexus-cyan) !important;
      font-family: var(--nexus-mono) !important;
    }}

    /* Section headers */
    .swagger-ui .opblock-section-header {{
      background: var(--nexus-surface2) !important;
      border-bottom: 1px solid var(--nexus-border) !important;
    }}

    .swagger-ui .opblock-section-header label span,
    .swagger-ui .opblock-section-header h4 {{
      color: var(--nexus-text) !important;
      font-size: 12px !important;
      font-weight: 600 !important;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }}

    /* Scrollbar */
    ::-webkit-scrollbar {{ width: 6px; height: 6px; }}
    ::-webkit-scrollbar-track {{ background: var(--nexus-bg); }}
    ::-webkit-scrollbar-thumb {{ background: var(--nexus-border); border-radius: 3px; }}
    ::-webkit-scrollbar-thumb:hover {{ background: var(--nexus-muted); }}

    /* Response status codes */
    .swagger-ui .responses-table .response-col_status {{
      font-size: 13px;
    }}

    /* Loading */
    .swagger-ui .loading-container {{
      background: var(--nexus-bg) !important;
    }}

    /* Filter */
    .swagger-ui .filter-container {{
      background: var(--nexus-surface) !important;
      border: 1px solid var(--nexus-border) !important;
      border-radius: 8px;
      padding: 10px 16px;
      margin: 0 0 16px;
    }}

    .swagger-ui .filter-container .operation-filter-input {{
      border: none !important;
      background: transparent !important;
      color: var(--nexus-text) !important;
      font-size: 13.5px !important;
    }}

    /* Arrow icons */
    .swagger-ui .arrow {{
      fill: var(--nexus-muted) !important;
    }}

    /* Auth modal */
    .swagger-ui .dialog-ux .modal-ux {{
      background: var(--nexus-surface) !important;
      border: 1px solid var(--nexus-border) !important;
      border-radius: 12px !important;
    }}

    .swagger-ui .dialog-ux .modal-ux-header {{
      background: var(--nexus-surface2) !important;
      border-bottom: 1px solid var(--nexus-border) !important;
      border-radius: 12px 12px 0 0 !important;
    }}

    .swagger-ui .dialog-ux .modal-ux-header h3 {{
      color: var(--nexus-text) !important;
    }}

    /* Misc text */
    .swagger-ui p, .swagger-ui li, .swagger-ui label {{
      color: #94a3b8 !important;
    }}

    .swagger-ui h4, .swagger-ui h5 {{
      color: var(--nexus-text) !important;
    }}

    .swagger-ui .required {{
      color: var(--nexus-red) !important;
    }}

    .swagger-ui .renderedMarkdown p {{
      color: #94a3b8 !important;
      font-size: 13px !important;
    }}

    /* Footer */
    #nexus-footer {{
      text-align: center;
      padding: 24px;
      color: var(--nexus-muted);
      font-size: 12px;
      border-top: 1px solid var(--nexus-border);
      margin-top: 40px;
    }}

    #nexus-footer span {{
      color: var(--nexus-accent);
    }}
  </style>
</head>
<body>

<div id="nexus-banner">
  <div class="logo">
    <div class="logo-icon">🤖</div>
    <div class="logo-text">
      <h1>Nexus Orchestrator</h1>
      <p id="banner-subtitle">Control Center API · Distributed Agentic Workflow System</p>
    </div>
  </div>
  <div class="badges">
    <span class="badge badge-live">● LIVE</span>
    <span class="badge badge-version">v1.0.0</span>
    <span class="badge badge-rate">100 req/min</span>
    <button id="lang-toggle" class="lang-btn" onclick="toggleLang()" title="Switch language / החלף שפה">
      <span id="lang-flag">🇮🇱</span>
      <span id="lang-label">עברית</span>
    </button>
  </div>
</div>

<div id="swagger-ui"></div>

<div id="nexus-footer">
  <span id="footer-text">Nexus Orchestrator · Built with <span>FastAPI</span> · Redis + ARQ · <span>22 API modules</span> · Master/Worker Architecture</span>
</div>

<script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script>
<script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-standalone-preset.js"></script>
<script>
  // ── i18n translations ────────────────────────────────────────────────────────
  const TRANSLATIONS = {{
    he: {{
      lang: 'he',
      dir: 'rtl',
      flag: '🇮🇱',
      btnLabel: 'English',
      subtitle: 'ממשק בקרה API · מערכת זרימת עבודה מבוזרת ואוטונומית',
      footer: 'Nexus Orchestrator · נבנה עם <span style="color:var(--nexus-accent)">FastAPI</span> · Redis + ARQ · <span style="color:var(--nexus-accent)">22 מודולי API</span> · ארכיטקטורת Master/Worker',
      tags: {{
        'meta':          '🟢 בדיקות חיות — Liveness & Readiness probes',
        'cluster':       '🖥️ טופולוגיית Cluster — מצב כל הצמתים בזמן אמת',
        'business':      '📊 אינטליגנציה עסקית — נתונים מ-Telefix',
        'hitl':          '🧑‍⚖️ אישור אנושי — Human-in-the-Loop',
        'system':        '🚨 בקרת חירום — PANIC, Kill-Switch, Black Box',
        'sentinel':      '🛡️ ניטור יציבות — Sentinel',
        'incubator':     '🧪 מנוע נישות — Incubator',
        'evolution':     '🧬 אבולוציה אוטונומית — Evolution Engine',
        'projects':      '📁 ניהול פרויקטים — Projects',
        'content':       '✍️ מפעל תוכן AI — Content Factory',
        'sessions':      '🔑 ניהול סשנים — Telegram Sessions',
        'swarm':         '🐝 ניהול נחיל — Swarm',
        'modules':       '🔌 מודולים חיצוניים — OpenClaw & Moltbot',
        'polymarket':    '📈 מסחר Polymarket',
        'polymarket-god-mode': '📈 מסחר Polymarket',
        'prediction':    '🔮 מנוע ניבוי — Prediction & Arbitrage',
        'scalper':       '⚡ סקאלפר מהיר — Scalper',
        'deploy':        '🚀 פריסה לשרתים — Deploy via SSH',
        'config':        '⚙️ הגדרות חיות — Live Config',
        'flight_mode':   '✈️ מצב טיסה — Flight Mode',
        'scan':          '🔍 סריקת פלוטה — Fleet Scan',
        'proxy':         '🌐 ניהול פרוקסי — Proxy',
        'notifications': '🔔 התראות — Notifications',
      }},
    }},
    en: {{
      lang: 'en',
      dir: 'ltr',
      flag: '🇺🇸',
      btnLabel: 'עברית',
      subtitle: 'Control Center API · Distributed Agentic Workflow System',
      footer: 'Nexus Orchestrator · Built with <span style="color:var(--nexus-accent)">FastAPI</span> · Redis + ARQ · <span style="color:var(--nexus-accent)">22 API modules</span> · Master/Worker Architecture',
      tags: {{
        'meta':          '🟢 Liveness & Readiness probes',
        'cluster':       '🖥️ Cluster Topology — live node status',
        'business':      '📊 Business Intelligence — Telefix data',
        'hitl':          '🧑‍⚖️ Human-in-the-Loop approvals',
        'system':        '🚨 Emergency Control — PANIC, Kill-Switch, Black Box',
        'sentinel':      '🛡️ Stability Monitor — Sentinel',
        'incubator':     '🧪 Niche Discovery — Incubator',
        'evolution':     '🧬 Autonomous Evolution Engine',
        'projects':      '📁 Project Management',
        'content':       '✍️ AI Content Factory',
        'sessions':      '🔑 Telegram Session Management',
        'swarm':         '🐝 Account Swarm Management',
        'modules':       '🔌 External Modules — OpenClaw & Moltbot',
        'polymarket':    '📈 Polymarket Trading',
        'polymarket-god-mode': '📈 Polymarket Trading',
        'prediction':    '🔮 Prediction & Arbitrage Engine',
        'scalper':       '⚡ Fast Scalper',
        'deploy':        '🚀 Remote Deploy via SSH',
        'config':        '⚙️ Live Configuration',
        'flight_mode':   '✈️ Flight Mode — Controlled Stop',
        'scan':          '🔍 Fleet Scan',
        'proxy':         '🌐 Proxy Management',
        'notifications': '🔔 Notifications',
      }},
    }},
  }};

  let currentLang = localStorage.getItem('nexus-docs-lang') || 'he';

  function applyLang(lang) {{
    const t = TRANSLATIONS[lang];
    document.documentElement.lang = t.lang;
    document.getElementById('lang-flag').textContent = t.flag;
    document.getElementById('lang-label').textContent = t.btnLabel;
    document.getElementById('banner-subtitle').textContent = t.subtitle;
    document.getElementById('footer-text').innerHTML = t.footer;

    // Allow re-translation after language switch (labels differ per locale).
    document.querySelectorAll('.opblock-tag h3 a[data-nexus-label]').forEach((a) =>
      a.removeAttribute('data-nexus-label')
    );
    translateTags(t.tags);
  }}

  function translateTags(tagMap) {{
    // Replace the entire <a> label once per tag+lang. Do NOT prepend text nodes:
    // Swagger UI nests <span>/<small> inside <a>; prepending caused "Engineprediction"
    // style concatenation. MutationObserver + old translateTags also re-fired endlessly.
    document.querySelectorAll('.opblock-tag[data-tag]').forEach(el => {{
      const tag = el.getAttribute('data-tag');
      const label = tagMap[tag];
      if (!label) return;
      const link = el.querySelector('h3 a');
      if (!link) return;
      if (link.getAttribute('data-nexus-label') === label) return;
      const svgs = Array.from(link.querySelectorAll(':scope > svg'));
      while (link.firstChild) link.removeChild(link.firstChild);
      svgs.forEach(svg => link.appendChild(svg));
      if (svgs.length) link.appendChild(document.createTextNode('\\u00A0'));
      link.appendChild(document.createTextNode(label));
      link.setAttribute('data-nexus-label', label);
    }});
  }}

  function toggleLang() {{
    currentLang = currentLang === 'he' ? 'en' : 'he';
    localStorage.setItem('nexus-docs-lang', currentLang);
    applyLang(currentLang);

    // Re-translate after a short delay (Swagger may re-render on filter/expand)
    setTimeout(() => translateTags(TRANSLATIONS[currentLang].tags), 400);
  }}

  // ── Swagger UI init ──────────────────────────────────────────────────────────
  window.onload = function() {{
    const ui = SwaggerUIBundle({{
      url: "{openapi_url}",
      dom_id: '#swagger-ui',
      presets: [
        SwaggerUIBundle.presets.apis,
        SwaggerUIStandalonePreset
      ],
      layout: "StandaloneLayout",
      deepLinking: true,
      displayRequestDuration: true,
      defaultModelsExpandDepth: 1,
      defaultModelExpandDepth: 2,
      docExpansion: "list",
      filter: true,
      showExtensions: true,
      showCommonExtensions: true,
      tryItOutEnabled: false,
      syntaxHighlight: {{
        activate: true,
        theme: "monokai"
      }},
      requestInterceptor: (req) => {{
        req.headers['X-Request-Source'] = 'swagger-ui';
        return req;
      }},
      onComplete: () => {{
        applyLang(currentLang);
        // Debounced: filter/expand mutates DOM; translating must stay idempotent (see translateTags).
        let tmo = null;
        const container = document.getElementById('swagger-ui');
        if (container) {{
          new MutationObserver(() => {{
            if (tmo) clearTimeout(tmo);
            tmo = setTimeout(() => {{
              tmo = null;
              translateTags(TRANSLATIONS[currentLang].tags);
            }}, 200);
          }}).observe(container, {{ childList: true, subtree: true }});
        }}
      }},
    }});
    window.ui = ui;
  }};
</script>
</body>
</html>"""


def create_app() -> FastAPI:
    app = FastAPI(
        title="Nexus Orchestrator — Control Center",
        description=_DESCRIPTION,
        version="1.0.0",
        lifespan=lifespan,
        docs_url=None,
        redoc_url="/redoc",
        openapi_tags=_OPENAPI_TAGS,
        contact={
            "name": "Nexus Control Center",
            "url": "http://localhost:8001",
        },
        license_info={
            "name": "Private — All Rights Reserved",
        },
    )

    # ── WebSocket: Live log stream for any node (Master Terminal) ─────────────
    # Registered BEFORE middleware so SlowAPI / CORS don't block the WS upgrade.
    @app.websocket("/api/v1/swarm/nodes/{node_id}/log_stream")
    async def node_log_stream(websocket: WebSocket, node_id: str) -> None:
        """
        Real-time log stream for a node.  Reads from Redis list
        ``nexus:log_stream:{node_id}`` (newest entries pushed by the master/worker)
        and streams them to the WebSocket client.  Falls back to a heartbeat
        keep-alive every 2 s when no new lines are available.
        """
        # Accept FIRST — before any state access so uvicorn never returns 403.
        await websocket.accept()
        _redis: Redis | None = getattr(websocket.app.state, "redis", None)
        if _redis is None:
            await websocket.send_text(__import__("json").dumps({"error": "Redis not ready", "node_id": node_id}))
            await websocket.close()
            return
        key = f"nexus:log_stream:{node_id}"
        last_len = 0
        try:
            while True:
                try:
                    current_len = await _redis.llen(key)
                    if current_len > last_len:
                        new_entries = await _redis.lrange(key, last_len, current_len - 1)
                        for entry in new_entries:
                            line = entry.decode() if isinstance(entry, bytes) else str(entry)
                            await websocket.send_text(
                                __import__("json").dumps({"line": line, "node_id": node_id})
                            )
                        last_len = current_len
                    else:
                        await websocket.send_text(
                            __import__("json").dumps({"heartbeat": True, "node_id": node_id})
                        )
                except WebSocketDisconnect:
                    break
                except Exception:
                    break
                await asyncio.sleep(2.0)
        except WebSocketDisconnect:
            pass
        except Exception:
            pass

    # ── Rate limiting ──────────────────────────────────────────────────────────
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.add_middleware(SlowAPIMiddleware)

    # ── CORS ───────────────────────────────────────────────────────────────────
    # Allow localhost dev server and Tailscale VPN range (100.x.x.x).
    # Tailscale origins are matched by allow_origin_regex — no CIDR strings here.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:3000",
            "http://127.0.0.1:3000",
        ],
        allow_origin_regex=r"http://100\.\d+\.\d+\.\d+(:\d+)?",
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Request ID middleware ──────────────────────────────────────────────────
    @app.middleware("http")
    async def add_request_id(request: Request, call_next):  # type: ignore[no-untyped-def]
        request_id = str(uuid.uuid4())[:8]
        structlog.contextvars.bind_contextvars(request_id=request_id)
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        structlog.contextvars.unbind_contextvars("request_id")
        return response

    # ── Global exception handler ───────────────────────────────────────────────
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        log.exception(
            "unhandled_api_exception",
            path=str(request.url.path),
            method=request.method,
            error=str(exc),
        )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "detail": "Internal server error",
                "error": type(exc).__name__,
                "path": str(request.url.path),
            },
        )

    # ── Routers ────────────────────────────────────────────────────────────────
    app.include_router(ai.router, prefix="/api")
    app.include_router(cluster.router, prefix="/api")
    app.include_router(hitl.router, prefix="/api")
    app.include_router(business.router, prefix="/api")
    app.include_router(content.router, prefix="/api")
    app.include_router(notifications.router, prefix="/api")
    app.include_router(incubator.router, prefix="/api")
    app.include_router(management_dashboard.router, prefix="/api")
    app.include_router(evolution.router, prefix="/api")
    app.include_router(factory.router, prefix="/api")
    app.include_router(deploy.router, prefix="/api")
    app.include_router(config.router, prefix="/api")
    app.include_router(projects.router, prefix="/api")
    app.include_router(modules.router, prefix="/api")
    app.include_router(openclaw_control.router, prefix="/api")
    app.include_router(polymarket.router, prefix="/api")
    app.include_router(prediction.router, prefix="/api")
    app.include_router(scalper.router, prefix="/api")
    app.include_router(sentinel.router, prefix="/api")
    app.include_router(sessions.router, prefix="/api")
    app.include_router(swarm.router, prefix="/api")
    # Dashboard (NexusOsGodMode) calls this path; keep in sync with WebSocket /api/v1/swarm/nodes/...
    app.add_api_route(
        "/api/v1/swarm/force-sync",
        swarm.force_git_pull_swarm,
        methods=["POST"],
        tags=["swarm"],
        summary="Broadcast FORCE_GIT_PULL (v1 path alias)",
    )
    app.include_router(system.router, prefix="/api")
    app.include_router(flight_mode.router, prefix="/api")
    app.include_router(scan.router, prefix="/api")
    app.include_router(seo.router, prefix="/api")
    app.include_router(proxy.router, prefix="/api")
    app.include_router(telefix.router, prefix="/api")
    app.include_router(group_infiltration.router, prefix="/api")
    app.include_router(factory_dashboard.router, prefix="/api")
    app.include_router(telefix_dashboard.router, prefix="/api")
    app.include_router(ahu.router, prefix="/api")

    # ── Custom Swagger UI docs ─────────────────────────────────────────────────
    @app.get("/docs", include_in_schema=False)
    async def custom_swagger_ui() -> HTMLResponse:
        return HTMLResponse(_get_swagger_ui_html(
            openapi_url=app.openapi_url or "/openapi.json",
            title="Nexus Orchestrator — Control Center",
        ))

    # ── Root redirect ──────────────────────────────────────────────────────────
    @app.get("/", include_in_schema=False)
    async def root_redirect() -> RedirectResponse:
        return RedirectResponse(url="/docs")

    # ── Health / readiness probes ──────────────────────────────────────────────
    @app.get("/health", tags=["meta"], summary="Liveness probe")
    async def health() -> dict[str, str]:
        return {"status": "ok", "version": "1.0.0"}

    @app.get("/ready", tags=["meta"], summary="Readiness probe")
    async def ready(request: Request) -> dict[str, str]:
        try:
            await request.app.state.redis.ping()
            degraded = bool(getattr(request.app.state, "redis_degraded", False))
            return {
                "status": "ready",
                "redis": "degraded" if degraded else "ok",
            }
        except Exception:
            return JSONResponse(  # type: ignore[return-value]
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "not_ready", "redis": "unreachable"},
            )

    return app


app = create_app()
