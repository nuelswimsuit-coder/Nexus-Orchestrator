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
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from urllib.parse import unquote, urlparse

import structlog
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from redis.asyncio import Redis
from slowapi import Limiter, _rate_limit_exceeded_handler  # type: ignore[import-untyped]
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from nexus.api.hitl_store import HitlStore
from nexus.api.routers import business, cluster, config, content, deploy, evolution, flight_mode, hitl, incubator, modules, notifications, prediction, projects, sentinel, system
from nexus.shared.config import settings
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


async def _connect_redis_with_retry(redis_url: str) -> Redis:
    """
    Connect to Redis with bounded retry/backoff and reduced warning noise.
    """
    attempt = 0
    delay_s = 1.0
    while True:
        attempt += 1
        client = _build_redis_client(redis_url)
        try:
            await client.ping()
            if attempt > 1:
                log.info("api_redis_recovered", attempts=attempt)
            return client
        except Exception as exc:
            await client.aclose()
            # Log only first, every 5th, and the immediate retry attempt.
            if attempt == 1 or attempt == 2 or attempt % 5 == 0:
                log.warning(
                    "api_redis_connect_retry",
                    attempt=attempt,
                    retry_in_s=round(delay_s, 2),
                    error=str(exc),
                )
            await asyncio.sleep(delay_s)
            delay_s = min(delay_s * 1.7, 10.0)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage startup and shutdown of shared resources."""
    configure_logging(level="ERROR", node_id=f"{settings.node_id}-api")

    redis: Redis = await _connect_redis_with_retry(settings.redis_url)
    app.state.redis = redis
    log.info("api_redis_connected", url=settings.redis_url)

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

    log.info("nexus_api_started", docs="/docs", rate_limit="100/min")

    yield

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

    await hitl_store.stop()
    await redis.aclose()
    log.info("nexus_api_stopped")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Nexus Orchestrator — Control Center",
        description=(
            "Production REST API for the Nexus distributed agentic workflow system. "
            "Monitors cluster health, manages HITL approvals, and drives the "
            "autonomous profit engine."
        ),
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # ── Rate limiting ──────────────────────────────────────────────────────────
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

    # ── CORS ───────────────────────────────────────────────────────────────────
    # Allow localhost dev server, Tailscale VPN range (100.x.x.x), and LAN.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            # Tailscale VPN — allow all 100.x.x.x origins for mobile access
            "http://100.0.0.0/8",
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
    app.include_router(cluster.router, prefix="/api")
    app.include_router(hitl.router, prefix="/api")
    app.include_router(business.router, prefix="/api")
    app.include_router(content.router, prefix="/api")
    app.include_router(notifications.router, prefix="/api")
    app.include_router(incubator.router, prefix="/api")
    app.include_router(evolution.router, prefix="/api")
    app.include_router(deploy.router, prefix="/api")
    app.include_router(config.router, prefix="/api")
    app.include_router(projects.router, prefix="/api")
    app.include_router(modules.router, prefix="/api")
    app.include_router(prediction.router, prefix="/api")
    app.include_router(sentinel.router, prefix="/api")
    app.include_router(system.router, prefix="/api")
    app.include_router(flight_mode.router, prefix="/api")

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
            return {"status": "ready", "redis": "ok"}
        except Exception:
            return JSONResponse(  # type: ignore[return-value]
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "not_ready", "redis": "unreachable"},
            )

    return app


app = create_app()
