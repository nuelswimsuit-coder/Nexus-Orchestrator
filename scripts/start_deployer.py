from __future__ import annotations

import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.getcwd())

# Suppress asyncio debug noise and paramiko buffer flooding
os.environ.setdefault("PYTHONASYNCIODEBUG", "0")

import logging as _logging
_logging.getLogger("paramiko").setLevel(_logging.CRITICAL)
_logging.getLogger("paramiko.transport").setLevel(_logging.CRITICAL)

"""
Hatan Industries — Nexus Deployer Facility
===========================================

Standalone HTTP surface for :class:`nexus.services.deployer.DeployerService`
via the existing FastAPI deploy router (cluster push, sync, SSE progress).

Usage
-----
    python scripts/start_deployer.py

Environment
-----------
    NEXUS_DEPLOYER_PORT   listen port (default 8002; host is always 0.0.0.0 via deployer_api_bind)

Endpoints match the main API under ``/api/deploy/*`` (e.g. ``POST /api/deploy/cluster``).

Note: Port 8002 is the deployer facility port — run the full Control Center on 8001
and this deployer facility on 8002 simultaneously without conflict.
"""

import asyncio
import logging
import pathlib
import sqlite3
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
elif os.environ.get("ENVIRONMENT", "PRODUCTION").upper() == "PRODUCTION":
    try:
        import uvloop  # type: ignore[import-not-found]

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except Exception:
        pass

_PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent

import structlog  # noqa: E402
import uvicorn  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from fastapi import FastAPI, Request, status  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse  # noqa: E402
from typing import Any  # noqa: E402

for _env in (_PROJECT_ROOT / "configs" / ".env", _PROJECT_ROOT / ".env"):
    load_dotenv(_env, override=False)

try:
    import ujson  # noqa: E402
except ImportError:
    import json as ujson  # type: ignore[no-redef]  # noqa: E402

from nexus.api.routers import deploy  # noqa: E402
from nexus.shared.config import settings  # noqa: E402
from nexus.shared.redis_util import apply_redis_url_to_environment  # noqa: E402

apply_redis_url_to_environment()

# ── Telefix DB — auto-locate with Desktop → project-root relocation ───────────

def _locate_telefix_db() -> pathlib.Path:
    """
    Locate telefix.db with the following priority:
    1. TELEFIX_DB_PATH env var (explicit override)
    2. Project root (next to this script's parent)
    3. Desktop (Windows: ~/Desktop/telefix.db or ~/Desktop/Nexus-Orchestrator/telefix.db)
       → auto-copy to project root on first find
    4. CRASH with a clear message if not found anywhere.

    All SQLite connections opened against the returned path must use
    check_same_thread=False.
    """
    # 1. Explicit env override
    env_path = os.environ.get("TELEFIX_DB_PATH", "").strip()
    if env_path:
        p = pathlib.Path(env_path)
        if p.exists():
            print(f"✅ [TELEFIX] DB from TELEFIX_DB_PATH: {p}", flush=True)
            return p

    # 2. Project root
    project_root = _PROJECT_ROOT
    canonical = project_root / "telefix.db"
    if canonical.exists():
        print(f"✅ [TELEFIX] DB at project root: {canonical}", flush=True)
        return canonical

    # 3. Desktop search + auto-copy
    desktop_candidates: list[pathlib.Path] = []
    home = pathlib.Path.home()
    for desktop_dir in (
        home / "Desktop",
        home / "OneDrive" / "Desktop",
        pathlib.Path("C:/Users/Yarin/Desktop"),
    ):
        desktop_candidates.append(desktop_dir / "telefix.db")
        desktop_candidates.append(desktop_dir / "Nexus-Orchestrator" / "telefix.db")

    for candidate in desktop_candidates:
        if candidate.exists():
            import shutil as _shutil
            print(
                f"⚠️  [TELEFIX] DB found on Desktop at {candidate} — "
                f"auto-relocating to project root: {canonical}",
                flush=True,
            )
            try:
                _shutil.copy2(str(candidate), str(canonical))
                print(f"✅ [TELEFIX] DB relocated to: {canonical}", flush=True)
                return canonical
            except Exception as _copy_exc:
                print(
                    f"⚠️  [TELEFIX] Copy failed ({_copy_exc}), using source path: {candidate}",
                    flush=True,
                )
                return candidate

    # 4. Not found anywhere — self-heal: create a fresh empty DB with schema
    print(
        f"⚠️  [TELEFIX] telefix.db not found in any search path — "
        f"triggering create_default_db() at: {canonical}",
        flush=True,
    )
    try:
        from nexus.shared.db_util import create_default_db as _create_db  # noqa: PLC0415
        from pathlib import Path as _Path  # noqa: PLC0415
        _create_db(_Path(canonical))
        print(f"✅ [TELEFIX] Self-healed: fresh telefix.db created at {canonical}", flush=True)
    except Exception as _heal_exc:
        print(
            f"❌ [TELEFIX] Self-heal failed ({_heal_exc}) — "
            f"deployer will start without telefix.db; analytics will be unavailable.",
            flush=True,
        )
    return canonical


_TELEFIX_DB_PATH = _locate_telefix_db()
_TELEFIX_DB = str(_TELEFIX_DB_PATH)


# ── Hatan Industries console branding ─────────────────────────────────────────

_BANNER = r"""
╔══════════════════════════════════════════════════════════════════╗
║  HATAN INDUSTRIES  ·  NEXUS DEPLOYER FACILITY                    ║
║  Zero-touch cluster push  ·  SSH sync  ·  live progress (SSE)     ║
╚══════════════════════════════════════════════════════════════════╝
"""


def _hatan_print(tag: str, message: str) -> None:
    print(f"| HATAN INDUSTRIES | {tag:12} | {message}", flush=True)


def _patch_redis_for_environment() -> None:
    """Rewrite Redis URL for the current runtime environment.

    - Docker: ``localhost`` / ``127.0.0.1`` → ``host.docker.internal``.
    - Windows (non-Docker): ``localhost`` → ``127.0.0.1`` to force IPv4 and
      prevent the "database not found" error caused by Windows resolving
      ``localhost`` to the IPv6 loopback (::1) before IPv4.
    """
    in_docker = (
        pathlib.Path("/.dockerenv").exists()
        or os.environ.get("DOCKER_CONTAINER", "").lower() in ("1", "true", "yes")
        or os.environ.get("RUNNING_IN_DOCKER", "").lower() in ("1", "true", "yes")
    )
    if in_docker:
        original = settings.redis_url
        settings.redis_url = (
            original.replace("localhost", "host.docker.internal").replace(
                "127.0.0.1", "host.docker.internal"
            )
        )
        _hatan_print("REDIS", f"docker remap {original} → {settings.redis_url}")
    elif sys.platform == "win32" and "localhost" in settings.redis_url:
        original = settings.redis_url
        settings.redis_url = original.replace("localhost", "127.0.0.1")
        _hatan_print("REDIS", f"win32 ipv4 fix {original} → {settings.redis_url}")
    else:
        _hatan_print("REDIS", f"broker {settings.redis_url}")


def configure_hatan_logging(level: str = "INFO") -> None:
    """
    Structlog → stdout, every line prefixed with the Hatan Industries rail style.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    def _hatan_rail(
        logger: logging.Logger, method_name: str, event_dict: dict[str, object]
    ) -> str:
        ts = str(event_dict.get("timestamp", ""))
        lvl = str(event_dict.get("level", method_name)).upper()
        event = str(event_dict.get("event", method_name))
        skip = frozenset({"timestamp", "level", "event", "logger", "_record", "_from_structlog"})
        parts = [
            f"{k}={event_dict[k]!r}"
            for k in sorted(event_dict.keys())
            if k not in skip
        ]
        tail = " ".join(parts)
        core = f"| HATAN INDUSTRIES | {ts} | {lvl:5} | {event}"
        return f"{core} | {tail}" if tail else core

    structlog.configure(
        processors=shared_processors
        + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            _hatan_rail,
        ],
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)

    structlog.contextvars.bind_contextvars(
        facility="hatan-industries-deployer",
        brand="Hatan Industries",
    )


async def _telefix_db_poller(redis: Any, interval: int = 60) -> None:
    """Background task: reads telefix.db every *interval* seconds and pushes
    aggregated stats to Redis.

    Field mapping:
      - ``revenue``    → ``nexus:analytics:daily_profit``
      - ``user_count`` → ``nexus:analytics:new_users``
    """
    log = structlog.get_logger("hatan.telefix_poller")
    while True:
        try:
            if not os.path.exists(_TELEFIX_DB):
                log.warning("telefix_poll_db_missing", path=_TELEFIX_DB)
            else:
                conn = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: sqlite3.connect(_TELEFIX_DB, timeout=5, check_same_thread=False)
                )
                try:
                    cur = conn.cursor()
                    # Try legacy 'telefix' table first; fall back to 'groups' table.
                    total_profit: float = 0.0
                    new_users: int = 0
                    try:
                        cur.execute("SELECT COALESCE(SUM(revenue), 0) FROM telefix")
                        total_profit = cur.fetchone()[0]
                        cur.execute("SELECT COALESCE(SUM(user_count), 0) FROM telefix")
                        new_users = int(cur.fetchone()[0])
                    except sqlite3.OperationalError:
                        # Self-healed DB uses 'groups' table — count rows as users
                        try:
                            cur.execute("SELECT COUNT(*) FROM groups")
                            row = cur.fetchone()
                            new_users = int(row[0]) if row else 0
                        except Exception:
                            new_users = 0
                    # Push VERIFIED status: >0 groups means data is present
                    verified = "VERIFIED" if new_users > 0 else "UNVERIFIED"
                finally:
                    conn.close()

                await redis.set("nexus:analytics:daily_profit", str(total_profit))
                await redis.set("nexus:analytics:new_users", str(new_users))
                await redis.set("nexus:analytics:verified_status", verified)
                log.info(
                    "telefix_stats_pushed",
                    daily_profit=total_profit,
                    new_users=new_users,
                    verified=verified,
                )
                _hatan_print("TELEFIX", f"profit={total_profit}  users={new_users}  status={verified}")
        except Exception as exc:
            log.warning("telefix_poll_error", error=str(exc))
        await asyncio.sleep(interval)


def create_deployer_app() -> FastAPI:
    """Minimal FastAPI app: Redis + deploy routes only."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        configure_hatan_logging(level=os.environ.get("LOG_LEVEL", "INFO"))
        log = structlog.get_logger("hatan.deployer")
        # Late import — pulls Redis retry helper without starting the full API stack.
        from nexus.api.main import _connect_redis_with_retry  # noqa: PLC0415

        redis, degraded = await _connect_redis_with_retry(settings.redis_url)
        app.state.redis = redis
        app.state.redis_degraded = degraded
        log.info(
            "hatan_deployer_redis_ready",
            degraded=degraded,
            url=settings.redis_url,
        )
        _hatan_print("ONLINE", "DeployerService bound to HTTP — awaiting orders")

        # Start telefix.db → Redis analytics poller.
        poller_task = asyncio.create_task(_telefix_db_poller(redis))
        _hatan_print("TELEFIX", f"DB poller started — path: {_TELEFIX_DB}")

        yield

        poller_task.cancel()
        try:
            await poller_task
        except asyncio.CancelledError:
            pass
        await redis.aclose()
        log.info("hatan_deployer_shutdown")
        _hatan_print("OFFLINE", "Redis connection closed — facility secure")

    app = FastAPI(
        title="Hatan Industries - Nexus Deployer",
        description="Standalone deploy / sync API backed by DeployerService.",
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url=None,
    )

    # ── CORS — allow React dashboard on :3000 and Nexus OS on :8001/:8002 ─────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "http://localhost:8001",
            "http://127.0.0.1:8001",
            "http://localhost:8002",
            "http://127.0.0.1:8002",
        ],
        allow_origin_regex=r"http://100\.\d+\.\d+\.\d+(:\d+)?",
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Legacy dashboard endpoints ─────────────────────────────────────────────

    @app.get(
        "/api/sessions/all",
        tags=["sessions"],
        summary="All active worker sessions (legacy UI endpoint)",
        response_model=None,
    )
    async def sessions_all(request: Request) -> JSONResponse:
        """Return every ``nexus:sessions:*`` key with its stored payload.
        Used by the legacy dashboard to display the full worker list (e.g. 153 workers)."""
        r = getattr(request.app.state, "redis", None)
        if r is None:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "error", "detail": "Redis unavailable"},
            )
        sessions: list[dict[str, Any]] = []
        try:
            cursor = 0
            while True:
                cursor, keys = await r.scan(cursor=cursor, match="nexus:sessions:*", count=200)
                for key in keys:
                    raw = await r.get(key)
                    if raw:
                        try:
                            payload = ujson.loads(raw)
                        except Exception:
                            payload = {"raw": raw}
                    else:
                        payload = {}
                    payload.setdefault("session_key", key)
                    sessions.append(payload)
                if cursor == 0:
                    break
        except Exception as exc:
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"status": "error", "detail": str(exc)},
            )
        return JSONResponse(
            status_code=200,
            content={"status": "ok", "count": len(sessions), "sessions": sessions},
        )

    @app.get(
        "/api/logs/stream",
        tags=["logs"],
        summary="Live trading log stream (SSE)",
        response_model=None,
    )
    async def logs_stream(request: Request) -> StreamingResponse:
        """Server-Sent Events stream of live trading logs from ``nexus:logs:trading`` Redis list.
        The legacy UI subscribes to this endpoint to display real-time log output."""
        r = getattr(request.app.state, "redis", None)

        async def _event_generator() -> AsyncGenerator[str, None]:
            last_index = 0
            while True:
                if await request.is_disconnected():
                    break
                try:
                    if r is not None:
                        entries = await r.lrange("nexus:logs:trading", last_index, last_index + 49)
                        for entry in entries:
                            yield f"data: {entry}\n\n"
                        last_index += len(entries)
                        # Also check pub/sub bus for real-time events
                        bus_entry = await r.get("nexus:logs:latest")
                        if bus_entry:
                            yield f"data: {bus_entry}\n\n"
                except Exception as exc:
                    yield f"data: {{\"error\": \"{exc}\"}}\n\n"
                await asyncio.sleep(1.0)

        return StreamingResponse(
            _event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    @app.post(
        "/api/config/update",
        tags=["config"],
        summary="Update bot parameters from the UI (legacy endpoint)",
        response_model=None,
    )
    async def config_update(request: Request) -> JSONResponse:
        """Accept a JSON body of key/value pairs and persist them to Redis under
        ``nexus:config:*`` so the running bots pick up the new parameters without restart."""
        r = getattr(request.app.state, "redis", None)
        if r is None:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "error", "detail": "Redis unavailable"},
            )
        try:
            body: dict[str, Any] = await request.json()
        except Exception as exc:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"status": "error", "detail": f"Invalid JSON: {exc}"},
            )
        if not isinstance(body, dict):
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"status": "error", "detail": "Body must be a JSON object"},
            )
        updated: list[str] = []
        try:
            for key, value in body.items():
                redis_key = f"nexus:config:{key}"
                await r.set(redis_key, ujson.dumps(value) if not isinstance(value, str) else value)
                updated.append(key)
            # Publish a config-changed event so running agents can react immediately.
            await r.publish("nexus:commands", ujson.dumps({"action": "CONFIG_UPDATE", "keys": updated}))
        except Exception as exc:
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"status": "error", "detail": str(exc)},
            )
        return JSONResponse(
            status_code=200,
            content={"status": "ok", "updated": updated},
        )

    @app.get("/", tags=["meta"], summary="Dashboard redirect", response_model=None, include_in_schema=False)
    async def root() -> HTMLResponse:
        return HTMLResponse(
            content='<html><head><meta http-equiv="refresh" content="0; url=/nexus-os"></head>'
            "<body>Redirecting to <a href='/nexus-os'>Nexus OS Dashboard</a>…</body></html>",
            status_code=200,
            headers={"Cache-Control": "no-cache"},
        )

    @app.get("/health", tags=["meta"], summary="Deployer liveness", response_model=None)
    async def health() -> Any:
        return {"status": "ok", "service": "deployer"}

    @app.get("/nexus-os", tags=["meta"], summary="Nexus OS Dashboard", response_model=None, include_in_schema=False)
    async def nexus_os(request: Request) -> HTMLResponse:
        r = getattr(request.app.state, "redis", None)
        redis_status = "OFFLINE"
        trading_status = "UNKNOWN"
        active_sessions = "N/A"
        last_buy_order = "N/A"
        daily_profit = None
        new_users = None
        if r is not None:
            try:
                await r.ping()
                redis_status = "ONLINE"
                trading_status_raw = await r.get("nexus:trading:status")
                if trading_status_raw:
                    trading_status = trading_status_raw.decode() if isinstance(trading_status_raw, (bytes, bytearray)) else str(trading_status_raw)
                else:
                    trading_status = "IDLE"
                # Scan ALL nexus:sessions:* keys for a live count.
                _session_count = 0
                _cursor = 0
                while True:
                    _cursor, _keys = await r.scan(cursor=_cursor, match="nexus:sessions:*", count=200)
                    _session_count += len(_keys)
                    if _cursor == 0:
                        break
                active_sessions = str(_session_count)
                last_buy_raw = await r.get("nexus:trading:last_buy_order")
                last_buy_order = last_buy_raw.decode() if last_buy_raw else "—"
                # Analytics from telefix.db poller.
                _profit_raw = await r.get("nexus:analytics:daily_profit")
                _users_raw = await r.get("nexus:analytics:new_users")
                daily_profit = _profit_raw.decode() if isinstance(_profit_raw, (bytes, bytearray)) else (str(_profit_raw) if _profit_raw is not None else None)
                new_users = _users_raw.decode() if isinstance(_users_raw, (bytes, bytearray)) else (str(_users_raw) if _users_raw is not None else None)
            except Exception:
                redis_status = "ERROR"

        # Determine stream status: DATA STREAMING when analytics keys are present.
        stream_status = "DATA STREAMING" if (daily_profit is not None and new_users is not None) else "IDLE"
        stream_css = "status-active" if stream_status == "DATA STREAMING" else "status-idle"
        profit_display = f"${daily_profit}" if daily_profit is not None else "—"
        users_display = new_users if new_users is not None else "—"

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta http-equiv="refresh" content="5" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>NEXUS OS — Command Dashboard</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&display=swap');
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    :root {{
      --green: #00ff41;
      --dim-green: #00c832;
      --dark: #0d0d0d;
      --panel: #0a0a0a;
      --border: #1a3a1a;
      --text: #c8ffc8;
      --muted: #4a7a4a;
      --red: #ff3c3c;
      --yellow: #ffe600;
      --cyan: #00e5ff;
    }}
    html, body {{
      height: 100%;
      background: var(--dark);
      color: var(--green);
      font-family: 'Share Tech Mono', 'Courier New', monospace;
      overflow-x: hidden;
    }}
    body::before {{
      content: '';
      position: fixed;
      inset: 0;
      background: repeating-linear-gradient(
        0deg,
        transparent,
        transparent 2px,
        rgba(0,255,65,0.015) 2px,
        rgba(0,255,65,0.015) 4px
      );
      pointer-events: none;
      z-index: 0;
    }}
    .container {{
      position: relative;
      z-index: 1;
      max-width: 1100px;
      margin: 0 auto;
      padding: 2rem 1.5rem;
    }}
    header {{
      text-align: center;
      margin-bottom: 2.5rem;
      border-bottom: 1px solid var(--border);
      padding-bottom: 1.5rem;
    }}
    header .logo {{
      font-size: 2.2rem;
      letter-spacing: 0.3em;
      color: var(--green);
      text-shadow: 0 0 20px var(--green), 0 0 40px rgba(0,255,65,0.4);
    }}
    header .sub {{
      font-size: 0.75rem;
      color: var(--muted);
      letter-spacing: 0.2em;
      margin-top: 0.4rem;
    }}
    .refresh-badge {{
      display: inline-block;
      margin-top: 0.6rem;
      font-size: 0.65rem;
      color: var(--muted);
      border: 1px solid var(--border);
      padding: 0.15rem 0.6rem;
      border-radius: 2px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 1.25rem;
      margin-bottom: 2rem;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 4px;
      padding: 1.4rem 1.6rem;
      position: relative;
      overflow: hidden;
    }}
    .card::before {{
      content: '';
      position: absolute;
      top: 0; left: 0; right: 0;
      height: 2px;
      background: linear-gradient(90deg, transparent, var(--green), transparent);
      opacity: 0.6;
    }}
    .card-label {{
      font-size: 0.65rem;
      letter-spacing: 0.25em;
      color: var(--muted);
      text-transform: uppercase;
      margin-bottom: 0.75rem;
    }}
    .card-value {{
      font-size: 1.6rem;
      letter-spacing: 0.05em;
      color: var(--green);
      text-shadow: 0 0 10px rgba(0,255,65,0.5);
      word-break: break-all;
    }}
    .card-value.status-idle {{ color: var(--yellow); text-shadow: 0 0 10px rgba(255,230,0,0.4); }}
    .card-value.status-active {{ color: var(--green); }}
    .card-value.status-error {{ color: var(--red); text-shadow: 0 0 10px rgba(255,60,60,0.4); }}
    .card-value.status-offline {{ color: var(--red); text-shadow: 0 0 10px rgba(255,60,60,0.4); }}
    .card-value.status-online {{ color: var(--cyan); text-shadow: 0 0 10px rgba(0,229,255,0.4); }}
    .section-title {{
      font-size: 0.7rem;
      letter-spacing: 0.3em;
      color: var(--muted);
      text-transform: uppercase;
      border-bottom: 1px solid var(--border);
      padding-bottom: 0.5rem;
      margin-bottom: 1.25rem;
    }}
    .card.perf-card::before {{
      background: linear-gradient(90deg, transparent, var(--cyan), transparent);
    }}
    .card-value.profit {{ color: #00ff9d; text-shadow: 0 0 12px rgba(0,255,157,0.5); }}
    .card-value.users {{ color: var(--cyan); text-shadow: 0 0 10px rgba(0,229,255,0.4); }}
    .card-icon {{
      position: absolute;
      top: 1.2rem; right: 1.4rem;
      font-size: 1.5rem;
      opacity: 0.15;
    }}
    .footer {{
      text-align: center;
      font-size: 0.65rem;
      color: var(--muted);
      letter-spacing: 0.15em;
      border-top: 1px solid var(--border);
      padding-top: 1.2rem;
    }}
    .cmd-row {{
      display: flex;
      gap: 1.25rem;
      margin-bottom: 2rem;
      flex-wrap: wrap;
      align-items: stretch;
    }}
    .cmd-btn {{
      flex: 1;
      min-width: 220px;
      padding: 1.1rem 1.5rem;
      font-family: 'Share Tech Mono', 'Courier New', monospace;
      font-size: 1.05rem;
      font-weight: bold;
      letter-spacing: 0.12em;
      border: none;
      border-radius: 4px;
      cursor: pointer;
      text-transform: uppercase;
      transition: box-shadow 0.15s, transform 0.1s;
    }}
    .cmd-btn:active {{ transform: scale(0.97); }}
    .btn-force-buy {{
      flex: 2;
      padding: 1.5rem 2rem;
      font-size: 1.5rem;
      letter-spacing: 0.2em;
      color: #fff;
      background: #cc0000;
      border: 2px solid #ff3c3c;
      box-shadow: 0 0 20px rgba(255,60,60,0.7), 0 0 40px rgba(255,60,60,0.4), inset 0 0 15px rgba(255,0,0,0.2);
      animation: force-buy-glow 1.2s ease-in-out infinite alternate;
    }}
    .btn-force-buy:hover {{
      background: #ff0000;
      box-shadow: 0 0 35px rgba(255,60,60,0.95), 0 0 70px rgba(255,60,60,0.6), inset 0 0 20px rgba(255,0,0,0.3);
    }}
    @keyframes force-buy-glow {{
      from {{ box-shadow: 0 0 15px rgba(255,60,60,0.6), 0 0 30px rgba(255,60,60,0.3); border-color: #cc0000; }}
      to   {{ box-shadow: 0 0 35px rgba(255,60,60,0.95), 0 0 70px rgba(255,60,60,0.6), 0 0 100px rgba(255,0,0,0.3); border-color: #ff3c3c; }}
    }}
    .btn-panic-sell {{
      background: #1a0500;
      color: #ff6600;
      border: 2px solid #ff4400;
      box-shadow: 0 0 14px rgba(255,68,0,0.6), 0 0 30px rgba(255,68,0,0.25);
      animation: panic-glow 1.6s ease-in-out infinite;
    }}
    .btn-panic-sell:hover {{
      box-shadow: 0 0 28px rgba(255,68,0,0.9), 0 0 60px rgba(255,68,0,0.45);
    }}
    @keyframes panic-glow {{
      0%, 100% {{ box-shadow: 0 0 14px rgba(255,68,0,0.6), 0 0 30px rgba(255,68,0,0.25); }}
      50% {{ box-shadow: 0 0 28px rgba(255,68,0,0.95), 0 0 55px rgba(255,68,0,0.5); }}
    }}
    #cmd-status {{
      text-align: center;
      font-size: 0.85rem;
      letter-spacing: 0.15em;
      min-height: 1.6rem;
      margin-bottom: 1rem;
      color: var(--muted);
      transition: color 0.2s;
    }}
    .pulse {{
      display: inline-block;
      width: 8px; height: 8px;
      border-radius: 50%;
      background: var(--green);
      box-shadow: 0 0 6px var(--green);
      animation: pulse 1.4s ease-in-out infinite;
      margin-right: 0.5rem;
      vertical-align: middle;
    }}
    @keyframes pulse {{
      0%, 100% {{ opacity: 1; transform: scale(1); }}
      50% {{ opacity: 0.3; transform: scale(0.8); }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="logo">&#9670; NEXUS OS &#9670;</div>
      <div class="sub">HATAN INDUSTRIES · COMMAND &amp; CONTROL DASHBOARD</div>
      <div class="refresh-badge"><span class="pulse"></span>AUTO-REFRESH EVERY 5s</div>
    </header>

    <div class="grid">
      <div class="card">
        <span class="card-icon">&#9889;</span>
        <div class="card-label">&#9632; Trading Status</div>
        <div class="card-value {'status-active' if trading_status not in ('IDLE','UNKNOWN','ERROR') else ('status-idle' if trading_status == 'IDLE' else 'status-error')}">{trading_status}</div>
      </div>
      <div class="card">
        <span class="card-icon">&#9679;</span>
        <div class="card-label">&#9632; Active Sessions</div>
        <div class="card-value">{active_sessions}</div>
      </div>
      <div class="card">
        <span class="card-icon">&#8679;</span>
        <div class="card-label">&#9632; Last Buy Order</div>
        <div class="card-value" style="font-size:1.1rem">{last_buy_order}</div>
      </div>
      <div class="card">
        <span class="card-icon">&#9670;</span>
        <div class="card-label">&#9632; Redis Broker</div>
        <div class="card-value {'status-online' if redis_status == 'ONLINE' else 'status-offline'}">{redis_status}</div>
      </div>
    </div>

    <div class="section-title">&#9632; LIVE PERFORMANCE</div>
    <div class="grid" style="margin-bottom:2rem">
      <div class="card perf-card">
        <span class="card-icon">&#36;</span>
        <div class="card-label">&#9632; Total Profit (Daily)</div>
        <div class="card-value profit">{profit_display}</div>
      </div>
      <div class="card perf-card">
        <span class="card-icon">&#128100;</span>
        <div class="card-label">&#9632; Scanned Users</div>
        <div class="card-value users">{users_display}</div>
      </div>
      <div class="card perf-card">
        <span class="card-icon">&#9732;</span>
        <div class="card-label">&#9632; Stream Status</div>
        <div class="card-value {stream_css}">{stream_status}</div>
      </div>
    </div>

    <div id="cmd-status">&#9632; AWAITING COMMAND</div>
    <div class="cmd-row">
      <button class="cmd-btn btn-force-buy" id="forceBuyBtn" onclick="forceBuy()">&#9888; FORCE BUY NOW &#9888;</button>
      <button class="cmd-btn btn-panic-sell" onclick="panicSell()">&#9888;&#65039; PANIC SELL ALL</button>
    </div>

    <div class="footer">
      NEXUS DEPLOYER FACILITY &nbsp;|&nbsp; PORT 8002 &nbsp;|&nbsp; HATAN INDUSTRIES
    </div>
  </div>

  <script>
    function setStatus(msg, color) {{
      var el = document.getElementById('cmd-status');
      el.textContent = msg;
      el.style.color = color || '#4a7a4a';
    }}

    function forceBuy() {{
      var btn = document.getElementById('forceBuyBtn');
      btn.disabled = true;
      btn.style.opacity = '0.6';
      setStatus('⚡ FORCE BUY dispatching...', '#ff3c3c');
      fetch('/api/force-buy', {{ method: 'POST' }})
        .then(function(r) {{ return r.json(); }})
        .then(function(d) {{
          setStatus('✔ SENT — FORCE BUY DISPATCHED', '#00ff41');
          setTimeout(function() {{
            setStatus('■ AWAITING COMMAND', '');
            btn.disabled = false;
            btn.style.opacity = '1';
          }}, 3000);
        }})
        .catch(function(e) {{
          setStatus('✘ FORCE BUY error: ' + e, '#ff3c3c');
          btn.disabled = false;
          btn.style.opacity = '1';
        }});
    }}

    function panicSell() {{
      var confirmed = window.confirm('⚠️ CRITICAL: Are you sure you want to LIQUIDATE ALL POSITIONS?\n\nThis action is IRREVERSIBLE and will immediately market-sell every open position.');
      if (!confirmed) {{
        setStatus('▸ Panic sell cancelled.', '#ffe600');
        return;
      }}
      setStatus('🚨 PANIC SELL dispatching...', '#ff4400');
      fetch('/api/panic-sell', {{ method: 'POST' }})
        .then(function(r) {{ return r.json(); }})
        .then(function(d) {{
          if (d.status === 'ok') {{
            setStatus('🚨 PANIC SELL EXECUTED — ' + (d.detail || 'Liquidation in progress'), '#ff4400');
          }} else {{
            setStatus('✘ Panic sell error: ' + (d.detail || d.status), '#ff3c3c');
          }}
        }})
        .catch(function(e) {{ setStatus('✘ Panic sell error: ' + e, '#ff3c3c'); }});
    }}
  </script>
</body>
</html>"""
        return HTMLResponse(content=html, status_code=200, headers={"Cache-Control": "no-cache, no-store, must-revalidate"})

    @app.post("/api/force-buy", tags=["trading"], summary="Force buy — immediate market order ignoring thresholds", response_model=None)
    async def force_buy(request: Request) -> JSONResponse:
        r = getattr(request.app.state, "redis", None)
        if r is None:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "error", "detail": "Redis unavailable"},
            )
        try:
            payload = ujson.dumps({"action": "FORCE_BUY", "amount": 10})
            await r.publish("nexus:commands", payload)
            return JSONResponse(
                status_code=200,
                content={"status": "ok", "action": "FORCE_BUY", "detail": "Force buy command dispatched"},
            )
        except Exception as exc:
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"status": "error", "detail": str(exc)},
            )

    @app.post("/api/panic-sell", tags=["trading"], summary="Panic sell — liquidate all positions", response_model=None)
    async def panic_sell(request: Request) -> JSONResponse:
        r = getattr(request.app.state, "redis", None)
        if r is None:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "error", "detail": "Redis unavailable"},
            )
        try:
            payload = ujson.dumps({"action": "PANIC_SELL"})
            await r.publish("nexus:commands", payload)
            return JSONResponse(
                status_code=200,
                content={"status": "ok", "action": "PANIC_SELL", "detail": "Liquidation command dispatched"},
            )
        except Exception as exc:
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"status": "error", "detail": str(exc)},
            )

    @app.post(
        "/api/v1/commands",
        tags=["commands"],
        summary="Dispatch a command action to the nexus:commands Redis channel",
        response_model=None,
    )
    async def v1_commands(request: Request) -> JSONResponse:
        """Accept ``{"action": "<ACTION>", ...}`` and publish it to the
        ``nexus:commands`` Redis pub/sub channel so ``nexus_core.py``'s
        command listener can handle it immediately."""
        r = getattr(request.app.state, "redis", None)
        if r is None:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "error", "detail": "Redis unavailable"},
            )
        try:
            body: dict[str, Any] = await request.json()
        except Exception:
            body = {}
        action = (body.get("action") or "").upper()
        if not action:
            return JSONResponse(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                content={"status": "error", "detail": "Missing required field: action"},
            )
        try:
            payload = ujson.dumps({**body, "action": action})
            await r.publish("nexus:commands", payload)
            return JSONResponse(
                status_code=200,
                content={"status": "ok", "action": action, "detail": f"Command '{action}' dispatched to nexus:commands"},
            )
        except Exception as exc:
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"status": "error", "detail": str(exc)},
            )

    @app.get("/api/stats", tags=["meta"], summary="Nexus OS live stats for the UI", response_model=None)
    async def api_stats(request: Request) -> JSONResponse:
        """
        JSON stats consumed by the Nexus OS dashboard.

        - ``active_sessions``: count of all ``nexus:sessions:*`` keys in Redis.
        - ``trading_status``: value of ``nexus:trading:status`` (defaults to ``"IDLE"``).
        - ``redis_status``: ``"ONLINE"`` | ``"OFFLINE"``.
        """
        r = getattr(request.app.state, "redis", None)
        if r is None:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "error", "detail": "Redis unavailable"},
            )
        try:
            await r.ping()
        except Exception as exc:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"redis_status": "OFFLINE", "active_sessions": 0, "trading_status": "IDLE", "detail": str(exc)},
            )

        # Scan all nexus:sessions:* keys to get a live count.
        session_count = 0
        try:
            cursor = 0
            while True:
                cursor, keys = await r.scan(cursor=cursor, match="nexus:sessions:*", count=200)
                session_count += len(keys)
                if cursor == 0:
                    break
        except Exception:
            session_count = 0

        # Read trading status; treat missing key as IDLE.
        trading_status = "IDLE"
        try:
            raw = await r.get("nexus:trading:status")
            if raw:
                trading_status = raw.decode() if isinstance(raw, (bytes, bytearray)) else str(raw)
        except Exception:
            pass

        # Read telefix analytics pushed by the background poller.
        daily_profit = None
        new_users = None
        try:
            _p = await r.get("nexus:analytics:daily_profit")
            _u = await r.get("nexus:analytics:new_users")
            daily_profit = _p.decode() if isinstance(_p, (bytes, bytearray)) else (str(_p) if _p is not None else None)
            new_users = _u.decode() if isinstance(_u, (bytes, bytearray)) else (str(_u) if _u is not None else None)
        except Exception:
            pass

        return JSONResponse(
            status_code=200,
            content={
                "redis_status": "ONLINE",
                "active_sessions": session_count,
                "trading_status": trading_status,
                "daily_profit": daily_profit,
                "new_users": new_users,
                "stream_status": "DATA STREAMING" if (daily_profit is not None and new_users is not None) else "IDLE",
            },
        )

    @app.get(
        "/api/v1/swarm/inventory",
        tags=["swarm"],
        summary="Swarm session inventory grouped by machine_id",
        response_model=None,
    )
    async def swarm_inventory(request: Request) -> JSONResponse:
        """Scan all nexus:sessions:* keys in Redis and return sessions grouped by machine_id.

        Each session entry exposes: phone, machine_id, status, current_task.
        """
        r = getattr(request.app.state, "redis", None)
        if r is None:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "error", "detail": "Redis unavailable"},
            )
        sessions_by_machine: dict[str, list[dict[str, Any]]] = {}
        total = 0
        try:
            cursor = 0
            while True:
                cursor, keys = await r.scan(cursor=cursor, match="nexus:sessions:*", count=200)
                for key in keys:
                    raw = await r.get(key)
                    if raw:
                        try:
                            payload = ujson.loads(raw)
                        except Exception:
                            payload = {}
                    else:
                        payload = {}

                    # Normalise field names — sessions may use different key conventions.
                    phone = (
                        payload.get("phone")
                        or payload.get("phone_number")
                        or payload.get("session_phone")
                        or ""
                    )
                    machine_id = (
                        payload.get("machine_id")
                        or payload.get("origin_machine")
                        or payload.get("node_id")
                        or "unknown"
                    )
                    sess_status = (
                        payload.get("status")
                        or payload.get("state")
                        or "unknown"
                    )
                    current_task = (
                        payload.get("current_task")
                        or payload.get("last_scanned_target")
                        or payload.get("task")
                        or None
                    )

                    entry: dict[str, Any] = {
                        "redis_key": key if isinstance(key, str) else key.decode("utf-8", errors="replace"),
                        "phone": phone,
                        "machine_id": machine_id,
                        "status": sess_status,
                        "current_task": current_task,
                    }
                    sessions_by_machine.setdefault(machine_id, []).append(entry)
                    total += 1
                if cursor == 0:
                    break
        except Exception as exc:
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"status": "error", "detail": str(exc)},
            )

        # Sort so Jacob-PC (master) always appears first in the machines list.
        machines_sorted = sorted(
            sessions_by_machine.keys(),
            key=lambda m: (0 if m == "Jacob-PC" else 1, m),
        )
        ordered: dict[str, list[dict[str, Any]]] = {m: sessions_by_machine[m] for m in machines_sorted}

        return JSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "total": total,
                "machines": machines_sorted,
                "sessions_by_machine": ordered,
            },
        )

    @app.get("/ready", tags=["meta"], summary="Deployer readiness (Redis ping)", response_model=None)
    async def ready(request: Request) -> Any:
        r = getattr(request.app.state, "redis", None)
        if r is None:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "not_ready", "redis": "uninitialized"},
            )
        try:
            await r.ping()
            degraded = bool(getattr(request.app.state, "redis_degraded", False))
            return {"status": "ready", "redis": "degraded" if degraded else "ok"}
        except Exception as exc:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "not_ready", "redis": f"error:{exc!s}"},
            )

    app.include_router(deploy.router, prefix="/api")
    return app


def _free_port(port: int, retries: int = 3, delay: float = 1.5) -> None:
    """Kill any process holding *port* on Windows, then wait for it to release."""
    if sys.platform != "win32":
        try:
            import subprocess as _sp
            _sp.run(["fuser", "-k", f"{port}/tcp"],
                    stdout=_sp.DEVNULL, stderr=_sp.DEVNULL)
        except Exception:
            pass
        return
    import subprocess as _sp
    import time as _time
    for attempt in range(retries):
        try:
            run_kw: dict = {"capture_output": True, "text": True}
            if hasattr(_sp, "CREATE_NO_WINDOW"):
                run_kw["creationflags"] = _sp.CREATE_NO_WINDOW
            result = _sp.run(["netstat", "-ano", "-p", "TCP"], **run_kw)
            for line in result.stdout.splitlines():
                parts = line.split()
                if len(parts) < 5:
                    continue
                local, state, pid_str = parts[1], parts[3].upper(), parts[4]
                if f":{port}" in local and state == "LISTENING":
                    try:
                        pid = int(pid_str)
                    except ValueError:
                        continue
                    kill_kw: dict = {"stdout": _sp.DEVNULL, "stderr": _sp.DEVNULL}
                    if hasattr(_sp, "CREATE_NO_WINDOW"):
                        kill_kw["creationflags"] = _sp.CREATE_NO_WINDOW
                    _sp.run(["taskkill", "/PID", str(pid), "/T", "/F"], **kill_kw)
                    _hatan_print("PORT-FREE", f"killed pid={pid} holding :{port}")
                    _time.sleep(delay)
                    return
        except Exception as exc:
            _hatan_print("PORT-FREE", f"attempt {attempt + 1} error: {exc}")
        _time.sleep(0.5)


def main() -> None:
    _patch_redis_for_environment()
    host = "0.0.0.0"
    port = 8002

    _free_port(port)

    print(_BANNER, flush=True)
    _hatan_print("BIND", f"{host}:{port}")
    _hatan_print("DOCS", f"http://localhost:{port}/docs  ← API explorer")
    print(f"NEXUS OS IS LIVE AT: http://localhost:{port}/nexus-os", flush=True)

    uvicorn.run(
        create_deployer_app(),
        host=host,
        port=port,  # 8002
        reload=False,
        log_level="error",
        log_config=None,
        access_log=False,
    )


if __name__ == "__main__":
    main()
