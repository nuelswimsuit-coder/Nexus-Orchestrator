"""
Lite Nexus launcher for low-memory machines.

Runs:
  - FastAPI control center (uvicorn) in a background thread
  - Telegram bot polling in the main asyncio loop

This keeps both services inside a single Python process to reduce RAM overhead.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import threading

import uvicorn

from nexus.shared.config import settings
from nexus.shared.logging_config import configure_logging
from nexus.shared.system_settings import read_system_settings
from scripts.start_api import _patch_redis_for_environment
from scripts.start_telegram_bot import start_bot_polling

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def _configure_low_overhead_logging() -> None:
    """
    Force WARNING logs globally to reduce CPU/IO pressure.
    """
    dynamic = read_system_settings()
    configure_logging(level=str(dynamic["log_level"]), node_id=f"{settings.node_id}-lite")
    logging.getLogger().setLevel(logging.WARNING)
    for name in (
        "uvicorn",
        "uvicorn.error",
        "uvicorn.access",
        "fastapi",
        "aiogram",
        "httpx",
        "httpcore",
        "asyncio",
    ):
        logging.getLogger(name).setLevel(logging.WARNING)


def _server_started(server: uvicorn.Server) -> bool:
    """
    Uvicorn may expose `started` as bool or Event-like object.
    """
    started = getattr(server, "started", False)
    if hasattr(started, "is_set"):
        return bool(started.is_set())
    return bool(started)


def _start_api_thread() -> tuple[uvicorn.Server, threading.Thread]:
    config = uvicorn.Config(
        "nexus.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
        log_level="warning",
        log_config=None,
        access_log=False,
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, name="nexus-lite-api", daemon=True)
    thread.start()
    return server, thread


async def run() -> None:
    _configure_low_overhead_logging()
    dynamic = read_system_settings()
    # Keep lite mode explicitly constrained for stability on low-RAM hosts.
    os.environ["WORKER_MAX_JOBS"] = str(max(2, min(int(dynamic["max_workers"]), 3)))
    os.environ["NEXUS_PREDICTION_THROTTLE_DELAY"] = "1.0"
    _patch_redis_for_environment()

    token = os.environ.get("TELEGRAM_BOT_TOKEN", "") or settings.telegram_bot_token
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is missing. Set it in .env or environment.")

    api_server, api_thread = _start_api_thread()

    # Wait briefly for API startup so the bot can call API endpoints immediately.
    for _ in range(200):  # ~20s max
        if _server_started(api_server):
            break
        if not api_thread.is_alive():
            raise RuntimeError("API thread exited during startup.")
        await asyncio.sleep(0.1)

    try:
        await start_bot_polling(token)
    finally:
        api_server.should_exit = True
        for _ in range(100):  # ~10s max graceful shutdown wait
            if not api_thread.is_alive():
                break
            await asyncio.sleep(0.1)


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
