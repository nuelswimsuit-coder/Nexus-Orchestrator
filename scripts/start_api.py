"""
API Server entrypoint.

Usage
-----
    python scripts/start_api.py

Or via the installed CLI entrypoint (after `pip install -e .`):
    nexus-api

Starts the FastAPI Control Center with uvicorn.
The API server is independent of the master and worker processes —
all three can run simultaneously on this machine.
"""

from __future__ import annotations

import os
import pathlib
import sys
import asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
elif os.environ.get("ENVIRONMENT", "PRODUCTION").upper() == "PRODUCTION":
    try:
        import uvloop  # type: ignore[import-not-found]

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except Exception:
        pass

import uvicorn

from nexus.shared.config import settings


def _patch_redis_for_environment() -> None:
    """
    Auto-detect whether we are running inside a Docker container or directly
    on the Windows host and adjust settings.redis_url accordingly.

    Detection heuristics (checked in order):
      1. /.dockerenv present  → always means we are inside a Linux container.
      2. DOCKER_CONTAINER env var set to 1/true/yes  → explicit override.
      3. RUNNING_IN_DOCKER env var set to 1/true/yes → explicit override.
      4. Nothing matched → assume Windows host, keep 'localhost'.
    """
    in_docker = (
        pathlib.Path("/.dockerenv").exists()
        or os.environ.get("DOCKER_CONTAINER", "").lower() in ("1", "true", "yes")
        or os.environ.get("RUNNING_IN_DOCKER", "").lower() in ("1", "true", "yes")
    )

    if in_docker:
        original = settings.redis_url
        settings.redis_url = (
            original
            .replace("localhost", "host.docker.internal")
            .replace("127.0.0.1", "host.docker.internal")
        )
        print(f"[nexus] Docker detected — Redis: {original} → {settings.redis_url}")
    else:
        print(f"[nexus] Host (Windows) detected — Redis: {settings.redis_url}")


def main() -> None:
    _patch_redis_for_environment()
    uvicorn.run(
        "nexus.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
        log_level="error",
        log_config=None,  # structlog handles all logging
    )


if __name__ == "__main__":
    main()
