"""Preflight checks before the bot stack comes fully online."""

from __future__ import annotations

import asyncio
from typing import Any


async def verify_redis(redis_url: str, *, timeout_s: float = 3.0) -> dict[str, Any]:
    """
    Return whether Redis answers PING. Runs the blocking client in a thread
    so callers can use this from asyncio without blocking the loop.
    """
    try:
        import redis as redis_sync  # type: ignore[import-untyped]
    except ImportError:
        return {
            "ok": False,
            "error": "redis package not installed",
            "redis_url_host": _safe_redis_display(redis_url),
        }

    def _ping() -> bool:
        client = redis_sync.from_url(redis_url, socket_timeout=timeout_s)
        try:
            return bool(client.ping())
        finally:
            try:
                client.close()
            except Exception:
                pass

    try:
        ok = await asyncio.to_thread(_ping)
        return {"ok": ok, "error": None, "redis_url_host": _safe_redis_display(redis_url)}
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "redis_url_host": _safe_redis_display(redis_url),
        }


def _safe_redis_display(redis_url: str) -> str:
    """Strip credentials from redis URL for logs."""
    if "@" in redis_url:
        return redis_url.split("@", 1)[-1]
    return redis_url


async def probe_http_ok(url: str, *, timeout_s: float = 5.0) -> dict[str, Any]:
    """GET ``url`` and report status (used after the API is listening)."""
    try:
        import httpx
    except ImportError:
        return {"ok": False, "error": "httpx not installed", "status_code": None}

    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            response = await client.get(url)
            return {
                "ok": response.status_code < 500,
                "error": None,
                "status_code": response.status_code,
            }
    except Exception as exc:
        return {"ok": False, "error": str(exc), "status_code": None}
