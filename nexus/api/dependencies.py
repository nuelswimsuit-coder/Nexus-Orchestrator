"""
FastAPI dependency providers.

All route handlers receive Redis and the HitlStore through FastAPI's
dependency injection system.  This keeps routes thin and makes the
dependencies trivially swappable in tests.

Usage in a route:
    @router.get("/something")
    async def my_route(redis: RedisDep, hitl: HitlStoreDep) -> ...:
        ...
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Annotated

from fastapi import Depends, Request
from redis.asyncio import Redis

from nexus.api.hitl_store import HitlStore


async def get_redis(request: Request) -> AsyncGenerator[Redis, None]:
    """Yield the shared Redis client stored in app state."""
    # region agent log
    try:
        import json as _j
        import time as _t
        from pathlib import Path as _Path

        _rp = _Path(__file__).resolve().parents[2] / "debug-43baa8.log"
        _r = getattr(request.app.state, "redis", None)
        _rp.open("a", encoding="utf-8").write(
            _j.dumps(
                {
                    "sessionId": "43baa8",
                    "hypothesisId": "H1",
                    "location": "dependencies.py:get_redis",
                    "message": "redis_dep_enter",
                    "data": {
                        "has_redis_attr": hasattr(request.app.state, "redis"),
                        "redis_type": type(_r).__name__,
                    },
                    "timestamp": int(_t.time() * 1000),
                }
            )
            + "\n"
        )
    except Exception:
        pass
    # endregion
    yield request.app.state.redis


async def get_hitl_store(request: Request) -> HitlStore:
    """Return the singleton HitlStore stored in app state."""
    return request.app.state.hitl_store


# Typed aliases — import these in route modules for clean annotations.
RedisDep = Annotated[Redis, Depends(get_redis)]
HitlStoreDep = Annotated[HitlStore, Depends(get_hitl_store)]
