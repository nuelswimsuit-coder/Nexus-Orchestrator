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
    yield request.app.state.redis


async def get_hitl_store(request: Request) -> HitlStore:
    """Return the singleton HitlStore stored in app state."""
    return request.app.state.hitl_store


# Typed aliases — import these in route modules for clean annotations.
RedisDep = Annotated[Redis, Depends(get_redis)]
HitlStoreDep = Annotated[HitlStore, Depends(get_hitl_store)]
