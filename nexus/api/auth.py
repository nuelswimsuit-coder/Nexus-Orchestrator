"""
Optional shared-secret gate for sensitive HTTP routes.

When ``NEXUS_API_KEY`` is set in the environment, matching routes require one of:
  - Header ``X-Nexus-Api-Key: <key>``
  - Header ``Authorization: Bearer <key>``
  - Query ``api_key=<key>`` (for EventSource/SSE clients that cannot set headers)
"""

from __future__ import annotations

import secrets
from typing import Annotated

from fastapi import Header, HTTPException, Request, status

from nexus.shared.config import settings


def _const_time_eq(a: str, b: str) -> bool:
    if len(a) != len(b):
        return False
    return secrets.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


async def require_nexus_api_key_if_configured(
    request: Request,
    x_nexus_api_key: Annotated[str | None, Header(alias="X-Nexus-Api-Key")] = None,
    authorization: Annotated[str | None, Header()] = None,
) -> None:
    expected = (settings.nexus_api_key or "").strip()
    if not expected:
        return

    got = (x_nexus_api_key or "").strip()
    if not got and authorization and authorization.lower().startswith("bearer "):
        got = authorization[7:].strip()
    if not got:
        q = request.query_params.get("api_key")
        if q:
            got = q.strip()

    if not got or not _const_time_eq(got, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=(
                "Invalid or missing API key — set NEXUS_API_KEY on the server; "
                "send X-Nexus-Api-Key, Authorization: Bearer, or api_key query (SSE)"
            ),
        )
