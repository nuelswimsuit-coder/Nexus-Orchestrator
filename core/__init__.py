"""Domain logic for the root `bot.py` entrypoint."""

from __future__ import annotations

from core.behavioral_analyzer import ReadinessReport, readiness_from_flags
from core.scanner import probe_http_ok, verify_redis

__all__ = [
    "ReadinessReport",
    "probe_http_ok",
    "readiness_from_flags",
    "verify_redis",
]
