"""
Smart Redis URL defaults and Windows auto-recovery helpers.

- Windows (``win32``): broker defaults to ``[::1]``; URLs that still point at
  the fleet master stub ``10.100.102.8``, ``127.0.0.1``, or ``localhost`` are
  rewritten to ``[::1]`` to avoid WSL2/Hyper-V port-proxy hijack (WinError 64).
- Linux: default fleet master host ``10.100.102.8`` when nothing else is set.

Used by :mod:`nexus.shared.config`, the API lifespan, and operator CLIs.
"""

from __future__ import annotations

import os
import subprocess
import sys
from typing import Any
from urllib.parse import urlparse, urlunparse

LINUX_FLEET_REDIS_HOST = "10.100.102.8"
# Use IPv6 loopback on Windows — the bundled redis-server binds [::] which covers
# IPv6. Avoid 127.0.0.1: WSL2/Hyper-V port-proxy rules can hijack it (WinError 64).
WINDOWS_LOCAL_REDIS_HOST = "[::1]" if sys.platform == "win32" else "127.0.0.1"
DEGRADED_ENV_FLAG = "NEXUS_ALLOW_DEGRADED"


def default_redis_host() -> str:
    """Platform-native default Redis hostname (no port)."""
    return WINDOWS_LOCAL_REDIS_HOST if sys.platform == "win32" else LINUX_FLEET_REDIS_HOST


def default_redis_url_string() -> str:
    """Full ``redis://`` DSN with platform-appropriate host."""
    return f"redis://{default_redis_host()}:6379/0"


def _replace_redis_hostname(url: str, new_host: str) -> str:
    u = urlparse(url)
    port = u.port or 6379
    auth = ""
    if u.username is not None:
        auth = u.username
        if u.password is not None:
            auth += f":{u.password}"
        auth += "@"
    elif u.password is not None:
        auth = f":{u.password}@"
    new_netloc = f"{auth}{new_host}:{port}"
    return urlunparse((u.scheme, new_netloc, u.path, u.params, u.query, u.fragment))


def coerce_redis_url_for_platform(redis_url: str) -> str:
    """
    Rewrite known misconfigurations on Windows:
    - Linux fleet master IP -> [::1]
    - 127.0.0.1 / localhost -> [::1]  (avoids WSL2/Hyper-V port-proxy hijack)
    """
    if not (redis_url or "").strip():
        return default_redis_url_string()
    if sys.platform == "win32":
        host = (urlparse(redis_url).hostname or "").lower()
        if host in (LINUX_FLEET_REDIS_HOST.lower(), "127.0.0.1", "localhost"):
            return _replace_redis_hostname(redis_url, WINDOWS_LOCAL_REDIS_HOST)
    return redis_url


def apply_redis_url_to_environment() -> None:
    """
    Normalise ``REDIS_URL`` in the process environment before Settings is built.

    Call from entrypoints that import settings lazily (e.g. ``start_api``).
    """
    raw = (os.environ.get("REDIS_URL") or "").strip()
    if raw:
        fixed = coerce_redis_url_for_platform(raw)
        if fixed != raw:
            os.environ["REDIS_URL"] = fixed
    else:
        os.environ.setdefault("REDIS_URL", default_redis_url_string())


def try_ping_sync(redis_url: str, *, timeout_s: float = 2.0) -> bool:
    """Blocking PING; safe to call from sync code paths."""
    try:
        import redis as redis_sync  # type: ignore[import-untyped]
    except ImportError:
        return False

    def _ping() -> bool:
        client = redis_sync.from_url(
            redis_url,
            socket_connect_timeout=timeout_s,
            socket_timeout=timeout_s,
        )
        try:
            return bool(client.ping())
        finally:
            try:
                client.close()
            except Exception:
                pass

    try:
        return _ping()
    except Exception:
        return False


def try_start_redis_via_wsl_windows() -> subprocess.CompletedProcess[str] | None:
    """
    Best-effort: start redis-server inside the default WSL distro as root.

    Returns the completed process, or None if not on Windows or launch failed to spawn.
    """
    if sys.platform != "win32":
        return None
    try:
        return subprocess.run(
            ["wsl", "-u", "root", "service", "redis-server", "start"],
            capture_output=True,
            text=True,
            timeout=90,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None


def mark_degraded_mode() -> None:
    os.environ[DEGRADED_ENV_FLAG] = "1"


def degraded_mode_active() -> bool:
    return os.getenv(DEGRADED_ENV_FLAG, "").strip().lower() in ("1", "true", "yes", "on")


def create_degraded_async_redis() -> Any:
    """
    In-memory async Redis for API boot when the real broker is down.

    Requires the ``fakeredis`` extra; raises ``RuntimeError`` if missing.
    """
    try:
        from fakeredis import aioredis as fake_aioredis  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "NEXUS_ALLOW_DEGRADED is set but fakeredis is not installed. "
            "Install requirements (fakeredis) or start a real Redis broker."
        ) from exc
    return fake_aioredis.FakeRedis(decode_responses=True)
