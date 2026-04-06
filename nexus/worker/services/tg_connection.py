"""
Core Telethon connection policy: bounded concurrent MTProto I/O, sticky residential
proxy per session stem, frozen device profile per session (stable seed — see
docs/architecture/01_core_and_opsec.md), and shared proxy-pool parsing.
"""

from __future__ import annotations

import asyncio
import json
import os
import zlib
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import structlog

log = structlog.get_logger(__name__)

_telegram_network_sem: asyncio.Semaphore | None = None

# Deterministic device table — chosen once per session and persisted next to the vault file.
_DEVICE_PROFILES: tuple[dict[str, str], ...] = (
    {"device_model": "iPhone 15 Pro", "system_version": "iOS 17.5", "app_version": "10.14.4"},
    {"device_model": "iPhone 15", "system_version": "iOS 17.4", "app_version": "10.14.3"},
    {"device_model": "iPhone 14 Pro", "system_version": "iOS 17.3", "app_version": "10.13.2"},
    {"device_model": "Samsung Galaxy S23 Ultra", "system_version": "SDK 34", "app_version": "10.14.2"},
    {"device_model": "Samsung Galaxy S23", "system_version": "SDK 33", "app_version": "10.14.1"},
    {"device_model": "Google Pixel 8 Pro", "system_version": "SDK 34", "app_version": "10.14.0"},
    {"device_model": "Google Pixel 8", "system_version": "SDK 34", "app_version": "10.13.9"},
    {"device_model": "OnePlus 12", "system_version": "SDK 34", "app_version": "10.13.8"},
    {"device_model": "Xiaomi 14", "system_version": "SDK 34", "app_version": "10.13.7"},
    {"device_model": "iPhone 13 Pro", "system_version": "iOS 16.7", "app_version": "10.12.0"},
    {"device_model": "Samsung Galaxy S22", "system_version": "SDK 33", "app_version": "10.11.5"},
    {"device_model": "Motorola Edge 40", "system_version": "SDK 33", "app_version": "10.11.2"},
)


def _network_concurrency_limit() -> int:
    try:
        from nexus.shared.config import settings

        return int(settings.telegram_network_concurrency)
    except Exception:
        for key in ("NEXUS_TELEGRAM_NETWORK_CONCURRENCY", "TELEGRAM_NETWORK_CONCURRENCY"):
            raw = (os.getenv(key) or "").strip()
            if raw:
                try:
                    return max(1, min(500, int(raw)))
                except ValueError:
                    break
        return 30


def get_telegram_network_sem() -> asyncio.Semaphore:
    """Shared semaphore — one pool for Telegram-class network I/O (default 30)."""
    global _telegram_network_sem
    if _telegram_network_sem is None:
        n = _network_concurrency_limit()
        _telegram_network_sem = asyncio.Semaphore(n)
        log.info(
            "telegram_network_sem_initialized",
            sem_limit=n,
            task_name="global",
        )
    return _telegram_network_sem


@asynccontextmanager
async def telegram_network_slot(*, task_name: str = ""):
    """
    Acquire a slot before opening a Telethon client or burst of RPCs.

    Lock ordering: acquire this semaphore before any secondary resource locks.
    """
    sem = get_telegram_network_sem()
    async with sem:
        if task_name:
            log.debug(
                "telegram_network_slot_acquired",
                task_name=task_name,
                sem_limit=_network_concurrency_limit(),
            )
        yield


def stable_u32_from_stem(stem: str) -> int:
    """Unsigned 32-bit stable digest for proxy index / device table selection."""
    return zlib.crc32(stem.encode("utf-8")) & 0xFFFFFFFF


def swarm_identity_sidecar_path(session_base: str) -> Path:
    """``<stem>.swarm_identity.json`` adjacent to ``<stem>.session``."""
    p = Path(session_base)
    return p.parent / f"{p.name}.swarm_identity.json"


def load_or_create_device_profile(session_base: str) -> dict[str, str]:
    """
    Return Telethon ``device_model`` / ``system_version`` / ``app_version`` for this
    session, persisting alongside the vault so reconnects stay consistent.
    """
    path = swarm_identity_sidecar_path(session_base)
    stem = Path(session_base).name
    merged: dict[str, Any] = {}
    if path.is_file():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                merged = raw
        except Exception as exc:
            log.debug("swarm_identity_read_failed", path=str(path), error=str(exc))
        dm = merged.get("device_model")
        sv = merged.get("system_version")
        av = merged.get("app_version")
        if isinstance(dm, str) and dm.strip() and isinstance(sv, str) and sv.strip():
            return {
                "device_model": dm.strip(),
                "system_version": sv.strip(),
                "app_version": (av.strip() if isinstance(av, str) and av.strip() else "10.14.1"),
            }

    pick = _DEVICE_PROFILES[stable_u32_from_stem(stem) % len(_DEVICE_PROFILES)]
    merged.update(
        {
            "device_model": pick["device_model"],
            "system_version": pick["system_version"],
            "app_version": pick["app_version"],
            "device_profile_version": 1,
        }
    )
    try:
        path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError as exc:
        log.warning("swarm_identity_write_failed", path=str(path), error=str(exc))
    return {
        "device_model": pick["device_model"],
        "system_version": pick["system_version"],
        "app_version": pick["app_version"],
    }


def parse_residential_proxy_pool() -> list[str]:
    raw = (os.getenv("NEXUS_RESIDENTIAL_PROXY_POOL") or "").strip()
    if raw:
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        if parts:
            return parts
    single = (os.getenv("NEXUS_RESIDENTIAL_PROXY_URL") or "").strip()
    return [single] if single else []


def proxy_tuple_from_url(url: str) -> tuple[Any, ...]:
    try:
        import socks  # type: ignore[import-untyped]
    except ImportError as exc:
        raise ImportError(
            "SOCKS proxy requested but PySocks is not installed — run: pip install PySocks"
        ) from exc

    parsed = urlparse(url)
    scheme = (parsed.scheme or "").lower()
    if scheme not in ("socks5", "socks5h"):
        raise ValueError(f"Unsupported proxy scheme {scheme!r}; use socks5 or socks5h")

    host = parsed.hostname or ""
    port = parsed.port or 1080
    user = unquote(parsed.username) if parsed.username else None
    password = unquote(parsed.password) if parsed.password else None
    rdns = scheme == "socks5h"

    return (socks.SOCKS5, host, port, rdns, user, password)


def proxy_tuple_for_session_stem(stem: str, pool: list[str] | None = None) -> tuple[Any, ...] | None:
    """
    Sticky binding: ``stem`` always maps to the same pool entry (hash modulo),
    not to enumeration order in a job.
    """
    pool = pool if pool is not None else parse_residential_proxy_pool()
    if not pool:
        if (os.getenv("NEXUS_STRICT_SESSION_PROXY") or "").strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        ):
            raise ValueError(
                "NEXUS_STRICT_SESSION_PROXY is set but no NEXUS_RESIDENTIAL_PROXY_POOL / URL is configured"
            )
        return None
    url = pool[stable_u32_from_stem(stem) % len(pool)]
    return proxy_tuple_from_url(url)


def telethon_connect_kwargs_for_meta_json(meta_json: Path) -> dict[str, Any]:
    """Sync Telethon ``TelegramClient(..., **kwargs)`` for vault ``*.json`` paths."""
    session_base = str(meta_json.with_suffix(""))
    prof = load_or_create_device_profile(session_base)
    proxy = proxy_tuple_for_session_stem(meta_json.stem)
    out: dict[str, Any] = {
        "device_model": prof["device_model"],
        "system_version": prof["system_version"],
        "app_version": prof["app_version"],
    }
    if proxy is not None:
        out["proxy"] = proxy
    return out


def telethon_connect_kwargs_for_session_base(session_base: str, stem: str) -> dict[str, Any]:
    prof = load_or_create_device_profile(session_base)
    proxy = proxy_tuple_for_session_stem(stem)
    out: dict[str, Any] = {
        "device_model": prof["device_model"],
        "system_version": prof["system_version"],
        "app_version": prof["app_version"],
    }
    if proxy is not None:
        out["proxy"] = proxy
    return out
