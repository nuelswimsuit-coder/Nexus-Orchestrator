"""
nexus/api/routers/proxy.py — Proxy Pool Status & Rotation Tracker

Endpoints
---------
GET  /api/proxy/status   — current proxy pool info, active proxy, last rotation time
POST /api/proxy/rotate   — manually force a rotation (records event in Redis)
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import structlog
from fastapi import APIRouter
from pydantic import BaseModel

from nexus.api.dependencies import RedisDep

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/proxy", tags=["proxy"])

# ── Redis keys ────────────────────────────────────────────────────────────────
PROXY_ACTIVE_KEY    = "nexus:proxy:active"          # JSON: current proxy details
PROXY_ROTATION_KEY  = "nexus:proxy:rotations"       # List of rotation events (JSON)
PROXY_IP_CACHE_KEY  = "nexus:proxy:ip_cache"        # Cached resolved public IP

PROXIES_FILE = Path(__file__).parents[3] / "proxies.txt"

# Evomi residential proxy endpoint for IP check
EVOMI_IP_CHECK_URL = "https://ip.evomi.com"
FALLBACK_IP_CHECK_URLS = [
    "https://api.ipify.org?format=json",
    "https://httpbin.org/ip",
]


# ── Pydantic models ───────────────────────────────────────────────────────────

class ProxyEntry(BaseModel):
    index: int
    scheme: str
    host: str
    port: int
    username: str
    label: str          # e.g. "IL – Netanya"
    raw_line: str       # masked password


class ProxyStatusResponse(BaseModel):
    pool_size: int
    proxies: list[ProxyEntry]
    active_index: int | None
    active_label: str | None
    active_public_ip: str | None
    active_ip_country: str | None
    active_ip_city: str | None
    active_ip_isp: str | None
    last_rotation_at: str | None
    last_rotation_ago_seconds: float | None
    total_rotations: int
    provider: str
    provider_plan: str
    proxies_file_path: str


class RotationEvent(BaseModel):
    ts: str
    from_index: int | None
    to_index: int
    to_label: str
    resolved_ip: str | None
    trigger: str


class RotationHistoryResponse(BaseModel):
    events: list[RotationEvent]
    total: int


class RotateResponse(BaseModel):
    status: str
    new_index: int
    new_label: str
    resolved_ip: str | None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_proxies_file() -> list[dict[str, Any]]:
    """Parse proxies.txt and return structured list."""
    if not PROXIES_FILE.exists():
        return []

    results = []
    for i, line in enumerate(PROXIES_FILE.read_text(encoding="utf-8").splitlines()):
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        # Format: socks5://host:port:user:pass_country-XX_city-YY
        # or socks5://user:pass@host:port
        label = _extract_label(line)

        # Parse scheme://host:port:user:pass (Evomi colon-separated format)
        m = re.match(r"(socks5|http)://([^:]+):(\d+):([^:]+):(.+)", line, re.I)
        if m:
            scheme, host, port, user, passw = m.groups()
            masked = f"{scheme}://{host}:{port}:{user}:***"
            results.append({
                "index": i,
                "scheme": scheme.lower(),
                "host": host,
                "port": int(port),
                "username": user,
                "password": passw,
                "label": label,
                "raw_line": masked,
            })
            continue

        # Fallback: socks5://user:pass@host:port
        m2 = re.match(r"(socks5|http)://([^:@]+):([^@]*)@([^:]+):(\d+)", line, re.I)
        if m2:
            scheme, user, passw, host, port = m2.groups()
            masked = f"{scheme}://{user}:***@{host}:{port}"
            results.append({
                "index": i,
                "scheme": scheme.lower(),
                "host": host,
                "port": int(port),
                "username": user,
                "password": passw,
                "label": label,
                "raw_line": masked,
            })

    return results


def _extract_label(line: str) -> str:
    """Derive a human-readable label from the proxy line."""
    line_lower = line.lower()

    city_match = re.search(r"_city-([a-z.]+)", line_lower)
    country_match = re.search(r"_country-([a-z]+)", line_lower)

    country = country_match.group(1).upper() if country_match else "??"
    if city_match:
        city = city_match.group(1).replace(".", " ").title()
        return f"{country} – {city}"
    return f"{country} – General"


async def _resolve_public_ip(proxy: dict[str, Any]) -> dict[str, Any]:
    """
    Connect through the given proxy and fetch the public IP + geo info.
    Returns dict with ip, country, city, isp or empty on failure.
    """
    proxies_url = (
        f"{proxy['scheme']}://{proxy['username']}:{proxy['password']}"
        f"@{proxy['host']}:{proxy['port']}"
    )
    try:
        async with httpx.AsyncClient(
            proxies=proxies_url,  # type: ignore[arg-type]
            timeout=8.0,
        ) as client:
            # Try Evomi's own IP check first (returns JSON with geo)
            try:
                r = await client.get(EVOMI_IP_CHECK_URL)
                if r.status_code == 200:
                    data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
                    if not data:
                        # Plain text IP
                        return {"ip": r.text.strip(), "country": None, "city": None, "isp": None}
                    return {
                        "ip": data.get("ip") or data.get("query") or r.text.strip(),
                        "country": data.get("country") or data.get("countryCode"),
                        "city": data.get("city"),
                        "isp": data.get("isp") or data.get("org"),
                    }
            except Exception:
                pass

            # Fallback to ipify
            r2 = await client.get("https://api.ipify.org?format=json")
            if r2.status_code == 200:
                ip = r2.json().get("ip", r2.text.strip())
                # Geo lookup
                try:
                    geo = await client.get(f"https://ipapi.co/{ip}/json/")
                    gd = geo.json() if geo.status_code == 200 else {}
                    return {
                        "ip": ip,
                        "country": gd.get("country_name") or gd.get("country"),
                        "city": gd.get("city"),
                        "isp": gd.get("org"),
                    }
                except Exception:
                    return {"ip": ip, "country": None, "city": None, "isp": None}
    except Exception as exc:
        log.warning("proxy_ip_resolve_failed", error=str(exc))
        return {"ip": None, "country": None, "city": None, "isp": None}


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get(
    "/status",
    response_model=ProxyStatusResponse,
    summary="Proxy pool status — active IP, last rotation, pool info",
)
async def get_proxy_status(redis: RedisDep) -> ProxyStatusResponse:
    proxies = _parse_proxies_file()
    pool_size = len(proxies)

    # Load active proxy index from Redis (defaults to 0)
    active_raw = await redis.get(PROXY_ACTIVE_KEY)
    active_data: dict[str, Any] = json.loads(active_raw) if active_raw else {}
    active_index: int | None = active_data.get("index", 0 if proxies else None)

    # Last rotation
    rotation_raw = await redis.lindex(PROXY_ROTATION_KEY, -1)
    last_rotation_at: str | None = None
    last_rotation_ago: float | None = None
    total_rotations = await redis.llen(PROXY_ROTATION_KEY)

    if rotation_raw:
        try:
            rev = json.loads(rotation_raw)
            last_rotation_at = rev.get("ts")
            if last_rotation_at:
                ts = datetime.fromisoformat(last_rotation_at.replace("Z", "+00:00"))
                last_rotation_ago = (datetime.now(timezone.utc) - ts).total_seconds()
        except Exception:
            pass

    # Resolve current public IP (cached for 60s)
    active_proxy = proxies[active_index] if (active_index is not None and proxies) else None
    active_label = active_proxy["label"] if active_proxy else None

    ip_info: dict[str, Any] = {"ip": None, "country": None, "city": None, "isp": None}
    if active_proxy:
        cached_ip = await redis.get(PROXY_IP_CACHE_KEY)
        if cached_ip:
            try:
                ip_info = json.loads(cached_ip)
            except Exception:
                pass
        else:
            ip_info = await _resolve_public_ip(active_proxy)
            await redis.setex(PROXY_IP_CACHE_KEY, 60, json.dumps(ip_info))

    return ProxyStatusResponse(
        pool_size=pool_size,
        proxies=[
            ProxyEntry(
                index=p["index"],
                scheme=p["scheme"],
                host=p["host"],
                port=p["port"],
                username=p["username"],
                label=p["label"],
                raw_line=p["raw_line"],
            )
            for p in proxies
        ],
        active_index=active_index,
        active_label=active_label,
        active_public_ip=ip_info.get("ip"),
        active_ip_country=ip_info.get("country"),
        active_ip_city=ip_info.get("city"),
        active_ip_isp=ip_info.get("isp"),
        last_rotation_at=last_rotation_at,
        last_rotation_ago_seconds=last_rotation_ago,
        total_rotations=total_rotations,
        provider="Evomi Residential",
        provider_plan="Core Residential – IL Pool",
        proxies_file_path=str(PROXIES_FILE),
    )


@router.get(
    "/rotations",
    response_model=RotationHistoryResponse,
    summary="Proxy rotation history",
)
async def get_rotation_history(redis: RedisDep, limit: int = 20) -> RotationHistoryResponse:
    limit = min(limit, 50)
    raw_entries = await redis.lrange(PROXY_ROTATION_KEY, -limit, -1)
    events: list[RotationEvent] = []
    for raw in reversed(raw_entries):
        try:
            d = json.loads(raw) if isinstance(raw, str) else raw
            events.append(RotationEvent(
                ts=d.get("ts", ""),
                from_index=d.get("from_index"),
                to_index=d.get("to_index", 0),
                to_label=d.get("to_label", ""),
                resolved_ip=d.get("resolved_ip"),
                trigger=d.get("trigger", "auto"),
            ))
        except Exception:
            pass
    return RotationHistoryResponse(events=events, total=len(events))


@router.post(
    "/rotate",
    response_model=RotateResponse,
    summary="Manually rotate to next proxy in pool",
)
async def rotate_proxy(redis: RedisDep) -> RotateResponse:
    proxies = _parse_proxies_file()
    if not proxies:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="No proxies configured in proxies.txt")

    active_raw = await redis.get(PROXY_ACTIVE_KEY)
    active_data: dict[str, Any] = json.loads(active_raw) if active_raw else {}
    current_index: int = active_data.get("index", 0)

    new_index = (current_index + 1) % len(proxies)
    new_proxy = proxies[new_index]

    # Invalidate IP cache so next /status call resolves fresh IP
    await redis.delete(PROXY_IP_CACHE_KEY)

    # Resolve new IP
    ip_info = await _resolve_public_ip(new_proxy)
    await redis.setex(PROXY_IP_CACHE_KEY, 60, json.dumps(ip_info))

    # Persist active proxy
    now = datetime.now(timezone.utc).isoformat()
    await redis.set(PROXY_ACTIVE_KEY, json.dumps({
        "index": new_index,
        "label": new_proxy["label"],
        "updated_at": now,
    }))

    # Record rotation event
    event = {
        "ts": now,
        "from_index": current_index,
        "to_index": new_index,
        "to_label": new_proxy["label"],
        "resolved_ip": ip_info.get("ip"),
        "trigger": "manual",
    }
    await redis.rpush(PROXY_ROTATION_KEY, json.dumps(event))
    await redis.ltrim(PROXY_ROTATION_KEY, -200, -1)

    log.info("proxy_rotated_manually", from_idx=current_index, to_idx=new_index, label=new_proxy["label"])

    return RotateResponse(
        status="ok",
        new_index=new_index,
        new_label=new_proxy["label"],
        resolved_ip=ip_info.get("ip"),
    )
