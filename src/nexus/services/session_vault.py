"""
Central Telethon session vault (Master node).

Primary layout: ``vault/sessions/*.session`` + matching ``*.json`` (override with
``NEXUS_SESSION_VAULT_DIR``). Legacy paths ``data/session_vault/`` and
``data/staged_accounts/`` remain discoverable. **Disk is authoritative** for which
Telegram accounts exist; Redis holds optional **cached** metadata, worker **leases**,
and discovery counters — not a substitute for scanning the vault on disk.

Session health is classified via Telethon (user MTProto). Aiogram is not used here
because vault entries are user sessions; bot tokens would need a separate path.

Environment:

* ``NEXUS_SESSION_VAULT_SKIP_PROBE`` — if ``1``/``true``, :func:`sync_disk_to_redis`
  warms Redis cache without Telethon probes (faster API startup for large vaults).
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

import structlog

log = structlog.get_logger(__name__)

_REPO_ROOT = Path(__file__).resolve().parents[3]

INDEX_KEY = "nexus:session_vault:index"
META_PREFIX = "nexus:session_vault:meta:"
LEASE_PREFIX = "nexus:session_vault:lease:"
DISCOVERY_TOTAL_KEY = "nexus:session_vault:discovery:total"
DISCOVERY_LAST_KEY = "nexus:session_vault:discovery:last"


class SessionHealth(str, Enum):
    GREEN = "green"
    YELLOW = "yellow"
    RED = "red"


class SessionStatus(str, Enum):
    ACTIVE = "active"
    DEGRADED = "degraded"
    BANNED = "banned"
    OFFLINE = "offline"
    UNKNOWN = "unknown"


def vault_root() -> Path:
    raw = (os.getenv("NEXUS_SESSION_VAULT_DIR") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    hub = (_REPO_ROOT / "vault" / "sessions").resolve()
    if hub.is_dir():
        return hub
    return (_REPO_ROOT / "data" / "session_vault").resolve()


def vault_candidate_roots() -> list[Path]:
    roots: list[Path] = []
    hub = (_REPO_ROOT / "vault" / "sessions").resolve()
    if hub.is_dir() and hub not in {r.resolve() for r in roots}:
        roots.append(hub)
    vr = vault_root()
    if vr.resolve() not in {r.resolve() for r in roots}:
        roots.append(vr.resolve())
    legacy_sv = (_REPO_ROOT / "data" / "session_vault").resolve()
    if legacy_sv.is_dir() and legacy_sv not in {r.resolve() for r in roots}:
        roots.append(legacy_sv)
    legacy = (_REPO_ROOT / "data" / "staged_accounts").resolve()
    if legacy.is_dir() and legacy not in {r.resolve() for r in roots}:
        roots.append(legacy)
    return roots


def meta_key(stem: str) -> str:
    return f"{META_PREFIX}{stem}"


def lease_key(stem: str) -> str:
    return f"{LEASE_PREFIX}{stem}"


def discover_meta_paths_from_session_sqlite() -> list[Path]:
    """
    Every Telethon ``*.session`` (sqlite) under the vault must be indexed; pair
    with ``<stem>.json`` containing ``api_id`` / ``api_hash``.
    """
    seen: set[Path] = set()
    out: list[Path] = []
    for root in vault_candidate_roots():
        if not root.is_dir():
            continue
        for sess in sorted(root.rglob("*.session")):
            if sess.name.endswith("-journal"):
                continue
            meta = sess.with_suffix(".json")
            if not meta.is_file():
                continue
            try:
                data = json.loads(meta.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(data, dict) or "api_id" not in data or "api_hash" not in data:
                continue
            rp = meta.resolve()
            if rp in seen:
                continue
            seen.add(rp)
            out.append(meta)
    return sorted(out, key=lambda p: p.as_posix().lower())


def discover_all_meta_json_files() -> list[Path]:
    seen: set[Path] = set()
    out: list[Path] = []
    for path in discover_meta_paths_from_session_sqlite():
        rp = path.resolve()
        if rp not in seen:
            seen.add(rp)
            out.append(path)
    for root in vault_candidate_roots():
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(data, dict) or "api_id" not in data or "api_hash" not in data:
                continue
            rp = path.resolve()
            if rp in seen:
                continue
            seen.add(rp)
            out.append(path)
    return sorted(out, key=lambda p: p.as_posix().lower())


def _session_path_base(meta_json: Path) -> str:
    return str(meta_json.with_suffix(""))


def check_session_health_sync(meta_json: Path) -> dict[str, Any]:
    """
    Blocking Telethon probe: authorized, banned/deactivated, or error.
    """
    from telethon.errors import (  # type: ignore[import-untyped]
        AuthKeyDuplicatedError,
        AuthKeyUnregisteredError,
        PhoneNumberBannedError,
        UserDeactivatedBanError,
        UserDeactivatedError,
    )
    from telethon.sync import TelegramClient  # type: ignore[import-untyped]

    stem = meta_json.stem
    try:
        meta = json.loads(meta_json.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "session_stem": stem,
            "phone": None,
            "status": SessionStatus.UNKNOWN.value,
            "health": SessionHealth.RED.value,
            "detail": f"invalid meta: {exc}",
        }

    if not isinstance(meta, dict):
        return {
            "session_stem": stem,
            "phone": None,
            "status": SessionStatus.UNKNOWN.value,
            "health": SessionHealth.RED.value,
            "detail": "meta is not a JSON object",
        }

    phone = meta.get("phone")
    try:
        api_id = int(meta["api_id"])
        api_hash = str(meta["api_hash"])
    except (KeyError, TypeError, ValueError) as exc:
        return {
            "session_stem": stem,
            "phone": phone,
            "status": SessionStatus.OFFLINE.value,
            "health": SessionHealth.RED.value,
            "detail": f"invalid api_id/api_hash: {exc}",
        }

    session_base = _session_path_base(meta_json)
    client = TelegramClient(session_base, api_id, api_hash)
    try:
        client.connect()
        if not client.is_user_authorized():
            return {
                "session_stem": stem,
                "phone": phone,
                "status": SessionStatus.OFFLINE.value,
                "health": SessionHealth.RED.value,
                "detail": "not authorized",
            }
        try:
            me = client.get_me()
        except (UserDeactivatedBanError, PhoneNumberBannedError) as exc:
            return {
                "session_stem": stem,
                "phone": phone,
                "status": SessionStatus.BANNED.value,
                "health": SessionHealth.RED.value,
                "detail": type(exc).__name__,
            }
        except UserDeactivatedError as exc:
            return {
                "session_stem": stem,
                "phone": phone,
                "status": SessionStatus.BANNED.value,
                "health": SessionHealth.RED.value,
                "detail": type(exc).__name__,
            }
        except (AuthKeyUnregisteredError, AuthKeyDuplicatedError) as exc:
            return {
                "session_stem": stem,
                "phone": phone,
                "status": SessionStatus.OFFLINE.value,
                "health": SessionHealth.RED.value,
                "detail": type(exc).__name__,
            }
        except Exception as exc:
            return {
                "session_stem": stem,
                "phone": phone,
                "status": SessionStatus.DEGRADED.value,
                "health": SessionHealth.YELLOW.value,
                "detail": str(exc),
            }

        return {
            "session_stem": stem,
            "phone": phone or getattr(me, "phone", None),
            "status": SessionStatus.ACTIVE.value,
            "health": SessionHealth.GREEN.value,
            "user_id": me.id,
            "username": getattr(me, "username", None),
            "detail": None,
        }
    except Exception as exc:
        return {
            "session_stem": stem,
            "phone": phone,
            "status": SessionStatus.OFFLINE.value,
            "health": SessionHealth.RED.value,
            "detail": str(exc),
        }
    finally:
        try:
            client.disconnect()
        except Exception:
            pass


def export_string_session_sync(meta_json: Path) -> dict[str, Any]:
    """Blocking: load file session and return Telethon StringSession + api creds."""
    from telethon.sessions import StringSession  # type: ignore[import-untyped]
    from telethon.sync import TelegramClient  # type: ignore[import-untyped]

    meta = json.loads(meta_json.read_text(encoding="utf-8"))
    api_id = int(meta["api_id"])
    api_hash = str(meta["api_hash"])
    session_base = _session_path_base(meta_json)
    client = TelegramClient(session_base, api_id, api_hash)
    try:
        client.connect()
        if not client.is_user_authorized():
            raise ValueError("session not authorized")
        string_session = StringSession.save(client.session)
        return {
            "session_stem": meta_json.stem,
            "string_session": string_session,
            "api_id": api_id,
            "api_hash": api_hash,
        }
    finally:
        try:
            client.disconnect()
        except Exception:
            pass


def _default_meta_record(meta_json: Path) -> dict[str, Any]:
    try:
        meta = json.loads(meta_json.read_text(encoding="utf-8"))
    except Exception:
        meta = {}
    phone = meta.get("phone") if isinstance(meta, dict) else None
    proxy_ip = ""
    if isinstance(meta, dict):
        proxy_ip = str(meta.get("proxy_ip") or meta.get("proxy_host") or "").strip()
    return {
        "session_stem": meta_json.stem,
        "phone": phone,
        "proxy_ip": proxy_ip,
        "status": SessionStatus.UNKNOWN.value,
        "health": SessionHealth.YELLOW.value,
        "user_id": meta.get("user_id") if isinstance(meta, dict) else None,
        "username": (meta.get("username") if isinstance(meta, dict) else None),
        "checked_at": None,
        "meta_path": str(meta_json.resolve()),
        "detail": None,
    }


async def emit_discovery_signal(redis: Any, stem: str) -> None:
    """Notify dashboards (Redis counters + log) when a new vault account stem is indexed."""
    try:
        await redis.incr(DISCOVERY_TOTAL_KEY)
        await redis.set(
            DISCOVERY_LAST_KEY,
            json.dumps(
                {"stem": stem, "ts": datetime.now(timezone.utc).isoformat()},
                ensure_ascii=False,
            ),
        )
    except Exception as exc:
        log.warning("session_vault_discovery_signal_failed", stem=stem, error=str(exc))
    log.info("session_vault_discovery", stem=stem, signal="Discovery")


async def merge_meta_row(redis: Any, meta_json: Path, row: dict[str, Any]) -> None:
    key = meta_key(meta_json.stem)
    raw = await redis.get(key)
    base: dict[str, Any] = {}
    if raw:
        try:
            base = json.loads(raw)
        except Exception:
            base = {}
    merged = {**base, **row}
    merged.setdefault("session_stem", meta_json.stem)
    merged["meta_path"] = str(meta_json.resolve())
    await redis.set(key, json.dumps(merged, ensure_ascii=False))


async def _probe_and_merge_meta(redis: Any, meta_json: Path) -> None:
    """Telethon health check (thread offload) + merge into Redis."""
    probe = await asyncio.to_thread(check_session_health_sync, meta_json)
    row = _default_meta_record(meta_json)
    row.update(
        {
            "status": probe.get("status", SessionStatus.UNKNOWN.value),
            "health": probe.get("health", SessionHealth.YELLOW.value),
            "user_id": probe.get("user_id"),
            "username": probe.get("username"),
            "phone": probe.get("phone") or row.get("phone"),
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "detail": probe.get("detail"),
        }
    )
    await merge_meta_row(redis, meta_json, row)


async def sync_disk_to_redis(redis: Any) -> dict[str, int]:
    """Warm Redis cache from disk: index stems, optionally Telethon-probe each meta file."""
    paths = discover_all_meta_json_files()
    skip_probe = (os.getenv("NEXUS_SESSION_VAULT_SKIP_PROBE") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    n = 0
    if skip_probe:
        for path in paths:
            added = await redis.sadd(INDEX_KEY, path.stem)
            if added:
                await emit_discovery_signal(redis, path.stem)
            await merge_meta_row(redis, path, _default_meta_record(path))
            n += 1
    else:
        sem = asyncio.Semaphore(4)

        async def _one(p: Path) -> None:
            async with sem:
                await _probe_and_merge_meta(redis, p)

        await asyncio.gather(*(_one(p) for p in paths))
        for path in paths:
            added = await redis.sadd(INDEX_KEY, path.stem)
            if added:
                await emit_discovery_signal(redis, path.stem)
            n += 1
    log.info("session_vault_disk_synced", sessions=n, probed=not skip_probe)
    return {"indexed": n}


async def ingest_new_session_meta(meta_json: Path, redis: Any) -> None:
    """Call after a new session is written on disk (e.g. login verify)."""
    meta_json = meta_json.resolve()
    added = await redis.sadd(INDEX_KEY, meta_json.stem)
    if added:
        await emit_discovery_signal(redis, meta_json.stem)
    probe = check_session_health_sync(meta_json)
    row = _default_meta_record(meta_json)
    row.update(
        {
            "status": probe.get("status", SessionStatus.UNKNOWN.value),
            "health": probe.get("health", SessionHealth.YELLOW.value),
            "user_id": probe.get("user_id"),
            "username": probe.get("username"),
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "detail": probe.get("detail"),
        }
    )
    await merge_meta_row(redis, meta_json, row)


async def refresh_stem_status(redis: Any, stem: str) -> dict[str, Any] | None:
    paths = [p for p in discover_all_meta_json_files() if p.stem == stem]
    if not paths:
        return None
    meta_json = paths[0]
    probe = check_session_health_sync(meta_json)

    row = {
        "status": probe.get("status", SessionStatus.UNKNOWN.value),
        "health": probe.get("health", SessionHealth.YELLOW.value),
        "user_id": probe.get("user_id"),
        "username": probe.get("username"),
        "phone": probe.get("phone"),
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "detail": probe.get("detail"),
    }
    await merge_meta_row(redis, meta_json, row)
    return row


async def refresh_all_statuses(redis: Any) -> int:
    """Re-run Telethon health for every vault meta file on disk (can be slow)."""
    count = 0
    for path in discover_all_meta_json_files():
        if await refresh_stem_status(redis, path.stem):
            count += 1
    return count


async def get_commander_snapshot(redis: Any) -> list[dict[str, Any]]:
    """
    Commander view: enumerate vault **from disk**, overlay Redis-cached meta and leases.
    Accounts present only on disk still appear with defaults until probed or synced.
    """
    by_stem: dict[str, Path] = {}
    for path in discover_all_meta_json_files():
        by_stem.setdefault(path.stem, path)
    stems = sorted(by_stem.keys())
    out: list[dict[str, Any]] = []
    for stem in stems:
        meta_json = by_stem[stem]
        row = dict(_default_meta_record(meta_json))
        raw_meta = await redis.get(meta_key(stem))
        if raw_meta:
            try:
                rm = json.loads(raw_meta)
                if isinstance(rm, dict):
                    row.update(rm)
            except Exception:
                pass
        row.setdefault("session_stem", stem)
        row["meta_path"] = str(meta_json.resolve())
        lease_raw = await redis.get(lease_key(stem))
        if lease_raw:
            try:
                lease = json.loads(lease_raw)
                row["lease_worker_id"] = lease.get("worker_id")
                row["lease_task_id"] = lease.get("task_id")
                ttl = await redis.ttl(lease_key(stem))
                row["lease_ttl_seconds"] = ttl if ttl and ttl > 0 else None
            except Exception:
                row["lease_worker_id"] = None
                row["lease_task_id"] = None
                row["lease_ttl_seconds"] = None
        else:
            row["lease_worker_id"] = None
            row["lease_task_id"] = None
            row["lease_ttl_seconds"] = None
        out.append(row)
    return out


@dataclass
class LeaseConflict:
    stem: str
    holder: str


async def lease_string_sessions(
    redis: Any,
    stems: list[str],
    worker_id: str,
    task_id: str,
    ttl_seconds: int = 900,
) -> tuple[list[dict[str, Any]], list[LeaseConflict]]:
    """
    Grant StringSession payloads for each stem if not leased by another worker.

    All-or-nothing: if any stem conflicts or fails export, no leases are left behind.
    """
    stem_set = sorted({s.strip() for s in stems if s.strip()})
    path_by_stem = {p.stem: p for p in discover_all_meta_json_files()}
    conflicts: list[LeaseConflict] = []

    for stem in stem_set:
        meta_json = path_by_stem.get(stem)
        if meta_json is None:
            conflicts.append(LeaseConflict(stem=stem, holder="missing_on_disk"))
            continue
        raw = await redis.get(lease_key(stem))
        if raw:
            try:
                prev = json.loads(raw)
                if prev.get("worker_id") and prev.get("worker_id") != worker_id:
                    conflicts.append(
                        LeaseConflict(stem=stem, holder=str(prev.get("worker_id")))
                    )
            except Exception:
                pass

    exports: list[tuple[str, dict[str, Any]]] = []
    if not conflicts:
        for stem in stem_set:
            meta_json = path_by_stem[stem]
            try:
                export = export_string_session_sync(meta_json)
            except Exception as exc:
                log.warning("session_vault_export_failed", stem=stem, error=str(exc))
                conflicts.append(LeaseConflict(stem=stem, holder=f"export_error:{exc}"))
                break
            exports.append((stem, export))

    if conflicts or len(exports) != len(stem_set):
        return [], conflicts

    granted: list[dict[str, Any]] = []
    leased: list[str] = []
    try:
        for stem, export in exports:
            payload = {"worker_id": worker_id, "task_id": task_id}
            await redis.set(lease_key(stem), json.dumps(payload), ex=ttl_seconds)
            leased.append(stem)
            granted.append(
                {
                    "session_stem": export["session_stem"],
                    "string_session": export["string_session"],
                    "api_id": export["api_id"],
                    "api_hash": export["api_hash"],
                }
            )
        return granted, []
    except Exception:
        for stem in leased:
            try:
                await redis.delete(lease_key(stem))
            except Exception:
                pass
        raise


async def release_leases(redis: Any, stems: list[str], worker_id: str) -> int:
    released = 0
    for stem in stems:
        raw = await redis.get(lease_key(stem))
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            await redis.delete(lease_key(stem))
            released += 1
            continue
        if data.get("worker_id") == worker_id:
            await redis.delete(lease_key(stem))
            released += 1
    return released
