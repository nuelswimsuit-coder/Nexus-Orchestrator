#!/usr/bin/env python3
"""
NIGHT-WATCH-EXECUTION-INITIALIZATION — one-shot operator setup.

1. Writes Redis ``nexus:swarm:warmer:groups`` from a manifest (four groups,
   four primary actor sessions prepended for each group).
2. Upserts Retention Guardian env in ``.env`` (baseline 2100, 4h interval).
3. Arms Ultimate Scalper LIVE (``nexus:scalper:simulation_mode=false``) and
   trading race mode (``nexus:control:trading_mode=race``).

Linux workers: ``launch_worker.sh`` already exports ``NEXUS_WORKER_CPU_UTIL_TARGET=90``
and optional ``cpulimit -l 90``.

Usage::
    python scripts/night_watch_execution_init.py
    python scripts/night_watch_execution_init.py --apply

Env::
    NIGHT_WATCH_MANIFEST — path to JSON (default: config/night_watch_manifest.json)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SWARM_GROUPS_KEY = "nexus:swarm:warmer:groups"
SWARM_STATE_PREFIX = "nexus:swarm:warmer:state:"
SIM_MODE_KEY = "nexus:scalper:simulation_mode"
TRADING_MODE_KEY = "nexus:control:trading_mode"


def _upsert_dotenv_key(env_path: Path, key: str, value: str) -> None:
    assign = f"{key}={value}"
    if not env_path.is_file():
        env_path.write_text(assign + "\n", encoding="utf-8")
        return
    lines = env_path.read_text(encoding="utf-8").splitlines()
    out: list[str] = []
    seen = False
    for line in lines:
        s = line.strip()
        if s and not s.startswith("#") and "=" in s:
            k, _, _ = s.partition("=")
            if k.strip() == key:
                if not seen:
                    out.append(assign)
                    seen = True
                continue
        out.append(line)
    if not seen:
        out.append(assign)
    env_path.write_text("\n".join(out) + "\n", encoding="utf-8")


def _session_entries(
    primary: list[str],
    extras: list[dict[str, str]],
) -> list[dict[str, str]]:
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for p in primary:
        p = str(p).strip()
        if not p or p in seen:
            continue
        seen.add(p)
        out.append({"session_path": p, "username": ""})
    for row in extras:
        if not isinstance(row, dict):
            continue
        p = str(row.get("session_path", "")).strip()
        if not p or p in seen:
            continue
        seen.add(p)
        out.append({
            "session_path": p,
            "username": str(row.get("username", "") or ""),
        })
    return out


def _load_manifest(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("manifest root must be an object")
    data.pop("_comment", None)
    return data


def _build_redis_groups(manifest: dict) -> dict[str, dict]:
    primary = manifest.get("primary_actor_session_paths") or []
    if not isinstance(primary, list) or len(primary) != 4:
        raise ValueError("primary_actor_session_paths must be a list of exactly 4 strings")
    groups = manifest.get("groups") or []
    if not isinstance(groups, list) or len(groups) != 4:
        raise ValueError("groups must be a list of exactly 4 objects")

    out: dict[str, dict] = {}
    for g in groups:
        if not isinstance(g, dict):
            raise ValueError("each group must be an object")
        gid = g.get("group_id")
        if gid is None:
            raise ValueError("each group needs group_id")
        gkey = str(g.get("group_key", gid)).strip()
        extras = g.get("extra_sessions") or []
        if not isinstance(extras, list):
            extras = []
        sessions = _session_entries([str(x) for x in primary], extras)
        out[gkey] = {
            "group_id": int(gid),
            "sessions": sessions,
            "timezone": str(g.get("timezone", "UTC") or "UTC"),
            "enabled": bool(g.get("enabled", True)),
            "group_title": str(g.get("group_title", "") or ""),
            "engagement_mode": str(g.get("engagement_mode", "high") or "high"),
        }
    return out


def _retention_json(groups_manifest: list) -> str:
    rows: list[dict[str, str]] = []
    for g in groups_manifest:
        if not isinstance(g, dict):
            continue
        gid = g.get("group_id")
        label = str(g.get("group_title") or g.get("group_key") or gid or "").strip()
        rows.append({"id": str(gid), "label": label or str(gid)})
    return json.dumps(rows, ensure_ascii=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Night Watch execution initialization")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write Redis and .env (default is dry-run)",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Override NIGHT_WATCH_MANIFEST",
    )
    args = parser.parse_args()

    mpath = args.manifest or Path(
        os.getenv("NIGHT_WATCH_MANIFEST", str(PROJECT_ROOT / "config" / "night_watch_manifest.json"))
    )
    if not mpath.is_file():
        print(f"[error] Manifest not found: {mpath}", file=sys.stderr)
        print("  Copy config/night_watch_manifest.example.json, fill real group_id + session paths.", file=sys.stderr)
        sys.exit(2)

    manifest = _load_manifest(mpath)
    redis_groups = _build_redis_groups(manifest)
    retention_payload = _retention_json(manifest.get("groups") or [])

    dry = not args.apply
    print("[night-watch] manifest:", mpath)
    print("[night-watch] mode:", "DRY-RUN" if dry else "APPLY")
    print("[night-watch] swarm registry groups:", list(redis_groups.keys()))
    print("[night-watch] engagement:", {k: v.get("engagement_mode") for k, v in redis_groups.items()})

    env_path = PROJECT_ROOT / ".env"
    env_lines = [
        ("RETENTION_MONITOR_ENABLED", "1"),
        ("RETENTION_MONITOR_INTERVAL_S", str(4 * 3600)),
        ("RETENTION_MEMBER_BASELINE", "2100"),
        ("RETENTION_GROUPS_JSON", retention_payload),
        ("POLY_SCALPER_SIMULATION_MODE", "false"),
    ]

    if dry:
        print("[night-watch] would set Redis", SWARM_GROUPS_KEY, "(len", len(json.dumps(redis_groups)), "bytes)")
        for k, v in env_lines:
            preview = v if k != "RETENTION_GROUPS_JSON" else v[:120] + ("…" if len(v) > 120 else "")
            print(f"[night-watch] would upsert .env {k}={preview}")
        print("[night-watch] would set Redis", SIM_MODE_KEY, "= false")
        print("[night-watch] would set Redis", TRADING_MODE_KEY, "= race")
        print("[night-watch] would delete", SWARM_STATE_PREFIX + "*", "(per-group keys listed):")
        for gk in redis_groups:
            print("   ", SWARM_STATE_PREFIX + gk)
        return

    from redis import Redis
    from redis.exceptions import ConnectionError as RedisConnectionError

    from nexus.shared.config import settings

    r = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        r.ping()
    except RedisConnectionError as exc:
        print(f"[error] Redis unreachable ({settings.redis_url}): {exc}", file=sys.stderr)
        sys.exit(1)
    r.set(SWARM_GROUPS_KEY, json.dumps(redis_groups, ensure_ascii=False))
    for gk in redis_groups:
        r.delete(f"{SWARM_STATE_PREFIX}{gk}")
    r.set(SIM_MODE_KEY, "false")
    r.set(TRADING_MODE_KEY, "race")

    for k, v in env_lines:
        _upsert_dotenv_key(env_path, k, v)

    print("[ok] Redis swarm registry + scalper live + trading race")
    print("[ok] .env retention guardian (baseline 2100, 4h) — restart Master to pick up env if running")
    print("[hint] Ensure workers run with NEXUS_WORKER_LOW_POWER unset and NEXUS_WORKER_CPU_UTIL_TARGET=90 (launch_worker.sh)")


if __name__ == "__main__":
    main()
