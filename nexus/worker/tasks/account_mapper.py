"""
account_mapper.map — ACCOUNT-MAPPER

Maps Telegram assets (channels, groups, bot peers) for every staged Telethon
session under ``data/staged_accounts/``, using PySocks residential SOCKS5
proxies when configured and a controlled warm-up between logins.

Task type
---------
account_mapper.map

Parameters (optional)
---------------------
staged_dir          : str   — override path (default: <repo>/data/staged_accounts)
output_path         : str   — JSON output file (default: staged_dir/map_<ts>.json)
controlled_warmup   : bool  — if True (default), delay uses μ+σε with μ∈[30,60]s
warmup_mu_min_s     : float — default 30
warmup_mu_max_s     : float — default 60
warmup_sigma_min_s  : float — default 5
warmup_sigma_max_s  : float — default 15
cooldown_min_s      : float — used only when controlled_warmup is False (default 8)
cooldown_max_s      : float — used only when controlled_warmup is False (default 45)
premium_scan_limit  : int   — max participants to inspect per group for Premium
                              headcount (None = no cap)

Environment
-----------
NEXUS_RESIDENTIAL_PROXY_URL / NEXUS_RESIDENTIAL_PROXY_POOL — SOCKS5 rotation
NEXUS_MAPPER_REQUIRE_PROXY=1 — fail fast if no proxy URL is configured
NEXUS_PREMIUM_SCAN_LIMIT — default cap for premium iter_participants
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psutil
import structlog

from nexus.shared.staged_accounts import (
    discover_session_meta_json_files,
    staged_accounts_root,
)
from nexus.shared.tg_connection import (
    parse_residential_proxy_pool,
    telethon_connect_kwargs_for_meta_json,
)
from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)

CPU_THRESHOLD = 60.0

_DEFAULT_STAGED = staged_accounts_root()

_PERM_KEYS = (
    "is_creator",
    "is_admin",
    "change_info",
    "post_messages",
    "edit_messages",
    "delete_messages",
    "ban_users",
    "invite_users",
    "pin_messages",
    "add_admins",
    "anonymous",
    "manage_call",
    "has_left",
    "is_banned",
)


def _controlled_warmup_delay_s(
    mu_min: float,
    mu_max: float,
    sigma_min: float,
    sigma_max: float,
) -> float:
    """T_delay = μ + σ·ε with μ,σ drawn per interval; ε ~ N(0,1)."""
    mu = random.uniform(mu_min, mu_max)
    sig = random.uniform(sigma_min, sigma_max)
    eps = random.gauss(0.0, 1.0)
    return max(15.0, mu + sig * eps)


def _participant_perm_dict(perm: Any) -> dict[str, bool]:
    if perm is None:
        return {}
    out: dict[str, bool] = {}
    for key in _PERM_KEYS:
        if hasattr(perm, key):
            out[key] = bool(getattr(perm, key))
    return out


def _member_count(client: Any, entity: Any) -> int:
    from telethon.tl.functions.channels import GetFullChannelRequest  # type: ignore
    from telethon.tl.functions.messages import GetFullChatRequest  # type: ignore
    from telethon.tl.types import Channel, Chat, User  # type: ignore

    if isinstance(entity, User):
        return 0
    try:
        if isinstance(entity, Channel):
            r = client(GetFullChannelRequest(entity))
            return int(getattr(r.full_chat, "participants_count", 0) or 0)
        if isinstance(entity, Chat):
            r = client(GetFullChatRequest(chat_id=entity.id))
            return int(getattr(r.full_chat, "participants_count", 0) or 0)
    except Exception as exc:
        log.debug("account_mapper_member_count_failed", error=str(exc))
    return 0


def _is_managed(client: Any, entity: Any) -> bool:
    from telethon.tl.types import Channel, Chat, User  # type: ignore

    if isinstance(entity, User):
        return bool(getattr(entity, "bot", False))
    if isinstance(entity, Chat):
        if getattr(entity, "creator", False):
            return True
        try:
            perm = client.get_permissions(entity)
            return bool(perm and (perm.is_admin or perm.is_creator))
        except Exception:
            return False
    if isinstance(entity, Channel):
        if getattr(entity, "creator", False):
            return True
        try:
            perm = client.get_permissions(entity)
            return bool(perm and (perm.is_admin or perm.is_creator))
        except Exception:
            return False
    return False


def _asset_kind(entity: Any) -> str | None:
    from telethon.tl.types import Channel, Chat, User  # type: ignore

    if isinstance(entity, User):
        return "bot_peer" if getattr(entity, "bot", False) else None
    if isinstance(entity, Chat):
        return "group"
    if isinstance(entity, Channel):
        if entity.broadcast:
            return "channel"
        if entity.megagroup:
            return "supergroup"
        return "channel"
    return None


def _count_premium_members(
    client: Any,
    entity: Any,
    limit: int | None,
) -> tuple[int | None, bool]:
    from telethon.tl.types import Channel, Chat  # type: ignore

    if not isinstance(entity, (Chat, Channel)):
        return None, False
    if isinstance(entity, Channel) and entity.broadcast:
        return None, False

    n = 0
    partial = False
    try:
        for idx, participant in enumerate(client.iter_participants(entity)):
            if limit is not None and idx >= limit:
                partial = True
                break
            if getattr(participant, "premium", False):
                n += 1
        return n, partial
    except Exception as exc:
        log.warning("account_mapper_premium_scan_failed", error=str(exc))
        return None, False


def _map_one_session(
    meta_json: Path,
    premium_scan_limit: int | None,
) -> dict[str, Any]:
    from telethon.sync import TelegramClient  # type: ignore
    from telethon.tl.types import Channel, Chat, User  # type: ignore

    with open(meta_json, encoding="utf-8") as f:
        meta = json.load(f)

    api_id = int(meta["api_id"])
    api_hash = str(meta["api_hash"])
    session_file = str(meta_json.with_suffix(""))

    session_label = meta_json.stem
    t_kw = telethon_connect_kwargs_for_meta_json(meta_json)
    client = TelegramClient(session_file, api_id, api_hash, **t_kw)
    client.connect()
    if not client.is_user_authorized():
        client.disconnect()
        raise PermissionError(f"Session not authorized: {session_label}")

    me = client.get_me()
    account_premium = bool(getattr(me, "premium", False))
    phone_raw = getattr(me, "phone", None)
    phone_str = str(phone_raw).strip() if phone_raw is not None else ""

    assets: list[dict[str, Any]] = []

    try:
        for dialog in client.iter_dialogs():
            entity = dialog.entity
            if not _is_managed(client, entity):
                continue

            kind = _asset_kind(entity)
            if kind is None:
                continue

            title = getattr(entity, "title", None) or getattr(
                entity, "first_name", ""
            ) or str(getattr(entity, "id", ""))
            username = getattr(entity, "username", None)

            perm = None
            try:
                if not (isinstance(entity, User) and getattr(entity, "bot", False)):
                    perm = client.get_permissions(entity)
            except Exception:
                perm = None

            admin_rights = _participant_perm_dict(perm)
            if isinstance(entity, Channel) and getattr(entity, "creator", False):
                admin_rights.setdefault("is_creator", True)
            if isinstance(entity, Chat) and getattr(entity, "creator", False):
                admin_rights.setdefault("is_creator", True)

            members = _member_count(client, entity)

            entry: dict[str, Any] = {
                "kind": kind,
                "id": entity.id,
                "title": title,
                "username": username,
                "member_count": members,
                "admin_rights": admin_rights,
            }

            if kind in ("group", "supergroup") and account_premium:
                pm, p_partial = _count_premium_members(
                    client, entity, premium_scan_limit
                )
                entry["premium_members"] = pm
                if p_partial:
                    entry["premium_scan_partial"] = True
            elif kind in ("group", "supergroup"):
                entry["premium_members"] = None

            assets.append(entry)

        assets.sort(key=lambda a: int(a.get("member_count") or 0), reverse=True)

        return {
            "session_file": session_label,
            "meta_path": str(meta_json),
            "user_id": me.id,
            "phone": phone_str,
            "account_premium": account_premium,
            "assets": assets,
        }
    finally:
        try:
            client.disconnect()
        except Exception:
            pass


def _run_map_job(
    staged_dir: Path,
    output_path: Path,
    *,
    controlled_warmup: bool,
    warmup_mu_min_s: float,
    warmup_mu_max_s: float,
    warmup_sigma_min_s: float,
    warmup_sigma_max_s: float,
    cooldown_min_s: float,
    cooldown_max_s: float,
    premium_scan_limit: int | None,
) -> dict[str, Any]:
    staged_dir = Path(staged_dir)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    pool = parse_residential_proxy_pool()
    require_proxy = (os.getenv("NEXUS_MAPPER_REQUIRE_PROXY") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if require_proxy and not pool:
        raise RuntimeError(
            "NEXUS_MAPPER_REQUIRE_PROXY is set but no NEXUS_RESIDENTIAL_PROXY_URL "
            "or NEXUS_RESIDENTIAL_PROXY_POOL configured"
        )

    meta_files = discover_session_meta_json_files(staged_dir)
    if not meta_files:
        return {
            "status": "no_sessions",
            "staged_dir": str(staged_dir),
            "sessions": [],
            "combined_assets": [],
            "output_path": str(output_path),
        }

    if not controlled_warmup and cooldown_max_s < cooldown_min_s:
        cooldown_min_s, cooldown_max_s = cooldown_max_s, cooldown_min_s

    if controlled_warmup and warmup_mu_max_s < warmup_mu_min_s:
        warmup_mu_min_s, warmup_mu_max_s = warmup_mu_max_s, warmup_mu_min_s
    if controlled_warmup and warmup_sigma_max_s < warmup_sigma_min_s:
        warmup_sigma_min_s, warmup_sigma_max_s = warmup_sigma_max_s, warmup_sigma_min_s

    sessions_out: list[dict[str, Any]] = []
    combined: list[dict[str, Any]] = []

    for i, meta_path in enumerate(meta_files):
        if i > 0:
            if controlled_warmup:
                delay = _controlled_warmup_delay_s(
                    warmup_mu_min_s,
                    warmup_mu_max_s,
                    warmup_sigma_min_s,
                    warmup_sigma_max_s,
                )
            else:
                delay = random.uniform(cooldown_min_s, cooldown_max_s)
            log.info(
                "account_mapper_warmup_delay",
                seconds=round(delay, 2),
                controlled=controlled_warmup,
                before_session=meta_path.stem,
            )
            time.sleep(delay)

        try:
            one = _map_one_session(
                meta_path,
                premium_scan_limit=premium_scan_limit,
            )
            one["status"] = "ok"
            sessions_out.append(one)
            for asset in one.get("assets", []):
                row = dict(asset)
                row["session"] = one.get("session_file")
                combined.append(row)
        except Exception as exc:
            log.error(
                "account_mapper_session_failed",
                session=meta_path.stem,
                error=str(exc),
            )
            sessions_out.append(
                {
                    "session_file": meta_path.stem,
                    "meta_path": str(meta_path),
                    "status": "failed",
                    "error": str(exc),
                    "assets": [],
                }
            )

    combined.sort(key=lambda a: int(a.get("member_count") or 0), reverse=True)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "staged_dir": str(staged_dir),
        "proxy_pool_size": len(pool),
        "controlled_warmup": controlled_warmup,
        "sessions": sessions_out,
        "combined_assets": combined,
    }
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    payload["output_path"] = str(output_path)
    payload["status"] = "completed"

    try:
        from nexus.shared.reporting import write_master_fleet_from_mapper_payload

        write_master_fleet_from_mapper_payload(payload, staged_dir)
    except Exception as exc:
        log.warning("master_fleet_report_write_failed", error=str(exc))

    return payload


@registry.register("account_mapper.map")
async def map_accounts(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Enumerate channels, supergroups, groups, and bot peers managed by each
    staged session; optionally count Premium members in groups (Premium
    accounts only). Results are written to JSON plus master fleet CSV/JSON.
    """
    t0 = time.monotonic()

    staged_dir = Path(
        parameters.get("staged_dir", str(_DEFAULT_STAGED)),
    )
    controlled = parameters.get("controlled_warmup")
    controlled_warmup = True if controlled is None else bool(controlled)

    warmup_mu_min_s = float(parameters.get("warmup_mu_min_s", 30))
    warmup_mu_max_s = float(parameters.get("warmup_mu_max_s", 60))
    warmup_sigma_min_s = float(parameters.get("warmup_sigma_min_s", 5))
    warmup_sigma_max_s = float(parameters.get("warmup_sigma_max_s", 15))

    cooldown_min_s = float(parameters.get("cooldown_min_s", 8))
    cooldown_max_s = float(parameters.get("cooldown_max_s", 45))

    raw_limit = parameters.get("premium_scan_limit")
    premium_scan_limit: int | None
    if raw_limit is None or raw_limit == "":
        env_lim = os.getenv("NEXUS_PREMIUM_SCAN_LIMIT")
        premium_scan_limit = int(env_lim) if env_lim and env_lim.isdigit() else None
    else:
        premium_scan_limit = int(raw_limit)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    default_out = staged_dir / f"map_{ts}.json"
    output_path = Path(parameters.get("output_path", str(default_out)))

    cpu = psutil.cpu_percent(interval=1)
    if cpu > CPU_THRESHOLD:
        log.warning("account_mapper_low_resources", cpu=cpu)
        return {
            "status": "low_resources",
            "cpu_percent": cpu,
            "duration_s": round(time.monotonic() - t0, 2),
        }

    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: _run_map_job(
                staged_dir,
                output_path,
                controlled_warmup=controlled_warmup,
                warmup_mu_min_s=warmup_mu_min_s,
                warmup_mu_max_s=warmup_mu_max_s,
                warmup_sigma_min_s=warmup_sigma_min_s,
                warmup_sigma_max_s=warmup_sigma_max_s,
                cooldown_min_s=cooldown_min_s,
                cooldown_max_s=cooldown_max_s,
                premium_scan_limit=premium_scan_limit,
            ),
        )
    except Exception as exc:
        log.exception("account_mapper_error", error=str(exc))
        return {
            "status": "failed",
            "error": str(exc),
            "duration_s": round(time.monotonic() - t0, 2),
        }

    result["duration_s"] = round(time.monotonic() - t0, 2)
    return result
