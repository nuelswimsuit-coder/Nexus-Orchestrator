"""
retention.guardian.monitor — RETENTION-GUARDIAN-V1

Periodic Telethon job: member counts for configured Telegram groups (full-chat
APIs via shared ``_member_count`` — supergroups use ``GetFullChannelRequest``,
basic groups ``GetFullChatRequest``), baseline comparison, optional invite-link
importer tracking, Redis snapshot for the API, and admin Telegram alerts on
material drops.

Configuration (environment)
---------------------------
RETENTION_GROUPS_JSON
    JSON array of up to (typically) four groups, e.g.
    [{"id": "-1001234567890", "label": "Alpha"}, ...]
    ``id`` may be numeric supergroup id, t.me slug, or username.

RETENTION_MEMBER_BASELINE
    Global baseline member count (default 2100) used for drop % and stability.

RETENTION_DROP_ALERT_PCT
    Alert when current count is more than this % below baseline (default 5).

RETENTION_INVITE_LINKS_JSON
    Optional JSON array:
    [{"group_id": "-100...", "invite_hash": "AbCdEfGh"}, ...]
    ``invite_hash`` is the private link fragment after ``t.me/+`` (no plus).

RETENTION_TELETHON_SESSION
    Path to Telethon session file *without* the ``.session`` suffix.
    Default: ``<TELEFIX_PROJECT>/sessions/retention_monitor``

TELEFIX_PROJECT
    Used only for default session path (see auto_scrape).

Secrets (Vault / .env)
----------------------
TELEFIX_API_ID, TELEFIX_API_HASH — Telethon MTProto.
TELEGRAM_BOT_TOKEN, TELEGRAM_ADMIN_CHAT_ID — aiogram alerts to admin chat.

Task type
---------
retention.guardian.monitor
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any

import structlog

from nexus.shared.notifications.base import Alert, AlertLevel
from nexus.shared.notifications.providers.telegram import TelegramProvider
from nexus.shared.retention_redis import RETENTION_HEALTH_SNAPSHOT_KEY, RETENTION_HEALTH_TTL_S
from nexus.agents.task_registry import registry
from nexus.agents.tasks.account_mapper import _member_count

log = structlog.get_logger(__name__)

_DEFAULT_TELEFIX = os.getenv("TELEFIX_PROJECT", r"C:\Users\Yarin\Desktop\Mangement Ahu")


def _parse_groups() -> list[dict[str, Any]]:
    raw = (os.getenv("RETENTION_GROUPS_JSON") or "").strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("retention_groups_json_invalid")
        return []
    if not isinstance(data, list):
        return []
    out: list[dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict) and item.get("id"):
            gid = str(item["id"]).strip()
            label = str(item.get("label") or gid).strip()
            out.append({"id": gid, "label": label})
    return out


def _parse_invite_links() -> list[dict[str, str]]:
    raw = (os.getenv("RETENTION_INVITE_LINKS_JSON") or "").strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("retention_invite_links_json_invalid")
        return []
    if not isinstance(data, list):
        return []
    out: list[dict[str, str]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        gid = str(item.get("group_id") or item.get("id") or "").strip()
        h = str(item.get("invite_hash") or item.get("hash") or "").strip().lstrip("+")
        if gid and h:
            out.append({"group_id": gid, "invite_hash": h})
    return out


def _baseline() -> int:
    try:
        return max(1, int(os.getenv("RETENTION_MEMBER_BASELINE", "2100")))
    except ValueError:
        return 2100


def _alert_drop_pct() -> float:
    try:
        return max(0.0, float(os.getenv("RETENTION_DROP_ALERT_PCT", "5")))
    except ValueError:
        return 5.0


def _session_path() -> str:
    explicit = (os.getenv("RETENTION_TELETHON_SESSION") or "").strip()
    if explicit:
        return explicit
    return os.path.join(_DEFAULT_TELEFIX, "sessions", "retention_monitor")


def _stability_score(current: int, baseline: int) -> int:
    if baseline <= 0:
        return 0
    if current >= baseline:
        return 100
    return max(0, min(100, int(round(100.0 * current / baseline))))


def _drop_pct_vs_baseline(current: int, baseline: int) -> float:
    if baseline <= 0 or current >= baseline:
        return 0.0
    return round(100.0 * (baseline - current) / baseline, 2)


def _sync_run(
    api_id: int,
    api_hash: str,
    session_file: str,
    groups: list[dict[str, Any]],
    baseline: int,
    alert_threshold_pct: float,
    invite_defs: list[dict[str, str]],
) -> dict[str, Any]:
    from telethon.sync import TelegramClient  # type: ignore[import-untyped]
    from telethon.tl.functions.messages import GetChatInviteImportersRequest  # type: ignore
    from telethon.tl.types import InputUser, InputUserEmpty  # type: ignore

    alerts: list[dict[str, Any]] = []
    group_results: list[dict[str, Any]] = []
    invite_tracking: list[dict[str, Any]] = []

    client = TelegramClient(session_file, api_id, api_hash)
    client.connect()
    if not client.is_user_authorized():
        client.disconnect()
        raise PermissionError(f"Telethon session not authorized: {session_file}")

    try:
        for g in groups:
            gid = g["id"]
            label = g["label"]
            try:
                entity = client.get_entity(gid)
            except Exception as exc:
                log.warning("retention_resolve_entity_failed", group=gid, error=str(exc))
                group_results.append({
                    "group_id": gid,
                    "label": label,
                    "error": str(exc),
                    "member_count": None,
                    "baseline": baseline,
                    "stability_score": 0,
                    "drop_pct_vs_baseline": 0.0,
                })
                continue

            count = _member_count(client, entity)
            drop_pct = _drop_pct_vs_baseline(count, baseline)
            score = _stability_score(count, baseline)
            row = {
                "group_id": gid,
                "label": label,
                "title": getattr(entity, "title", None) or label,
                "member_count": count,
                "baseline": baseline,
                "stability_score": score,
                "drop_pct_vs_baseline": drop_pct,
            }
            group_results.append(row)

            if count < baseline and drop_pct > alert_threshold_pct:
                alerts.append({
                    "label": row["title"],
                    "group_id": gid,
                    "current": count,
                    "baseline": baseline,
                    "drop_pct": drop_pct,
                })

        member_ids_by_group: dict[str, set[int]] = {}
        if invite_defs:
            for r in group_results:
                gid = str(r.get("group_id") or "")
                if not gid or r.get("member_count") is None:
                    continue
                try:
                    ent = client.get_entity(gid)
                except Exception:
                    member_ids_by_group[gid] = set()
                    continue
                ids: set[int] = set()
                try:
                    for p in client.iter_participants(ent):
                        ids.add(int(p.id))
                except Exception as exc:
                    log.warning("retention_iter_participants_failed", group=gid, error=str(exc))
                member_ids_by_group[gid] = ids

        for inv in invite_defs:
            gid = inv["group_id"]
            link = inv["invite_hash"]
            try:
                peer = client.get_input_entity(gid)
            except Exception as exc:
                log.warning("retention_invite_peer_failed", group=gid, error=str(exc))
                continue

            offset_user: Any = InputUserEmpty()
            offset_date: datetime | None = None
            user_by_id: dict[int, Any] = {}

            while True:
                try:
                    chunk = client(
                        GetChatInviteImportersRequest(
                            peer=peer,
                            offset_date=offset_date,
                            offset_user=offset_user,
                            limit=100,
                            link=link,
                        )
                    )
                except Exception as exc:
                    log.warning(
                        "retention_get_invite_importers_failed",
                        group=gid,
                        link=link[:8],
                        error=str(exc),
                    )
                    break

                importers = getattr(chunk, "importers", None) or []
                users = getattr(chunk, "users", None) or []
                for u in users:
                    if hasattr(u, "id"):
                        user_by_id[int(u.id)] = u

                if not importers:
                    break

                id_set = member_ids_by_group.get(gid, set())
                known_count = next(
                    (
                        int(x["member_count"])
                        for x in group_results
                        if str(x.get("group_id")) == gid and x.get("member_count") is not None
                    ),
                    None,
                )

                for imp in importers:
                    uid = int(getattr(imp, "user_id", 0))
                    dt = getattr(imp, "date", None)
                    join_iso = (
                        dt.replace(tzinfo=timezone.utc).isoformat()
                        if isinstance(dt, datetime)
                        else None
                    )
                    if known_count is None:
                        still: bool | None = None
                    elif not id_set and known_count > 0:
                        still = None
                    else:
                        still = uid in id_set
                    invite_tracking.append({
                        "group_id": gid,
                        "invite_hash": link,
                        "user_id": uid,
                        "join_date": join_iso,
                        "still_member": still,
                    })

                last = importers[-1]
                lud = getattr(last, "date", None)
                if not isinstance(lud, datetime) or len(importers) < 100:
                    break
                offset_date = lud
                u_obj = user_by_id.get(int(getattr(last, "user_id", 0)))
                if u_obj is None or not hasattr(u_obj, "access_hash"):
                    break
                offset_user = InputUser(
                    user_id=int(u_obj.id),
                    access_hash=int(u_obj.access_hash),
                )

    finally:
        client.disconnect()

    return {
        "groups": group_results,
        "invite_tracking": invite_tracking,
        "alerts": alerts,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


async def _send_alerts(secrets: dict[str, str], alerts: list[dict[str, Any]]) -> None:
    if not alerts:
        return
    token = (
        secrets.get("TELEGRAM_BOT_TOKEN", "").strip()
        or os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    )
    chat = (
        secrets.get("TELEGRAM_ADMIN_CHAT_ID", "").strip()
        or os.getenv("TELEGRAM_ADMIN_CHAT_ID", "").strip()
    )
    if not token or not chat:
        log.warning("retention_alert_skipped_no_telegram_config")
        return

    tg = TelegramProvider(bot_token=token, admin_chat_id=chat)
    for a in alerts:
        body = (
            f"Group: {a['label']} ({a['group_id']}). "
            f"Members {a['current']} vs baseline {a['baseline']}. "
            f"Lost {a['drop_pct']:.2f}% vs baseline."
        )
        await tg.send(
            Alert(
                title="ALERT: Member Drop Detected",
                body=body,
                level=AlertLevel.CRITICAL,
                metadata={
                    "drop_pct": f"{a['drop_pct']:.2f}%",
                    "current": a["current"],
                    "baseline": a["baseline"],
                },
            )
        )


@registry.register("retention.guardian.monitor")
async def retention_guardian_monitor(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Check configured groups, persist Redis snapshot, alert on >threshold drop
    from baseline, and merge invite-link importer rows with live membership.
    """
    redis = parameters.get("__redis__")
    secrets: dict[str, str] = dict(parameters.get("__secrets__") or {})

    groups = _parse_groups()
    if not groups:
        log.info("retention_guardian_skip_no_groups")
        payload = {
            "ok": True,
            "skipped": True,
            "reason": "RETENTION_GROUPS_JSON empty",
            "groups": [],
            "invite_tracking": [],
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        if redis is not None:
            await redis.set(
                RETENTION_HEALTH_SNAPSHOT_KEY,
                json.dumps(payload, ensure_ascii=False),
                ex=RETENTION_HEALTH_TTL_S,
            )
        return payload

    api_id_s = secrets.get("TELEFIX_API_ID") or os.getenv("TELEFIX_API_ID", "")
    api_hash = secrets.get("TELEFIX_API_HASH") or os.getenv("TELEFIX_API_HASH", "")
    if not api_id_s or not api_hash:
        log.error("retention_guardian_missing_api_credentials")
        return {"ok": False, "error": "Missing TELEFIX_API_ID / TELEFIX_API_HASH"}

    try:
        api_id = int(api_id_s)
    except ValueError:
        return {"ok": False, "error": "Invalid TELEFIX_API_ID"}

    session_file = _session_path()
    baseline = _baseline()
    threshold = _alert_drop_pct()
    invites = _parse_invite_links()

    try:
        result = await asyncio.to_thread(
            _sync_run,
            api_id,
            api_hash,
            session_file,
            groups,
            baseline,
            threshold,
            invites,
        )
    except Exception as exc:
        log.exception("retention_guardian_failed", error=str(exc))
        return {"ok": False, "error": str(exc)}

    alerts = list(result.get("alerts") or [])
    out = {
        "ok": True,
        "skipped": False,
        "groups": result.get("groups", []),
        "invite_tracking": result.get("invite_tracking", []),
        "checked_at": result.get("checked_at"),
        "alert_count": len(alerts),
    }

    if redis is not None:
        await redis.set(
            RETENTION_HEALTH_SNAPSHOT_KEY,
            json.dumps(out, ensure_ascii=False),
            ex=RETENTION_HEALTH_TTL_S,
        )

    await _send_alerts(secrets, alerts)
    log.info(
        "retention_guardian_complete",
        groups=len(out["groups"]),
        invites_tracked=len(out["invite_tracking"]),
        alerts=len(alerts),
    )
    return out
