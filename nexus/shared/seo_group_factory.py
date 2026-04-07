"""
RankSEO Group Factory — Redis snapshot for UI (report rows + progress).

Reads live Community Factory keys (``nexus:swarm:factory:*``) and materializes:

- ``nexus:factory:seo:report`` — JSON array of
  ``{group_name, invite_link, owner}``
- ``nexus:factory:seo:status`` — JSON
  ``{phase, total_links_created, raw_phase}``

Called from the ``seo_group_factory`` worker after bootstrap and from API
GET handlers so polling stays aligned with swarm state.
"""

from __future__ import annotations

import json
from typing import Any

# Materialized for the RankSEO UI
SEO_FACTORY_REPORT_KEY = "nexus:factory:seo:report"
SEO_FACTORY_STATUS_KEY = "nexus:factory:seo:status"

# Must match nexus.worker.tasks.swarm community factory keys
SWARM_FACTORY_GROUPS_KEY = "nexus:swarm:factory:groups"
SWARM_FACTORY_STATE_KEY = "nexus:swarm:factory:state"

# Rank-SEO Telethon factory (nexus.worker.tasks.seo_group_factory)
SEO_TELETHON_LINKS_KEY = "nexus:seo_factory:generated_links"
SEO_TELETHON_STATE_KEY = "nexus:seo_factory:state"


def _json_loads(raw: str | bytes | None) -> Any:
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if not str(raw).strip():
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _derive_ui_phase(state: Any) -> tuple[str, str]:
    """
    Map internal factory phase to short UI strings.

    Returns (ui_phase, raw_phase).
    """
    if not isinstance(state, dict):
        return ("Idle", "")
    raw = str(state.get("phase") or "").strip().lower()
    if raw in ("allocating", "creating", ""):
        return ("Creating groups...", raw or "unknown")
    if raw == "joining":
        return ("Mass Joining...", raw)
    if raw == "chatting":
        return ("Warming up...", raw)
    if raw == "complete":
        return ("Done", raw)
    return ("Creating groups...", raw or "unknown")


def groups_to_report_rows(groups: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if not isinstance(groups, list):
        return out
    for g in groups:
        if not isinstance(g, dict):
            continue
        name = str(g.get("title") or g.get("group_name") or "").strip()
        link = str(g.get("invite_link") or "").strip()
        owner = str(g.get("owner_session") or g.get("owner") or "").strip()
        out.append(
            {
                "group_name": name,
                "invite_link": link,
                "owner": owner,
            }
        )
    return out


def _seo_telethon_phase_ui(seo_state: Any) -> tuple[str, str] | None:
    if not isinstance(seo_state, dict):
        return None
    raw = str(seo_state.get("phase") or "").strip().lower()
    if raw in ("", "idle"):
        return None
    if raw == "creating":
        return ("Creating groups...", raw)
    if raw == "joining":
        return ("Mass Joining...", raw)
    if raw == "complete":
        return ("Done", raw)
    return ("Creating groups...", raw)


async def persist_seo_factory_snapshot(redis: Any) -> dict[str, Any]:
    """
    Refresh SEO report + status keys from Community Factory and/or Telethon SEO factory Redis state.

    Returns ``{"report": [...], "status": {...}}``.
    """
    groups_raw = await redis.get(SWARM_FACTORY_GROUPS_KEY)
    state_raw = await redis.get(SWARM_FACTORY_STATE_KEY)
    groups = _json_loads(groups_raw)
    state = _json_loads(state_raw)

    report = groups_to_report_rows(groups)
    seen_links = {str(r.get("invite_link") or "") for r in report if r.get("invite_link")}

    seo_state_raw = await redis.get(SEO_TELETHON_STATE_KEY)
    seo_state = _json_loads(seo_state_raw)
    raw_list = await redis.lrange(SEO_TELETHON_LINKS_KEY, 0, -1)
    for raw in raw_list or []:
        try:
            row = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        link = str(row.get("invite_link") or "").strip()
        if link and link in seen_links:
            continue
        if link:
            seen_links.add(link)
        report.append(
            {
                "group_name": str(row.get("group_name") or "").strip(),
                "invite_link": link,
                "owner": str(row.get("owner_session") or row.get("owner_id") or "").strip(),
            }
        )

    ui_phase, raw_phase = _derive_ui_phase(state)
    tele_ui = _seo_telethon_phase_ui(seo_state)
    if tele_ui is not None:
        ui_phase, raw_phase = tele_ui

    with_links = sum(1 for r in report if r.get("invite_link"))

    status_obj: dict[str, Any] = {
        "phase": ui_phase,
        "raw_phase": raw_phase,
        "total_links_created": with_links,
    }

    await redis.set(SEO_FACTORY_REPORT_KEY, json.dumps(report, ensure_ascii=False))
    await redis.set(SEO_FACTORY_STATUS_KEY, json.dumps(status_obj, ensure_ascii=False))

    return {"report": report, "status": status_obj}


async def load_seo_factory_report_only(redis: Any) -> list[dict[str, str]]:
    """Read materialized report key without recomputing (may be stale)."""
    raw = await redis.get(SEO_FACTORY_REPORT_KEY)
    data = _json_loads(raw)
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


async def load_seo_factory_status_only(redis: Any) -> dict[str, Any]:
    raw = await redis.get(SEO_FACTORY_STATUS_KEY)
    data = _json_loads(raw)
    if isinstance(data, dict):
        return data
    return {
        "phase": "Idle",
        "raw_phase": "",
        "total_links_created": 0,
    }
