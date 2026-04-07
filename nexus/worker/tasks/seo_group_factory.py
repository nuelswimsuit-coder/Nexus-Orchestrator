"""
seo_group_factory — RankSEO Group Factory entrypoint.

Starts the Community Factory pipeline (Telethon group creation + join + chat)
by running ``swarm.community_factory.bootstrap`` in-process, then refreshes
``nexus:factory:seo:*`` snapshots for the control API.
"""

from __future__ import annotations

from typing import Any

import structlog

from nexus.shared.seo_group_factory import persist_seo_factory_snapshot
from nexus.worker.task_registry import registry

log = structlog.get_logger(__name__)


@registry.register("seo_group_factory")
async def seo_group_factory(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Bootstrap community factory from parameters and update SEO Redis snapshots.

    Typical parameters mirror Community Factory bootstrap:
    ``sessions_dir``, ``phases``, ``dry_run``, ``reset``,
    ``max_joins_per_tick``, ``converse_chain_limit``.
    """
    redis = parameters.get("__redis__")

    carry: dict[str, Any] = {
        "sessions_dir": str(parameters.get("sessions_dir", "") or ""),
        "phases": list(parameters.get("phases") or ["allocate", "create", "join", "chat"]),
        "dry_run": bool(parameters.get("dry_run", False)),
        "reset": bool(parameters.get("reset", False)),
        "max_joins_per_tick": int(parameters.get("max_joins_per_tick", 1) or 1),
        "converse_chain_limit": int(parameters.get("converse_chain_limit", 5000) or 5000),
        "__redis__": redis,
    }
    if "__secrets__" in parameters:
        carry["__secrets__"] = parameters["__secrets__"]

    # Deferred import: swarm is already loaded by the worker listener before this module.
    from nexus.worker.tasks.swarm import community_factory_bootstrap

    try:
        bootstrap_out = await community_factory_bootstrap(carry)
    except Exception as exc:
        log.exception("seo_group_factory_bootstrap_failed", error=str(exc))
        return {"status": "failed", "error": str(exc)}

    if redis:
        try:
            await persist_seo_factory_snapshot(redis)
        except Exception as exc:
            log.warning("seo_group_factory_snapshot_failed", error=str(exc))

    return {
        "status": "completed",
        "message": "RankSEO group factory pipeline started; community factory bootstrap finished.",
        "bootstrap": bootstrap_out,
    }
