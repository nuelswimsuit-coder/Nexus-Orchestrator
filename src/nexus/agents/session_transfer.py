"""
Worker ↔ Master Telethon session vault (StringSession over HTTPS).

Vault-managed task types receive ``__vault_string_sessions__`` in parameters
(populated by :mod:`nexus.agents.listener` before :func:`run_task` runs).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
import structlog

from nexus.services.session_vault import INDEX_KEY
from nexus.shared.config import settings

log = structlog.get_logger(__name__)

VAULT_TASK_TYPES = frozenset({"account_mapper.map", "telegram.run_warmup"})


def get_session_vault_api_base() -> str:
    """Base URL for ``/api/sessions/vault/*`` (Master Command Hub, not worker disk)."""
    hub = (settings.nexus_master_hub_url or "").strip().rstrip("/")
    if hub:
        return hub
    return (settings.nexus_api_base_url or "").strip().rstrip("/")


@dataclass
class VaultAttachResult:
    """Outcome of attempting to lease StringSessions for a task."""

    param_patch: dict[str, Any]
    release_stems: list[str] | None = None
    block_error: str | None = None


async def attach_vault_sessions_to_task(
    task_type: str,
    parameters: dict[str, Any],
    *,
    worker_id: str,
    task_id: str,
    redis: Any,
) -> VaultAttachResult:
    """
    For vault-managed handlers, populate ``__vault_string_sessions__`` via the
    master API. Returns stems that must be released in a ``finally`` block.

    Set ``use_local_staged_sessions: true`` in task parameters to skip the API
    and read ``data/session_vault`` / legacy staged dirs on the worker (legacy).
    """
    if task_type not in VAULT_TASK_TYPES:
        return VaultAttachResult(param_patch={})

    if parameters.get("use_local_staged_sessions"):
        return VaultAttachResult(param_patch={})

    secret = (settings.nexus_session_vault_secret or "").strip()
    api_base = get_session_vault_api_base()
    if not secret:
        return VaultAttachResult(
            param_patch={},
            block_error=(
                "NEXUS_SESSION_VAULT_SECRET is not set on the worker — "
                "cannot lease sessions from the master API"
            ),
        )

    stems = sorted(str(s) for s in (await redis.smembers(INDEX_KEY)) or [])
    if task_type == "telegram.run_warmup":
        max_n = int(parameters.get("max_sessions", 10))
        max_n = max(1, max_n)
        stems = stems[:max_n]

    if not stems:
        return VaultAttachResult(
            param_patch={"__vault_string_sessions__": []},
            release_stems=None,
        )

    url = f"{api_base}/api/sessions/vault/lease-batch"
    payload = {
        "session_stems": stems,
        "worker_id": worker_id,
        "task_id": task_id,
    }
    headers = {"X-Nexus-Session-Secret": secret}

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(url, json=payload, headers=headers)
    except Exception as exc:
        log.error("session_vault_lease_http_failed", error=str(exc))
        return VaultAttachResult(
            param_patch={},
            block_error=f"Session vault lease request failed: {exc}",
        )

    if resp.status_code != 200:
        return VaultAttachResult(
            param_patch={},
            block_error=f"Session vault lease HTTP {resp.status_code}: {resp.text[:500]}",
        )

    try:
        data = resp.json()
    except Exception as exc:
        return VaultAttachResult(
            param_patch={},
            block_error=f"Session vault lease invalid JSON: {exc}",
        )

    if not data.get("ok"):
        conflicts = data.get("conflicts") or []
        return VaultAttachResult(
            param_patch={},
            block_error=f"Session vault lease denied: {conflicts}",
        )

    sessions = data.get("sessions") or []
    release_stems = [str(s.get("session_stem")) for s in sessions if s.get("session_stem")]
    return VaultAttachResult(
        param_patch={"__vault_string_sessions__": sessions},
        release_stems=release_stems,
    )


async def release_vault_leases(stems: list[str] | None, worker_id: str) -> None:
    if not stems:
        return
    secret = (settings.nexus_session_vault_secret or "").strip()
    api_base = get_session_vault_api_base()
    if not secret:
        return
    url = f"{api_base}/api/sessions/vault/release-batch"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            await client.post(
                url,
                json={"session_stems": stems, "worker_id": worker_id},
                headers={"X-Nexus-Session-Secret": secret},
            )
    except Exception as exc:
        log.warning("session_vault_release_failed", error=str(exc))
