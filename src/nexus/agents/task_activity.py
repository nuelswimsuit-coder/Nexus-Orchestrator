"""
Per-worker \"current task\" snapshot in Redis for Command Center / cluster UI.

Written by :mod:`nexus.agents.listener` around each ``execute_task`` invocation.
Keys expire quickly so crashed workers do not show stale work forever.
"""

from __future__ import annotations

import json
import socket
from typing import Any
from urllib.parse import urlparse

ACTIVITY_KEY_PREFIX = "nexus:worker:activity:"
ACTIVITY_TTL_S = 300

_WORKER_HOSTNAME = socket.gethostname()


def worker_activity_key(worker_id: str) -> str:
    return f"{ACTIVITY_KEY_PREFIX}{worker_id}"


def worker_hostname() -> str:
    return _WORKER_HOSTNAME


def _scrape_host(parameters: dict[str, Any]) -> str | None:
    sources = parameters.get("sources") or parameters.get("links") or []
    if not isinstance(sources, list):
        return None
    for raw in sources:
        s = str(raw).strip()
        if not s:
            continue
        parsed = urlparse(s if "://" in s else f"https://{s}")
        host = (parsed.netloc or "").strip() or (parsed.path.split("/")[0] if parsed.path else "")
        if host:
            return host[:48]
    return None


def describe_task(task_payload: dict[str, Any]) -> str:
    """Human-readable label for operator dashboards (EN + short HE where useful)."""
    tt = str(task_payload.get("task_type") or "").strip()
    params = task_payload.get("parameters") or {}
    if not isinstance(params, dict):
        params = {}

    if tt == "nexus.llm.gemini_terminal":
        mode = str(params.get("analysis_mode") or "chat").strip().lower()
        if mode == "personality":
            return "ניתוח אופי"
        return "AI terminal"

    if tt == "telegram.auto_scrape":
        host = _scrape_host(params)
        return f"Scraping {host}" if host else "Scraping Telegram"

    if tt == "telegram.super_scrape":
        return "Super scrape"

    if tt.startswith("scraper.openclaw") or tt == "openclaw.browser_scrape":
        return "OpenClaw scrape"

    if tt == "nexus.project.deploy":
        name = str(params.get("project_name") or params.get("project_id") or "").strip()
        return f"Deploying {name}" if name else "Deploying project"

    if tt.startswith("swarm.") or "warmer" in tt:
        return "Group warmer"

    if tt.startswith("telegram."):
        return tt.replace("telegram.", "Telegram ")[:40]

    short = tt.split(".")[-1] if "." in tt else tt
    return short.replace("_", " ")[:44] or "Task"


async def set_worker_activity(
    redis: Any,
    *,
    worker_id: str,
    task_id: str,
    label: str,
) -> None:
    payload = {
        "hostname": worker_hostname(),
        "label": label,
        "job_id": task_id,
    }
    await redis.set(
        worker_activity_key(worker_id),
        json.dumps(payload, ensure_ascii=False),
        ex=ACTIVITY_TTL_S,
    )


async def clear_worker_activity_if_matches(redis: Any, *, worker_id: str, task_id: str) -> None:
    key = worker_activity_key(worker_id)
    raw = await redis.get(key)
    if raw is None:
        return
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()
    try:
        cur = json.loads(raw)
    except Exception:
        await redis.delete(key)
        return
    if str(cur.get("job_id") or "") != str(task_id):
        return
    idle = {
        "hostname": cur.get("hostname") or worker_hostname(),
        "label": "Idle",
        "job_id": None,
    }
    await redis.set(key, json.dumps(idle, ensure_ascii=False), ex=ACTIVITY_TTL_S)
