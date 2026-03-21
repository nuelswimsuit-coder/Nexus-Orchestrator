"""
Worker Sentinel Dashboard (terminal live monitor).

Features
--------
- Rich live dashboard with four required columns:
  [Node Health | Current Task | AI Thinking | Action History]
- Reads local node health from API cluster status endpoint.
- Reads intent / vision / history / task state from Master's Redis.
- Auto-reconnects to API and Redis if either side drops.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import time
from dataclasses import dataclass
from typing import Any

import httpx
from redis import Redis
from redis.exceptions import RedisError
from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from nexus.shared.config import settings

MISSION_STATEMENT = "Telefix OS - Distributed Intelligence v2.0"
DEFAULT_INTENT = "Standing by for intent stream from worker tasks"
DEFAULT_VISION = "Forecasting next 5 minutes of execution focus"
DEFAULT_HISTORY = "No action history yet"

STATUS_KEYS: list[tuple[str, str]] = [
    ("nexus:openclaw:status", "OpenClaw"),
    ("nexus:content:status", "Content Factory"),
    ("nexus:super_scraper:status", "Super Scraper"),
    ("nexus:scrape:status", "Auto Scrape"),
    ("nexus:add:status", "Auto Add"),
    ("nexus:sentinel:status", "Sentinel"),
]


@dataclass
class RuntimeState:
    node_id: str
    node_name: str
    api_base_url: str
    redis_url: str
    refresh_s: float
    timeout_s: float
    redis_client: Redis | None = None
    api_client: httpx.Client | None = None
    api_failures: int = 0
    redis_failures: int = 0
    last_api_error: str = ""
    last_redis_error: str = ""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Worker Sentinel Dashboard (Rich terminal monitor)"
    )
    parser.add_argument(
        "--node-id",
        default=os.getenv("NODE_ID") or settings.node_id or socket.gethostname(),
        help="Node ID used for Redis + API lookup",
    )
    parser.add_argument(
        "--node-name",
        default=os.getenv("NODE_NAME") or os.getenv("NODE_ID") or socket.gethostname(),
        help="Display name in dashboard header",
    )
    parser.add_argument(
        "--api-base-url",
        default=os.getenv("NEXUS_API_BASE_URL", "http://127.0.0.1:8001"),
        help="Base URL of local API (example: http://127.0.0.1:8001)",
    )
    parser.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", settings.redis_url),
        help="Master Redis URL",
    )
    parser.add_argument("--refresh", type=float, default=1.0, help="Refresh interval in seconds")
    parser.add_argument("--timeout", type=float, default=3.0, help="API/Redis timeout in seconds")
    return parser.parse_args()


def _ensure_api(state: RuntimeState) -> httpx.Client | None:
    if state.api_client is not None:
        return state.api_client
    try:
        state.api_client = httpx.Client(
            base_url=state.api_base_url.rstrip("/"),
            timeout=state.timeout_s,
        )
        return state.api_client
    except Exception as exc:
        state.api_failures += 1
        state.last_api_error = str(exc)
        return None


def _ensure_redis(state: RuntimeState) -> Redis | None:
    if state.redis_client is not None:
        return state.redis_client
    try:
        client = Redis.from_url(state.redis_url, decode_responses=True)
        client.ping()
        state.redis_client = client
        return state.redis_client
    except Exception as exc:
        state.redis_failures += 1
        state.last_redis_error = str(exc)
        return None


def _drop_api(state: RuntimeState, exc: Exception) -> None:
    state.api_failures += 1
    state.last_api_error = str(exc)
    if state.api_client is not None:
        try:
            state.api_client.close()
        except Exception:
            pass
    state.api_client = None


def _drop_redis(state: RuntimeState, exc: Exception) -> None:
    state.redis_failures += 1
    state.last_redis_error = str(exc)
    if state.redis_client is not None:
        try:
            state.redis_client.close()
        except Exception:
            pass
    state.redis_client = None


def _format_task_status(raw: str) -> str:
    try:
        payload = json.loads(raw)
        status = str(payload.get("status", "unknown"))
        detail = str(payload.get("detail", "")).strip()
        if detail:
            return f"{status}: {detail[:120]}"
        return status
    except Exception:
        return raw[:120]


def _read_first_key(redis_client: Redis, keys: list[str], default: str) -> str:
    for key in keys:
        val = redis_client.get(key)
        if val:
            return str(val)
    return default


def _read_current_task(redis_client: Redis) -> str:
    for key, label in STATUS_KEYS:
        raw = redis_client.get(key)
        if not raw:
            continue
        view = _format_task_status(raw)
        if any(term in view.lower() for term in ("running", "awaiting", "active", "paused")):
            return f"{label} | {view}"
    return "Idle - waiting for next dispatch cycle"


def _read_action_history(redis_client: Redis, node_id: str) -> list[str]:
    for key in [f"node:{node_id}:history", "node:history"]:
        entries = redis_client.lrange(key, 0, 4)
        if entries:
            return [str(entry) for entry in entries]
    return [DEFAULT_HISTORY]


def _fetch_api_node_health(state: RuntimeState) -> dict[str, Any]:
    api = _ensure_api(state)
    if api is None:
        return {"online": False, "reason": f"API offline: {state.last_api_error or 'unreachable'}"}

    try:
        response = api.get("/api/cluster/status")
        response.raise_for_status()
        payload = response.json()
        nodes = payload.get("nodes", [])
        selected = None
        for node in nodes:
            if node.get("node_id") == state.node_id:
                selected = node
                break
        if selected is None and nodes:
            selected = nodes[0]
        if selected is None:
            return {"online": False, "reason": "API online, but no heartbeat nodes found"}
        return {
            "online": True,
            "node_id": selected.get("node_id", state.node_id),
            "cpu_percent": float(selected.get("cpu_percent", 0.0)),
            "ram_used_mb": float(selected.get("ram_used_mb", 0.0)),
            "active_jobs": int(selected.get("active_jobs", 0)),
            "active_tasks_count": int(selected.get("active_tasks_count", 0)),
            "os_info": selected.get("os_info", "unknown"),
            "local_ip": selected.get("local_ip", "unknown"),
            "online_flag": bool(selected.get("online", True)),
        }
    except Exception as exc:
        _drop_api(state, exc)
        return {"online": False, "reason": f"API dropped, reconnecting: {state.last_api_error}"}


def _collect_snapshot(state: RuntimeState) -> dict[str, Any]:
    node_health = _fetch_api_node_health(state)

    intent = DEFAULT_INTENT
    vision = DEFAULT_VISION
    current_task = "Unknown"
    history = [DEFAULT_HISTORY]
    redis_ok = False

    redis_client = _ensure_redis(state)
    if redis_client is not None:
        try:
            intent = _read_first_key(
                redis_client,
                [f"node:{state.node_id}:intent", "node:intent"],
                DEFAULT_INTENT,
            )
            vision = _read_first_key(
                redis_client,
                [f"node:{state.node_id}:vision", "node:vision"],
                DEFAULT_VISION,
            )
            current_task = _read_current_task(redis_client)
            history = _read_action_history(redis_client, state.node_id)
            redis_ok = True
            state.last_redis_error = ""
        except (RedisError, OSError) as exc:
            _drop_redis(state, exc)
    return {
        "node_health": node_health,
        "intent": intent,
        "vision": vision,
        "current_task": current_task,
        "history": history,
        "redis_ok": redis_ok,
    }


def _render_dashboard(state: RuntimeState, snapshot: dict[str, Any]) -> Group:
    health = snapshot["node_health"]

    if health.get("online"):
        node_health_text = (
            f"Node: {health.get('node_id')} ({state.node_name})\n"
            f"OS: {health.get('os_info')} | IP: {health.get('local_ip')}\n"
            f"CPU: {health.get('cpu_percent', 0.0):.1f}% | RAM: {health.get('ram_used_mb', 0.0):.0f} MB\n"
            f"Jobs: {health.get('active_jobs', 0)} | Active Tasks: {health.get('active_tasks_count', 0)}\n"
            f"API: Online | Redis: {'Online' if snapshot['redis_ok'] else 'Reconnecting'}"
        )
    else:
        node_health_text = (
            f"Node: {state.node_id} ({state.node_name})\n"
            f"{health.get('reason', 'API offline')}\n"
            f"API reconnect attempts: {state.api_failures}\n"
            f"Redis: {'Online' if snapshot['redis_ok'] else 'Reconnecting'} "
            f"(attempts: {state.redis_failures})"
        )

    ai_thinking_text = f"Intent:\n{snapshot['intent']}\n\nVision (next 5m):\n{snapshot['vision']}"
    history_text = "\n".join(f"- {line}" for line in snapshot["history"][:5])

    table = Table(expand=True, show_lines=True)
    table.add_column("Node Health", ratio=3)
    table.add_column("Current Task", ratio=2)
    table.add_column("AI Thinking", ratio=4)
    table.add_column("Action History", ratio=4)
    table.add_row(
        node_health_text,
        snapshot["current_task"],
        ai_thinking_text,
        history_text,
    )

    mission = Panel(
        Text(MISSION_STATEMENT, style="bold cyan", justify="center"),
        border_style="cyan",
    )
    body = Panel(table, border_style="bright_blue")

    footer = Text(
        f"refresh={state.refresh_s:.2f}s | api_failures={state.api_failures} | redis_failures={state.redis_failures}",
        style="dim",
        justify="right",
    )
    return Group(mission, body, footer)


def main() -> None:
    args = _parse_args()
    state = RuntimeState(
        node_id=args.node_id,
        node_name=args.node_name,
        api_base_url=args.api_base_url,
        redis_url=args.redis_url,
        refresh_s=max(0.25, float(args.refresh)),
        timeout_s=max(1.0, float(args.timeout)),
    )

    try:
        with Live(screen=False, refresh_per_second=max(1, int(1 / state.refresh_s))) as live:
            while True:
                snapshot = _collect_snapshot(state)
                live.update(_render_dashboard(state, snapshot))
                time.sleep(state.refresh_s)
    except KeyboardInterrupt:
        pass
    finally:
        if state.api_client is not None:
            try:
                state.api_client.close()
            except Exception:
                pass
        if state.redis_client is not None:
            try:
                state.redis_client.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
