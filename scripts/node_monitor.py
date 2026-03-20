"""
High-end terminal node monitor for distributed Nexus workers.

Features
--------
- Rich cyberpunk dashboard with live hardware + AI context panels
- Redis-backed dynamic intent/vision strings
- Rolling node history (last 5 successful tasks)
- Sleep-prevention guard while monitor is active
"""

from __future__ import annotations

import argparse
import ctypes
import os
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any

import psutil
from redis import Redis
from redis.exceptions import RedisError
from rich.align import Align
from rich.console import Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

NEON_BLUE = "#00D7FF"
WARNING_YELLOW = "#FFD84D"
SUCCESS_GREEN = "#46FF8B"

DEFAULT_INTENT = "Analyzing Telegram API limits to avoid shadowban"
DEFAULT_VISION = "Acquiring 10,000 high-intent leads for NUEL project"


@dataclass
class RuntimeState:
    node_name: str
    node_id: str
    redis: Redis
    previous_net_sent: int
    previous_net_recv: int
    previous_net_ts: float


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Nexus distributed node terminal monitor")
    parser.add_argument(
        "--node-name",
        default=os.getenv("NODE_NAME") or os.getenv("NODE_ID") or socket.gethostname(),
        help="Display name in dashboard header",
    )
    parser.add_argument(
        "--node-id",
        default=os.getenv("NODE_ID") or socket.gethostname(),
        help="Node ID used for Redis keys: node:<node_id>:*",
    )
    parser.add_argument(
        "--redis-host",
        default="127.0.0.1",
        help="Redis host (use 127.0.0.1 on master or pass worker target)",
    )
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")
    parser.add_argument("--redis-db", type=int, default=0, help="Redis database index")
    parser.add_argument(
        "--redis-url",
        default="",
        help="Optional full Redis URL (overrides host/port/db)",
    )
    parser.add_argument("--refresh", type=float, default=1.0, help="Dashboard refresh in seconds")
    return parser.parse_args()


def _ensure_stay_awake_linux() -> None:
    """
    Re-exec this script under systemd-inhibit to block sleep while active.
    """
    if os.getenv("NEXUS_MONITOR_INHIBITED") == "1":
        return
    inhibit = shutil.which("systemd-inhibit")
    if not inhibit:
        return
    env = os.environ.copy()
    env["NEXUS_MONITOR_INHIBITED"] = "1"
    cmd = [
        inhibit,
        "--what=sleep",
        "--why=Nexus node monitor is active",
        "--mode=block",
        sys.executable,
        *sys.argv,
    ]
    completed = subprocess.run(cmd, env=env, check=False)
    raise SystemExit(completed.returncode)


def _stay_awake_tick_windows() -> None:
    """
    Signal Windows power manager to keep the system awake.
    """
    es_continuous = 0x80000000
    es_system_required = 0x00000001
    es_awaymode_required = 0x00000040
    ctypes.windll.kernel32.SetThreadExecutionState(
        es_continuous | es_system_required | es_awaymode_required
    )


def _stay_awake_release_windows() -> None:
    es_continuous = 0x80000000
    ctypes.windll.kernel32.SetThreadExecutionState(es_continuous)


def _build_redis_client(args: argparse.Namespace) -> Redis:
    if args.redis_url:
        return Redis.from_url(args.redis_url, decode_responses=True)
    return Redis(
        host=args.redis_host,
        port=args.redis_port,
        db=args.redis_db,
        decode_responses=True,
    )


def _get_first_key(redis_client: Redis, keys: list[str], default: str) -> str:
    for key in keys:
        try:
            value = redis_client.get(key)
            if value:
                return str(value)
        except RedisError:
            continue
    return default


def _get_history(redis_client: Redis, node_id: str) -> list[str]:
    for key in [f"node:{node_id}:history", "node:history"]:
        try:
            entries = redis_client.lrange(key, 0, 4)
            if entries:
                return [str(entry) for entry in entries]
        except RedisError:
            continue
    return ["No successful tasks recorded yet"]


def _format_speed(num_bytes_per_s: float) -> str:
    if num_bytes_per_s >= 1024 ** 2:
        return f"{num_bytes_per_s / (1024 ** 2):.2f} MB/s"
    if num_bytes_per_s >= 1024:
        return f"{num_bytes_per_s / 1024:.2f} KB/s"
    return f"{num_bytes_per_s:.0f} B/s"


def _collect_snapshot(state: RuntimeState) -> dict[str, Any]:
    cpu = psutil.cpu_percent(interval=None)
    ram = psutil.virtual_memory().percent

    now = time.time()
    counters = psutil.net_io_counters()
    elapsed = max(now - state.previous_net_ts, 0.001)

    upload_bps = (counters.bytes_sent - state.previous_net_sent) / elapsed
    download_bps = (counters.bytes_recv - state.previous_net_recv) / elapsed

    state.previous_net_sent = counters.bytes_sent
    state.previous_net_recv = counters.bytes_recv
    state.previous_net_ts = now

    intent = _get_first_key(
        state.redis,
        [f"node:{state.node_id}:intent", "node:intent"],
        DEFAULT_INTENT,
    )
    vision = _get_first_key(
        state.redis,
        [f"node:{state.node_id}:vision", "node:vision"],
        DEFAULT_VISION,
    )
    history = _get_history(state.redis, state.node_id)

    return {
        "cpu": cpu,
        "ram": ram,
        "upload_bps": upload_bps,
        "download_bps": download_bps,
        "intent": intent,
        "vision": vision,
        "history": history,
    }


def _hardware_panel(snapshot: dict[str, Any]) -> Panel:
    table = Table.grid(expand=True)
    table.add_column(justify="left")
    table.add_column(justify="right")

    cpu_color = WARNING_YELLOW if snapshot["cpu"] >= 95 else SUCCESS_GREEN
    ram_color = WARNING_YELLOW if snapshot["ram"] >= 90 else SUCCESS_GREEN

    table.add_row(
        "CPU Utilization (Target 95%)",
        f"[{cpu_color}]{snapshot['cpu']:.1f}%[/{cpu_color}]",
    )
    table.add_row(
        "RAM Usage",
        f"[{ram_color}]{snapshot['ram']:.1f}%[/{ram_color}]",
    )
    table.add_row(
        "Network Download",
        f"[{SUCCESS_GREEN}]{_format_speed(snapshot['download_bps'])}[/{SUCCESS_GREEN}]",
    )
    table.add_row(
        "Network Upload",
        f"[{SUCCESS_GREEN}]{_format_speed(snapshot['upload_bps'])}[/{SUCCESS_GREEN}]",
    )
    return Panel(table, title="[bold]Hardware[/bold]", border_style=NEON_BLUE)


def _intent_panel(snapshot: dict[str, Any]) -> Panel:
    return Panel(
        Text(snapshot["intent"], style="bold white"),
        title="[bold]AI Thinking / Current Intent[/bold]",
        border_style=NEON_BLUE,
    )


def _vision_panel(snapshot: dict[str, Any]) -> Panel:
    return Panel(
        Text(snapshot["vision"], style="bold white"),
        title="[bold]Strategic Vision / Long-term Goal[/bold]",
        border_style=NEON_BLUE,
    )


def _history_panel(snapshot: dict[str, Any]) -> Panel:
    rows: list[Text] = []
    for item in snapshot["history"][:5]:
        rows.append(Text(f"+ {item}", style=SUCCESS_GREEN))
    return Panel(
        Group(*rows),
        title="[bold]History / Last 5 Successful Tasks[/bold]",
        border_style=NEON_BLUE,
    )


def _render_dashboard(state: RuntimeState, snapshot: dict[str, Any]) -> Layout:
    layout = Layout(name="root")
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body"),
    )
    layout["body"].split_row(
        Layout(name="left"),
        Layout(name="right"),
    )
    layout["left"].split_column(Layout(name="hardware"), Layout(name="intent"))
    layout["right"].split_column(Layout(name="vision"), Layout(name="history"))

    header = Text(
        f"NEXUS DISTRIBUTED NODE: {state.node_name} | TELEFIX OS v2.0",
        style=f"bold {NEON_BLUE}",
    )
    layout["header"].update(Panel(Align.center(header), border_style=NEON_BLUE))
    layout["hardware"].update(_hardware_panel(snapshot))
    layout["intent"].update(_intent_panel(snapshot))
    layout["vision"].update(_vision_panel(snapshot))
    layout["history"].update(_history_panel(snapshot))
    return layout


def main() -> None:
    args = _parse_args()

    if sys.platform.startswith("linux"):
        _ensure_stay_awake_linux()

    redis_client = _build_redis_client(args)

    counters = psutil.net_io_counters()
    state = RuntimeState(
        node_name=args.node_name,
        node_id=args.node_id,
        redis=redis_client,
        previous_net_sent=counters.bytes_sent,
        previous_net_recv=counters.bytes_recv,
        previous_net_ts=time.time(),
    )

    psutil.cpu_percent(interval=None)
    try:
        with Live(refresh_per_second=max(1, int(1 / max(args.refresh, 0.25))), screen=False) as live:
            while True:
                if sys.platform == "win32":
                    _stay_awake_tick_windows()
                snapshot = _collect_snapshot(state)
                live.update(_render_dashboard(state, snapshot))
                time.sleep(max(args.refresh, 0.25))
    except KeyboardInterrupt:
        pass
    finally:
        if sys.platform == "win32":
            _stay_awake_release_windows()
        try:
            redis_client.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
