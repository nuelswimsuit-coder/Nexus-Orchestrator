"""
High-end terminal monitor for Nexus distributed nodes.
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
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
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

NEON_CYAN = "#00D7FF"
NEON_GREEN = "#46FF8B"
NEON_YELLOW = "#FFD84D"
NEON_MAGENTA = "#FF4FD8"
SOFT_WHITE = "#EAF7FF"

DEFAULT_INTENT = "Awaiting node intent stream from Redis"
DEFAULT_VISION = "Expand autonomous execution quality and reliability"
SLEEP_GUARD_ENV = "NEXUS_SLEEP_GUARD_ACTIVE"


@dataclass
class RuntimeState:
    node_name: str
    node_id: str
    redis: Redis
    previous_net_sent: int
    previous_net_recv: int
    previous_net_ts: float
    intent_stream: deque[str] = field(default_factory=lambda: deque(maxlen=24))
    vision_stream: deque[str] = field(default_factory=lambda: deque(maxlen=8))
    last_intent: str = ""
    last_vision: str = ""


class StayAwake:
    """Windows sleep inhibitor while monitor is active."""

    _ES_CONTINUOUS = 0x80000000
    _ES_SYSTEM_REQUIRED = 0x00000001
    _ES_AWAYMODE_REQUIRED = 0x00000040

    def __enter__(self) -> "StayAwake":
        if os.name == "nt":
            ctypes.windll.kernel32.SetThreadExecutionState(
                self._ES_CONTINUOUS | self._ES_SYSTEM_REQUIRED | self._ES_AWAYMODE_REQUIRED
            )
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if os.name == "nt":
            ctypes.windll.kernel32.SetThreadExecutionState(self._ES_CONTINUOUS)


def _maybe_reexec_with_systemd_inhibit() -> int | None:
    """
    On Linux, restart this process under systemd-inhibit to block system sleep.
    Returns an exit code when re-exec happened, otherwise None.
    """
    if sys.platform != "linux":
        return None
    if os.getenv(SLEEP_GUARD_ENV) == "1":
        return None
    if not shutil.which("systemd-inhibit"):
        return None

    env = os.environ.copy()
    env[SLEEP_GUARD_ENV] = "1"
    cmd = [
        "systemd-inhibit",
        "--what=sleep",
        "--why=Nexus node monitor active",
        "--mode=block",
        sys.executable,
        *sys.argv,
    ]
    return subprocess.call(cmd, env=env)


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
        help="Node ID used for node-specific Redis fallback keys",
    )
    parser.add_argument("--redis-host", default="127.0.0.1", help="Master Redis host")
    parser.add_argument("--redis-port", type=int, default=6379, help="Master Redis port")
    parser.add_argument("--redis-db", type=int, default=0, help="Redis database index")
    parser.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", ""),
        help="Optional full Redis URL (overrides host/port/db)",
    )
    parser.add_argument("--refresh", type=float, default=1.0, help="Dashboard refresh in seconds")
    return parser.parse_args()


def _build_redis_client(args: argparse.Namespace) -> Redis:
    if args.redis_url:
        return Redis.from_url(args.redis_url, decode_responses=True)
    return Redis(host=args.redis_host, port=args.redis_port, db=args.redis_db, decode_responses=True)


def _redis_get_first(redis_client: Redis, keys: list[str], default: str) -> str:
    for key in keys:
        try:
            value = redis_client.get(key)
            if value:
                return str(value)
        except RedisError:
            continue
    return default


def _get_action_history(redis_client: Redis, node_id: str) -> list[str]:
    for key in [f"node:{node_id}:history", "node:history"]:
        try:
            rows = redis_client.lrange(key, 0, 4)
            if rows:
                parsed = [str(row) for row in rows]
                successful = [
                    line for line in parsed
                    if any(tag in line.lower() for tag in ("completed", "success", "done"))
                ]
                return successful[:5] if successful else parsed[:5]
        except RedisError:
            continue
    return ["No completed actions recorded"]


def _format_speed(bytes_per_second: float) -> str:
    if bytes_per_second >= 1024**2:
        return f"{bytes_per_second / (1024**2):.2f} MB/s"
    if bytes_per_second >= 1024:
        return f"{bytes_per_second / 1024:.2f} KB/s"
    return f"{bytes_per_second:.0f} B/s"


def _format_bar(value: float, width: int = 24) -> str:
    clamped = max(0.0, min(100.0, value))
    filled = int((clamped / 100.0) * width)
    return "█" * filled + "░" * (width - filled)


def _gpu_snapshot() -> tuple[str, float | None]:
    try:
        import GPUtil  # type: ignore[import-untyped]

        gpus = GPUtil.getGPUs()
        if gpus:
            gpu = gpus[0]
            return f"{gpu.name[:28]}", float(gpu.load * 100.0)
    except Exception:
        pass

    try:
        completed = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=name,utilization.gpu",
                "--format=csv,noheader,nounits",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=1.5,
        )
        if completed.returncode == 0 and completed.stdout.strip():
            first = completed.stdout.strip().splitlines()[0]
            name, util = [piece.strip() for piece in first.split(",", maxsplit=1)]
            return name[:28], float(util)
    except Exception:
        pass

    return "GPU metrics unavailable", None


def _collect_snapshot(state: RuntimeState) -> dict[str, Any]:
    cpu = psutil.cpu_percent(interval=None)
    ram = psutil.virtual_memory().percent

    now = time.time()
    net = psutil.net_io_counters()
    elapsed = max(now - state.previous_net_ts, 0.001)
    upload_bps = (net.bytes_sent - state.previous_net_sent) / elapsed
    download_bps = (net.bytes_recv - state.previous_net_recv) / elapsed
    state.previous_net_sent = net.bytes_sent
    state.previous_net_recv = net.bytes_recv
    state.previous_net_ts = now

    intent = _redis_get_first(
        state.redis,
        ["node:intent", f"node:{state.node_id}:intent"],
        DEFAULT_INTENT,
    )
    vision = _redis_get_first(
        state.redis,
        ["node:vision", f"node:{state.node_id}:vision"],
        DEFAULT_VISION,
    )
    history = _get_action_history(state.redis, state.node_id)

    stamp = datetime.now().strftime("%H:%M:%S")
    if intent != state.last_intent:
        state.intent_stream.appendleft(f"[{stamp}] intent: {intent}")
        state.last_intent = intent
    if vision != state.last_vision:
        state.vision_stream.appendleft(f"[{stamp}] objective sync: {vision}")
        state.last_vision = vision
        state.intent_stream.appendleft(f"[{stamp}] goal update observed")

    return {
        "cpu": cpu,
        "ram": ram,
        "upload_bps": upload_bps,
        "download_bps": download_bps,
        "intent": intent,
        "vision": vision,
        "history": history,
        "intent_stream": list(state.intent_stream)[:12],
        "vision_stream": list(state.vision_stream)[:5],
    }


def _build_hardware_panel(snapshot: dict[str, Any]) -> Panel:
    table = Table.grid(expand=True)
    table.add_column(justify="left", ratio=2)
    table.add_column(justify="right", ratio=3)

    cpu_color = NEON_YELLOW if snapshot["cpu"] >= 95 else NEON_GREEN
    ram_color = NEON_YELLOW if snapshot["ram"] >= 90 else NEON_GREEN
    table.add_row(
        "CPU (95% target)",
        f"[{cpu_color}]{snapshot['cpu']:5.1f}% [/{cpu_color}][{SOFT_WHITE}]{_format_bar(snapshot['cpu'])}[/{SOFT_WHITE}]",
    )
    table.add_row(
        "RAM",
        f"[{ram_color}]{snapshot['ram']:5.1f}% [/{ram_color}][{SOFT_WHITE}]{_format_bar(snapshot['ram'])}[/{SOFT_WHITE}]",
    )
    table.add_row("NET IN", f"[{NEON_GREEN}]{_format_speed(snapshot['download_bps'])}[/{NEON_GREEN}]")
    table.add_row("NET OUT", f"[{NEON_GREEN}]{_format_speed(snapshot['upload_bps'])}[/{NEON_GREEN}]")
    return Panel(table, title="[bold]PANEL A | HARDWARE[/bold]", border_style=NEON_CYAN)


def _build_ai_thinking_panel(snapshot: dict[str, Any]) -> Panel:
    rows = snapshot["intent_stream"][:4] or ["[boot] waiting for intent stream"]
    text_rows = [
        Text(f"Current Intent: {snapshot['intent']}", style=f"bold {SOFT_WHITE}"),
        Text(" ", style=SOFT_WHITE),
        *[Text(entry, style=f"dim {NEON_GREEN}") for entry in rows],
    ]
    return Panel(
        Group(*text_rows),
        title="[bold]PANEL B | AI THINKING[/bold]",
        subtitle=f"[{NEON_YELLOW}]Warning lights indicate high node load[/{NEON_YELLOW}]",
        border_style=NEON_CYAN,
    )


def _build_vision_panel(snapshot: dict[str, Any]) -> Panel:
    content = Group(
        Text(f"Long-term Goal: {snapshot['vision']}", style=f"bold {SOFT_WHITE}"),
        Text(" ", style=SOFT_WHITE),
        *[Text(line, style=f"dim {NEON_GREEN}") for line in snapshot["vision_stream"]],
    )
    return Panel(
        content,
        title="[bold]PANEL C | STRATEGIC VISION[/bold]",
        subtitle="[dim]long-horizon objective map[/dim]",
        border_style=NEON_CYAN,
    )


def _build_history_panel(snapshot: dict[str, Any]) -> Panel:
    lines = [Text(f"- {item}", style=NEON_GREEN) for item in snapshot["history"][:5]]
    return Panel(
        Group(*lines),
        title="[bold]PANEL D | HISTORY (LAST 5 SUCCESSFUL TASKS)[/bold]",
        border_style=NEON_CYAN,
    )


def _render_dashboard(state: RuntimeState, snapshot: dict[str, Any]) -> Layout:
    layout = Layout(name="root")
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body", ratio=12),
    )
    layout["body"].split_column(
        Layout(name="top", ratio=1),
        Layout(name="bottom", ratio=1),
    )
    layout["top"].split_row(Layout(name="left", ratio=1), Layout(name="right", ratio=1))
    layout["bottom"].split_row(Layout(name="left_bottom", ratio=1), Layout(name="right_bottom", ratio=1))

    header = Text(
        f"NEXUS DISTRIBUTED NODE: {state.node_name} | TELEFIX OS v2.0",
        style=f"bold {NEON_CYAN}",
    )
    layout["header"].update(Panel(Align.center(header), border_style=NEON_CYAN))
    layout["left"].update(_build_hardware_panel(snapshot))
    layout["right"].update(_build_ai_thinking_panel(snapshot))
    layout["left_bottom"].update(_build_vision_panel(snapshot))
    layout["right_bottom"].update(_build_history_panel(snapshot))
    return layout


def main() -> None:
    # reexec_code = _maybe_reexec_with_systemd_inhibit()
    reexec_code = None  # ביטול זמני של המנגנון התקוע
    if reexec_code is not None:
        raise SystemExit(reexec_code)

    args = _parse_args()
    redis_client = _build_redis_client(args)
    net = psutil.net_io_counters()

    state = RuntimeState(
        node_name=args.node_name,
        node_id=args.node_id,
        redis=redis_client,
        previous_net_sent=net.bytes_sent,
        previous_net_recv=net.bytes_recv,
        previous_net_ts=time.time(),
    )
    psutil.cpu_percent(interval=None)

    with StayAwake():
        try:
            with Live(
                refresh_per_second=max(1, int(1 / max(args.refresh, 0.25))),
                screen=True,
                transient=False,
            ) as live:
                while True:
                    snapshot = _collect_snapshot(state)
                    live.update(_render_dashboard(state, snapshot))
                    time.sleep(max(args.refresh, 0.25))
        except KeyboardInterrupt:
            pass
        finally:
            try:
                redis_client.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
