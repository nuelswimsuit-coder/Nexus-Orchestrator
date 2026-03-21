"""
High-end terminal monitor for Nexus distributed nodes.
"""

from __future__ import annotations

import argparse
import ctypes
import inspect
import math
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import psutil
from redis import Redis
from redis.exceptions import RedisError
from rich.align import Align
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskID, TextColumn
from rich.table import Table
from rich.text import Text

# Explorer / desktop catalog uses this key; Fleet Insight treats "scanning" as Scavenger active.
EXPLORER_SCAN_STATE_KEY = "nexus:explorer:scan_state"
SCAVENGER_STATE_KEYS = ("nexus:fleet:scavenger:state", EXPLORER_SCAN_STATE_KEY)

NEON_CYAN = "#00D7FF"
NEON_GREEN = "#46FF8B"
NEON_YELLOW = "#FFD84D"
NEON_MAGENTA = "#FF4FD8"
SOFT_WHITE = "#EAF7FF"

DEFAULT_INTENT = "Awaiting node intent stream from Redis"
DEFAULT_VISION = "Expand autonomous execution quality and reliability"
DEFAULT_GLOBAL_MISSION = "— (set via nexus_core --task / global_mission key) —"
SLEEP_GUARD_ENV = "NEXUS_SLEEP_GUARD_ACTIVE"
SKIP_INHIBIT_ENV = "NEXUS_SKIP_INHIBIT"


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
    fleet_seen_paths: set[str] = field(default_factory=set)
    fleet_discovery_log: deque[str] = field(default_factory=lambda: deque(maxlen=32))
    fleet_log_lock: threading.Lock = field(default_factory=threading.Lock)
    fleet_scanning: bool = False
    fleet_mapping_complete: bool = False
    fleet_top_groups: list[tuple[str, int]] = field(default_factory=list)
    fleet_groups_error: str = ""
    fleet_groups_fetched_at: float = 0.0
    fleet_progress: Progress | None = None
    fleet_progress_task: TaskID | None = None
    fleet_progress_oscillate: bool = False
    fleet_worker_stop: threading.Event = field(default_factory=threading.Event)
    fleet_prev_scanning: bool = False


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
    On Linux, optionally restart under systemd-inhibit (blocks sleep).
    Off by default — wrapping the TUI in systemd-inhibit can hang or confuse
    some terminals; set NEXUS_USE_SYSTEMD_INHIBIT=true to enable.
    """
    if os.getenv("NEXUS_USE_SYSTEMD_INHIBIT", "").lower() not in {"1", "true", "yes", "on"}:
        return None
    if os.getenv(SKIP_INHIBIT_ENV) == "true":
        print("DEBUG: Skipping systemd-inhibit as requested.", flush=True)
        return None
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
    parser.add_argument(
        "--redis-host",
        default=(
            os.getenv("REDIS_HOST")
            or os.getenv("MASTER_IP")
            or ("10.100.102.8" if sys.platform == "linux" else "127.0.0.1")
        ),
        help="Master Redis host (env: REDIS_HOST / MASTER_IP; Linux default LAN stub)",
    )
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


def _read_scavenger_scanning(redis_client: Redis) -> tuple[bool, str]:
    """
    True when a fleet disk map is in progress.
    Prefers nexus:fleet:scavenger:state; falls back to Explorer scan_state.
    """
    for key in SCAVENGER_STATE_KEYS:
        try:
            raw = redis_client.get(key)
            if not raw:
                continue
            val = str(raw).strip().lower()
            if val in ("scanning", "running", "active"):
                return True, val
            if val in ("complete", "idle", "error", "done"):
                return False, val
        except RedisError:
            continue
    return False, "idle"


def _fleet_scan_roots() -> list[Path]:
    raw = os.environ.get("NEXUS_FLEET_SCAN_PATHS", "").strip()
    if raw:
        return [Path(p.strip()) for p in raw.split(",") if p.strip()]
    desktop = Path.home() / "Desktop"
    roots: list[Path] = []
    for name in ("OTP_Sessions_Creator", "Mangement Ahu", "Reporter", "Downloads"):
        p = desktop / name
        if p.is_dir():
            roots.append(p)
    if not roots and desktop.is_dir():
        roots.append(desktop)
    return roots


def _telefix_db_path() -> str:
    try:
        from nexus.api.services.telefix_bridge import DB_PATH as _db

        return str(_db)
    except Exception:
        root = os.environ.get("TELEFIX_PROJECT_ROOT", "").strip()
        if root:
            return str(Path(root) / "data" / "telefix.db")
        return str(Path.home() / "Desktop" / "Mangement Ahu" / "data" / "telefix.db")


def _fetch_top_groups_fleet(limit: int = 5) -> tuple[list[tuple[str, int]], str]:
    db_path = _telefix_db_path()
    if not os.path.isfile(db_path):
        return [], "database not found"
    try:
        uri = f"file:{db_path.replace(chr(92), '/')}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5.0)
        try:
            conn.row_factory = sqlite3.Row
            cur = conn.execute(
                """
                SELECT COALESCE(NULLIF(TRIM(source_group), ''), '(unknown)') AS g, COUNT(*) AS c
                FROM scraped_users
                GROUP BY 1
                ORDER BY c DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = [(str(r["g"]), int(r["c"])) for r in cur.fetchall()]
            if rows:
                return rows, ""
            cur2 = conn.execute(
                """
                SELECT COALESCE(NULLIF(TRIM(title), ''), NULLIF(TRIM(username), ''), '(untitled)') AS g, 0 AS c
                FROM managed_groups
                ORDER BY (last_automation IS NULL), last_automation DESC
                LIMIT ?
                """,
                (limit,),
            )
            return [(str(r["g"]), int(r["c"])) for r in cur2.fetchall()], ""
        finally:
            conn.close()
    except Exception as exc:
        return [], str(exc)


def _install_fleet_progress(state: RuntimeState, console: Console) -> None:
    bar_kw: dict[str, Any] = {}
    if "pulse" in inspect.signature(BarColumn.__init__).parameters:
        bar_kw["pulse"] = True
    bar_col = BarColumn(complete_style=NEON_GREEN, finished_style=NEON_GREEN, **bar_kw)
    prog = Progress(
        SpinnerColumn(style=NEON_CYAN),
        TextColumn("[bold #00D7FF]{task.description}[/]", justify="left"),
        bar_col,
        console=console,
        expand=True,
    )
    if bar_kw.get("pulse"):
        tid = prog.add_task("Scanning Disk / ZIPs...", total=None)
        oscillate = False
    else:
        tid = prog.add_task("Scanning Disk / ZIPs...", total=1000)
        oscillate = True
    state.fleet_progress = prog
    state.fleet_progress_task = tid
    state.fleet_progress_oscillate = oscillate


def _fleet_discovery_worker(state: RuntimeState) -> None:
    max_per_root = int(os.environ.get("NEXUS_FLEET_SCAN_CAP", "400"))
    interval = float(os.environ.get("NEXUS_FLEET_SCAN_INTERVAL", "2.5"))
    while not state.fleet_worker_stop.wait(timeout=interval):
        if not state.fleet_scanning:
            continue
        roots = _fleet_scan_roots()
        for root in roots:
            if not root.is_dir():
                continue
            n = 0
            try:
                for pattern in ("*.session", "*.zip"):
                    for p in root.rglob(pattern):
                        if not p.is_file():
                            continue
                        key = str(p.resolve())
                        with state.fleet_log_lock:
                            if key in state.fleet_seen_paths:
                                continue
                            state.fleet_seen_paths.add(key)
                            parent = str(p.parent).replace("\\", "/")
                            line = f"[FOUND] {p.name} in {parent}"
                            state.fleet_discovery_log.appendleft(line)
                        n += 1
                        if n >= max_per_root:
                            break
                    if n >= max_per_root:
                        break
            except OSError:
                continue


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

    global_mission = _redis_get_first(
        state.redis,
        ["global_mission"],
        DEFAULT_GLOBAL_MISSION,
    )
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

    scanning, redis_scan_state = _read_scavenger_scanning(state.redis)
    was_scanning = state.fleet_prev_scanning
    state.fleet_prev_scanning = scanning
    state.fleet_scanning = scanning

    if scanning and not was_scanning:
        with state.fleet_log_lock:
            state.fleet_seen_paths.clear()
            state.fleet_discovery_log.clear()

    state.fleet_mapping_complete = redis_scan_state == "complete" and not scanning

    now_ts = time.time()
    force_groups = was_scanning and not scanning
    if not scanning and (force_groups or now_ts - state.fleet_groups_fetched_at > 20.0):
        rows, gerr = _fetch_top_groups_fleet(5)
        state.fleet_top_groups = rows
        state.fleet_groups_error = gerr
        state.fleet_groups_fetched_at = now_ts

    if (
        scanning
        and state.fleet_progress_oscillate
        and state.fleet_progress is not None
        and state.fleet_progress_task is not None
    ):
        ph = time.time() * 1.8
        completed = int(350 + 280 * (1 + math.sin(ph)) / 2)
        state.fleet_progress.update(state.fleet_progress_task, completed=completed)

    with state.fleet_log_lock:
        discovery_lines = list(state.fleet_discovery_log)[:14]

    return {
        "cpu": cpu,
        "ram": ram,
        "upload_bps": upload_bps,
        "download_bps": download_bps,
        "global_mission": global_mission,
        "intent": intent,
        "vision": vision,
        "history": history,
        "intent_stream": list(state.intent_stream)[:12],
        "vision_stream": list(state.vision_stream)[:5],
        "fleet_scanning": scanning,
        "fleet_discovery_lines": discovery_lines,
        "fleet_top_groups": list(state.fleet_top_groups),
        "fleet_groups_error": state.fleet_groups_error,
        "fleet_mapping_complete": state.fleet_mapping_complete,
    }


def _build_hardware_panel(snapshot: dict[str, Any]) -> Panel:
    table = Table.grid(expand=True)
    table.add_column(justify="left", ratio=2)
    table.add_column(justify="right", ratio=3)

    cpu_color = NEON_YELLOW if snapshot["cpu"] >= 95 else NEON_GREEN
    ram_color = NEON_YELLOW if snapshot["ram"] >= 90 else NEON_GREEN
    table.add_row(
        "CPU (90% muscle target)",
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
        Text(
            f"Global mission: {snapshot.get('global_mission', DEFAULT_GLOBAL_MISSION)}",
            style=f"bold {NEON_MAGENTA}",
        ),
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


def _build_fleet_insight_panel(state: RuntimeState, snapshot: dict[str, Any]) -> Panel:
    rows: list[Any] = []
    if snapshot["fleet_scanning"] and state.fleet_progress is not None:
        rows.append(state.fleet_progress)
    elif snapshot["fleet_scanning"]:
        rows.append(Text("Scanning Disk / ZIPs…", style=f"bold {NEON_CYAN}"))

    rows.append(Text("", style=""))
    rows.append(Text("Real-time discovery", style=f"bold {SOFT_WHITE}"))
    disc = snapshot["fleet_discovery_lines"]
    if disc:
        rows.extend(Text(line, style=NEON_GREEN) for line in disc)
    else:
        rows.append(
            Text(
                "— no new .session / .zip paths this pass (Scavenger watches Desktop project trees) —",
                style="dim",
            )
        )

    top = snapshot["fleet_top_groups"]
    err = snapshot.get("fleet_groups_error") or ""
    rows.append(Text("", style=""))
    if top:
        summary = Table(show_header=True, header_style=NEON_MAGENTA, box=None, padding=(0, 1), expand=True)
        summary.add_column("#", justify="right", width=3)
        summary.add_column("Group", ratio=2, no_wrap=True)
        summary.add_column("Scraped", justify="right", width=8)
        for i, (name, cnt) in enumerate(top[:5], 1):
            display = name if len(name) <= 48 else name[:45] + "…"
            summary.add_row(str(i), display, str(cnt))
        rows.append(Text("Top 5 Groups in Fleet", style=f"bold {SOFT_WHITE}"))
        rows.append(summary)
    elif err:
        rows.append(Text(f"Top 5 Groups in Fleet — unavailable ({err})", style="dim"))
    else:
        rows.append(Text("Top 5 Groups in Fleet — no data yet", style="dim"))

    map_note = (
        "[dim]Explorer map: complete[/]"
        if snapshot["fleet_mapping_complete"]
        else ("[dim]Explorer map: scanning…[/]" if snapshot["fleet_scanning"] else "[dim]Explorer map: idle[/]")
    )
    return Panel(
        Group(*rows),
        title="[bold]FLEET INSIGHT | Scavenger + Fleet summary[/bold]",
        subtitle=map_note,
        border_style=NEON_MAGENTA,
    )


def _render_dashboard_compact(state: RuntimeState, snapshot: dict[str, Any]) -> Layout:
    """Single-column layout for short terminals (e.g. laptop 80×24)."""
    layout = Layout(name="root")
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="hw", ratio=2),
        Layout(name="ai", ratio=2),
        Layout(name="fleet", ratio=3),
        Layout(name="hist", ratio=2),
    )
    header = Text(
        f"NEXUS NODE {state.node_name} | {snapshot['cpu']:.0f}% CPU",
        style=f"bold {NEON_CYAN}",
    )
    layout["header"].update(Panel(Align.center(header), border_style=NEON_CYAN))
    layout["hw"].update(_build_hardware_panel(snapshot))
    layout["ai"].update(_build_ai_thinking_panel(snapshot))
    layout["fleet"].update(_build_fleet_insight_panel(state, snapshot))
    layout["hist"].update(_build_history_panel(snapshot))
    return layout


def _render_dashboard(state: RuntimeState, snapshot: dict[str, Any]) -> Layout:
    layout = Layout(name="root")
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body", ratio=12),
    )
    layout["body"].split_column(
        Layout(name="top", ratio=5),
        Layout(name="fleet", ratio=4),
        Layout(name="bottom", ratio=5),
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
    layout["fleet"].update(_build_fleet_insight_panel(state, snapshot))
    layout["left_bottom"].update(_build_vision_panel(snapshot))
    layout["right_bottom"].update(_build_history_panel(snapshot))
    return layout


def main() -> None:
    reexec_code = _maybe_reexec_with_systemd_inhibit()
    if reexec_code is not None:
        raise SystemExit(reexec_code)

    args = _parse_args()
    redis_client = _build_redis_client(args)
    net = psutil.net_io_counters()

    term = shutil.get_terminal_size(fallback=(100, 28))
    use_compact = os.getenv("NEXUS_MONITOR_COMPACT", "").lower() in {"1", "true", "yes", "on"} or (
        term.lines < 26 or term.columns < 100
    )
    console = Console(
        force_terminal=True,
        width=max(80, min(term.columns, 120)),
        height=max(24, min(term.lines, 48)) if term.lines else None,
    )
    state = RuntimeState(
        node_name=args.node_name,
        node_id=args.node_id,
        redis=redis_client,
        previous_net_sent=net.bytes_sent,
        previous_net_recv=net.bytes_recv,
        previous_net_ts=time.time(),
    )
    _install_fleet_progress(state, console)

    fleet_thread = threading.Thread(
        target=_fleet_discovery_worker,
        args=(state,),
        name="nexus-fleet-scavenger",
        daemon=True,
    )
    fleet_thread.start()

    psutil.cpu_percent(interval=None)

    with StayAwake():
        try:
            render = _render_dashboard_compact if use_compact else _render_dashboard
            with Live(
                render(state, _collect_snapshot(state)),
                refresh_per_second=max(1, int(1 / max(args.refresh, 0.25))),
                screen=True,
                transient=False,
                console=console,
            ) as live:
                while True:
                    snapshot = _collect_snapshot(state)
                    live.update(render(state, snapshot))
                    time.sleep(max(args.refresh, 0.25))
        except KeyboardInterrupt:
            pass
        finally:
            state.fleet_worker_stop.set()
            try:
                redis_client.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
