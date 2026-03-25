"""
Swarm Heartbeat Test — Jacob-PC Master Node
============================================
Scans all nexus:heartbeat:* keys in Redis and prints a live status table.
Heartbeat values are stored as JSON strings (not hashes), matching the
actual heartbeat_scan.py implementation in this codebase.

Usage:
    $env:PYTHONPATH = "."; python scripts/swarm_heartbeat_test.py
"""

from __future__ import annotations

import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.getcwd())

import asyncio
import json
import time

import redis.asyncio as aioredis
from rich.console import Console
from rich.table import Table
from rich import box


REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
HEARTBEAT_PREFIX = "nexus:heartbeat:*"
STALE_THRESHOLD_S = 30


async def run_swarm_heartbeat() -> None:
    console = Console()

    console.print()
    console.print("[bold cyan]  NEXUS ORCHESTRATOR — SWARM HEARTBEAT TEST[/bold cyan]")
    console.print(
        f"[dim]Master: Jacob-PC  |  Redis: {REDIS_URL}  |  {time.strftime('%Y-%m-%d %H:%M:%S')}[/dim]"
    )
    console.print()

    r = aioredis.from_url(REDIS_URL, decode_responses=True)

    try:
        await r.ping()
    except Exception as exc:
        console.print(f"[bold red]Redis connection failed:[/bold red] {exc}")
        return

    # Publish a system.echo command so workers know we're probing
    probe = {"command": "system.echo", "params": {"text": "PING", "ts": time.time()}}
    await r.publish("nexus:commands", json.dumps(probe))

    # Collect all heartbeat keys
    keys: list[str] = []
    cursor = 0
    while True:
        cursor, batch = await r.scan(cursor=cursor, match=HEARTBEAT_PREFIX, count=100)
        keys.extend(batch)
        if cursor == 0:
            break

    table = Table(
        title="Swarm Worker Status",
        box=box.ROUNDED,
        show_lines=True,
        title_style="bold white",
    )
    table.add_column("Node ID", style="cyan", no_wrap=True)
    table.add_column("IP Address", style="magenta")
    table.add_column("Role", style="blue")
    table.add_column("Git Rev", style="green")
    table.add_column("CPU", style="yellow")
    table.add_column("RAM", style="yellow")
    table.add_column("Last Seen", style="white")
    table.add_column("Status", justify="center")

    if not keys:
        console.print(
            "[bold red]No workers reporting.[/bold red] "
            "Check Redis bind address on worker laptops (redis.conf: bind 0.0.0.0) "
            "and that the Nexus listener process is running."
        )
        await r.aclose()
        return

    now = time.time()
    alive = 0
    stale = 0

    for key in sorted(keys):
        raw = await r.get(key)
        if raw is None:
            continue

        try:
            data: dict = json.loads(raw)
        except json.JSONDecodeError:
            data = {}

        node_id: str = data.get("node_id") or key.split(":")[-1]
        ip: str = data.get("ip", "N/A")
        role: str = data.get("role", "worker")

        # Git revision — field name used by dispatcher.py heartbeat publisher
        git_rev: str = data.get("git_revision") or data.get("git_rev") or "Unknown"
        git_short = git_rev[:7] if git_rev != "Unknown" else "[dim]Unknown[/dim]"

        # Resource fields
        cpu_pct = data.get("cpu_percent") or data.get("cpu", "?")
        ram_pct = data.get("ram_percent") or data.get("ram", "?")

        # Timestamp
        ts = float(data.get("timestamp", 0))
        diff_s = now - ts if ts else float("inf")

        if diff_s < STALE_THRESHOLD_S:
            status = "[bold green]ALIVE[/bold green]"
            alive += 1
        elif diff_s < 120:
            status = "[bold yellow]STALE[/bold yellow]"
            stale += 1
        else:
            status = "[bold red]DEAD[/bold red]"
            stale += 1

        last_seen = f"{int(diff_s)}s ago" if ts else "[dim]never[/dim]"

        table.add_row(
            node_id,
            ip,
            role,
            git_short,
            f"{cpu_pct}%",
            f"{ram_pct}%",
            last_seen,
            status,
        )

    console.print(table)
    console.print()
    console.print(
        f"[bold]Summary:[/bold] "
        f"[green]{alive} alive[/green]  |  "
        f"[yellow]{stale} stale/dead[/yellow]  |  "
        f"[cyan]{len(keys)} total keys[/cyan]"
    )
    console.print()

    # Diagnosis hints
    if stale:
        console.print("[bold yellow]Diagnosis hints:[/bold yellow]")
        console.print(
            "  [yellow]•[/yellow] STALE/DEAD workers: check that "
            "[cyan]scripts/nexus_launcher.py[/cyan] is running on those laptops."
        )
        console.print(
            "  [yellow]•[/yellow] Git Rev = Unknown: the worker's "
            "[cyan]hardware.py[/cyan] heartbeat payload is missing git_revision — "
            "run [cyan]git pull[/cyan] + restart Nexus on that node."
        )
        console.print(
            "  [yellow]•[/yellow] No workers at all: ensure Redis on Jacob-PC "
            "is bound to [cyan]0.0.0.0[/cyan] (not 127.0.0.1) in redis.conf."
        )

    await r.aclose()


if __name__ == "__main__":
    asyncio.run(run_swarm_heartbeat())
