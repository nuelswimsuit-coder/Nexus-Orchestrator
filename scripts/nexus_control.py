#!/usr/bin/env python3
"""
NEXUS CONTROL — Operator command deck (Yarin).

Central Rich CLI for sync, fleet scavenge, trading mode, health, emergency stop,
and optional scheduled automation (09:00 scavenge, 00:00 Telegram report).

Linux: exports NEXUS_SKIP_INHIBIT=true for child compatibility (see launch_worker.sh).
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

# ── Linux / worker compatibility (before other project imports) ─────────────
os.environ.setdefault("NEXUS_SKIP_INHIBIT", "true")

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

try:
    asyncio.get_running_loop()
except RuntimeError:
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from nexus.shared.operator_targets import load_operator_targets_raw
from nexus.trading.config import PREDICTION_MANUAL_HALT_KEY

from rich.align import Align
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table
from rich.text import Text
from rich.theme import Theme

THEME = Theme(
    {
        "cmd": "bold cyan",
        "ok": "bold green",
        "warn": "bold yellow",
        "danger": "bold red",
        "muted": "dim",
        "accent": "bold magenta",
    }
)

_term_w = shutil.get_terminal_size((120, 24)).columns
console = Console(theme=THEME, width=max(120, _term_w))

LIVE_CONFIRM_ENV = "NEXUS_CONTROL_LIVE_CONFIRM"
PANIC_KEY = "SYSTEM_STATE:PANIC"
STRATEGY_SNAPSHOT_KEY = "nexus:strategy_brain:snapshot"


def _parse_dotenv_value(env_path: Path, key: str) -> str | None:
    if not env_path.is_file():
        return None
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        if k.strip() != key:
            continue
        return v.strip().strip('"').strip("'")
    return None


def _upsert_dotenv_key(env_path: Path, key: str, value: str) -> None:
    """Set or replace ``key=value`` in .env (preserves other lines)."""
    env_path.parent.mkdir(parents=True, exist_ok=True)
    assign = f"{key}={value}"
    if not env_path.is_file():
        env_path.write_text(assign + "\n", encoding="utf-8")
        return
    lines = env_path.read_text(encoding="utf-8").splitlines()
    out: list[str] = []
    seen = False
    for line in lines:
        s = line.strip()
        if s and not s.startswith("#") and "=" in s:
            k, _, _ = s.partition("=")
            if k.strip() == key:
                if not seen:
                    out.append(assign)
                    seen = True
                continue
        out.append(line)
    if not seen:
        out.append(assign)
    env_path.write_text("\n".join(out) + "\n", encoding="utf-8")


def _ensure_live_confirm_secret() -> str:
    """
    If NEXUS_CONTROL_LIVE_CONFIRM is unset, load from .env or append MASTER and
    set os.environ so the operator does not restart the script.
    """
    env_path = PROJECT_ROOT / ".env"
    cur = os.environ.get(LIVE_CONFIRM_ENV, "").strip()
    if cur:
        return cur
    file_val = (_parse_dotenv_value(env_path, LIVE_CONFIRM_ENV) or "").strip()
    if file_val:
        os.environ[LIVE_CONFIRM_ENV] = file_val
        return file_val
    _upsert_dotenv_key(env_path, LIVE_CONFIRM_ENV, "MASTER")
    os.environ[LIVE_CONFIRM_ENV] = "MASTER"
    return "MASTER"


def _bootstrap_operator_redis() -> None:
    """Coerce broker URL, try WSL Redis on Windows, then allow degraded control-deck boot."""
    from nexus.shared import redis_util
    from nexus.shared.config import settings

    url = redis_util.coerce_redis_url_for_platform(settings.redis_url)
    if url != settings.redis_url:
        settings.redis_url = url
    os.environ["REDIS_URL"] = url

    if redis_util.try_ping_sync(url):
        return

    if sys.platform == "win32":
        console.print(
            "[warn]Redis unreachable — running WSL: service redis-server start[/]"
        )
        proc = redis_util.try_start_redis_via_wsl_windows()
        if proc and (proc.stderr or "").strip():
            console.print(f"[muted]{proc.stderr.strip()[:240]}[/]")
        time.sleep(3)
        if redis_util.try_ping_sync(url):
            console.print("[ok]Redis broker is reachable.[/]")
            return

    redis_util.mark_degraded_mode()
    console.print(
        Panel.fit(
            "[warn]Redis still unreachable — NEXUS_ALLOW_DEGRADED=1. "
            "Menu opens; broker-backed actions will fail until Redis is up.[/]",
            border_style="yellow",
        )
    )


def _node_profile() -> tuple[str, bool]:
    """Returns (label, is_windows_master_hint)."""
    if sys.platform == "win32":
        return "COMMAND NODE — Windows Master", True
    return "COMMAND NODE — Linux Worker / Edge", False


_IPV4_RE = re.compile(
    r"^(?:(?:25[0-5]|2[0-4]\d|[01]?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d?\d)$"
)


def _detect_external_ipv4(*, timeout: float = 3.0) -> str | None:
    """Return the host's public IPv4 if an external check succeeds, else None."""
    import urllib.error
    import urllib.request

    req = urllib.request.Request(
        "https://api.ipify.org",
        headers={"User-Agent": "NexusControl/1.0"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("ascii", errors="replace").strip()
    except (OSError, urllib.error.URLError, TimeoutError):
        return None
    if raw and _IPV4_RE.match(raw):
        return raw
    return None


def _print_master_fleet_visibility_if_applicable() -> None:
    """On Windows master, if a public IP is detected, confirm fleet reachability."""
    _, is_master = _node_profile()
    if not is_master or sys.platform != "win32":
        return
    ext = _detect_external_ipv4()
    if ext:
        console.print(f"[ok][OK] Master is visible to the Fleet at {ext}[/]")


def _banner() -> None:
    from nexus.shared import redis_util
    from nexus.shared.config import settings

    title = Text()
    title.append(" NEXUS ", style="bold white on blue")
    title.append(" ORCHESTRATOR ", style="bold black on cyan")
    title.append(" CONTROL ", style="bold white on magenta")
    sub = Text()
    node_line, is_master = _node_profile()
    sub.append(f"{node_line}\n", style="cmd")
    sub.append("Strategy Brain + Dispatcher integrated · Redis authority\n", style="muted")
    sub.append(f"NEXUS_SKIP_INHIBIT={os.environ.get('NEXUS_SKIP_INHIBIT')}\n", style="muted")
    sub.append(f"REDIS_URL={settings.redis_url}", style="muted")
    if redis_util.degraded_mode_active():
        sub.append("\nNEXUS_ALLOW_DEGRADED=1 — broker bypass", style="warn")
    console.print()
    console.print(Panel(Align.center(title + Text("\n") + sub), border_style="cyan", padding=(1, 2)))


def _override_label(ov: str) -> str:
    return {
        "auto": "AUTO",
        "force_night": "FORCE-NIGHT",
        "force_active": "FORCE-ACTIVE",
    }.get(ov.strip().lower(), ov.upper())


def _redis_sync_client():
    from redis import Redis
    from nexus.shared.config import settings

    return Redis.from_url(settings.redis_url, decode_responses=True)


def _print_power_status_line() -> None:
    try:
        from nexus.shared import redis_util
        from nexus.shared.power_profile import REDIS_OVERRIDE_KEY, REDIS_SNAPSHOT_KEY, parse_snapshot

        if redis_util.degraded_mode_active():
            return
        r = _redis_sync_client()
        try:
            ov = (r.get(REDIS_OVERRIDE_KEY) or "auto").strip().lower()
            snap = parse_snapshot(r.get(REDIS_SNAPSHOT_KEY))
            olabel = _override_label(ov)
            if snap:
                sec = snap.get("seconds_until_shift")
                console.print(
                    f"[muted][POWER][/] {snap.get('display_label', '')} · "
                    f"[POWER-MODE]: {olabel} · next shift ≈ {sec}s"
                )
            else:
                console.print(
                    f"[muted][POWER][/] (awaiting Master snapshot) · [POWER-MODE]: {olabel}"
                )
        finally:
            r.close()
    except Exception:
        pass


def _cycle_power_override(*, apply_local_affinity: bool = True) -> str:
    from nexus.shared.power_profile import (
        REDIS_OVERRIDE_KEY,
        REDIS_SNAPSHOT_KEY,
        apply_power_to_process,
        cycle_override_mode,
        decide_power_profile,
        parse_snapshot,
    )

    r = _redis_sync_client()
    try:
        cur = (r.get(REDIS_OVERRIDE_KEY) or "auto").strip().lower()
        nxt = cycle_override_mode(cur)
        r.set(REDIS_OVERRIDE_KEY, nxt)
        d = decide_power_profile(override_raw=nxt)
        lines = [
            f"[POWER-MODE]: {_override_label(nxt)} (was {_override_label(cur)})",
            d.display_line,
        ]
        snap = parse_snapshot(r.get(REDIS_SNAPSHOT_KEY))
        if apply_local_affinity and snap and snap.get("master_pid"):
            pid = int(snap["master_pid"])
            st = apply_power_to_process(pid, d, set_affinity=True)
            lines.append(
                "Master affinity: "
                + ("re-applied" if st.get("affinity_ok") else "could not set (check permissions)")
            )
        elif not apply_local_affinity:
            lines.append("Override stored in Redis — Master node will apply on next tick.")
        else:
            lines.append("Master PID unknown yet — cap/affinity apply on next Master tick.")
        return "\n".join(lines)
    finally:
        r.close()


def _power_coherence_touch() -> None:
    """Re-apply affinity to the Master PID from Redis (covers OS resets)."""
    try:
        from nexus.shared import redis_util
        from nexus.shared.power_profile import (
            REDIS_OVERRIDE_KEY,
            REDIS_SNAPSHOT_KEY,
            apply_power_to_process,
            decide_power_profile,
            parse_snapshot,
        )

        if redis_util.degraded_mode_active():
            return
        r = _redis_sync_client()
        try:
            ov = (r.get(REDIS_OVERRIDE_KEY) or "auto").strip().lower()
            d = decide_power_profile(override_raw=ov)
            snap = parse_snapshot(r.get(REDIS_SNAPSHOT_KEY))
            if snap and snap.get("master_pid"):
                apply_power_to_process(int(snap["master_pid"]), d, set_affinity=True)
        finally:
            r.close()
    except Exception:
        pass


def _menu_table(is_master_hint: bool) -> Table:
    t = Table(show_header=False, box=None, padding=(0, 2))
    t.add_column("Key", style="accent", justify="right")
    t.add_column("Action", style="bold")
    t.add_column("Detail", style="muted")
    rows = [
        ("a", "[SYNC]", "Git pull + pip refresh (requirements.txt)"),
        ("b", "[SCAVENGE]", "Super-scrape + account mapper (via Dispatcher)"),
        ("s", "[GLOBAL-SCAVENGE]", "Deep local scan (.session / tdata / zips) + mapper queue"),
        ("c", "[TRADE-SIM]", "Prediction / Polymarket → simulation (Redis)"),
        ("d", "[TRADE-LIVE]", "Master trader real mode — confirm code required"),
        ("e", "[STATUS]", "Hardware + swarm + Strategy Brain snapshot"),
        ("f", "[EMERGENCY]", "Kill-switch: PANIC + manual halt + sim mode"),
        ("p", "[POWER]", "Toggle [POWER-MODE]: AUTO / FORCE-NIGHT / FORCE-ACTIVE"),
        ("q", "[QUIT]", "Exit"),
    ]
    if not is_master_hint:
        rows[4] = ("d", "[TRADE-LIVE]", "Arm live on Redis (worker shares queue — same confirm)")
    for r in rows:
        t.add_row(*r)
    return t


async def _redis():
    from redis.asyncio import from_url as redis_from_url
    from nexus.shared import redis_util
    from nexus.shared.config import settings

    if redis_util.degraded_mode_active():
        raise RuntimeError(
            "Redis unavailable (NEXUS_ALLOW_DEGRADED=1). "
            "Start the broker or fix REDIS_URL, then restart."
        )

    r = redis_from_url(settings.redis_url, decode_responses=True)
    await r.ping()
    return r


async def _close_redis(r: Any) -> None:
    try:
        await r.aclose()
    except Exception:
        pass


def _sync_repo() -> None:
    console.print(Panel.fit("[cmd]SYNC[/] — repository & environment", border_style="cyan"))
    git = subprocess.run(
        ["git", "pull", "--ff-only"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    if git.returncode == 0:
        console.print("[ok]git pull[/]", git.stdout.strip() or "(already up to date)")
    else:
        console.print("[warn]git pull[/]", git.stderr or git.stdout or str(git.returncode))
    pip = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q", "-r", str(PROJECT_ROOT / "requirements.txt")],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    if pip.returncode == 0:
        console.print("[ok]pip install -r requirements.txt[/]")
    else:
        console.print("[warn]pip[/]", pip.stderr.strip() or pip.stdout.strip())


async def _dispatch_via_dispatcher(tasks: list[Any]) -> list[str]:
    from arq.connections import RedisSettings

    from nexus.master.dispatcher import Dispatcher
    from nexus.master.resource_guard import ResourceGuard
    from nexus.master.services.vault import Vault
    from nexus.shared.config import settings
    from nexus.shared.notifications.providers.telegram import TelegramProvider
    from nexus.shared.notifications.providers.whatsapp import WhatsAppProvider
    from nexus.shared.notifications.service import NotificationService

    notifier = NotificationService()
    notifier.register(
        WhatsAppProvider(
            to_number=settings.whatsapp_to_number,
            dashboard_url=settings.telegram_dashboard_url,
        )
    )
    if settings.telegram_bot_token and settings.telegram_admin_chat_id:
        notifier.register(
            TelegramProvider(
                bot_token=settings.telegram_bot_token,
                admin_chat_id=settings.telegram_admin_chat_id,
                dashboard_url=settings.telegram_dashboard_url,
            )
        )

    vault = Vault()
    try:
        from nexus.shared.paths import get_telefix_path

        tenv = get_telefix_path("Mangement Ahu") / ".env"
        vault.load_env_file(
            tenv,
            key_mapping={
                "BOT_TOKEN": "TELEFIX_BOT_TOKEN",
                "API_ID": "TELEFIX_API_ID",
                "API_HASH": "TELEFIX_API_HASH",
            },
        )
        vault.register_task_secrets(
            "telegram",
            ["TELEFIX_BOT_TOKEN", "TELEFIX_API_ID", "TELEFIX_API_HASH"],
        )
    except Exception:
        pass

    vault.register_task_secrets(
        "swarm",
        [
            "TELEFIX_API_ID",
            "TELEFIX_API_HASH",
            "GEMINI_API_KEY",
            "OPENAI_API_KEY",
        ],
    )

    guard = ResourceGuard(cpu_cap_percent=80, ram_cap_mb=4096)
    dispatcher = Dispatcher(
        redis_settings=RedisSettings.from_dsn(settings.redis_url),
        node_id=f"{settings.node_id}-control",
        resource_guard=guard,
        vault=vault,
        notification_service=notifier,
    )
    await dispatcher.start()
    ids: list[str] = []
    try:
        for t in tasks:
            ids.append(await dispatcher.dispatch(t))
    finally:
        await dispatcher.stop()
    return ids


def _run_account_scavenge_blocking() -> Any:
    from nexus.worker.services.scavenger import run_account_scavenge

    return run_account_scavenge()


async def cmd_global_scavenge() -> None:
    from nexus.shared.schemas import TaskPayload

    console.print(
        Panel.fit(
            "[cmd]GLOBAL-SCAVENGE[/] — recursive disk + zip scan → "
            "data/staged_accounts, then account_mapper.map",
            border_style="cyan",
        )
    )
    loop = asyncio.get_event_loop()
    with console.status("[accent]Deep-scanning user profile for sessions, JSON, tdata, zips…[/]"):
        res = await loop.run_in_executor(None, _run_account_scavenge_blocking)
    console.print(
        f"[ok]Staged[/] accounts={res.accounts_staged} "
        f"files_written={res.files_written} deduped={res.files_deduplicated}"
    )
    if getattr(res, "errors", None):
        n = len(res.errors)
        if n:
            console.print(f"[warn]{n} scanner messages — check structlog for details.[/]")
    t_map = TaskPayload(
        task_id=str(uuid.uuid4()),
        task_type="account_mapper.map",
        parameters={"controlled_warmup": True},
        project_id="nexus-control",
        priority=3,
    )
    with console.status("[accent]Dispatching account_mapper.map via Dispatcher…[/]"):
        ids = await _dispatch_via_dispatcher([t_map])
    for jid in ids:
        console.print(f"[ok]Enqueued[/] job_id={jid}")


async def cmd_scavenge() -> None:
    from nexus.shared.schemas import TaskPayload

    console.print(Panel.fit("[cmd]SCAVENGE[/] — fleet intelligence run", border_style="cyan"))
    stealth = Confirm.ask("Bypass super-scraper CPU guard (stealth_override)?", default=True)
    t_super = TaskPayload(
        task_id=str(uuid.uuid4()),
        task_type="telegram.super_scrape",
        parameters={"stealth_override": stealth},
        project_id="nexus-control",
        priority=2,
    )
    t_map = TaskPayload(
        task_id=str(uuid.uuid4()),
        task_type="account_mapper.map",
        parameters={},
        project_id="nexus-control",
        priority=3,
    )
    with console.status("[accent]Dispatching via Dispatcher…[/]"):
        ids = await _dispatch_via_dispatcher([t_super, t_map])
    for jid in ids:
        console.print(f"[ok]Enqueued[/] job_id={jid}")


async def cmd_trade_sim() -> None:
    from nexus.trading.runtime_mode import TRADING_MODE_REDIS_KEY

    r = await _redis()
    try:
        await r.set(TRADING_MODE_REDIS_KEY, "paper")
        console.print(Panel.fit("[ok]TRADE-SIM[/] Redis mode → paper", border_style="green"))
        console.print(f"  [muted]{TRADING_MODE_REDIS_KEY}=paper[/]")
    finally:
        await _close_redis(r)


async def cmd_trade_live() -> None:
    from nexus.trading.runtime_mode import TRADING_MODE_REDIS_KEY

    secret = _ensure_live_confirm_secret()
    code = (Prompt.ask("Enter authorization code:") or "").strip()
    if code != secret:
        console.print("[danger]Code mismatch — live mode NOT armed.[/]")
        return
    r = await _redis()
    try:
        await r.set(TRADING_MODE_REDIS_KEY, "race")
        await r.delete(PREDICTION_MANUAL_HALT_KEY)
        console.print(Panel.fit("[ok]TRADE-LIVE[/] Redis mode → race (live)", border_style="red"))
        console.print(f"  [muted]{TRADING_MODE_REDIS_KEY}=race[/]")
    finally:
        await _close_redis(r)


async def cmd_emergency() -> None:
    from nexus.trading.runtime_mode import TRADING_MODE_REDIS_KEY

    if not Confirm.ask("[danger]ENGAGE EMERGENCY KILL-SWITCH?[/]", default=False):
        return
    r = await _redis()
    try:
        await r.set(PANIC_KEY, "true")
        await r.set(PREDICTION_MANUAL_HALT_KEY, "1")
        await r.set(TRADING_MODE_REDIS_KEY, "paper")
        console.print(Panel.fit("[danger]KILL-SWITCH ACTIVE[/]", border_style="red"))
        console.print(f"  {PANIC_KEY}=true")
        console.print(f"  {PREDICTION_MANUAL_HALT_KEY}=1")
        console.print(f"  {TRADING_MODE_REDIS_KEY}=paper")
    finally:
        await _close_redis(r)


async def cmd_status() -> None:
    import psutil
    from nexus.shared.config import settings

    console.print(Panel.fit("[cmd]STATUS[/] — hardware & swarm", border_style="cyan"))

    tbl = Table(title="Local hardware", show_edge=False)
    tbl.add_column("Metric")
    tbl.add_column("Value", justify="right")
    tbl.add_row("Host", os.environ.get("NODE_ID", sys.platform))
    tbl.add_row("CPU %", f"{psutil.cpu_percent(interval=0.4):.1f}")
    vm = psutil.virtual_memory()
    tbl.add_row("RAM", f"{vm.percent:.1f}% used ({vm.used // (1024**2)} / {vm.total // (1024**2)} MiB)")
    console.print(tbl)

    r = await _redis()
    try:
        raw = await r.get(STRATEGY_SNAPSHOT_KEY)
        if raw:
            try:
                snap = json.loads(raw)
                s_tbl = Table(title="Strategy Brain (Redis snapshot)", show_edge=False)
                s_tbl.add_column("Field")
                s_tbl.add_column("Value")
                for k in ("updated_at", "composite_score", "regime", "bias"):
                    if k in snap:
                        s_tbl.add_row(k, str(snap[k])[:120])
                console.print(s_tbl)
            except json.JSONDecodeError:
                console.print("[warn]Strategy snapshot present but not JSON.[/]")
        else:
            console.print("[muted]No strategy snapshot yet (nexus:strategy_brain:snapshot).[/]")

        q = await r.zcard("arq:queue:nexus:tasks")
        console.print(f"[muted]ARQ pending jobs:[/] {q}")
    finally:
        await _close_redis(r)

    base = f"http://127.0.0.1:{settings.api_port}"
    try:
        import httpx

        async with httpx.AsyncClient(timeout=8.0) as client:
            res = await client.get(f"{base}/api/cluster/status")
            if res.status_code == 200:
                data = res.json()
                nodes = data.get("nodes") or []
                n_tbl = Table(title=f"Cluster API ({base})", show_edge=False)
                n_tbl.add_column("node_id")
                n_tbl.add_column("role")
                n_tbl.add_column("CPU%")
                n_tbl.add_column("online")
                for n in nodes[:24]:
                    n_tbl.add_row(
                        str(n.get("node_id", "")),
                        str(n.get("role", "")),
                        str(n.get("cpu_percent", "")),
                        "yes" if n.get("online") else "no",
                    )
                console.print(n_tbl)
            else:
                console.print(f"[warn]Cluster API HTTP {res.status_code}[/]")
            dres = await client.get(f"{base}/api/business/decisions")
            if dres.status_code == 200:
                dj = dres.json()
                decs = dj.get("decisions") or []
                if decs:
                    top = decs[0]
                    console.print(
                        f"[muted]Decision Engine top:[/] {top.get('title','')} "
                        f"(conf={top.get('confidence')}, task={top.get('action_task_type','')})"
                    )
    except Exception as exc:
        console.print(f"[muted]Cluster API unreachable ({exc}). Is the API up?[/]")


async def cmd_midnight_report() -> None:
    from nexus.master.services.reporting import ReportingService
    from nexus.shared.config import settings
    from nexus.shared.notifications.providers.telegram import TelegramProvider
    from nexus.shared.notifications.providers.whatsapp import WhatsAppProvider
    from nexus.shared.notifications.service import NotificationService

    r = await _redis()
    notifier = NotificationService()
    notifier.register(WhatsAppProvider(to_number=settings.whatsapp_to_number))
    if settings.telegram_bot_token and settings.telegram_admin_chat_id:
        notifier.register(
            TelegramProvider(
                bot_token=settings.telegram_bot_token,
                admin_chat_id=settings.telegram_admin_chat_id,
                dashboard_url=settings.telegram_dashboard_url,
            )
        )
    svc = ReportingService(
        notifier=notifier,
        redis=r,
        report_hour=0,
        report_minute=0,
        window_hours=24,
        period_name="MIDNIGHT",
    )
    try:
        with console.status("[accent]Generating & sending Telegram report…[/]"):
            await svc.send_report()
        console.print("[ok]Midnight report dispatched.[/]")
    finally:
        await _close_redis(r)


async def schedule_runner() -> None:
    """09:00 scavenge, 00:00 Telegram report (local time)."""
    from nexus.shared.schemas import TaskPayload

    last_scavenge_date = ""
    last_report_date = ""
    console.print(
        Panel.fit(
            "[cmd]SCHEDULED MODE[/]\n"
            "· 09:00 — SCAVENGE (super_scrape + account_mapper)\n"
            "· 00:00 — Telegram profit report",
            border_style="magenta",
        )
    )
    while True:
        await asyncio.sleep(20)
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        if now.hour == 9 and now.minute == 0 and last_scavenge_date != today:
            last_scavenge_date = today  # guard before slow dispatch
            try:
                t_super = TaskPayload(
                    task_id=str(uuid.uuid4()),
                    task_type="telegram.super_scrape",
                    parameters={"stealth_override": True},
                    project_id="nexus-control-schedule",
                    priority=2,
                )
                t_map = TaskPayload(
                    task_id=str(uuid.uuid4()),
                    task_type="account_mapper.map",
                    parameters={},
                    project_id="nexus-control-schedule",
                    priority=3,
                )
                ids = await _dispatch_via_dispatcher([t_super, t_map])
                console.print(f"[ok]{now.isoformat()}[/] Scheduled SCAVENGE → {ids}")
            except Exception as exc:
                console.print(f"[warn]Scheduled scavenge failed:[/] {exc}")
        if now.hour == 0 and now.minute == 0 and last_report_date != today:
            last_report_date = today
            try:
                await cmd_midnight_report()
                console.print(f"[ok]{now.isoformat()}[/] Scheduled midnight report done.")
            except Exception as exc:
                console.print(f"[warn]Scheduled report failed:[/] {exc}")


def _interactive() -> None:
    _, is_master = _node_profile()
    stop_power = threading.Event()

    def _power_periodic() -> None:
        while not stop_power.wait(300):
            _power_coherence_touch()

    if is_master:
        threading.Thread(
            target=_power_periodic,
            name="nexus-power-coherence",
            daemon=True,
        ).start()

    try:
        while True:
            console.print()
            if is_master:
                _print_power_status_line()
            console.print(_menu_table(is_master))
            choice = Prompt.ask(
                "\n[accent]Select[/]",
                choices=["a", "b", "s", "c", "d", "e", "f", "p", "q"],
                default="e",
            )
            if choice == "q":
                console.print("[muted]NEXUS Control — session end.[/]")
                return
            if choice == "a":
                _sync_repo()
            elif choice == "b":
                asyncio.run(cmd_scavenge())
            elif choice == "s":
                asyncio.run(cmd_global_scavenge())
            elif choice == "c":
                asyncio.run(cmd_trade_sim())
            elif choice == "d":
                asyncio.run(cmd_trade_live())
            elif choice == "e":
                asyncio.run(cmd_status())
            elif choice == "f":
                asyncio.run(cmd_emergency())
            elif choice == "p":
                try:
                    console.print(
                        Panel.fit(
                            _cycle_power_override(apply_local_affinity=is_master),
                            border_style="magenta",
                        )
                    )
                except Exception as exc:
                    console.print(f"[warn]Power toggle failed:[/] {exc}")
    finally:
        stop_power.set()


def main() -> None:
    parser = argparse.ArgumentParser(description="Nexus operator control deck")
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Run scheduled automation (09:00 scavenge, 00:00 report); blocks.",
    )
    args = parser.parse_args()

    _bootstrap_operator_redis()
    load_operator_targets_raw()
    _banner()
    _print_master_fleet_visibility_if_applicable()
    if args.schedule:
        try:
            asyncio.run(schedule_runner())
        except KeyboardInterrupt:
            console.print("\n[warn]Scheduler stopped.[/]")
        return
    _interactive()


if __name__ == "__main__":
    main()
