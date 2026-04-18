"""
DevOps Monitor Tools
Covers tools 1–6 of the DevOps category:
  /sysmon         — CPU / RAM / temp / disk of the master node
  /logs [n]       — tail last N lines from master/worker logs
  /sessions_check — ping every .session file and report live/dead
  /watchdog       — show watchdog status
  /watchdog_on    — enable auto-restart watchdog
  /watchdog_off   — disable watchdog
"""
from __future__ import annotations

import asyncio
import os
import platform
import sys
import time
from pathlib import Path

import structlog

log = structlog.get_logger(__name__)

ROOT         = Path(__file__).resolve().parents[3]
LOGS_DIR     = ROOT / "logs"
SESSIONS_DIR = Path(os.environ.get("TELEFIX_SESSIONS_DIR", "")).expanduser() \
               or Path(os.environ.get("TELEFIX_ROOT",
                   str(Path.home() / "Desktop" / "Mangement Ahu")
               )) / "sessions"

_WATCHDOG_FLAG = ROOT / ".nexus_watchdog_enabled"
_WATCHDOG_ENABLED = True   # runtime state (toggled by commands)


# ── System metrics ────────────────────────────────────────────────────────────

def get_system_metrics() -> dict:
    """
    Returns a dict with CPU%, RAM used/total, disk used/total, temperatures.
    Uses psutil if available; falls back to platform info.
    """
    info: dict = {
        "cpu_pct":      0.0,
        "ram_used_gb":  0.0,
        "ram_total_gb": 0.0,
        "ram_pct":      0.0,
        "disk_used_gb": 0.0,
        "disk_total_gb":0.0,
        "disk_pct":     0.0,
        "cpu_temp_c":   None,
        "platform":     platform.system(),
        "python":       sys.version[:6],
        "uptime_h":     0.0,
    }
    try:
        import psutil
        cpu = psutil.cpu_percent(interval=0.5)
        ram = psutil.virtual_memory()
        disk= psutil.disk_usage(str(ROOT))

        info["cpu_pct"]       = cpu
        info["ram_used_gb"]   = round(ram.used  / 1e9, 2)
        info["ram_total_gb"]  = round(ram.total / 1e9, 2)
        info["ram_pct"]       = ram.percent
        info["disk_used_gb"]  = round(disk.used  / 1e9, 1)
        info["disk_total_gb"] = round(disk.total / 1e9, 1)
        info["disk_pct"]      = disk.percent

        # Boot time → uptime
        boot   = psutil.boot_time()
        uptime = (time.time() - boot) / 3600
        info["uptime_h"] = round(uptime, 1)

        # CPU temperature (Linux / macOS via sensors)
        try:
            temps = psutil.sensors_temperatures()
            if temps:
                for key in ("coretemp", "cpu_thermal", "k10temp", "zenpower"):
                    if key in temps and temps[key]:
                        info["cpu_temp_c"] = round(temps[key][0].current, 1)
                        break
        except Exception:
            pass
    except ImportError:
        pass
    return info


def format_sysmon(m: dict) -> str:
    """Format system metrics as a MarkdownV2-safe message."""

    def bar(pct: float, width: int = 10) -> str:
        filled = int(pct / 100 * width)
        return "█" * filled + "░" * (width - filled)

    cpu_bar  = bar(m["cpu_pct"])
    ram_bar  = bar(m["ram_pct"])
    disk_bar = bar(m["disk_pct"])

    cpu_c = "🔴" if m["cpu_pct"] > 85 else "🟡" if m["cpu_pct"] > 60 else "🟢"
    ram_c = "🔴" if m["ram_pct"] > 85 else "🟡" if m["ram_pct"] > 60 else "🟢"

    temp_line = ""
    if m["cpu_temp_c"] is not None:
        tc = m["cpu_temp_c"]
        ti = "🔴" if tc > 85 else "🟡" if tc > 70 else "🟢"
        temp_line = f"\n🌡 טמפרטורה:  {ti} {tc}°C"

    uptime_line = f"{m['uptime_h']:.1f} שעות" if m["uptime_h"] else "—"

    lines = [
        "🖥 *מנטר מערכת — Nexus Master*",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        f"{cpu_c} CPU:   `{cpu_bar}` {m['cpu_pct']:.1f}%",
        f"{ram_c} RAM:   `{ram_bar}` {m['ram_pct']:.0f}%  "
        f"({m['ram_used_gb']} / {m['ram_total_gb']} GB)",
        f"💾 דיסק:  `{disk_bar}` {m['disk_pct']:.0f}%  "
        f"({m['disk_used_gb']} / {m['disk_total_gb']} GB)",
        temp_line,
        f"⏱ Uptime: {uptime_line}",
        f"🐍 Python {m['python']}  |  {m['platform']}",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        "_עדכן עם /sysmon_",
    ]
    return "\n".join(l for l in lines if l)


# ── Log tail ──────────────────────────────────────────────────────────────────

def tail_logs(n: int = 40) -> str:
    """Return the last N lines from master.log + worker.log (interleaved by mtime)."""
    log_files = sorted(LOGS_DIR.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not log_files:
        return "_אין לוגים זמינים_"

    # Take up to 2 most-recent log files
    lines: list[tuple[float, str, str]] = []
    for lf in log_files[:2]:
        try:
            content = lf.read_text(encoding="utf-8", errors="replace").splitlines()
            for raw in content[-n:]:
                lines.append((lf.stat().st_mtime, lf.stem[:6], raw.rstrip()))
        except Exception:
            pass

    # Sort by file mtime (approximate line order)
    selected = [f"[{src}] {ln}" for _, src, ln in lines][-n:]
    if not selected:
        return "_לוגים ריקים_"
    return "\n".join(selected)


# ── Session health ────────────────────────────────────────────────────────────

async def check_sessions_health(api_id: int, api_hash: str) -> dict[str, str]:
    """
    For every .session file under SESSIONS_DIR, attempt a quick Telethon
    connect + GetMe. Returns {stem: "live" | "dead" | "flood" | "error"}.
    """
    results: dict[str, str] = {}
    if not SESSIONS_DIR.exists():
        return {"_error": "ספריית הסשנים לא נמצאה"}

    session_files = list(SESSIONS_DIR.rglob("*.session"))[:30]   # cap at 30
    if not session_files:
        return {"_empty": "לא נמצאו קבצי .session"}

    try:
        from telethon import TelegramClient
        from telethon.errors import (
            AuthKeyUnregisteredError,
            FloodWaitError,
            UserDeactivatedBanError,
        )
    except ImportError:
        return {"_error": "telethon לא מותקן"}

    async def _ping(path: Path) -> tuple[str, str]:
        stem = path.stem
        try:
            client = TelegramClient(str(path.with_suffix("")), api_id, api_hash)
            await asyncio.wait_for(client.connect(), timeout=10)
            if not await client.is_user_authorized():
                await client.disconnect()
                return stem, "dead"
            await asyncio.wait_for(client.get_me(), timeout=8)
            await client.disconnect()
            return stem, "live"
        except FloodWaitError as e:
            return stem, f"flood:{e.seconds}s"
        except (AuthKeyUnregisteredError, UserDeactivatedBanError):
            return stem, "banned"
        except asyncio.TimeoutError:
            return stem, "timeout"
        except Exception as exc:
            return stem, f"error:{str(exc)[:30]}"

    tasks   = [asyncio.create_task(_ping(sf)) for sf in session_files]
    done, _ = await asyncio.wait(tasks, timeout=60)
    for t in done:
        stem, status = t.result()
        results[stem] = status
    return results


def format_sessions(results: dict[str, str]) -> str:
    live  = [k for k, v in results.items() if v == "live"]
    dead  = [k for k, v in results.items() if v == "dead"]
    flood = [k for k, v in results.items() if v.startswith("flood")]
    ban   = [k for k, v in results.items() if v == "banned"]
    err   = [k for k, v in results.items() if v.startswith("error") or v == "timeout"]

    lines = [
        "🔌 *בדיקת תקינות סשנים*",
        f"━━━━━━━━━━━━━━━━━━━━━━━━",
        f"✅ פעיל:  {len(live)}/{len(results)}",
        f"❌ מנותק: {len(dead)}",
        f"🚫 חסום:  {len(ban)}",
        f"⏳ Flood:  {len(flood)}",
        f"⚠️ שגיאה: {len(err)}",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    if dead:
        lines.append("❌ " + ", ".join(dead[:8]))
    if ban:
        lines.append("🚫 " + ", ".join(ban[:8]))
    if flood:
        lines.append("⏳ " + ", ".join(flood[:5]))
    return "\n".join(lines)


# ── Watchdog ──────────────────────────────────────────────────────────────────

def watchdog_status() -> str:
    enabled = _WATCHDOG_FLAG.exists()
    icon    = "✅ פעיל" if enabled else "❌ כבוי"
    return (
        f"🐕 *Watchdog Control*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"סטטוס: {icon}\n\n"
        f"פקודות:\n"
        f"`/watchdog_on`  — הפעל הפעלה אוטומטית\n"
        f"`/watchdog_off` — כבה הפעלה אוטומטית\n\n"
        f"_Watchdog מפעיל מחדש את הבוט והשרת אחרי קריסה_"
    )


def watchdog_enable() -> str:
    _WATCHDOG_FLAG.write_text("1", encoding="utf-8")
    return "✅ *Watchdog הופעל* — המערכת תופעל מחדש אוטומטית אחרי קריסה"


def watchdog_disable() -> str:
    _WATCHDOG_FLAG.unlink(missing_ok=True)
    return "⏹ *Watchdog כובה* — לא תהיה הפעלה אוטומטית עד להפעלה מחדש"
