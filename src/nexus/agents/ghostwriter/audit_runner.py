"""
Audit Runner — CLI entrypoint for the full session audit pipeline:
  1. Scan entire machine for .session files (or use existing staging)
  2. Connect to each account via rotating residential proxies
  3. Collect groups/channels/bots owned + member counts + premium counts
  4. Export sorted Excel report

Usage:
    python -m src.nexus.agents.ghostwriter.audit_runner --proxies proxies.txt
    python -m src.nexus.agents.ghostwriter.audit_runner --proxies proxies.txt --limit 100 --only-israeli
    python -m src.nexus.agents.ghostwriter.audit_runner --proxies proxies.txt --sessions-dir vault/sessions
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import os
import shutil
import sys
import tempfile
import time
import threading
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Force UTF-8 on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

_env_path = Path(__file__).resolve().parents[4] / ".env"
load_dotenv(dotenv_path=_env_path, override=False)

# ── ANSI colours ──────────────────────────────────────────────────────────────
RESET   = "\033[0m";  BOLD    = "\033[1m"
GREEN   = "\033[92m"; YELLOW  = "\033[93m"
RED     = "\033[91m"; CYAN    = "\033[96m"
DIM     = "\033[2m";  MAGENTA = "\033[95m"
BLUE    = "\033[94m"; WHITE   = "\033[97m"

# ANSI cursor / screen control
HIDE_CURSOR  = "\033[?25l"
SHOW_CURSOR  = "\033[?25h"
SAVE_CUR     = "\033[s"
REST_CUR     = "\033[u"
ERASE_LINE   = "\033[2K"
MOVE_UP      = "\033[{n}A"

BANNER = f"""{CYAN}{BOLD}
+----------------------------------------------------------+
|   [IL] Nexus Session Auditor  --  Full Account Inspector |
|        Groups / Channels / Premium / Proxy Rotation      |
+----------------------------------------------------------+
{RESET}"""

LEVEL_COLOURS = {"info": GREEN, "warning": YELLOW, "error": RED, "debug": DIM}

# ── Split-screen state ────────────────────────────────────────────────────────

_PANEL_LINES = 7          # height of the bottom stats panel (excluding separator)
_panel_lock  = threading.Lock()
_panel_active = False     # set to True once _reserve_panel() is called

# Live counters (updated by progress callback, read by panel renderer)
_stats: dict = {
    "total": 0, "done": 0,
    "ok": 0, "err": 0, "banned": 0, "dead": 0,
    "last_phone": "",
    "start_ts": time.monotonic(),
}

SPEED_PRESETS: dict[str, dict[str, float | int]] = {
    # Conservative profile for lower Telegram pressure.
    "safe": {
        "min_delay": 0.5,
        "max_delay": 1.5,
        "concurrency_base": 6,
        "pause_scale": 0.6,
        "premium_cap": 2000,
    },
    # New default: significantly faster while keeping some jitter.
    "fast": {
        "min_delay": 0.05,
        "max_delay": 0.35,
        "concurrency_base": 16,
        "pause_scale": 0.2,
        "premium_cap": 500,
    },
    # Maximum throughput; disables premium member crawling by default.
    "turbo": {
        "min_delay": 0.0,
        "max_delay": 0.1,
        "concurrency_base": 28,
        "pause_scale": 0.05,
        "premium_cap": 0,
    },
}


def _term_size() -> tuple[int, int]:
    s = shutil.get_terminal_size((120, 40))
    return s.columns, s.lines


def _panel_row(col: int, row: int) -> str:
    """ANSI: move cursor to absolute row/col (1-based)."""
    return f"\033[{row};{col}H"


def _render_panel() -> None:
    """Redraw the fixed bottom stats panel using absolute cursor positioning."""
    if not _panel_active:
        return
    s        = _stats
    total    = s["total"] or 1
    done     = s["done"]
    ok       = s["ok"]
    err      = s["err"]
    banned   = s["banned"]
    dead     = s["dead"]
    elapsed  = time.monotonic() - s["start_ts"]
    rate     = done / elapsed if elapsed > 0 else 0
    remaining = (total - done) / rate if rate > 0 else 0
    pct      = done / total * 100
    w, h     = _term_size()

    bar_w    = max(w - 12, 10)
    filled   = int(pct / 100 * bar_w)
    bar      = f"{GREEN}{'█' * filled}{DIM}{'░' * (bar_w - filled)}{RESET}"

    eta_str  = f"{int(remaining // 60)}m{int(remaining % 60):02d}s" if rate > 0 else "--:--"
    rate_str = f"{rate:.1f}/s"

    sep   = f"{DIM}{'─' * w}{RESET}"
    title = (
        f"{CYAN}{BOLD}  ╔══ NEXUS AUDIT DASHBOARD ══╗{RESET}"
        f"  {WHITE}Sessions: {done}/{total}{RESET}"
    )
    row1 = (
        f"  {GREEN}{BOLD}✓ Connected : {ok:<7}{RESET}"
        f"  {RED}{BOLD}✗ Error     : {err:<7}{RESET}"
        f"  {YELLOW}{BOLD}⚠ Banned    : {banned:<7}{RESET}"
        f"  {DIM}☠ Dead      : {dead:<7}{RESET}"
    )
    row2 = (
        f"  {CYAN}Progress: {pct:5.1f}%   "
        f"Rate: {rate_str:<8}  ETA: {eta_str}{RESET}"
    )
    row3 = f"  {DIM}Last audited: {s['last_phone']}{RESET}"

    panel_lines = [sep, title, row1, row2, f"  [{bar}]", row3, sep]

    # Panel occupies the last _PANEL_LINES+1 rows of the terminal
    panel_start_row = h - _PANEL_LINES  # first row of panel (1-based)

    out = []
    out.append(SAVE_CUR)
    for i, line in enumerate(panel_lines):
        out.append(_panel_row(1, panel_start_row + i))
        out.append(f"{ERASE_LINE}{line}")
    out.append(REST_CUR)
    sys.stdout.write("".join(out))
    sys.stdout.flush()


def _reserve_panel() -> None:
    """
    Set up the scrolling region to exclude the bottom panel rows,
    then draw the panel for the first time.
    """
    global _panel_active
    w, h = _term_size()
    scroll_bottom = h - _PANEL_LINES - 1   # last row of the scroll region

    # Set scrolling region: rows 1 .. scroll_bottom
    sys.stdout.write(f"\033[1;{scroll_bottom}r")
    # Move cursor to bottom of scroll region so new log lines push upward
    sys.stdout.write(_panel_row(1, scroll_bottom))
    sys.stdout.flush()
    _panel_active = True
    _render_panel()


def _teardown_panel() -> None:
    """Restore full-screen scrolling region and show cursor."""
    _, h = _term_size()
    sys.stdout.write(f"\033[1;{h}r")   # reset scroll region
    sys.stdout.write(_panel_row(1, h - _PANEL_LINES - 2))  # move above panel
    sys.stdout.write(SHOW_CURSOR)
    sys.stdout.flush()


# ── Logging that stays above the panel ───────────────────────────────────────

def live_log(msg: str, level: str = "info") -> None:
    c  = LEVEL_COLOURS.get(level, "")
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"{DIM}[{ts}]{RESET} {c}{BOLD}{level.upper():<7}{RESET} {msg}"
    with _panel_lock:
        if _panel_active:
            # Print inside the scroll region — terminal auto-scrolls the log area
            w, h = _term_size()
            scroll_bottom = h - _PANEL_LINES - 1
            out = []
            out.append(SAVE_CUR)
            out.append(_panel_row(1, scroll_bottom))
            out.append(f"\n{ERASE_LINE}{line}")
            out.append(REST_CUR)
            sys.stdout.write("".join(out))
        else:
            print(line)
        sys.stdout.flush()


# ── Progress callback — called after each account completes ──────────────────

def _progress(current: int, total: int, audit) -> None:
    with _panel_lock:
        _stats["total"]      = total
        _stats["done"]       = current
        _stats["last_phone"] = audit.phone
        if audit.is_banned:
            _stats["banned"] += 1
        elif audit.is_unregistered:
            _stats["dead"] += 1
        elif audit.error:
            _stats["err"] += 1
        else:
            _stats["ok"] += 1
        _render_panel()


# ── Session discovery ─────────────────────────────────────────────────────────

def _collect_sessions(
    sessions_dir: str | None,
    use_scan: bool,
    only_israeli: bool,
    limit: int,
) -> list[Path]:
    from .session_scanner import SessionScanner

    if sessions_dir:
        d = Path(sessions_dir)
        if not d.is_absolute():
            d = Path(__file__).resolve().parents[4] / d
        files = list(d.glob("*.session"))
    elif use_scan:
        staging = Path(tempfile.gettempdir()) / "nexus_sessions_staging"
        scanner = SessionScanner(staging_dir=staging, log=live_log)
        result  = scanner.scan()
        scanner.print_report(result)
        files   = [s.session_path for s in result.sessions]
    else:
        # Default: use existing staging + vault/sessions
        staging = Path(tempfile.gettempdir()) / "nexus_sessions_staging"
        vault   = Path(__file__).resolve().parents[4] / "vault" / "sessions"
        files   = list(staging.glob("*.session")) + list(vault.glob("*.session"))
        # Deduplicate by stem
        seen: set[str] = set()
        unique: list[Path] = []
        for f in files:
            if f.stem not in seen:
                seen.add(f.stem)
                unique.append(f)
        files = unique

    if only_israeli:
        files = [f for f in files if f.stem.startswith("972") or f.stem.startswith("+972")]

    # Israeli first
    israeli = [f for f in files if f.stem.startswith("972") or f.stem.startswith("+972")]
    others  = [f for f in files if f not in set(israeli)]
    files   = israeli + others

    if limit:
        files = files[:limit]

    return files


# ── Main ──────────────────────────────────────────────────────────────────────

def _load_checkpoint(cp_path: Path) -> tuple[list, set[str]]:
    """
    Load a checkpoint JSON file.

    Returns (list_of_audit_dicts, set_of_done_phones).
    """
    if not cp_path.exists():
        return [], set()
    try:
        data = json.loads(cp_path.read_text(encoding="utf-8"))
        done_phones = {str(entry.get("phone", "")) for entry in data if entry.get("phone")}
        live_log(
            f"Checkpoint loaded: {cp_path} — {len(done_phones)} accounts already done",
            "info",
        )
        return data, done_phones
    except Exception as exc:
        live_log(f"Failed to load checkpoint {cp_path}: {exc}", "warning")
        return [], set()


def _rebuild_audits_from_checkpoint(data: list) -> list:
    """Reconstruct AccountAudit objects from checkpoint JSON."""
    from .session_auditor import AccountAudit, EntityAudit
    audits = []
    for entry in data:
        name_parts = (entry.get("name") or "").split(" ", 1)
        audit = AccountAudit(
            session_path=entry.get("session", ""),
            phone=entry.get("phone", ""),
            username=entry.get("username", ""),
            first_name=name_parts[0] if name_parts else "",
            last_name=name_parts[1] if len(name_parts) > 1 else "",
            is_premium=bool(entry.get("premium")),
            is_banned=bool(entry.get("banned")),
            is_unregistered=bool(entry.get("unregistered")),
            error=entry.get("error") or "",
        )
        for e in entry.get("entities", []):
            audit.entities.append(EntityAudit(
                entity_id=int(e.get("id") or 0),
                title=e.get("title", ""),
                entity_type=e.get("type", "group"),
                username="",
                role=e.get("role", ""),
                member_count=int(e.get("members") or 0),
                premium_real=int(e.get("premium_real") or 0),
                premium_boosts=int(e.get("premium_boosts") or 0),
                invite_link=e.get("link", ""),
                is_israeli=bool(e.get("is_israeli")),
            ))
        audits.append(audit)
    return audits


async def run(args: argparse.Namespace) -> None:
    from .proxy_rotator import ProxyRotator
    from .session_auditor import SessionAuditor, _parse_filter_keywords
    from .excel_reporter import generate_report

    profile = SPEED_PRESETS[args.speed]
    api_id   = int(os.getenv("TELEGRAM_API_ID") or "0")
    api_hash = os.getenv("TELEGRAM_API_HASH") or ""
    _stats["start_ts"] = time.monotonic()

    if not api_id or not api_hash:
        live_log("TELEGRAM_API_ID / TELEGRAM_API_HASH not set in .env", "error")
        sys.exit(1)

    # ── Checkpoint path (auto-default so saving always happens) ───────────────
    cp_path: Path
    if args.checkpoint:
        cp_path = Path(args.checkpoint)
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        cp_path = Path(f"audit_checkpoint_{ts}.json")

    # ── Resume: load existing checkpoint if --resume or if file already exists ─
    existing_audits: list = []
    done_phones: set[str] = set()

    if args.resume and cp_path.exists():
        raw_data, done_phones = _load_checkpoint(cp_path)
        existing_audits = _rebuild_audits_from_checkpoint(raw_data)
        live_log(
            f"{GREEN}{BOLD}Resuming from checkpoint:{RESET} {cp_path} "
            f"— {len(done_phones)} accounts already done, skipping them",
            "info",
        )
    elif not args.resume and cp_path.exists():
        live_log(
            f"{YELLOW}Checkpoint file exists at {cp_path}. "
            f"Use --resume to continue from it, or delete it to start fresh.{RESET}",
            "warning",
        )

    # ── Proxies ───────────────────────────────────────────────────────────────
    if args.proxies and Path(args.proxies).exists():
        rotator = ProxyRotator.from_file(args.proxies, mode=args.proxy_mode)
        live_log(f"Loaded {rotator.count} proxies from {args.proxies}", "info")
    elif args.proxy:
        rotator = ProxyRotator.from_list(args.proxy, mode=args.proxy_mode)
        live_log(f"Using {rotator.count} inline proxies", "info")
    else:
        rotator = ProxyRotator([])
        live_log("No proxies provided — running WITHOUT proxy (risky!)", "warning")

    # ── Effective speed config (CLI override > speed preset) ──────────────────
    effective_min_delay = args.min_delay if args.min_delay is not None else float(profile["min_delay"])
    effective_max_delay = args.max_delay if args.max_delay is not None else float(profile["max_delay"])
    if effective_max_delay < effective_min_delay:
        effective_min_delay, effective_max_delay = effective_max_delay, effective_min_delay

    if args.concurrency is not None:
        effective_concurrency = max(1, args.concurrency)
    else:
        base = int(profile["concurrency_base"])
        # Scale with proxy pool when available (avoids leaving proxy capacity idle).
        effective_concurrency = max(base, rotator.count * 4) if rotator.available else base

    effective_premium_cap = args.premium_cap if args.premium_cap is not None else int(profile["premium_cap"])
    effective_pause_scale = max(0.0, args.pause_scale if args.pause_scale is not None else float(profile["pause_scale"]))
    live_log(
        "Speed profile="
        f"{CYAN}{BOLD}{args.speed}{RESET} | "
        f"concurrency={effective_concurrency} | "
        f"stagger={effective_min_delay:.2f}-{effective_max_delay:.2f}s | "
        f"intra_pause_scale={effective_pause_scale:.2f} | "
        f"premium_cap={effective_premium_cap}",
        "info",
    )

    # ── Sessions ──────────────────────────────────────────────────────────────
    all_sessions = _collect_sessions(
        sessions_dir=args.sessions_dir,
        use_scan=args.scan,
        only_israeli=args.only_israeli,
        limit=args.limit,
    )

    # Filter out already-done sessions when resuming
    if done_phones:
        sessions = [s for s in all_sessions if s.stem not in done_phones]
        live_log(
            f"Sessions to audit: {CYAN}{BOLD}{len(sessions)}{RESET} "
            f"(skipping {len(all_sessions) - len(sessions)} already done)",
            "info",
        )
    else:
        sessions = all_sessions
        live_log(f"Sessions to audit: {CYAN}{BOLD}{len(sessions)}{RESET}", "info")

    if not sessions and not existing_audits:
        live_log("No sessions found. Run --scan first or specify --sessions-dir.", "error")
        sys.exit(1)

    # ── Initialise split-screen panel ─────────────────────────────────────────
    total_to_run = len(sessions) + len(existing_audits)
    _stats["total"] = total_to_run
    _stats["done"]  = len(existing_audits)
    # Seed counters from already-done checkpoint data
    _stats["ok"]     = sum(1 for a in existing_audits if not a.error and not a.is_banned and not a.is_unregistered)
    _stats["banned"] = sum(1 for a in existing_audits if a.is_banned)
    _stats["dead"]   = sum(1 for a in existing_audits if a.is_unregistered)
    _stats["err"]    = sum(1 for a in existing_audits if a.error and not a.is_banned and not a.is_unregistered)
    _reserve_panel()

    if not sessions:
        live_log("All sessions already audited — generating report from checkpoint.", "info")
        audits = existing_audits
    else:
        # ── Audit ─────────────────────────────────────────────────────────────
        filter_kw = _parse_filter_keywords(getattr(args, "filter", None))
        if filter_kw:
            live_log(
                f"Entity filter active ({len(filter_kw)} keywords): "
                f"{', '.join(filter_kw[:8])}{'…' if len(filter_kw) > 8 else ''}",
                "info",
            )

        auditor = SessionAuditor(
            api_id=api_id,
            api_hash=api_hash,
            proxy_rotator=rotator,
            log=live_log,
            delay_between_accounts=(effective_min_delay, effective_max_delay),
            concurrency=effective_concurrency,
            premium_scan_cap=effective_premium_cap,
            pause_scale=effective_pause_scale,
            filter_keywords=filter_kw,
        )

        audits = await auditor.audit_all(
            sessions,
            progress_callback=_progress,
            checkpoint_path=cp_path,
            existing_results=existing_audits,
        )

    live_log(f"Checkpoint saved: {cp_path}", "info")

    # ── Excel report ──────────────────────────────────────────────────────────
    out_path = Path(args.output) if args.output else None
    report_path = generate_report(audits, out_path)
    live_log(f"\n{GREEN}{BOLD}Report saved: {report_path}{RESET}", "info")

    # ── Tear down split-screen panel before printing final summary ────────────
    _teardown_panel()

    # ── Quick summary ─────────────────────────────────────────────────────────
    working = sum(1 for a in audits if not a.error and not a.is_banned and not a.is_unregistered)
    banned  = sum(1 for a in audits if a.is_banned)
    dead    = sum(1 for a in audits if a.is_unregistered)
    all_ent = [e for a in audits for e in a.entities]

    print(f"""
{CYAN}{BOLD}+---------------------- FINAL SUMMARY ----------------------+{RESET}
  Total audited    : {len(audits)}
  {GREEN}Working          : {working}{RESET}
  {RED}Banned           : {banned}{RESET}
  {DIM}Dead/Unreg       : {dead}{RESET}
  Groups/Channels  : {len(all_ent)}
  Total members    : {sum(e.member_count for e in all_ent):,}
  Premium (real)   : {sum(e.premium_real for e in all_ent):,}
  Premium (boosts) : {sum(e.premium_boosts for e in all_ent):,}
{CYAN}{BOLD}+-----------------------------------------------------------+{RESET}
    """)

    # ── Auto-classify active sessions ─────────────────────────────────────────
    if not args.skip_classify:
        live_log("Starting classification of active sessions...", "info")
        from .session_classifier import SessionClassifier
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        classify_out = Path(args.classify_output or f"classified_{ts}")
        classifier = SessionClassifier(output_dir=classify_out, log=live_log)
        classifier.classify_from_audit(audits)


def setup_args() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Nexus Session Auditor — full account + group inspector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--proxies",       help="Path to proxy list file (one per line)")
    parser.add_argument("--proxy",         nargs="*", help="Inline proxy strings")
    parser.add_argument("--proxy-mode",    default="round_robin", choices=["round_robin", "random"])
    parser.add_argument("--sessions-dir",  help="Specific folder of .session files")
    parser.add_argument("--scan",          action="store_true", help="Run full disk scan first")
    parser.add_argument("--only-israeli",  action="store_true", help="Only audit +972 accounts")
    parser.add_argument("--limit",         type=int, default=0, help="Max sessions to audit (0=all)")
    parser.add_argument(
        "--filter",
        type=str,
        help="Keywords to filter groups (e.g., ישראל,חדשות) — comma-separated, match title or @username",
    )
    parser.add_argument("--speed",         choices=["safe", "fast", "turbo"], default="fast",
                        help="Throughput profile (default: fast)")
    parser.add_argument("--min-delay",     type=float, default=None,
                        help="Override min stagger between account starts (seconds)")
    parser.add_argument("--max-delay",     type=float, default=None,
                        help="Override max stagger between account starts (seconds)")
    parser.add_argument("--concurrency",   type=int, default=None,
                        help="Override parallel accounts to audit")
    parser.add_argument("--premium-cap",   type=int, default=None,
                        help="Override max members scanned for premium per entity (0=skip)")
    parser.add_argument("--pause-scale",   type=float, default=None,
                        help="Override internal per-account pause multiplier (0 disables pauses)")
    parser.add_argument("--timeout",       type=int,   default=60,   help="Per-account timeout in seconds (default: 60)")
    parser.add_argument("--output",          help="Output Excel file path")
    parser.add_argument("--checkpoint",      help="Checkpoint JSON file path (auto-created if not set)")
    parser.add_argument("--resume",          action="store_true",
                        help="Resume from existing checkpoint file (skip already-audited accounts)")
    parser.add_argument("--skip-classify",   action="store_true",
                        help="Skip auto-classification after audit")
    parser.add_argument("--classify-output", help="Output folder for classified sessions (default: classified_TIMESTAMP)")
    return parser


def main() -> None:
    args = setup_args().parse_args()

    # Hide cursor for clean split-screen rendering
    sys.stdout.write(HIDE_CURSOR)
    sys.stdout.flush()

    print(BANNER)
    # Propagate --timeout to session_auditor via env var before asyncio.run
    import os as _os
    _os.environ["AUDIT_ACCOUNT_TIMEOUT"] = str(args.timeout)
    try:
        asyncio.run(run(args))
    finally:
        _teardown_panel()


if __name__ == "__main__":
    main()
