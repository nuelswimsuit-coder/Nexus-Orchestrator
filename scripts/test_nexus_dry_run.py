"""
Nexus Orchestrator — Global System Dry Run
==========================================

Tests the full notification + HITL + dashboard RGB pipeline in one shot.

What this script does
---------------------
Step 1  Connect to Redis and verify it is reachable.
Step 2  Initialise the notification stack (WhatsApp + Telegram).
Step 3  [NEON BLUE] Send the daily profit report via ReportingService.
        → Dashboard Master PC glows Neon Blue for ~10 s.
        → WhatsApp/Telegram receive the formatted report.
Step 4  [GOLD] Dispatch a dummy telegram.auto_scrape task with
        requires_approval=True via the full Dispatcher pipeline.
        → HitlGate publishes a HitlRequest to Redis.
        → NotificationService fires WhatsApp approval request + Telegram buttons.
        → Dashboard Master PC glows Gold while waiting.
Step 5  Print a live status loop so you can watch the dashboard react.
        The script waits up to 120 s for you to Approve/Reject in the dashboard.
        If no response arrives, it times out gracefully.

Usage
-----
    python scripts/test_nexus_dry_run.py

    # Skip the HITL wait (just test the report):
    python scripts/test_nexus_dry_run.py --report-only

    # Skip the report (just test HITL):
    python scripts/test_nexus_dry_run.py --hitl-only

    # Use a custom HITL timeout (seconds):
    python scripts/test_nexus_dry_run.py --hitl-timeout 60

Prerequisites
-------------
- Redis must be running (REDIS_URL in .env)
- .env must be populated (WHATSAPP_TO_NUMBER, TELEGRAM_BOT_TOKEN, etc.)
- No worker process is required — the HITL task is dispatched to the queue
  but the test does NOT wait for worker execution, only for the HITL decision.
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import sys
import time
from datetime import datetime, timezone

# Windows / Python 3.10+ fix: the default ProactorEventLoop does not support
# all asyncio features used by ARQ.  Switch to SelectorEventLoop and ensure a
# loop exists in the main thread before anything else runs.
if sys.platform == "win32":
    # Required for Windows + Python 3.8+ compatibility with aiohttp/arq
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

try:
    asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

# Force UTF-8 stdout so emoji and Unicode print correctly on Windows terminals.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ── Ensure project root is on sys.path ────────────────────────────────────────
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ── Imports ───────────────────────────────────────────────────────────────────
from nexus.shared.config import settings  # noqa: E402
from nexus.shared.logging_config import configure_logging  # noqa: E402
from nexus.shared.paths import get_telefix_path  # noqa: E402

# ── ANSI colours for terminal output ─────────────────────────────────────────
RESET  = "\033[0m"
BOLD   = "\033[1m"
BLUE   = "\033[94m"
CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
GREY   = "\033[90m"
WHITE  = "\033[97m"


def _banner(text: str, color: str = WHITE) -> None:
    width = 60
    print(f"\n{color}{BOLD}{'=' * width}{RESET}")
    print(f"{color}{BOLD}  {text}{RESET}")
    print(f"{color}{BOLD}{'=' * width}{RESET}")


def _step(n: int, text: str, color: str = CYAN) -> None:
    print(f"\n{color}{BOLD}[Step {n}]{RESET} {text}")


def _ok(text: str) -> None:
    print(f"  {GREEN}✓{RESET}  {text}")


def _warn(text: str) -> None:
    print(f"  {YELLOW}⚠{RESET}  {text}")


def _err(text: str) -> None:
    print(f"  {RED}✗{RESET}  {text}")


def _info(text: str) -> None:
    print(f"  {GREY}›{RESET}  {text}")


# ── Main test coroutine ────────────────────────────────────────────────────────

async def run_dry_run(report_only: bool, hitl_only: bool, hitl_timeout: int) -> None:
    configure_logging(level="WARNING", node_id="dry-run")

    _banner("NEXUS ORCHESTRATOR — GLOBAL DRY RUN", BLUE)
    print(f"  {GREY}Redis:     {settings.redis_url}{RESET}")
    print(f"  {GREY}WA mode:   {settings.whatsapp_provider}{RESET}")
    print(f"  {GREY}WA to:     {settings.whatsapp_to_number}{RESET}")
    print(f"  {GREY}TG token:  {'SET' if settings.telegram_bot_token else 'NOT SET'}{RESET}")
    print(f"  {GREY}TG chat:   {settings.telegram_admin_chat_id or 'NOT SET'}{RESET}")
    print(f"  {GREY}Dashboard: {settings.telegram_dashboard_url}{RESET}")
    print(f"  {GREY}Time:      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}")

    # ── Step 1: Redis connection ───────────────────────────────────────────────
    _step(1, "Connecting to Redis...")
    from redis.asyncio import from_url as redis_from_url
    redis = redis_from_url(settings.redis_url, decode_responses=True)
    try:
        await redis.ping()
        _ok(f"Redis reachable at {settings.redis_url}")
    except Exception as exc:
        _err(f"Redis connection failed: {exc}")
        _err("Start Redis with: docker run -d -p 6379:6379 redis:7-alpine")
        await redis.aclose()
        return

    # ── Step 2: Notification stack ─────────────────────────────────────────────
    _step(2, "Initialising notification providers...")
    from nexus.shared.notifications.providers.telegram import TelegramProvider
    from nexus.shared.notifications.providers.whatsapp import WhatsAppProvider
    from nexus.shared.notifications.service import NotificationService

    notifier = NotificationService()
    wa = WhatsAppProvider(
        to_number=settings.whatsapp_to_number,
        dashboard_url=settings.telegram_dashboard_url,
    )
    notifier.register(wa)
    _ok(f"WhatsApp provider: mode={wa._mode}, to={wa._to}")

    if settings.telegram_bot_token and settings.telegram_admin_chat_id:
        tg = TelegramProvider(
            bot_token=settings.telegram_bot_token,
            admin_chat_id=settings.telegram_admin_chat_id,
            dashboard_url=settings.telegram_dashboard_url,
        )
        notifier.register(tg)
        _ok("Telegram provider: configured")
    else:
        _warn("Telegram provider: NOT configured (set TELEGRAM_BOT_TOKEN + TELEGRAM_ADMIN_CHAT_ID)")

    # ── Step 3: Daily profit report ────────────────────────────────────────────
    if not hitl_only:
        _step(3, f"{BLUE}[NEON BLUE]{RESET} Sending daily profit report...")
        print(f"  {GREY}→ Dashboard Master PC will glow Neon Blue for ~10 s{RESET}")

        from nexus.master.services.reporting import ReportingService
        reporting = ReportingService(
            notifier=notifier,
            redis=redis,
            window_hours=24,
        )

        t0 = time.monotonic()
        try:
            data = await reporting.send_report()
            elapsed = round(time.monotonic() - t0, 1)

            _ok(f"Report sent in {elapsed}s")
            _ok(f"New scraped users (24h): {data.get('new_scraped_users', 0)}")
            _ok(f"Total pipeline:          {data.get('total_pipeline', 0)}")
            _ok(f"Estimated ROI:           {data.get('estimated_roi', 0)}%")
            _ok(f"Active sessions:         {data.get('active_sessions', 0)}")
            _ok(f"Session health:          {data.get('health_ratio', 0):.0f}%")
            _ok(f"DB available:            {data.get('db_available', False)}")

            if wa._mode == "mock":
                _warn("WhatsApp is in MOCK mode — message logged, not sent.")
                _warn("Set WHATSAPP_PROVIDER=twilio or evolution in .env to send for real.")
            else:
                _ok(f"WhatsApp message delivered to {wa._to}")

        except Exception as exc:
            _err(f"Report failed: {exc}")
            import traceback
            traceback.print_exc()

        # Verify the Redis flag was set
        raw = await redis.get("nexus:report:sending")
        if raw:
            _ok("nexus:report:sending key is SET in Redis (dashboard will show Neon Blue)")
        else:
            _info("nexus:report:sending key has already expired (TTL=10s)")

        # Brief pause so you can see the blue glow
        print(f"\n  {BLUE}Holding for 3 s so you can observe the Neon Blue glow...{RESET}")
        await asyncio.sleep(3)

    # ── Step 4: HITL dispatch ──────────────────────────────────────────────────
    if not report_only:
        _step(4, f"{YELLOW}[GOLD]{RESET} Dispatching HITL-flagged scrape task...")
        print(f"  {GREY}→ Dashboard Master PC will glow Gold while waiting{RESET}")
        print(
            f"  {GREY}→ WhatsApp approval request will be sent to "
            f"{settings.whatsapp_to_number}{RESET}"
        )
        print(f"  {GREY}→ Telegram bot will send Approve/Reject buttons{RESET}")

        from arq.connections import RedisSettings

        from nexus.master.dispatcher import Dispatcher
        from nexus.master.resource_guard import ResourceGuard
        from nexus.master.services.vault import Vault
        from nexus.shared.schemas import TaskPayload

        vault = Vault()
        guard = ResourceGuard(cpu_cap_percent=80, ram_cap_mb=4096)

        redis_settings = RedisSettings.from_dsn(settings.redis_url)
        dispatcher = Dispatcher(
            redis_settings=redis_settings,
            node_id="dry-run-master",
            resource_guard=guard,
            vault=vault,
            notification_service=notifier,
        )
        await dispatcher.start()
        _ok("Dispatcher started")

        # Create a HITL-flagged task
        hitl_task = TaskPayload(
            task_type="telegram.auto_scrape",
            parameters={"sources": [], "force": False},
            project_id="nexus-dry-run",
            requires_approval=True,
            approval_context=(
                "🧪 DRY RUN TEST: This is a simulated scrape task. "
                "Approve to verify the full HITL pipeline works end-to-end. "
                "Reject to cancel. No actual scraping will occur."
            ),
        )

        print(f"\n  {YELLOW}Task ID: {hitl_task.task_id}{RESET}")
        print(f"  {GREY}Dispatching — this will block until you respond in the dashboard...{RESET}")
        print(f"  {GREY}Open: {settings.telegram_dashboard_url}{RESET}")
        print(f"  {GREY}Timeout: {hitl_timeout}s{RESET}\n")

        # Set engine state to "dispatching" for Gold RGB
        engine_state_payload = json.dumps({
            "state": "dispatching",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
        await redis.set("nexus:engine:state", engine_state_payload, ex=hitl_timeout + 30)
        _ok("Engine state → 'dispatching' (Gold RGB active)")

        # Dispatch with a timeout
        try:
            dispatch_task = asyncio.create_task(
                dispatcher.dispatch(hitl_task),
                name="dry-run-hitl-dispatch",
            )

            # Live status loop while waiting
            start_wait = time.monotonic()
            while not dispatch_task.done():
                elapsed = int(time.monotonic() - start_wait)
                remaining = hitl_timeout - elapsed
                if remaining <= 0:
                    dispatch_task.cancel()
                    break
                status_line = (
                    f"  {YELLOW}⏳ Waiting for HITL response... "
                    f"{elapsed}s elapsed / {remaining}s remaining{RESET}"
                )
                print(status_line, end="\r", flush=True)
                await asyncio.sleep(1)

            print()  # newline after the \r loop

            if dispatch_task.cancelled():
                _warn(f"HITL timed out after {hitl_timeout}s — no response received.")
                _warn("This is expected if no dashboard/bot is running.")
            else:
                exc = dispatch_task.exception()
                if exc is not None:
                    from nexus.master.hitl_gate import TaskRejectedError
                    if isinstance(exc, TaskRejectedError):
                        _ok(f"HITL REJECTED — {exc}")
                    elif isinstance(exc, asyncio.TimeoutError):
                        _warn("HITL timed out (no response within HITL_APPROVAL_TIMEOUT)")
                    else:
                        _err(f"Dispatch error: {exc}")
                else:
                    job_id = dispatch_task.result()
                    _ok(f"HITL APPROVED — task enqueued as job_id={job_id}")
                    _ok("Task is now in the ARQ queue for a worker to pick up.")

        except Exception as exc:
            _err(f"Unexpected error: {exc}")
            import traceback
            traceback.print_exc()
        finally:
            # Clear engine state
            await redis.delete("nexus:engine:state")
            await dispatcher.stop()
            _ok("Dispatcher stopped, engine state cleared")

    # ── Step 5: Final summary ──────────────────────────────────────────────────
    _banner("DRY RUN COMPLETE", GREEN)

    checks = []

    # Check Redis keys
    report_raw = await redis.get("nexus:report:last")
    checks.append(("Daily report stored in Redis",    report_raw is not None))

    wa_configured = wa._mode != "mock"
    checks.append(("WhatsApp live mode configured",   wa_configured))

    tg_configured = settings.telegram_bot_token and settings.telegram_admin_chat_id
    checks.append(("Telegram bot configured",         bool(tg_configured)))

    db_path = get_telefix_path("Mangement Ahu") / "data" / "telefix.db"
    checks.append(("Telefix DB accessible",           db_path.exists()))

    for label, ok in checks:
        if ok:
            _ok(label)
        else:
            _warn(f"{label}  ← action needed")

    print(f"\n  {GREY}Next steps:{RESET}")
    if not wa_configured:
        print(f"  {GREY}  • Set WHATSAPP_PROVIDER=twilio (or evolution) in .env{RESET}")
        print(
            f"  {GREY}  • Set TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN"
            f" / TWILIO_WHATSAPP_FROM{RESET}"
        )
    if not tg_configured:
        print(f"  {GREY}  • Set TELEGRAM_BOT_TOKEN in .env{RESET}")
        print(f"  {GREY}  • Set TELEGRAM_ADMIN_CHAT_ID in .env{RESET}")
    print(f"  {GREY}  • Start the dashboard: cd frontend && npm run dev{RESET}")
    print(f"  {GREY}  • Start the API:       python scripts/start_api.py{RESET}")
    print(f"  {GREY}  • Start the master:    python scripts/start_master.py{RESET}")
    print(f"  {GREY}  • Start a worker:      python scripts/start_worker.py{RESET}")
    print()

    await redis.aclose()


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Nexus Orchestrator — Global System Dry Run",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Only run the profit report test (skip HITL dispatch)",
    )
    parser.add_argument(
        "--hitl-only",
        action="store_true",
        help="Only run the HITL dispatch test (skip report)",
    )
    parser.add_argument(
        "--hitl-timeout",
        type=int,
        default=120,
        metavar="SECONDS",
        help="How long to wait for a HITL response (default: 120 s)",
    )
    args = parser.parse_args()

    if args.report_only and args.hitl_only:
        print("Error: --report-only and --hitl-only are mutually exclusive.")
        sys.exit(1)

    asyncio.run(run_dry_run(
        report_only=args.report_only,
        hitl_only=args.hitl_only,
        hitl_timeout=args.hitl_timeout,
    ))


if __name__ == "__main__":
    main()
