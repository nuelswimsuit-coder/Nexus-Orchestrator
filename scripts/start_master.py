"""
Master Node entrypoint.

Usage
-----
    python scripts/start_master.py
    nexus-master   (after pip install -e .)

Startup sequence
----------------
1. Logging — structured JSON via structlog.
2. Resource management — OS priority lowered + ResourceGuard background monitor.
3. Vault — secrets manager initialised (EnvVaultBackend by default).
4. Notifications — NotificationService with WhatsAppProvider registered.
5. Dispatcher — connects to Redis, starts real HITL gate listener.
6. Smoke tests:
   a. system.echo  — instant, no approval.
   b. system.sleep — 2 s, no approval.
   c. HITL test    — pauses here until you Approve/Reject in the dashboard.
"""

from __future__ import annotations

import asyncio
import os
import signal as _signal
import sys
import time as _time
from pathlib import Path
from typing import Any

# Windows / Python 3.10+ fix: the default ProactorEventLoop does not support
# all asyncio features used by ARQ.  Switch to SelectorEventLoop and ensure a
# loop exists in the main thread before anything else runs.
if sys.platform == "win32":
    # Required for Windows + Python 3.8+ compatibility with aiohttp/arq
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
elif os.environ.get("ENVIRONMENT", "PRODUCTION").upper() == "PRODUCTION":
    try:
        import uvloop  # type: ignore[import-not-found]

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except Exception:
        pass

try:
    asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

# ── Force-load .env BEFORE any nexus imports so pydantic-settings always
# sees the correct values regardless of working directory.
# This is the fix for "telegram_provider_no_token" when running from a
# directory other than the project root.
_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"
if _ENV_FILE.exists():
    # Manual parse — avoids a python-dotenv dependency while being robust
    for _line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if not _line or _line.startswith("#") or "=" not in _line:
            continue
        _key, _, _val = _line.partition("=")
        _key = _key.strip()
        _val = _val.strip().split("#")[0].strip()  # strip inline comments
        if _key and _key not in os.environ:          # don't override real env vars
            os.environ[_key] = _val

import importlib as _importlib

# Inline import — avoids a circular-import risk; the bot module is scripts-level.
import sys as _sys  # noqa: E402

import structlog  # noqa: E402
from arq.connections import RedisSettings  # noqa: E402

from nexus.master.dispatcher import Dispatcher  # noqa: E402
from nexus.master.hitl_gate import TaskRejectedError  # noqa: E402
from nexus.master.resource_guard import ResourceGuard, apply_low_priority  # noqa: E402
from nexus.master.sentinel import SentinelEngine  # noqa: E402
from nexus.master.services.architect import ArchitectService  # noqa: E402
from nexus.master.services.daily_reporter import DailyPnLReporter  # noqa: E402
from nexus.master.services.decision_engine import AutonomousOrchestrator  # noqa: E402
from nexus.master.services.evolution import EvolutionEngine  # noqa: E402
from nexus.master.services.feedback_loop import FeedbackLoopService  # noqa: E402
from nexus.master.services.polymarket_bot import PolymarketBotService  # noqa: E402
from nexus.master.services.reporting import MultiReportingService, ReportingService  # noqa: E402
from nexus.master.services.scout import ScoutService  # noqa: E402
from nexus.master.services.strategy_brain import StrategyBrainService  # noqa: E402
from nexus.master.services.vault import Vault  # noqa: E402
from nexus.master.supervisor import (
    ProcessSupervisor,  # noqa: E402
    Supervisor,  # noqa: E402
)
from nexus.shared.config import settings  # noqa: E402
from nexus.shared.logging_config import configure_logging  # noqa: E402
from nexus.shared.notifications.providers.telegram import TelegramProvider  # noqa: E402
from nexus.shared.notifications.providers.whatsapp import WhatsAppProvider  # noqa: E402
from nexus.shared.health_monitor import run_openclaw_health_monitor_loop  # noqa: E402
from nexus.shared.paths import get_telefix_path  # noqa: E402
from nexus.shared.reporting import DailyHustleReporter  # noqa: E402
from nexus.shared.system_settings import sync_runtime_from_system_settings  # noqa: E402

_sys.path.insert(0, str(Path(__file__).resolve().parent))  # ensure scripts/ is on path
from nexus.shared.notifications.service import NotificationService  # noqa: E402
from nexus.shared.schemas import TaskPayload  # noqa: E402

log = structlog.get_logger(__name__)


def _dynamic_power_enabled() -> bool:
    return os.getenv("NEXUS_DYNAMIC_POWER", "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


async def _system_settings_sync_loop(
    guard: ResourceGuard,
    interval_s: float = 10.0,
) -> None:
    """
    Keep master runtime synced from config/system_settings.json.
    CPU cap is owned by the power-profile loop when dynamic power is on.
    """
    dyn = _dynamic_power_enabled()
    while True:
        await asyncio.sleep(interval_s)
        dynamic = sync_runtime_from_system_settings(apply_power_limit=not dyn)
        if not dyn:
            guard.cpu_cap = float(dynamic["power_limit"])


async def _power_profile_loop(
    guard: ResourceGuard,
    redis: Any,
    *,
    poll_s: float = 30.0,
    full_reconcile_s: float = 300.0,
) -> None:
    """
    Polls local time / idle / Redis override every ``poll_s`` seconds (default 30s).
    Schedule-based night window is re-evaluated each tick; ``full_reconcile_s`` is
    the minimum interval between *info* logs when nothing changed.
    """
    from nexus.shared.power_profile import (
        REDIS_OVERRIDE_KEY,
        REDIS_POLY_CYCLE_KEY,
        REDIS_SNAPSHOT_KEY,
        apply_power_to_process,
        decide_power_profile,
        snapshot_json,
    )

    pid = os.getpid()
    last_sig: tuple[Any, ...] | None = None
    last_log = 0.0
    first = True
    while True:
        if not first:
            await asyncio.sleep(poll_s)
        first = False
        try:
            raw_ov = await redis.get(REDIS_OVERRIDE_KEY)
            ov = (
                (raw_ov or "auto").strip().lower()
                if isinstance(raw_ov, str)
                else "auto"
            )
            d = decide_power_profile(override_raw=ov)
            sig = (
                d.effective,
                round(d.cpu_cap_percent, 4),
                tuple(d.affinity_cores),
                d.poly5m_cycle_seconds,
                ov,
            )
            now_m = _time.monotonic()
            guard.cpu_cap = float(d.cpu_cap_percent)
            object.__setattr__(settings, "master_cpu_cap_percent", float(d.cpu_cap_percent))
            st = apply_power_to_process(pid, d, set_affinity=True)
            await redis.set(REDIS_POLY_CYCLE_KEY, str(d.poly5m_cycle_seconds), ex=86400)
            await redis.set(
                REDIS_SNAPSHOT_KEY,
                snapshot_json(pid, d, bool(st.get("affinity_ok"))),
                ex=7200,
            )
            changed = sig != last_sig
            last_sig = sig
            if changed or (now_m - last_log) >= full_reconcile_s:
                last_log = now_m
                log.info(
                    "power_profile_applied",
                    effective=d.effective,
                    cpu_cap=d.cpu_cap_percent,
                    affinity_cores=d.affinity_cores,
                    override=d.override,
                    next_shift_s=d.seconds_until_shift,
                )
        except Exception as exc:
            log.warning("power_profile_tick_failed", error=str(exc))


async def run() -> None:
    _dyn = _dynamic_power_enabled()
    sync_runtime_from_system_settings(apply_power_limit=not _dyn)
    # Master brain split: cap total master CPU so API/UI stay responsive (static mode only).
    if (
        not _dyn
        and os.getenv("NEXUS_MASTER_BRAIN_SPLIT", "1").lower() in {"1", "true", "yes", "on"}
    ):
        mgmt_cap = float(os.getenv("NEXUS_MASTER_MANAGEMENT_CPU_PCT", "50"))
        object.__setattr__(
            settings,
            "master_cpu_cap_percent",
            min(float(settings.master_cpu_cap_percent), mgmt_cap),
        )
    # ── 1. Logging ─────────────────────────────────────────────────────────────
    configure_logging(level="INFO", node_id=settings.node_id)
    print("[START] מוודא שאין מופעים כפולים ומריץ את @sasaNexusBot...")
    log.info("nexus_master_starting", node_id=settings.node_id)

    # ── Graceful shutdown event — set by SIGINT / SIGTERM handlers ─────────────
    loop = asyncio.get_running_loop()
    _stop_event = asyncio.Event()

    def _on_signal(signum: int, frame: object) -> None:  # noqa: ARG001
        print("[CLEANUP] סוגר חיבורים ישנים ומתנתק משרתי טלגרם...")
        log.warning("nexus_master_shutdown_signal", signum=signum)
        loop.call_soon_threadsafe(_stop_event.set)

    _signal.signal(_signal.SIGINT, _on_signal)
    try:
        _signal.signal(_signal.SIGTERM, _on_signal)
    except (OSError, AttributeError, ValueError):
        pass  # SIGTERM not available on Windows

    # ── 2. Resource management ─────────────────────────────────────────────────
    apply_low_priority()
    guard = ResourceGuard(
        cpu_cap_percent=settings.master_cpu_cap_percent,
        ram_cap_mb=settings.master_ram_cap_mb,
    )
    asyncio.create_task(guard.monitor(), name="resource-guard")
    asyncio.create_task(_system_settings_sync_loop(guard), name="system-settings-sync")

    # ── 2b. Process supervisor (TERM → 3s → KILL for worker restarts) ──────────
    proc_supervisor = ProcessSupervisor(term_timeout_s=3.0)

    # ── 3. Vault ───────────────────────────────────────────────────────────────
    # Reads secrets from NEXUS_SECRET_<KEY> environment variables by default.
    # Also loads the Mangement Ahu project's .env so Telefix credentials are
    # available to any Worker task that needs them (e.g. telegram.* tasks).
    vault = Vault()

    telefix_env = get_telefix_path("Mangement Ahu") / ".env"
    loaded = vault.load_env_file(
        telefix_env,
        key_mapping={
            "BOT_TOKEN": "TELEFIX_BOT_TOKEN",
            "API_ID":    "TELEFIX_API_ID",
            "API_HASH":  "TELEFIX_API_HASH",
        },
    )
    if loaded:
        # Register which task types get Telefix credentials injected.
        vault.register_task_secrets("telegram", [
            "TELEFIX_BOT_TOKEN",
            "TELEFIX_API_ID",
            "TELEFIX_API_HASH",
        ])

    # Community Factory — Telethon + Gemini/OpenAI when Master dispatches swarm.* tasks
    vault.register_task_secrets("swarm", [
        "TELEFIX_API_ID",
        "TELEFIX_API_HASH",
        "GEMINI_API_KEY",
        "OPENAI_API_KEY",
    ])

    log.info("vault_ready", summary=vault.audit_summary())

    # ── 4. Notifications ───────────────────────────────────────────────────────
    notifier = NotificationService()

    # ── Telegram (primary channel — token is in .env) ──────────────────────────
    # Re-read directly from os.environ to guarantee we see the force-loaded values
    # even if pydantic-settings cached an empty default before the .env was loaded.
    tg_token   = os.environ.get("TELEGRAM_BOT_TOKEN", "") or settings.telegram_bot_token
    tg_chat_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "") or settings.telegram_admin_chat_id
    tg_url     = os.environ.get("TELEGRAM_DASHBOARD_URL", "") or settings.telegram_dashboard_url

    tg_provider = None
    if tg_token and tg_chat_id:
        tg_provider = TelegramProvider(
            bot_token=tg_token,
            admin_chat_id=tg_chat_id,
            dashboard_url=tg_url or "http://localhost:3000",
        )
        notifier.register(tg_provider)
        log.info(
            "telegram_notifications_active",
            chat_id=tg_chat_id,
            token_prefix=tg_token[:12] + "...",
        )

        # ── Boot notification ──────────────────────────────────────────────────
        # If the system was rebooted less than 5 minutes ago, send a Hebrew
        # Telegram message so the operator knows Nexus came back online.
        # Redis de-duplication prevents a duplicate message when Worker also
        # starts within the same boot window.
        from nexus.shared.boot_notifier import check_and_notify_boot  # noqa: PLC0415
        await check_and_notify_boot(
            bot_token=tg_token,
            admin_chat_id=tg_chat_id,
            node_id=settings.node_id,
        )
    elif tg_token:
        log.warning(
            "telegram_outbound_disabled_no_admin_chat",
            hint=(
                "TELEGRAM_ADMIN_CHAT_ID is unset — outbound alerts/boot notify are off. "
                "Command Center polling still starts so /start works once you set the chat id for ACL."
            ),
        )
    else:
        log.warning(
            "telegram_notifications_disabled",
            hint="Set TELEGRAM_BOT_TOKEN (and TELEGRAM_ADMIN_CHAT_ID for alerts) in .env",
        )

    # ── Telegram Command Center (polling) ────────────────────────────────────────
    # Outbound git/Polymarket messages may use TELEGRAM_NEXUS_BOT_TOKEN while the
    # main menu runs on TELEGRAM_BOT_TOKEN; embedded polling must cover both when
    # they differ, otherwise /start on the Nexus bot is never received.
    nexus_tg_token = (os.environ.get("TELEGRAM_NEXUS_BOT_TOKEN") or "").strip()
    if tg_token or nexus_tg_token:
        try:
            _bot_mod = _importlib.import_module("start_telegram_bot")
            if tg_token:
                asyncio.create_task(
                    _bot_mod.start_bot_polling(tg_token),
                    name="telegram-command-center",
                )
            if nexus_tg_token and nexus_tg_token != tg_token:
                asyncio.create_task(
                    _bot_mod.start_nexus_project_bot_polling(nexus_tg_token),
                    name="telegram-nexus-project-bot",
                )
            log.info(
                "telegram_command_center_live",
                hint="INFO: Telegram Command Center is now LIVE and listening.",
                chat_id=tg_chat_id or "(no admin chat — set TELEGRAM_ADMIN_CHAT_ID for alerts)",
                nexus_polling=bool(nexus_tg_token and nexus_tg_token != tg_token),
                supervisor_term_timeout_s=proc_supervisor.term_timeout_s,
            )
        except Exception as _bot_exc:
            log.warning(
                "telegram_command_center_failed",
                error=str(_bot_exc),
                hint="Bot polling could not start.",
            )

    # ── WhatsApp ───────────────────────────────────────────────────────────────
    # If WhatsApp is mock (no Evolution/Twilio credentials) but Telegram IS
    # configured, we skip registering WhatsApp entirely — Telegram already
    # covers all HITL alerts with inline Approve/Reject buttons.
    # If neither is configured, register WhatsApp in mock mode so at least
    # the messages appear in the structlog console output.
    wa_mode = os.environ.get("WHATSAPP_PROVIDER", "") or settings.whatsapp_provider
    if wa_mode != "mock":
        notifier.register(WhatsAppProvider(to_number=settings.whatsapp_to_number))
        log.info("whatsapp_notifications_active", mode=wa_mode, to=settings.whatsapp_to_number)
    elif not tg_provider:
        # No Telegram either — register mock WhatsApp so console shows alerts
        notifier.register(WhatsAppProvider(to_number=settings.whatsapp_to_number))
        log.warning(
            "notifications_mock_only",
            hint=(
                "Both Telegram and WhatsApp are unconfigured. "
                "HITL alerts will only appear in the console. "
                "Set TELEGRAM_BOT_TOKEN + TELEGRAM_ADMIN_CHAT_ID in .env "
                "to receive real notifications."
            ),
        )
    else:
        log.info(
            "whatsapp_skipped_telegram_active",
            reason="WhatsApp is mock — Telegram covers all HITL alerts with inline buttons.",
        )

    # ── 5. Dispatcher ──────────────────────────────────────────────────────────
    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    dispatcher = Dispatcher(
        redis_settings=redis_settings,
        node_id=settings.node_id,
        resource_guard=guard,
        vault=vault,
        notification_service=notifier,
    )
    await dispatcher.start()

    if _dynamic_power_enabled():
        asyncio.create_task(
            _power_profile_loop(
                guard,
                dispatcher._arq,
                poll_s=30.0,
                full_reconcile_s=300.0,
            ),
            name="power-profile-loop",
        )

    from nexus.master.services.retention_guardian_loop import (  # noqa: PLC0415
        run_retention_guardian_loop,
    )

    asyncio.create_task(
        run_retention_guardian_loop(dispatcher),
        name="retention-guardian-loop",
    )

    log.info(
        "nexus_kill_switch_wired",
        module="nexus.shared.kill_switch",
        api="POST /api/system/kill-switch",
        telegram_command="/terminate_nexus_now",
        hint="Set TELEGRAM_ADMIN_USER_ID for secret Telegram trigger; optional NEXUS_KILL_SWITCH_API_TOKEN.",
    )

    # ── 5a. Sentinel AI (Autonomous Error Management) ─────────────────────────
    # The SentinelEngine is the self-healing layer: it monitors Binance latency,
    # RAM, and worker heartbeats, calls Gemini AI to diagnose crashes, and
    # executes recommended actions (restart / cooldown / failover) autonomously.
    # It also exposes its state to the API via app.state.sentinel.
    sentinel = SentinelEngine(
        redis=dispatcher._arq,
        gemini_api_key=os.environ.get("GEMINI_API_KEY", "") or settings.gemini_api_key,
        node_id=settings.node_id,
        dispatcher=dispatcher,
        notifier=notifier,
    )
    asyncio.create_task(sentinel.run_loop(), name="sentinel-ai")
    log.info(
        "sentinel_ai_registered",
        hint="[SENTINEL-AI] מערכת הגנה אוטונומית פעילה — ניטור שגיאות, חביון, וזיכרון.",
    )

    # Expose sentinel on app.state so the API router can read live status
    from nexus.api.main import app as _nexus_app_sentinel  # noqa: PLC0415
    _nexus_app_sentinel.state.sentinel = sentinel

    # ── 5a. Supervisor Watchdog (3-Strikes Auto-Recovery) ──────────────────────
    # The Supervisor monitors the remote Worker (via Redis heartbeat + SSH restart)
    # and any locally-registered processes.  On failure it applies exponential
    # backoff (10 s / 30 s / 60 s) and sends a CRITICAL Telegram alert after 3
    # consecutive crashes inside a 5-minute window.
    # Each recovery runs in its own asyncio task — other workers are unaffected.
    supervisor = Supervisor(
        redis=dispatcher._arq,
        settings=settings,
        telegram_provider=tg_provider,
    )
    # Optionally supervise the Telegram Command Center bot as a local process.
    import sys as _sys_sup
    from pathlib import Path as _Path_sup
    _bot_script = str(_Path_sup(__file__).resolve().parent / "start_telegram_bot.py")
    supervisor.register_local(
        name        = "telegram-bot",
        restart_cmd = [_sys_sup.executable, _bot_script],
        node_id     = settings.node_id,
    )
    await supervisor.start()
    log.info(
        "supervisor_watchdog_registered",
        hint="3-Strikes auto-recovery active. CRITICAL failures escalated to Telegram.",
    )

    # Expose supervisor on the app state so the API router can call manual_reset
    # and serve /api/business/supervisor-status without importing supervisor again.
    from nexus.api.main import app as _nexus_app  # noqa: PLC0415
    _nexus_app.state.supervisor = supervisor

    # ── 5b. Autonomous Orchestrator (5-minute brain loop) ─────────────────────
    # The AutonomousOrchestrator scores all candidate actions every 5 minutes,
    # dispatches the top action automatically if confidence ≥ 60, and writes
    # its reasoning to the Redis agent log (visible in the dashboard terminal).
    #
    # RGB sync:
    #   "calculating" → Master PC pulses Deep Indigo
    #   "dispatching" → Master PC flashes Gold/Yellow
    #   "warning"     → Master PC pulses Red
    orchestrator = AutonomousOrchestrator(
        dispatcher=dispatcher,
        redis=dispatcher._arq,
        notifier=notifier,   # enables STUCK alerts via Telegram
    )
    asyncio.create_task(
        orchestrator.run_loop(interval_seconds=60),
        name="autonomous-orchestrator",
    )
    log.info("autonomous_orchestrator_registered", interval_s=60)

    # ── 5c. Reporting Service (3× daily — 09:00, 14:00, 21:00) ────────────────
    # Generates a Telegram profit + performance report three times per day:
    #   09:00 → Morning briefing   (last  9 h)
    #   14:00 → Afternoon pulse    (last  5 h)
    #   21:00 → Evening report     (last 24 h, full stats)
    # Reports include: Virtual PnL, Win Rate, Auto-Restarts, Node Status,
    # Peak Opportunity (max arbitrage % identified), and Session Health.
    # When sending, writes nexus:report:sending to Redis for 10 s →
    # the dashboard flashes the Master PC RGB Neon Blue.
    reporting = MultiReportingService(
        notifier=notifier,
        redis=dispatcher._arq,
    )
    asyncio.create_task(reporting.run_loop(), name="reporting-service")
    log.info(
        "reporting_service_registered",
        schedule="09:00 (Morning) · 14:00 (Afternoon) · 21:00 (Evening)",
    )

    strategy_brain = StrategyBrainService(redis=dispatcher._arq)
    asyncio.create_task(strategy_brain.run_loop(), name="strategy-brain")
    log.info("strategy_brain_registered", hint="War-room intel + Kelly / swarm strike logic")

    if os.getenv("NEXUS_OPENCLAW_SYNC_MONITOR", "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }:
        asyncio.create_task(
            run_openclaw_health_monitor_loop(dispatcher._arq, notifier),
            name="openclaw-sync-health-monitor",
        )
        log.info(
            "openclaw_sync_monitor_registered",
            hint="Listens for OpenClaw test heartbeat; War Room red alert + admin notify if stale >60m",
        )

    daily_hustle = DailyHustleReporter(notifier=notifier, redis=dispatcher._arq)
    asyncio.create_task(daily_hustle.run_loop(), name="daily-hustle-telegram")
    log.info("daily_hustle_registered", at="00:00 local — operator bottom-line summary")

    # ── 5c-i. Daily PnL — Race to 1,000% (Hebrew Telegram digest) ────────────
    # Local wall-clock default 07:30, or set NEXUS_DAILY_PNL_INTERVAL_S=86400 for
    # fixed-period loop. Disable with NEXUS_DAILY_PNL_REPORT_ENABLED=0.
    if os.getenv("NEXUS_DAILY_PNL_REPORT_ENABLED", "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        daily_pnl_race = DailyPnLReporter(redis=dispatcher._arq, telegram=tg_provider)
        asyncio.create_task(daily_pnl_race.run_loop(), name="daily-pnl-race-report")
        log.info(
            "daily_pnl_race_report_registered",
            hint=(
                "NEXUS_DAILY_PNL_HOUR / NEXUS_DAILY_PNL_MINUTE (local) "
                "or NEXUS_DAILY_PNL_INTERVAL_S"
            ),
        )

    # ── 5c-ii. Evolution Engine — First-Birth Protocol (Phase 14) ─────────────
    # The EvolutionEngine is the Phase 14 Scout+Architect+BirthGate.
    # It runs every 30 minutes, picks the highest-confidence niche from the
    # catalogue, generates a full project scaffold, and either:
    #   - Sends a PROJECT_BIRTH_APPROVAL HITL (first project ever), or
    #   - Auto-deploys if confidence > 80% and first_project_approved is True.
    evolution_engine = EvolutionEngine(
        redis=dispatcher._arq,
        dispatcher=dispatcher,
        notifier=notifier,
    )
    asyncio.create_task(
        evolution_engine.run_loop(interval_seconds=1800),
        name="evolution-engine",
    )
    log.info("evolution_engine_registered", interval_s=1800)

    # ── 5d. Scout + Architect + Feedback Loop (Phase 13) ─────────────────────
    # Scout: scans Google Trends / crypto news every 24h and produces an
    #        "Opportunity Report" stored in Redis.
    # Architect: when an opportunity is approved (or auto-started), scaffolds
    #            a new project folder and deploys it to an idle Worker.
    # FeedbackLoop: monitors project metrics every 10 min and scales Workers
    #               when a project hits the graduation threshold (100 users/2 days).
    gemini_key = os.environ.get("GEMINI_API_KEY", "") or settings.gemini_api_key

    scout = ScoutService(
        redis=dispatcher._arq,
        gemini_api_key=gemini_key,
        interval_hours=24,
    )
    asyncio.create_task(scout.run_loop(), name="scout-service")
    log.info("scout_service_registered", interval_hours=24)

    poly_bot = PolymarketBotService(dispatcher=dispatcher)
    asyncio.create_task(poly_bot.run_loop(), name="polymarket-bot-service")
    log.info("polymarket_bot_service_registered")

    if os.getenv("POLY5M_SCALPER_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}:
        from nexus.master.services.poly_5m_scalper import Poly5mScalperService

        _poly5m = Poly5mScalperService(redis=dispatcher._arq)
        asyncio.create_task(_poly5m.run_loop(), name="poly5m-scalper")
        log.info("poly5m_scalper_registered", env="POLY5M_SCALPER_ENABLED")

    architect = ArchitectService(
        redis=dispatcher._arq,
        dispatcher=dispatcher,
        gemini_api_key=gemini_key,
    )

    feedback = FeedbackLoopService(
        redis=dispatcher._arq,
        architect=architect,
        dispatcher=dispatcher,
    )
    asyncio.create_task(feedback.run_loop(), name="feedback-loop")
    log.info("feedback_loop_registered", interval_s=600)

    # ── 5e. Self-Architect Agent (Phase 9) ────────────────────────────────────
    # Scans codebase every 6 hours, finds issues, generates optimization prompts,
    # and produces the OTP Sessions Creator compatibility report.
    from nexus.master.services.architect_agent import ArchitectAgent
    architect_agent = ArchitectAgent(redis=dispatcher._arq, interval_hours=6)
    asyncio.create_task(architect_agent.run_loop(), name="architect-agent")
    log.info("architect_agent_registered", interval_hours=6)

    # Auto-start any high-confidence opportunities from the last Scout report
    asyncio.create_task(
        _auto_start_opportunities(scout, architect),
        name="auto-start-opportunities",
    )

    # ── 5e. Cron schedule ──────────────────────────────────────────────────────
    # Nightly auto-scrape at 02:00 local time.
    # The CronScheduler is already running (started inside dispatcher.start()).
    nightly_scrape = TaskPayload(
        task_type="telegram.auto_scrape",
        parameters={},
        project_id="telefix",
        priority=3,
    )
    dispatcher.cron.add(hour=2, minute=0, task=nightly_scrape, name="nightly-scrape")
    log.info("cron_nightly_scrape_registered", at="02:00 local")

    nightly_lore = TaskPayload(
        task_type="swarm.lore_nightly",
        parameters={},
        project_id="community-factory",
        priority=3,
    )
    dispatcher.cron.add(hour=2, minute=0, task=nightly_lore, name="nightly-swarm-lore")
    log.info("cron_nightly_swarm_lore_registered", at="02:00 local")

    if os.getenv("NEXUS_SPAMBOT_WEEKLY_CRON_ENABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        try:
            sp_h = int((os.getenv("NEXUS_SPAMBOT_CRON_HOUR") or "4").strip() or "4")
            sp_m = int((os.getenv("NEXUS_SPAMBOT_CRON_MINUTE") or "10").strip() or "10")
        except ValueError:
            sp_h, sp_m = 4, 10
        sp_h = max(0, min(23, sp_h))
        sp_m = max(0, min(59, sp_m))
        spambot_weekly = TaskPayload(
            task_type="management.vault_spambot_weekly",
            parameters={},
            project_id="session_vault",
            priority=3,
        )
        dispatcher.cron.add(
            hour=sp_h,
            minute=sp_m,
            task=spambot_weekly,
            name="vault-spambot-weekly",
        )
        log.info(
            "cron_vault_spambot_weekly_registered",
            at=f"{sp_h:02d}:{sp_m:02d} local",
            note="task self-gates to 7d unless parameters.force=true",
        )

    if os.getenv("NEXUS_POLL_GENERATOR_CRON_ENABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        chat_raw = (os.getenv("POLL_GENERATOR_CHAT") or "").strip()
        if not chat_raw:
            log.warning(
                "cron_poll_generator_skipped",
                reason="POLL_GENERATOR_CHAT is not set",
            )
        else:
            try:
                pg_h = int((os.getenv("NEXUS_POLL_GENERATOR_CRON_HOUR") or "12").strip() or "12")
                pg_m = int((os.getenv("NEXUS_POLL_GENERATOR_CRON_MINUTE") or "0").strip() or "0")
            except ValueError:
                pg_h, pg_m = 12, 0
            pg_h = max(0, min(23, pg_h))
            pg_m = max(0, min(59, pg_m))
            poll_gen_params: dict[str, Any] = {"chat": chat_raw}
            for key, env_name in (
                ("poster_session", "POLL_GENERATOR_POSTER_SESSION"),
                ("sessions_dir", "VAULT_SESSIONS_DIR"),
            ):
                raw = (os.getenv(env_name) or "").strip()
                if raw:
                    poll_gen_params[key] = raw
            poll_generator_task = TaskPayload(
                task_type="swarm.poll_generator",
                parameters=poll_gen_params,
                project_id="swarm-poll",
                priority=3,
            )
            dispatcher.cron.add(
                hour=pg_h,
                minute=pg_m,
                task=poll_generator_task,
                name="swarm-poll-generator-daily",
            )
            log.info(
                "cron_poll_generator_registered",
                at=f"{pg_h:02d}:{pg_m:02d} local",
                note="uses POLL_GENERATOR_POSTER_SESSION or parameters.poster_session",
            )

    if os.getenv("SEO_WATCHDOG_CRON_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}:
        raw_shards = (os.getenv("SEO_WATCHDOG_SHARDS") or "1").strip() or "1"
        try:
            seo_watchdog_shards = max(1, int(raw_shards))
        except ValueError:
            seo_watchdog_shards = 1
        for shard_i in range(seo_watchdog_shards):
            seo_watchdog = TaskPayload(
                task_type="seo.watchdog.audit",
                parameters={
                    "session_start_offset": -1,
                    "session_shard_index": shard_i,
                    "session_shard_total": seo_watchdog_shards,
                },
                project_id="management",
                priority=4,
            )
            dispatcher.cron.add(
                hour=3,
                minute=30,
                task=seo_watchdog,
                name=f"seo-watchdog-audit-shard-{shard_i}-of-{seo_watchdog_shards}",
            )
        log.info(
            "cron_seo_watchdog_registered",
            at="03:30 local",
            shards=seo_watchdog_shards,
        )

    # Swarm Social Synthesis — AI group warmer + community classification
    if os.getenv("SWARM_SOCIAL_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}:
        from nexus.master.services.swarm_social_scheduler import SwarmSocialScheduler

        _swarm_sched = SwarmSocialScheduler(dispatcher, dispatcher._arq)
        asyncio.create_task(_swarm_sched.run_loop(60.0), name="swarm-social-scheduler")
        log.info("swarm_social_scheduler_registered", interval_s=60)

    # Group Factory — private warm-up → public t.me indexing probes (vault state + Redis UI)
    if os.getenv("NEXUS_GROUP_FACTORY_ENABLED", "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        try:
            from src.nexus.services.group_factory import GroupFactoryService

            asyncio.create_task(
                GroupFactoryService(dispatcher._arq).run_loop(300.0),
                name="group-factory",
            )
            log.info("group_factory_service_registered", interval_s=300)
        except Exception as exc:
            log.warning("group_factory_service_failed", error=str(exc))

    # Also update the docstring
    log.info(
        "startup_sequence",
        hint=(
            "Orchestrator runs every 5 min. "
            "Watch the Agent Thinking Log at http://localhost:3000"
        ),
    )

    # ── 6a. Standard smoke tests ───────────────────────────────────────────────
    log.info("dispatching_smoke_tests")

    echo_task = TaskPayload(
        task_type="system.echo",
        parameters={"message": "Hello from the Master Node!"},
        project_id="nexus-demo",
    )
    sleep_task = TaskPayload(
        task_type="system.sleep",
        parameters={"seconds": 2},
        project_id="nexus-demo",
    )

    job_id_echo, job_id_sleep = await asyncio.gather(
        dispatcher.dispatch(echo_task),
        dispatcher.dispatch(sleep_task),
    )
    echo_result, sleep_result = await asyncio.gather(
        dispatcher.get_result(job_id_echo),
        dispatcher.get_result(job_id_sleep),
    )
    log.info("echo_result", result=echo_result.model_dump())
    log.info("sleep_result", result=sleep_result.model_dump())

    # ── 6b. HITL smoke test ────────────────────────────────────────────────────
    # This task requires human approval before the worker executes it.
    # The master PAUSES here until you click Approve or Reject at
    # http://localhost:3000  (the React dashboard).
    # A WhatsApp notification is also sent to WHATSAPP_TO_NUMBER.
    hitl_task = TaskPayload(
        task_type="system.echo",
        parameters={"message": "HITL-approved payload — ran after human sign-off."},
        project_id="nexus-demo",
        requires_approval=True,
        approval_context=(
            "⚠ Smoke test: the worker will echo a message to the task log. "
            "Approve to proceed, or Reject to cancel. "
            "(Master is blocked right now — open http://localhost:3000)"
        ),
    )

    log.info(
        "hitl_smoke_test_waiting",
        hint="Open http://localhost:3000 and approve or reject the pending task.",
    )

    try:
        job_id_hitl = await dispatcher.dispatch(hitl_task)
        hitl_result = await dispatcher.get_result(job_id_hitl)
        log.info("hitl_smoke_test_approved_and_completed", result=hitl_result.model_dump())
    except TaskRejectedError as exc:
        log.info("hitl_smoke_test_rejected", reason=str(exc))
    except TimeoutError:
        log.error("hitl_smoke_test_timed_out")

    # ── 7. Keep master alive ───────────────────────────────────────────────────
    log.info("master_ready", hint="All smoke tests complete. Master is running.")
    try:
        await _stop_event.wait()
    except asyncio.CancelledError:
        pass
    finally:
        print("[CLEANUP] סוגר חיבורים ישנים ומתנתק משרתי טלגרם...")
        await dispatcher.stop()
        log.info("nexus_master_stopped")


async def _auto_start_opportunities(
    scout: ScoutService,
    architect: ArchitectService,
) -> None:
    """
    On startup, check the latest Scout report for any high-confidence
    opportunities that haven't been built yet and auto-start them.
    """
    import asyncio as _asyncio
    await _asyncio.sleep(10)   # let the system settle first

    try:
        report = await scout.get_latest_report()
        if report is None:
            log.info("auto_start_no_report_yet")
            return

        for opp in report.get("opportunities", []):
            if opp.get("auto_start"):
                log.info(
                    "auto_starting_opportunity",
                    niche=opp.get("niche"),
                    confidence=opp.get("confidence"),
                )
                project_id = await architect.build_project(opp)
                log.info("auto_started_project", project_id=project_id, niche=opp.get("niche"))
    except Exception as exc:
        log.error("auto_start_error", error=str(exc))


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
