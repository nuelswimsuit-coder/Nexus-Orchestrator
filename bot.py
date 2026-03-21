"""
Primary operator entrypoint — loads environment, logs startup milestones,
then runs the Nexus lite stack (FastAPI + Telegram polling).

Run from the repository root::

    python bot.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path
from typing import NoReturn

# Repository root must be importable before `nexus.*` and `scripts.*`.
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config import ConfigurationError, bootstrap_environment, get_env  # noqa: E402


def _configure_logging(verbose: bool) -> logging.Logger:
    level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )
    return logging.getLogger("nexus.bot")


def main() -> None:
    verbose = "--quiet" not in sys.argv
    log = _configure_logging(verbose)

    try:
        from core.behavioral_analyzer import readiness_from_flags
        from core.scanner import probe_http_ok, verify_redis

        repo_root = bootstrap_environment()
        log.info("Bot is starting... (root=%s)", repo_root)

        from nexus.shared.config import settings

        redis_check = asyncio.run(verify_redis(settings.redis_url))
        if redis_check.get("ok"):
            log.info("Redis broker reachable (%s)", redis_check.get("redis_url_host"))
        else:
            log.warning(
                "Redis check failed: %s — ensure Redis is running before relying on workers.",
                redis_check.get("error") or "no response",
            )

        token_present = bool(get_env("TELEGRAM_BOT_TOKEN") or settings.telegram_bot_token)
        gemini_present = bool(get_env("GEMINI_API_KEY") or settings.gemini_api_key)
        openai_present = bool(get_env("OPENAI_API_KEY"))

        report = readiness_from_flags(
            {"telegram": token_present, "gemini": gemini_present, "openai": openai_present}
        )
        log.info("Configuration snapshot: %s", report.summary)

        if not token_present:
            raise ConfigurationError(
                "TELEGRAM_BOT_TOKEN is not set. Add it to .env or set TELEGRAM_BOT_TOKEN "
                "in your environment."
            )

        if gemini_present:
            log.info("Gemini AI ready (API key present).")
        else:
            log.warning("Gemini API key not set; content features that need Gemini will fail.")

        if openai_present:
            log.info("OpenAI API key present (used when LLM tasks require it).")
        else:
            log.info(
                "OPENAI_API_KEY not set — optional unless you dispatch tasks that need OpenAI."
            )

        from scripts.nexus_lite import run as run_lite

        async def after_api_ready() -> None:
            host = "localhost" if settings.api_host == "0.0.0.0" else settings.api_host
            base = f"http://{host}:{settings.api_port}"
            doc_probe = await probe_http_ok(f"{base}/docs")
            if doc_probe.get("ok"):
                log.info("Control plane API up (GET /docs → %s).", doc_probe.get("status_code"))
            else:
                log.warning("Could not reach API docs yet: %s", doc_probe.get("error"))

        async def before_telegram_poll() -> None:
            log.info("Connected to Telegram (starting long-poll)...")

        async def run_with_hooks() -> None:
            await run_lite(
                verbose=verbose,
                after_api_ready=after_api_ready,
                before_telegram_poll=before_telegram_poll,
            )

        asyncio.run(run_with_hooks())

    except KeyboardInterrupt:
        log.info("Shutdown requested by user.")
    except ConfigurationError as exc:
        log.error("Configuration error: %s", exc)
        sys.exit(1)
    except Exception as exc:
        log.exception("Fatal error during startup: %s", exc)
        sys.exit(1)


def _cli_entry() -> NoReturn:
    main()
    raise SystemExit(0)


if __name__ == "__main__":
    _cli_entry()
