"""
Israeli AI Swarm Ghostwriter — CLI Entrypoint
Usage:
    python main.py [--config config.yaml] [--stealth]
"""

from __future__ import annotations

import argparse
import asyncio
import io
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import yaml
from dotenv import load_dotenv

# Force UTF-8 output on Windows so Hebrew and box-drawing chars render correctly
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# Load .env from repo root (two levels up from this file's package)
_env_path = Path(__file__).resolve().parents[4] / ".env"
load_dotenv(dotenv_path=_env_path, override=False)

# ── ANSI colour helpers ────────────────────────────────────────────────────────
RESET   = "\033[0m"
BOLD    = "\033[1m"
GREEN   = "\033[92m"
YELLOW  = "\033[93m"
RED     = "\033[91m"
CYAN    = "\033[96m"
MAGENTA = "\033[95m"
DIM     = "\033[2m"

LEVEL_COLOURS = {
    "info":    GREEN,
    "warning": YELLOW,
    "error":   RED,
    "debug":   DIM,
}

BANNER = f"""{CYAN}{BOLD}
+----------------------------------------------------------+
|   [IL] Israeli AI Swarm Ghostwriter  --  Nexus Engine    |
|        Powered by Telethon + Gemini / OpenAI             |
+----------------------------------------------------------+
{RESET}"""


def live_log(message: str, level: str = "info") -> None:
    colour = LEVEL_COLOURS.get(level, "")
    ts = datetime.now().strftime("%H:%M:%S")
    tag = level.upper().ljust(7)
    print(f"{DIM}[{ts}]{RESET} {colour}{BOLD}{tag}{RESET} {message}")
    sys.stdout.flush()


def load_config(config_path: Path) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # Allow .env / environment variable overrides
    env_overrides = {
        ("telegram", "api_id"):        os.getenv("TELEGRAM_API_ID"),
        ("telegram", "api_hash"):      os.getenv("TELEGRAM_API_HASH"),
        ("ai", "gemini_api_key"):      os.getenv("GEMINI_API_KEY"),
        ("ai", "openai_api_key"):      os.getenv("OPENAI_API_KEY"),
        ("ai", "anthropic_api_key"):   os.getenv("ANTHROPIC_API_KEY"),
    }
    for (section, key), value in env_overrides.items():
        if value:
            cfg.setdefault(section, {})[key] = value

    # Convert api_id to int if it's a string
    try:
        cfg["telegram"]["api_id"] = int(cfg["telegram"]["api_id"])
    except (KeyError, ValueError, TypeError):
        pass

    return cfg


def validate_config(cfg: dict) -> None:
    errors = []
    tg = cfg.get("telegram", {})
    if not tg.get("api_id") or tg["api_id"] == 0:
        errors.append(
            "TELEGRAM_API_ID is not set in .env  "
            "-> get it from https://my.telegram.org -> API Development Tools"
        )
    if not tg.get("api_hash"):
        errors.append(
            "TELEGRAM_API_HASH is not set in .env  "
            "-> get it from https://my.telegram.org -> API Development Tools"
        )

    ai = cfg.get("ai", {})
    provider = ai.get("provider", "gemini")
    if provider == "gemini" and not ai.get("gemini_api_key"):
        errors.append("ai.gemini_api_key is not set (or GEMINI_API_KEY env var)")
    if provider == "openai" and not ai.get("openai_api_key"):
        errors.append("ai.openai_api_key is not set (or OPENAI_API_KEY env var)")
    if provider == "anthropic" and not ai.get("anthropic_api_key"):
        errors.append("ai.anthropic_api_key is not set (or ANTHROPIC_API_KEY env var)")

    if not cfg.get("groups"):
        errors.append("No target groups defined in config.yaml")

    if errors:
        for e in errors:
            live_log(f"Config error: {e}", level="error")
        sys.exit(1)


async def run(cfg: dict) -> None:
    from .ghostwriter import GhostwriterEngine

    engine = GhostwriterEngine(cfg, live_log)
    try:
        await engine.run()
    except KeyboardInterrupt:
        live_log("Interrupt received — shutting down…", level="warning")
    finally:
        await engine.stop_all()
        live_log("Ghostwriter stopped.", level="info")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Israeli AI Swarm Ghostwriter",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).parent / "config.yaml",
        help="Path to config.yaml (default: config.yaml next to main.py)",
    )
    parser.add_argument(
        "--stealth",
        action="store_true",
        help="Override config: only reply to direct questions (ending with ?)",
    )
    parser.add_argument(
        "--personality",
        choices=["Expert", "Skeptic", "Hype-Man", "Beginner"],
        default=None,
        help="Override personality from config",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable DEBUG log level",
    )
    parser.add_argument(
        "--scan",
        action="store_true",
        help="Scan entire machine for .session files (ZIP/RAR included) and exit",
    )
    parser.add_argument(
        "--scan-roots", nargs="*", metavar="PATH",
        help="Limit scan to specific root paths (default: all drives)",
    )
    args = parser.parse_args()

    print(BANNER)

    # ── Standalone scan mode ───────────────────────────────────────────────────
    if args.scan:
        from .session_scanner import run_scan
        live_log("Running standalone session scan...", level="info")
        run_scan(scan_roots=args.scan_roots or None)
        return

    # Configure stdlib logging (Telethon uses it internally)
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    # Silence noisy Telethon internals unless debug
    if not args.debug:
        logging.getLogger("telethon").setLevel(logging.WARNING)

    if not args.config.exists():
        live_log(f"Config file not found: {args.config}", level="error")
        sys.exit(1)

    cfg = load_config(args.config)

    # CLI flag overrides
    if args.stealth:
        cfg.setdefault("behavior", {})["stealth_mode"] = True
        live_log("Stealth mode ENABLED — only replying to direct questions.", level="warning")
    if args.personality:
        cfg["personality"] = args.personality

    validate_config(cfg)

    personality = cfg.get("personality", "Expert")
    provider = cfg.get("ai", {}).get("provider", "openai")
    groups_count = len(cfg.get("groups", []))
    stealth = cfg.get("behavior", {}).get("stealth_mode", False)

    live_log(f"Personality  : {MAGENTA}{personality}{RESET}", level="info")
    live_log(f"AI Provider  : {CYAN}{provider}{RESET}", level="info")
    live_log(f"Target Groups: {groups_count}", level="info")
    live_log(f"Stealth Mode : {'ON' if stealth else 'OFF'}", level="info")
    live_log("Starting engine…\n", level="info")

    asyncio.run(run(cfg))


if __name__ == "__main__":
    main()
