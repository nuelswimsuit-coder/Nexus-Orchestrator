"""
One-shot Telegram connectivity test: load .env, send a CRITICAL SYSTEM READY alert.

Uses TelegramProvider for configuration; delivery uses aiogram Bot directly so
Telegram API errors (invalid token, bad chat id, etc.) are printed and exit non-zero.
"""

from __future__ import annotations

import asyncio
import io
import os
import sys
from pathlib import Path

# Windows: Selector event loop for aiogram/aiohttp compatibility
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

from aiogram import Bot  # noqa: E402
from aiogram.client.default import DefaultBotProperties  # noqa: E402
from aiogram.enums import ParseMode  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from nexus.shared.notifications.base import Alert, AlertLevel  # noqa: E402
from nexus.shared.notifications.providers.telegram import (  # noqa: E402
    _LEVEL_ICON,
    TelegramProvider,
    _esc,
)
from nexus.shared.notifications.service import NotificationService  # noqa: E402

MESSAGE_BODY = (
    "🚀 Nexus Night-Watch Test: Signal received. Yaakov Hatan, the system is armed "
    "and the $100 Race is active. Sleep well, the bots are on duty."
)


def _load_env() -> None:
    env_path = PROJECT_ROOT / ".env"
    load_dotenv(env_path, override=False)


def _resolve_credentials() -> tuple[str, str]:
    token = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (
        os.environ.get("TELEGRAM_ADMIN_CHAT_ID")
        or os.environ.get("TELEGRAM_ADMIN_ID")
        or ""
    )
    chat_id = chat_id.strip()
    return token, chat_id


def _format_alert_markdown_v2(alert: Alert) -> str:
    icon = _LEVEL_ICON.get(alert.level, "•")
    lines = [
        f"{icon} *{_esc(alert.title)}*",
        "",
        _esc(alert.body),
    ]
    return "\n".join(lines)


async def _send_markdown_v2_or_raise(token: str, chat_id: str, text: str) -> None:
    async with Bot(
        token=token,
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN_V2),
    ) as bot:
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            disable_web_page_preview=True,
        )


async def main() -> int:
    _load_env()
    token, chat_id = _resolve_credentials()

    if not token:
        print(
            "ERROR: TELEGRAM_BOT_TOKEN is missing or empty in .env.",
            file=sys.stderr,
        )
        return 1
    if not chat_id:
        print(
            "ERROR: TELEGRAM_ADMIN_CHAT_ID (or TELEGRAM_ADMIN_ID) is missing or empty in .env.",
            file=sys.stderr,
        )
        return 1

    alert = Alert(
        title="SYSTEM READY",
        body=MESSAGE_BODY,
        level=AlertLevel.CRITICAL,
    )

    tg = TelegramProvider(bot_token=token, admin_chat_id=chat_id)
    service = NotificationService()
    service.register(tg)

    text = _format_alert_markdown_v2(alert)
    try:
        await _send_markdown_v2_or_raise(token, chat_id, text)
    except Exception as exc:
        err = str(exc).lower()
        detail = f"{type(exc).__name__}: {exc}"
        if "unauthorized" in err or "401" in err or (
            "invalid" in err and "token" in err
        ):
            print(f"ERROR (likely invalid bot token): {detail}", file=sys.stderr)
        elif "chat not found" in err or "wrong chat" in err or "peer_id" in err:
            print(f"ERROR (likely invalid chat id): {detail}", file=sys.stderr)
        else:
            print(f"ERROR: {detail}", file=sys.stderr)
        return 1

    print("Telegram message sent successfully (SYSTEM READY, CRITICAL).")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
