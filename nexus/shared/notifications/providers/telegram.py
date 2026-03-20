"""
Telegram Notification Provider — aiogram 3.x

Responsibilities
----------------
1. TelegramProvider.send()  — fan-out entry point called by NotificationService.
   Formats any Alert into a Markdown message and delivers it via Bot.send_message().

2. TelegramProvider.send_hitl_alert() — specialised HITL method that sends a
   rich message with *Approve* / *Reject* inline keyboard buttons.  When the
   operator taps a button, the callback payload is:
       hitl_approve:<request_id>
       hitl_reject:<request_id>
   The bot entrypoint (scripts/start_telegram_bot.py) handles these callbacks
   and POSTs to POST /api/hitl/resolve, bridging Telegram → React dashboard.

3. TelegramProvider.send_message() — raw text delivery for ad-hoc use.

Configuration (env vars / .env)
--------------------------------
TELEGRAM_BOT_TOKEN      — BotFather token, e.g. "7123456789:AAF..."
TELEGRAM_ADMIN_CHAT_ID  — Your personal chat ID or a group/channel ID.
                          Find yours by messaging @userinfobot.
TELEGRAM_DASHBOARD_URL  — URL shown in HITL messages (default: http://localhost:3000)

Alert level → Telegram icon mapping
-------------------------------------
INFO     → ℹ️
WARNING  → ⚠️
CRITICAL → 🚨

Message format (MarkdownV2)
----------------------------
🚨 *HITL Approval Required*

📋 *Task ID:* `abc-123`
🏷 *Type:* `llm.summarise`
📁 *Project:* `my-project`
📝 *Reason:* Task needs human sign-off before execution\.

🔗 [Open Dashboard](http://192\.168\.1\.10:3000)

[✅ Approve](callback) [❌ Reject](callback)
"""

from __future__ import annotations

import os
import re

import structlog
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from nexus.shared.notifications.base import Alert, AlertLevel, NotificationProvider

log = structlog.get_logger(__name__)

# ── Markdown escaping ──────────────────────────────────────────────────────────
# Telegram MarkdownV2 requires escaping these characters outside code spans.
_MD_ESCAPE_RE = re.compile(r"([_\*\[\]\(\)~`>#+\-=|{}.!\\])")


def _esc(text: str) -> str:
    """Escape a plain string for safe use in MarkdownV2."""
    return _MD_ESCAPE_RE.sub(r"\\\1", text)


# ── Level icons ────────────────────────────────────────────────────────────────
_LEVEL_ICON: dict[AlertLevel, str] = {
    AlertLevel.INFO: "ℹ️",
    AlertLevel.WARNING: "⚠️",
    AlertLevel.CRITICAL: "🚨",
}


class TelegramProvider(NotificationProvider):
    """
    Sends alerts and HITL notifications to a Telegram chat via aiogram 3.x.

    Parameters
    ----------
    bot_token    : Telegram Bot API token from BotFather.
                   Defaults to TELEGRAM_BOT_TOKEN env var.
    admin_chat_id: Target chat / group / channel ID.
                   Defaults to TELEGRAM_ADMIN_CHAT_ID env var.
    dashboard_url: URL embedded in HITL messages.
                   Defaults to TELEGRAM_DASHBOARD_URL or http://localhost:3000.
    min_level    : Only alerts at or above this level are delivered.
                   Defaults to INFO (all alerts).
    """

    def __init__(
        self,
        bot_token: str | None = None,
        admin_chat_id: str | None = None,
        dashboard_url: str | None = None,
        min_level: AlertLevel = AlertLevel.INFO,
    ) -> None:
        self._token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN", "")
        self._chat_id = admin_chat_id or os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "")
        self._dashboard_url = (
            dashboard_url
            or os.environ.get("TELEGRAM_DASHBOARD_URL", "http://localhost:3000")
        )
        self._min_level = min_level

        if not self._token:
            log.warning(
                "telegram_provider_no_token",
                hint="Set TELEGRAM_BOT_TOKEN in .env — messages will be dropped.",
            )
        if not self._chat_id:
            log.warning(
                "telegram_provider_no_chat_id",
                hint="Set TELEGRAM_ADMIN_CHAT_ID in .env — messages will be dropped.",
            )

        log.info(
            "telegram_provider_init",
            chat_id=self._chat_id or "NOT SET",
            dashboard_url=self._dashboard_url,
            min_level=min_level.value,
        )

    @property
    def name(self) -> str:
        return "telegram"

    # ── NotificationProvider interface ────────────────────────────────────────

    async def send(self, alert: Alert) -> None:
        """
        Format `alert` as a MarkdownV2 message and deliver it.

        Respects min_level filter — alerts below the threshold are silently
        dropped so INFO-level chatter doesn't flood the operator's phone.
        """
        if not self._is_configured():
            return
        if not self._meets_level(alert.level):
            return

        icon = _LEVEL_ICON.get(alert.level, "•")
        lines = [
            f"{icon} *{_esc(alert.title)}*",
            "",
            _esc(alert.body),
        ]
        if alert.metadata:
            lines.append("")
            for k, v in alert.metadata.items():
                lines.append(f"• *{_esc(str(k))}:* `{_esc(str(v))}`")

        await self.send_message("\n".join(lines))

    # ── Specialised HITL method ───────────────────────────────────────────────

    async def send_hitl_alert(
        self,
        request_id: str,
        task_id: str,
        task_type: str,
        project_id: str,
        context: str,
    ) -> None:
        """
        Send a rich HITL approval request with Approve / Reject inline buttons.

        The callback_data values are consumed by the bot entrypoint
        (scripts/start_telegram_bot.py) which POSTs to /api/hitl/resolve.

        Callback payload format:
            hitl_approve:<request_id>
            hitl_reject:<request_id>
        """
        if not self._is_configured():
            return

        # Build MarkdownV2 message body.
        dashboard_escaped = _esc(self._dashboard_url)
        lines = [
            "🚨 *Action Required — HITL Approval*",
            "",
            f"📋 *Task ID:* `{_esc(task_id)}`",
            f"🏷 *Type:* `{_esc(task_type)}`",
            f"📁 *Project:* `{_esc(project_id)}`",
            f"📝 *Reason:* {_esc(context)}",
            "",
            f"🔗 [Open Dashboard]({dashboard_escaped})",
        ]
        text = "\n".join(lines)

        # Inline keyboard: Approve / Reject buttons.
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="✅ Approve",
                callback_data=f"hitl_approve:{request_id}",
            ),
            InlineKeyboardButton(
                text="❌ Reject",
                callback_data=f"hitl_reject:{request_id}",
            ),
        ]])

        await self._send_raw(text=text, reply_markup=keyboard)
        log.info(
            "telegram_hitl_alert_sent",
            request_id=request_id,
            task_id=task_id,
            chat_id=self._chat_id,
        )

    # ── First-Birth Proposal ──────────────────────────────────────────────────

    async def send_birth_proposal(
        self,
        request_id: str,
        project_id: str,
        project_name: str,
        niche_description: str,
        ai_logic: str,
        file_path: str,
        estimated_roi_pct: int,
    ) -> None:
        """
        Send the PROJECT_BIRTH_APPROVAL proposal with APPROVE & REJECT buttons.

        Callback data format:
            birth_approve:<request_id>
            birth_reject:<request_id>
        """
        if not self._is_configured():
            return

        dashboard_escaped = _esc(self._dashboard_url)
        lines = [
            "👶 *PROJECT BIRTH PROPOSAL*",
            "",
            f"👶 *Project Name:* `{_esc(project_name)}`",
            f"🎯 *Target Niche:* {_esc(niche_description)}",
            f"💡 *AI Logic:* {_esc(ai_logic)}",
            f"📂 *Code Location:* `{_esc(file_path)}`",
            f"📈 *Est\\. Initial ROI:* `{_esc(str(estimated_roi_pct))}%`",
            "",
            "⚡ _Approving will deploy this project to an available Worker\\._",
            "_Future projects with confidence \\> 80% will deploy automatically\\._",
            "",
            f"🔗 [Open Incubator]({dashboard_escaped}/incubator)",
        ]
        text = "\n".join(lines)

        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="🚀 APPROVE & ENABLE GOD MODE",
                callback_data=f"birth_approve:{request_id}",
            ),
            InlineKeyboardButton(
                text="❌ REJECT & REGENERATE",
                callback_data=f"birth_reject:{request_id}",
            ),
        ]])

        await self._send_raw(text=text, reply_markup=keyboard)
        log.info(
            "telegram_birth_proposal_sent",
            request_id=request_id,
            project_id=project_id,
            chat_id=self._chat_id,
        )

    # ── Raw send helpers ──────────────────────────────────────────────────────

    async def send_message(self, text: str) -> None:
        """
        Send a plain MarkdownV2 message to the configured admin chat.

        Use this for ad-hoc messages from anywhere in the codebase:
            await telegram_provider.send_message("*Hello* from Nexus\\!")
        """
        await self._send_raw(text=text)

    async def _send_raw(
        self,
        text: str,
        reply_markup: InlineKeyboardMarkup | None = None,
    ) -> None:
        """Create a one-shot Bot instance, send the message, then close it."""
        if not self._is_configured():
            return
        try:
            async with Bot(
                token=self._token,
                default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN_V2),
            ) as bot:
                await bot.send_message(
                    chat_id=self._chat_id,
                    text=text,
                    reply_markup=reply_markup,
                    disable_web_page_preview=True,
                )
        except Exception as exc:
            log.error(
                "telegram_send_error",
                error=str(exc),
                chat_id=self._chat_id,
            )

    # ── Internal ──────────────────────────────────────────────────────────────

    def _is_configured(self) -> bool:
        if not self._token or not self._chat_id:
            log.debug("telegram_provider_not_configured", skipping=True)
            return False
        return True

    def _meets_level(self, level: AlertLevel) -> bool:
        order = [AlertLevel.INFO, AlertLevel.WARNING, AlertLevel.CRITICAL]
        return order.index(level) >= order.index(self._min_level)
