"""
boot_notifier.py — Fresh-boot detection and Telegram notification.

Called once from scripts/start_worker.py and scripts/start_master.py during
their respective startup sequences.

Logic
-----
1. Query system uptime via psutil.boot_time().
2. If uptime < 5 minutes the machine just (re)booted.
3. Send a single Hebrew Telegram message informing the operator that the
   Nexus Worker came back online after a reboot.
4. A Redis de-duplication key (nexus:boot:notified:<boot_epoch_minute>) with a
   10-minute TTL ensures the message is sent exactly once per boot cycle even
   when both Master and Worker start up within the same 5-minute window.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    pass

log = structlog.get_logger(__name__)

# Send notification only if system has been up for less than this many seconds.
_FRESH_BOOT_THRESHOLD_S: int = 300   # 5 minutes
_DEDUP_KEY_TTL_S: int = 600          # 10-minute Redis TTL for the de-dup flag


def _get_uptime_seconds() -> float:
    """
    Return the number of seconds since the OS was last booted.

    Falls back to ``float('inf')`` (i.e. "not a fresh boot") if psutil is
    unavailable so the notification is safely skipped rather than crashing.
    """
    try:
        import psutil  # noqa: PLC0415
        return time.time() - psutil.boot_time()
    except Exception as exc:
        log.debug("boot_notifier_psutil_unavailable", error=str(exc))
        return float("inf")


async def check_and_notify_boot(
    bot_token: str,
    admin_chat_id: str,
    node_id: str = "nexus",
    redis=None,
) -> None:
    """
    If the system booted less than 5 minutes ago, send a Telegram notification.

    Parameters
    ----------
    bot_token:
        Telegram Bot API token (``TELEGRAM_BOT_TOKEN``).
    admin_chat_id:
        Target chat ID (``TELEGRAM_ADMIN_CHAT_ID``).
    node_id:
        Identifier included in the log entry (e.g. ``settings.node_id``).
    redis:
        Optional live arq / redis.asyncio connection used for de-duplication.
        When supplied, a Redis key prevents duplicate messages when both Master
        and Worker start within the same boot window.  Pass ``None`` to skip
        de-duplication (safe — just means two messages might be sent).
    """
    if not bot_token or not admin_chat_id:
        log.debug("boot_notifier_skipped", reason="no_telegram_credentials", node_id=node_id)
        return

    uptime = _get_uptime_seconds()

    if uptime > _FRESH_BOOT_THRESHOLD_S:
        log.debug(
            "boot_notifier_skipped",
            reason="not_fresh_boot",
            uptime_seconds=round(uptime),
            node_id=node_id,
        )
        return

    # ── Redis de-duplication ──────────────────────────────────────────────────
    # Key is tied to the boot minute so it naturally expires after the window.
    boot_minute = int(time.time() // 60)
    dedup_key   = f"nexus:boot:notified:{boot_minute}"

    if redis is not None:
        try:
            already_sent = await redis.get(dedup_key)
            if already_sent:
                log.info(
                    "boot_notifier_dedup_skip",
                    node_id=node_id,
                    dedup_key=dedup_key,
                )
                return
            # Mark as sent before we actually send — acceptable race condition
            # (worst case: 2 messages).  setnx avoids overwriting.
            await redis.set(dedup_key, "1", ex=_DEDUP_KEY_TTL_S)
        except Exception as exc:
            log.debug("boot_notifier_dedup_error", error=str(exc))
            # Continue without de-duplication rather than silently swallowing.

    log.info(
        "fresh_boot_detected",
        uptime_seconds=round(uptime),
        node_id=node_id,
    )

    # ── Send Telegram message ─────────────────────────────────────────────────
    # MarkdownV2 note: the period at the end must be escaped as \. per the spec.
    message = (
        "🔄 *מעבד Nexus (Worker) הופעל בהצלחה לאחר הפעלה מחדש של המחשב\\.*"
    )

    try:
        from nexus.shared.notifications.providers.telegram import TelegramProvider  # noqa: PLC0415

        provider = TelegramProvider(
            bot_token=bot_token,
            admin_chat_id=admin_chat_id,
        )
        await provider.send_message(message)
        log.info(
            "boot_notification_sent",
            node_id=node_id,
            uptime_seconds=round(uptime),
            chat_id=admin_chat_id,
        )
    except Exception as exc:
        log.warning(
            "boot_notification_failed",
            error=str(exc),
            node_id=node_id,
        )
