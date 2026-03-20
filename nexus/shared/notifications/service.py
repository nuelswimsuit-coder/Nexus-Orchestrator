"""
NotificationService — provider-agnostic alert dispatcher.

Every notification attempt is logged at INFO level so operators can see
in the terminal exactly what was sent, to whom, and whether it succeeded.

Diagnostic log events
---------------------
  notification_dispatching   — fired before each provider call
  notification_sent          — fired after a successful provider call
  notification_provider_error — fired when a provider raises an exception
"""

from __future__ import annotations

import asyncio
import inspect
from typing import TYPE_CHECKING

import structlog

from nexus.shared.notifications.base import Alert, AlertLevel, NotificationProvider

if TYPE_CHECKING:
    pass

log = structlog.get_logger(__name__)


class NotificationService:
    """
    Aggregates multiple NotificationProviders and dispatches alerts to all
    of them concurrently.
    """

    def __init__(self) -> None:
        self._providers: list[NotificationProvider] = []

    def register(self, provider: NotificationProvider) -> None:
        """Add a provider to the notification pipeline."""
        self._providers.append(provider)
        log.info("notification_provider_registered", provider=provider.name)

    def unregister(self, name: str) -> None:
        """Remove a provider by name."""
        before = len(self._providers)
        self._providers = [p for p in self._providers if p.name != name]
        if len(self._providers) < before:
            log.info("notification_provider_removed", provider=name)

    @property
    def provider_names(self) -> list[str]:
        return [p.name for p in self._providers]

    async def notify(self, alert: Alert) -> None:
        """
        Fan out `alert` to all registered providers concurrently.

        Individual provider failures are caught and logged; they never
        raise to the caller.
        """
        if not self._providers:
            log.debug("notification_no_providers", title=alert.title)
            return

        log.info(
            "notification_dispatching",
            title=alert.title,
            level=alert.level.value,
            providers=self.provider_names,
        )

        await asyncio.gather(
            *[self._safe_send(p, alert) for p in self._providers],
            return_exceptions=True,
        )

    # ── HITL convenience wrappers ─────────────────────────────────────────────

    async def notify_hitl_requested(
        self,
        task_id: str,
        task_type: str,
        project_id: str,
        context: str,
        request_id: str = "",
        dashboard_url: str = "http://localhost:3000",
    ) -> None:
        """
        Dispatch a HITL approval-required event to all providers.

        For TelegramProvider: calls `send_hitl_alert()` so the message
        includes Approve / Reject inline keyboard buttons.
        For WhatsAppProvider: calls `send_approval_request()` for the
        structured mobile-friendly format.
        All other providers receive the standard Alert via `send()`.
        """
        log.info(
            "hitl_notification_dispatching",
            task_id=task_id,
            task_type=task_type,
            project_id=project_id,
            request_id=request_id,
            providers=self.provider_names,
        )

        # Build the generic Alert for providers that don't have a special HITL method.
        alert = Alert(
            title="⏸ HITL Approval Required",
            body=context,
            level=AlertLevel.WARNING,
            metadata={
                "task_id": task_id,
                "task_type": task_type,
                "project_id": project_id,
                "dashboard": dashboard_url,
            },
        )

        coros = []
        for provider in self._providers:

            if provider.name == "telegram" and request_id:
                from nexus.shared.notifications.providers.telegram import TelegramProvider
                if isinstance(provider, TelegramProvider):
                    log.info(
                        "hitl_notification_via_telegram",
                        task_id=task_id,
                        request_id=request_id,
                        chat_id=provider._chat_id,
                    )
                    coros.append(
                        self._safe_call(
                            provider.send_hitl_alert(
                                request_id=request_id,
                                task_id=task_id,
                                task_type=task_type,
                                project_id=project_id,
                                context=context,
                            ),
                            provider_name="telegram",
                            event_label="hitl_telegram",
                        )
                    )
                    continue

            if provider.name == "whatsapp":
                from nexus.shared.notifications.providers.whatsapp import WhatsAppProvider
                if isinstance(provider, WhatsAppProvider):
                    log.info(
                        "hitl_notification_via_whatsapp",
                        task_id=task_id,
                        to=provider._to,
                        mode=provider._mode,
                    )
                    coros.append(
                        self._safe_call(
                            provider.send_approval_request(
                                project_id=project_id,
                                description=context,
                                task_id=task_id,
                                task_type=task_type,
                                dashboard_url=dashboard_url,
                            ),
                            provider_name="whatsapp",
                            event_label="hitl_whatsapp",
                        )
                    )
                    continue

            coros.append(self._safe_send(provider, alert))

        if coros:
            await asyncio.gather(*coros, return_exceptions=True)
        else:
            log.warning(
                "hitl_notification_no_providers",
                hint="No notification providers are registered. "
                     "Check TELEGRAM_BOT_TOKEN / TELEGRAM_ADMIN_CHAT_ID in .env.",
            )

    async def notify_hitl_resolved(
        self,
        task_id: str,
        approved: bool,
        reviewer_id: str,
        reason: str | None = None,
    ) -> None:
        """Dispatch a HITL resolved event to all providers."""
        action = "approved ✓" if approved else "rejected ✗"
        log.info(
            "hitl_resolved_notification",
            task_id=task_id,
            approved=approved,
            reviewer=reviewer_id,
        )
        await self.notify(Alert(
            title=f"HITL Task {action}",
            body=(
                f"Task {task_id} was {action} by {reviewer_id}."
                + (f"\nReason: {reason}" if reason else "")
            ),
            level=AlertLevel.INFO if approved else AlertLevel.WARNING,
            metadata={"task_id": task_id, "reviewer": reviewer_id},
        ))

    async def notify_task_failed(
        self,
        task_id: str,
        task_type: str,
        error: str,
        attempt: int,
        max_tries: int,
    ) -> None:
        """Dispatch a task failure / retry event to all providers."""
        final = attempt >= max_tries
        await self.notify(Alert(
            title=f"{'❌ Task Failed' if final else '🔄 Task Retry'}: {task_type}",
            body=f"Error: {error}",
            level=AlertLevel.CRITICAL if final else AlertLevel.WARNING,
            metadata={
                "task_id": task_id,
                "attempt": attempt,
                "max_tries": max_tries,
                "final_failure": final,
            },
        ))

    # ── Internal ───────────────────────────────────────────────────────────────

    async def _safe_send(self, provider: NotificationProvider, alert: Alert) -> None:
        await self._safe_call(
            provider.send(alert),
            provider_name=provider.name,
            event_label=f"alert_{alert.level.value}",
        )

    async def _safe_call(
        self,
        coro: object,
        provider_name: str,
        event_label: str = "notification",
    ) -> None:
        """
        Await `coro` and log the outcome.

        Success → INFO log with provider name and event label.
        Failure → ERROR log with full exception — never propagates.
        """
        if not inspect.isawaitable(coro):
            return
        try:
            await coro  # type: ignore[misc]
            log.info(
                "notification_sent",
                provider=provider_name,
                event=event_label,
            )
        except Exception as exc:
            log.error(
                "notification_provider_error",
                provider=provider_name,
                event=event_label,
                error=str(exc),
            )
