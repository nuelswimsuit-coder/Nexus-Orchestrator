"""
WhatsApp Notification Provider.

Supports three backends (selected via WHATSAPP_PROVIDER env var):
  mock      — logs to structlog (default, no credentials needed)
  twilio    — Twilio WhatsApp Business API
  evolution — Evolution API (self-hosted, free)

HITL integration
----------------
`send_approval_request()` sends a structured approval-request message
whenever a HITL event fires.

To activate live delivery, set in .env:

  # Evolution API (self-hosted, free):
  WHATSAPP_PROVIDER=evolution
  EVOLUTION_API_URL=https://your-evolution-instance.com
  EVOLUTION_API_KEY=your-api-key
  EVOLUTION_INSTANCE=your-instance-name

  # Twilio:
  WHATSAPP_PROVIDER=twilio
  TWILIO_ACCOUNT_SID=ACxxx
  TWILIO_AUTH_TOKEN=xxx
  TWILIO_WHATSAPP_FROM=+14155238886
"""

from __future__ import annotations

import os

import structlog

from nexus.shared.notifications.base import Alert, AlertLevel, NotificationProvider

log = structlog.get_logger(__name__)

_LEVEL_EMOJI: dict[AlertLevel, str] = {
    AlertLevel.INFO:     "ℹ️",
    AlertLevel.WARNING:  "⚠️",
    AlertLevel.CRITICAL: "🚨",
}

# Placeholder numbers — if the configured number matches one of these,
# we know no real recipient has been set.
_PLACEHOLDER_NUMBERS = {"+0000000000", "0000000000", "", "+"}


def _format_alert(alert: Alert) -> str:
    """Render a generic Alert as a WhatsApp plain-text message."""
    emoji = _LEVEL_EMOJI.get(alert.level, "•")
    ts = alert.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
    divider = "─" * 32
    lines = [f"{emoji} *{alert.title}*", divider, alert.body]
    if alert.metadata:
        for k, v in alert.metadata.items():
            lines.append(f"• {k}: {v}")
    lines += [divider, ts]
    return "\n".join(lines)


def _format_approval_request(
    project_id: str,
    description: str,
    dashboard_url: str,
    task_id: str,
    task_type: str,
) -> str:
    """Render a HITL approval-request as a structured WhatsApp message."""
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    divider = "─" * 32
    return "\n".join([
        "🛠️ *NEXUS — Action Required*",
        divider,
        f"🏷️ Project: {project_id}",
        f"⚠️ Action Needed: {description}",
        f"🔗 Dashboard: {dashboard_url}",
        divider,
        f"Task ID: {task_id}",
        f"Type: {task_type}",
        divider,
        ts,
    ])


class WhatsAppProvider(NotificationProvider):
    """
    WhatsApp notification provider.

    Parameters
    ----------
    to_number : str
        Recipient in E.164 format, e.g. "+972536236645".
        Defaults to WHATSAPP_TO_NUMBER env var.
    min_level : AlertLevel
        Only alerts at or above this level are sent.  Defaults to WARNING.
    dashboard_url : str
        URL embedded in HITL approval messages.
    """

    def __init__(
        self,
        to_number: str | None = None,
        min_level: AlertLevel = AlertLevel.WARNING,
        dashboard_url: str | None = None,
    ) -> None:
        self._to = to_number or os.getenv("WHATSAPP_TO_NUMBER", "+0000000000")
        self._min_level = min_level
        self._dashboard_url = (
            dashboard_url
            or os.getenv("WHATSAPP_DASHBOARD_URL", "http://localhost:3000")
        )
        self._mode = self._detect_mode()

        has_real_number = self._to not in _PLACEHOLDER_NUMBERS

        if self._mode == "mock":
            if has_real_number:
                # Real number configured but no backend — operator needs to act
                log.critical(
                    "whatsapp_mock_with_real_number",
                    to=self._to,
                    hint=(
                        "WHATSAPP_TO_NUMBER is set to a real number but "
                        "WHATSAPP_PROVIDER=mock. "
                        "Messages will NOT be delivered. "
                        "Set WHATSAPP_PROVIDER=evolution (or twilio) and add "
                        "the corresponding credentials to .env to activate live delivery."
                    ),
                )
            else:
                log.info(
                    "whatsapp_mock_mode",
                    hint="Set WHATSAPP_PROVIDER=evolution or twilio in .env for live delivery.",
                )
        else:
            log.info(
                "whatsapp_provider_init",
                mode=self._mode,
                to=self._to,
                min_level=min_level.value,
            )

    @property
    def name(self) -> str:
        return "whatsapp"

    # ── NotificationProvider interface ────────────────────────────────────────

    async def send(self, alert: Alert) -> None:
        """Send a generic alert, respecting min_level filter."""
        level_order = [AlertLevel.INFO, AlertLevel.WARNING, AlertLevel.CRITICAL]
        if level_order.index(alert.level) < level_order.index(self._min_level):
            return
        await self._dispatch(_format_alert(alert))

    # ── HITL-specific method ──────────────────────────────────────────────────

    async def send_approval_request(
        self,
        project_id: str,
        description: str,
        task_id: str,
        task_type: str,
        dashboard_url: str | None = None,
    ) -> None:
        """Send a structured HITL approval-request message."""
        url = dashboard_url or self._dashboard_url
        message = _format_approval_request(
            project_id=project_id,
            description=description,
            dashboard_url=url,
            task_id=task_id,
            task_type=task_type,
        )
        await self._dispatch(message)
        log.info(
            "whatsapp_approval_request_sent",
            task_id=task_id,
            task_type=task_type,
            project_id=project_id,
            mode=self._mode,
            to=self._to,
            delivered=(self._mode != "mock"),
        )

    # ── Internal dispatch ─────────────────────────────────────────────────────

    async def _dispatch(self, message: str) -> None:
        """Route the message to the active backend."""
        if self._mode == "twilio":
            await self._send_twilio(message)
        elif self._mode == "evolution":
            await self._send_evolution(message)
        else:
            self._send_mock(message)

    def _send_mock(self, message: str) -> None:
        log.info(
            "whatsapp_mock_send",
            to=self._to,
            preview=message[:120].replace("\n", " | "),
        )

    async def _send_twilio(self, message: str) -> None:
        """
        Send via Twilio WhatsApp API.
        Required env vars: TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_WHATSAPP_FROM
        """
        try:
            from twilio.rest import Client  # type: ignore[import-untyped]
        except ImportError:
            log.error(
                "whatsapp_twilio_not_installed",
                hint="pip install twilio",
            )
            return

        import asyncio

        missing = [
            v for v in ["TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN", "TWILIO_WHATSAPP_FROM"]
            if not os.environ.get(v)
        ]
        if missing:
            log.critical(
                "whatsapp_twilio_missing_credentials",
                missing=missing,
                hint="Add these to your .env file.",
            )
            return

        sid      = os.environ["TWILIO_ACCOUNT_SID"]
        token    = os.environ["TWILIO_AUTH_TOKEN"]
        from_num = os.environ["TWILIO_WHATSAPP_FROM"]
        client   = Client(sid, token)
        loop     = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: client.messages.create(
                body=message,
                from_=f"whatsapp:{from_num}",
                to=f"whatsapp:{self._to}",
            ),
        )
        log.info("whatsapp_twilio_sent", to=self._to)

    async def _send_evolution(self, message: str) -> None:
        """
        Send via Evolution API (self-hosted WhatsApp gateway).
        Required env vars: EVOLUTION_API_URL, EVOLUTION_API_KEY, EVOLUTION_INSTANCE
        """
        import httpx

        missing = [
            v for v in ["EVOLUTION_API_URL", "EVOLUTION_API_KEY", "EVOLUTION_INSTANCE"]
            if not os.environ.get(v)
        ]
        if missing:
            log.critical(
                "whatsapp_evolution_missing_credentials",
                missing=missing,
                hint=(
                    "Add EVOLUTION_API_URL, EVOLUTION_API_KEY, and "
                    "EVOLUTION_INSTANCE to your .env file."
                ),
            )
            return

        url      = os.environ["EVOLUTION_API_URL"]
        api_key  = os.environ["EVOLUTION_API_KEY"]
        instance = os.environ["EVOLUTION_INSTANCE"]
        payload  = {
            "number": self._to,
            "options": {"delay": 0},
            "textMessage": {"text": message},
        }
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{url}/message/sendText/{instance}",
                json=payload,
                headers={"apikey": api_key},
            )
            resp.raise_for_status()
        log.info("whatsapp_evolution_sent", to=self._to, status=resp.status_code)

    @staticmethod
    def _detect_mode() -> str:
        provider = os.getenv("WHATSAPP_PROVIDER", "mock").lower()
        if provider == "twilio":
            return "twilio"
        if provider == "evolution":
            return "evolution"
        return "mock"

    @property
    def is_configured(self) -> bool:
        """True if a real backend is configured (not mock)."""
        return self._mode != "mock"
