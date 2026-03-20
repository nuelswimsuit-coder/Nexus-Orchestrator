"""
Abstract base for notification providers.

All providers implement the same `send()` interface so the
NotificationService can fan-out to multiple channels without knowing
their specifics.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class AlertLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class Alert:
    """A structured notification payload sent to all registered providers."""

    title: str
    body: str
    level: AlertLevel = AlertLevel.INFO
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def format_text(self) -> str:
        """Plain-text rendering used by logging and simple providers."""
        level_icon = {"info": "ℹ️", "warning": "⚠️", "critical": "🚨"}.get(
            self.level.value, "•"
        )
        ts = self.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
        lines = [
            f"{level_icon} [{self.level.value.upper()}] {self.title}",
            f"{self.body}",
            f"— {ts}",
        ]
        if self.metadata:
            for k, v in self.metadata.items():
                lines.append(f"  {k}: {v}")
        return "\n".join(lines)


class NotificationProvider(ABC):
    """Base class for all notification providers."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable provider name (used in logs)."""

    @abstractmethod
    async def send(self, alert: Alert) -> None:
        """
        Dispatch `alert` through this provider.

        Implementations must not raise — catch and log errors internally
        so a failing provider never blocks the master's event loop.
        """
