"""Telegram notification integration — facade over shared providers."""

from __future__ import annotations

from nexus.shared.notifications.providers.telegram import TelegramProvider

__all__ = ["TelegramProvider"]
