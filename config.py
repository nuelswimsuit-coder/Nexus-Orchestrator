"""
Environment bootstrap for root entrypoints (`bot.py`).

Loads `.env` from the repository root via python-dotenv (non-destructive:
existing OS environment variables win). Prefer :func:`os.getenv` with
defaults instead of ``os.environ[key]`` to avoid blind KeyErrors.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

from utils.paths import repository_root


class ConfigurationError(RuntimeError):
    """Raised when required configuration is missing or invalid."""


def bootstrap_environment() -> Path:
    """
    Load ``.env`` from the repo root and return that path.

    Safe to call more than once; dotenv is only applied if the file exists.
    """
    root = repository_root()
    env_file = root / ".env"
    if env_file.is_file():
        load_dotenv(env_file, override=False)
    return root


def get_env(key: str, default: str | None = None) -> str | None:
    """Return a stripped env value, or ``default`` if unset or empty."""
    raw = os.getenv(key)
    if raw is None:
        return default
    stripped = raw.strip()
    return stripped if stripped else default


def require_env(key: str, *, hint: str | None = None) -> str:
    """Require an environment variable; raise :class:`ConfigurationError` if missing."""
    val = get_env(key)
    if not val:
        msg = f"Required environment variable {key!r} is not set or is empty."
        if hint:
            msg += f" {hint}"
        raise ConfigurationError(msg)
    return val


def openai_api_key(*, required: bool = False) -> str | None:
    """
    Return ``OPENAI_API_KEY`` if set.

    If ``required`` is True, raises :class:`ConfigurationError` with a clear
    message instead of letting callers hit KeyError downstream.
    """
    key = get_env("OPENAI_API_KEY")
    if required and not key:
        raise ConfigurationError(
            "OPENAI_API_KEY is not set or is empty. Add it to your .env file at the "
            "repository root (or export it in your environment) before running tasks "
            "that call the OpenAI API."
        )
    return key
