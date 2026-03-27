"""Stable path to session NDJSON debug log (repo root), regardless of process cwd."""
from __future__ import annotations

from pathlib import Path

_DEBUG_SESSION = "020f7b"


def ndjson_debug_log_path() -> Path:
    return Path(__file__).resolve().parent.parent / f"debug-{_DEBUG_SESSION}.log"
