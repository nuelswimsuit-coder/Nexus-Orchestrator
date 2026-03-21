"""
Staged Telethon accounts under ``data/staged_accounts/``.

Each account is a pair of files with the same basename:
  ``<name>.session`` — Telethon SQLite session
  ``<name>.json``    — metadata including ``api_id`` and ``api_hash``

Used by account mapping, session creation API, and group warmup tasks.
"""

from __future__ import annotations

import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]


def staged_accounts_root() -> Path:
    return _REPO_ROOT / "data" / "staged_accounts"


def discover_session_meta_json_files(staged_root: Path | None = None) -> list[Path]:
    """
    Return sorted paths to ``*.json`` files that look like Telethon session metadata
    (dict with ``api_id`` and ``api_hash``).
    """
    root = staged_root or staged_accounts_root()
    if not root.is_dir():
        return []
    found: list[Path] = []
    for path in sorted(root.rglob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(data, dict) and "api_id" in data and "api_hash" in data:
            found.append(path)
    return found
