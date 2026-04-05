"""
Telegram (Telethon) session files on disk — source of truth for vault inventory.

Counts and lists ``*.session`` + companion ``*.json`` (api_id/api_hash) under
:func:`nexus.services.session_vault.vault_candidate_roots`, recursively.
This is intentionally separate from Redis (arq, group cache, runtime presence).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from nexus.services.session_vault import discover_meta_paths_from_session_sqlite


def list_tg_session_meta_paths_on_disk() -> list[Path]:
    """Meta JSON paths paired with a live ``.session`` sqlite file and valid credentials."""
    return list(discover_meta_paths_from_session_sqlite())


def count_live_telethon_session_files() -> int:
    """Number of Telethon account pairs (``.session`` + valid ``.json``) on disk."""
    return len(discover_meta_paths_from_session_sqlite())


def tg_session_disk_scan_rows(*, machine_id: str) -> list[dict[str, Any]]:
    """
    Rows for APIs that merge Telegram inventory (e.g. ``/swarm/sessions/all_scanned``).

    ``dedupe_stem`` is the Telethon session stem used to merge with Redis-sourced rows.
    """
    rows: list[dict[str, Any]] = []
    for meta in discover_meta_paths_from_session_sqlite():
        stem = meta.stem
        phone = ""
        try:
            data = json.loads(meta.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                phone = str(data.get("phone") or data.get("phone_number") or "").strip()
        except Exception:
            pass
        rk = f"disk:tg_session:{machine_id}:{stem}"
        rows.append(
            {
                "redis_key": rk,
                "dedupe_stem": stem,
                "phone_number": phone,
                "origin_machine": machine_id,
                "status": "on_disk",
                "last_scanned_target": "vault_disk",
                "last_seen": None,
                "session_id": stem,
                "source": "vault_disk",
            }
        )
    return rows


def inventory_rows_for_local_machine(machine_id: str) -> list[dict[str, Any]]:
    """Shape compatible with ``/swarm/sessions/inventory`` session list entries."""
    out: list[dict[str, Any]] = []
    for meta in discover_meta_paths_from_session_sqlite():
        stem = meta.stem
        phone = ""
        try:
            data = json.loads(meta.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                phone = str(data.get("phone") or data.get("phone_number") or "").strip()
        except Exception:
            pass
        out.append(
            {
                "session_stem": stem,
                "phone": phone,
                "origin_machine": machine_id,
                "status": "on_disk",
                "source": "vault_disk",
            }
        )
    return out
