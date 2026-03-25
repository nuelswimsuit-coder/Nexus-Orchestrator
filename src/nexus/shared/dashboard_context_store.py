"""
SQLite persistence for per-project dashboard context rows.

Initialized when the active project changes (API switcher or ``nexus_core`` dispatch).
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from nexus.shared.active_project_scope import resolve_project_type


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def dashboard_db_path() -> Path:
    data = _repo_root() / "data"
    data.mkdir(parents=True, exist_ok=True)
    return data / "nexus_dashboard.sqlite3"


def _default_context_json(project_type: str) -> dict[str, Any]:
    if project_type == "ecommerce_swimwear":
        return {
            "shopify_sync": {"status": "idle", "last_sync_at": None},
            "ad_spend_usd": 0.0,
            "image_gen_queue": 0,
        }
    if project_type == "operations_legal":
        return {
            "doc_analysis": {"pending": 0, "completed": 0, "percent": 0},
            "lead_extraction": {"last_run_at": None, "batch_size": 0},
            "automation_logs": {"tail_ref": "nexus:agent:log"},
        }
    return {
        "widgets": [],
        "notes": "generic project dashboard context",
    }


def ensure_project_dashboard_context(project_id: str, display_name: str | None = None) -> None:
    """
    Insert a dashboard context row for ``project_id`` if missing.
    Does not overwrite existing JSON (operator or workers may enrich it).
    """
    from nexus.shared.active_project_scope import normalize_project_id

    pid = normalize_project_id(project_id)
    ptype = resolve_project_type(pid, display_name)
    ctx = _default_context_json(ptype)
    now = datetime.now(timezone.utc).isoformat()
    path = dashboard_db_path()
    with sqlite3.connect(path, check_same_thread=False) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS project_dashboard_context (
                project_id TEXT PRIMARY KEY,
                project_type TEXT NOT NULL,
                context_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        row = conn.execute(
            "SELECT project_id FROM project_dashboard_context WHERE project_id = ?",
            (pid,),
        ).fetchone()
        if row is None:
            conn.execute(
                """
                INSERT INTO project_dashboard_context
                    (project_id, project_type, context_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (pid, ptype, json.dumps(ctx), now, now),
            )
        conn.commit()
