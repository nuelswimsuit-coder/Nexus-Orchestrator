"""
Unified Command & Control hub — SQLite backing store for project visions
and arbitrary per-project metadata (JSON blobs).

Path: ``nexus/data/cc_hub.db`` (created on first use).
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = _REPO_ROOT / "nexus" / "data" / "cc_hub.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS cc_projects (
    slug TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    vision_summary TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL
);
"""

_SEED_PROJECTS: tuple[tuple[str, str, str], ...] = (
    (
        "nuel",
        "NUEL",
        "E-commerce growth: Shopify revenue, ad spend efficiency, creative pipeline.",
    ),
    (
        "management_ahu",
        "Management Ahu",
        "Lead ops: scraped prospects, Telegram outreach, conversion funnel.",
    ),
    (
        "default",
        "Nexus Core",
        "Fleet orchestration, trading stack, and cross-project telemetry.",
    ),
)


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _init_sync() -> None:
    conn = _connect()
    try:
        conn.executescript(_SCHEMA)
        now = datetime.now(timezone.utc).isoformat()
        for slug, name, vision in _SEED_PROJECTS:
            conn.execute(
                """
                INSERT OR IGNORE INTO cc_projects (slug, name, vision_summary, metadata_json, updated_at)
                VALUES (?, ?, ?, '{}', ?)
                """,
                (slug, name, vision, now),
            )
        conn.commit()
    finally:
        conn.close()


async def ensure_cc_hub_schema() -> None:
    await asyncio.to_thread(_init_sync)


def _list_projects_sync() -> list[dict[str, Any]]:
    conn = _connect()
    try:
        cur = conn.execute(
            "SELECT slug, name, vision_summary, metadata_json, updated_at "
            "FROM cc_projects ORDER BY slug"
        )
        rows = cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            meta_raw = r["metadata_json"] or "{}"
            try:
                meta = json.loads(meta_raw)
            except json.JSONDecodeError:
                meta = {}
            out.append(
                {
                    "slug": r["slug"],
                    "name": r["name"],
                    "vision_summary": r["vision_summary"],
                    "metadata": meta,
                    "updated_at": r["updated_at"],
                }
            )
        return out
    finally:
        conn.close()


def _get_project_sync(slug: str) -> dict[str, Any] | None:
    conn = _connect()
    try:
        cur = conn.execute(
            "SELECT slug, name, vision_summary, metadata_json, updated_at "
            "FROM cc_projects WHERE slug = ?",
            (slug,),
        )
        r = cur.fetchone()
        if not r:
            return None
        try:
            meta = json.loads(r["metadata_json"] or "{}")
        except json.JSONDecodeError:
            meta = {}
        return {
            "slug": r["slug"],
            "name": r["name"],
            "vision_summary": r["vision_summary"],
            "metadata": meta,
            "updated_at": r["updated_at"],
        }
    finally:
        conn.close()


def _patch_metadata_sync(slug: str, patch: dict[str, Any]) -> dict[str, Any] | None:
    conn = _connect()
    try:
        cur = conn.execute(
            "SELECT metadata_json FROM cc_projects WHERE slug = ?", (slug,)
        )
        row = cur.fetchone()
        if not row:
            return None
        try:
            meta = json.loads(row["metadata_json"] or "{}")
        except json.JSONDecodeError:
            meta = {}
        meta.update(patch)
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE cc_projects SET metadata_json = ?, updated_at = ? WHERE slug = ?",
            (json.dumps(meta), now, slug),
        )
        conn.commit()
        return meta
    finally:
        conn.close()


async def list_projects() -> list[dict[str, Any]]:
    await ensure_cc_hub_schema()
    return await asyncio.to_thread(_list_projects_sync)


async def get_project(slug: str) -> dict[str, Any] | None:
    await ensure_cc_hub_schema()
    return await asyncio.to_thread(_get_project_sync, slug)


async def patch_project_metadata(slug: str, patch: dict[str, Any]) -> dict[str, Any] | None:
    await ensure_cc_hub_schema()
    return await asyncio.to_thread(_patch_metadata_sync, slug, patch)
