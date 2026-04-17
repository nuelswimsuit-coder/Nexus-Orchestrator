"""
Nexus Supreme — AHU Legacy Migration
Absorbs all Management AHU (TeleFix) SQLite data into the unified Nexus schema.

Usage:
    python -m nexus_supreme.core.db.migration
    # or from code:
    from nexus_supreme.core.db.migration import run_migration
    run_migration(force=False)
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .models import (
    ManagedBot, ManagedGroup, ManagedGroup, ManagedGroup,
    Metric, ScrapedUser, Setting, Target, get_session,
)

FLAG = Path(__file__).resolve().parents[3] / ".nexus_ahu_migrated_v2"

# Default AHU path (override via TELEFIX_ROOT env)
import os
_ahu_root_env = os.environ.get("TELEFIX_ROOT", "").strip()
AHU_ROOT = Path(_ahu_root_env) if _ahu_root_env else (Path.home() / "Desktop" / "Mangement Ahu")
AHU_DB   = AHU_ROOT / "data" / "telefix.db"


def _ahu_query(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    if not AHU_DB.exists():
        return []
    try:
        conn = sqlite3.connect(f"file:{AHU_DB.as_posix()}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
        conn.close()
        return rows
    except Exception:
        return []


def run_migration(
    force: bool = False,
    progress: Callable[[str], None] | None = None,
    db_path: str = "data/nexus_supreme.db",
) -> dict[str, Any]:
    """
    Migrate AHU data into nexus_supreme.db.
    Returns a summary dict: {ok, copied, skipped, errors}.
    """
    log = progress or print

    if FLAG.exists() and not force:
        log("Migration already completed. Pass force=True to re-run.")
        return {"ok": True, "copied": 0, "skipped": 0, "errors": [], "detail": "already done"}

    if not AHU_DB.exists():
        msg = f"AHU DB not found at {AHU_DB}"
        log(f"ERROR: {msg}")
        return {"ok": False, "copied": 0, "skipped": 0, "errors": [msg]}

    sess = get_session(db_path)
    copied  = 0
    skipped = 0
    errors: list[str] = []

    # ── scraped_users ──────────────────────────────────────────────────────────
    log("Migrating scraped_users...")
    for row in _ahu_query("SELECT * FROM scraped_users"):
        uid = row.get("user_id")
        if uid is None:
            continue
        exists = sess.get(ScrapedUser, uid)
        if exists:
            skipped += 1
            continue
        try:
            sess.add(ScrapedUser(
                user_id            = uid,
                access_hash        = row.get("access_hash"),
                username           = row.get("username"),
                source_group       = row.get("source_group"),
                is_premium         = int(row.get("is_premium") or 0),
                last_active        = row.get("last_active"),
                scraped_by_session = row.get("scraped_by_session"),
            ))
            copied += 1
        except Exception as exc:
            errors.append(f"scraped_users/{uid}: {exc}")

    # ── targets ────────────────────────────────────────────────────────────────
    log("Migrating targets...")
    for row in _ahu_query("SELECT * FROM targets"):
        existing = sess.query(Target).filter_by(link=row.get("link")).first()
        if existing:
            skipped += 1
            continue
        try:
            sess.add(Target(
                title = row.get("title"),
                link  = row.get("link"),
                role  = row.get("role", "target"),
            ))
            copied += 1
        except Exception as exc:
            errors.append(f"targets: {exc}")

    # ── managed_groups ─────────────────────────────────────────────────────────
    log("Migrating managed_groups...")
    for row in _ahu_query("SELECT * FROM managed_groups"):
        gid = row.get("group_id")
        if gid is None:
            continue
        existing = sess.get(ManagedGroup, gid)
        if existing:
            skipped += 1
            continue
        try:
            sess.add(ManagedGroup(
                group_id      = gid,
                title         = row.get("title"),
                username      = row.get("username"),
                owner_session = row.get("owner_session"),
            ))
            copied += 1
        except Exception as exc:
            errors.append(f"managed_groups/{gid}: {exc}")

    # ── nexus_bots → managed_bots ──────────────────────────────────────────────
    log("Migrating nexus_bots...")
    for row in _ahu_query("SELECT * FROM nexus_bots"):
        name = row.get("name", "")
        existing = sess.query(ManagedBot).filter_by(name=name).first()
        if existing:
            skipped += 1
            continue
        try:
            sess.add(ManagedBot(
                name       = name,
                niche      = row.get("niche", ""),
                bot_token  = row.get("bot_token"),
                channel_id = row.get("channel_id"),
                keywords   = row.get("keywords", "[]"),
                auto_start = bool(row.get("auto_start", 0)),
                stats_json = row.get("stats_json", "{}"),
            ))
            copied += 1
        except Exception as exc:
            errors.append(f"nexus_bots/{name}: {exc}")

    # ── settings ───────────────────────────────────────────────────────────────
    log("Migrating settings...")
    for row in _ahu_query("SELECT * FROM settings"):
        key = row.get("key")
        if not key:
            continue
        existing = sess.get(Setting, key)
        if existing:
            skipped += 1
            continue
        try:
            sess.add(Setting(key=key, value=row.get("value")))
            copied += 1
        except Exception as exc:
            errors.append(f"settings/{key}: {exc}")

    # ── metrics ────────────────────────────────────────────────────────────────
    log("Migrating metrics...")
    for row in _ahu_query("SELECT * FROM metrics"):
        key = row.get("key")
        if not key:
            continue
        existing = sess.get(Metric, key)
        if existing:
            skipped += 1
            continue
        try:
            val = row.get("value")
            sess.add(Metric(key=key, value=float(val) if val is not None else None))
            copied += 1
        except Exception as exc:
            errors.append(f"metrics/{key}: {exc}")

    try:
        sess.commit()
    except Exception as exc:
        sess.rollback()
        errors.append(f"commit: {exc}")
        log(f"ERROR during commit: {exc}")
        return {"ok": False, "copied": copied, "skipped": skipped, "errors": errors}
    finally:
        sess.close()

    FLAG.write_text("v2", encoding="utf-8")
    log(f"Done. Copied: {copied}, Skipped: {skipped}, Errors: {len(errors)}")
    return {"ok": True, "copied": copied, "skipped": skipped, "errors": errors}


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--force", action="store_true")
    args = p.parse_args()
    result = run_migration(force=args.force)
    print(result)
