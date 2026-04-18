"""
migrate_ahu_to_nexus.py
────────────────────────────────────────────────────────────────────────────
One-time migration: copies all Management AHU data (9 SQLite tables) from
  C:/Users/Yarin/Desktop/Mangement Ahu/data/telefix.db
into a Nexus-managed snapshot at
  <nexus_root>/data/ahu_snapshot.db

Run once on first setup:
  python scripts/migrate_ahu_to_nexus.py

Safe to re-run — uses INSERT OR IGNORE (duplicate rows are skipped).
A flag file .nexus_ahu_migrated is written after success to prevent
accidental repetition; pass --force to override.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[1]

# Source: Management AHU SQLite (reads TELEFIX_ROOT env or falls back to default)
import os
_telefix_root_env = os.environ.get("TELEFIX_ROOT", "").strip()
AHU_ROOT = Path(_telefix_root_env) if _telefix_root_env else (Path.home() / "Desktop" / "Mangement Ahu")
SRC_DB   = AHU_ROOT / "data" / "telefix.db"

# Destination: Nexus-managed snapshot
DEST_DB   = ROOT / "data" / "ahu_snapshot.db"
FLAG_FILE = ROOT / ".nexus_ahu_migrated"

# All 9 tables defined in Management AHU's Repository.create_tables()
TABLES = [
    "scraped_users",
    "targets",
    "managed_groups",
    "enrollments",
    "settings",
    "metrics",
    "bot_seo",
    "bot_users",
    "nexus_bots",
]


def migrate(force: bool = False) -> None:
    print(f"\n{'='*60}")
    print("  Nexus ← Management AHU — Data Migration")
    print(f"{'='*60}")
    print(f"  Source : {SRC_DB}")
    print(f"  Dest   : {DEST_DB}")
    print()

    # Guard: already done?
    if FLAG_FILE.exists() and not force:
        print("✅ Migration already completed (flag file present).")
        print(f"   Delete {FLAG_FILE} or pass --force to re-run.\n")
        return

    if not SRC_DB.exists():
        print(f"❌ Source DB not found: {SRC_DB}")
        print("   Set TELEFIX_ROOT env var to point to the Management AHU project root.\n")
        sys.exit(1)

    DEST_DB.parent.mkdir(parents=True, exist_ok=True)

    src  = sqlite3.connect(f"file:{SRC_DB.as_posix()}?mode=ro", uri=True)
    dest = sqlite3.connect(str(DEST_DB))
    src.row_factory  = sqlite3.Row
    dest.row_factory = sqlite3.Row

    total_copied  = 0
    total_skipped = 0
    errors: list[str] = []

    with dest:
        dest.execute("PRAGMA journal_mode=WAL")
        dest.execute("PRAGMA synchronous=NORMAL")

        for tbl in TABLES:
            # Mirror schema
            try:
                schema_row = src.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (tbl,)
                ).fetchone()
                if schema_row and schema_row[0]:
                    dest.execute(schema_row[0])
                else:
                    print(f"  ⚠️  Table not found in source: {tbl}")
                    continue
            except Exception as exc:
                errors.append(f"schema:{tbl}: {exc}")
                print(f"  ❌ Schema error [{tbl}]: {exc}")
                continue

            # Copy rows
            try:
                rows = src.execute(f"SELECT * FROM {tbl}").fetchall()  # noqa: S608
                if not rows:
                    print(f"  ○  {tbl:<20} — empty, skipped")
                    continue

                cols    = rows[0].keys()
                col_str = ", ".join(cols)
                ph      = ", ".join("?" * len(cols))

                before = dest.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]  # noqa: S608
                dest.executemany(
                    f"INSERT OR IGNORE INTO {tbl} ({col_str}) VALUES ({ph})",  # noqa: S608
                    [tuple(r) for r in rows],
                )
                after   = dest.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]  # noqa: S608
                n_new   = after - before
                n_skip  = len(rows) - n_new
                total_copied  += n_new
                total_skipped += n_skip

                status = "✅" if n_new > 0 else "○ "
                print(f"  {status} {tbl:<20}  copied={n_new:>6,}  skipped={n_skip:>6,}")

            except Exception as exc:
                errors.append(f"data:{tbl}: {exc}")
                print(f"  ❌ Data error [{tbl}]: {exc}")

    src.close()
    dest.close()

    print()
    print(f"{'─'*60}")
    print(f"  Total copied : {total_copied:,}")
    print(f"  Total skipped: {total_skipped:,}")
    if errors:
        print(f"  Errors       : {len(errors)}")
        for e in errors:
            print(f"    • {e}")

    if not errors or total_copied > 0:
        FLAG_FILE.write_text("migrated", encoding="utf-8")
        print(f"\n✅ Migration complete!  Snapshot saved to:\n   {DEST_DB}")
        print(f"   Flag written: {FLAG_FILE}")
    else:
        print("\n⚠️  Migration completed with errors — flag NOT written.")

    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate Management AHU data → Nexus snapshot DB")
    parser.add_argument("--force", action="store_true", help="Re-run even if migration was already done")
    args = parser.parse_args()
    migrate(force=args.force)
