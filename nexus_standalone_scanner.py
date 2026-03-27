# pip install telethon
#
# USAGE:
#   1. Place your .session files inside a folder named 'sessions/' next to this script.
#   2. Edit API_ID and API_HASH below (from https://my.telegram.org).
#   3. Run: python nexus_standalone_scanner.py
#
# OUTPUT:
#   sessions/validated_active/   — sessions that connected successfully
#   sessions/validated_errors/   — banned / corrupted / unauthorized sessions
#   nexus_group_audit.csv        — full group audit sorted by member count DESC

from __future__ import annotations

import asyncio
import csv
import os
import random
import shutil
import sys
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import (
    AuthKeyUnregisteredError,
    FloodWaitError,
    PhoneMigrateError,
    RPCError,
    SessionPasswordNeededError,
    UserDeactivatedBanError,
)
from telethon.tl.functions.channels import GetFullChannelRequest, GetParticipantsRequest
from telethon.tl.types import Channel, ChannelParticipantsSearch, Chat, User

try:
    from telethon.tl.functions.premium import GetBoostsStatusRequest as _GetBoostsStatusRequest
    _BOOSTS_AVAILABLE = True
except ImportError:
    try:
        from telethon.tl.functions.messages import GetBoostsStatusRequest as _GetBoostsStatusRequest
        _BOOSTS_AVAILABLE = True
    except ImportError:
        _BOOSTS_AVAILABLE = False
        _GetBoostsStatusRequest = None  # type: ignore[assignment]

# ── CONFIGURATION ─────────────────────────────────────────────────────────────
# Get your API credentials from https://my.telegram.org
API_ID: int   = 23808459          # <-- fill in your api_id
API_HASH: str = "b9169d445ae968cbcc0977646e97f1b2"         # <-- fill in your api_hash

# How many members to scan for premium badge (per group). 0 = skip premium scan.
PREMIUM_SCAN_CAP: int = 2_000

# Per-account hard timeout in seconds.
ACCOUNT_TIMEOUT_S: int = 90

# Max concurrent sessions audited at once.
CONCURRENCY: int = 5

# Output CSV filename.
CSV_OUTPUT = "nexus_group_audit.csv"
# ─────────────────────────────────────────────────────────────────────────────


# ── Folder helpers ────────────────────────────────────────────────────────────

def setup_folders(base: Path) -> tuple[Path, Path, Path]:
    """Create sessions/ and its two sub-dirs. Returns (sessions_dir, active_dir, errors_dir)."""
    sessions_dir  = base / "sessions"
    active_dir    = sessions_dir / "validated_active"
    errors_dir    = sessions_dir / "validated_errors"
    sessions_dir.mkdir(exist_ok=True)
    active_dir.mkdir(exist_ok=True)
    errors_dir.mkdir(exist_ok=True)
    return sessions_dir, active_dir, errors_dir


def collect_sessions(sessions_dir: Path) -> list[Path]:
    """Return all *.session files in sessions_dir (non-recursive, top-level only)."""
    return sorted(sessions_dir.glob("*.session"))


def move_session(session_path: Path, dest_dir: Path) -> Path:
    """Move a .session file (and its optional .session-journal) to dest_dir."""
    dest = dest_dir / session_path.name
    shutil.move(str(session_path), str(dest))
    journal = session_path.with_suffix(".session-journal")
    if journal.exists():
        shutil.move(str(journal), str(dest_dir / journal.name))
    return dest


# ── Async helpers ─────────────────────────────────────────────────────────────

async def _pause(min_s: float, max_s: float) -> None:
    await asyncio.sleep(random.uniform(min_s, max_s))


async def _count_premium_members(client: TelegramClient, entity, cap: int) -> int:
    """Paginated GetParticipants scan counting users with the premium badge."""
    if cap == 0:
        return 0
    premium_count = 0
    offset = 0
    limit = 200
    try:
        while offset < cap:
            result = await client(GetParticipantsRequest(
                channel=entity,
                filter=ChannelParticipantsSearch(""),
                offset=offset,
                limit=limit,
                hash=0,
            ))
            if not result.users:
                break
            for user in result.users:
                if getattr(user, "premium", False):
                    premium_count += 1
            offset += len(result.users)
            if len(result.users) < limit:
                break
            await _pause(0.1, 0.3)
    except Exception:
        pass
    return premium_count


async def _get_boost_premium_count(client: TelegramClient, entity) -> int:
    """Fetch the number of premium members contributing to the group's boost level."""
    if not _BOOSTS_AVAILABLE or _GetBoostsStatusRequest is None:
        return 0
    try:
        result = await client(_GetBoostsStatusRequest(peer=entity))
        raw = getattr(result, "premium_audience", None) or 0
        try:
            return int(raw)
        except (TypeError, ValueError):
            try:
                return int(float(str(raw)))
            except (TypeError, ValueError):
                return 0
    except Exception:
        return 0


def _safe_int(val) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        try:
            return int(float(str(val)))
        except (TypeError, ValueError):
            return 0


# ── Per-session audit ─────────────────────────────────────────────────────────

async def _audit_session_inner(
    session_path: Path,
    api_id: int,
    api_hash: str,
) -> dict:
    """
    Connect to one session, iterate dialogs, collect admin/owner groups.
    Returns a result dict with keys: ok, error, groups (list of dicts).
    """
    result = {"ok": False, "error": "", "groups": []}
    client: TelegramClient | None = None
    try:
        client = TelegramClient(str(session_path), api_id=api_id, api_hash=api_hash)
        await client.connect()

        if not await client.is_user_authorized():
            result["error"] = "not_authorized"
            return result

        await _pause(1.0, 2.0)

        async for dialog in client.iter_dialogs():
            entity = dialog.entity

            if not isinstance(entity, (Channel, Chat)):
                continue

            role: str | None = None
            if getattr(entity, "creator", False):
                role = "owner"
            elif getattr(entity, "admin_rights", None):
                role = "admin"

            if role is None:
                continue

            if isinstance(entity, Channel):
                etype = "channel" if entity.broadcast else "group"
            else:
                etype = "group"

            title = dialog.name or ""

            await _pause(0.5, 1.5)

            # Member count
            member_count = 0
            try:
                full = await client(GetFullChannelRequest(entity))
                member_count = _safe_int(getattr(full.full_chat, "participants_count", 0))
            except Exception:
                member_count = _safe_int(getattr(entity, "participants_count", 0))

            await _pause(0.5, 1.5)

            # Premium counts — only meaningful for supergroups/channels (Channel type)
            premium_count = 0
            boost_premiums = 0
            if isinstance(entity, Channel):
                premium_count  = await _count_premium_members(client, entity, PREMIUM_SCAN_CAP)
                await _pause(0.2, 0.5)
                boost_premiums = await _get_boost_premium_count(client, entity)

            result["groups"].append({
                "group_name":    title,
                "group_id":      entity.id,
                "entity_type":   etype,
                "member_count":  member_count,
                "premium_count": premium_count,
                "boost_premiums": boost_premiums,
                "role":          role,
            })

            await _pause(0.3, 0.8)

        result["ok"] = True

    except (UserDeactivatedBanError, PhoneMigrateError):
        result["error"] = "banned_or_migrated"
    except AuthKeyUnregisteredError:
        result["error"] = "unregistered"
    except FloodWaitError as e:
        result["error"] = f"flood_wait_{e.seconds}s"
    except SessionPasswordNeededError:
        result["error"] = "2fa_required"
    except RPCError as e:
        result["error"] = f"rpc_error: {e}"
    except Exception as e:
        result["error"] = f"unexpected: {e}"
    finally:
        if client is not None:
            try:
                await client.disconnect()
            except Exception:
                pass

    return result


async def audit_session(
    session_path: Path,
    api_id: int,
    api_hash: str,
) -> dict:
    """Wrapper that enforces a hard timeout per account."""
    try:
        return await asyncio.wait_for(
            _audit_session_inner(session_path, api_id, api_hash),
            timeout=ACCOUNT_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        return {"ok": False, "error": f"timeout_{ACCOUNT_TIMEOUT_S}s", "groups": []}


# ── Main scan loop ────────────────────────────────────────────────────────────

async def run_scan(
    sessions: list[Path],
    active_dir: Path,
    errors_dir: Path,
    api_id: int,
    api_hash: str,
) -> list[dict]:
    """
    Audit all sessions with bounded concurrency.
    Returns a flat list of CSV row dicts.
    """
    semaphore = asyncio.Semaphore(CONCURRENCY)
    total = len(sessions)
    completed = 0
    active_count = 0
    error_count  = 0
    all_rows: list[dict] = []
    lock = asyncio.Lock()

    async def process_one(idx: int, session_path: Path) -> None:
        nonlocal completed, active_count, error_count

        async with semaphore:
            session_name = session_path.stem
            print(f"[SCANNING] Session {idx}/{total} — {session_name} ...", flush=True)

            result = await audit_session(session_path, api_id, api_hash)

            async with lock:
                completed += 1
                if result["ok"]:
                    active_count += 1
                    groups = result["groups"]
                    dest = move_session(session_path, active_dir)
                    print(
                        f"  [OK]    {session_name} → validated_active/ "
                        f"| Found {len(groups)} admin group(s)",
                        flush=True,
                    )
                    for g in groups:
                        all_rows.append({
                            "Session_Name":  session_name,
                            "Group_Name":    g["group_name"],
                            "Group_ID":      g["group_id"],
                            "Entity_Type":   g["entity_type"],
                            "Member_Count":  g["member_count"],
                            "Premium_Count": g["premium_count"],
                            "Boost_Premiums": g["boost_premiums"],
                            "Role":          g["role"],
                        })
                else:
                    error_count += 1
                    dest = move_session(session_path, errors_dir)
                    print(
                        f"  [ERR]   {session_name} → validated_errors/ "
                        f"| Reason: {result['error']}",
                        flush=True,
                    )

    tasks = [process_one(i + 1, s) for i, s in enumerate(sessions)]
    await asyncio.gather(*tasks)

    print(
        f"\n{'─'*60}\n"
        f"Scan Complete.  Active: {active_count}  |  Errors: {error_count}  |  Total: {total}\n"
        f"{'─'*60}",
        flush=True,
    )
    return all_rows


# ── CSV export ────────────────────────────────────────────────────────────────

def export_csv(rows: list[dict], output_path: Path) -> None:
    if not rows:
        print("[INFO] No admin groups found — CSV not written.", flush=True)
        return

    sorted_rows = sorted(
        rows,
        key=lambda r: (-(r["Member_Count"]), -(r["Premium_Count"])),
    )

    fieldnames = [
        "Session_Name",
        "Group_Name",
        "Group_ID",
        "Entity_Type",
        "Member_Count",
        "Premium_Count",
        "Boost_Premiums",
        "Role",
    ]

    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sorted_rows)

    print(f"[EXPORT] Results saved → {output_path}  ({len(sorted_rows)} rows)", flush=True)


# ── Entry point ───────────────────────────────────────────────────────────────

def _validate_config() -> None:
    errors = []
    if not API_ID:
        errors.append("  • API_ID is not set (edit the top of this script)")
    if not API_HASH:
        errors.append("  • API_HASH is not set (edit the top of this script)")
    if errors:
        print("[ERROR] Missing configuration:\n" + "\n".join(errors), flush=True)
        sys.exit(1)


def main() -> None:
    _validate_config()

    base = Path(__file__).resolve().parent
    sessions_dir, active_dir, errors_dir = setup_folders(base)

    sessions = collect_sessions(sessions_dir)
    if not sessions:
        print(
            f"[INFO] No .session files found in '{sessions_dir}'.\n"
            "       Place your Telethon session files there and re-run.",
            flush=True,
        )
        sys.exit(0)

    print(
        f"\n{'═'*60}\n"
        f"  Nexus Standalone Session Auditor\n"
        f"{'═'*60}\n"
        f"  Sessions found : {len(sessions)}\n"
        f"  Concurrency    : {CONCURRENCY}\n"
        f"  Premium cap    : {PREMIUM_SCAN_CAP} members\n"
        f"  Timeout/session: {ACCOUNT_TIMEOUT_S}s\n"
        f"{'─'*60}\n",
        flush=True,
    )

    rows = asyncio.run(
        run_scan(sessions, active_dir, errors_dir, API_ID, API_HASH)
    )

    csv_path = base / CSV_OUTPUT
    export_csv(rows, csv_path)

    print(
        f"\n[DONE] Active sessions → {active_dir}\n"
        f"       Error sessions  → {errors_dir}\n"
        f"       Audit CSV       → {csv_path}\n",
        flush=True,
    )


if __name__ == "__main__":
    main()
