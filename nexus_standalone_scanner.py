# pip install telethon
# pip install rarfile          (optional — needed for RAR archive support)
#
# USAGE:
#   1. Edit API_ID and API_HASH below (from https://my.telegram.org).
#   2. Run: python nexus_standalone_scanner.py
#      The script will scan the ENTIRE machine for .session files, zip/rar
#      archives containing sessions, and sibling tdata/ folders.
#
# OUTPUT:
#   sessions/validated_active/   — sessions that connected successfully
#   sessions/validated_errors/   — banned / corrupted / unauthorized sessions
#   nexus_group_audit.csv        — full group audit sorted by member count DESC

from __future__ import annotations

import asyncio
import csv
import io
import os
import random
import shutil
import string
import sys
import tempfile
import time
import zipfile
from dataclasses import dataclass, field
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
from telethon.tl.types import Channel, ChannelParticipantsSearch, Chat

try:
    import rarfile
    _RAR_AVAILABLE = True
except ImportError:
    _RAR_AVAILABLE = False

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


# ── Directories to skip during full-disk scan ─────────────────────────────────
_SKIP_DIRS: set[str] = {
    "$Recycle.Bin", "System Volume Information", "Windows",
    "Program Files", "Program Files (x86)",
    "AppData\\Local\\Temp", "AppData\\LocalLow",
    "AppData\\Local\\Programs",
    "AppData\\Local\\Microsoft",
    "AppData\\Local\\Google",
    "AppData\\Local\\BraveSoftware",
    "AppData\\Local\\Mozilla",
    "AppData\\Local\\Steam",
    "AppData\\Local\\Razer",
    "AppData\\Local\\Discord",
    "AppData\\Local\\slack",
    "AppData\\Local\\Packages",
    "AppData\\Local\\Publishers",
    "AppData\\Roaming\\npm",
    "AppData\\Roaming\\Code",
    "AppData\\Roaming\\cursor",
    "__pycache__", ".git", "node_modules", ".venv", "venv",
    "steamapps",
}


# ── ANSI colours ──────────────────────────────────────────────────────────────
_RESET  = "\033[0m"
_BOLD   = "\033[1m"
_GREEN  = "\033[92m"
_YELLOW = "\033[93m"
_RED    = "\033[91m"
_CYAN   = "\033[96m"
_DIM    = "\033[2m"


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class FoundSession:
    session_path: Path          # Path to the usable .session file (may be in staging)
    json_path: Path | None      # Sibling .json, if found
    tdata_path: Path | None     # Sibling tdata/ folder, if found
    source: str                 # "disk" | "zip" | "rar"
    archive_path: Path | None   # Original archive (if extracted)
    phone: str                  # Derived from stem


# ── Skip-dir helper ───────────────────────────────────────────────────────────

def _should_skip(path: Path) -> bool:
    parts = path.parts
    for part in parts:
        if part in _SKIP_DIRS:
            return True
    for skip in _SKIP_DIRS:
        if "\\" in skip:
            skip_parts = tuple(skip.split("\\"))
            n = len(skip_parts)
            for i in range(len(parts) - n + 1):
                if parts[i : i + n] == skip_parts:
                    return True
    return False


def _stem_to_phone(stem: str) -> str:
    s = stem.strip()
    if s and not s.startswith("+") and s[0].isdigit():
        s = "+" + s
    return s


# ── tdata sibling detection ───────────────────────────────────────────────────

def _find_tdata(session_path: Path) -> Path | None:
    """
    Look for a tdata/ folder associated with this session file.
    Checks the four most common layouts:
      session.parent/tdata
      session.parent/stem/tdata
      session.parent.parent/stem/tdata
      session.parent.parent/tdata
    """
    stem = session_path.stem
    candidates = [
        session_path.parent / "tdata",
        session_path.parent / stem / "tdata",
        session_path.parent.parent / stem / "tdata",
        session_path.parent.parent / "tdata",
    ]
    for c in candidates:
        if c.exists() and c.is_dir():
            return c
    return None


# ── Full-disk scanner ─────────────────────────────────────────────────────────

def _detect_drive_roots() -> list[Path]:
    if os.name == "nt":
        roots = []
        for letter in string.ascii_uppercase:
            p = Path(f"{letter}:\\")
            if p.exists():
                roots.append(p)
        return roots or [Path("C:\\")]
    return [Path("/")]


def scan_machine(staging_dir: Path) -> list[FoundSession]:
    """
    Walk every drive, find .session files on disk and inside ZIP/RAR archives.
    Extracted archive sessions land in staging_dir so Telethon can open them.
    Returns a deduplicated list of FoundSession objects.
    """
    staging_dir.mkdir(parents=True, exist_ok=True)
    roots = _detect_drive_roots()
    seen_stems: set[str] = set()
    found: list[FoundSession] = []
    dirs_scanned = 0
    archives_scanned = 0

    print(
        f"\n{_CYAN}{_BOLD}{'═'*60}{_RESET}\n"
        f"  Full-disk scan starting on: {[str(r) for r in roots]}\n"
        f"  RAR support: {'YES' if _RAR_AVAILABLE else 'NO  (pip install rarfile)'}\n"
        f"{_CYAN}{_BOLD}{'─'*60}{_RESET}\n"
        f"  Scanning — this may take a minute on large drives...\n",
        flush=True,
    )

    for root in roots:
        try:
            for dirpath, dirnames, filenames in os.walk(root, topdown=True, onerror=None):
                current = Path(dirpath)

                # Prune skip dirs so os.walk doesn't recurse into them
                dirnames[:] = [
                    d for d in dirnames
                    if not _should_skip(current / d)
                ]

                dirs_scanned += 1
                # Live progress line (overwrite in place)
                print(
                    f"\r{_DIM}[SCAN]{_RESET} {_CYAN}{str(current)[:110]:<110}{_RESET}",
                    end="",
                    flush=True,
                )

                name_set = set(filenames)

                for fname in filenames:
                    fpath = current / fname
                    ext = fpath.suffix.lower()

                    if ext == ".session":
                        stem = fpath.stem
                        if stem in seen_stems:
                            continue
                        seen_stems.add(stem)
                        json_p = (current / (stem + ".json")) if (stem + ".json") in name_set else None
                        tdata_p = _find_tdata(fpath)
                        found.append(FoundSession(
                            session_path=fpath,
                            json_path=json_p,
                            tdata_path=tdata_p,
                            source="disk",
                            archive_path=None,
                            phone=_stem_to_phone(stem),
                        ))

                    elif ext == ".zip":
                        archives_scanned += 1
                        _scan_zip(fpath, staging_dir, seen_stems, found)

                    elif ext == ".rar" and _RAR_AVAILABLE:
                        archives_scanned += 1
                        _scan_rar(fpath, staging_dir, seen_stems, found)

        except PermissionError:
            pass

    print()  # newline after live scan line
    print(
        f"\n{_CYAN}{_BOLD}{'─'*60}{_RESET}\n"
        f"  Scan complete.\n"
        f"  Sessions found : {_BOLD}{_GREEN}{len(found)}{_RESET}\n"
        f"  Dirs scanned   : {dirs_scanned}\n"
        f"  Archives scanned: {archives_scanned}\n"
        f"{_CYAN}{_BOLD}{'═'*60}{_RESET}\n",
        flush=True,
    )
    return found


def _scan_zip(
    archive_path: Path,
    staging_dir: Path,
    seen_stems: set[str],
    found: list[FoundSession],
) -> None:
    try:
        with zipfile.ZipFile(archive_path, "r") as zf:
            names = zf.namelist()
            session_names = [n for n in names if n.lower().endswith(".session")]
            for sname in session_names:
                stem = Path(sname).stem
                if stem in seen_stems:
                    continue
                dest = staging_dir / f"{stem}.session"
                try:
                    dest.write_bytes(zf.read(sname))
                except Exception:
                    continue
                # Look for matching JSON inside the same archive
                json_key_full = str(Path(sname).parent / (stem + ".json")).replace("\\", "/")
                json_key_flat = stem + ".json"
                json_dest: Path | None = None
                actual_json = json_key_full if json_key_full in names else (json_key_flat if json_key_flat in names else None)
                if actual_json:
                    json_dest = staging_dir / f"{stem}.json"
                    try:
                        json_dest.write_bytes(zf.read(actual_json))
                    except Exception:
                        json_dest = None
                # tdata inside zip: look for a tdata/ entry in the archive
                tdata_dest: Path | None = None
                tdata_prefix_candidates = [
                    str(Path(sname).parent / "tdata").replace("\\", "/") + "/",
                    "tdata/",
                ]
                for prefix in tdata_prefix_candidates:
                    tdata_members = [n for n in names if n.startswith(prefix) and not n.endswith("/")]
                    if tdata_members:
                        tdata_dest = staging_dir / f"{stem}_tdata"
                        tdata_dest.mkdir(exist_ok=True)
                        for member in tdata_members:
                            rel = member[len(prefix):]
                            out = tdata_dest / rel
                            out.parent.mkdir(parents=True, exist_ok=True)
                            try:
                                out.write_bytes(zf.read(member))
                            except Exception:
                                pass
                        break

                seen_stems.add(stem)
                found.append(FoundSession(
                    session_path=dest,
                    json_path=json_dest,
                    tdata_path=tdata_dest,
                    source="zip",
                    archive_path=archive_path,
                    phone=_stem_to_phone(stem),
                ))
    except (zipfile.BadZipFile, Exception):
        pass


def _scan_rar(
    archive_path: Path,
    staging_dir: Path,
    seen_stems: set[str],
    found: list[FoundSession],
) -> None:
    try:
        with rarfile.RarFile(str(archive_path), "r") as rf:  # type: ignore[name-defined]
            names = rf.namelist()
            session_names = [n for n in names if n.lower().endswith(".session")]
            for sname in session_names:
                stem = Path(sname).stem
                if stem in seen_stems:
                    continue
                dest = staging_dir / f"{stem}.session"
                try:
                    dest.write_bytes(rf.read(sname))
                except Exception:
                    continue
                json_key_full = str(Path(sname).parent / (stem + ".json")).replace("\\", "/")
                json_key_flat = stem + ".json"
                json_dest = None
                actual_json = json_key_full if json_key_full in names else (json_key_flat if json_key_flat in names else None)
                if actual_json:
                    json_dest = staging_dir / f"{stem}.json"
                    try:
                        json_dest.write_bytes(rf.read(actual_json))
                    except Exception:
                        json_dest = None

                seen_stems.add(stem)
                found.append(FoundSession(
                    session_path=dest,
                    json_path=json_dest,
                    tdata_path=None,
                    source="rar",
                    archive_path=archive_path,
                    phone=_stem_to_phone(stem),
                ))
    except Exception:
        pass


# ── Folder setup & session moving ────────────────────────────────────────────

def setup_output_folders(base: Path) -> tuple[Path, Path]:
    """Create sessions/validated_active/ and sessions/validated_errors/."""
    active_dir = base / "sessions" / "validated_active"
    errors_dir = base / "sessions" / "validated_errors"
    active_dir.mkdir(parents=True, exist_ok=True)
    errors_dir.mkdir(parents=True, exist_ok=True)
    return active_dir, errors_dir


def move_session(session_path: Path, dest_dir: Path) -> Path:
    """Move a .session file (and optional journal) to dest_dir."""
    dest = dest_dir / session_path.name
    try:
        shutil.move(str(session_path), str(dest))
    except Exception:
        try:
            shutil.copy2(str(session_path), str(dest))
        except Exception:
            pass
    journal = session_path.with_suffix(".session-journal")
    if journal.exists():
        try:
            shutil.move(str(journal), str(dest_dir / journal.name))
        except Exception:
            pass
    return dest


# ── Telethon helpers ──────────────────────────────────────────────────────────

async def _pause(min_s: float, max_s: float) -> None:
    await asyncio.sleep(random.uniform(min_s, max_s))


async def _count_premium_members(client: TelegramClient, entity, cap: int) -> int:
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

async def _audit_session_inner(session_path: Path, api_id: int, api_hash: str) -> dict:
    result: dict = {"ok": False, "error": "", "groups": []}
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

            etype = "channel" if (isinstance(entity, Channel) and entity.broadcast) else "group"
            title = dialog.name or ""

            await _pause(0.5, 1.5)

            member_count = 0
            try:
                full = await client(GetFullChannelRequest(entity))
                member_count = _safe_int(getattr(full.full_chat, "participants_count", 0))
            except Exception:
                member_count = _safe_int(getattr(entity, "participants_count", 0))

            await _pause(0.5, 1.5)

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


async def audit_session(session_path: Path, api_id: int, api_hash: str) -> dict:
    try:
        return await asyncio.wait_for(
            _audit_session_inner(session_path, api_id, api_hash),
            timeout=ACCOUNT_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        return {"ok": False, "error": f"timeout_{ACCOUNT_TIMEOUT_S}s", "groups": []}


# ── Main scan loop ────────────────────────────────────────────────────────────

async def run_audit(
    sessions: list[FoundSession],
    active_dir: Path,
    errors_dir: Path,
    api_id: int,
    api_hash: str,
) -> list[dict]:
    semaphore = asyncio.Semaphore(CONCURRENCY)
    total = len(sessions)
    completed = 0
    active_count = 0
    error_count  = 0
    all_rows: list[dict] = []
    lock = asyncio.Lock()

    async def process_one(idx: int, fs: FoundSession) -> None:
        nonlocal completed, active_count, error_count

        async with semaphore:
            session_name = fs.session_path.stem
            src_tag = f"[{fs.source.upper()}]" if fs.source != "disk" else ""
            print(
                f"[SCANNING] Session {idx}/{total} — {session_name} {src_tag}",
                flush=True,
            )

            result = await audit_session(fs.session_path, api_id, api_hash)

            async with lock:
                completed += 1
                if result["ok"]:
                    active_count += 1
                    groups = result["groups"]
                    move_session(fs.session_path, active_dir)
                    tdata_note = f"  tdata={fs.tdata_path}" if fs.tdata_path else ""
                    print(
                        f"  {_GREEN}[OK]{_RESET}    {session_name} → validated_active/"
                        f" | {len(groups)} admin group(s){tdata_note}",
                        flush=True,
                    )
                    for g in groups:
                        all_rows.append({
                            "Session_Name":   session_name,
                            "Group_Name":     g["group_name"],
                            "Group_ID":       g["group_id"],
                            "Entity_Type":    g["entity_type"],
                            "Member_Count":   g["member_count"],
                            "Premium_Count":  g["premium_count"],
                            "Boost_Premiums": g["boost_premiums"],
                            "Role":           g["role"],
                        })
                else:
                    error_count += 1
                    move_session(fs.session_path, errors_dir)
                    print(
                        f"  {_RED}[ERR]{_RESET}   {session_name} → validated_errors/"
                        f" | {result['error']}",
                        flush=True,
                    )

    await asyncio.gather(*[process_one(i + 1, s) for i, s in enumerate(sessions)])

    print(
        f"\n{_CYAN}{'─'*60}{_RESET}\n"
        f"  Scan Complete.  "
        f"{_GREEN}Active: {active_count}{_RESET}  |  "
        f"{_RED}Errors: {error_count}{_RESET}  |  "
        f"Total: {total}\n"
        f"{_CYAN}{'─'*60}{_RESET}",
        flush=True,
    )
    return all_rows


# ── CSV export ────────────────────────────────────────────────────────────────

def export_csv(rows: list[dict], output_path: Path) -> None:
    if not rows:
        print("[INFO] No admin groups found — CSV not written.", flush=True)
        return

    sorted_rows = sorted(rows, key=lambda r: (-(r["Member_Count"]), -(r["Premium_Count"])))

    fieldnames = [
        "Session_Name", "Group_Name", "Group_ID", "Entity_Type",
        "Member_Count", "Premium_Count", "Boost_Premiums", "Role",
    ]
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sorted_rows)

    print(
        f"\n[EXPORT] {_GREEN}{_BOLD}Results saved → {output_path}{_RESET}  "
        f"({len(sorted_rows)} rows)",
        flush=True,
    )


# ── Entry point ───────────────────────────────────────────────────────────────

def _validate_config() -> None:
    errors = []
    if not API_ID:
        errors.append("  • API_ID is not set (edit the top of this script)")
    if not API_HASH:
        errors.append("  • API_HASH is not set (edit the top of this script)")
    if errors:
        print(f"{_RED}[ERROR] Missing configuration:\n" + "\n".join(errors) + _RESET, flush=True)
        sys.exit(1)


def main() -> None:
    # Force UTF-8 output on Windows
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    _validate_config()

    base = Path(__file__).resolve().parent
    staging_dir = base / "sessions" / "_staging"
    active_dir, errors_dir = setup_output_folders(base)

    # ── Phase 1: full-disk scan ───────────────────────────────────────────────
    found_sessions = scan_machine(staging_dir)

    if not found_sessions:
        print(
            f"{_YELLOW}[INFO] No .session files found anywhere on this machine.{_RESET}",
            flush=True,
        )
        sys.exit(0)

    print(
        f"\n{_CYAN}{_BOLD}{'═'*60}{_RESET}\n"
        f"  Nexus Standalone Session Auditor\n"
        f"{_CYAN}{_BOLD}{'═'*60}{_RESET}\n"
        f"  Sessions to audit : {_BOLD}{len(found_sessions)}{_RESET}\n"
        f"  Concurrency       : {CONCURRENCY}\n"
        f"  Premium cap       : {PREMIUM_SCAN_CAP} members\n"
        f"  Timeout/session   : {ACCOUNT_TIMEOUT_S}s\n"
        f"{_CYAN}{'─'*60}{_RESET}\n",
        flush=True,
    )

    # ── Phase 2: Telegram audit ───────────────────────────────────────────────
    rows = asyncio.run(run_audit(found_sessions, active_dir, errors_dir, API_ID, API_HASH))

    # ── Phase 3: CSV export ───────────────────────────────────────────────────
    csv_path = base / CSV_OUTPUT
    export_csv(rows, csv_path)

    print(
        f"\n{_GREEN}[DONE]{_RESET}\n"
        f"  Active sessions → {active_dir}\n"
        f"  Error sessions  → {errors_dir}\n"
        f"  Audit CSV       → {csv_path}\n",
        flush=True,
    )


if __name__ == "__main__":
    main()
