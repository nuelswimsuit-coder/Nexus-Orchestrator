"""
Session Scanner — scans the entire machine for .session files (+ matching .json),
including inside ZIP and RAR archives. Extracts found sessions to a staging folder
so Telethon can use them directly.
"""

from __future__ import annotations

import io
import json
import os
import shutil
import sys
import tempfile
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

# RAR support is optional — needs `rarfile` + unrar binary
try:
    import rarfile
    RAR_AVAILABLE = True
except ImportError:
    RAR_AVAILABLE = False

# ── ANSI colours (same palette as main.py) ────────────────────────────────────
RESET   = "\033[0m"
BOLD    = "\033[1m"
GREEN   = "\033[92m"
YELLOW  = "\033[93m"
RED     = "\033[91m"
CYAN    = "\033[96m"
MAGENTA = "\033[95m"
DIM     = "\033[2m"

# Skip these folders entirely — they're noise or dangerous to recurse
SKIP_DIRS = {
    "$Recycle.Bin", "System Volume Information", "Windows",
    "Program Files", "Program Files (x86)",
    "AppData\\Local\\Temp", "AppData\\LocalLow",
    "AppData\\Local\\Programs",       # installed apps (Cursor, VS Code, etc.)
    "AppData\\Local\\Microsoft",      # Windows/Office internals
    "AppData\\Local\\Google",         # Chrome cache
    "AppData\\Local\\BraveSoftware",
    "AppData\\Local\\Mozilla",
    "AppData\\Local\\Steam",
    "AppData\\Local\\Razer",
    "AppData\\Local\\Discord",
    "AppData\\Local\\slack",
    "AppData\\Local\\Packages",       # UWP apps
    "AppData\\Local\\Publishers",
    "AppData\\Roaming\\npm",
    "AppData\\Roaming\\Code",         # VS Code extensions
    "AppData\\Roaming\\cursor",
    "__pycache__", ".git", "node_modules", ".venv", "venv",
    "steamapps",
}


@dataclass
class FoundSession:
    session_path: Path          # Path to the .session file (may be in staging if extracted)
    json_path: Path | None      # Matching .json file (same stem), or None
    source: str                 # "disk" | "zip" | "rar"
    archive_path: Path | None   # Original archive path (if extracted)
    phone: str                  # Derived from filename stem


@dataclass
class ScanResult:
    sessions: list[FoundSession] = field(default_factory=list)
    scanned_dirs: int = 0
    scanned_archives: int = 0
    skipped_errors: list[str] = field(default_factory=list)
    elapsed_seconds: float = 0.0

    @property
    def total(self) -> int:
        return len(self.sessions)

    @property
    def with_json(self) -> int:
        return sum(1 for s in self.sessions if s.json_path is not None)

    @property
    def from_disk(self) -> int:
        return sum(1 for s in self.sessions if s.source == "disk")

    @property
    def from_archives(self) -> int:
        return sum(1 for s in self.sessions if s.source in ("zip", "rar"))


def _should_skip(path: Path) -> bool:
    parts = path.parts
    # Check single-component matches
    for part in parts:
        if part in SKIP_DIRS:
            return True
    # Check multi-component suffix matches (e.g. "AppData\\Local\\Programs")
    for skip in SKIP_DIRS:
        if "\\" in skip:
            skip_parts = tuple(skip.split("\\"))
            n = len(skip_parts)
            for i in range(len(parts) - n + 1):
                if parts[i:i + n] == skip_parts:
                    return True
    return False


def _stem_to_phone(stem: str) -> str:
    """Best-effort: return the stem as phone (already looks like +972XXXXXX)."""
    s = stem.strip()
    if s and not s.startswith("+") and s[0].isdigit():
        s = "+" + s
    return s


class SessionScanner:
    def __init__(
        self,
        scan_roots: list[str] | None = None,
        staging_dir: Path | None = None,
        log: Callable[[str, str], None] | None = None,
    ) -> None:
        # Default: scan all drive roots on Windows, / on Linux
        if scan_roots:
            self.scan_roots = [Path(r) for r in scan_roots]
        else:
            self.scan_roots = self._detect_roots()

        # Staging dir: extracted archive sessions land here
        self.staging_dir = staging_dir or Path(tempfile.gettempdir()) / "nexus_sessions_staging"
        self.staging_dir.mkdir(parents=True, exist_ok=True)

        self._log = log or self._default_log
        self._seen_stems: set[str] = set()   # deduplicate by session stem

    # ── Logging ───────────────────────────────────────────────────────────────

    @staticmethod
    def _default_log(msg: str, level: str = "info") -> None:
        colours = {"info": GREEN, "warning": YELLOW, "error": RED, "debug": DIM, "scan": CYAN}
        c = colours.get(level, "")
        ts = time.strftime("%H:%M:%S")
        tag = level.upper().ljust(7)
        print(f"{DIM}[{ts}]{RESET} {c}{BOLD}{tag}{RESET} {msg}")
        sys.stdout.flush()

    def _log_scan(self, path: str) -> None:
        # Overwrite same line for speed — looks like a live scanner
        print(f"\r{DIM}[SCAN]{RESET} {CYAN}{path[:100]:<100}{RESET}", end="", flush=True)

    # ── Root detection ────────────────────────────────────────────────────────

    @staticmethod
    def _detect_roots() -> list[Path]:
        if os.name == "nt":
            import string
            roots = []
            for letter in string.ascii_uppercase:
                p = Path(f"{letter}:\\")
                if p.exists():
                    roots.append(p)
            return roots or [Path("C:\\")]
        return [Path("/")]

    # ── Main scan ─────────────────────────────────────────────────────────────

    def scan(self) -> ScanResult:
        result = ScanResult()
        t0 = time.monotonic()

        self._log(
            f"Starting full-disk scan on: {[str(r) for r in self.scan_roots]}",
            "info",
        )
        self._log(
            f"RAR support: {'YES' if RAR_AVAILABLE else 'NO (pip install rarfile)'}",
            "info" if RAR_AVAILABLE else "warning",
        )
        self._log("Scanning — this may take a minute on large drives...\n", "info")

        for root in self.scan_roots:
            self._scan_tree(root, result)

        print()  # newline after live scan line
        result.elapsed_seconds = time.monotonic() - t0
        return result

    def _scan_tree(self, root: Path, result: ScanResult) -> None:
        try:
            for dirpath, dirnames, filenames in os.walk(root, topdown=True, onerror=None):
                current = Path(dirpath)

                # Prune skip dirs in-place so os.walk doesn't recurse into them
                dirnames[:] = [
                    d for d in dirnames
                    if not _should_skip(current / d)
                ]

                result.scanned_dirs += 1
                self._log_scan(str(current))

                # Collect all names in this dir for JSON matching
                name_set = set(filenames)

                for fname in filenames:
                    fpath = current / fname
                    ext = fpath.suffix.lower()

                    if ext == ".session":
                        self._register_disk_session(fpath, name_set, result)

                    elif ext == ".zip":
                        self._scan_zip(fpath, result)
                        result.scanned_archives += 1

                    elif ext == ".rar" and RAR_AVAILABLE:
                        self._scan_rar(fpath, result)
                        result.scanned_archives += 1

        except PermissionError:
            pass

    # ── Disk session ──────────────────────────────────────────────────────────

    def _register_disk_session(
        self, session_path: Path, sibling_names: set[str], result: ScanResult
    ) -> None:
        stem = session_path.stem
        if stem in self._seen_stems:
            return
        self._seen_stems.add(stem)

        json_name = stem + ".json"
        json_path = (session_path.parent / json_name) if json_name in sibling_names else None

        result.sessions.append(FoundSession(
            session_path=session_path,
            json_path=json_path,
            source="disk",
            archive_path=None,
            phone=_stem_to_phone(stem),
        ))

    # ── ZIP archive ───────────────────────────────────────────────────────────

    def _scan_zip(self, archive_path: Path, result: ScanResult) -> None:
        try:
            with zipfile.ZipFile(archive_path, "r") as zf:
                names = zf.namelist()
                session_names = [n for n in names if n.lower().endswith(".session")]

                for sname in session_names:
                    stem = Path(sname).stem
                    if stem in self._seen_stems:
                        continue

                    # Extract session file to staging
                    dest = self.staging_dir / f"{stem}.session"
                    try:
                        data = zf.read(sname)
                        dest.write_bytes(data)
                    except Exception as e:
                        result.skipped_errors.append(f"ZIP extract {archive_path}::{sname}: {e}")
                        continue

                    # Look for matching JSON inside same archive
                    json_name_in_zip = str(Path(sname).parent / (stem + ".json")).replace("\\", "/")
                    json_dest: Path | None = None
                    if json_name_in_zip in names or (stem + ".json") in names:
                        actual_json = json_name_in_zip if json_name_in_zip in names else (stem + ".json")
                        json_dest = self.staging_dir / f"{stem}.json"
                        try:
                            json_dest.write_bytes(zf.read(actual_json))
                        except Exception:
                            json_dest = None

                    self._seen_stems.add(stem)
                    result.sessions.append(FoundSession(
                        session_path=dest,
                        json_path=json_dest,
                        source="zip",
                        archive_path=archive_path,
                        phone=_stem_to_phone(stem),
                    ))
        except zipfile.BadZipFile:
            pass
        except Exception as e:
            result.skipped_errors.append(f"ZIP open {archive_path}: {e}")

    # ── RAR archive ───────────────────────────────────────────────────────────

    def _scan_rar(self, archive_path: Path, result: ScanResult) -> None:
        try:
            with rarfile.RarFile(str(archive_path), "r") as rf:
                names = rf.namelist()
                session_names = [n for n in names if n.lower().endswith(".session")]

                for sname in session_names:
                    stem = Path(sname).stem
                    if stem in self._seen_stems:
                        continue

                    dest = self.staging_dir / f"{stem}.session"
                    try:
                        data = rf.read(sname)
                        dest.write_bytes(data)
                    except Exception as e:
                        result.skipped_errors.append(f"RAR extract {archive_path}::{sname}: {e}")
                        continue

                    json_name_in_rar = str(Path(sname).parent / (stem + ".json")).replace("\\", "/")
                    json_dest = None
                    if json_name_in_rar in names or (stem + ".json") in names:
                        actual_json = json_name_in_rar if json_name_in_rar in names else (stem + ".json")
                        json_dest = self.staging_dir / f"{stem}.json"
                        try:
                            json_dest.write_bytes(rf.read(actual_json))
                        except Exception:
                            json_dest = None

                    self._seen_stems.add(stem)
                    result.sessions.append(FoundSession(
                        session_path=dest,
                        json_path=json_dest,
                        source="rar",
                        archive_path=archive_path,
                        phone=_stem_to_phone(stem),
                    ))
        except Exception as e:
            result.skipped_errors.append(f"RAR open {archive_path}: {e}")

    # ── Report printer ────────────────────────────────────────────────────────

    def print_report(self, result: ScanResult) -> None:
        sep = "-" * 64
        print(f"\n{CYAN}{BOLD}{sep}{RESET}")
        print(f"{CYAN}{BOLD}  SESSION SCAN COMPLETE{RESET}")
        print(f"{CYAN}{BOLD}{sep}{RESET}")
        print(f"  {BOLD}Total sessions found  :{RESET} {GREEN}{BOLD}{result.total}{RESET}")
        print(f"  {BOLD}  - From disk          :{RESET} {result.from_disk}")
        print(f"  {BOLD}  - From ZIP/RAR       :{RESET} {result.from_archives}")
        print(f"  {BOLD}With matching .json   :{RESET} {result.with_json}")
        print(f"  {BOLD}Dirs scanned          :{RESET} {result.scanned_dirs}")
        print(f"  {BOLD}Archives scanned      :{RESET} {result.scanned_archives}")
        print(f"  {BOLD}Errors / skipped      :{RESET} {len(result.skipped_errors)}")
        print(f"  {BOLD}Time elapsed          :{RESET} {result.elapsed_seconds:.1f}s")
        print(f"{CYAN}{BOLD}{sep}{RESET}\n")

        if result.sessions:
            print(f"{BOLD}{'#':<5} {'PHONE':<25} {'SOURCE':<6} {'JSON':<5} PATH{RESET}")
            print(DIM + "-" * 100 + RESET)
            for i, s in enumerate(result.sessions, 1):
                src_colour = GREEN if s.source == "disk" else YELLOW
                json_mark = f"{GREEN}YES{RESET}" if s.json_path else f"{DIM}no{RESET}"
                archive_note = f"  {DIM}(from {s.archive_path.name}){RESET}" if s.archive_path else ""
                print(
                    f"{DIM}{i:<5}{RESET}"
                    f"{MAGENTA}{s.phone:<25}{RESET}"
                    f"{src_colour}{s.source:<6}{RESET}"
                    f"{json_mark:<5}"
                    f"{str(s.session_path)}{archive_note}"
                )

        if result.skipped_errors:
            print(f"\n{YELLOW}{BOLD}Errors / skipped:{RESET}")
            for e in result.skipped_errors[:20]:
                print(f"  {DIM}{e}{RESET}")
            if len(result.skipped_errors) > 20:
                print(f"  {DIM}... and {len(result.skipped_errors) - 20} more{RESET}")

        print()


# ── Standalone runner ─────────────────────────────────────────────────────────

def run_scan(
    scan_roots: list[str] | None = None,
    staging_dir: Path | None = None,
) -> ScanResult:
    """Run a full scan and print the report. Returns ScanResult for programmatic use."""
    scanner = SessionScanner(scan_roots=scan_roots, staging_dir=staging_dir)
    result = scanner.scan()
    scanner.print_report(result)
    return result


if __name__ == "__main__":
    import argparse

    # Force UTF-8 on Windows
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="Scan entire machine for Telegram .session files")
    parser.add_argument(
        "--roots", nargs="*",
        help="Specific root paths to scan (default: all drives)",
    )
    parser.add_argument(
        "--staging", type=Path,
        default=Path(tempfile.gettempdir()) / "nexus_sessions_staging",
        help="Folder where extracted archive sessions are saved",
    )
    args = parser.parse_args()

    run_scan(scan_roots=args.roots, staging_dir=args.staging)
