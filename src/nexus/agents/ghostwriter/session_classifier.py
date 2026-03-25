"""
Session Classifier — after the audit is complete, takes all ACTIVE (working)
accounts and organises their files into clean output folders:

Output structure:
    classified/
        sessions_with_json/
            +972501234567/
                +972501234567.session
                +972501234567.json
            +972509999999/
                ...
        tdata/
            +972501234567/          ← named after the phone number
                tdata/              ← original tdata folder contents
                    ...
        summary.txt                 ← quick OK list

The classifier:
  1. Reads the audit results (list[AccountAudit] or JSON checkpoint)
  2. For each ACTIVE account, searches for:
       a. Matching .json file (same stem, anywhere near the .session)
       b. tdata/ folder (sibling of .session, or inside the same ZIP's staging)
  3. Copies everything to the output folder
  4. Prints "OK" for each successfully classified account
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Callable

# ── ANSI ──────────────────────────────────────────────────────────────────────
RESET  = "\033[0m"; BOLD  = "\033[1m"
GREEN  = "\033[92m"; YELLOW = "\033[93m"
RED    = "\033[91m"; CYAN   = "\033[96m"
DIM    = "\033[2m";  MAGENTA = "\033[95m"


def _log(msg: str, level: str = "info") -> None:
    colours = {"info": GREEN, "warning": YELLOW, "error": RED, "debug": DIM, "ok": CYAN}
    c  = colours.get(level, "")
    ts = time.strftime("%H:%M:%S")
    print(f"{DIM}[{ts}]{RESET} {c}{BOLD}{level.upper():<7}{RESET} {msg}")
    sys.stdout.flush()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_copy(src: Path, dst: Path) -> bool:
    """Copy src → dst, creating parent dirs. Returns True on success."""
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        return True
    except Exception as e:
        _log(f"Copy failed {src} → {dst}: {e}", "error")
        return False


def _safe_copytree(src: Path, dst: Path) -> bool:
    """Copy entire directory tree src → dst."""
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src, dst)
        return True
    except Exception as e:
        _log(f"Copytree failed {src} → {dst}: {e}", "error")
        return False


def _find_json_for_session(session_path: Path) -> Path | None:
    """
    Search for a matching .json file with the same stem as the session.
    Search order:
      1. Same directory as .session
      2. Parent directory
      3. Staging directory (AppData/Temp/nexus_sessions_staging)
      4. vault/sessions
    """
    stem = session_path.stem
    candidates = [
        session_path.parent / f"{stem}.json",
        session_path.parent.parent / f"{stem}.json",
    ]

    import tempfile
    staging = Path(tempfile.gettempdir()) / "nexus_sessions_staging" / f"{stem}.json"
    candidates.append(staging)

    for c in candidates:
        if c.exists():
            return c
    return None


def _find_tdata_for_session(session_path: Path) -> Path | None:
    """
    Search for a tdata/ folder associated with this session.
    Checks:
      1. session_path.parent / "tdata"
      2. session_path.parent / stem / "tdata"   (common pattern: phone/tdata)
      3. session_path.parent.parent / stem / "tdata"
      4. session_path.parent.parent / "tdata"
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


def _phone_label(session_path: Path) -> str:
    """Return a clean phone label like +972501234567."""
    stem = session_path.stem
    if stem.startswith("+"):
        return stem
    if stem[0].isdigit():
        return "+" + stem
    return stem


# ── Main classifier ───────────────────────────────────────────────────────────

class SessionClassifier:
    def __init__(
        self,
        output_dir: Path,
        log: Callable[[str, str], None] | None = None,
    ) -> None:
        self.output_dir = output_dir
        self.log = log or _log

        self.dir_sessions_json = output_dir / "sessions_with_json"
        self.dir_tdata         = output_dir / "tdata"
        self.dir_sessions_only = output_dir / "sessions_no_json"

        for d in (self.dir_sessions_json, self.dir_tdata, self.dir_sessions_only):
            d.mkdir(parents=True, exist_ok=True)

    # ── Classify from audit results ───────────────────────────────────────────

    def classify_from_audit(self, audits: list) -> dict:
        """
        audits: list[AccountAudit] from session_auditor.py
        Returns summary dict.
        """
        active = [
            a for a in audits
            if not a.is_banned
            and not a.is_unregistered
            and not a.error
        ]
        self.log(
            f"Classifying {len(active)} active accounts "
            f"(out of {len(audits)} total audited)",
            "info",
        )
        return self._process(active)

    def classify_from_checkpoint(self, checkpoint_path: Path) -> dict:
        """Load from JSON checkpoint file saved by audit_runner."""
        data = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        active = [
            d for d in data
            if not d.get("banned") and not d.get("unregistered") and not d.get("error")
        ]
        self.log(
            f"Loaded checkpoint: {len(data)} total, {len(active)} active",
            "info",
        )

        # Convert dicts to simple namespace objects
        class _A:
            pass

        result_list = []
        for d in active:
            a = _A()
            a.session_path   = d["session"]
            a.phone          = d["phone"]
            a.is_banned      = d.get("banned", False)
            a.is_unregistered = d.get("unregistered", False)
            a.error          = d.get("error", "")
            result_list.append(a)

        return self._process(result_list)

    def classify_from_session_list(self, session_paths: list[Path]) -> dict:
        """Classify a plain list of session paths (no audit data needed)."""
        class _A:
            pass

        result_list = []
        for sp in session_paths:
            a = _A()
            a.session_path   = str(sp)
            a.phone          = _phone_label(sp)
            a.is_banned      = False
            a.is_unregistered = False
            a.error          = ""
            result_list.append(a)

        return self._process(result_list)

    # ── Core processing ───────────────────────────────────────────────────────

    def _process(self, active_accounts: list) -> dict:
        ok_list:      list[str] = []
        missing_json: list[str] = []
        has_tdata:    list[str] = []
        failed:       list[str] = []

        total = len(active_accounts)

        for i, account in enumerate(active_accounts, 1):
            session_path = Path(account.session_path)
            phone        = _phone_label(session_path)

            if not session_path.exists():
                self.log(f"[{i}/{total}] {phone} — session file missing, skipping", "warning")
                failed.append(phone)
                continue

            # ── Find companions ───────────────────────────────────────────────
            json_path  = _find_json_for_session(session_path)
            tdata_path = _find_tdata_for_session(session_path)

            # ── Copy session + json ───────────────────────────────────────────
            if json_path:
                dest_dir = self.dir_sessions_json / phone
                dest_dir.mkdir(parents=True, exist_ok=True)

                ok1 = _safe_copy(session_path, dest_dir / session_path.name)
                ok2 = _safe_copy(json_path,    dest_dir / json_path.name)

                if ok1 and ok2:
                    ok_list.append(phone)
                    self.log(f"[{i}/{total}] {GREEN}OK{RESET}  {phone}  [session + json]", "ok")
                else:
                    failed.append(phone)
            else:
                # Session only — no JSON found
                dest_dir = self.dir_sessions_only / phone
                dest_dir.mkdir(parents=True, exist_ok=True)
                ok1 = _safe_copy(session_path, dest_dir / session_path.name)
                if ok1:
                    missing_json.append(phone)
                    self.log(
                        f"[{i}/{total}] {YELLOW}OK (no json){RESET}  {phone}",
                        "warning",
                    )
                else:
                    failed.append(phone)

            # ── Copy tdata ────────────────────────────────────────────────────
            if tdata_path:
                tdata_dest = self.dir_tdata / phone / "tdata"
                ok3 = _safe_copytree(tdata_path, tdata_dest)
                if ok3:
                    has_tdata.append(phone)
                    self.log(
                        f"  {CYAN}tdata{RESET} copied for {phone} "
                        f"({sum(1 for _ in tdata_path.rglob('*'))} files)",
                        "info",
                    )

        # ── Write summary.txt ─────────────────────────────────────────────────
        summary_path = self.output_dir / "summary.txt"
        lines = [
            "=" * 60,
            "NEXUS SESSION CLASSIFIER — SUMMARY",
            f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            "=" * 60,
            f"Total active accounts processed : {total}",
            f"Session + JSON (full)           : {len(ok_list)}",
            f"Session only (no JSON found)    : {len(missing_json)}",
            f"With tdata folder               : {len(has_tdata)}",
            f"Failed / skipped                : {len(failed)}",
            "",
            "─" * 60,
            "ACTIVE ACCOUNTS (session + json):",
            "─" * 60,
        ]
        for phone in sorted(ok_list):
            lines.append(f"  OK  {phone}")

        if missing_json:
            lines += ["", "─" * 60, "SESSION ONLY (no json):", "─" * 60]
            for phone in sorted(missing_json):
                lines.append(f"  OK (no json)  {phone}")

        if has_tdata:
            lines += ["", "─" * 60, "WITH TDATA:", "─" * 60]
            for phone in sorted(has_tdata):
                lines.append(f"  tdata  {phone}")

        if failed:
            lines += ["", "─" * 60, "FAILED:", "─" * 60]
            for phone in sorted(failed):
                lines.append(f"  FAIL  {phone}")

        summary_path.write_text("\n".join(lines), encoding="utf-8")

        # ── Final print ───────────────────────────────────────────────────────
        print(f"""
{CYAN}{BOLD}+------------------- CLASSIFICATION DONE -------------------+{RESET}
  Output folder    : {self.output_dir}
  Session + JSON   : {GREEN}{BOLD}{len(ok_list)}{RESET}  → {self.dir_sessions_json}
  Session only     : {YELLOW}{len(missing_json)}{RESET}  → {self.dir_sessions_only}
  tdata copied     : {CYAN}{len(has_tdata)}{RESET}  → {self.dir_tdata}
  Failed           : {RED}{len(failed)}{RESET}
  Summary file     : {summary_path}
{CYAN}{BOLD}+-----------------------------------------------------------+{RESET}

{GREEN}{BOLD}OK — Classification complete. All active sessions organised.{RESET}
        """)

        return {
            "ok":           ok_list,
            "missing_json": missing_json,
            "has_tdata":    has_tdata,
            "failed":       failed,
            "output_dir":   str(self.output_dir),
        }


# ── Standalone CLI ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import tempfile

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="Classify active Telegram sessions")
    parser.add_argument("--checkpoint", help="Path to audit JSON checkpoint file")
    parser.add_argument("--sessions-dir", help="Folder of .session files to classify directly")
    parser.add_argument(
        "--output", default="classified",
        help="Output base folder (default: ./classified)",
    )
    parser.add_argument(
        "--only-israeli", action="store_true",
        help="Only classify +972 accounts",
    )
    args = parser.parse_args()

    out = Path(args.output)
    classifier = SessionClassifier(output_dir=out)

    if args.checkpoint:
        classifier.classify_from_checkpoint(Path(args.checkpoint))

    elif args.sessions_dir:
        d = Path(args.sessions_dir)
        sessions = list(d.glob("*.session"))
        if args.only_israeli:
            sessions = [s for s in sessions if s.stem.startswith("972") or s.stem.startswith("+972")]
        classifier.classify_from_session_list(sessions)

    else:
        # Default: use staging + vault
        staging = Path(tempfile.gettempdir()) / "nexus_sessions_staging"
        vault   = Path(__file__).resolve().parents[4] / "vault" / "sessions"
        files   = list(staging.glob("*.session")) + list(vault.glob("*.session"))
        seen: set[str] = set()
        unique: list[Path] = []
        for f in files:
            if f.stem not in seen:
                seen.add(f.stem)
                unique.append(f)
        if args.only_israeli:
            unique = [f for f in unique if f.stem.startswith("972") or f.stem.startswith("+972")]
        classifier.classify_from_session_list(unique)
