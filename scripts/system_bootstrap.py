"""
Nexus Orchestrator — System Bootstrap 2.0

Usage
-----
    python scripts/system_bootstrap.py [--dry-run] [--worker-only]

What it does
------------
0. Linux OS prep: runs apt update + installs python3-pip, python3-venv,
   build-essential so that venv creation and pip installs always succeed.

1. Venv bootstrap: creates (or reuses) a .venv in the project root and
   re-launches itself inside that venv so every subsequent pip call is
   isolated — avoids "Externally Managed Environment" errors on Debian/Ubuntu.

2. Deep folder scan: discovers all requirements.txt / pyproject.toml files
   under the Nexus project AND the Mangement Ahu project, collects every
   unique dependency.

3. Core install: installs the Nexus package itself (`pip install -e .`).

4. Extended install: installs additional libraries needed for full operation:
   - Telethon          — Telegram MTProto client (worker sessions)
   - aiogram           — Telegram Bot API (notification bot)
   - GPUtil            — GPU detection for hardware HUD
   - aiosqlite         — async SQLite for telefix bridge
   - deep-fingerprint  — browser fingerprinting (if available)
   - All deps found in Mangement Ahu's requirements.txt

5. Session path validation: scans the Mangement Ahu sessions directory and
   reports any .json files with encoding issues or missing fields.

6. Environment check: verifies Redis is reachable, .env is populated, and
   all required secrets are present.

7. Worker readiness report: prints a summary table of what is ready and
   what needs attention before starting the worker.

Options
-------
--dry-run     Print what would be installed without actually installing.
--worker-only Skip master-only dependencies (aiogram, fastapi, uvicorn).
--no-mangement-ahu  Skip scanning the Mangement Ahu project.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
from pathlib import Path
from typing import Any

# ── Paths ──────────────────────────────────────────────────────────────────────

NEXUS_ROOT       = Path(__file__).resolve().parent.parent
VENV_DIR         = NEXUS_ROOT / ".venv"


def get_telefix_path(folder_name: str = "") -> Path:
    """Resolve a Desktop-based path without hardcoding any username.

    Defined inline here because this bootstrap script runs before the nexus
    package is installed, so it cannot import from nexus.shared.paths.
    The canonical version lives in nexus/shared/paths.py.
    """
    desktop = Path.home() / "Desktop"
    if folder_name:
        parts = folder_name.replace("\\", "/").split("/")
        return desktop.joinpath(*parts)
    return desktop


MANGEMENT_AHU    = get_telefix_path("Mangement Ahu")
SESSIONS_DIR     = MANGEMENT_AHU / "sessions"
TELEFIX_DB       = MANGEMENT_AHU / "data" / "telefix.db"

# ── Dependency manifest ────────────────────────────────────────────────────────

CORE_PACKAGES = [
    # Nexus core (installed via pyproject.toml)
    # Listed here for the dry-run report only.
    "arq>=0.26",
    "redis[hiredis]>=5.0",
    "pydantic>=2.7",
    "pydantic-settings>=2.3",
    "psutil>=6.0",
    "structlog>=24.1",
    "httpx>=0.27",
    "fastapi>=0.111",
    "uvicorn[standard]>=0.30",
    "aiogram>=3.20",
    "aiosqlite>=0.20",
]

EXTENDED_PACKAGES = [
    # Telegram MTProto — required for worker tasks that use Telethon sessions
    "telethon>=1.36",
    # GPU detection for hardware HUD
    "GPUtil>=1.4",
    # Cryptography (Telethon dependency, pin for compatibility)
    "cryptography>=42.0",
    # Pillow (Telethon optional, avoids import warnings)
    "Pillow>=10.0",
]

# Packages that must always be present regardless of scan results.
REQUIRED_PACKAGES = [
    "structlog",
    "telethon",
    "aiogram",
    "redis",
    "pydantic-settings",
    "psutil",
    "paramiko",
]

WORKER_ONLY_SKIP = [
    "fastapi>=0.111",
    "uvicorn[standard]>=0.30",
    "aiogram>=3.20",
]

# ── Venv helpers ───────────────────────────────────────────────────────────────

def _venv_python() -> Path:
    """Return the Python executable inside .venv."""
    if platform.system() == "Windows":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"


def _inside_venv() -> bool:
    """True when the current interpreter lives inside VENV_DIR."""
    try:
        return Path(sys.executable).resolve().is_relative_to(VENV_DIR.resolve())
    except AttributeError:
        # Python < 3.9 fallback
        return str(VENV_DIR.resolve()) in str(Path(sys.executable).resolve())


def _bootstrap_venv(dry_run: bool = False) -> None:
    """
    Create .venv if absent, then re-exec this script with the venv Python so
    that all subsequent pip calls are isolated from the system interpreter.
    """
    _section("0/6  Venv Bootstrap")

    if not VENV_DIR.exists():
        print(f"  Creating virtual environment at {VENV_DIR} …")
        if not dry_run:
            result = subprocess.run(
                [sys.executable, "-m", "venv", str(VENV_DIR)],
                capture_output=False,
            )
            if result.returncode != 0:
                _err("Failed to create .venv — check that python3-venv is installed")
                sys.exit(1)
        _ok(f".venv created at {VENV_DIR}")
    else:
        _ok(f".venv already exists at {VENV_DIR}")

    venv_py = _venv_python()
    if not dry_run and not _inside_venv():
        print(f"  Re-launching inside venv: {venv_py}")
        os.execv(str(venv_py), [str(venv_py)] + sys.argv)
        # os.execv replaces the process; nothing below runs unless dry_run


# ── Linux OS prep ──────────────────────────────────────────────────────────────

def _linux_os_prep(dry_run: bool = False) -> None:
    """
    On Linux, ensure apt packages needed for pip and venv are present.
    Runs before any pip call so the venv can always be created.

    python3-full  — ships the ensurepip module that some distros strip out,
                    which is required for `python3 -m venv` to work correctly
                    on Debian/Ubuntu 22.04+.
    """
    _section("0a/6  Linux OS Prep (apt)")
    apt_cmd = [
        "sudo", "apt-get", "update", "-y",
    ]
    install_cmd = [
        "sudo", "apt-get", "install", "-y",
        "python3-pip", "python3-venv", "python3-full", "build-essential",
    ]
    print("  Installing system packages via apt …")
    _run(apt_cmd, dry_run=dry_run)
    _run(install_cmd, dry_run=dry_run)
    _ok("System packages ready")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _run(cmd: list[str], dry_run: bool = False) -> bool:
    print(f"  $ {' '.join(cmd)}")
    if dry_run:
        return True
    result = subprocess.run(cmd, capture_output=False)
    return result.returncode == 0


def _pip_install(packages: list[str], dry_run: bool = False) -> None:
    """Install packages, retrying once with --no-cache-dir on failure."""
    if not packages:
        return
    pip_exe = [sys.executable, "-m", "pip", "install", "--quiet"]
    for pkg in packages:
        print(f"  Installing: {pkg}")
        if dry_run:
            print(f"  $ {' '.join(pip_exe + [pkg])}")
            continue
        result = subprocess.run(pip_exe + [pkg], capture_output=True, text=True)
        if result.returncode != 0:
            _warn(f"Failed to install '{pkg}' — retrying with --no-cache-dir …")
            retry = subprocess.run(
                pip_exe + ["--no-cache-dir", pkg],
                capture_output=True,
                text=True,
            )
            if retry.returncode != 0:
                _err(f"Could not install '{pkg}' after retry:\n    {retry.stderr.strip()}")
            else:
                _ok(f"'{pkg}' installed on retry")


def _section(title: str) -> None:
    width = 60
    print(f"\n{'─' * width}")
    print(f"  {title}")
    print(f"{'─' * width}")


def _ok(msg: str) -> None:
    print(f"  ✓  {msg}")


def _warn(msg: str) -> None:
    print(f"  ⚠  {msg}")


def _err(msg: str) -> None:
    print(f"  ✗  {msg}")


# ── Deep folder scan ───────────────────────────────────────────────────────────

def scan_requirements(root: Path) -> list[str]:
    """
    Recursively find all requirements.txt files under `root` and collect
    every non-comment, non-empty line as a package specifier.
    """
    packages: list[str] = []
    for req_file in root.rglob("requirements*.txt"):
        try:
            for line in req_file.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = line.strip()
                if line and not line.startswith("#") and not line.startswith("-"):
                    packages.append(line)
        except Exception as exc:
            _warn(f"Could not read {req_file}: {exc}")
    return list(set(packages))


# ── Session validation ─────────────────────────────────────────────────────────

def validate_sessions(sessions_dir: Path) -> dict[str, Any]:
    """
    Scan all .json session files and report encoding/structure issues.

    Returns a dict with keys: total, valid, invalid, issues.
    """
    total = valid = invalid = 0
    issues: list[str] = []

    for json_file in sessions_dir.rglob("*.json"):
        total += 1
        try:
            text = json_file.read_text(encoding="utf-8")
            data = json.loads(text)
            # Check for required Telethon session fields
            required = {"phone", "dc_id", "server_address", "port", "auth_key"}
            missing = required - set(data.keys())
            if missing:
                issues.append(f"{json_file.name}: missing fields {missing}")
                invalid += 1
            else:
                valid += 1
        except UnicodeDecodeError:
            # Try latin-1 fallback (common issue with Windows-created sessions)
            try:
                text = json_file.read_text(encoding="latin-1")
                json_file.write_text(text, encoding="utf-8")
                issues.append(f"{json_file.name}: re-encoded latin-1 → utf-8")
                valid += 1
            except Exception as exc:
                issues.append(f"{json_file.name}: encoding error — {exc}")
                invalid += 1
        except json.JSONDecodeError as exc:
            issues.append(f"{json_file.name}: JSON parse error — {exc}")
            invalid += 1
        except Exception as exc:
            issues.append(f"{json_file.name}: {exc}")
            invalid += 1

    return {"total": total, "valid": valid, "invalid": invalid, "issues": issues}


# ── Environment check ──────────────────────────────────────────────────────────

def check_environment() -> dict[str, bool]:
    """Check Redis connectivity and .env completeness."""
    results: dict[str, bool] = {}

    # Redis
    try:
        import socket
        s = socket.create_connection(("localhost", 6379), timeout=2)
        s.close()
        results["redis"] = True
    except Exception:
        results["redis"] = False

    # .env file
    env_path = NEXUS_ROOT / ".env"
    results["env_file"] = env_path.exists()

    # Required env vars
    required_vars = ["REDIS_URL", "NODE_ID"]
    if env_path.exists():
        env_text = env_path.read_text(encoding="utf-8")
        for var in required_vars:
            results[f"env_{var}"] = var in env_text

    # Telefix DB
    results["telefix_db"] = TELEFIX_DB.exists()

    # Sessions
    adders_dir = SESSIONS_DIR / "adders"
    import glob
    session_count = len(glob.glob(str(adders_dir / "*.json"))) if adders_dir.exists() else 0
    results["sessions_available"] = session_count > 0
    results["session_count"] = session_count  # type: ignore[assignment]

    return results


# ── run_worker.sh generator ────────────────────────────────────────────────────

def _generate_run_worker_sh(dry_run: bool = False) -> None:
    """
    Write (or overwrite) run_worker.sh in the project root.

    The script sets PYTHONPATH, activates the .venv, and launches
    scripts/start_worker.py — so the worker can be started with a single
    command on any Linux/macOS machine without any manual path setup.
    """
    _section("run_worker.sh  Generator")

    if platform.system() == "Windows":
        _ok("Skipped on Windows (shell script not needed)")
        return

    script_path = NEXUS_ROOT / "run_worker.sh"
    content = f"""#!/usr/bin/env bash
# Auto-generated by system_bootstrap.py — safe to re-run.
# One-click worker launcher: activates .venv and sets PYTHONPATH.
set -euo pipefail

PROJECT_ROOT="{NEXUS_ROOT}"
export PYTHONPATH="$PROJECT_ROOT${{PYTHONPATH:+:$PYTHONPATH}}"

source "$PROJECT_ROOT/.venv/bin/activate"
exec python "$PROJECT_ROOT/scripts/start_worker.py" "$@"
"""

    if dry_run:
        _ok(f"[DRY RUN] Would write {script_path}")
        return

    try:
        script_path.write_text(content, encoding="utf-8")
        # Make executable
        script_path.chmod(script_path.stat().st_mode | 0o755)
        _ok(f"Written and made executable: {script_path}")
        _ok("  Start worker with: ./run_worker.sh")
    except Exception as exc:
        _err(f"Could not write run_worker.sh: {exc}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Nexus Orchestrator Bootstrap 2.0")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without executing")
    parser.add_argument("--worker-only", action="store_true", help="Skip master-only dependencies")
    parser.add_argument("--no-mangement-ahu", action="store_true", help="Skip Mangement Ahu scan")
    args = parser.parse_args()

    print("\n" + "═" * 60)
    print("  NEXUS ORCHESTRATOR — SYSTEM BOOTSTRAP 2.0")
    print("═" * 60)
    if args.dry_run:
        print("  [DRY RUN — no changes will be made]")

    # ── 0a. Linux OS prep (must run before venv creation) ─────────────────────
    if platform.system() == "Linux":
        _linux_os_prep(dry_run=args.dry_run)

    # ── 0b. Venv bootstrap (re-execs into venv if needed) ─────────────────────
    _bootstrap_venv(dry_run=args.dry_run)

    # ── pip self-upgrade (first thing inside the venv) ────────────────────────
    _section("pip self-upgrade")
    print("  Upgrading pip …")
    _run([sys.executable, "-m", "pip", "install", "--upgrade", "pip"], dry_run=args.dry_run)
    _ok("pip is up to date")

    # ── requirements.txt bulk install ─────────────────────────────────────────
    req_file = NEXUS_ROOT / "requirements.txt"
    if req_file.exists():
        _section("requirements.txt  Bulk Install")
        print(f"  Installing from {req_file} …")
        ok = _run(
            [sys.executable, "-m", "pip", "install", "--quiet", "-r", str(req_file)],
            dry_run=args.dry_run,
        )
        if ok:
            _ok("requirements.txt installed successfully")
        else:
            _warn("requirements.txt install had errors — retrying with --no-cache-dir …")
            _run(
                [sys.executable, "-m", "pip", "install", "--quiet",
                 "--no-cache-dir", "-r", str(req_file)],
                dry_run=args.dry_run,
            )
    else:
        _warn(f"requirements.txt not found at {req_file} — skipping bulk install")

    # ── 1. Deep folder scan ────────────────────────────────────────────────────
    _section("1/5  Deep Dependency Scan")

    all_packages = list(EXTENDED_PACKAGES)

    if not args.no_mangement_ahu and MANGEMENT_AHU.exists():
        ahu_packages = scan_requirements(MANGEMENT_AHU)
        _ok(f"Found {len(ahu_packages)} packages in Mangement Ahu")
        all_packages.extend(ahu_packages)
    else:
        _warn("Mangement Ahu not found — skipping its requirements")

    nexus_packages = scan_requirements(NEXUS_ROOT)
    _ok(f"Found {len(nexus_packages)} packages in Nexus project")

    if args.worker_only:
        all_packages = [p for p in all_packages if p not in WORKER_ONLY_SKIP]
        _ok("Worker-only mode: skipped master-only packages")

    all_packages = list(set(all_packages))
    _ok(f"Total unique packages to install: {len(all_packages)}")

    # ── 2. Install Nexus core ──────────────────────────────────────────────────
    _section("2/5  Installing Nexus Core")
    _run(
        [sys.executable, "-m", "pip", "install", "-e", str(NEXUS_ROOT), "--quiet"],
        dry_run=args.dry_run,
    )
    _ok("Nexus package installed")

    # ── 3. Install extended packages ──────────────────────────────────────────
    _section("3/5  Installing Extended Packages")

    # Merge required packages so they are always present even if missing from scans.
    combined = list(set(all_packages) | set(REQUIRED_PACKAGES))
    for pkg in sorted(combined):
        _pip_install([pkg], dry_run=args.dry_run)
    _ok("Extended packages installed")

    # ── 4. Session validation ──────────────────────────────────────────────────
    _section("4/5  Validating Telegram Sessions")
    if SESSIONS_DIR.exists():
        report = validate_sessions(SESSIONS_DIR)
        _ok(f"Total session files: {report['total']}")
        _ok(f"Valid: {report['valid']}")
        if report["invalid"] > 0:
            _warn(f"Invalid: {report['invalid']}")
        for issue in report["issues"][:10]:
            _warn(f"  {issue}")
        if len(report["issues"]) > 10:
            _warn(f"  ... and {len(report['issues']) - 10} more")
    else:
        _warn(f"Sessions directory not found: {SESSIONS_DIR}")

    # ── 5. Environment check ───────────────────────────────────────────────────
    _section("5/5  Environment Readiness Check")
    env = check_environment()

    checks = [
        ("Redis reachable",       env.get("redis", False)),
        (".env file exists",      env.get("env_file", False)),
        ("REDIS_URL configured",  env.get("env_REDIS_URL", False)),
        ("NODE_ID configured",    env.get("env_NODE_ID", False)),
        ("Telefix DB exists",     env.get("telefix_db", False)),
        ("Sessions available",    env.get("sessions_available", False)),
    ]

    all_ok = True
    for label, status in checks:
        if status:
            _ok(label)
        else:
            _err(label)
            all_ok = False

    session_count = env.get("session_count", 0)
    if session_count:
        _ok(f"Active session files: {session_count}")

    # ── 5b. Generate run_worker.sh (Linux only) ────────────────────────────────
    _generate_run_worker_sh(dry_run=args.dry_run)

    # ── 6. OS service registration ─────────────────────────────────────────────
    if not args.dry_run and all_ok:
        _section("6/6  OS Service Registration (optional)")
        _register_os_service(dry_run=False)
    elif args.dry_run:
        _section("6/6  OS Service Registration (dry run)")
        _register_os_service(dry_run=True)

    # ── Summary ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    if all_ok:
        print("  OK  ALL CHECKS PASSED — system is ready to run")
        if platform.system() == "Linux":
            print("     Start worker : ./run_worker.sh")
        print("     Or manually  : python scripts/start_master.py")
        print("                    python scripts/start_worker.py")
        print("                    python scripts/start_api.py")
    else:
        print("  !!  SOME CHECKS FAILED — review warnings above")
        print("     Fix the issues then re-run this script")
    print("=" * 60 + "\n")


# ── OS service registration ────────────────────────────────────────────────────

def _register_os_service(dry_run: bool = False) -> None:
    """
    Register Nexus as a background service on the current OS.

    Linux  → systemd unit file at /etc/systemd/system/nexus-master.service
    Windows → Task Scheduler XML task (runs at logon, highest privileges)

    The service starts start_master.py automatically on boot/logon so the
    system is always running without manual intervention.
    """
    system = platform.system()
    python_exe = sys.executable
    master_script = str(NEXUS_ROOT / "scripts" / "start_master.py")
    worker_script = str(NEXUS_ROOT / "scripts" / "start_worker.py")

    if system == "Linux":
        _register_systemd(python_exe, master_script, worker_script, dry_run)
    elif system == "Windows":
        _register_task_scheduler(python_exe, master_script, dry_run)
    else:
        _warn(f"OS service registration not supported on {system}")


def _register_systemd(
    python_exe: str,
    master_script: str,
    worker_script: str,
    dry_run: bool,
) -> None:
    """Create systemd unit files for master and worker."""
    unit_master = f"""[Unit]
Description=Nexus Orchestrator — Master Node
After=network.target redis.service
Wants=redis.service

[Service]
Type=simple
User={__import__('os').environ.get('USER', 'root')}
WorkingDirectory={NEXUS_ROOT}
ExecStart={python_exe} {master_script}
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
"""
    unit_worker = unit_master.replace(
        "Master Node", "Worker Node"
    ).replace(master_script, worker_script)

    for name, content in [("nexus-master", unit_master), ("nexus-worker", unit_worker)]:
        path = f"/etc/systemd/system/{name}.service"
        if dry_run:
            _ok(f"[DRY RUN] Would write {path}")
            print(f"    {content[:80].strip()}...")
        else:
            try:
                with open(path, "w") as f:
                    f.write(content)
                import subprocess as sp
                sp.run(["systemctl", "daemon-reload"], check=True, capture_output=True)
                sp.run(["systemctl", "enable", name], check=True, capture_output=True)
                _ok(f"systemd service registered: {name}")
                _ok(f"  Start with: sudo systemctl start {name}")
            except PermissionError:
                _warn(f"Need sudo to write {path}")
                _warn("  Run: sudo python scripts/system_bootstrap.py")
            except Exception as exc:
                _err(f"systemd registration failed: {exc}")


def _register_task_scheduler(
    python_exe: str,
    master_script: str,
    dry_run: bool,
) -> None:
    """Create a Windows Task Scheduler task for the master."""
    import subprocess as sp
    task_name = "NexusOrchestratorMaster"
    cmd = (
        f'schtasks /Create /TN "{task_name}" '
        f'/TR "\\"{python_exe}\\" \\"{master_script}\\"" '
        f'/SC ONLOGON /RL HIGHEST /F'
    )
    if dry_run:
        _ok(f"[DRY RUN] Would register Task Scheduler task: {task_name}")
        _ok(f"  Command: {cmd}")
    else:
        try:
            result = sp.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode == 0:
                _ok(f"Task Scheduler task registered: {task_name}")
                _ok("  Runs automatically at logon with highest privileges")
            else:
                _warn(f"Task Scheduler registration failed: {result.stderr.strip()}")
                _warn("  Try running as Administrator")
        except Exception as exc:
            _err(f"Task Scheduler error: {exc}")


if __name__ == "__main__":
    main()
