"""
Build ``Nexus_Master.exe`` — one-file (``--onefile``); console enabled for crash visibility.

Bundles ``scripts/nexus_launcher.py`` plus the ``scripts/`` tree (so child processes
can ``runpy`` ``start_api.py`` / ``start_telegram_bot.py`` / ``start_worker.py``) and
``redis-local/`` when present.

Usage (from repo root, venv activated)::

    pip install pyinstaller
    pip install -r requirements.txt
    python scripts/build_exe.py

Output: ``dist/Nexus_Master.exe`` (then copied to repo root on Windows for testing).
"""

from __future__ import annotations

import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.getcwd())

import argparse
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
root_dir = str(REPO_ROOT)
SCRIPT = REPO_ROOT / "scripts" / "nexus_launcher.py"
SCRIPTS_DIR = REPO_ROOT / "scripts"
ICON = os.path.join(root_dir, "nexus_icon.ico")
EXE_NAME = "Nexus_Master"


def _scripts_data_arg() -> str:
    if not SCRIPTS_DIR.is_dir():
        raise FileNotFoundError(f"Missing scripts directory: {SCRIPTS_DIR}")
    return f"{SCRIPTS_DIR}{os.pathsep}scripts"


def _redis_local_arg() -> str | None:
    src = REPO_ROOT / "redis-local"
    if not src.is_dir():
        return None
    return f"{src}{os.pathsep}redis-local"


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Nexus_Master.exe with PyInstaller.")
    parser.add_argument(
        "--onedir",
        action="store_true",
        help="Produce dist/Nexus_Master/ folder instead of onefile.",
    )
    args, _ = parser.parse_known_args()

    if not SCRIPT.is_file():
        print(f"Missing entry script: {SCRIPT}", file=sys.stderr)
        return 1

    if not os.path.isfile(ICON):
        print(f"Missing icon (place nexus_icon.ico in repo root): {ICON}", file=sys.stderr)
        return 1

    dist = REPO_ROOT / "dist"
    work = REPO_ROOT / "build" / "pyinstaller_nexus_master"
    work.mkdir(parents=True, exist_ok=True)

    py_args: list[str] = [
        str(SCRIPT),
        "--name",
        EXE_NAME,
        "--clean",
        f"--distpath={dist}",
        f"--workpath={work}",
        f"--specpath={REPO_ROOT / 'build'}",
        f"--icon={ICON}",
        "--collect-submodules",
        "nexus",
        "--collect-all",
        "uvicorn",
        "--collect-all",
        "fastapi",
        "--collect-all",
        "pydantic",
        "--collect-all",
        "structlog",
        "--collect-all",
        "arq",
        "--collect-all",
        "redis",
        "--collect-all",
        "rich",
        "--collect-all",
        "aiogram",
        "--hidden-import",
        "uvicorn.logging",
        "--hidden-import",
        "uvicorn.loops",
        "--hidden-import",
        "uvicorn.loops.auto",
        "--hidden-import",
        "uvicorn.protocols",
        "--hidden-import",
        "uvicorn.protocols.http",
        "--hidden-import",
        "uvicorn.protocols.http.auto",
        "--hidden-import",
        "uvicorn.protocols.websockets",
        "--hidden-import",
        "uvicorn.protocols.websockets.auto",
        "--hidden-import",
        "uvicorn.lifespan",
        "--hidden-import",
        "uvicorn.lifespan.on",
        "--hidden-import",
        "rich.console",
        "--hidden-import",
        "rich.live",
        "--hidden-import",
        "rich.table",
        "--hidden-import",
        "rich.panel",
        "--hidden-import",
        "rich.layout",
        "--hidden-import",
        "rich.progress",
        "--hidden-import",
        "rich.text",
        "--add-data",
        _scripts_data_arg(),
    ]
    redis_arg = _redis_local_arg()
    if redis_arg:
        py_args.extend(["--add-data", redis_arg])

    if args.onedir:
        py_args.append("--onedir")
    else:
        py_args.append("--onefile")

    cmd = [sys.executable, "-m", "PyInstaller", *py_args]
    print("Running:", " ".join(cmd))
    rc = subprocess.call(cmd, cwd=str(REPO_ROOT))
    if rc == 0:
        import shutil as _shutil

        built = os.path.join(str(dist), f"{EXE_NAME}.exe")
        dest = os.path.join(root_dir, f"{EXE_NAME}.exe")
        if os.path.isfile(built):
            try:
                _shutil.copy2(built, dest)
                print(f"[build_exe] Copied {built} -> {dest}")
            except Exception as copy_err:
                print(f"[build_exe] WARNING: could not copy EXE to root: {copy_err}", file=sys.stderr)
        else:
            print(f"[build_exe] WARNING: expected EXE not found at {built}", file=sys.stderr)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
