"""
Nexus Supreme Control — Entry Point
Run directly:      python Launch_NexusSupreme.py
Compile to .exe:   python -m PyInstaller NexusSupreme.spec
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# ── Windows event loop fix (must be before Qt import) ─────────────────────────
if sys.platform == "win32":
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── .env loader (before anything that reads os.environ) ───────────────────────
ROOT = Path(__file__).resolve().parent

def _load_dotenv() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val

_load_dotenv()

# ── DPI + Windows taskbar ──────────────────────────────────────────────────────
try:
    import ctypes
    ctypes.windll.shcore.SetProcessDpiAwareness(2)   # Per-monitor DPI
    ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(
        "NexusSupreme.Control.2"
    )
except Exception:
    pass

# ── PyQt6 ─────────────────────────────────────────────────────────────────────
try:
    from PyQt6.QtWidgets import QApplication
    from PyQt6.QtCore import Qt
except ImportError:
    print("PyQt6 is not installed. Run:  pip install PyQt6")
    sys.exit(1)

# ── RTL layout for Hebrew UI ──────────────────────────────────────────────────
os.environ.setdefault("QT_AUTO_SCREEN_SCALE_FACTOR", "1")

# ── Watchdog: auto-restart on crash ───────────────────────────────────────────
def _self_restart() -> None:
    """Re-launch this script after a 3-second delay."""
    import subprocess, time
    print("[WATCHDOG] Restarting Nexus Supreme in 3 s...")
    time.sleep(3)
    subprocess.Popen([sys.executable, __file__], env=os.environ.copy())


def main() -> int:
    app = QApplication(sys.argv)
    app.setApplicationName("Nexus Supreme Control")
    app.setOrganizationName("Jacob Chatan")
    app.setQuitOnLastWindowClosed(False)   # keep alive in tray

    # Ensure data dirs exist
    for d in ("data/archives", "data/archives/media", "logs"):
        (ROOT / d).mkdir(parents=True, exist_ok=True)

    from nexus_supreme.gui.main_window import NexusSupremeWindow
    win = NexusSupremeWindow()
    win.show()

    return app.exec()


if __name__ == "__main__":
    try:
        code = main()
    except Exception as exc:
        print(f"[CRITICAL] {exc}")
        code = 1

    if code != 0:
        _self_restart()

    sys.exit(code)
