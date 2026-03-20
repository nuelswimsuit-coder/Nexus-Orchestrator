#!/usr/bin/env python3
"""
setup_autostart.py — Cross-platform autostart installer for Nexus Orchestrator Worker.

Usage
-----
    python setup_autostart.py

Windows
-------
    Creates nexus_startup.bat  — activates the venv and runs start_worker.py.
    Creates nexus_hidden.vbs   — VBScript wrapper that launches the bat silently
                                  (no black CMD window stays open).
    Copies   nexus_hidden.vbs  → %APPDATA%\\Microsoft\\Windows\\Start Menu\\Programs\\Startup\\
    so the Worker starts automatically on every user login.

Linux
-----
    Generates nexus-worker.service  — a systemd unit file in the project root.
    Prints the exact sudo commands needed to install and enable it.
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

# ── ANSI color support ─────────────────────────────────────────────────────────
# Enable VT100 processing on Windows 10+ terminals (no-op on Linux/macOS).
if os.name == "nt":
    os.system("")

_G = "\033[92m"       # Green
_Y = "\033[93m"       # Yellow
_R = "\033[91m"       # Red
_C = "\033[96m"       # Cyan
_B = "\033[1m"        # Bold
_X = "\033[0m"        # Reset


def _success(msg: str) -> None:
    print(f"{_G}{_B}✅  {msg}{_X}")


def _info(msg: str) -> None:
    print(f"{_C}ℹ️   {msg}{_X}")


def _warn(msg: str) -> None:
    print(f"{_Y}⚠️   {msg}{_X}")


def _error(msg: str) -> None:
    print(f"{_R}{_B}❌  {msg}{_X}")


def _header(msg: str) -> None:
    line = "═" * 62
    print(f"\n{_B}{_C}{line}")
    print(f"  {msg}")
    print(f"{line}{_X}\n")


def _cmd(msg: str) -> None:
    """Print a command the user should run."""
    print(f"  {_C}{msg}{_X}")


# ── Project root detection ─────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent


def _find_venv_activate_bat() -> Path | None:
    """Locate activate.bat for the venv (Windows only)."""
    for name in (".venv", "venv", "env"):
        p = PROJECT_ROOT / name / "Scripts" / "activate.bat"
        if p.exists():
            return p
    return None


def _find_venv_python() -> Path | None:
    """Locate the Python binary inside the project venv."""
    sub = "Scripts" if os.name == "nt" else "bin"
    exe = "python.exe" if os.name == "nt" else "python"
    for name in (".venv", "venv", "env"):
        p = PROJECT_ROOT / name / sub / exe
        if p.exists():
            return p
    return None


# ── Windows ────────────────────────────────────────────────────────────────────

def setup_windows() -> None:
    _header("⚙️  הגדרת הפעלה אוטומטית עבור Windows")
    _info(f"שורש הפרויקט: {PROJECT_ROOT}")

    activate_bat = _find_venv_activate_bat()
    python_exe   = _find_venv_python()

    if activate_bat:
        _success(f"נמצאה סביבה וירטואלית: {activate_bat.parent.parent.name}\\")
    elif python_exe:
        _warn("activate.bat לא נמצא — ייעשה שימוש ישיר ב-Python של הסביבה הווירטואלית.")
    else:
        _warn("לא נמצאה סביבה וירטואלית (.venv / venv / env). ייעשה שימוש ב-Python הנוכחי.")

    # ── 1. nexus_startup.bat ──────────────────────────────────────────────────
    bat_path = PROJECT_ROOT / "nexus_startup.bat"
    _info("יוצר nexus_startup.bat ...")

    if activate_bat:
        bat_lines = [
            "@echo off",
            f'cd /d "{PROJECT_ROOT}"',
            f'call "{activate_bat}"',
            "python scripts\\start_worker.py",
        ]
    elif python_exe:
        bat_lines = [
            "@echo off",
            f'cd /d "{PROJECT_ROOT}"',
            f'"{python_exe}" scripts\\start_worker.py',
        ]
    else:
        bat_lines = [
            "@echo off",
            f'cd /d "{PROJECT_ROOT}"',
            "python scripts\\start_worker.py",
        ]

    bat_path.write_text("\r\n".join(bat_lines) + "\r\n", encoding="utf-8")
    _success(f"נוצר: {bat_path.name}")

    # ── 2. nexus_hidden.vbs ───────────────────────────────────────────────────
    vbs_path = PROJECT_ROOT / "nexus_hidden.vbs"
    _info("יוצר nexus_hidden.vbs (הפעלה שקטה ללא חלון CMD) ...")

    vbs_content = (
        'Set WshShell = CreateObject("WScript.Shell")\r\n'
        f'WshShell.Run Chr(34) & "{bat_path}" & Chr(34), 0, False\r\n'
        'Set WshShell = Nothing\r\n'
    )
    vbs_path.write_text(vbs_content, encoding="utf-8")
    _success(f"נוצר: {vbs_path.name}")

    # ── 3. Copy VBS to Windows Startup folder ─────────────────────────────────
    appdata = os.environ.get("APPDATA", "")
    if not appdata:
        _warn("משתנה הסביבה %APPDATA% לא נמצא. לא ניתן להעתיק אוטומטית.")
        _warn(f"העתק ידנית את nexus_hidden.vbs אל:")
        _cmd(r"%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\")
        return

    startup_folder = (
        Path(appdata) / "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup"
    )

    if not startup_folder.exists():
        _warn(f"תיקיית Startup לא נמצאה: {startup_folder}")
        _warn("העתק ידנית:")
        _cmd(f'copy "{vbs_path}" "{startup_folder / vbs_path.name}"')
        return

    dest = startup_folder / "nexus_hidden.vbs"
    _info(f"מעתיק VBS אל תיקיית ההפעלה האוטומטית של Windows ...")

    try:
        shutil.copy2(vbs_path, dest)
    except PermissionError:
        _error("אין הרשאת כתיבה לתיקיית Startup.")
        _warn("הרץ את הסקריפט כמנהל (Run as Administrator), או הדבק ידנית:")
        _cmd(f'copy "{vbs_path}" "{dest}"')
        return

    _success(f"הועתק בהצלחה: {dest}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    _success("ההגדרה הושלמה! הפעלה אוטומטית פעילה. 🎉")
    print(
        f"\n{_B}{_G}"
        f"  📋 סיכום:\n"
        f"{_X}{_G}"
        f"  • בכל כניסת משתמש: nexus_hidden.vbs מופעל אוטומטית.\n"
        f"  • VBScript מריץ nexus_startup.bat ברקע (ללא חלון שחור).\n"
        f"  • Worker מתחבר ל-Redis ומתחיל לטפל במשימות.\n"
        f"\n"
        f"  🗑️  להסרה: מחק את הקובץ:\n"
        f"  {dest}\n"
        f"{_X}"
    )


# ── Linux ──────────────────────────────────────────────────────────────────────

def setup_linux() -> None:
    _header("⚙️  הגדרת שירות systemd עבור Linux")
    _info(f"שורש הפרויקט: {PROJECT_ROOT}")

    python_exe = _find_venv_python()
    if python_exe:
        _success(f"נמצא Python בסביבה הווירטואלית: {python_exe}")
    else:
        python_exe = Path(sys.executable)
        _warn(f"סביבה וירטואלית לא נמצאה. ייעשה שימוש ב: {python_exe}")

    current_user = os.environ.get("USER", "yadmin")
    service_name = "nexus-worker"
    service_file = f"{service_name}.service"
    local_path   = PROJECT_ROOT / service_file
    system_path  = Path("/etc/systemd/system") / service_file
    worker_script = PROJECT_ROOT / "scripts" / "start_worker.py"

    # ── Generate service unit ─────────────────────────────────────────────────
    _info(f"יוצר {service_file} ...")

    unit_content = (
        "[Unit]\n"
        "Description=Nexus Orchestrator Worker\n"
        "After=network.target\n"
        "\n"
        "[Service]\n"
        f"ExecStart={python_exe} {worker_script}\n"
        f"WorkingDirectory={PROJECT_ROOT}\n"
        "Restart=always\n"
        "RestartSec=10\n"
        f"User={current_user}\n"
        "Environment=PYTHONUNBUFFERED=1\n"
        "\n"
        "[Install]\n"
        "WantedBy=multi-user.target\n"
    )

    local_path.write_text(unit_content, encoding="utf-8")
    _success(f"נוצר: {local_path}")

    # ── Print sudo instructions ───────────────────────────────────────────────
    _header("📋 פקודות להפעלת השירות — העתק והרץ בטרמינל")

    steps = [
        ("# 1. העתק את קובץ השירות ל-systemd:", f'sudo cp "{local_path}" "{system_path}"'),
        ("# 2. טען מחדש את הגדרות systemd:",     "sudo systemctl daemon-reload"),
        ("# 3. הפעל הפעלה אוטומטית עם כל אתחול:", f"sudo systemctl enable {service_name}"),
        ("# 4. הפעל את השירות עכשיו:",            f"sudo systemctl start {service_name}"),
    ]

    for comment, command in steps:
        print(f"  {_Y}{_B}{comment}{_X}")
        _cmd(command)
        print()

    print(f"  {_Y}{_B}# לבדיקת סטטוס:{_X}")
    _cmd(f"sudo systemctl status {service_name}")
    print()
    print(f"  {_Y}{_B}# לצפייה בלוגים בזמן אמת:{_X}")
    _cmd(f"sudo journalctl -u {service_name} -f")
    print()

    _success(f"קובץ השירות מוכן: {local_path}")
    _info("הרץ את הפקודות למעלה עם sudo כדי להפעיל את הסרוויס.")


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    _header("🚀 Nexus Orchestrator — מתקין הפעלה אוטומטית")
    _info(f"מערכת הפעלה: {'Windows' if os.name == 'nt' else 'Linux / macOS'} ({os.name})")
    _info(f"Python: {sys.executable}")

    if os.name == "nt":
        setup_windows()
    elif os.name == "posix":
        setup_linux()
    else:
        _error(f"מערכת הפעלה לא נתמכת: {os.name!r}")
        sys.exit(1)


if __name__ == "__main__":
    main()
