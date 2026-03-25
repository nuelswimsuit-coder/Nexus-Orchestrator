"""
Terminal UI helpers for the Nexus worker TUI.

Linux / WSL detection
---------------------
On Linux and WSL environments stdout is often a pipe (systemd service, SSH
forwarding, screen/tmux) so Rich defaults to a dumb 80-column terminal.
This module forces Rich to:
  1. Detect the real terminal dimensions on every render tick via
     shutil.get_terminal_size().
  2. Use force_terminal=True so ANSI / box-drawing codes are always emitted.
  3. Set width=None so the Console re-queries the terminal width each time
     rather than caching the value from import time.

Usage
-----
    from nexus.shared.tui import get_console

    console = get_console()
    console.print("[bold cyan]NEXUS[/]")
"""

from __future__ import annotations

import os
import shutil
import sys
from functools import lru_cache


def _is_linux_or_wsl() -> bool:
    """Return True when running on Linux or inside WSL."""
    if sys.platform == "win32":
        return False
    if sys.platform.startswith("linux"):
        return True
    # macOS: not WSL, but treat similarly for terminal detection
    return False


def _is_wsl() -> bool:
    """Detect WSL specifically (Linux kernel on Windows)."""
    if not _is_linux_or_wsl():
        return False
    try:
        with open("/proc/version", encoding="utf-8") as f:
            return "microsoft" in f.read().lower()
    except Exception:
        return False


def get_terminal_width() -> int:
    """
    Return the current terminal width.

    Forces a fresh shutil.get_terminal_size() call so the value reflects
    any resize events that occurred since process start.
    """
    try:
        return shutil.get_terminal_size(fallback=(150, 50)).columns
    except Exception:
        return 150


@lru_cache(maxsize=1)
def get_console():  # type: ignore[return]
    """
    Return a Rich Console configured for the current environment.

    On Linux / WSL:
      - force_terminal=True  → always emit ANSI codes even in pipes
      - width is set dynamically via shutil.get_terminal_size() so that
        terminal resizes are reflected without restarting the process.

    On Windows / other:
      - Standard auto-detection (works correctly in Windows Terminal / ConEmu).
    """
    try:
        from rich.console import Console  # noqa: PLC0415
    except ImportError:
        return None

    if _is_linux_or_wsl():
        # Use width=None so Rich re-queries the terminal size on every render.
        # This relies on Rich's internal _width property falling back to
        # os.get_terminal_size() when width is None.
        return Console(
            force_terminal=True,
            width=None,
        )

    return Console()


def refresh_console_width() -> None:
    """
    Update the cached console's width to match the current terminal.

    Call this inside the render loop on Linux / WSL to pick up terminal
    resizes without restarting the process.
    """
    if not _is_linux_or_wsl():
        return

    console = get_console()
    if console is None:
        return

    try:
        new_width = get_terminal_width()
        if new_width > 0:
            console.width = new_width  # type: ignore[assignment]
    except Exception:
        pass


def configure_rich_global(width: int = 150) -> None:
    """
    Monkey-patch the Rich global ``_console`` singleton used by ``rich.print``
    and logging handlers on Linux / WSL.

    Uses ``shutil.get_terminal_size()`` to detect the real terminal width at
    call time so that the value is accurate even when stdout is a pipe or the
    terminal was resized before this function runs.

    Call once at process startup (e.g. from ``start_worker.py``) before any
    Rich output is produced.
    """
    if not _is_linux_or_wsl():
        return

    try:
        import rich  # noqa: PLC0415
        from rich.console import Console  # noqa: PLC0415

        # Always query the live terminal size; fall back to the provided default.
        detected_width = shutil.get_terminal_size(fallback=(width, 50)).columns
        effective_width = detected_width if detected_width > 0 else width

        rich._console = Console(  # type: ignore[attr-defined]
            force_terminal=True,
            width=effective_width,
        )

        # Propagate to subprocesses via COLUMNS env var.
        os.environ["COLUMNS"] = str(effective_width)

    except Exception:
        pass
