"""
Shared path helpers for Nexus Orchestrator.

Provides a single `get_telefix_path` helper that resolves Desktop-based
project paths dynamically using `Path.home() / "Desktop"`, eliminating
hardcoded usernames and raw-string Windows paths (which cause unicodeescape
errors).  The same helper works on both Windows (Master) and Linux (Workers).
"""

from __future__ import annotations

from pathlib import Path


def get_telefix_path(folder_name: str = "") -> Path:
    """Return a Path rooted at the current user's Desktop.

    Uses ``Path.home() / "Desktop"`` so the result is correct for any OS user
    and avoids raw-string backslash literals that raise ``SyntaxWarning:
    invalid escape sequence`` on Python 3.12+.

    Args:
        folder_name: Optional sub-path under Desktop (e.g. ``"Mangement Ahu"``
                     or ``"Mangement Ahu/data"``).  Forward slashes are
                     accepted on both Windows and Linux.  Pass an empty string
                     (default) to get the Desktop directory itself.

    Returns:
        A :class:`pathlib.Path` object.  The path is *not* guaranteed to exist;
        call ``.exists()`` at the usage site if needed.

    Examples::

        desktop = get_telefix_path()
        # → Path("/home/user/Desktop")  or  Path("C:/Users/User/Desktop")

        ahu_root = get_telefix_path("Mangement Ahu")
        # → Desktop / "Mangement Ahu"

        telefix_env = get_telefix_path("Mangement Ahu") / ".env"
        telefix_db  = get_telefix_path("Mangement Ahu") / "data" / "telefix.db"
    """
    desktop = Path.home() / "Desktop"
    if folder_name:
        # Accept both "/" and "\" as separators for cross-platform callers.
        parts = folder_name.replace("\\", "/").split("/")
        return desktop.joinpath(*parts)
    return desktop
