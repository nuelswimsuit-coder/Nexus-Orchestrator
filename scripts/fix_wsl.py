"""
One-time helper to cap WSL resource usage.

Creates/updates %USERPROFILE%\\.wslconfig with:
  [wsl2]
  memory=2GB
  processors=2
"""

from __future__ import annotations

import os
from pathlib import Path


def _resolve_profile_dir() -> Path:
    profile = os.environ.get("USERPROFILE")
    if profile:
        return Path(profile).expanduser()
    return Path.home()


def _upsert_wsl2_limits(content: str) -> str:
    lines = content.splitlines()
    section_start = None
    section_end = None

    for idx, line in enumerate(lines):
        if line.strip().lower() == "[wsl2]":
            section_start = idx
            break

    if section_start is None:
        if lines and lines[-1].strip():
            lines.append("")
        lines.extend(["[wsl2]", "memory=2GB", "processors=2"])
        return "\n".join(lines) + "\n"

    section_end = len(lines)
    for idx in range(section_start + 1, len(lines)):
        stripped = lines[idx].strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            section_end = idx
            break

    section_lines = lines[section_start + 1 : section_end]
    filtered: list[str] = []
    seen_memory = False
    seen_processors = False

    for line in section_lines:
        key = line.split("=", 1)[0].strip().lower()
        if key == "memory":
            filtered.append("memory=2GB")
            seen_memory = True
        elif key == "processors":
            filtered.append("processors=2")
            seen_processors = True
        else:
            filtered.append(line)

    if not seen_memory:
        filtered.append("memory=2GB")
    if not seen_processors:
        filtered.append("processors=2")

    updated = lines[: section_start + 1] + filtered + lines[section_end:]
    return "\n".join(updated).rstrip() + "\n"


def main() -> None:
    profile_dir = _resolve_profile_dir()
    wslconfig = profile_dir / ".wslconfig"
    original = wslconfig.read_text(encoding="utf-8") if wslconfig.exists() else ""
    updated = _upsert_wsl2_limits(original)
    wslconfig.write_text(updated, encoding="utf-8")

    print(f"Updated: {wslconfig}")
    print("Applied WSL2 limits: memory=2GB, processors=2")
    print("Run this script once, then restart WSL with: wsl --shutdown")


if __name__ == "__main__":
    main()
