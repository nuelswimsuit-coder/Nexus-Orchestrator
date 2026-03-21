"""Filesystem helpers for repository-relative paths."""

from __future__ import annotations

from pathlib import Path


def repository_root() -> Path:
    """Return the Nexus-Orchestrator repository root (parent of this package)."""
    return Path(__file__).resolve().parent.parent
