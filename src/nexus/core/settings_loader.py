"""
Optional YAML overlay: ``configs/settings.yaml`` → environment defaults.

Keys become ``UPPER_SNAKE`` env names; existing ``os.environ`` wins (no override).
"""

from __future__ import annotations

import os
from pathlib import Path


def apply_yaml_settings_defaults(repo_root: Path) -> None:
    path = repo_root / "configs" / "settings.yaml"
    if not path.is_file():
        return
    try:
        import yaml
    except ImportError:
        return
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(data, dict):
        return
    for k, v in data.items():
        if not isinstance(k, str):
            continue
        env_key = k.upper()
        if env_key in os.environ or v is None:
            continue
        os.environ[env_key] = str(v)
