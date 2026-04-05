"""
Bridge module: canonical Telefix/SQLite helpers live in ``src/nexus/shared/db_util.py``.

The repo-root ``nexus`` package is preferred on ``sys.path`` for settings and API routers;
this file ensures ``import nexus.shared.db_util`` resolves without requiring ``src`` first.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

_CANONICAL = Path(__file__).resolve().parents[2] / "src" / "nexus" / "shared" / "db_util.py"
_IMPL_NAME = "nexus.shared._db_util_canonical"


def _canonical_mod() -> ModuleType:
    if _IMPL_NAME in sys.modules:
        return sys.modules[_IMPL_NAME]
    if not _CANONICAL.is_file():
        raise ImportError(f"nexus.shared.db_util: missing canonical file {_CANONICAL}")
    spec = importlib.util.spec_from_file_location(_IMPL_NAME, _CANONICAL)
    if spec is None or spec.loader is None:
        raise ImportError(f"nexus.shared.db_util: cannot load {_CANONICAL}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[_IMPL_NAME] = mod
    spec.loader.exec_module(mod)
    return mod


_impl: ModuleType | None = None


def __getattr__(name: str) -> Any:
    global _impl
    if _impl is None:
        _impl = _canonical_mod()
    return getattr(_impl, name)


def __dir__() -> list[str]:
    return sorted(dir(_canonical_mod() if _impl is None else _impl))
