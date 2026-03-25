"""
Master node entry mirror.

The canonical CLI is ``python tools/start_master.py`` or the ``nexus-master``
console script. This module exists so tooling and docs can reference
``nexus.core.main`` without duplicating startup logic.

Ultra-Data pipeline hooks
-------------------------
After the dispatcher connects to Redis, :func:`schedule_ultra_data_pipelines`
starts background tasks: UI scrape/swarm Redis mirrors, optional group factory,
and optional BotFather factory (queue-driven).
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import sys
from pathlib import Path
from typing import Any

# ── Windows UTF-8 / Unicode fix ───────────────────────────────────────────────
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
if sys.platform == "win32":
    os.system("chcp 65001 > nul 2>&1")
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]

# ── Windows multiprocessing Pickle fix ────────────────────────────────────────
# On Windows, multiprocessing uses 'spawn' by default which requires all target
# functions to be importable at the top level (not nested closures).
# freeze_support() is required when running from a frozen executable (PyInstaller).
if sys.platform == "win32":
    multiprocessing.freeze_support()
    # Use 'spawn' explicitly — it is already the default on Windows but making it
    # explicit prevents accidental 'fork' usage that causes Pickle errors.
    try:
        multiprocessing.set_start_method("spawn", force=False)
    except RuntimeError:
        pass  # Context already set — safe to ignore


def main() -> None:
    repo = Path(__file__).resolve().parents[3]
    tools_dir = str(repo / "tools")
    if tools_dir not in sys.path:
        sys.path.insert(0, tools_dir)
    from start_master import main as _start_master_main

    _start_master_main()


def schedule_ultra_data_pipelines(redis: Any) -> None:
    """
    Fire-and-forget asyncio tasks: ``nexus:ui:scrapes``, ``nexus:ui:swarm``,
    optional group lifecycle, optional bot factory (Redis list / env queue).
    """
    from nexus.services.bot_factory import BotFactoryService
    from nexus.services.group_factory import GroupFactoryService
    from nexus.services.ui_redis_streams import run_ui_streams_loop

    asyncio.create_task(run_ui_streams_loop(redis), name="ui-redis-streams")

    if os.getenv("NEXUS_GROUP_FACTORY_ENABLED", "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        asyncio.create_task(
            GroupFactoryService(redis).run_loop(300.0),
            name="group-factory",
        )

    if os.getenv("NEXUS_BOT_FACTORY_ENABLED", "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        asyncio.create_task(
            BotFactoryService(redis).run_loop(45.0),
            name="bot-factory",
        )


if __name__ == "__main__":
    main()
